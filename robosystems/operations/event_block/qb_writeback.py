"""QuickBooks write-back helpers.

Translates a RoboSystems-side `Event` row's `metadata` payload into a
`quickbooks.objects.JournalEntry` and posts it via the QB API with a
RequestId for idempotency. Returns the `qb_txn_id` on success.

Used by `operations/event_block/commands.execute_event_block`.

The QB SDK's `JournalEntry.save(qb=client, request_id=event.id)`
treats `request_id` as the RequestId header — QB recognises duplicate
RequestIds for ~5 minutes and returns the prior result rather than
creating a new entry, so our retry-on-network-blip path is safe.

`Event.metadata` shape accepted (mirrors the journal_entry_recorded
handler's `JournalEntryRecordedMetadata` Pydantic):

- **Flat**: top-level `posting_date`, `memo`, `line_items` (a single
  entry per event — the common case for manual JE and schedule drafts).
- **Nested**: top-level `entries` array, each with its own
  `posting_date`, `memo`, `line_items` (multi-entry path used by QB
  ingest; one event → multiple journal entries on the QB side).

For the nested case we post **one QB JournalEntry per entry**. QB's
JournalEntry endpoint doesn't natively support multi-entry-per-call,
so a multi-entry RL event becomes N round-trips. The returned
`qb_txn_id` is a list when N > 1.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import requests
from quickbooks.exceptions import QuickbooksException
from quickbooks.objects.base import Ref
from quickbooks.objects.journalentry import (
  JournalEntry as QBJournalEntry,
)
from quickbooks.objects.journalentry import (
  JournalEntryLine,
  JournalEntryLineDetail,
)
from sqlalchemy.orm import Session

from robosystems.adapters.quickbooks.client.api import _QB_RETRY
from robosystems.logger import logger
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.line_item import LineItem


class QBWritebackError(Exception):
  """QB API rejected the journal-entry write.

  Carries a structured payload the caller stamps on
  `event.metadata.last_outbound_error` for the reconciliation queue.
  """

  def __init__(self, payload: dict[str, Any]) -> None:
    super().__init__(payload.get("message", "QB JE write rejected"))
    self.payload = payload


def _validated_cents(value: Any, field: str) -> int:
  """Coerce a line-item amount to integer cents (``None`` maps to 0).

  Amounts are integer cents end-to-end. A float would silently lose
  precision via `int(125.50)` → `125`, so any non-``int`` type raises
  `QBWritebackError` rather than being coerced. ``bool`` counts as invalid
  even though `isinstance(True, int)` is True in Python — a True/False
  arriving as an amount is a caller bug.
  """
  if value is None:
    return 0
  if isinstance(value, bool) or not isinstance(value, int):
    raise QBWritebackError(
      {
        "code": "invalid_amount_type",
        "message": (
          f"{field} must be an integer (cents); got "
          f"{type(value).__name__}: {value!r}. Floats and strings are "
          f"rejected to prevent silent precision loss."
        ),
      }
    )
  return value


def _resolve_qb_account_id(session: Session, element_id: str) -> str:
  """Local element_id → QB Account.Id (the `external_id` we captured
  at QB-import time).

  Raises if the element isn't QB-sourced or has no external_id — both
  cases mean we can't post this line to QB.
  """
  element = session.query(Element).filter(Element.id == element_id).first()
  if element is None:
    raise QBWritebackError(
      {
        "code": "element_not_found",
        "message": f"Element {element_id} not found in extensions DB.",
      }
    )
  if not element.external_id:
    raise QBWritebackError(
      {
        "code": "element_not_qb_mapped",
        "message": (
          f"Element {element_id} (code={element.code}, name={element.name}) "
          f"has no external_id — it isn't mapped to a QuickBooks account "
          f"and can't be posted via write-back."
        ),
      }
    )
  return str(element.external_id)


def _build_qb_line(
  session: Session,
  line_item: dict[str, Any],
) -> JournalEntryLine:
  """Translate one RL line_item dict into a QB JournalEntryLine.

  RL line shape: `{element_id, debit_amount, credit_amount, description}`.
  Exactly one of debit_amount / credit_amount is non-zero (validated by
  the JE handler upstream). Amounts are integer cents — a float would
  silently lose precision via `int()` truncation, so we fail loud on
  any non-integer type.
  """
  debit = _validated_cents(line_item.get("debit_amount"), "debit_amount")
  credit = _validated_cents(line_item.get("credit_amount"), "credit_amount")
  if debit > 0 and credit > 0:
    raise QBWritebackError(
      {
        "code": "invalid_line_both_debit_and_credit",
        "message": "Line has both debit_amount and credit_amount > 0.",
      }
    )
  if debit == 0 and credit == 0:
    raise QBWritebackError(
      {
        "code": "invalid_line_zero_amount",
        "message": "Line has neither debit nor credit set.",
      }
    )

  element_id = line_item.get("element_id")
  if not element_id:
    raise QBWritebackError(
      {
        "code": "line_missing_element_id",
        "message": "Line is missing element_id — unresolved external_id?",
      }
    )
  qb_account_id = _resolve_qb_account_id(session, str(element_id))

  qb_line = JournalEntryLine()
  # Amounts in QB are dollars (float); RL stores cents (int).
  qb_line.Amount = (debit if debit > 0 else credit) / 100.0
  qb_line.Description = str(line_item.get("description") or "")

  detail = JournalEntryLineDetail()
  detail.PostingType = "Debit" if debit > 0 else "Credit"
  account_ref = Ref()
  account_ref.value = qb_account_id
  detail.AccountRef = account_ref
  qb_line.JournalEntryLineDetail = detail

  return qb_line


def _build_qb_journal_entry(
  session: Session,
  posting_date,
  memo: str | None,
  line_items: list[dict[str, Any]],
) -> QBJournalEntry:
  """Build one QB JournalEntry from the (posting_date, memo, line_items)
  triple. Caller iterates over nested-shape entries when applicable."""
  je = QBJournalEntry()
  je.TxnDate = (
    posting_date.isoformat()
    if hasattr(posting_date, "isoformat")
    else str(posting_date)
  )
  je.PrivateNote = memo or ""
  je.Line = [_build_qb_line(session, li) for li in line_items]
  return je


def _entries_from_draft_rows(session: Session, event_id: str) -> list[dict[str, Any]]:
  """Build nested-shape entry dicts from the GL rows linked to an event.

  These rows are the ledger's version of the event — the drafts close will
  post — and so the thing to publish. Every handler materializes them
  (``journal_entry_recorded`` via `create_journal_entry`,
  ``schedule_entry_due`` / ``asset_disposed`` via the schedule service);
  ``event.metadata`` is the capture, which a draft correction leaves behind.
  Returns one entry dict per linked Entry, in creation order.
  """
  entries = (
    session.query(Entry)
    .filter(Entry.triggered_by_event_id == event_id)
    .order_by(Entry.created_at, Entry.id)
    .all()
  )
  result: list[dict[str, Any]] = []
  for entry in entries:
    lines = (
      session.query(LineItem)
      .filter(LineItem.entry_id == entry.id)
      .order_by(LineItem.line_order)
      .all()
    )
    result.append(
      {
        "posting_date": entry.posting_date,
        "memo": entry.memo,
        "line_items": [
          {
            "element_id": li.element_id,
            "debit_amount": int(li.debit_amount or 0),
            "credit_amount": int(li.credit_amount or 0),
            "description": li.description,
          }
          for li in lines
        ],
      }
    )
  return result


def post_event_to_qb(
  session: Session,
  event: Event,
  qb_client,
) -> list[str]:
  """Post the event's journal-entry plan to QB.

  Returns the list of QB-side transaction IDs (one per nested entry,
  or a single-element list for flat-shape events). Raises
  `QBWritebackError` on QB rejection — caller stamps the payload on
  the event's metadata and transitions to status='pending' for retry.

  Idempotency: each QB POST carries `request_id=event.id`. Multi-entry
  nested-shape events suffix the request_id with the entry index so
  each call has a distinct key but the per-entry write is still
  retry-safe within QB's ~5-minute dedup window.
  """
  metadata = dict(event.metadata_ or {})
  qb_txn_ids: list[str] = []

  # Resolve the entries to post. The ledger rows come first: what publishes
  # to QuickBooks must be what the ledger posts, and the rows are what
  # `execute` promotes to `posted` a few lines later. `event.metadata` is
  # the *capture* — a draft corrected through `update-journal-entry` before
  # close has moved on from it, and publishing the capture would put the
  # original in QuickBooks and the correction in the ledger.
  #   1. Entry/LineItem rows linked by ``triggered_by_event_id`` — every
  #      handler materializes these (journal_entry_recorded via
  #      `create_journal_entry`; schedule_entry_due / asset_disposed via the
  #      schedule service).
  #   2. metadata["entries"]      — nested shape, for an event that never
  #      materialized rows
  #   3. metadata["line_items"]   — flat shape, same case
  nested_entries = _entries_from_draft_rows(session, str(event.id))
  if not nested_entries:
    captured = metadata.get("entries")
    if isinstance(captured, list) and captured:
      nested_entries = captured
    elif metadata.get("line_items"):
      nested_entries = [
        {
          "posting_date": metadata.get("posting_date"),
          "memo": metadata.get("memo"),
          "line_items": metadata.get("line_items"),
        }
      ]

  if not nested_entries:
    raise QBWritebackError(
      {
        "code": "no_line_items",
        "message": (
          f"Event {event.id} has no line items in metadata and no linked "
          f"draft entries — nothing to publish to QuickBooks."
        ),
      }
    )

  # One QB JournalEntry per entry. Single-entry events keep request_id =
  # event.id (original idempotency key); multi-entry events suffix per
  # entry so each call has a distinct key but stays retry-safe in QB's
  # ~5-minute dedup window.
  single = len(nested_entries) == 1
  for idx, entry in enumerate(nested_entries):
    je = _build_qb_journal_entry(
      session,
      posting_date=entry.get("posting_date"),
      memo=entry.get("memo"),
      line_items=entry.get("line_items") or [],
    )
    request_id = str(event.id) if single else f"{event.id}-e{idx}"
    qb_id = _save_with_retry(je, qb_client, request_id, event.id)
    qb_txn_ids.append(f"JournalEntry_{qb_id}")

  # The bare QB Id is prefixed with the entity type ("JournalEntry_") above so
  # the stamped value matches the external_id format the QB importer builds
  # for incoming rows (`f"{qb_class}_{tx_id}"`). The cross-source matcher in
  # the extensions loader compares incoming external_ids against
  # `metadata.qb_external_id`, so the two must use the same convention.
  return qb_txn_ids


@_QB_RETRY
def _qb_save_with_retry(je: QBJournalEntry, qb_client, request_id: str):
  """Inner save call — decorated with `_QB_RETRY` so transient
  transport errors retry per `_is_retryable_qb_error` (network +
  429/5xx). Lets exceptions propagate raw so the decorator can decide
  whether to retry; the outer `_save_with_retry` wrapper catches what
  the decorator re-raises (after retry exhaustion or for non-retryable
  errors) and converts to `QBWritebackError`.
  """
  return je.save(qb=qb_client, request_id=request_id)


def _save_with_retry(
  je: QBJournalEntry,
  qb_client,
  request_id: str,
  event_id: str,
) -> str:
  """Wrap `_qb_save_with_retry` in `QBWritebackError` conversion.

  `_QB_RETRY` (applied to the inner function) retries on transient
  transport errors. QB's RequestId dedup window (`request_id`
  parameter forwarded into the API call) keeps the retry safe —
  repeated POSTs with the same RequestId return the prior result
  rather than creating a new entry.

  Caught exception types (after retry decisions):
  - `requests.exceptions.RequestException` (network / timeout /
    transport): tenacity retries; if it exhausts attempts the
    exception comes out here and we wrap as `qb_transport_error`.
  - `QuickbooksException` (QB API error envelope — validation,
    balance, closed-period in QB, auth): non-retryable per the
    predicate; surfaces immediately. Wrap as `qb_validation_error`.
  """
  try:
    saved = _qb_save_with_retry(je, qb_client, request_id)
  except (QuickbooksException, requests.exceptions.RequestException) as e:
    is_network = isinstance(e, requests.exceptions.RequestException)
    payload = {
      "code": "qb_transport_error" if is_network else "qb_validation_error",
      "message": str(e),
      "qb_error_code": getattr(e, "error_code", None) if not is_network else None,
      "qb_response_at": datetime.now(UTC).isoformat(),
      "event_id": event_id,
      "request_id": request_id,
    }
    logger.warning(
      f"QB rejected journal entry for event {event_id} "
      f"(request_id={request_id}, transport_error={is_network}): {e}"
    )
    raise QBWritebackError(payload) from e
  if not getattr(saved, "Id", None):
    raise QBWritebackError(
      {
        "code": "qb_no_id_returned",
        "message": (
          "QB save() returned without an Id — unexpected SDK response shape."
        ),
        "event_id": event_id,
        "request_id": request_id,
      }
    )
  return str(saved.Id)
