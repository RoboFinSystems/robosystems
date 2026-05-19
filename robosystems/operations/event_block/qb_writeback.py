"""QuickBooks write-back helpers (Phase 4 §4.2).

Translates a RoboSystems-side `Event` row's `metadata` payload into a
`quickbooks.objects.JournalEntry` and posts it via the QB API with a
RequestId for idempotency. Returns the `qb_txn_id` on success.

Used by `operations/event_block/commands.execute_event_block`. Kept
out of `commands.py` because the JE-build + Element-lookup logic is
focused and worth isolating from the command orchestrator.

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

from datetime import datetime
from typing import Any

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

from robosystems.logger import logger
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger.event import Event


class QBWritebackError(Exception):
  """QB API rejected the journal-entry write.

  Carries a structured payload the caller stamps on
  `event.metadata.last_outbound_error` for the reconciliation queue.
  """

  def __init__(self, payload: dict[str, Any]) -> None:
    super().__init__(payload.get("message", "QB JE write rejected"))
    self.payload = payload


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
  the JE handler upstream).
  """
  debit = int(line_item.get("debit_amount") or 0)
  credit = int(line_item.get("credit_amount") or 0)
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
  nested_entries = metadata.get("entries")
  qb_txn_ids: list[str] = []

  if isinstance(nested_entries, list) and nested_entries:
    # Nested shape — one QB JE per entry.
    for idx, entry in enumerate(nested_entries):
      je = _build_qb_journal_entry(
        session,
        posting_date=entry.get("posting_date"),
        memo=entry.get("memo"),
        line_items=entry.get("line_items") or [],
      )
      # Suffix per-entry so multi-entry events have distinct request_ids
      # but the per-call write is still retry-safe via QB's dedup window.
      request_id = f"{event.id}-e{idx}"
      qb_txn_ids.append(_save_with_retry(je, qb_client, request_id, event.id))
  else:
    # Flat shape — single QB JE.
    je = _build_qb_journal_entry(
      session,
      posting_date=metadata.get("posting_date"),
      memo=metadata.get("memo"),
      line_items=metadata.get("line_items") or [],
    )
    qb_txn_ids.append(_save_with_retry(je, qb_client, event.id, event.id))

  return qb_txn_ids


def _save_with_retry(
  je: QBJournalEntry,
  qb_client,
  request_id: str,
  event_id: str,
) -> str:
  """Wrap `je.save(qb=client, request_id=...)` in QBWritebackError
  conversion.

  The underlying QB SDK call inherits `_QB_RETRY` retry semantics
  automatically when called against a `QBClient` whose `client` is
  the patched instance — but the SDK itself doesn't wrap save() in
  the retry decorator (only our pull-side methods do). Network blips
  on save() therefore surface here; QB's RequestId dedup keeps the
  retry safe.
  """
  try:
    saved = je.save(qb=qb_client, request_id=request_id)
  except QuickbooksException as e:
    payload = {
      "code": "qb_validation_error",
      "message": str(e),
      "qb_error_code": getattr(e, "error_code", None),
      "qb_response_at": datetime.now().isoformat(),
      "event_id": event_id,
      "request_id": request_id,
    }
    logger.warning(
      f"QB rejected journal entry for event {event_id} (request_id={request_id}): {e}"
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
