"""QuickBooks write-back: post an event's draft Entry rows as QB JournalEntries.

Each draft Entry posts as its own JournalEntry with the entry id as its
RequestId (QB dedups repeats for ~5 minutes, so retries are safe), and the
QB id is recorded per entry in ``metadata.qb_entry_ids``. Entries spanning
periods publish one period at a time; a partial failure keeps what landed.
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
  """QB rejected the write; ``payload`` goes to
  `event.metadata.last_outbound_error`."""

  def __init__(self, payload: dict[str, Any]) -> None:
    super().__init__(payload.get("message", "QB JE write rejected"))
    self.payload = payload
    # Entries that reached QuickBooks before the failure, entry_id → QB id.
    self.published: dict[str, str] = {}


QB_ENTRY_IDS_KEY = "qb_entry_ids"


def published_entry_ids(metadata: dict[str, Any] | None) -> dict[str, str]:
  """Entry id → QB JournalEntry id for the event's entries already in QB."""
  recorded = (metadata or {}).get(QB_ENTRY_IDS_KEY)
  return dict(recorded) if isinstance(recorded, dict) else {}


def _validated_cents(value: Any, field: str) -> int:
  """Integer cents (``None`` → 0); any non-int, including bool, raises
  rather than risk silent precision loss."""
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
  """Local element_id → QB Account.Id (the element's `external_id`)."""
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
  """Translate one line_item dict (amounts in cents) into a QB JournalEntryLine."""
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
  """Build one QB JournalEntry from (posting_date, memo, line_items)."""
  je = QBJournalEntry()
  je.TxnDate = (
    posting_date.isoformat()
    if hasattr(posting_date, "isoformat")
    else str(posting_date)
  )
  je.PrivateNote = memo or ""
  je.Line = [_build_qb_line(session, li) for li in line_items]
  return je


def _draft_entries(
  session: Session, event_id: str, entry_ids: list[str] | None
) -> list[tuple[str, dict[str, Any]]]:
  """(entry_id, entry dict) for the event's draft GL rows, in creation order.

  The rows, not ``event.metadata``, are published: a draft correction
  leaves the captured metadata behind.
  """
  query = session.query(Entry).filter(
    Entry.triggered_by_event_id == event_id, Entry.status == "draft"
  )
  if entry_ids is not None:
    query = query.filter(Entry.id.in_(entry_ids))
  result: list[tuple[str, dict[str, Any]]] = []
  for entry in query.order_by(Entry.created_at, Entry.id).all():
    lines = (
      session.query(LineItem)
      .filter(LineItem.entry_id == entry.id)
      .order_by(LineItem.line_order)
      .all()
    )
    result.append(
      (
        str(entry.id),
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
        },
      )
    )
  return result


def post_event_to_qb(
  session: Session,
  event: Event,
  qb_client,
  *,
  entry_ids: list[str] | None = None,
) -> dict[str, str]:
  """Post the event's unpublished draft entries to QB, one JournalEntry each.

  Returns entry_id → QB id (prefixed ``JournalEntry_``, the external_id
  format the QB importer builds, so the cross-source matcher can compare
  the two) for the entries posted by this call.

  Every entry is built — accounts resolved, amounts validated — before the
  first POST, so a mapping error publishes nothing. A QB rejection mid-batch
  raises `QBWritebackError` with ``published`` holding the entries that
  landed first; the caller must record them. Each POST carries the entry id
  as its RequestId, so a retry inside QB's ~5-minute window is deduplicated.
  """
  already = published_entry_ids(event.metadata_)
  pending = [
    (entry_id, entry)
    for entry_id, entry in _draft_entries(session, str(event.id), entry_ids)
    if entry_id not in already
  ]
  if not pending:
    return {}

  built = [
    (
      entry_id,
      _build_qb_journal_entry(
        session,
        posting_date=entry.get("posting_date"),
        memo=entry.get("memo"),
        line_items=entry.get("line_items") or [],
      ),
    )
    for entry_id, entry in pending
  ]

  published: dict[str, str] = {}
  for entry_id, je in built:
    try:
      qb_id = _save_with_retry(je, qb_client, entry_id, event.id)
    except QBWritebackError as e:
      e.published = published
      raise
    published[entry_id] = f"JournalEntry_{qb_id}"
  return published


@_QB_RETRY
def _qb_save_with_retry(je: QBJournalEntry, qb_client, request_id: str):
  """Raw save; exceptions propagate so `_QB_RETRY` can decide on retry."""
  return je.save(qb=qb_client, request_id=request_id)


def _save_with_retry(
  je: QBJournalEntry,
  qb_client,
  request_id: str,
  event_id: str,
) -> str:
  """Save with retry, converting failures to `QBWritebackError`
  (`qb_transport_error` after retries are exhausted, `qb_validation_error`
  for a QB API error). Returns the QB Id.
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
