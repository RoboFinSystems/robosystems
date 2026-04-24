"""Journal entry recorded event handler.

Fires when create-event-block runs with event_type='journal_entry_recorded'
and apply_handlers=True. Creates a balanced journal entry (plus a synthetic
Transaction when none is supplied) and links both rows to the event.

Replaces the standalone `create-journal-entry` operation. The underlying
logic (balance validation + closed-period gate + auto Transaction) lives in
`create_journal_entry` (commands/journal_entries.py) and is reused verbatim.

Event status after success:
- 'classified' when metadata.status == 'draft' (default) — the draft still
  has to go through close-period to be posted.
- 'fulfilled' when metadata.status == 'posted' (historical data import) —
  the entry is terminal.
"""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, Field
from sqlalchemy import update
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.api.extensions.journal_entries import (
  CreateJournalEntryRequest,
  JournalEntryLineItemInput,
)
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.transaction import Transaction
from robosystems.operations.roboledger.commands._guards import (
  ClosedPeriodError,
  assert_period_not_closed,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  UnbalancedJournalEntryError,
  _validate_and_normalize_lines,
  create_journal_entry,
)

from .types import (
  EventBlockPythonHandler,
  HandlerPreview,
  HandlerResult,
)


class JournalEntryRecordedMetadata(BaseModel):
  """Metadata for a journal_entry_recorded event.

  Mirrors `CreateJournalEntryRequest` so the wire shape is 1:1 with the
  retired `create-journal-entry` op — agents that already build a journal
  entry body can wrap it in `metadata` and switch to `create-event-block`
  without re-deriving field names.
  """

  posting_date: date
  memo: str
  line_items: list[JournalEntryLineItemInput] = Field(..., min_length=2)
  type: Literal["standard", "adjusting", "closing", "reversing"] = "standard"
  status: Literal["draft", "posted"] = "draft"
  transaction_id: str | None = None


def dispatch(
  session: Session,
  event: Event,
  metadata: JournalEntryRecordedMetadata,
  created_by: str,
) -> HandlerResult:
  """Create the journal entry; link Entry + Transaction to the event.

  Mutates `event.status` to 'fulfilled' when metadata.status=='posted'
  (the row is flushed-but-not-committed so this just works — the caller
  in commands.py commits after dispatch).
  """
  body = CreateJournalEntryRequest(
    posting_date=metadata.posting_date,
    memo=metadata.memo,
    line_items=metadata.line_items,
    type=metadata.type,
    status=metadata.status,
    transaction_id=metadata.transaction_id,
  )
  response = create_journal_entry(session, body, created_by)

  entry_ids: list[str] = []
  transaction_ids: list[str] = []

  if response.id:
    session.execute(
      update(Entry)
      .where(Entry.id == response.id)
      .values(triggered_by_event_id=event.id)
    )
    entry_ids.append(response.id)

  if response.transaction_id:
    session.execute(
      update(Transaction)
      .where(Transaction.id == response.transaction_id)
      .values(triggered_by_event_id=event.id)
    )
    transaction_ids.append(response.transaction_id)

  # Dynamic status override: posted entries are terminal.
  if metadata.status == "posted":
    event.status = "fulfilled"

  logger.info(
    "journal_entry_recorded event %s fired: entry=%s txn=%s status=%s",
    event.id,
    response.id,
    response.transaction_id,
    metadata.status,
  )

  return HandlerResult(entry_ids=entry_ids, transaction_ids=transaction_ids)


def dispatch_preview(
  session: Session,
  body: CreateEventBlockRequest,
  metadata: JournalEntryRecordedMetadata,
) -> HandlerPreview:
  """Validate balance + closed-period + line items without persisting."""
  errors: list[str] = []
  total_debit = 0
  total_credit = 0

  try:
    assert_period_not_closed(session, metadata.posting_date)
  except ClosedPeriodError as e:
    errors.append(str(e))

  try:
    _normalized, total_debit, total_credit = _validate_and_normalize_lines(
      metadata.line_items
    )
  except UnbalancedJournalEntryError as e:
    errors.append(str(e))
  except ValueError as e:
    errors.append(str(e))

  if errors:
    return HandlerPreview(
      would_succeed=False,
      planned_entries=[],
      computed_values={
        "total_debit_cents": total_debit,
        "total_credit_cents": total_credit,
      },
      validation_errors=errors,
    )

  return HandlerPreview(
    would_succeed=True,
    planned_entries=[
      {
        "posting_date": str(metadata.posting_date),
        "memo": metadata.memo,
        "entry_type": metadata.type,
        "line_items": [li.model_dump() for li in metadata.line_items],
      }
    ],
    computed_values={
      "total_debit_cents": total_debit,
      "total_credit_cents": total_credit,
      "target_status": "fulfilled" if metadata.status == "posted" else "classified",
    },
    validation_errors=[],
  )


JOURNAL_ENTRY_RECORDED_HANDLER = EventBlockPythonHandler(
  event_type="journal_entry_recorded",
  display_name="Journal Entry Recorded",
  metadata_schema=JournalEntryRecordedMetadata,
  # Initial status — the handler overrides to 'fulfilled' when
  # metadata.status == 'posted'.
  target_status="classified",
  dispatch=dispatch,
  dispatch_preview=dispatch_preview,
)
