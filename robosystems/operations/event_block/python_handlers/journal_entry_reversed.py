"""Journal entry reversed event handler.

Fires when create-event-block runs with event_type='journal_entry_reversed'
and apply_handlers=True. Posts a reversing entry against an existing posted
journal entry, flipping debits and credits and marking the original as
reversed. Both Entry rows (the original and the new reversing entry) carry
a `triggered_by_event_id` link to this event.

Replaces the standalone `reverse-journal-entry` operation. The underlying
logic (entry-status validation + line-item flip + closed-period gate) lives
in `reverse_journal_entry` (commands/journal_entries.py) and is reused
verbatim.

Event status after success: 'fulfilled' (the reversal is terminal — the
original is now closed by an offsetting posted entry).
"""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field
from sqlalchemy import update
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.api.extensions.journal_entries import (
  ReverseJournalEntryRequest,
)
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.roboledger.commands._guards import (
  ClosedPeriodError,
  assert_period_not_closed,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  reverse_journal_entry,
)

from .types import (
  EventBlockPythonHandler,
  HandlerPreview,
  HandlerResult,
)


class JournalEntryReversedMetadata(BaseModel):
  """Metadata for a journal_entry_reversed event."""

  entry_id: str = Field(
    ..., description="The posted journal entry being reversed (`je_` prefix)."
  )
  posting_date: date | None = Field(
    None,
    description=(
      "Posting date for the reversing entry. Defaults to today when omitted."
    ),
  )
  memo: str | None = Field(
    None,
    description=(
      "Memo for the reversing entry. Defaults to "
      "'Reversal of journal entry {entry_id}' when omitted."
    ),
  )
  reason: str | None = Field(
    None,
    description=(
      "Free-form business reason for the reversal (audit trail). Stored on "
      "the event metadata; not propagated to the entry memo unless the "
      "caller passes it explicitly via `memo`."
    ),
  )


def dispatch(
  session: Session,
  event: Event,
  metadata: JournalEntryReversedMetadata,
  created_by: str,
) -> HandlerResult:
  """Reverse the original entry; link both rows to the event."""
  body = ReverseJournalEntryRequest(
    entry_id=metadata.entry_id,
    posting_date=metadata.posting_date,
    memo=metadata.memo,
  )
  reversing = reverse_journal_entry(session, body, created_by)

  # Link the reversing Entry to the event.
  session.execute(
    update(Entry).where(Entry.id == reversing.id).values(triggered_by_event_id=event.id)
  )

  # Also link the now-`reversed` original — completes the audit chain so
  # queries can find the originating reversal event from either side.
  session.execute(
    update(Entry)
    .where(Entry.id == metadata.entry_id)
    .values(triggered_by_event_id=event.id)
  )

  logger.info(
    "journal_entry_reversed event %s fired: original=%s reversing=%s",
    event.id,
    metadata.entry_id,
    reversing.id,
  )

  return HandlerResult(entry_ids=[reversing.id])


def dispatch_preview(
  session: Session,
  body: CreateEventBlockRequest,
  metadata: JournalEntryReversedMetadata,
) -> HandlerPreview:
  """Validate the original entry + closed-period gate without persisting."""
  from sqlalchemy import select

  from robosystems.models.extensions.roboledger.line_item import LineItem

  errors: list[str] = []

  original = session.get(Entry, metadata.entry_id)
  if original is None:
    return HandlerPreview(
      would_succeed=False,
      planned_entries=[],
      computed_values={},
      validation_errors=[f"Journal entry not found: {metadata.entry_id}"],
    )

  if original.status != "posted":
    errors.append(
      f"Journal entry {metadata.entry_id} is {original.status!r}; only "
      "posted entries can be reversed."
    )

  posting_date = metadata.posting_date or body.occurred_at.date()
  try:
    assert_period_not_closed(session, posting_date)
  except ClosedPeriodError as e:
    errors.append(str(e))

  if errors:
    return HandlerPreview(
      would_succeed=False,
      planned_entries=[],
      computed_values={},
      validation_errors=errors,
    )

  # Build the planned reversal preview by flipping the original's line items.
  original_lines = list(
    session.execute(select(LineItem).where(LineItem.entry_id == original.id))
    .scalars()
    .all()
  )
  flipped = [
    {
      "element_id": li.element_id,
      "debit_amount": int(li.credit_amount),
      "credit_amount": int(li.debit_amount),
    }
    for li in sorted(original_lines, key=lambda x: x.line_order)
  ]

  memo = metadata.memo or f"Reversal of journal entry {metadata.entry_id}"
  return HandlerPreview(
    would_succeed=True,
    planned_entries=[
      {
        "posting_date": str(posting_date),
        "memo": memo,
        "entry_type": "reversing",
        "line_items": flipped,
      }
    ],
    computed_values={
      "original_entry_id": metadata.entry_id,
      "original_status": original.status,
      "reason": metadata.reason,
    },
    validation_errors=[],
  )


JOURNAL_ENTRY_REVERSED_HANDLER = EventBlockPythonHandler(
  event_type="journal_entry_reversed",
  display_name="Journal Entry Reversed",
  metadata_schema=JournalEntryReversedMetadata,
  target_status="fulfilled",
  dispatch=dispatch,
  dispatch_preview=dispatch_preview,
)
