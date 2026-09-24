"""journal_entry_reversed handler: posts a reversing entry via
`reverse_journal_entry` and links it to the event, which ends ``fulfilled``.
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
from robosystems.operations.locking import RowLockedError
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
  """Reverse the original entry; link the reversing entry to the event."""
  body = ReverseJournalEntryRequest(
    entry_id=metadata.entry_id,
    posting_date=metadata.posting_date,
    memo=metadata.memo,
  )
  reversing = reverse_journal_entry(session, body, created_by)

  session.execute(
    update(Entry).where(Entry.id == reversing.id).values(triggered_by_event_id=event.id)
  )

  # Never re-point the original: `triggered_by_event_id` is creation
  # provenance (retraction guard, QB rebuild wipe). The reversal is reachable
  # from the original through the reversing entry's `reversal_of`.

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

  # Same check as the command: an `auto_reverse` schedule leaves the original
  # `posted` with a reversal already in place.
  existing_reversal = session.execute(
    select(Entry.id).where(Entry.reversal_of == original.id)
  ).scalar_one_or_none()
  if existing_reversal is not None:
    errors.append(
      f"Journal entry {metadata.entry_id} already has a reversing entry "
      f"({existing_reversal}); an entry is reversed at most once."
    )

  posting_date = metadata.posting_date or body.occurred_at.date()
  try:
    assert_period_not_closed(session, posting_date)
  except (ClosedPeriodError, RowLockedError) as e:
    errors.append(str(e))

  if errors:
    return HandlerPreview(
      would_succeed=False,
      planned_entries=[],
      computed_values={},
      validation_errors=errors,
    )

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
