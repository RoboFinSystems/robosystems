"""Schedule entry due event handler.

Fires when create-event-block runs with event_type='schedule_entry_due' and
apply_handlers=True. Drafts (or refreshes) a closing entry derived from a
schedule's facts for the given period, then links the entry to the event.

Replaces the standalone `create-closing-entry` operation. The underlying
logic (idempotent 5-outcome reconcile) lives in
`ScheduleService.create_closing_entry` and is reused verbatim — this
handler just sequences it inside the event-block unit of work.

Event status after success: 'classified' (the draft still has to go
through close-period to become posted).
"""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field
from sqlalchemy import update
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.roboledger.schedules.service import ScheduleService

from .types import (
  EventBlockPythonHandler,
  HandlerPreview,
  HandlerResult,
)


class ScheduleEntryDueMetadata(BaseModel):
  """Metadata for a schedule_entry_due event."""

  schedule_id: str = Field(
    ..., description="The schedule structure id (struct_ prefix)."
  )
  posting_date: date = Field(..., description="Posting date for the draft entry.")
  period_start: date = Field(..., description="Period start to pull facts from.")
  period_end: date = Field(..., description="Period end.")
  memo: str | None = Field(
    None,
    description=(
      "Override memo. Defaults to the schedule's memo_template with "
      "{structure_name} interpolated."
    ),
  )


def dispatch(
  session: Session,
  event: Event,
  metadata: ScheduleEntryDueMetadata,
  created_by: str,
) -> HandlerResult:
  """Draft or refresh the closing entry; link it to the event."""
  service = ScheduleService()
  result = service.create_closing_entry(
    session,
    structure_id=metadata.schedule_id,
    posting_date=metadata.posting_date,
    period_start=metadata.period_start,
    period_end=metadata.period_end,
    created_by=created_by,
    memo=metadata.memo,
  )

  entry_ids: list[str] = []
  if result.entry_id:
    session.execute(
      update(Entry)
      .where(Entry.id == result.entry_id)
      .values(triggered_by_event_id=event.id)
    )
    entry_ids.append(result.entry_id)

  logger.info(
    "schedule_entry_due event %s fired: schedule=%s outcome=%s entry=%s",
    event.id,
    metadata.schedule_id,
    result.outcome,
    result.entry_id,
  )

  return HandlerResult(entry_ids=entry_ids)


def dispatch_preview(
  session: Session,
  body: CreateEventBlockRequest,
  metadata: ScheduleEntryDueMetadata,
) -> HandlerPreview:
  """Read-only: inspect the schedule + target period fact and describe the plan.

  Does not call `create_closing_entry` — that writes rows. We mirror the
  subset of logic needed to describe what *would* happen: load the
  schedule, find the in-scope fact for the period, report what the draft
  would look like.
  """
  from sqlalchemy import text

  from robosystems.models.extensions import Structure

  structure = session.get(Structure, metadata.schedule_id)
  if structure is None or structure.structure_type != "schedule":
    return HandlerPreview(
      would_succeed=False,
      planned_entries=[],
      computed_values={},
      validation_errors=[f"Schedule not found: {metadata.schedule_id}"],
    )

  template = (structure.metadata_ or {}).get("entry_template")
  if not template:
    return HandlerPreview(
      would_succeed=False,
      planned_entries=[],
      computed_values={},
      validation_errors=[f"Schedule '{metadata.schedule_id}' has no entry template"],
    )

  debit_element_id = template.get("debit_element_id")
  credit_element_id = template.get("credit_element_id")

  fact_row = session.execute(
    text(
      """
      SELECT value FROM facts
      WHERE structure_id = :structure_id
        AND element_id = :element_id
        AND period_start >= :period_start
        AND period_end <= :period_end
        AND fact_scope = 'in_scope'
      LIMIT 1
      """
    ),
    {
      "structure_id": metadata.schedule_id,
      "element_id": debit_element_id,
      "period_start": metadata.period_start,
      "period_end": metadata.period_end,
    },
  ).fetchone()

  if not fact_row:
    return HandlerPreview(
      would_succeed=True,
      planned_entries=[],
      computed_values={
        "outcome": "skipped",
        "reason": "No in-scope fact for this period — nothing to draft.",
      },
      validation_errors=[],
    )

  amount_dollars = fact_row.value
  amount_cents = round(amount_dollars * 100)
  memo_template = template.get("memo_template", "") or ""
  entry_memo = metadata.memo or memo_template.replace(
    "{structure_name}", structure.name
  )

  return HandlerPreview(
    would_succeed=True,
    planned_entries=[
      {
        "posting_date": str(metadata.posting_date),
        "memo": entry_memo,
        "entry_type": template.get("entry_type", "closing"),
        "line_items": [
          {
            "element_id": debit_element_id,
            "debit_amount": amount_cents,
            "credit_amount": 0,
          },
          {
            "element_id": credit_element_id,
            "debit_amount": 0,
            "credit_amount": amount_cents,
          },
        ],
      }
    ],
    computed_values={
      "amount_cents": amount_cents,
      "amount_dollars": amount_dollars,
      "debit_element_id": debit_element_id,
      "credit_element_id": credit_element_id,
    },
    validation_errors=[],
  )


SCHEDULE_ENTRY_DUE_HANDLER = EventBlockPythonHandler(
  event_type="schedule_entry_due",
  display_name="Schedule Entry Due",
  metadata_schema=ScheduleEntryDueMetadata,
  target_status="classified",
  dispatch=dispatch,
  dispatch_preview=dispatch_preview,
)
