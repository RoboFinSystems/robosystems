"""schedule_created handler: capture-only. The event is the row a schedule's
materialized `schedule_entry_due` obligations point at via
`obligated_by_event_id`; ScheduleService.create_schedule emits it itself.
"""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.extensions.roboledger.event import Event

from .types import (
  EventBlockPythonHandler,
  HandlerPreview,
  HandlerResult,
)


class ScheduleCreatedMetadata(BaseModel):
  """Metadata for a schedule_created event."""

  schedule_id: str = Field(
    ..., description="The schedule structure id (struct_ prefix)."
  )
  taxonomy_id: str = Field(
    ..., description="Taxonomy that owns the schedule structure."
  )
  period_start: date = Field(..., description="First period start date.")
  period_end: date = Field(..., description="Last period end date.")
  monthly_amount: int = Field(
    ...,
    description="Monthly amount in cents (informational; per-period values may vary).",
  )
  pending_event_count: int = Field(
    ...,
    ge=0,
    description=(
      "Number of pending schedule_entry_due events materialized from this "
      "schedule. Cached on the originating event for cheap counterparty queries."
    ),
  )


def dispatch(
  session: Session,
  event: Event,
  metadata: ScheduleCreatedMetadata,
  created_by: str,
) -> HandlerResult:
  """No-op: ScheduleService.create_schedule materializes the obligations."""
  return HandlerResult()


def dispatch_preview(
  session: Session,
  body: CreateEventBlockRequest,
  metadata: ScheduleCreatedMetadata,
) -> HandlerPreview:
  """Capture-only preview: the event row is the only artifact."""
  return HandlerPreview(
    would_succeed=True,
    planned_entries=[],
    computed_values={
      "schedule_id": metadata.schedule_id,
      "pending_event_count": metadata.pending_event_count,
    },
    validation_errors=[],
  )


SCHEDULE_CREATED_HANDLER = EventBlockPythonHandler(
  event_type="schedule_created",
  display_name="Schedule Created",
  metadata_schema=ScheduleCreatedMetadata,
  target_status="committed",
  dispatch=dispatch,
  dispatch_preview=dispatch_preview,
)
