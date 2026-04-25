"""Schedule created event handler — capture-only originator for materialized obligations.

Fires when create-event-block runs with event_type='schedule_created'.
Holds no GL-write side effects; its sole job is to be the row that
materialized `schedule_entry_due` events point at via
`obligated_by_event_id`. The event chain becomes:

    schedule_created (committed, captured-only)
        ▲
        │ obligated_by_event_id
        │
    schedule_entry_due (pending) x N periods
        │ on period boundary, sensor flips → classified
        │ handler drafts the closing entry, links via triggered_by_event_id
        ▼
    Entry (draft → posted at close-period)

When Stream 4 introduces domain-specific originators
(``asset_acquired`` / ``prepaid_recorded`` / ``contract_signed``),
``schedule_created`` events themselves get an
``obligated_by_event_id`` pointing at the upstream domain event so
the disposal cascade and AR/AP queries traverse uniformly.

ScheduleService.create_schedule emits this event directly inside its
own unit of work — it is NOT created via create-event-block by
external callers. Registering it here is what gives the row a known
event_type, a metadata schema for validation, and a place for future
agents to dry-run via preview.
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
  """No-op dispatch.

  The materialization side-effect (inserting N pending schedule_entry_due
  rows) is performed by ScheduleService.create_schedule, which emits this
  event itself. When/if external callers ever fire a schedule_created event
  via create-event-block, the only meaningful side-effect is the event row
  itself — the schedule + facts are already in place.
  """
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
