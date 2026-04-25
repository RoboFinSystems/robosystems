"""Asset disposal event handler.

Fires when create-event-block runs with event_type='asset_disposed' and
apply_handlers=True. Atomically:

1. Computes the disposal plan (NBV, gain/loss, line items) from the schedule's
   existing facts.
2. Truncates forward facts past the disposal date via ScheduleService.
3. Deletes the schedule's SumEquals rule (it's no longer satisfiable after
   the forward facts are gone) plus any verification_results rows that
   reference it.
4. Posts a balanced 4-leg disposal entry via ScheduleService.create_manual_closing_entry.
5. Links the resulting Entry to the event via triggered_by_event_id.

Event status after success: 'fulfilled' (disposal is terminal — no further work).

All writes happen in one session. If any step raises, the outer transaction
rolls back — nothing persists, no half-disposed state.
"""

from __future__ import annotations

from pydantic import BaseModel, Field
from sqlalchemy import delete, text, update
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.extensions import Rule
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.roboledger.schedules.service import ScheduleService

from ._disposal_plan import ScheduleNotFoundError, compute_disposal_plan
from .types import (
  EventBlockPythonHandler,
  HandlerPreview,
  HandlerResult,
)


class AssetDisposedMetadata(BaseModel):
  """Metadata for an asset_disposed event."""

  schedule_id: str = Field(
    ..., description="The depreciation schedule being disposed (struct_ prefix)"
  )
  proceeds: int = Field(
    0,
    ge=0,
    description="Sale proceeds in cents. Use 0 for abandonment / full write-off.",
  )
  proceeds_element_id: str | None = Field(
    None, description="Cash/AR element. Required when proceeds > 0."
  )
  gain_loss_element_id: str | None = Field(
    None,
    description=(
      "Gain/Loss on Disposal element. Required when NBV > 0 and "
      "proceeds differ from NBV."
    ),
  )
  memo: str | None = Field(None, description="Closing entry memo (optional)")
  reason: str = Field(
    "asset_disposed_event",
    description="Truncation reason recorded on the schedule's audit log",
  )


def _delete_sum_equals_rule(session: Session, structure_id: str) -> None:
  """Delete the schedule's SumEquals rule + its verification_results rows.

  The auto-generated SumEquals rule (sum of periodic amounts == original cost)
  is no longer satisfiable once we truncate forward facts, so it must be
  removed atomically with the disposal.

  Raw SQL for the verification_results DELETE because FactSet/VerificationResult
  FK chains don't cascade through rules.id by default.
  """
  session.execute(
    text(
      "DELETE FROM verification_results WHERE rule_id IN ("
      "  SELECT id FROM rules"
      "  WHERE target_structure_id = :sid"
      "  AND rule_pattern = 'SumEquals'"
      "  AND rule_origin = 'native'"
      ")"
    ),
    {"sid": structure_id},
  )
  session.execute(
    delete(Rule).where(
      Rule.target_structure_id == structure_id,
      Rule.rule_pattern == "SumEquals",
      Rule.rule_origin == "native",
    )
  )


def dispatch(
  session: Session,
  event: Event,
  metadata: AssetDisposedMetadata,
  created_by: str,
) -> HandlerResult:
  """Execute the disposal atomically.

  The event row has already been inserted by the caller with
  status=target_status (='fulfilled'). We flush GL rows; the caller commits.
  """
  if event.occurred_at is None:
    raise ValueError("asset_disposed event requires occurred_at")
  disposal_date = event.occurred_at.date()

  # 1. Compute the disposal plan (read-only — no writes yet)
  plan = compute_disposal_plan(
    session,
    structure_id=metadata.schedule_id,
    disposal_date=disposal_date,
    sale_proceeds=metadata.proceeds,
    proceeds_element_id=metadata.proceeds_element_id,
    gain_loss_element_id=metadata.gain_loss_element_id,
  )

  service = ScheduleService()

  # 2. Truncate forward facts + delete stale drafts
  service.truncate_schedule(
    session,
    structure_id=metadata.schedule_id,
    new_end_date=disposal_date,
    reason=metadata.reason,
    updated_by=created_by,
  )

  # 3. Delete the now-invalid SumEquals rule
  _delete_sum_equals_rule(session, metadata.schedule_id)

  # 4. Post the disposal entry
  memo = metadata.memo or f"Asset disposal for schedule {metadata.schedule_id}"
  entry_result = service.create_manual_closing_entry(
    session,
    posting_date=disposal_date,
    line_items=plan.line_items,
    memo=memo,
    created_by=created_by,
    entry_type="closing",
  )

  # 5. Link the entry to the event (audit chain)
  session.execute(
    update(Entry)
    .where(Entry.id == entry_result.entry_id)
    .values(triggered_by_event_id=event.id)
  )

  logger.info(
    "asset_disposed event %s fired: schedule=%s entry=%s nbv=%s gain_loss=%s",
    event.id,
    metadata.schedule_id,
    entry_result.entry_id,
    plan.nbv,
    plan.gain_loss,
  )

  return HandlerResult(entry_ids=[entry_result.entry_id])


def dispatch_preview(
  session: Session,
  body: CreateEventBlockRequest,
  metadata: AssetDisposedMetadata,
) -> HandlerPreview:
  """Read + compute without writing. Returns the plan the handler would execute."""
  try:
    plan = compute_disposal_plan(
      session,
      structure_id=metadata.schedule_id,
      disposal_date=body.occurred_at.date(),
      sale_proceeds=metadata.proceeds,
      proceeds_element_id=metadata.proceeds_element_id,
      gain_loss_element_id=metadata.gain_loss_element_id,
    )
  except (ValueError, ScheduleNotFoundError) as e:
    return HandlerPreview(
      would_succeed=False,
      planned_entries=[],
      computed_values={},
      validation_errors=[str(e)],
    )

  memo = metadata.memo or f"Asset disposal for schedule {metadata.schedule_id}"
  return HandlerPreview(
    would_succeed=True,
    planned_entries=[
      {
        "posting_date": str(body.occurred_at.date()),
        "memo": memo,
        "entry_type": "closing",
        "line_items": plan.line_items,
      }
    ],
    computed_values={
      "original_amount_cents": plan.original_amount,
      "accumulated_depreciation_cents": plan.accumulated_depreciation,
      "nbv_cents": plan.nbv,
      "sale_proceeds_cents": plan.sale_proceeds,
      "gain_loss_cents": plan.gain_loss,
    },
    validation_errors=[],
  )


ASSET_DISPOSED_HANDLER = EventBlockPythonHandler(
  event_type="asset_disposed",
  display_name="Asset Disposal",
  metadata_schema=AssetDisposedMetadata,
  target_status="fulfilled",
  dispatch=dispatch,
  dispatch_preview=dispatch_preview,
)
