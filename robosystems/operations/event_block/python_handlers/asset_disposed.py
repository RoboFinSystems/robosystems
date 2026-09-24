"""asset_disposed handler: atomically voids the schedule's pending
obligations, drops its SumEquals rule, and posts the disposal entry.

The obligation register is the source of truth for what is still due, so
disposal voids pending obligations rather than deleting facts.
"""

from __future__ import annotations

from pydantic import BaseModel, Field
from sqlalchemy import delete, text, update
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.extensions import Rule, Structure
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
    description=(
      "Free-text disposal reason. Stored on the event row's narrative as "
      "informational context; does not drive any side effect on the schedule."
    ),
  )


def _void_pending_obligations_for_schedule(
  session: Session,
  *,
  structure_id: str,
  disposal_event_id: str,
) -> int:
  """Void a disposed schedule's `pending` obligations; 0 if the structure is
  missing."""
  structure = session.get(Structure, structure_id)
  if structure is None:
    return 0
  return ScheduleService().void_pending_obligations(
    session,
    structure=structure,
    void_reason="asset_disposed",
    voided_by_event_id=disposal_event_id,
  )


def _delete_sum_equals_rule(session: Session, structure_id: str) -> None:
  """Delete the schedule's SumEquals rule (unsatisfiable once obligations are
  voided) and its verification_results rows, which don't cascade."""
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
  """Execute the disposal; the caller commits."""
  if event.occurred_at is None:
    raise ValueError("asset_disposed event requires occurred_at")
  disposal_date = event.occurred_at.date()

  plan = compute_disposal_plan(
    session,
    structure_id=metadata.schedule_id,
    disposal_date=disposal_date,
    sale_proceeds=metadata.proceeds,
    proceeds_element_id=metadata.proceeds_element_id,
    gain_loss_element_id=metadata.gain_loss_element_id,
  )

  voided_count = _void_pending_obligations_for_schedule(
    session,
    structure_id=metadata.schedule_id,
    disposal_event_id=event.id,
  )

  _delete_sum_equals_rule(session, metadata.schedule_id)

  service = ScheduleService()
  memo = metadata.memo or f"Asset disposal for schedule {metadata.schedule_id}"
  entry_result = service.create_manual_closing_entry(
    session,
    posting_date=disposal_date,
    line_items=plan.line_items,
    memo=memo,
    created_by=created_by,
    entry_type="closing",
    provenance="event_handler",
  )

  session.execute(
    update(Entry)
    .where(Entry.id == entry_result.entry_id)
    .values(triggered_by_event_id=event.id)
  )

  logger.info(
    "asset_disposed event %s fired: schedule=%s entry=%s nbv=%s gain_loss=%s "
    "voided_obligations=%s",
    event.id,
    metadata.schedule_id,
    entry_result.entry_id,
    plan.nbv,
    plan.gain_loss,
    voided_count,
  )

  return HandlerResult(entry_ids=[entry_result.entry_id])


def dispatch_preview(
  session: Session,
  body: CreateEventBlockRequest,
  metadata: AssetDisposedMetadata,
) -> HandlerPreview:
  """The plan ``dispatch`` would execute, behind the same validation gates."""
  from robosystems.operations.locking import RowLockedError
  from robosystems.operations.roboledger.commands._guards import (
    ClosedPeriodError,
    assert_period_not_closed,
  )

  try:
    plan = compute_disposal_plan(
      session,
      structure_id=metadata.schedule_id,
      disposal_date=body.occurred_at.date(),
      sale_proceeds=metadata.proceeds,
      proceeds_element_id=metadata.proceeds_element_id,
      gain_loss_element_id=metadata.gain_loss_element_id,
    )
    # dispatch posts on the disposal date and refuses a closed period.
    assert_period_not_closed(session, body.occurred_at.date())
  except (ValueError, ClosedPeriodError, RowLockedError, ScheduleNotFoundError) as e:
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
