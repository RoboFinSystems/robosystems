"""asset_disposed handler: atomically retires the schedule's months after
the disposal (their drafts deleted, their obligations voided), drops its
SumEquals rule, and posts the disposal entry.

The obligation register is the source of truth for what is still due, so
disposal voids obligations rather than deleting facts.
"""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field
from sqlalchemy import delete, text, update
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.extensions import Rule, Structure
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.roboledger.entity_scope import owner_entity_id
from robosystems.operations.roboledger.schedules.service import ScheduleService

from ._disposal_plan import (
  ScheduleNotFoundError,
  compute_disposal_plan,
  disposal_cutoff,
)
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
      "Gain/Loss on Disposal element. Required whenever proceeds differ "
      "from the net book value."
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


def _end_schedule_at(
  session: Session,
  *,
  structure_id: str,
  cutoff: date,
  disposal_event_id: str,
) -> int:
  """Retire every month after ``cutoff``: delete its drafts and void its
  obligations, `classified` included (a backdated disposal finds them already
  drafted). Months up to the cutoff stay, since the plan counts their
  depreciation as booked. Returns the obligations voided."""
  from robosystems.operations.locking import bounded_lock_wait

  structure = session.get(Structure, structure_id)
  if structure is None:
    return 0
  service = ScheduleService()
  action = "dispose of the asset"
  service.fence_drafts_after(session, structure=structure, cutoff=cutoff, action=action)
  # Void before deleting: a promotion sweep drafting a later month waits on
  # the voided rows and then finds nothing to promote, where deleting first
  # would let its draft land after the delete. Bounded: the sweep holds them.
  with bounded_lock_wait(
    session,
    "This schedule's obligations are being written by another process. "
    "Retry in a moment.",
  ):
    voided = service.void_pending_obligations(
      session,
      structure=structure,
      void_reason="asset_disposed",
      voided_by_event_id=disposal_event_id,
      period_start_after=cutoff,
      include_classified=True,
    )
  service.delete_drafts_after(
    session, structure=structure, cutoff=cutoff, action=action, fenced=True
  )
  return voided


def _schedule_entity_id(session: Session, structure_id: str) -> str:
  """The entity whose books a disposal posts in: the schedule's own."""
  return owner_entity_id(session, session.get(Structure, structure_id))


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

  voided_count = _end_schedule_at(
    session,
    structure_id=metadata.schedule_id,
    cutoff=disposal_cutoff(disposal_date),
    disposal_event_id=event.id,
  )

  _delete_sum_equals_rule(session, metadata.schedule_id)

  # The disposal is the schedule's, whichever entity the event was filed under.
  entity_id = _schedule_entity_id(session, metadata.schedule_id)
  event.entity_id = entity_id

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
    entity_id=entity_id,
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
  entity_id: str | None = None,
) -> HandlerPreview:
  """The plan ``dispatch`` would execute, behind the same validation gates.
  The schedule decides the entity, as it does in ``dispatch``."""
  from robosystems.operations.locking import RowLockedError
  from robosystems.operations.roboledger.commands._guards import (
    ClosedPeriodError,
    assert_accounts_postable,
    assert_period_not_closed,
    assert_within_calendar,
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
    # dispatch posts on the disposal date and refuses a closed period, or
    # one before the calendar.
    disposal_entity_id = _schedule_entity_id(session, metadata.schedule_id)
    assert_period_not_closed(
      session, body.occurred_at.date(), entity_id=disposal_entity_id
    )
    assert_within_calendar(
      session, body.occurred_at.date(), entity_id=disposal_entity_id
    )
    assert_accounts_postable(
      session,
      (li["element_id"] for li in plan.line_items),
      entity_id=disposal_entity_id,
    )
    # dispatch refuses to retire a month whose entry already posted.
    cutoff = disposal_cutoff(body.occurred_at.date())
    landed = ScheduleService().count_landed_entries_after(
      session, metadata.schedule_id, cutoff
    )
    if landed:
      raise ValueError(
        f"Cannot dispose of the asset: {landed} posted or shadowed entries "
        f"exist for periods after {cutoff}; reverse them or dispose on a "
        "later date."
      )
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
