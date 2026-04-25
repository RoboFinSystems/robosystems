"""Asset disposal event handler.

Fires when create-event-block runs with event_type='asset_disposed' and
apply_handlers=True. Atomically:

1. Computes the disposal plan (NBV, gain/loss, line items) from the schedule's
   existing facts.
2. Voids all `pending` `schedule_entry_due` obligations linked to the
   schedule via its originating `schedule_created` event. The voided rows
   carry `replaced_by_event_id` pointing at the disposal event so the audit
   chain stays queryable. Facts stay in place as a historical record (the
   GL has the disposal entry that nets the asset gone).
3. Deletes the schedule's SumEquals rule (it's no longer satisfiable after
   the obligations are voided) plus any verification_results rows that
   reference it.
4. Posts a balanced disposal entry via ScheduleService.create_manual_closing_entry.
5. Links the resulting Entry to the event via triggered_by_event_id.

Event status after success: 'fulfilled' (disposal is terminal — no further work).

All writes happen in one session. If any step raises, the outer transaction
rolls back — nothing persists, no half-disposed state.

Stream 2.C migration note: prior versions called `ScheduleService.truncate_schedule`
to hard-delete forward facts. That path is gone — the obligation register is
now the single source of truth for "what's still due", and voiding events is
how a disposal terminates a schedule's remaining lifespan.
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
      "Free-text disposal reason. Stored on the event row's narrative; "
      "purely informational since Stream 2.C — no longer drives schedule "
      "truncation."
    ),
  )


def _void_pending_obligations_for_schedule(
  session: Session,
  *,
  structure_id: str,
  disposal_event_id: str,
) -> int:
  """Void all `pending` schedule_entry_due events for a disposed schedule.

  Thin wrapper that loads the structure and delegates to the shared
  ``ScheduleService.void_pending_obligations``. Each voided row
  carries ``replaced_by_event_id=disposal_event_id`` so the audit
  chain answers "what voided this obligation?". Facts stay in place
  — the GL's disposal entry is the authoritative end-state.

  Returns 0 (no-op) when the structure is missing or has no
  ``schedule_created_event_id`` — covers schedules created before
  Stream 2.A and any test fixtures that build Structure rows directly.
  """
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

  # 2. Void any remaining `pending` obligations on this schedule.
  voided_count = _void_pending_obligations_for_schedule(
    session,
    structure_id=metadata.schedule_id,
    disposal_event_id=event.id,
  )

  # 3. Delete the now-invalid SumEquals rule
  _delete_sum_equals_rule(session, metadata.schedule_id)

  # 4. Post the disposal entry
  service = ScheduleService()
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
