"""Write operations for schedules and closing entries.

Thin wrappers over `ScheduleService`. The service does the heavy lifting
(fact generation, entry creation, balance validation) — these functions
translate request bodies to service calls and assemble responses.
"""

from __future__ import annotations

from sqlalchemy import or_, select, text
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.schedules import (
  ClosingEntryResponse,
  CreateClosingEntryOperation,
  CreateManualClosingEntryRequest,
  CreateScheduleRequest,
  DeleteScheduleRequest,
  DisposeScheduleRequest,
  DisposeScheduleResponse,
  ScheduleCreatedResponse,
  TruncateScheduleOperation,
  TruncateScheduleResponse,
  UpdateScheduleRequest,
)
from robosystems.models.extensions import (
  Association,
  AssociationClassification,
  FactSet,
  Rule,
  Structure,
  VerificationResult,
)
from robosystems.models.extensions.roboledger import Fact
from robosystems.operations.information_block.rules.engine import (
  evaluate_rules_for_structure,
)
from robosystems.operations.roboledger.schedules import ScheduleService
from robosystems.operations.roboledger.schedules.service import (
  EntryTemplate,
  ScheduleMetadata,
)


def _rule_summary(results: list) -> dict[str, int] | None:
  """Tally verification results by status. Returns None when no rules exist."""
  if not results:
    return None
  tally: dict[str, int] = {"pass": 0, "fail": 0, "error": 0, "skipped": 0}
  for r in results:
    tally[r.status] = tally.get(r.status, 0) + 1
  return tally


class ScheduleNotFoundError(LookupError):
  """Raised when a schedule structure is not found by id."""

  def __init__(self, structure_id: str) -> None:
    super().__init__(f"Schedule not found: {structure_id}")
    self.structure_id = structure_id


def _build_closing_entry_response(result) -> ClosingEntryResponse:
  """Map a ScheduleService ClosingEntryResult to the wire response."""
  reversal_resp = None
  if result.reversal:
    reversal_resp = ClosingEntryResponse(
      outcome=result.reversal.outcome,
      entry_id=result.reversal.entry_id,
      status=result.reversal.status,
      posting_date=result.reversal.posting_date,
      memo=result.reversal.memo,
      debit_element_id=result.reversal.debit_element_id,
      credit_element_id=result.reversal.credit_element_id,
      amount=result.reversal.amount,
      reason=result.reversal.reason,
    )
  return ClosingEntryResponse(
    outcome=result.outcome,
    entry_id=result.entry_id,
    status=result.status,
    posting_date=result.posting_date,
    memo=result.memo,
    debit_element_id=result.debit_element_id,
    credit_element_id=result.credit_element_id,
    amount=result.amount,
    reason=result.reason,
    reversal=reversal_resp,
  )


def create_schedule(
  session: Session,
  body: CreateScheduleRequest,
  created_by: str,
) -> ScheduleCreatedResponse:
  """Create a schedule with pre-generated facts for each period.

  Raises `ValueError` for validation failures — caller maps to 422.
  """
  service = ScheduleService()
  et = EntryTemplate(
    debit_element_id=body.entry_template.debit_element_id,
    credit_element_id=body.entry_template.credit_element_id,
    entry_type=body.entry_template.entry_type,
    memo_template=body.entry_template.memo_template,
    auto_reverse=body.entry_template.auto_reverse,
  )

  sm = None
  if body.schedule_metadata:
    sm = ScheduleMetadata(
      method=body.schedule_metadata.method,
      original_amount=body.schedule_metadata.original_amount,
      residual_value=body.schedule_metadata.residual_value,
      useful_life_months=body.schedule_metadata.useful_life_months,
      asset_element_id=body.schedule_metadata.asset_element_id,
    )

  structure = service.create_schedule(
    session,
    name=body.name,
    taxonomy_id=body.taxonomy_id,
    element_ids=body.element_ids,
    period_start=body.period_start,
    period_end=body.period_end,
    monthly_amount=body.monthly_amount,
    entry_template=et,
    schedule_metadata=sm,
    created_by=created_by,
    closed_through=body.closed_through,
    source_transaction_id=body.source_transaction_id,
  )

  # Count generated facts and distinct periods for the response.
  count_row = session.execute(
    text("SELECT COUNT(*) AS cnt FROM facts WHERE structure_id = :sid"),
    {"sid": structure.id},
  ).fetchone()
  period_row = session.execute(
    text(
      "SELECT COUNT(DISTINCT (period_start, period_end)) AS cnt "
      "FROM facts WHERE structure_id = :sid"
    ),
    {"sid": structure.id},
  ).fetchone()

  rule_results = evaluate_rules_for_structure(
    session,
    structure.id,
    period_start=body.period_start,
    period_end=body.period_end,
    created_by=created_by,
  )

  session.commit()

  return ScheduleCreatedResponse(
    structure_id=structure.id,
    name=structure.name,
    taxonomy_id=structure.taxonomy_id,
    total_periods=period_row.cnt if period_row else 0,
    total_facts=count_row.cnt if count_row else 0,
    rule_summary=_rule_summary(rule_results),
  )


def truncate_schedule(
  session: Session,
  body: TruncateScheduleOperation,
  created_by: str,
) -> TruncateScheduleResponse:
  """End a schedule early, deleting forward facts + stale drafts.

  Raises `ValueError` for validation failures — caller maps to 422.
  """
  service = ScheduleService()
  result = service.truncate_schedule(
    session,
    structure_id=body.structure_id,
    new_end_date=body.new_end_date,
    reason=body.reason,
    updated_by=created_by,
  )
  session.commit()

  return TruncateScheduleResponse(
    structure_id=result["structure_id"],
    new_end_date=result["new_end_date"],
    facts_deleted=result["facts_deleted"],
    reason=result["reason"],
  )


def create_closing_entry(
  session: Session,
  body: CreateClosingEntryOperation,
  created_by: str,
) -> ClosingEntryResponse:
  """Create a draft closing entry from a schedule's facts for a period.

  Raises `ValueError` for validation failures — caller maps to 422.
  """
  service = ScheduleService()
  result = service.create_closing_entry(
    session,
    structure_id=body.structure_id,
    posting_date=body.posting_date,
    period_start=body.period_start,
    period_end=body.period_end,
    created_by=created_by,
    memo=body.memo,
  )
  session.commit()
  return _build_closing_entry_response(result)


def create_manual_closing_entry(
  session: Session,
  body: CreateManualClosingEntryRequest,
  created_by: str,
) -> ClosingEntryResponse:
  """Create a manual (non-schedule) draft closing entry.

  Used for one-off adjustments (asset disposals, impairments,
  reclassifications). Total debits must equal total credits.
  Raises `ValueError` for validation failures — caller maps to 422.
  """
  service = ScheduleService()
  result = service.create_manual_closing_entry(
    session,
    posting_date=body.posting_date,
    line_items=[
      {
        "element_id": li.element_id,
        "debit_amount": li.debit_amount,
        "credit_amount": li.credit_amount,
        "description": li.description,
      }
      for li in body.line_items
    ],
    memo=body.memo,
    created_by=created_by,
    entry_type=body.entry_type,
  )
  session.commit()
  return _build_closing_entry_response(result)


# ─── Schedule update / delete ─────────────────────────────────────────────


def _load_schedule_or_404(session: Session, structure_id: str) -> Structure:
  """Load a schedule Structure row by id, raising ScheduleNotFoundError."""
  structure = session.execute(
    select(Structure).where(
      Structure.id == structure_id,
      Structure.structure_type == "schedule",
    )
  ).scalar_one_or_none()
  if structure is None:
    raise ScheduleNotFoundError(structure_id)
  return structure


def update_schedule(
  session: Session, body: UpdateScheduleRequest
) -> ScheduleCreatedResponse:
  """Update mutable fields on a schedule.

  Editable: `name`, `entry_template`, `schedule_metadata`. These live
  on the Structure row and its `metadata_` JSONB column.

  Period range and monthly amount are NOT editable — they define the
  fact grid. Use truncate-schedule + create-schedule for those changes.

  Raises `ScheduleNotFoundError` if the schedule does not exist.
  """
  structure = _load_schedule_or_404(session, body.structure_id)

  if body.name is not None:
    structure.name = body.name

  metadata = dict(structure.metadata_) if structure.metadata_ else {}

  if body.entry_template is not None:
    metadata["entry_template"] = {
      "debit_element_id": body.entry_template.debit_element_id,
      "credit_element_id": body.entry_template.credit_element_id,
      "entry_type": body.entry_template.entry_type,
      "memo_template": body.entry_template.memo_template,
      "auto_reverse": body.entry_template.auto_reverse,
    }

  if body.schedule_metadata is not None:
    metadata["schedule_metadata"] = {
      "method": body.schedule_metadata.method,
      "original_amount": body.schedule_metadata.original_amount,
      "residual_value": body.schedule_metadata.residual_value,
      "useful_life_months": body.schedule_metadata.useful_life_months,
      "asset_element_id": body.schedule_metadata.asset_element_id,
    }

  structure.metadata_ = metadata
  # Phase δ: keep artifact_mechanics in sync with metadata_ while both
  # live on the row. Envelope reads prefer artifact_mechanics; writes
  # stamp both during the transition window.
  # periods_with_entries is transient (queried from facts at read time) —
  # intentionally excluded rather than using ScheduleMechanics.model_dump().
  structure.artifact_mechanics = {
    "kind": "closing_entry_generator",
    "entry_template": metadata.get("entry_template", {}),
    "schedule_metadata": metadata.get("schedule_metadata"),
  }
  session.flush()

  # Recount for response (same as create_schedule response shape)
  count_row = session.execute(
    text("SELECT COUNT(*) AS cnt FROM facts WHERE structure_id = :sid"),
    {"sid": structure.id},
  ).fetchone()
  period_row = session.execute(
    text(
      "SELECT COUNT(DISTINCT (period_start, period_end)) AS cnt "
      "FROM facts WHERE structure_id = :sid"
    ),
    {"sid": structure.id},
  ).fetchone()

  return ScheduleCreatedResponse(
    structure_id=structure.id,
    name=structure.name,
    taxonomy_id=structure.taxonomy_id,
    total_periods=period_row.cnt if period_row else 0,
    total_facts=count_row.cnt if count_row else 0,
  )


def dispose_schedule(
  session: Session,
  body: DisposeScheduleRequest,
  created_by: str,
) -> DisposeScheduleResponse:
  """Atomically truncate a schedule and create the disposal closing entry.

  Computes accumulated depreciation from the schedule's own instant facts,
  derives NBV and gain/loss, truncates forward facts past disposal_date,
  then creates a balanced multi-line disposal closing entry.

  Raises `ValueError` for validation failures — caller maps to 422.
  Raises `ScheduleNotFoundError` if the schedule does not exist.
  """
  structure = _load_schedule_or_404(session, body.structure_id)
  mechanics = structure.artifact_mechanics or {}
  sm = mechanics.get("schedule_metadata") or {}
  et = mechanics.get("entry_template") or {}

  original_amount: int = int(sm.get("original_amount") or 0)
  asset_element_id: str | None = sm.get("asset_element_id")
  credit_element_id: str | None = et.get(
    "credit_element_id"
  )  # accumulated depreciation

  if not asset_element_id:
    raise ValueError(
      "dispose-schedule requires schedule_metadata.asset_element_id "
      "(the balance-sheet asset element). Update the schedule first."
    )
  if not credit_element_id:
    raise ValueError("dispose-schedule requires entry_template.credit_element_id.")

  # Accumulated depreciation = most recent cumulative instant fact up to disposal_date
  acc_row = session.execute(
    text(
      "SELECT value FROM facts "
      "WHERE structure_id = :sid AND element_id = :eid "
      "AND period_type = 'instant' AND period_end <= :d "
      "ORDER BY period_end DESC LIMIT 1"
    ),
    {"sid": structure.id, "eid": credit_element_id, "d": body.disposal_date},
  ).fetchone()
  accumulated_depreciation_dollars = float(acc_row.value) if acc_row else 0.0
  accumulated_depreciation = round(accumulated_depreciation_dollars * 100)

  round(original_amount / 100.0, 2)
  net_book_value = original_amount - accumulated_depreciation
  sale_proceeds = body.sale_proceeds or 0
  gain_loss = sale_proceeds - net_book_value

  if sale_proceeds > 0 and not body.proceeds_element_id:
    raise ValueError("proceeds_element_id is required when sale_proceeds > 0.")
  if net_book_value != 0 and gain_loss != 0 and not body.gain_loss_element_id:
    raise ValueError(
      "gain_loss_element_id is required when net book value > 0 and "
      "the disposal produces a gain or loss."
    )

  # Truncate forward facts
  service = ScheduleService()
  trunc_result = service.truncate_schedule(
    session,
    structure_id=body.structure_id,
    new_end_date=body.disposal_date,
    reason=body.reason,
    updated_by=created_by,
  )

  # Build balanced disposal line items (all amounts in cents)
  line_items: list[dict] = [
    # DR accumulated depreciation (remove the contra account)
    {
      "element_id": credit_element_id,
      "debit_amount": accumulated_depreciation,
      "credit_amount": 0,
      "description": "Remove accumulated depreciation",
    },
    # CR asset at cost
    {
      "element_id": asset_element_id,
      "debit_amount": 0,
      "credit_amount": original_amount,
      "description": "Remove asset at cost",
    },
  ]
  if sale_proceeds > 0 and body.proceeds_element_id:
    line_items.append(
      {
        "element_id": body.proceeds_element_id,
        "debit_amount": sale_proceeds,
        "credit_amount": 0,
        "description": "Sale proceeds",
      }
    )
  if gain_loss > 0 and body.gain_loss_element_id:
    line_items.append(
      {
        "element_id": body.gain_loss_element_id,
        "debit_amount": 0,
        "credit_amount": gain_loss,
        "description": "Gain on disposal",
      }
    )
  elif gain_loss < 0 and body.gain_loss_element_id:
    line_items.append(
      {
        "element_id": body.gain_loss_element_id,
        "debit_amount": abs(gain_loss),
        "credit_amount": 0,
        "description": "Loss on disposal",
      }
    )

  entry_result = service.create_manual_closing_entry(
    session,
    posting_date=body.disposal_date,
    line_items=line_items,
    memo=body.memo,
    created_by=created_by,
    entry_type="closing",
  )
  session.commit()

  return DisposeScheduleResponse(
    structure_id=body.structure_id,
    disposal_date=body.disposal_date,
    original_amount=original_amount,
    accumulated_depreciation=accumulated_depreciation,
    net_book_value=net_book_value,
    gain_loss=gain_loss,
    facts_deleted=trunc_result.get("facts_deleted", 0),
    closing_entry=_build_closing_entry_response(entry_result),
  )


def delete_schedule(session: Session, body: DeleteScheduleRequest) -> dict:
  """Delete a schedule — cascades through facts and associations.

  Deletion order respects FK constraints:
  1. Verification results (referencing rules / structure_id)
  2. Facts and FactSets (referencing structure_id)
  3. Rules and association classifications (referencing associations)
  4. Associations (referencing structure_id)
  5. Structure row itself

  Raises `ScheduleNotFoundError` if the schedule does not exist.
  """
  structure = _load_schedule_or_404(session, body.structure_id)
  association_ids = (
    session.execute(
      select(Association.id).where(Association.structure_id == structure.id)
    )
    .scalars()
    .all()
  )
  rule_filters = [Rule.target_structure_id == structure.id]
  if association_ids:
    rule_filters.append(Rule.target_association_id.in_(association_ids))
  rule_ids = session.execute(select(Rule.id).where(or_(*rule_filters))).scalars().all()

  verification_filters = [VerificationResult.structure_id == structure.id]
  if rule_ids:
    verification_filters.append(VerificationResult.rule_id.in_(rule_ids))
  session.query(VerificationResult).filter(or_(*verification_filters)).delete(
    synchronize_session=False
  )

  session.query(Fact).filter(Fact.structure_id == structure.id).delete(
    synchronize_session=False
  )
  session.query(FactSet).filter(FactSet.structure_id == structure.id).delete(
    synchronize_session=False
  )
  if rule_ids:
    session.query(Rule).filter(Rule.id.in_(rule_ids)).delete(synchronize_session=False)
  if association_ids:
    session.query(AssociationClassification).filter(
      AssociationClassification.association_id.in_(association_ids)
    ).delete(synchronize_session=False)
  session.query(Association).filter(Association.structure_id == structure.id).delete(
    synchronize_session=False
  )
  session.delete(structure)
  session.flush()

  return {"deleted": True}
