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
  ScheduleCreatedResponse,
  UpdateScheduleRequest,
)
from robosystems.models.extensions import (
  Association,
  AssociationClassification,
  Element,
  FactSet,
  Rule,
  Structure,
  VerificationResult,
)
from robosystems.models.extensions.roboledger import Fact
from robosystems.operations.information_block.rules.engine import (
  evaluate_rules_for_structure,
)
from robosystems.operations.roboledger.commands._guards import (
  rule_summary as _rule_summary,
)
from robosystems.operations.roboledger.schedules import ScheduleService
from robosystems.operations.roboledger.schedules.service import (
  EntryTemplate,
  ScheduleMetadata,
)


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


def _validate_element_references(session: Session, body: CreateScheduleRequest) -> None:
  """Check that every element id on the request actually exists.

  Without this, a typo in ``entry_template.debit_element_id`` silently
  succeeds and writes facts pointing at a phantom Element row — the
  auto-rule generator then skips (qname lookup fails) leaving a
  corrupted schedule with no rules and no error surfaced. Validate
  up-front and fail with a clear 422 instead.
  """
  referenced: set[str] = set(body.element_ids)
  referenced.add(body.entry_template.debit_element_id)
  referenced.add(body.entry_template.credit_element_id)
  if body.schedule_metadata and body.schedule_metadata.asset_element_id:
    referenced.add(body.schedule_metadata.asset_element_id)

  existing = set(
    session.execute(select(Element.id).where(Element.id.in_(referenced))).scalars()
  )
  missing = referenced - existing
  if missing:
    raise ValueError(
      f"Element(s) not found: {sorted(missing)}. "
      f"Check that the element ids exist in this graph's taxonomy."
    )

  template_refs = {
    body.entry_template.debit_element_id,
    body.entry_template.credit_element_id,
  }
  undeclared = template_refs - set(body.element_ids)
  if undeclared:
    raise ValueError(
      f"entry_template element(s) not declared in element_ids: "
      f"{sorted(undeclared)}. Add them to element_ids."
    )


def create_schedule(
  session: Session,
  body: CreateScheduleRequest,
  created_by: str,
) -> ScheduleCreatedResponse:
  """Create a schedule with pre-generated facts for each period.

  Raises `ValueError` for validation failures — caller maps to 422.
  """
  _validate_element_references(session, body)
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
      periodic_amounts=body.schedule_metadata.periodic_amounts,
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

  metadata = structure.metadata_ or {}
  return ScheduleCreatedResponse(
    structure_id=structure.id,
    name=structure.name,
    taxonomy_id=structure.taxonomy_id,
    total_periods=period_row.cnt if period_row else 0,
    total_facts=count_row.cnt if count_row else 0,
    rule_summary=_rule_summary(rule_results),
    schedule_created_event_id=metadata.get("schedule_created_event_id"),
    pending_event_count=metadata.get("pending_event_count", 0),
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
  session: Session,
  body: UpdateScheduleRequest,
  updated_by: str = "system",
) -> ScheduleCreatedResponse:
  """Update mutable fields on a schedule.

  Editable: `name`, `entry_template`, `schedule_metadata`. These live
  on the Structure row and its `metadata_` JSONB column.

  Period range and monthly amount are NOT editable — they define the
  fact grid. Fire an event block that terminates the schedule (e.g.,
  `asset_disposed`) and create a fresh schedule via `create-schedule`.

  When the entry template changes, all remaining `pending`
  schedule_entry_due obligations are voided and replaced with a fresh
  set linked via `replaces_event_id` / `replaced_by_event_id` (Stream
  2.E supersession). Already-classified / fulfilled obligations are
  untouched — the new template applies prospectively.

  Raises `ScheduleNotFoundError` if the schedule does not exist.
  """
  structure = _load_schedule_or_404(session, body.structure_id)

  if body.name is not None:
    structure.name = body.name

  existing_template = (
    dict(structure.metadata_["entry_template"])
    if structure.metadata_ and structure.metadata_.get("entry_template")
    else {}
  )
  metadata = dict(structure.metadata_) if structure.metadata_ else {}
  template_changed = False

  if body.entry_template is not None:
    new_template = {
      "debit_element_id": body.entry_template.debit_element_id,
      "credit_element_id": body.entry_template.credit_element_id,
      "entry_type": body.entry_template.entry_type,
      "memo_template": body.entry_template.memo_template,
      "auto_reverse": body.entry_template.auto_reverse,
    }
    template_changed = new_template != existing_template
    metadata["entry_template"] = new_template

  if body.schedule_metadata is not None:
    metadata["schedule_metadata"] = {
      "method": body.schedule_metadata.method,
      "original_amount": body.schedule_metadata.original_amount,
      "residual_value": body.schedule_metadata.residual_value,
      "useful_life_months": body.schedule_metadata.useful_life_months,
      "asset_element_id": body.schedule_metadata.asset_element_id,
      "periodic_amounts": body.schedule_metadata.periodic_amounts,
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

  # Stream 2.E: void + re-materialize the pending obligation chain when
  # the entry template changed. Same transaction as the metadata update
  # so a partial state is impossible — either the new template + new
  # pending events both land, or neither does.
  if template_changed:
    ScheduleService().supersede_pending_obligations(
      session,
      structure=structure,
      created_by=updated_by,
    )

  session.commit()

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


# Asset disposal was retired as a standalone operation in Phase 4b of the
# event-driven ledger. The atomic truncate + SumEquals cleanup + balanced
# disposal entry logic now lives in the Python handler registry at
# operations/event_block/python_handlers/asset_disposed.py
# and is invoked via create-event-block(event_type='asset_disposed').


def delete_schedule(session: Session, body: DeleteScheduleRequest) -> dict:
  """Delete a schedule — cascades through facts and associations.

  Deletion order respects FK constraints:
  1. Pending obligation events (Stream 2.E: void before parent disappears)
  2. Verification results (referencing rules / structure_id)
  3. Facts and FactSets (referencing structure_id)
  4. Rules and association classifications (referencing associations)
  5. Associations (referencing structure_id)
  6. Structure row itself

  Raises `ScheduleNotFoundError` if the schedule does not exist.
  """
  structure = _load_schedule_or_404(session, body.structure_id)
  # Void any pending obligations first so they can't outlive their
  # `schedule_created` originator and trip the close-period gate
  # after the schedule is gone.
  ScheduleService().void_pending_obligations(
    session,
    structure=structure,
    void_reason="schedule_deleted",
  )
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
  session.commit()

  return {"deleted": True}
