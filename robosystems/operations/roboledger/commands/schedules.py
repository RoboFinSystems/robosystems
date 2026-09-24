"""Schedule write operations: request bodies to ScheduleService calls."""

from __future__ import annotations

from datetime import UTC, date, datetime

from sqlalchemy import or_, select, text
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from robosystems.models.api.extensions.schedules import (
  CreateScheduleRequest,
  DeleteScheduleRequest,
  PromoteObligationsRequest,
  PromoteObligationsResponse,
  RebuildScheduleRequest,
  ScheduleCreatedResponse,
  TerminateScheduleRequest,
  TerminateScheduleResponse,
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
from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.information_block.rules.engine import (
  evaluate_rules_for_structure,
)
from robosystems.operations.roboledger.commands._guards import (
  rule_summary as _rule_summary,
)
from robosystems.operations.roboledger.entry_status import (
  landed_entry_bindparam,
)
from robosystems.operations.roboledger.schedules import ScheduleService
from robosystems.operations.roboledger.schedules.service import (
  EntryTemplate,
  ScheduleMetadata,
)


class ScheduleNotFoundError(LookupError):
  def __init__(self, structure_id: str) -> None:
    super().__init__(f"Schedule not found: {structure_id}")
    self.structure_id = structure_id


def _calendar_closed_through_date(session: Session):
  """End date of the calendar's `closed_through_period`; None when there is
  no calendar or nothing is closed."""
  from robosystems.models.extensions.roboledger.fiscal_calendar import (
    FiscalCalendar,
  )
  from robosystems.operations.roboledger.fiscal_calendar import (
    period_date_range,
  )

  cal = session.query(FiscalCalendar).first()
  if cal is None or not cal.closed_through_period:
    return None
  _, period_end = period_date_range(str(cal.closed_through_period))
  return period_end


def reinstate_reopened_schedule_scopes(session: Session) -> int:
  """Re-stamp ``historical`` schedule facts after ``closed_through`` back to
  ``in_scope`` once reopen-period has moved the boundary back.

  Scope is stamped only at generation, so without this a reopened month drops
  out of the roll-forward and the re-close skips it. Idempotent; returns the
  number of facts re-stamped.
  """
  from sqlalchemy import text

  closed_through = _calendar_closed_through_date(session)
  result = session.execute(
    text(
      """
      UPDATE facts
      SET fact_scope = 'in_scope'
      WHERE fact_scope = 'historical'
        AND structure_id IN (
          SELECT id FROM structures WHERE block_type = 'schedule'
        )
        AND (:closed_through IS NULL OR period_end > :closed_through)
      """
    ),
    {"closed_through": closed_through},
  )
  return result.rowcount or 0


def _validate_element_references(session: Session, body: CreateScheduleRequest) -> None:
  """Fail fast on element ids that don't exist or template accounts missing
  from ``element_ids``; otherwise facts land on phantom elements."""
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
  """Raises `ValueError` for validation failures (mapped to 422)."""
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

  # Default to the calendar's boundary: pending obligations inside an
  # already-closed range would block close-period forever.
  effective_closed_through = body.closed_through
  if effective_closed_through is None:
    effective_closed_through = _calendar_closed_through_date(session)

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
    closed_through=effective_closed_through,
    source_transaction_id=body.source_transaction_id,
  )

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

  # Read before commit: a post-commit refresh can run on a pooled connection
  # whose search_path was reset to `public`, resolving the wrong schema.
  metadata = structure.metadata_ or {}
  structure_id = structure.id
  structure_name = structure.name
  structure_taxonomy_id = structure.taxonomy_id

  session.commit()

  return ScheduleCreatedResponse(
    structure_id=structure_id,
    name=structure_name,
    taxonomy_id=structure_taxonomy_id,
    total_periods=period_row.cnt if period_row else 0,
    total_facts=count_row.cnt if count_row else 0,
    rule_summary=_rule_summary(rule_results),
    schedule_created_event_id=metadata.get("schedule_created_event_id"),
    pending_event_count=metadata.get("pending_event_count", 0),
  )


def promote_obligations(
  session: Session,
  body: PromoteObligationsRequest,
  created_by: str,
) -> PromoteObligationsResponse:
  """Run the `scheduled_obligation_promoter` sensor's sweep on demand.

  Classifies matured pending obligations and, with ``dispatch_handlers``,
  drafts their closing entries, including stranded classified-but-undrafted
  ones. Idempotent.
  """
  from robosystems.operations.event_block.promotion import promote_pending_obligations
  from robosystems.operations.locking import bounded_lock_wait

  # Bounded: request-facing, and the sensor runs the same sweep every few
  # minutes. Wraps the whole sweep because the lock is taken inside it.
  with bounded_lock_wait(
    session,
    "Obligations for this graph are being written by another process. "
    "Retry in a moment.",
  ):
    result = promote_pending_obligations(
      session,
      graph_id="(on-demand)",  # logging-only; data scope is the session search_path
      as_of=datetime.now(UTC),
      dispatch_handlers=body.dispatch_handlers,
      created_by=created_by,
    )
  session.commit()
  return PromoteObligationsResponse(
    classified_count=result.classified_count,
    dispatched_count=result.dispatched_count,
    error_count=result.error_count,
    stranded_count=result.stranded_count,
    classified_event_ids=result.classified_event_ids,
    stranded_event_ids=result.stranded_event_ids,
    errors=[{"event_id": eid, "error": msg} for eid, msg in result.errors],
  )


def _load_schedule_or_404(session: Session, structure_id: str) -> Structure:
  """Load and lock a schedule Structure, raising ScheduleNotFoundError.

  Every caller reads the template and originator id and writes back a decision
  from them; unlocked, two writers could create two live obligation registers.
  Bounded (``lock_by_id``) so contention raises the RowLockedError the callers
  map to 409, rather than blocking a pooled connection.
  """
  from robosystems.operations.locking import lock_by_id

  structure = lock_by_id(
    session,
    Structure,
    structure_id,
    f"Schedule {structure_id} is being written by another process. Retry in a moment.",
  )
  if structure is None or structure.block_type != "schedule":
    raise ScheduleNotFoundError(structure_id)
  return structure


def update_schedule(
  session: Session,
  body: UpdateScheduleRequest,
  updated_by: str = "system",
) -> ScheduleCreatedResponse:
  """Update `name`, `entry_template`, or `schedule_metadata`.

  Period range and monthly amount define the fact grid and are not editable.
  A template change supersedes the pending obligations (prospective only).
  Raises `ScheduleNotFoundError`.
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
  # Both columns are written; see ScheduleService._build_schedule_definition_blobs.
  structure.artifact_mechanics = {
    **(structure.artifact_mechanics or {}),
    "kind": "closing_entry_generator",
    "entry_template": metadata.get("entry_template", {}),
    "schedule_metadata": metadata.get("schedule_metadata"),
  }

  # Same transaction as the template write, so both land or neither does.
  if template_changed:
    from robosystems.operations.locking import (
      bounded_lock_wait as _bounded_lock_wait,
    )

    # Bounded: contends with the promotion sweep over the same rows.
    with _bounded_lock_wait(
      session,
      "This schedule's pending obligations are being written by another "
      "process. Retry in a moment.",
    ):
      ScheduleService().supersede_pending_obligations(
        session,
        structure=structure,
        created_by=updated_by,
      )

  rule_summary: dict[str, int] | None = None
  if template_changed:
    rule_results = evaluate_rules_for_structure(
      session,
      structure.id,
      created_by=updated_by,
    )
    rule_summary = _rule_summary(rule_results)

  # Read before commit (see create_schedule); the recounts below bind these.
  structure_id = structure.id
  structure_name = structure.name
  structure_taxonomy_id = structure.taxonomy_id

  session.commit()

  count_row = session.execute(
    text("SELECT COUNT(*) AS cnt FROM facts WHERE structure_id = :sid"),
    {"sid": structure_id},
  ).fetchone()
  period_row = session.execute(
    text(
      "SELECT COUNT(DISTINCT (period_start, period_end)) AS cnt "
      "FROM facts WHERE structure_id = :sid"
    ),
    {"sid": structure_id},
  ).fetchone()

  return ScheduleCreatedResponse(
    structure_id=structure_id,
    name=structure_name,
    taxonomy_id=structure_taxonomy_id,
    total_periods=period_row.cnt if period_row else 0,
    total_facts=count_row.cnt if count_row else 0,
    rule_summary=rule_summary,
  )


def delete_schedule(session: Session, body: DeleteScheduleRequest) -> dict:
  """Delete a schedule and everything under it, in FK order. Pending
  obligations are voided first so they can't outlive their originator and
  trip the close gate. Raises `ScheduleNotFoundError`.
  """
  structure = _load_schedule_or_404(session, body.structure_id)
  # Bounded: the void locks rows the promotion sweep holds.
  from robosystems.operations.locking import bounded_lock_wait

  with bounded_lock_wait(
    session,
    "This schedule's pending obligations are being written by another "
    "process. Retry in a moment.",
  ):
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


def _rewrite_sum_equals_rule(session: Session, structure: Structure) -> bool:
  """Re-anchor the native SumEquals rule to the truncated curve's sum and
  clear its verification results. False when there is no rule or no debit
  element to sum.
  """
  rule = (
    session.execute(
      select(Rule).where(
        Rule.target_structure_id == structure.id,
        Rule.rule_pattern == "SumEquals",
        Rule.rule_origin == "native",
      )
    )
    .scalars()
    .first()
  )
  if rule is None:
    return False

  mechanics = structure.artifact_mechanics or {}
  template = mechanics.get("entry_template") or {}
  debit_element_id = template.get("debit_element_id")
  if not debit_element_id:
    return False

  remaining = session.execute(
    text(
      "SELECT COALESCE(SUM(value), 0) FROM facts "
      "WHERE structure_id = :sid AND element_id = :eid"
    ),
    {"sid": structure.id, "eid": debit_element_id},
  ).scalar()
  new_total = round(float(remaining or 0), 2)

  # The evaluator compares against metadata expected_total; rule_expression
  # is display only. Both must change.
  rule.rule_expression = f"sum($periodic_amount) = {new_total}"
  rule_metadata = dict(rule.metadata_ or {})
  rule_metadata["expected_total"] = new_total
  rule.metadata_ = rule_metadata
  flag_modified(rule, "metadata_")

  # The stored basis must match too, or rebuild-schedule would spread the
  # original amount over the surviving months. The old basis stays in the
  # `truncations` audit log.
  mechanics_metadata = dict(structure.metadata_ or {})
  schedule_meta = dict(mechanics_metadata.get("schedule_metadata") or {})
  if schedule_meta.get("original_amount"):
    # Generation expenses original_amount less residual_value.
    schedule_meta["original_amount"] = round(new_total * 100) + int(
      schedule_meta.get("residual_value") or 0
    )
    mechanics_metadata["schedule_metadata"] = schedule_meta
    structure.metadata_ = mechanics_metadata
    structure.artifact_mechanics = {
      **(structure.artifact_mechanics or {}),
      "kind": "closing_entry_generator",
      "entry_template": mechanics_metadata.get("entry_template", {}),
      "schedule_metadata": schedule_meta,
    }
    flag_modified(structure, "metadata_")
    flag_modified(structure, "artifact_mechanics")

  session.query(VerificationResult).filter(
    VerificationResult.rule_id == rule.id
  ).delete(synchronize_session=False)
  return True


def terminate_schedule(
  session: Session,
  body: TerminateScheduleRequest,
  created_by: str = "system",
) -> TerminateScheduleResponse:
  """End a schedule at a month-end cutoff without booking an entry.

  For terminations whose GL effect is already booked or unwanted; when a
  derecognition entry is needed, the asset_disposed handler posts it with
  the same void. In one transaction: truncate facts and drafts past the
  cutoff, void obligations past it (``pending`` and ``classified`` — a
  classified one there can only be an undrafted stray), and re-anchor the
  SumEquals rule.

  Raises ``ScheduleNotFoundError``, or ``ValueError`` on the truncation guards.
  """
  structure = _load_schedule_or_404(session, body.structure_id)
  service = ScheduleService()

  truncation = service.truncate_schedule(
    session,
    structure_id=structure.id,
    new_end_date=body.new_end_date,
    reason=body.reason,
    updated_by=created_by,
  )

  # Bounded: the void locks rows the promotion sweep holds.
  from robosystems.operations.locking import bounded_lock_wait

  with bounded_lock_wait(
    session,
    "This schedule's pending obligations are being written by another "
    "process. Retry in a moment.",
  ):
    obligations_voided = service.void_pending_obligations(
      session,
      structure=structure,
      void_reason=f"schedule_terminated: {body.reason}",
      period_start_after=body.new_end_date,
      include_classified=True,
    )

  rule_updated = _rewrite_sum_equals_rule(session, structure)
  session.commit()

  return TerminateScheduleResponse(
    structure_id=structure.id,
    name=structure.name,
    new_end_date=truncation["new_end_date"],
    facts_deleted=truncation["facts_deleted"],
    obligations_voided=obligations_voided,
    rule_updated=rule_updated,
    reason=body.reason,
  )


def _reconstruct_schedule_definition(
  session: Session, structure: Structure
) -> tuple[EntryTemplate, ScheduleMetadata | None, int, date, date, str | None]:
  """Recover generation inputs from ``metadata_``; for rows missing the
  scalar keys, derive period bounds from the FactSet and ``monthly_amount``
  from the first duration debit fact. Raises ``ValueError`` if neither works.
  """
  metadata = structure.metadata_ or {}
  raw_template = metadata.get("entry_template")
  if not raw_template:
    raise ValueError(
      f"Schedule {structure.id!r} has no entry_template in metadata; cannot rebuild."
    )
  entry_template = EntryTemplate(
    debit_element_id=raw_template["debit_element_id"],
    credit_element_id=raw_template["credit_element_id"],
    entry_type=raw_template.get("entry_type", "closing"),
    memo_template=raw_template.get("memo_template", ""),
    auto_reverse=raw_template.get("auto_reverse", False),
  )

  raw_meta = metadata.get("schedule_metadata")
  schedule_metadata = (
    ScheduleMetadata(
      method=raw_meta.get("method", "straight_line"),
      original_amount=raw_meta.get("original_amount", 0),
      residual_value=raw_meta.get("residual_value", 0),
      useful_life_months=raw_meta.get("useful_life_months", 0),
      asset_element_id=raw_meta.get("asset_element_id"),
      periodic_amounts=raw_meta.get("periodic_amounts"),
    )
    if raw_meta
    else None
  )

  source_transaction_id = (structure.artifact_mechanics or {}).get(
    "source_transaction_id"
  ) or (structure.metadata_ or {}).get("source_transaction_id")

  monthly_amount = metadata.get("monthly_amount")
  period_start_iso = metadata.get("period_start")
  period_end_iso = metadata.get("period_end")

  period_start: date | None = (
    date.fromisoformat(period_start_iso) if period_start_iso else None
  )
  period_end: date | None = (
    date.fromisoformat(period_end_iso) if period_end_iso else None
  )

  if period_start is None or period_end is None:
    bounds = session.execute(
      select(
        FactSet.period_start.label("min_start"),
        FactSet.period_end.label("max_end"),
      ).where(
        FactSet.structure_id == structure.id,
        FactSet.factset_type == "schedule",
      )
    ).fetchall()
    starts = [b.min_start for b in bounds if b.min_start is not None]
    ends = [b.max_end for b in bounds if b.max_end is not None]
    if not starts or not ends:
      raise ValueError(
        f"Schedule {structure.id!r} has no stored period bounds and no "
        "schedule FactSet to derive them from; cannot rebuild."
      )
    period_start = period_start or min(starts)
    period_end = period_end or max(ends)

  if monthly_amount is None:
    debit_fact = session.execute(
      select(Fact.value)
      .where(
        Fact.structure_id == structure.id,
        Fact.element_id == entry_template.debit_element_id,
        Fact.period_type == "duration",
      )
      .order_by(Fact.period_start.asc())
      .limit(1)
    ).scalar()
    if debit_fact is None:
      raise ValueError(
        f"Schedule {structure.id!r} has no stored monthly_amount and no "
        "duration debit fact to derive it from; cannot rebuild."
      )
    monthly_amount = round(float(debit_fact) * 100)

  return (
    entry_template,
    schedule_metadata,
    int(monthly_amount),
    period_start,
    period_end,
    source_transaction_id,
  )


def rebuild_schedule(
  session: Session,
  body: RebuildScheduleRequest,
  created_by: str = "system",
) -> ScheduleCreatedResponse:
  """Regenerate a schedule in place from its stored definition.

  Keeps the structure id, associations and taxonomy; replaces facts, rules,
  drafts and the obligation chain. Scope is re-derived from the current
  calendar's ``closed_through``. Raises ``ScheduleNotFoundError``, or
  ``ValueError`` when the definition can't be reconstructed or landed
  entries exist.
  """
  structure = _load_schedule_or_404(session, body.structure_id)

  (
    entry_template,
    schedule_metadata,
    monthly_amount,
    period_start,
    period_end,
    source_transaction_id,
  ) = _reconstruct_schedule_definition(session, structure)

  # Landed entries depend on the facts a rebuild regenerates.
  posted_row = session.execute(
    text(
      "SELECT COUNT(*) AS c FROM entries "
      "WHERE source_structure_id = :sid AND status IN :landed_entry_statuses"
    ).bindparams(landed_entry_bindparam()),
    {"sid": structure.id},
  ).fetchone()
  if posted_row and posted_row.c:
    raise ValueError(
      f"Cannot rebuild schedule {structure.id!r}: {posted_row.c} posted "
      "closing entries exist. Reopen the affected periods and void those "
      "entries first — reopening alone leaves entries posted, so it does "
      "not clear this guard."
    )

  old_schedule_created_event_id = (structure.metadata_ or {}).get(
    "schedule_created_event_id"
  )

  closed_through = _calendar_closed_through_date(session)

  service = ScheduleService()

  # Fence before the draft deletes lock rows: fence, then rows, as every
  # ledger writer does.
  from robosystems.operations.roboledger.commands._guards import (
    assert_period_not_closed,
  )

  draft_dates = (
    session.execute(
      text(
        "SELECT DISTINCT posting_date FROM entries "
        "WHERE source_structure_id = :sid AND status = 'draft'"
      ),
      {"sid": structure.id},
    )
    .scalars()
    .all()
  )
  assert_period_not_closed(session, *draft_dates)

  # Bounded: contends with the promotion sweep over these rows.
  from robosystems.operations.locking import bounded_lock_wait, lock_by_id

  with bounded_lock_wait(
    session,
    "This schedule's pending obligations are being written by another "
    "process. Retry in a moment.",
  ):
    # Classified too: the guard above leaves them only draft entries, which
    # are deleted below, and the new chain re-issues every period.
    service.void_pending_obligations(
      session,
      structure=structure,
      void_reason="schedule_rebuilt",
      include_classified=True,
    )

  # delete_schedule's cascade, minus the Structure and Associations.
  rule_ids = (
    session.execute(select(Rule.id).where(Rule.target_structure_id == structure.id))
    .scalars()
    .all()
  )
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

  # The rebuilt obligation chain re-drafts these on the next promote.
  session.execute(
    text(
      "DELETE FROM line_items WHERE entry_id IN ("
      "SELECT id FROM entries "
      "WHERE source_structure_id = :sid AND status = 'draft')"
    ),
    {"sid": structure.id},
  )
  session.execute(
    text("DELETE FROM entries WHERE source_structure_id = :sid AND status = 'draft'"),
    {"sid": structure.id},
  )
  session.flush()

  structure = service.create_schedule(
    session,
    name=structure.name,
    taxonomy_id=structure.taxonomy_id,
    element_ids=[],
    period_start=period_start,
    period_end=period_end,
    monthly_amount=monthly_amount,
    entry_template=entry_template,
    schedule_metadata=schedule_metadata,
    created_by=created_by,
    closed_through=closed_through,
    existing_structure=structure,
    source_transaction_id=source_transaction_id,
  )

  # Void the old originator and link it to the new one.
  new_schedule_created_event_id = (structure.metadata_ or {}).get(
    "schedule_created_event_id"
  )
  if (
    old_schedule_created_event_id
    and new_schedule_created_event_id
    and old_schedule_created_event_id != new_schedule_created_event_id
  ):
    old_evt = lock_by_id(
      session,
      Event,
      old_schedule_created_event_id,
      f"Event {old_schedule_created_event_id} is being written by another "
      "process. Retry in a moment.",
    )
    if old_evt is not None:
      old_evt.status = "voided"
      old_evt.replaced_by_event_id = new_schedule_created_event_id
      session.flush()

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
    period_start=period_start,
    period_end=period_end,
    created_by=created_by,
  )

  # Read before commit (see create_schedule).
  metadata = structure.metadata_ or {}
  structure_id = structure.id
  structure_name = structure.name
  structure_taxonomy_id = structure.taxonomy_id

  session.commit()

  return ScheduleCreatedResponse(
    structure_id=structure_id,
    name=structure_name,
    taxonomy_id=structure_taxonomy_id,
    total_periods=period_row.cnt if period_row else 0,
    total_facts=count_row.cnt if count_row else 0,
    rule_summary=_rule_summary(rule_results),
    schedule_created_event_id=metadata.get("schedule_created_event_id"),
    pending_event_count=metadata.get("pending_event_count", 0),
  )
