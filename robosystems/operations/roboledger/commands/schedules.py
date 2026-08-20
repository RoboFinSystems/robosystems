"""Write operations for schedules and closing entries.

Thin wrappers over `ScheduleService`. The service does the heavy lifting
(fact generation, entry creation, balance validation) — these functions
translate request bodies to service calls and assemble responses.
"""

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


def _calendar_closed_through_date(session: Session):
  """Return the active fiscal calendar's `closed_through_period` as a date.

  Used by `create_schedule` to default the historical-voiding boundary
  when the caller doesn't supply `closed_through` in the request body.
  Returns None when no calendar is initialized or its
  `closed_through_period` is null, which leaves every period pending for a
  graph that hasn't called initialize-ledger yet.
  """
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
  """Promote schedule facts the retreated close boundary has re-opened.

  Schedule fact scope (``historical`` vs ``in_scope``) is stamped at
  generation from ``closed_through`` (``period_end <= closed_through`` →
  historical). ``reopen-period`` moves ``closed_through`` backward but does not
  re-stamp existing facts, so a reopened month's facts stay ``historical``: its
  movement drops out of the roll-forward (the carry-in then renders that
  month's ending balance as the opening balance) and the re-close skips it.
  Flip the now-open window back to ``in_scope`` so every reader agrees with the
  calendar. Returns the number of facts re-stamped. Idempotent — a no-op when
  the boundary didn't move (e.g. reopening an older period).
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

  # `closed_through` controls which schedule-generated periods become
  # `pending` obligations vs `voided` (historical). When the caller
  # doesn't supply one, fall back to the active fiscal calendar's
  # `closed_through_period`. Without this default, callers who don't know
  # to thread the field end up creating schedules that emit pending
  # obligations for periods that are *already closed* — those obligations
  # then block close-period forever because they're sealed inside a closed
  # range yet still `pending`.
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

  # Read every attribute the response needs before the commit expires the
  # instance. A post-commit access issues a refresh SELECT on a connection the
  # commit already returned to the pool, whose search_path may have been reset
  # to `public` — so the row resolves to the wrong schema and a schedule that
  # committed comes back to the caller as a 500.
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
  """On-demand obligation-promotion sweep (the `scheduled_obligation_promoter`
  Dagster sensor's function, exposed for interactive use).

  Flips matured `pending` `schedule_entry_due` events to `classified` and,
  when ``dispatch_handlers`` is set, drafts their closing entries — so a
  schedule-driven close can be completed in one session without waiting for
  the background sensor. Stranded obligations (already `classified` but
  never drafted, e.g. by an earlier flip-only sweep) are dispatched too.
  The data scope is the session's search_path (the tenant graph); the
  sweep is idempotent (re-running skips already-classified rows and
  reconciles to existing drafts).
  """
  from robosystems.operations.event_block.promotion import promote_pending_obligations
  from robosystems.operations.locking import bounded_lock_wait

  # Request-facing, so the wait is bounded: the sweep locks its whole candidate
  # set, and the Dagster sensor runs the same function every few minutes. An
  # unbounded wait here would hold this request and its pooled connection until
  # a background tick finished.
  # The wrap covers the whole sweep, not just its locked candidate load — the
  # lock is taken inside. Autopilot dispatch also writes GL rows, so a wait
  # here is *usually* the background sweep holding the obligations but need not
  # be; the message says what is true rather than naming a cause it cannot know.
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


# ─── Schedule update / delete ─────────────────────────────────────────────


def _load_schedule_or_404(session: Session, structure_id: str) -> Structure:
  """Load a schedule Structure row by id, raising ScheduleNotFoundError."""
  structure = session.execute(
    select(Structure).where(
      Structure.id == structure_id,
      Structure.block_type == "schedule",
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
  `asset_disposed`) and create a fresh schedule via
  `create-information-block` (`block_type='schedule'`).

  When the entry template changes, all remaining `pending`
  schedule_entry_due obligations are voided and replaced with a fresh
  set linked via `replaces_event_id` / `replaced_by_event_id`.
  Already-classified / fulfilled obligations are untouched — the new
  template applies prospectively.

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
  # Dual-column write: envelope reads prefer artifact_mechanics; writes
  # stamp both columns so older rows that pre-date artifact_mechanics
  # remain readable through metadata_.
  # periods_with_entries is transient (queried from facts at read time) —
  # intentionally excluded rather than using ScheduleMechanics.model_dump().
  structure.artifact_mechanics = {
    "kind": "closing_entry_generator",
    "entry_template": metadata.get("entry_template", {}),
    "schedule_metadata": metadata.get("schedule_metadata"),
  }

  # When the entry template changed, void and re-materialize the pending
  # obligation chain in the same transaction so partial state is
  # impossible: either the new template + new pending events both land,
  # or neither does.
  if template_changed:
    from robosystems.operations.locking import (
      bounded_lock_wait as _bounded_lock_wait,
    )

    # Request-facing, and it contends with the promotion sweep over the same
    # pending obligations — bound the wait rather than hold this request for
    # the length of a background tick.
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

  # Re-run rule engine when the template changes, since the underlying
  # fact shape may have moved. No-op when the template was unchanged
  # (existing verification_results stay authoritative).
  rule_summary: dict[str, int] | None = None
  if template_changed:
    rule_results = evaluate_rules_for_structure(
      session,
      structure.id,
      created_by=updated_by,
    )
    rule_summary = _rule_summary(rule_results)

  # Captured before the commit expires the instance — these are not only the
  # response's values, they are the parameter the recounts below bind, so a
  # refresh that resolves to the wrong schema would break the counts too.
  structure_id = structure.id
  structure_name = structure.name
  structure_taxonomy_id = structure.taxonomy_id

  session.commit()

  # Recount for response (same as create_schedule response shape)
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
  """Delete a schedule — cascades through facts and associations.

  Deletion order respects FK constraints:
  1. Pending obligation events (voided before the parent disappears)
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
  # after the schedule is gone. Request-facing, and the void locks the
  # same pending rows the promotion sweep holds — bound the wait rather
  # than hold this request for the length of a background tick.
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
  """Re-anchor the schedule's native SumEquals rule to the truncated curve.

  The rule proves sum(periodic facts) == the schedule's original amount;
  truncation deletes forward facts, so the old total can never be
  satisfied again. The remaining curve is still worth proving — the
  expression is rewritten to its current sum, and verification results
  proved against the old expression are cleared for re-evaluation.

  Returns False (no-op) when the schedule has no native SumEquals rule
  or its entry template carries no debit element to sum.
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

  # `expected_total` is the value the evaluator actually compares against
  # (`rules/evaluators.py::_evaluate_sum_equals`); `rule_expression` is the
  # human-readable form. Rewriting only the expression leaves the rule
  # failing against the pre-truncation total while reading as re-anchored —
  # which is exactly what shipped and was caught on live books.
  rule.rule_expression = f"sum($periodic_amount) = {new_total}"
  rule_metadata = dict(rule.metadata_ or {})
  rule_metadata["expected_total"] = new_total
  rule.metadata_ = rule_metadata
  flag_modified(rule, "metadata_")

  # The stored definition has to describe the truncated curve too. Generation
  # derives `expected_total` from `schedule_metadata.original_amount`, so
  # leaving the original basis behind makes `rebuild-schedule` redistribute it
  # across only the surviving months — silently rewriting closed, posted
  # history. The pre-truncation basis stays in the `truncations` audit log.
  mechanics_metadata = dict(structure.metadata_ or {})
  schedule_meta = dict(mechanics_metadata.get("schedule_metadata") or {})
  if schedule_meta.get("original_amount"):
    schedule_meta["original_amount"] = round(new_total * 100)
    mechanics_metadata["schedule_metadata"] = schedule_meta
    structure.metadata_ = mechanics_metadata
    structure.artifact_mechanics = {
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
  """End a schedule early at a month-end cutoff — no entry is booked.

  The no-entry half of schedule retirement, for terminations whose GL
  effect is already booked (an asset transferred via a manual journal
  entry, a prepaid refunded in the source system) or where none is
  wanted. In one transaction:

  1. ``truncate_schedule`` deletes facts with period_start past the
     cutoff (guards: month-end cutoff only; refuses when posted entries
     exist past it; deletes stale draft entries past it).
  2. The remaining obligation chain past the cutoff is voided —
     ``pending`` and ``classified`` rows both. A ``classified`` row here
     can only be an undrafted stray: drafted entries past the cutoff
     were deleted in step 1 and posted ones blocked it.
  3. The schedule's SumEquals rule is rewritten to prove the truncated
     curve.

  Obligations and facts at or before the cutoff are untouched, so open
  months the schedule still covers close normally.

  When the derecognition entry still needs to be booked, use
  ``create-event-block(event_type='asset_disposed')`` instead — the
  disposal handler posts it atomically with the same obligation void.

  Raises ``ScheduleNotFoundError`` if the schedule does not exist, and
  ``ValueError`` on the truncation guards (mid-month cutoff, posted
  entries past the cutoff, cutoff before the schedule's first fact).
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

  # The void locks the same pending rows the promotion sweep holds —
  # bound the wait rather than hold this request for the length of a
  # background tick (mirrors delete_schedule).
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
  """Recover the generation inputs for a rebuild from a Structure row.

  Prefers the stored definition on ``metadata_`` (entry_template,
  schedule_metadata, monthly_amount, period_start, period_end — persisted at
  create time so the rebuild is unambiguous). A row missing those scalar keys
  falls back to deriving the period bounds from the schedule's FactSet and
  ``monthly_amount`` from a non-final duration debit Fact.

  Raises ``ValueError`` when the definition can't be reconstructed.
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

  # Audit back-ref to the source transaction — stored in artifact_mechanics
  # at create time, with metadata_ as the fallback location. Recovering it
  # keeps the rebuilt schedule pointing at its originating transaction.
  source_transaction_id = (structure.artifact_mechanics or {}).get(
    "source_transaction_id"
  ) or (structure.metadata_ or {}).get("source_transaction_id")

  # Reproducible scalar inputs — stored at create time on new rows.
  monthly_amount = metadata.get("monthly_amount")
  period_start_iso = metadata.get("period_start")
  period_end_iso = metadata.get("period_end")

  period_start: date | None = (
    date.fromisoformat(period_start_iso) if period_start_iso else None
  )
  period_end: date | None = (
    date.fromisoformat(period_end_iso) if period_end_iso else None
  )

  # Fallback: derive period bounds from the schedule's FactSet rows.
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

  # Fallback: derive monthly_amount from a non-final duration debit fact
  # (the per-period straight-line amount, in cents).
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
  """Re-run the schedule generator in place on an existing schedule.

  Atomic alternative to delete-then-recreate (which orphans the
  obligation chain). Preserves the structure id, its element
  associations, and its taxonomy; voids the old pending obligation
  chain; deletes the old facts, FactSets, and SumEquals rules; then
  regenerates the forward facts + a fresh obligation chain from the
  schedule's stored definition.

  The historical-vs-in-scope split is re-derived from the CURRENT fiscal
  calendar `closed_through`, re-scoping the schedule to today's close state.

  Raises:
      ScheduleNotFoundError: if the schedule does not exist.
      ValueError: if the schedule's definition can't be reconstructed.
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

  # Refuse to rebuild underneath posted closing entries — a rebuild
  # regenerates the facts those entries depend on, which would orphan the
  # audit trail. Reopen the affected periods first (mirrors truncate_schedule).
  posted_row = session.execute(
    text(
      "SELECT COUNT(*) AS c FROM entries "
      "WHERE source_structure_id = :sid AND status = 'posted'"
    ),
    {"sid": structure.id},
  ).fetchone()
  if posted_row and posted_row.c:
    raise ValueError(
      f"Cannot rebuild schedule {structure.id!r}: {posted_row.c} posted "
      "closing entries exist. Reopen the affected periods and void those "
      "entries first — reopening alone leaves entries posted, so it does "
      "not clear this guard."
    )

  # Capture the old originator event id so we can supersede it after the
  # rebuild stamps a fresh one (avoids two unlinked committed originators).
  old_schedule_created_event_id = (structure.metadata_ or {}).get(
    "schedule_created_event_id"
  )

  # Re-derive the close watermark from the CURRENT fiscal calendar — a
  # rebuild re-scopes the schedule to today's close state.
  closed_through = _calendar_closed_through_date(session)

  service = ScheduleService()

  # Fence first, then rows — the order every other ledger writer keeps. The
  # rebuild deletes this schedule's draft closing entries below and re-drafts
  # them through `create_closing_entry`, whose own fence would otherwise be
  # the first, taken *after* the deletes had already locked the rows.
  # A closer holding the exclusive side (or a closed month with a stale
  # draft still in it) is refused here, before anything is touched.
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

  # Void the old pending obligation chain so the regenerated chain doesn't
  # double-count and the old pending events can't trip the close gate.
  # Bounded for the same reason as `delete_schedule`: request-facing, and
  # it contends with the promotion sweep over these rows.
  from robosystems.operations.locking import bounded_lock_wait, lock_by_id

  with bounded_lock_wait(
    session,
    "This schedule's pending obligations are being written by another "
    "process. Retry in a moment.",
  ):
    service.void_pending_obligations(
      session,
      structure=structure,
      void_reason="schedule_rebuilt",
    )

  # Cascade-delete the old facts, FactSets, and SumEquals rule(s) — mirror
  # delete_schedule's cascade, but NOT the Structure, Associations, or
  # taxonomy (those are preserved). Verification results referencing the
  # structure or its rules are cleared too so stale rows don't linger.
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

  # Sweep stale DRAFT closing entries for this structure — the rebuilt
  # obligation chain re-drafts them on the next promote. Line items first
  # for the FK. Posted entries are guarded above, so this only hits drafts.
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

  # Regenerate in place — preserves structure id + associations + arcs.
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

  # Supersede the old originator event: the rebuild stamps a fresh
  # `schedule_created` event, leaving the old one orphaned. Mark it voided
  # and back-link it to its replacement so the audit chain stays connected.
  new_schedule_created_event_id = (structure.metadata_ or {}).get(
    "schedule_created_event_id"
  )
  if (
    old_schedule_created_event_id
    and new_schedule_created_event_id
    and old_schedule_created_event_id != new_schedule_created_event_id
  ):
    # Locked: this is a status write off a read, like every other event
    # transition. An inbox transition on the originator is the only thing
    # that could race it, and it should lose cleanly rather than be
    # overwritten.
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

  # Read every attribute the response needs before the commit expires the
  # instance. A post-commit access issues a refresh SELECT on a connection the
  # commit already returned to the pool, whose search_path may have been reset
  # to `public` — so the row resolves to the wrong schema and a schedule that
  # committed comes back to the caller as a 500.
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
