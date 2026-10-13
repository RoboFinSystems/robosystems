"""Schedule write operations: request bodies to ScheduleService calls."""

from __future__ import annotations

from datetime import UTC, date, datetime

from sqlalchemy import bindparam, func, or_, select, text
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
from robosystems.operations.roboledger.entity_scope import (
  ensure_entity_id,
  owner_entity_id,
)
from robosystems.operations.roboledger.entry_status import (
  terminal_entry_bindparam,
)
from robosystems.operations.roboledger.schedules import ScheduleService
from robosystems.operations.roboledger.schedules.service import (
  BEFORE_CALENDAR,
  EntryTemplate,
  ScheduleMetadata,
)


class ScheduleNotFoundError(LookupError):
  def __init__(self, structure_id: str) -> None:
    super().__init__(f"Schedule not found: {structure_id}")
    self.structure_id = structure_id


def _calendar_bounds(
  session: Session, entity_id: str
) -> tuple[date | None, date | None]:
  """The end of the entity's ``closed_through_period`` and the start of its
  first fiscal period; each None when there is none."""
  from robosystems.models.extensions.roboledger.fiscal_calendar import (
    FiscalCalendar,
  )
  from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
  from robosystems.operations.roboledger.fiscal_calendar import (
    period_date_range,
  )

  cal = (
    session.query(FiscalCalendar).filter(FiscalCalendar.entity_id == entity_id).first()
  )
  closed_end = (
    period_date_range(str(cal.closed_through_period))[1]
    if cal is not None and cal.closed_through_period
    else None
  )
  first_start = (
    session.query(func.min(FiscalPeriod.start_date))
    .filter(FiscalPeriod.entity_id == entity_id)
    .scalar()
  )
  return closed_end, first_start


def _calendar_history_end(session: Session, entity_id: str) -> date | None:
  """The last day of the entity's history: nothing dated on or before it
  posts through a close. The later of its ``closed_through_period``'s end
  and the day before its first fiscal period; None with no calendar yet.

  A schedule's months up to it are history (facts ``historical``,
  obligations voided), whichever side of the calendar they fall on.
  """
  from datetime import timedelta

  closed_end, first_start = _calendar_bounds(session, entity_id)
  ends = [d for d in (closed_end,) if d is not None]
  if first_start is not None:
    ends.append(first_start - timedelta(days=1))
  return max(ends) if ends else None


def _calendar_open_start(session: Session, entity_id: str) -> date | None:
  """The first day of the entity's calendar while none of it has closed, the
  only time its start can move; None otherwise."""
  closed_end, first_start = _calendar_bounds(session, entity_id)
  return first_start if closed_end is None else None


def _schedule_drafts_before(
  session: Session, entity_id: str, before: date
) -> list[str]:
  """The entity's schedule-written draft entries dated before ``before``, and
  the draft auto-reversals of them (dated the month after, so possibly not)."""
  return list(
    session.execute(
      text(
        """
        WITH early AS (
          SELECT e.id FROM entries e
          JOIN structures s ON s.id = e.source_structure_id
          WHERE s.block_type = 'schedule'
            AND e.entity_id = :entity_id
            AND e.status = 'draft'
            AND e.posting_date < :before
        )
        SELECT id FROM early
        UNION
        SELECT id FROM entries
        WHERE status = 'draft' AND reversal_of IN (SELECT id FROM early)
        """
      ),
      {"entity_id": entity_id, "before": before},
    ).scalars()
  )


def retire_schedule_history(
  session: Session, entity_id: str, *, before: date, void_reason: str
) -> int:
  """Retire the entity's schedule months ending before ``before`` as history,
  as `create_schedule` does for months on or before the calendar's history
  end, for a calendar set up after its schedules: obligations still to post
  (``pending``, or ``classified`` by a sweep that ran with no calendar)
  voided and marked with ``void_reason``, the drafts written for them
  deleted, facts stamped ``historical``. Returns the obligations voided.
  """
  schedule_ids = {
    sid
    for (sid,) in session.query(Event.metadata_["schedule_id"].astext)
    .filter(
      Event.entity_id == entity_id,
      Event.event_type == "schedule_entry_due",
      Event.status.in_(("pending", "classified")),
      Event.metadata_["period_end"].astext < before.isoformat(),
    )
    .distinct()
    if sid
  }
  service = ScheduleService()
  voided = 0
  for structure in (
    session.query(Structure)
    .filter(Structure.id.in_(schedule_ids))
    .order_by(Structure.id)
  ):
    voided += service.void_pending_obligations(
      session,
      structure=structure,
      void_reason=void_reason,
      period_end_before=before,
      include_classified=True,
      mark_obligations=True,
    )
  drafts = _schedule_drafts_before(session, entity_id, before)
  if drafts:
    ids = bindparam("ids", expanding=True)
    session.execute(
      text("DELETE FROM line_items WHERE entry_id IN :ids").bindparams(ids),
      {"ids": drafts},
    )
    session.execute(
      text("DELETE FROM entries WHERE id IN :ids").bindparams(ids),
      {"ids": drafts},
    )
  session.execute(
    text(
      """
      UPDATE facts
      SET fact_scope = 'historical'
      WHERE fact_scope = 'in_scope'
        AND entity_id = :entity_id
        AND structure_id IN (
          SELECT id FROM structures WHERE block_type = 'schedule'
        )
        AND period_end < :before
      """
    ),
    {"entity_id": entity_id, "before": before},
  )
  return voided


def restore_schedule_history(session: Session, entity_id: str, *, since: date) -> int:
  """Give back the schedule months a calendar's start had retired, once it
  moves earlier to ``since``: each obligation voided as ``before_calendar``
  and ending on or after it returns to ``pending``, and its month's facts to
  ``in_scope``, so the close drafts them. Obligations voided for any other
  reason (a user's void, booked history, a rebuild, a deletion, a
  termination's truncated months) stay as they are. Returns the count.
  """
  rows = session.execute(
    text(
      """
      SELECT ob.id, ob.obligated_by_event_id AS originator_id,
             ob.metadata->>'schedule_id' AS structure_id,
             ob.metadata->>'period_start' AS period_start,
             ob.metadata->>'period_end' AS period_end
      FROM events ob
      JOIN events origin
        ON origin.id = ob.obligated_by_event_id AND origin.status = 'committed'
      JOIN structures s ON s.id = ob.metadata->>'schedule_id'
      WHERE ob.entity_id = :entity_id
        AND ob.event_type = 'schedule_entry_due'
        AND ob.status = 'voided'
        AND ob.replaced_by_event_id IS NULL
        AND ob.metadata->>'void_reason' = :reason
        AND ob.metadata->>'period_end' >= :since
        AND EXISTS (
          SELECT 1 FROM facts f
          WHERE f.structure_id = s.id
            AND f.period_type = 'duration'
            AND f.period_end = (ob.metadata->>'period_end')::date
        )
      ORDER BY ob.id
      FOR UPDATE OF ob
      """
    ),
    {"entity_id": entity_id, "reason": BEFORE_CALENDAR, "since": since.isoformat()},
  ).all()
  if not rows:
    return 0

  session.execute(
    text(
      "UPDATE events SET status = 'pending', metadata = metadata - 'void_reason' "
      "WHERE id IN :ids AND status = 'voided'"
    ).bindparams(bindparam("ids", expanding=True)),
    {"ids": [row.id for row in rows]},
  )

  by_structure: dict[str, list] = {}
  by_originator: dict[str, int] = {}
  for row in rows:
    by_structure.setdefault(row.structure_id, []).append(row)
    by_originator[row.originator_id] = by_originator.get(row.originator_id, 0) + 1
  for structure_id, months in by_structure.items():
    # A month's duration and month-end facts end on its last day; the
    # schedule's opening instants sit on its first month's first day.
    session.execute(
      text(
        """
        UPDATE facts SET fact_scope = 'in_scope'
        WHERE fact_scope = 'historical'
          AND structure_id = :sid
          AND (
            period_end IN :ends
            OR (period_type = 'instant' AND period_end IN :starts)
          )
        """
      ).bindparams(
        bindparam("ends", expanding=True), bindparam("starts", expanding=True)
      ),
      {
        "sid": structure_id,
        "ends": [date.fromisoformat(m.period_end) for m in months],
        "starts": [date.fromisoformat(m.period_start) for m in months],
      },
    )
  now = datetime.now(UTC).isoformat()
  for originator_id, restored in by_originator.items():
    originator = session.get(Event, originator_id)
    if originator is None:
      continue
    meta = dict(originator.metadata_ or {})
    meta["pending_event_count"] = int(meta.get("pending_event_count", 0)) + restored
    meta.setdefault("void_history", []).append(
      {
        "restored_count": restored,
        "restore_reason": "calendar_start_moved_earlier",
        "restored_at": now,
      }
    )
    originator.metadata_ = meta
    flag_modified(originator, "metadata_")
  return len(rows)


def reinstate_reopened_schedule_scopes(session: Session, entity_id: str) -> int:
  """Re-stamp the entity's ``historical`` schedule facts after its
  ``closed_through`` back to ``in_scope`` once reopen-period has moved the
  boundary back.

  Scope is stamped only at generation, so without this a reopened month drops
  out of the roll-forward and the re-close skips it. Idempotent; returns the
  number of facts re-stamped.
  """
  from sqlalchemy import text

  closed_through = _calendar_history_end(session, entity_id)
  result = session.execute(
    text(
      """
      UPDATE facts
      SET fact_scope = 'in_scope'
      WHERE fact_scope = 'historical'
        AND entity_id = :entity_id
        AND structure_id IN (
          SELECT id FROM structures WHERE block_type = 'schedule'
        )
        AND (:closed_through IS NULL OR period_end > :closed_through)
      """
    ),
    {"closed_through": closed_through, "entity_id": entity_id},
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
  *,
  entity_id: str | None = None,
) -> ScheduleCreatedResponse:
  """Create a schedule in one entity's books, default the group parent.

  Raises `ValueError` for validation failures (mapped to 422).
  """
  entity_id = ensure_entity_id(session, entity_id)
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
      booked_on=body.schedule_metadata.booked_on,
    )

  # Never earlier than the calendar's history end: a pending obligation in a
  # closed month, or one before the calendar, would block close-period forever.
  history_end = _calendar_history_end(session, entity_id)
  effective_closed_through = max(
    (d for d in (body.closed_through, history_end) if d is not None), default=None
  )

  structure = service.create_schedule(
    session,
    calendar_start=_calendar_open_start(session, entity_id),
    booked_through=body.closed_through,
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
    entity_id=entity_id,
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
    # Only the fields the caller named change. Replacing the whole block
    # would reset the rest to defaults, so adding a date to an existing
    # schedule would wipe its cost basis.
    metadata["schedule_metadata"] = {
      **(metadata.get("schedule_metadata") or {}),
      **body.schedule_metadata.model_dump(
        mode="json", include=body.schedule_metadata.model_fields_set
      ),
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


def _terminal_entry_count(session: Session, structure_id: str) -> int:
  """The schedule's entries that stand on its facts: landed, or shadowed by
  a shadow close, whose receipt refers to them as its expectations."""
  row = session.execute(
    text(
      "SELECT COUNT(*) AS c FROM entries "
      "WHERE source_structure_id = :sid AND status IN :terminal_entry_statuses"
    ).bindparams(terminal_entry_bindparam()),
    {"sid": structure_id},
  ).fetchone()
  return int(row.c) if row and row.c else 0


def _fence_draft_periods(session: Session, structure: Structure) -> None:
  """Fence before the draft deletes lock rows: fence, then rows, as every
  ledger writer does.

  Only the drafts a close posts are fenced. A retracted event's leftover is
  one no close posts, so it can sit in a closed month without being part of
  its books, and must not hold the schedule there.
  """
  from robosystems.operations.roboledger.commands._guards import (
    assert_period_not_closed,
  )
  from robosystems.operations.roboledger.fiscal_calendar.qb_writeback import (
    WRITEBACK_EXCLUDED_EVENT_STATUSES,
  )

  draft_dates = (
    session.execute(
      text(
        "SELECT DISTINCT e.posting_date FROM entries e "
        "LEFT JOIN events ev ON ev.id = e.triggered_by_event_id "
        "WHERE e.source_structure_id = :sid AND e.status = 'draft' "
        "AND (ev.id IS NULL OR ev.status NOT IN :retracted)"
      ).bindparams(bindparam("retracted", expanding=True)),
      {"sid": structure.id, "retracted": list(WRITEBACK_EXCLUDED_EVENT_STATUSES)},
    )
    .scalars()
    .all()
  )
  assert_period_not_closed(
    session, *draft_dates, entity_id=owner_entity_id(session, structure)
  )


def _delete_draft_entries(session: Session, structure_id: str) -> None:
  session.execute(
    text(
      "DELETE FROM line_items WHERE entry_id IN ("
      "SELECT id FROM entries "
      "WHERE source_structure_id = :sid AND status = 'draft')"
    ),
    {"sid": structure_id},
  )
  session.execute(
    text("DELETE FROM entries WHERE source_structure_id = :sid AND status = 'draft'"),
    {"sid": structure_id},
  )


def delete_schedule(session: Session, body: DeleteScheduleRequest) -> dict:
  """Delete a schedule and everything under it, in FK order. Obligations are
  voided and drafts deleted first so neither outlives the schedule and trips
  the close gate. Raises `ScheduleNotFoundError`, or ``ValueError`` when
  landed entries exist: the schedule's facts are their support.
  """
  structure = _load_schedule_or_404(session, body.structure_id)

  # Fence, then count: a close that posts one of these drafts either finished
  # before the fence, and is counted, or waits behind it.
  _fence_draft_periods(session, structure)
  landed = _terminal_entry_count(session, structure.id)
  if landed:
    raise ValueError(
      f"Cannot delete schedule {structure.id!r}: {landed} posted or shadowed "
      "closing entries exist, and the schedule is their support. Use "
      "terminate-schedule to stop it and keep its history."
    )

  # Bounded: the void locks rows the promotion sweep holds.
  from robosystems.operations.locking import bounded_lock_wait

  with bounded_lock_wait(
    session,
    "This schedule's pending obligations are being written by another "
    "process. Retry in a moment.",
  ):
    # Classified too: the guard above leaves them only draft entries, which
    # are deleted below.
    ScheduleService().void_pending_obligations(
      session,
      structure=structure,
      void_reason="schedule_deleted",
      include_classified=True,
    )
  _delete_draft_entries(session, structure.id)
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
  # original amount over the surviving months. The cost it started from is
  # kept as `original_amount_at_start`.
  mechanics_metadata = dict(structure.metadata_ or {})
  schedule_meta = dict(mechanics_metadata.get("schedule_metadata") or {})
  if schedule_meta.get("original_amount"):
    # The cost the schedule started from: the periods before the cutoff
    # still carry it, and the re-anchored figure below is not it.
    schedule_meta.setdefault(
      "original_amount_at_start", schedule_meta["original_amount"]
    )
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
      booked_on=(
        date.fromisoformat(raw_meta["booked_on"]) if raw_meta.get("booked_on") else None
      ),
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


def _before_calendar_note(session: Session, structure_id: str) -> str:
  """For a refused rebuild: where the schedule's months before the calendar
  went, since a rebuild is not the way back for them. Empty when it has none."""
  first, count = session.execute(
    select(func.min(Event.metadata_["period_end"].astext), func.count(Event.id)).where(
      Event.event_type == "schedule_entry_due",
      Event.status == "voided",
      Event.metadata_["schedule_id"].astext == structure_id,
      Event.metadata_["void_reason"].astext == BEFORE_CALENDAR,
    )
  ).one()
  if not count:
    return ""
  return (
    f" Its {count} month{'s' if count != 1 else ''} from {first[:7]}, before "
    "the calendar's first month, "
    "are retired, not lost: moving the calendar's start earlier "
    "(change-calendar-start) returns them to post, which only a calendar with "
    "no month closed allows; once one has, book them with a journal entry "
    "instead."
  )


def rebuild_schedule(
  session: Session,
  body: RebuildScheduleRequest,
  created_by: str = "system",
) -> ScheduleCreatedResponse:
  """Regenerate a schedule in place from its stored definition.

  Keeps the structure id, associations and taxonomy; replaces facts, rules,
  drafts and the obligation chain. Scope is re-derived from the current
  calendar's history end (`_calendar_history_end`). Raises
  ``ScheduleNotFoundError``, or ``ValueError`` when the definition can't be
  reconstructed or landed entries exist.
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

  # Landed entries depend on the facts a rebuild regenerates. Fence, then
  # count: a close that posts one of these drafts either finished before the
  # fence, and is counted, or waits behind it.
  _fence_draft_periods(session, structure)
  landed = _terminal_entry_count(session, structure.id)
  if landed:
    raise ValueError(
      f"Cannot rebuild schedule {structure.id!r}: {landed} posted or shadowed "
      "closing entries exist. A posted entry clears once its period is "
      "reopened and the entry voided; reopening alone leaves it posted. A "
      "shadowed entry is a shadow close's record and is never voided: "
      "terminate the schedule after the last shadow-closed period instead."
      + _before_calendar_note(session, structure.id)
    )

  old_schedule_created_event_id = (structure.metadata_ or {}).get(
    "schedule_created_event_id"
  )

  owner = owner_entity_id(session, structure)
  closed_through = _calendar_history_end(session, owner)

  service = ScheduleService()

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
  _delete_draft_entries(session, structure.id)
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
    calendar_start=_calendar_open_start(session, owner),
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
