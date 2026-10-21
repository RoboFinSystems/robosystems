"""A report's columns freeze one by one as the calendar closes their periods;
the rest are live.

A column is frozen when its period is fully closed — its ``period_end`` on or
before the day the entity's books are closed through — and its facts are the
record, never rewritten. A live column is a cache: a read refreshes it when
an entry of the report's entities landed in it since the last generation, so
postings accrue into the report as they land. The close freezes the columns
it completes; a read freezes any column due that no close reached (a report
made over a closed window, a close that ran before the report existed). A
reopen puts the columns it reopens back to live. Spec ``fact-lineage`` §9.6.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, date, datetime

from sqlalchemy import text
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.extensions import Report
from robosystems.models.extensions.roboledger.fiscal_calendar import FiscalCalendar
from robosystems.operations.roboledger.entity_scope import (
  group_members,
  report_entity_id,
  resolve_entity_id,
)
from robosystems.operations.roboledger.fiscal_calendar.periods import period_date_range
from robosystems.operations.roboledger.reads.reports import build_periods
from robosystems.operations.roboledger.reports.fact_grid import PeriodSpec
from robosystems.operations.roboledger.reports.statement_sets import (
  _build_structure_mapping,
  _evaluate_report_structures,
  _group_parent_of,
  _pre_create_report_fact_sets,
  _stamp_facts_into_sets,
  generate_entity_report_facts,
)


@dataclass
class RefreshResult:
  """What a refresh did: whether it rewrote anything, and the rule summary
  of the evaluation that followed when it did."""

  rewritten: bool
  rule_summary: dict[str, int] | None = None


def closed_through_end(session: Session, entity_id: str) -> date | None:
  """The last day the entity's books are closed through; None before any close."""
  period = (
    session.query(FiscalCalendar.closed_through_period)
    .filter(FiscalCalendar.entity_id == entity_id)
    .scalar()
  )
  return period_date_range(period)[1] if period else None


def refreshable(report_def: Report, *, forced: bool = False) -> bool:
  """A report that moves with the books: its own and not final, and — unless
  a regenerate is driving — generated."""
  if report_def.source_report_id is not None:
    return False
  if report_def.filing_status in {"filed", "archived"}:
    return False
  return forced or report_def.generation_status in {"complete", "published"}


def report_entity(session: Session, report_def: Report) -> str:
  """The entity a report states, the way its generation resolves it."""
  return resolve_entity_id(
    session, report_def.entity_id or report_entity_id(session, report_def.id)
  )


def report_columns(report_def: Report) -> list[PeriodSpec]:
  return build_periods(
    report_def.period_start,
    report_def.period_end,
    report_def.comparative,
    report_def.periods,
  )


def open_columns(session: Session, report_def: Report) -> list[PeriodSpec]:
  """The columns whose period the books have not fully closed."""
  cutoff = closed_through_end(session, report_entity(session, report_def))
  return [p for p in report_columns(report_def) if cutoff is None or p.end > cutoff]


def _naive_utc(stamp: datetime) -> datetime:
  """The ledger's timestamps are naive UTC; a value set this transaction is
  aware, and Postgres must not be left to cast between them."""
  return stamp.astimezone(UTC).replace(tzinfo=None) if stamp.tzinfo else stamp


def _ledger_entities(session: Session, entity_id: str) -> list[str]:
  """The entities whose postings a report for ``entity_id`` reads: for the
  consolidation entity, the group's members and itself."""
  parent = _group_parent_of(session, entity_id)
  if parent is None:
    return [entity_id]
  return [m.id for m in group_members(session, parent, consolidated=True)]


def live_columns_stale(
  session: Session, report_def: Report, entity_id: str, cutoff: date | None
) -> bool:
  """Whether what the live columns read moved since the last generation: an
  entry of the report's entities posted past the closed-through date, or a
  line on one, written since; or the mapping touched (an association added,
  removed or re-pointed bumps its structure). Landed rows are never deleted
  or re-dated — a correction is a reversal, which is a new row — and a
  draft is not in the pivot, so a draft's deletion changes nothing."""
  if report_def.last_generated is None:
    return True
  since = _naive_utc(report_def.last_generated)
  moved = session.execute(
    text(
      "SELECT 1 FROM entries e "
      "WHERE e.entity_id = ANY(:entities) AND e.posting_date > :cutoff "
      "  AND e.updated_at > :since "
      "UNION ALL "
      "SELECT 1 FROM line_items li JOIN entries e ON e.id = li.entry_id "
      "WHERE e.entity_id = ANY(:entities) AND e.posting_date > :cutoff "
      "  AND li.updated_at > :since "
      "LIMIT 1"
    ),
    {
      "entities": _ledger_entities(session, entity_id),
      "cutoff": cutoff or date.min,
      "since": since,
    },
  ).first()
  if moved is not None:
    return True
  if not report_def.mapping_id:
    return False
  touched = session.execute(
    text(
      "SELECT 1 FROM structures WHERE id = :mapping AND updated_at > :since "
      "UNION ALL "
      "SELECT 1 FROM associations "
      "WHERE structure_id = :mapping AND created_at > :since "
      "LIMIT 1"
    ),
    {"mapping": report_def.mapping_id, "since": since},
  ).first()
  return touched is not None


def _statement_sets(session: Session, report_id: str) -> dict[str, str]:
  """structure_id → fact_set_id of the report's pivoted statement sets. Its
  text-block snapshots are document sets, and left alone."""
  rows = session.execute(
    text(
      "SELECT structure_id, id FROM fact_sets "
      "WHERE report_id = :rid AND factset_type = 'report' "
      "  AND structure_id IS NOT NULL AND provenance->>'origin' = 'pivot'"
    ),
    {"rid": report_id},
  ).all()
  return {str(row.structure_id): str(row.id) for row in rows}


def _frozen_windows(
  session: Session, set_ids: list[str]
) -> set[tuple[date | None, date]]:
  if not set_ids:
    return set()
  rows = session.execute(
    text(
      "SELECT DISTINCT period_start, period_end FROM facts "
      "WHERE fact_set_id = ANY(:ids) AND frozen_at IS NOT NULL"
    ),
    {"ids": set_ids},
  ).all()
  return {(row.period_start, row.period_end) for row in rows}


def _has_due_unfrozen(
  session: Session, set_ids: list[str], cutoff: date | None
) -> bool:
  if cutoff is None or not set_ids:
    return False
  found = session.execute(
    text(
      "SELECT 1 FROM facts WHERE fact_set_id = ANY(:ids) "
      "  AND frozen_at IS NULL AND period_end <= :cutoff LIMIT 1"
    ),
    {"ids": set_ids, "cutoff": cutoff},
  ).first()
  return found is not None


def refresh_report_columns(
  session: Session,
  report_id: str,
  *,
  actor_id: str,
  force: bool = False,
  prune: bool = False,
) -> RefreshResult:
  """Bring a report's live columns up to the books and freeze the columns
  the calendar has closed.

  Nothing happens for a shared copy, a filed or archived report, one not yet
  generated, or when nothing landed since the last generation and no column
  is due to freeze. ``force`` rewrites the live columns regardless; ``prune``
  (a regenerate whose columns changed) also drops the frozen facts of windows
  no column states any more. A frozen column that is still a column is never
  touched.
  """
  from robosystems.operations.locking import lock_by_id

  report_def = session.get(Report, report_id)
  if report_def is None or not refreshable(report_def, forced=force):
    return RefreshResult(rewritten=False)
  entity_id = report_entity(session, report_def)
  cutoff = closed_through_end(session, entity_id)
  sets = _statement_sets(session, report_id)
  periods = report_columns(report_def)
  has_live = any(cutoff is None or p.end > cutoff for p in periods)
  if not (
    force
    or _has_due_unfrozen(session, list(sets.values()), cutoff)
    or (has_live and live_columns_stale(session, report_def, entity_id, cutoff))
  ):
    return RefreshResult(rewritten=False)

  # Locked against a concurrent refresh, regenerate or filing.
  report_def = lock_by_id(
    session,
    Report,
    report_id,
    f"Report {report_id} is being written by another process. Retry in a moment.",
  )
  if report_def is None or not refreshable(report_def, forced=force):
    return RefreshResult(rewritten=False)

  now = datetime.now(UTC)
  facts, reporting_style_id = generate_entity_report_facts(
    session,
    taxonomy_id=report_def.taxonomy_id,
    mapping_id=report_def.mapping_id or "",
    periods=periods,
    entity_id=entity_id,
  )
  element_to_structures, picked = _build_structure_mapping(
    session,
    reporting_style_id,
    fact_element_ids={f.element_id for f in facts.facts},
    taxonomy_id=report_def.taxonomy_id,
  )
  # A structure keeps its set: frozen facts live there, and every reader of
  # the report holds its id. A structure picked for the first time gets one.
  structure_to_factset = {sid: sets.get(sid, fresh) for sid, fresh in picked.items()}
  new_sets = {
    sid: fsid for sid, fsid in structure_to_factset.items() if sid not in sets
  }
  if new_sets:
    _pre_create_report_fact_sets(
      session,
      report_id,
      entity_id,
      actor_id,
      periods,
      new_sets,
      report_def.mapping_id or "",
    )
  # Every set the report had is cleared of live rows, not only the ones
  # picked this time: a structure the style no longer composes must not keep
  # serving last generation's numbers.
  set_ids = list(dict.fromkeys([*sets.values(), *structure_to_factset.values()]))
  session.execute(
    text("DELETE FROM verification_results WHERE fact_set_id = ANY(:ids)"),
    {"ids": set_ids},
  )
  if prune:
    # The columns changed: the sets' envelope follows them, and the frozen
    # facts of a window no column states any more go.
    envelope_start = min(p.start for p in periods)
    envelope_end = max(p.end for p in periods)
    session.execute(
      text(
        "UPDATE fact_sets SET period_start = :start, period_end = :end, "
        "provenance = jsonb_set(provenance, '{period}', to_jsonb(CAST(:key AS text))) "
        "WHERE id = ANY(:ids)"
      ),
      {
        "start": envelope_start,
        "end": envelope_end,
        "key": f"{envelope_start}/{envelope_end}",
        "ids": set_ids,
      },
    )
    columns = {(p.start, p.end) for p in periods}
    for start, end in _frozen_windows(session, set_ids) - columns:
      session.execute(
        text(
          "DELETE FROM facts WHERE fact_set_id = ANY(:ids) AND frozen_at IS NOT NULL "
          "  AND period_start IS NOT DISTINCT FROM :start AND period_end = :end"
        ),
        {"ids": set_ids, "start": start, "end": end},
      )
  session.execute(
    text("DELETE FROM facts WHERE fact_set_id = ANY(:ids) AND frozen_at IS NULL"),
    {"ids": set_ids},
  )
  frozen = _frozen_windows(session, set_ids)
  live = replace(
    facts,
    facts=[f for f in facts.facts if (f.period_start, f.period_end) not in frozen],
  )
  _stamp_facts_into_sets(
    session,
    live,
    entity_id,
    element_to_structures,
    structure_to_factset,
    mapping_id=report_def.mapping_id or None,
    frozen_at=now,
    freeze_through=cutoff,
  )
  summary = _evaluate_report_structures(
    session,
    facts,
    element_to_structures,
    structure_to_factset,
    report_def.period_start,
    report_def.period_end,
    actor_id,
  )
  # A set no structure fills any more goes once nothing frozen is left in it;
  # one holding a frozen column stays, as that column's record.
  abandoned = [fsid for sid, fsid in sets.items() if sid not in structure_to_factset]
  if abandoned:
    session.execute(
      text(
        "DELETE FROM fact_sets WHERE id = ANY(:ids) "
        "AND NOT EXISTS (SELECT 1 FROM facts WHERE facts.fact_set_id = fact_sets.id)"
      ),
      {"ids": abandoned},
    )
  report_def.last_generated = now
  session.flush()
  return RefreshResult(rewritten=True, rule_summary=summary)


def refresh_for_read(session: Session, report_id: str) -> None:
  """A read's refresh: on its own savepoint, so a refresh that fails midway
  leaves the cached rows as they were and the read serves them; the failure
  is logged, never the reader's."""
  from robosystems.operations.locking import RowLockedError

  try:
    with session.begin_nested():
      refresh_report_columns(session, report_id, actor_id="system")
  except RowLockedError:
    # Another read or write is refreshing it now; this one serves the rows.
    logger.debug(f"Report {report_id} was read as last generated: being written.")
  except Exception as exc:
    logger.warning(
      f"Report {report_id} was read as last generated; its refresh failed: {exc}"
    )


def freeze_reports_for_close(session: Session, *, entity_id: str, actor_id: str) -> int:
  """After a close: every report of ``entity_id`` is brought up to the books
  and the columns the month completed are frozen there. Each report on its
  own savepoint: one that cannot be is logged and left for its next read,
  and the close stands. Returns how many were rewritten."""
  # A report from before the entity column has none; its sets name the
  # entity, and ``report_entity`` resolves it the way its generation did.
  rows = session.execute(
    text(
      "SELECT id FROM reports "
      "WHERE (entity_id = :entity OR entity_id IS NULL) "
      "  AND source_report_id IS NULL "
      "  AND filing_status NOT IN ('filed', 'archived') "
      "  AND generation_status IN ('complete', 'published')"
    ),
    {"entity": entity_id},
  ).all()
  rewritten = 0
  for row in rows:
    report_id = str(row.id)
    try:
      with session.begin_nested():
        report_def = session.get(Report, report_id)
        if report_def is None or report_entity(session, report_def) != entity_id:
          continue
        if refresh_report_columns(session, report_id, actor_id=actor_id).rewritten:
          rewritten += 1
    except Exception as exc:
      logger.warning(
        f"Report {report_id} was not frozen at the close of entity {entity_id}; "
        f"its next read will: {exc}"
      )
  return rewritten


def unfreeze_reports_after_reopen(session: Session, *, entity_id: str) -> int:
  """After a reopen: the entity's report columns whose period is no longer
  fully closed go back to live. Returns the facts unfrozen. Scoped by the
  sets' entity, which every report's sets carry, a legacy report with no
  ``entity_id`` of its own included."""
  cutoff = closed_through_end(session, entity_id)
  result = session.execute(
    text(
      "UPDATE facts SET frozen_at = NULL "
      "WHERE frozen_at IS NOT NULL AND period_end > :cutoff "
      "  AND fact_set_id IN ("
      "    SELECT fs.id FROM fact_sets fs JOIN reports r ON r.id = fs.report_id "
      "    WHERE fs.entity_id = :entity AND r.source_report_id IS NULL "
      "      AND r.filing_status NOT IN ('filed', 'archived'))"
    ),
    {"cutoff": cutoff or date.min, "entity": entity_id},
  )
  return result.rowcount
