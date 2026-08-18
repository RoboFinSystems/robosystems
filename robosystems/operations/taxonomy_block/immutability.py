"""What curation may never destroy: filed snapshots and closed-month history.

Two producers write statement FactSets that the rest of the ledger treats as
immutable — a Report that has been *filed* keeps its publication snapshot
(``fact_sets.report_id`` set, ``reports.filing_status`` in filed/archived),
and close mints the *canonical* sets for a month (``report_id IS NULL``,
``factset_type='report'``, ``scenario_id IS NULL``) which only reopen may
retract. ``regenerate_report`` and ``delete_report`` refuse a filed report;
reopen is the only path to a closed month's canonical sets.

The taxonomy-block cascade (``delete-taxonomy-block cascade_facts=true``,
``update-taxonomy-block structures_to_remove``) deletes facts by element and
by structure, and reached both kinds of set with no guard. This module is the
single check both paths run before deleting anything.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.extensions import FactSet, Report
from robosystems.models.extensions.roboledger.fact import Fact
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod

_FILED_STATUSES = ("filed", "archived")


class ProtectedFactsError(ValueError):
  """Curation would delete facts the ledger treats as immutable."""

  def __init__(self, *, filed_report_count: int, closed_period_count: int) -> None:
    self.filed_report_count = filed_report_count
    self.closed_period_count = closed_period_count
    parts = []
    if filed_report_count:
      parts.append(
        f"{filed_report_count} statement set(s) belong to filed reports "
        "(delete or un-file the report first — its snapshot is immutable)"
      )
    if closed_period_count:
      parts.append(
        f"{closed_period_count} canonical statement set(s) belong to closed "
        "periods (reopen the period first — closed history is immutable)"
      )
    super().__init__(
      "refusing to delete facts that the ledger treats as immutable: "
      + "; ".join(parts)
    )


@dataclass(frozen=True)
class ProtectedFactSets:
  filed_report_fact_set_ids: tuple[str, ...]
  closed_period_fact_set_ids: tuple[str, ...]

  @property
  def any(self) -> bool:
    return bool(self.filed_report_fact_set_ids or self.closed_period_fact_set_ids)


def _affected_fact_set_ids(
  session: Session,
  *,
  structure_ids: Sequence[str],
  element_ids: Sequence[str],
) -> list[str]:
  """FactSets a cascade over these structures/elements would touch.

  Facts die two ways — by referencing an element being deleted, and by
  membership in a set attached to (or facts stamped with) a structure being
  deleted — so the affected sets are the union of both routes.
  """
  predicates = []
  if structure_ids:
    predicates.append(FactSet.structure_id.in_(structure_ids))
    predicates.append(
      FactSet.id.in_(
        select(Fact.fact_set_id).where(Fact.structure_id.in_(structure_ids))
      )
    )
  if element_ids:
    predicates.append(
      FactSet.id.in_(select(Fact.fact_set_id).where(Fact.element_id.in_(element_ids)))
    )
  if not predicates:
    return []
  clause = predicates[0]
  for pred in predicates[1:]:
    clause = clause | pred
  return list(session.execute(select(FactSet.id).where(clause)).scalars().all())


def find_protected_fact_sets(
  session: Session,
  *,
  structure_ids: Sequence[str] = (),
  element_ids: Sequence[str] = (),
) -> ProtectedFactSets:
  """Which of the sets a cascade would delete are immutable."""
  affected = _affected_fact_set_ids(
    session, structure_ids=structure_ids, element_ids=element_ids
  )
  if not affected:
    return ProtectedFactSets((), ())

  filed = (
    session.execute(
      select(FactSet.id)
      .join(Report, Report.id == FactSet.report_id)
      .where(FactSet.id.in_(affected), Report.filing_status.in_(_FILED_STATUSES))
    )
    .scalars()
    .all()
  )

  closed_period = (
    select(FiscalPeriod.id)
    .where(
      FiscalPeriod.status == "closed",
      FiscalPeriod.start_date <= FactSet.period_start,
      FiscalPeriod.end_date >= FactSet.period_end,
    )
    .exists()
  )
  closed = (
    session.execute(
      select(FactSet.id).where(
        FactSet.id.in_(affected),
        FactSet.report_id.is_(None),
        FactSet.scenario_id.is_(None),
        FactSet.factset_type == "report",
        FactSet.period_start.is_not(None),
        closed_period,
      )
    )
    .scalars()
    .all()
  )
  return ProtectedFactSets(tuple(filed), tuple(closed))


def assert_facts_deletable(
  session: Session,
  *,
  structure_ids: Sequence[str] = (),
  element_ids: Sequence[str] = (),
) -> None:
  """Raise ``ProtectedFactsError`` if the cascade would reach an immutable set."""
  protected = find_protected_fact_sets(
    session, structure_ids=structure_ids, element_ids=element_ids
  )
  if protected.any:
    raise ProtectedFactsError(
      filed_report_count=len(protected.filed_report_fact_set_ids),
      closed_period_count=len(protected.closed_period_fact_set_ids),
    )


__all__ = [
  "ProtectedFactSets",
  "ProtectedFactsError",
  "assert_facts_deletable",
  "find_protected_fact_sets",
]
