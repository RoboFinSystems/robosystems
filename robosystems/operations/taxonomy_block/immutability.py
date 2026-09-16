"""What curation may never destroy or disturb: filed snapshots and closed-month history.

Two producers write statement FactSets that the rest of the ledger treats as
immutable — a Report that has been *filed* keeps its publication snapshot
(``fact_sets.report_id`` set, ``reports.filing_status`` in filed/archived),
and close mints the *canonical* sets for a month (``report_id IS NULL``,
``factset_type='report'``, ``scenario_id IS NULL``) which only reopen may
retract. ``regenerate_report`` and ``delete_report`` refuse a filed report;
reopen is the only path to a closed month's canonical sets.

Curation reaches those sets two ways, and this module is the single check
for both:

- **Destroy.** The taxonomy-block cascade (``delete-taxonomy-block
  cascade_facts=true``, ``update-taxonomy-block structures_to_remove``)
  deletes facts by element and by structure, so it can reach both kinds of
  set. Both populations are protected.
- **Disturb.** A canonical stamp freezes each statement line's amount, but
  every read rebuilds the row set, hierarchy, classification, sign and
  rollups from the live mapping arcs and element attributes
  (``reports/fact_grid.py``). Re-mapping an account that has landed history
  in a closed month, or changing such an account's ``balance_type`` /
  ``period_type`` / EFS trait, changes what the stamped month *means* while
  the stamp keeps the old answer — two authoritative-looking statements for
  one closed month, with no reopen and no audit row. Only the closed-month
  canonical population is protected against this: a filed report is a
  snapshot by contract and is expected to drift from the live render.

History reaches forward, so the disturb predicate is ``posting_date <=
period_end``, not within-window: an instant balance carries every earlier
posting, and a P&L→balance-sheet remap moves net income into every later
retained-earnings carry-in. A P&L→P&L remap of an account with no activity
in a later closed month is refused with it; the message names the earliest
month so the operator knows how far back to reopen.

Neither check takes the period fence. A close that stamps a month between
this read and the curation's write is a race the fence would close; it
needs a cascade delete to land inside a close window and has never been
observed. The ledger README carries it as the un-park trigger.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from sqlalchemy import and_, exists, or_, select
from sqlalchemy.orm import Session

from robosystems.models.extensions import FactSet, Report
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.fact import Fact
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.operations.roboledger.entry_status import LANDED_ENTRY_STATUSES

_FILED_STATUSES = ("filed", "archived")


class ProtectedFactsError(ValueError):
  """Curation would destroy or disturb facts the ledger treats as immutable."""

  def __init__(
    self,
    *,
    filed_report_count: int,
    closed_period_count: int,
    disturbed_count: int = 0,
    closed_period_names: Sequence[str] = (),
  ) -> None:
    self.filed_report_count = filed_report_count
    self.closed_period_count = closed_period_count
    self.disturbed_count = disturbed_count
    self.closed_period_names = tuple(closed_period_names)
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
    if disturbed_count:
      span = _period_span(self.closed_period_names)
      parts.append(
        f"{disturbed_count} canonical statement set(s) of closed period(s) "
        f"{span} were computed through the mapping arcs or element attributes "
        "this change alters (reopen latest-first down to "
        f"{self.closed_period_names[0] if self.closed_period_names else 'the earliest'}"
        " — closed history is immutable)"
      )
    verb = (
      "disturb"
      if disturbed_count and not (filed_report_count or closed_period_count)
      else "delete"
    )
    super().__init__(
      f"refusing to {verb} facts that the ledger treats as immutable: "
      + "; ".join(parts)
    )


def _period_span(names: Sequence[str]) -> str:
  if not names:
    return "(unknown)"
  if len(names) == 1:
    return names[0]
  return f"{names[0]}–{names[-1]}"


@dataclass(frozen=True)
class ProtectedFactSets:
  filed_report_fact_set_ids: tuple[str, ...]
  closed_period_fact_set_ids: tuple[str, ...]
  disturbed_fact_set_ids: tuple[str, ...] = ()
  closed_period_names: tuple[str, ...] = ()

  @property
  def any(self) -> bool:
    return bool(
      self.filed_report_fact_set_ids
      or self.closed_period_fact_set_ids
      or self.disturbed_fact_set_ids
    )

  def to_error(self) -> ProtectedFactsError:
    return ProtectedFactsError(
      filed_report_count=len(self.filed_report_fact_set_ids),
      closed_period_count=len(self.closed_period_fact_set_ids),
      disturbed_count=len(self.disturbed_fact_set_ids),
      closed_period_names=self.closed_period_names,
    )


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
  return list(session.execute(select(FactSet.id).where(or_(*predicates))).scalars())


def _closed_canonical_predicates():
  """The close-minted population: canonical sets whose window a closed period covers."""
  closed_period = (
    select(FiscalPeriod.id)
    .where(
      FiscalPeriod.status == "closed",
      FiscalPeriod.start_date <= FactSet.period_start,
      FiscalPeriod.end_date >= FactSet.period_end,
    )
    .exists()
  )
  return (
    FactSet.report_id.is_(None),
    FactSet.scenario_id.is_(None),
    FactSet.factset_type == "report",
    FactSet.period_start.is_not(None),
    closed_period,
  )


def _disturbed_fact_set_ids(
  session: Session,
  *,
  account_ids: Sequence[str],
  semantic_element_ids: Sequence[str],
) -> list[str]:
  """Closed canonical sets whose meaning this change would alter.

  Two roles an element can play in a stamp, checked together. As a
  *source* — a chart account the pivot reads through a mapping arc — it
  disturbs every closed month at or after its first landed posting. As a
  *target* — an element the stamp holds a fact on — a ``balance_type`` /
  ``period_type`` / trait change re-signs or re-places that fact at read
  time. ``account_ids`` are sources only; ``semantic_element_ids`` are
  checked in both roles, since the caller does not know which one an
  element plays.
  """
  sources = sorted({*account_ids, *semantic_element_ids})
  targets = sorted(set(semantic_element_ids))
  predicates = []
  if sources:
    predicates.append(
      exists(
        select(LineItem.id)
        .join(Entry, Entry.id == LineItem.entry_id)
        .where(
          LineItem.element_id.in_(sources),
          Entry.status.in_(sorted(LANDED_ENTRY_STATUSES)),
          Entry.posting_date <= FactSet.period_end,
        )
      )
    )
  if targets:
    predicates.append(
      FactSet.id.in_(select(Fact.fact_set_id).where(Fact.element_id.in_(targets)))
    )
  if not predicates:
    return []
  return list(
    session.execute(
      select(FactSet.id).where(*_closed_canonical_predicates(), or_(*predicates))
    ).scalars()
  )


def _closed_period_names(
  session: Session, fact_set_ids: Sequence[str]
) -> tuple[str, ...]:
  """The closed periods covering these sets, oldest first, for the refusal."""
  if not fact_set_ids:
    return ()
  rows = session.execute(
    select(FiscalPeriod.name)
    .distinct()
    .join(
      FactSet,
      and_(
        FiscalPeriod.start_date <= FactSet.period_start,
        FiscalPeriod.end_date >= FactSet.period_end,
      ),
    )
    .where(FiscalPeriod.status == "closed", FactSet.id.in_(fact_set_ids))
  ).scalars()
  return tuple(sorted(str(name) for name in rows))


def find_protected_fact_sets(
  session: Session,
  *,
  structure_ids: Sequence[str] = (),
  element_ids: Sequence[str] = (),
  account_ids: Sequence[str] = (),
  semantic_element_ids: Sequence[str] = (),
) -> ProtectedFactSets:
  """Which immutable sets a curation change would destroy or disturb.

  ``structure_ids`` / ``element_ids`` describe a cascade delete and reach
  both protected populations. ``account_ids`` (chart accounts whose mapping
  arc is added or removed) and ``semantic_element_ids`` (elements whose
  ``balance_type`` / ``period_type`` / trait actually changes) describe a
  disturbance and reach the closed-month canonical population only.
  """
  affected = _affected_fact_set_ids(
    session, structure_ids=structure_ids, element_ids=element_ids
  )
  filed: list[str] = []
  closed: list[str] = []
  if affected:
    filed = list(
      session.execute(
        select(FactSet.id)
        .join(Report, Report.id == FactSet.report_id)
        .where(FactSet.id.in_(affected), Report.filing_status.in_(_FILED_STATUSES))
      ).scalars()
    )
    closed = list(
      session.execute(
        select(FactSet.id).where(
          FactSet.id.in_(affected), *_closed_canonical_predicates()
        )
      ).scalars()
    )

  disturbed = _disturbed_fact_set_ids(
    session, account_ids=account_ids, semantic_element_ids=semantic_element_ids
  )
  names = _closed_period_names(session, [*closed, *disturbed])
  return ProtectedFactSets(tuple(filed), tuple(closed), tuple(disturbed), names)


def assert_history_undisturbed(
  session: Session,
  *,
  structure_ids: Sequence[str] = (),
  element_ids: Sequence[str] = (),
  account_ids: Sequence[str] = (),
  semantic_element_ids: Sequence[str] = (),
) -> None:
  """Raise ``ProtectedFactsError`` if the change would reach an immutable set."""
  protected = find_protected_fact_sets(
    session,
    structure_ids=structure_ids,
    element_ids=element_ids,
    account_ids=account_ids,
    semantic_element_ids=semantic_element_ids,
  )
  if protected.any:
    raise protected.to_error()


__all__ = [
  "ProtectedFactSets",
  "ProtectedFactsError",
  "assert_history_undisturbed",
  "find_protected_fact_sets",
]
