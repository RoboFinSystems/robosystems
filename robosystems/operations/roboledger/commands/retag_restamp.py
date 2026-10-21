"""A re-tag of a posted line is a correction to a closed month's statements.

A flow or dimension tag moves no balance, so re-tagging a posted line needs
no reopen, closed month included. It does change what that month's cash flow
and breakdowns present, so the month's stamped statements are re-stamped from
the books as they now stand, under the exclusive period fence: a close or a
reopen of the month cannot interleave, and two re-tags cannot each leave a
live set. The group's consolidated statements are re-stamped too where the
group has closed the month.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date

from sqlalchemy import text
from sqlalchemy.orm import Session

from robosystems.operations.locking import acquire_exclusive_period_fence_xact


def _ledger_graph_id(session: Session) -> str | None:
  row = session.execute(
    text(
      "SELECT graph_id FROM fiscal_calendar "
      "UNION ALL SELECT graph_id FROM fiscal_periods LIMIT 1"
    )
  ).first()
  return str(row.graph_id) if row is not None else None


def _months(dates: Iterable[date | None]) -> list[str]:
  return sorted({f"{d:%Y-%m}" for d in dates if d is not None})


def fence_retag_months(session: Session, posting_dates: Iterable[date | None]) -> None:
  """Take the exclusive transaction-scoped fence on each month the re-tagged
  rows post in, ascending. Before any row lock, as every writer fences."""
  graph_id = _ledger_graph_id(session)
  if graph_id is None:
    return
  for month in _months(posting_dates):
    acquire_exclusive_period_fence_xact(
      session,
      graph_id,
      month,
      detail=(
        f"Period {month} is being written, closed or reopened by another "
        "process. Retry in a moment."
      ),
    )


def restamp_retagged_months(
  session: Session,
  posting_dates: Iterable[date | None],
  *,
  entity_id: str,
  actor_id: str,
) -> list[str]:
  """Re-stamp the statements a close stamped for each of the months
  ``entity_id`` has closed, and the group's where its consolidation entity
  has. Returns the months re-stamped. Raises `StatementStampError` as a close does, which rolls
  the re-tag back with it."""
  from robosystems.operations.roboledger.commands._guards import closed_periods
  from robosystems.operations.roboledger.entity_scope import (
    find_consolidation_entity,
  )
  from robosystems.operations.roboledger.fiscal_calendar.periods import (
    period_date_range,
  )
  from robosystems.operations.roboledger.reports.statement_sets import (
    has_canonical_statement_sets,
    stamp_canonical_statement_sets,
  )

  dates = [d for d in posting_dates if d is not None]
  graph_id = _ledger_graph_id(session)
  if not dates or graph_id is None:
    return []
  scopes = [entity_id]
  consolidation = find_consolidation_entity(session)
  if consolidation is not None and str(consolidation.id) != entity_id:
    scopes.append(str(consolidation.id))

  restamped: list[str] = []
  for scope in scopes:
    for month, _first in closed_periods(session, dates, entity_id=scope):
      start, end = period_date_range(month)
      # Only what a close stamped: months the calendar was set up behind hold
      # none, and stamping them here would invent statements no close made.
      if not has_canonical_statement_sets(
        session, period_start=start, period_end=end, entity_id=scope
      ):
        continue
      stamp_canonical_statement_sets(
        session,
        graph_id=graph_id,
        period_start=start,
        period_end=end,
        actor_id=actor_id,
        entity_id=scope,
      )
      restamped.append(month)
  return sorted(set(restamped))


__all__ = ["fence_retag_months", "restamp_retagged_months"]
