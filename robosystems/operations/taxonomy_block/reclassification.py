"""A mapping change that moves an account's line, not the month's totals.

The closed-period guard refuses a mapping arc for an account with landed
history in a closed month, because the month's stamped statements were
computed through the arcs as they stood. A remap that keeps the account on
the same primary statement preserves every total the close asserted (net
income, total assets, liabilities, equity, the change in cash): it is a
reclassification, applied to the comparatives and disclosed, not a
restatement. This module judges which one a remap is. Either way the
stamped sets keep the answer as closed; a live render follows the new arc.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from robosystems.operations.roboledger.reports.network_picker import (
  DEFAULT_STYLE_ID,
  load_primary_reporting_style,
)
from robosystems.operations.taxonomy_block.immutability import (
  ProtectedFactsError,
  ProtectedFactSets,
  find_protected_fact_sets,
)

# The statements whose totals a close asserts and a remap must leave alone.
PRIMARY_STATEMENT_TYPES: tuple[str, ...] = (
  "balance_sheet",
  "income_statement",
  "comprehensive_income",
)

_STATEMENT_LABELS = {
  "balance_sheet": "balance sheet",
  "income_statement": "income statement",
  "comprehensive_income": "statement of comprehensive income",
}

# Synthesized detail (PP&E gross, accumulated depreciation) is not presented
# itself; the renderer absorbs it into a calculation parent that is.
_CALCULATION_HOPS = 3


class ReclassificationRefusedError(ProtectedFactsError):
  """The remap would restate the closed months rather than reclassify them."""

  def __init__(self, protected: ProtectedFactSets, note: str) -> None:
    self.protected = protected
    base = protected.to_error(note=note)
    super().__init__(
      filed_report_count=base.filed_report_count,
      closed_period_count=base.closed_period_count,
      disturbed_count=base.disturbed_count,
      closed_period_names=base.closed_period_names,
      note=note,
    )


@dataclass(frozen=True)
class Reclassification:
  """A remap judged safe: the statement it stays on, and the closed months
  whose live render now differs from their stamp."""

  statement_type: str
  closed_periods: tuple[str, ...]


def _presented_on(
  session: Session, reporting_style_id: str, element_ids: Sequence[str]
) -> dict[str, set[str]]:
  """Each concept's primary statements under the style, by the presentation
  arcs of the networks the style composes."""
  if not element_ids:
    return {}
  rows = session.execute(
    text("""
      SELECT a.to_element_id AS element_id, rsn.statement_type
      FROM reporting_style_networks rsn
      JOIN associations a ON a.structure_id = rsn.network_id
      WHERE rsn.reporting_style_id = :rsid
        AND rsn.statement_type = ANY(:statement_types)
        AND a.association_type = 'presentation'
        AND a.to_element_id = ANY(:ids)
    """),
    {
      "rsid": reporting_style_id,
      "statement_types": list(PRIMARY_STATEMENT_TYPES),
      "ids": list(element_ids),
    },
  ).fetchall()
  placed: dict[str, set[str]] = {}
  for row in rows:
    placed.setdefault(str(row.element_id), set()).add(str(row.statement_type))
  return placed


def _calculation_parents(session: Session) -> dict[str, list[str]]:
  from robosystems.operations.roboledger.reports.calc_dag import (
    load_rs_gaap_calculations,
  )

  parents: dict[str, list[str]] = {}
  for parent, children in load_rs_gaap_calculations(session).items():
    for child, _weight in children:
      parents.setdefault(str(child), []).append(str(parent))
  return parents


def statement_of_concepts(
  session: Session, reporting_style_id: str, element_ids: Sequence[str]
) -> dict[str, str | None]:
  """The primary statement each concept renders on under the style: the
  network it is presented in, or, for a concept the renderer absorbs into a
  calculation parent, that parent's. ``None`` for a concept on no primary
  statement or on more than one."""
  ids = sorted(set(element_ids))
  if not ids:
    return {}
  placed = _presented_on(session, reporting_style_id, ids)
  frontier = {i: {i} for i in ids if i not in placed}
  if frontier:
    parents_of = _calculation_parents(session)
    for _ in range(_CALCULATION_HOPS):
      ancestors = {
        p
        for concepts in frontier.values()
        for c in concepts
        for p in parents_of.get(c, ())
      }
      if not ancestors:
        break
      found = _presented_on(session, reporting_style_id, sorted(ancestors))
      next_frontier: dict[str, set[str]] = {}
      for concept, current in frontier.items():
        parents = {p for c in current for p in parents_of.get(c, ())}
        statements = {s for p in parents for s in found.get(p, ())}
        if statements:
          placed[concept] = statements
        elif parents:
          next_frontier[concept] = parents
      frontier = next_frontier
      if not frontier:
        break
  return {
    i: next(iter(statements)) if len(statements := placed.get(i, set())) == 1 else None
    for i in ids
  }


def reporting_style_for_account(session: Session, account_taxonomy_id: str) -> str:
  """The Reporting Style of the entity whose chart holds the account, else
  the group parent's, else the corporate default."""
  row = session.execute(
    text("""
      SELECT e.reporting_style_id
      FROM entity_taxonomies et
      JOIN entities e ON e.id = et.entity_id
      WHERE et.taxonomy_id = :tid AND et.basis = 'chart_of_accounts'
      ORDER BY et.is_primary DESC
      LIMIT 1
    """),
    {"tid": account_taxonomy_id},
  ).fetchone()
  if row is not None:
    return str(row.reporting_style_id) if row.reporting_style_id else DEFAULT_STYLE_ID
  try:
    return load_primary_reporting_style(session)
  except LookupError:
    return DEFAULT_STYLE_ID


def _why_refused(account: str, before: str | None, after: str | None) -> str:
  if before is None or after is None:
    side = "current" if before is None else "new"
    return (
      f"{account} cannot be reclassified in place: its {side} concept is on "
      "no primary statement of this company's reporting style, so the closed "
      "months' totals cannot be shown unchanged."
    )
  return (
    f"Moving {account} from the {_STATEMENT_LABELS.get(before, before)} to the "
    f"{_STATEMENT_LABELS.get(after, after)} changes net income and every later "
    "carry-in: a restatement, not a reclassification."
  )


def judge_remap(
  session: Session,
  *,
  account_id: str,
  account_taxonomy_id: str,
  account_qname: str | None,
  old_target_id: str,
  new_target_id: str,
) -> Reclassification | None:
  """Whether moving the account's mapping arc from ``old_target_id`` to
  ``new_target_id`` is a reclassification. ``None`` when the account has no
  landed history in a closed month (an ordinary write). Raises
  :class:`ReclassificationRefusedError` when the move crosses a primary
  statement, or either concept sits on none, so a closed month's totals
  would change."""
  protected = find_protected_fact_sets(session, account_ids=[account_id])
  if not protected.any:
    return None
  style = reporting_style_for_account(session, account_taxonomy_id)
  placed = statement_of_concepts(session, style, [old_target_id, new_target_id])
  before, after = placed.get(old_target_id), placed.get(new_target_id)
  if before is not None and before == after:
    return Reclassification(
      statement_type=before, closed_periods=protected.closed_period_names
    )
  raise ReclassificationRefusedError(
    protected, _why_refused(account_qname or account_id, before, after)
  )


def reclassification_record(
  reclassified: Reclassification,
  *,
  replaces_association_id: str,
  previous_target_qname: str | None,
  reason: str | None,
) -> dict[str, Any]:
  """What the new arc keeps of the move: the arc it replaced, the concept
  the account came from, the statement it stayed on, the closed months it
  reaches and why. The row's own ``created_by`` / ``created_at`` say who
  and when."""
  return {
    "reclassification": {
      "replaces": replaces_association_id,
      "from": previous_target_qname,
      "statement_type": reclassified.statement_type,
      "closed_periods": list(reclassified.closed_periods),
      "reason": reason,
    }
  }


__all__ = [
  "PRIMARY_STATEMENT_TYPES",
  "Reclassification",
  "ReclassificationRefusedError",
  "judge_remap",
  "reclassification_record",
  "reporting_style_for_account",
  "statement_of_concepts",
]
