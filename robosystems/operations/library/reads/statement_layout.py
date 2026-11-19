"""A primary statement laid out from the library, for the concepts a report
gives values for.

The library holds each statement twice: a subtype tree that places every
concept, and a calculation network that says which lines sum to which. Laid
over the concepts a report reports, the two give its lines and its totals, so
whoever read the values out of a document does not also have to describe the
statement.

Reads ``public`` by name: a tenant's own copy of the library leaves out the
concepts its curation excludes, and a received report may use any of them.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.orm import Session

_PRESENTATION = (
  "http://www.xbrlsite.com/seattlemethod/conceptual-model/cm-roles/roles/"
  "rs-gaap-presentation/"
)
_CALCULATION = "https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-calculations/"

# Statement to (subtype tree, calculation network, the period type of its
# lines). The cash flow statement is left out on purpose: the library signs
# its lines by their effect on cash, which is not how a document prints them,
# so totals laid out from it would refuse statements that foot.
_NETWORKS: dict[str, tuple[str, str, str]] = {
  "balance_sheet": (
    f"{_PRESENTATION}rs-gaap-pres-bs",
    f"{_CALCULATION}BalanceSheet",
    "instant",
  ),
  "income_statement": (
    f"{_PRESENTATION}rs-gaap-pres-is",
    f"{_CALCULATION}IncomeStatement",
    "duration",
  ),
}

LAID_OUT_STATEMENTS = frozenset(_NETWORKS)


@dataclass(frozen=True)
class StatementLine:
  """One line: its concept, the reported line it sits under, and its weight
  in that line's total."""

  concept: str
  parent: str | None
  weight: float | None


@dataclass(frozen=True)
class LibraryConcept:
  """What the library says a concept is."""

  qname: str
  label: str
  monetary: bool
  period_type: str
  balance: str
  abstract: bool


def lay_out_statement(
  session: Session, block_type: str, reported: set[str]
) -> list[StatementLine]:
  """The lines of ``block_type`` among ``reported``, top to bottom.

  Each reported concept sits under its nearest reported ancestor, weighted by
  the arcs between them, so a subtotal the report leaves out does not break
  the total above it. Concepts the statement does not place are left out.
  Raises ``KeyError`` for a statement the library does not lay out.
  """
  tree_role, calc_role, period_type = _NETWORKS[block_type]
  rows = session.execute(
    text(
      """
      SELECT a.association_type, f.qname AS parent, t.qname AS child,
             a.weight, a.order_value,
             f.balance_type AS parent_balance, t.balance_type AS child_balance
      FROM public.associations a
      JOIN public.structures s ON s.id = a.structure_id
      JOIN public.elements f ON f.id = a.from_element_id
      JOIN public.elements t ON t.id = a.to_element_id
      WHERE s.metadata ->> 'role_uri' IN (:tree, :calc)
        AND f.period_type = :period AND t.period_type = :period
        AND NOT f.is_abstract AND NOT t.is_abstract
      """
    ),
    {"tree": tree_role, "calc": calc_role, "period": period_type},
  ).all()

  calculation: dict[tuple[str, str], tuple[float, float]] = {}
  balance: dict[str, str] = {}
  parents: dict[str, set[str]] = {}
  children: dict[str, set[str]] = {}
  for row in rows:
    balance[row.parent] = row.parent_balance
    balance[row.child] = row.child_balance
    parents.setdefault(row.child, set()).add(row.parent)
    children.setdefault(row.parent, set()).add(row.child)
    if row.association_type == "calculation" and row.weight is not None:
      calculation[(row.parent, row.child)] = (float(row.weight), row.order_value or 0.0)

  def weight(parent: str, child: str) -> float:
    if (parent, child) in calculation:
      return calculation[(parent, child)][0]
    # A subtype adds to its type, unless it carries the opposite balance.
    return 1.0 if balance[parent] == balance[child] else -1.0

  def ordered(parent: str, kids: set[str]) -> list[str]:
    return sorted(
      kids, key=lambda k: (calculation.get((parent, k), (0.0, float("inf")))[1], k)
    )

  # The statement is what hangs from its calculation network; the subtype
  # tree also carries stray roots that belong to no statement line.
  skeleton = {q for arc in calculation for q in arc}
  roots = sorted(q for q in skeleton if not parents.get(q, set()) & skeleton)
  sequence: dict[str, int] = {}
  stack = list(reversed(roots))
  while stack:
    concept = stack.pop()
    if concept in sequence:
      continue
    sequence[concept] = len(sequence)
    stack.extend(reversed(ordered(concept, children.get(concept, set()))))

  placed = reported & set(sequence)
  lines: list[StatementLine] = []
  for concept in sorted(placed, key=sequence.__getitem__):
    parent, line_weight = _nearest_reported(
      concept, placed, parents, calculation, weight
    )
    lines.append(StatementLine(concept, parent, line_weight))
  return lines


def _nearest_reported(
  concept: str,
  placed: set[str],
  parents: dict[str, set[str]],
  calculation: dict[tuple[str, str], tuple[float, float]],
  weight,
) -> tuple[str | None, float | None]:
  """The closest ancestor that is itself reported, and the product of the
  weights on the way up to it. A calculation arc is preferred at each step."""
  seen = {concept}
  level: list[tuple[str, float]] = [(concept, 1.0)]
  while level:
    above: list[tuple[str, float]] = []
    for node, product in level:
      for parent in sorted(
        parents.get(node, set()), key=lambda p: ((p, node) not in calculation, p)
      ):
        if parent in seen:
          continue
        seen.add(parent)
        above.append((parent, product * weight(parent, node)))
    for parent, product in above:
      if parent in placed:
        return parent, product
    level = above
  return None, None


def library_concepts(session: Session, qnames: set[str]) -> dict[str, LibraryConcept]:
  """The library's own description of each of ``qnames`` it holds."""
  if not qnames:
    return {}
  rows = session.execute(
    text(
      """
      SELECT e.qname, e.name, e.is_monetary, e.period_type, e.balance_type,
             e.is_abstract,
             (SELECT l.text FROM public.element_labels l
              WHERE l.element_id = e.id AND l.role = 'standard'
              ORDER BY l.id LIMIT 1) AS label
      FROM public.elements e
      WHERE e.qname = ANY(:qnames)
      """
    ),
    {"qnames": sorted(qnames)},
  ).all()
  return {
    row.qname: LibraryConcept(
      qname=row.qname,
      label=row.label or row.name,
      monetary=bool(row.is_monetary),
      period_type=row.period_type,
      balance=row.balance_type,
      abstract=bool(row.is_abstract),
    )
    for row in rows
  }
