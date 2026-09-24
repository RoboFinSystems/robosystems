"""Shared rs-gaap calculation-DAG loading + bottom-up subtotal resolution.

The fact producer (``fact_grid``) and the rollup validator
(``information_block.rules``) both resolve subtotals here, so they can never
disagree.

- children come from the ``rs-gaap-calculations`` calculation arcs;
- a direct fact wins over the calc sum, keyed on presence, so a legitimately
  zero fact is not overwritten;
- an absent summand contributes 0;
- targets resolve in topological order, so chained subtotals work.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session


def load_rs_gaap_calculations(
  session: Session,
) -> dict[str, list[tuple[str, float]]]:
  """Load the rs-gaap-calculations DAG as ``parent_element_id → [(child_id, weight)]``.

  Global to the standard, not scoped to any presentation structure.
  """
  rows = session.execute(
    text("""
      SELECT a.from_element_id AS parent, a.to_element_id AS child, a.weight
      FROM associations a
      JOIN structures s ON s.id = a.structure_id
      JOIN taxonomies t ON t.id = s.taxonomy_id
      WHERE a.association_type = 'calculation'
        AND t.standard = 'rs-gaap-calculations'
      ORDER BY a.order_value
    """)
  ).fetchall()
  calculations: dict[str, list[tuple[str, float]]] = {}
  seen: set[tuple[str, str]] = set()
  for r in rows:
    # A pair arced in two calc structures would double-count the child.
    if (r.parent, r.child) in seen:
      continue
    seen.add((r.parent, r.child))
    weight = float(r.weight) if r.weight is not None else 1.0
    calculations.setdefault(r.parent, []).append((r.child, weight))
  return calculations


def merge_calculations(
  global_calcs: dict[str, list[tuple[str, float]]],
  local_calcs: dict[str, list[tuple[str, float]]],
) -> dict[str, list[tuple[str, float]]]:
  """Merged DAG for evaluating one structure: LOCAL arcs win per parent.

  A note decomposing ``rs-gaap:Revenues`` into its own members must foot
  against those, not the global children absent from its FactSet. Neither
  input is mutated.
  """
  merged = dict(global_calcs)
  merged.update(local_calcs)
  return merged


def topo_sort_calculations(
  calculations: dict[str, list[tuple[str, float]]],
) -> list[str]:
  """Return calc subtotal targets in topological dependency order."""
  targets = set(calculations.keys())
  deps: dict[str, set[str]] = {t: set() for t in targets}
  for target, sources in calculations.items():
    for src_id, _ in sources:
      if src_id in targets:
        deps[target].add(src_id)

  # Kahn's algorithm.
  ready = [t for t, d in deps.items() if not d]
  ordered: list[str] = []
  while ready:
    n = ready.pop(0)
    ordered.append(n)
    for other, other_deps in deps.items():
      if n in other_deps:
        other_deps.discard(n)
        if not other_deps and other not in ordered and other not in ready:
          ready.append(other)
  # Leftovers are a cycle (rejected upstream); degrade rather than crash.
  for t in targets:
    if t not in ordered:
      ordered.append(t)
  return ordered


def resolve_calc_dag(
  balances: dict[str, float],
  present: set[str],
  calculations: dict[str, list[tuple[str, float]]],
  order: list[str] | None = None,
) -> dict[str, float]:
  """Resolve every calc target bottom-up: direct fact wins, else Σ child·weight.

  ``present`` is the element_ids with a direct fact. Returns ``balances`` plus
  every computed subtotal. Pass ``order`` to avoid re-sorting per period.
  """
  if order is None:
    order = topo_sort_calculations(calculations)
  computed: dict[str, float] = dict(balances)
  for elem_id in order:
    direct = computed.get(elem_id, 0.0)
    summed = sum(computed.get(src, 0.0) * w for src, w in calculations.get(elem_id, ()))
    computed[elem_id] = direct if elem_id in present else summed
  return computed
