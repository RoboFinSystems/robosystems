"""Shared rs-gaap calculation-DAG loading + bottom-up subtotal resolution.

The single source of truth for "resolve subtotal values over the
``rs-gaap-calculations`` DAG". Extracted from ``fact_grid.py`` so the fact
PRODUCER (``_emit_subtotal_facts``, ``_reconcile_operating_to_cash``) and the
rollup VALIDATOR (``information_block.rules``) derive subtotals *identically* —
the validator must mirror the producer or it re-derives the rollup wrong. The
frozen-enumeration ``RollUp`` evaluator did exactly that: it summed a hardcoded
child list bound one-fact-per-qname, so a subtotal footing over a sibling
concept (``IntangibleAssetsNetExcludingGoodwill`` vs the enumerated
``...IncludingGoodwill``) or over two facts in one period reported a false
failure. Deriving children from the arcs — the way the producer already does —
closes that class.

Resolution semantics (unchanged from the original inline copies in fact_grid):

- children come from ``association_type='calculation'`` arcs of the
  ``rs-gaap-calculations`` taxonomy standard (direct parent→child + weight);
- a DIRECT fact for an element wins over the calc sum (calc is the fallback for
  an un-reported subtotal, never an override), keyed on PRESENCE not a non-zero
  test so a legitimately-zero direct fact isn't overwritten by the calc sum;
- an absent summand contributes 0;
- targets are resolved in topological order, so a subtotal whose summand is
  itself a subtotal (e.g. ``...IncludingGoodwill`` = ``...ExcludingGoodwill`` +
  ``Goodwill``) is resolved from its own children first and collapses to the
  present leaf.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session


def load_rs_gaap_calculations(
  session: Session,
) -> dict[str, list[tuple[str, float]]]:
  """Load the rs-gaap-calculations DAG as ``parent_element_id → [(child_id, weight)]``.

  Global to the taxonomy standard (not scoped to any one report structure) —
  which is what makes subtotal resolution independent of which presentation
  structure is being rendered or validated. This is the same query the fact
  producer runs (``fact_grid._emit_subtotal_facts``).
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
  for r in rows:
    weight = float(r.weight) if r.weight is not None else 1.0
    calculations.setdefault(r.parent, []).append((r.child, weight))
  return calculations


def topo_sort_calculations(
  calculations: dict[str, list[tuple[str, float]]],
) -> list[str]:
  """Return calc subtotal targets in topological dependency order.

  When calcs chain (e.g., GrossProfit = Rev - COGS, then OperatingIncome =
  GrossProfit - OpEx, then NetIncome = OperatingIncome - Tax), resolution must
  compute them in order so each depends on the already-resolved values of the
  prior ones. Targets with no internal dependencies come first.
  """
  targets = set(calculations.keys())
  # Edge: target → target it depends on (when its summand is itself a calc
  # target). Inputs that are leaves (not calc targets) don't create edges.
  deps: dict[str, set[str]] = {t: set() for t in targets}
  for target, sources in calculations.items():
    for src_id, _ in sources:
      if src_id in targets:
        deps[target].add(src_id)

  # Kahn's algorithm: emit nodes with no remaining deps; remove from graph.
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
  # Any remaining targets indicate a cycle in the calc DAG — emit them last in
  # arbitrary order. (Cycles are rejected upstream at authoring/framework-build;
  # this degrades rather than crashes if one slips through.)
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

  ``balances`` maps ``element_id → summed fact value`` for the period; ``present``
  is the set of element_ids that have a direct fact (presence, not non-zero).
  Returns the resolved value map — a superset of ``balances`` that also carries
  each computed subtotal, so callers can read both a subtotal and the leaves
  under it. Pass a precomputed ``order`` to avoid re-sorting across periods.
  """
  if order is None:
    order = topo_sort_calculations(calculations)
  computed: dict[str, float] = dict(balances)
  for elem_id in order:
    direct = computed.get(elem_id, 0.0)
    summed = sum(computed.get(src, 0.0) * w for src, w in calculations.get(elem_id, ()))
    computed[elem_id] = direct if elem_id in present else summed
  return computed
