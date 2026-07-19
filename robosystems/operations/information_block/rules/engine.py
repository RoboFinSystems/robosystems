"""Rule evaluation engine entry point.

``evaluate_rules_for_structure`` is the single public function. It:

1. Loads all rules scoped to the structure (via the existing
   ``load_rules_for_structure`` helper so element/association-scoped
   rules are included alongside structure-scoped ones).
2. For each rule: resolves ``$Variable`` → fact value bindings by
   qname lookup, dispatches to the per-pattern evaluator, and writes
   a :class:`~robosystems.models.extensions.VerificationResult` row.
3. Returns the written rows (ids assigned after ``session.flush()``).

The engine is side-effect-free from the caller's perspective — it
``session.add()``s rows and calls ``session.flush()`` to assign ids,
but leaves ``session.commit()`` to the OperationSpec wrapper (which
commits on success per the existing pattern).

Binding semantics
-----------------
For each ``{variable_name, variable_qname}`` entry in
``rule.rule_variables``:

1. Resolve ``variable_qname`` → ``element_id`` via elements table.
2. If the element is missing → bind ``variable_name`` to ``None``
   (the pattern evaluator surfaces this as ``skipped`` or ``fail``).
3. Resolve ``element_id`` + period window → fact value via the facts
   table (``fact_scope = 'in_scope'``, most recent ``period_end`` first).
   Facts are filtered by ``structure_id`` (or ``fact_set_id`` when the
   caller pins one); every fact has both stamped at write time, so no
   report-id fallback is needed. None on miss.

One query per variable (N+1 is fine for 3-5 variables per rule).
"""

from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from robosystems.models.extensions import (
  Association,
  Element,
  Rule,
  Structure,
  VerificationResult,
)
from robosystems.models.extensions.roboledger import Fact, FactSet
from robosystems.operations.information_block.envelope import load_rules_for_structure
from robosystems.operations.information_block.rules.evaluators import (
  EvaluationOutcome,
  evaluate_rule,
  rule_tolerance,
)


def _bind_variables(
  session: Session,
  rule: Rule,
  structure_id: str,
  period_start: date | None,
  period_end: date | None,
  fact_set_id: str | None = None,
) -> dict[str, float | None]:
  """Resolve each ``$Variable`` in a rule to a fact value or ``None``."""
  bindings: dict[str, float | None] = {}
  for var in rule.rule_variables or []:
    name = var.get("variable_name", "")
    qname = var.get("variable_qname", "")
    if not name:
      continue

    # Prefer an explicit element id when the rule carries one. Tenant CoA
    # elements have a null qname (they key on `code`), so qname resolution
    # can't bind them — schedule rules pass `variable_element_id` directly.
    element_id: str | None = (
      var.get("variable_element_id")
      or session.execute(
        select(Element.id).where(Element.qname == qname).limit(1)
      ).scalar()
    )

    if element_id is None:
      bindings[name] = None
      continue

    base_stmt = (
      select(Fact.value)
      .where(
        Fact.element_id == element_id,
        Fact.fact_scope == "in_scope",
      )
      .order_by(Fact.period_end.desc(), Fact.created_at.desc())
      .limit(1)
    )
    if period_end is not None:
      base_stmt = base_stmt.where(Fact.period_end <= period_end)
    if period_start is not None:
      base_stmt = base_stmt.where(Fact.period_start >= period_start)

    if fact_set_id is not None:
      stmt = base_stmt.where(Fact.fact_set_id == fact_set_id)
    else:
      stmt = base_stmt.where(Fact.structure_id == structure_id)
    bindings[name] = session.execute(stmt).scalar()

  return bindings


def _bind_sum_variables(
  session: Session,
  rule: Rule,
  structure_id: str,
) -> dict[str, float | None]:
  """Aggregate SUM of duration facts per variable — used exclusively by SumEquals rules."""
  bindings: dict[str, float | None] = {}
  for var in rule.rule_variables or []:
    name = var.get("variable_name", "")
    qname = var.get("variable_qname", "")
    if not name:
      continue
    # Prefer an explicit element id (CoA elements have null qname — see
    # _bind_variables); schedule SumEquals rules carry variable_element_id.
    element_id: str | None = (
      var.get("variable_element_id")
      or session.execute(
        select(Element.id).where(Element.qname == qname).limit(1)
      ).scalar()
    )
    if element_id is None:
      bindings[name] = None
      continue
    # SumEquals checks a structural invariant — Σ(periodic facts) ==
    # contracted total — so it must sum every periodic fact on the
    # structure regardless of `fact_scope`. Filtering to `in_scope`
    # would silently break schedules that bisect `closed_through`:
    # only the post-close tail would be summed, but `expected_total`
    # still encodes the full contract amount, producing spurious
    # failures. Schedule facts aren't replicated into GL line items,
    # so summing historical periods here doesn't double-count
    # anything that landed in opening balances.
    row = session.execute(
      text(
        "SELECT ROUND(SUM(value)::numeric, 2) AS total "
        "FROM facts "
        "WHERE element_id = :eid AND structure_id = :sid "
        "AND period_type = 'duration'"
      ),
      {"eid": element_id, "sid": structure_id},
    ).fetchone()
    bindings[name] = float(row.total) if row and row.total is not None else None
  return bindings


def _load_period_balances(
  session: Session,
  structure_id: str,
  fact_set_id: str | None,
  period_start: date | None,
  period_end: date | None,
) -> dict[tuple[date | None, date | None], tuple[dict[str, float], set[str]]]:
  """Group in-scope facts into per-period ``(balances, present)`` — SUMMING
  every fact for an element+period, the way the fact producer does (a single
  element can carry multiple facts in one period, e.g. a derived flow plus a
  reconciling plug), within the period window.

  Scoped to a single FactSet: pinned when ``fact_set_id`` is given, else the
  LATEST FactSet for the structure. The FactSet is the summing unit on purpose
  — two coexisting reports over the same period both stamp this structure, so a
  bare ``structure_id`` scope would sum across reports and double every balance.
  """
  if fact_set_id is None:
    fact_set_id = session.execute(
      select(FactSet.id)
      .where(FactSet.structure_id == structure_id)
      .order_by(FactSet.created_at.desc(), FactSet.id.desc())
      .limit(1)
    ).scalar()

  stmt = select(Fact.element_id, Fact.value, Fact.period_start, Fact.period_end).where(
    Fact.fact_scope == "in_scope"
  )
  if fact_set_id is not None:
    stmt = stmt.where(Fact.fact_set_id == fact_set_id)
  else:
    # No FactSet exists for this structure (never reported) — no balances.
    stmt = stmt.where(Fact.structure_id == structure_id)
  if period_end is not None:
    stmt = stmt.where(Fact.period_end <= period_end)
  if period_start is not None:
    stmt = stmt.where(Fact.period_start >= period_start)

  by_period: dict[
    tuple[date | None, date | None], tuple[dict[str, float], set[str]]
  ] = {}
  for row in session.execute(stmt).all():
    key = (row.period_start, row.period_end)
    balances, present = by_period.setdefault(key, ({}, set()))
    if row.value is not None:
      balances[row.element_id] = balances.get(row.element_id, 0.0) + float(row.value)
    present.add(row.element_id)
  return by_period


def _rollup_parent_element_id(
  session: Session, rule: Rule, cache: dict[str, str | None]
) -> tuple[str | None, str]:
  """Resolve a RollUp rule's parent (the LHS = first variable) to an element_id.

  Prefers an explicit ``variable_element_id`` (tenant CoA elements have a null
  qname); otherwise resolves the qname, cached across rules in one run.
  """
  variables = rule.rule_variables or []
  if not variables:
    return None, ""
  v0 = variables[0]
  qname = v0.get("variable_qname", "") or ""
  explicit = v0.get("variable_element_id")
  if explicit:
    return explicit, qname
  if qname in cache:
    return cache[qname], qname
  element_id = session.execute(
    select(Element.id).where(Element.qname == qname).limit(1)
  ).scalar()
  cache[qname] = element_id
  return element_id, qname


def _evaluate_rollup_arc_derived(
  session: Session,
  rule: Rule,
  by_period: dict[tuple[date | None, date | None], tuple[dict[str, float], set[str]]],
  calculations: dict[str, list[tuple[str, float]]],
  cache: dict[str, str | None],
) -> EvaluationOutcome | None:
  """Evaluate a RollUp against the parent's DIRECT rs-gaap-calculations children.

  Standard XBRL calculation semantics: a summation parent is checked against the
  weighted sum of its *direct* calc children (each parent validated on its own
  rule; the tree holds by induction). Children come from the live calc arcs —
  not the rule's frozen enumeration — so a subtotal footing over a sibling
  concept (``...ExcludingGoodwill`` where the frozen rule named
  ``...IncludingGoodwill``) passes, and multi-fact-per-period elements sum
  (``_load_period_balances`` already summed them). Compares against the parent's
  own reported value, so a genuine mismatch still fails.

  Crucially it uses only the parent's *direct* children and each element's own
  reported balance — NOT a global DAG resolution — so an element that appears in
  a different statement's calc path (e.g. DDA as both a CF add-back and an IS
  expense) can't contaminate the sum. A rollup whose direct children aren't
  reported on this structure (e.g. an IS decomposition rule landing on the CF)
  is skipped, matching the prior evaluator.

  Returns ``None`` when the parent has no calc children in the merged DAG
  (global rs-gaap-calculations overlaid with the structure's own calc arcs),
  so the caller falls back to the frozen-expression evaluator (custom / fac
  rollups).
  """
  parent_id, parent_qname = _rollup_parent_element_id(session, rule, cache)
  if parent_id is None or parent_id not in calculations:
    return None

  parent_label = parent_qname.split(":")[-1] if parent_qname else parent_id
  children = calculations.get(parent_id, [])
  periods = [key for key, (_, present) in by_period.items() if parent_id in present]
  if not periods:
    return EvaluationOutcome(
      status="skipped",
      message=f"no fact bound for subtotal: {parent_label}",
      detail={"parent": parent_qname, "source": "calc-dag"},
    )

  tolerance = rule_tolerance(rule)

  per_period: list[dict[str, Any]] = []
  for key in sorted(periods, key=lambda k: (k[1] is None, k[1] or date.min)):
    balances, present = by_period[key]
    # Skip a period where the subtotal is reported but none of its direct calc
    # children are present here — nothing to foot against (e.g. an IS
    # decomposition rule evaluated on the CF statement).
    if not any(cid in present for cid, _ in children):
      continue
    children_sum = sum(balances.get(cid, 0.0) * w for cid, w in children)
    reported = balances.get(parent_id, 0.0)
    residual = reported - children_sum
    per_period.append(
      {
        "period_end": str(key[1]) if key[1] is not None else None,
        "reported": reported,
        "children_sum": children_sum,
        "residual": residual,
        "passed": abs(residual) <= tolerance,
      }
    )

  if not per_period:
    return EvaluationOutcome(
      status="skipped",
      message="subtotal reported but none of its rollup children are present here",
      detail={"parent": parent_qname, "source": "calc-dag"},
    )
  failed = [p for p in per_period if not p["passed"]]
  if not failed:
    return EvaluationOutcome(
      status="pass",
      detail={"parent": parent_qname, "source": "calc-dag", "periods": per_period},
    )
  worst = max(failed, key=lambda p: abs(p["residual"]))
  return EvaluationOutcome(
    status="fail",
    message=f"rollup failed; residual = {abs(worst['residual']):.2f}",
    detail={
      "parent": parent_qname,
      "source": "calc-dag",
      "tolerance": tolerance,
      "periods": per_period,
    },
  )


def evaluate_rules_for_structure(
  session: Session,
  structure_id: str,
  *,
  fact_set_id: str | None = None,
  period_start: date | None = None,
  period_end: date | None = None,
  created_by: str = "engine",
  global_calculations: dict[str, list[tuple[str, float]]] | None = None,
) -> list[VerificationResult]:
  """Evaluate every rule scoped to ``structure_id`` and persist results.

  Loads rules via :func:`~robosystems.operations.information_block.envelope.load_rules_for_structure`
  (which includes element and association-scoped rules for the structure's
  atoms). For each rule: binds variables → dispatches to the pattern
  evaluator → writes one :class:`VerificationResult` row.

  A binding or dispatch failure writes ``status='error'`` rather than
  propagating — one broken rule can't abort the whole evaluation run.

  ``session.flush()`` is called before returning so that row ids are
  assigned; the caller (OperationSpec wrapper) owns the commit.
  """
  structure = session.get(Structure, structure_id)
  if structure is None:
    raise ValueError(f"Structure not found: {structure_id}")

  associations = (
    session.execute(select(Association).where(Association.structure_id == structure_id))
    .scalars()
    .all()
  )
  element_ids = {
    x
    for x in (
      {a.from_element_id for a in associations}
      | {a.to_element_id for a in associations}
    )
    if x is not None
  }

  rule_lites = load_rules_for_structure(
    session,
    structure_id,
    element_ids=list(element_ids),
    association_ids=[a.id for a in associations],
  )

  if not rule_lites:
    return []

  rule_ids = [r.id for r in rule_lites]
  rules = session.execute(select(Rule).where(Rule.id.in_(rule_ids))).scalars().all()
  rule_map = {r.id: r for r in rules}

  # Arc-derived RollUp prep. RollUp rules are re-derived from the live
  # rs-gaap-calculations DAG (mirroring the fact producer) rather than the
  # rule's frozen child enumeration bound one-fact-per-qname, which reported
  # false failures when a subtotal foots over a sibling concept or over two
  # facts in one period. The global DAG is reused from ``global_calculations``
  # when the caller precomputed it (a report run evaluates many structures),
  # else loaded here — only when a RollUp rule exists.
  calculations: dict[str, list[tuple[str, float]]] = {}
  by_period: dict[
    tuple[date | None, date | None], tuple[dict[str, float], set[str]]
  ] = {}
  parent_id_cache: dict[str, str | None] = {}
  if any(r.rule_pattern == "RollUp" for r in rules):
    if global_calculations is None:
      from robosystems.operations.roboledger.reports.calc_dag import (
        load_rs_gaap_calculations,
      )

      calculations = load_rs_gaap_calculations(session)
    else:
      # Copy the caller-provided global DAG so the per-structure local overlay
      # below can't leak arcs back into the shared dict across structures.
      calculations = dict(global_calculations)
    # Tenant-authored roll_up structures (disclosure notes) foot against
    # their OWN calculation arcs — their parents aren't rs-gaap subtotals,
    # so the global DAG carries no entry for them. Overlay the structure's
    # local calc arcs, letting the global DAG win where both define a
    # parent (statement structures mirror it anyway). Same live-arc
    # doctrine as the global path: children come from arcs, never from a
    # frozen enumeration.
    local_calcs: dict[str, list[tuple[str, float]]] = {}
    for assoc in sorted(
      associations,
      key=lambda x: x.order_value if x.order_value is not None else float("inf"),
    ):
      if assoc.association_type != "calculation":
        continue
      if assoc.from_element_id is None or assoc.to_element_id is None:
        continue
      local_calcs.setdefault(assoc.from_element_id, []).append(
        (assoc.to_element_id, assoc.weight if assoc.weight is not None else 1.0)
      )
    for parent_id, children in local_calcs.items():
      calculations.setdefault(parent_id, children)
    by_period = _load_period_balances(
      session, structure_id, fact_set_id, period_start, period_end
    )

  results: list[VerificationResult] = []
  for rule_lite in rule_lites:
    rule = rule_map.get(rule_lite.id)
    if rule is None:
      continue
    try:
      outcome: EvaluationOutcome | None = None
      if rule.rule_pattern == "RollUp" and calculations:
        outcome = _evaluate_rollup_arc_derived(
          session, rule, by_period, calculations, parent_id_cache
        )
      if outcome is None:
        if rule.rule_pattern == "SumEquals":
          bindings = _bind_sum_variables(session, rule, structure_id)
        else:
          bindings = _bind_variables(
            session,
            rule,
            structure_id,
            period_start,
            period_end,
            fact_set_id=fact_set_id,
          )
        outcome = evaluate_rule(rule, bindings)
    except Exception as exc:
      outcome = EvaluationOutcome(
        status="error",
        message=f"engine failure: {exc}",
        detail={"exception_type": type(exc).__name__},
      )
    row = VerificationResult(
      rule_id=rule.id,
      structure_id=structure_id,
      fact_set_id=fact_set_id,
      period_start=period_start,
      period_end=period_end,
      status=outcome.status,
      message=outcome.message,
      detail=outcome.detail or {},
      created_by=created_by,
    )
    session.add(row)
    results.append(row)

  session.flush()
  return results


__all__ = [
  "evaluate_rules_for_structure",
]
