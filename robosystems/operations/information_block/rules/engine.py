"""Rule evaluation engine: ``evaluate_rules_for_structure`` binds each rule's
``$Variable``s to fact values (``None`` on a miss), dispatches to the
per-pattern evaluator, and writes one ``VerificationResult`` per rule.
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
from robosystems.operations.information_block.rules.expressions import (
  InvalidRuleExpression,
  lhs_variable_names,
  parse_arithmetic_expression,
)


def _actual_set_ids(structure_id: str):
  """The structure's actual (``scenario_id IS NULL``) FactSets.

  Every unpinned fact read here goes through this: scenario month sets are
  stamped with the statement structure's id, so ``structure_id`` alone
  would bind forecast months. Rules evaluate the books, actuals only.
  """
  return select(FactSet.id).where(
    FactSet.structure_id == structure_id,
    FactSet.scenario_id.is_(None),
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
    if name in bindings:
      # A repeated name would silently merge two elements; raise so the rule
      # records status='error' instead of a wrong verdict.
      raise ValueError(f"duplicate variable_name {name!r} in rule variables")

    # Tenant CoA elements have a null qname, so schedule rules pass
    # `variable_element_id` directly.
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
        Fact.value.is_not(None),
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
      stmt = base_stmt.where(
        Fact.structure_id == structure_id,
        Fact.fact_set_id.in_(_actual_set_ids(structure_id)),
      )
    bindings[name] = session.execute(stmt).scalar()

  return bindings


def _bind_sum_variables(
  session: Session,
  rule: Rule,
  structure_id: str,
) -> dict[str, float | None]:
  """Aggregate SUM of duration facts per variable — SumEquals rules only."""
  bindings: dict[str, float | None] = {}
  for var in rule.rule_variables or []:
    name = var.get("variable_name", "")
    qname = var.get("variable_qname", "")
    if not name:
      continue
    if name in bindings:
      raise ValueError(f"duplicate variable_name {name!r} in rule variables")
    element_id: str | None = (
      var.get("variable_element_id")
      or session.execute(
        select(Element.id).where(Element.qname == qname).limit(1)
      ).scalar()
    )
    if element_id is None:
      bindings[name] = None
      continue
    # SumEquals checks Σ(periodic facts) == contracted total, so it sums
    # every periodic fact regardless of `fact_scope`: filtering to
    # `in_scope` would sum only the post-close tail of a schedule that
    # straddles `closed_through`. Scenario sets are excluded as in
    # `_actual_set_ids`.
    row = session.execute(
      text(
        "SELECT ROUND(SUM(f.value)::numeric, 2) AS total "
        "FROM facts f "
        "JOIN fact_sets fs ON fs.id = f.fact_set_id "
        "WHERE f.element_id = :eid AND f.structure_id = :sid "
        "AND f.period_type = 'duration' AND f.fact_type = 'Numeric' "
        "AND fs.scenario_id IS NULL"
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
  """Group in-scope facts into per-period ``(balances, present)``, summing
  multiple facts per element+period (e.g. a derived flow plus a plug).

  Scoped to one FactSet (``fact_set_id``, else the latest actual set): two
  reports over the same period both stamp this structure, so a
  ``structure_id`` scope would double every balance.
  """
  if fact_set_id is None:
    fact_set_id = session.execute(
      select(FactSet.id)
      .where(
        FactSet.structure_id == structure_id,
        FactSet.scenario_id.is_(None),
      )
      .order_by(FactSet.created_at.desc(), FactSet.id.desc())
      .limit(1)
    ).scalar()

  # A text-block fact must not mark its element "present" for the rollup
  # skip guard.
  stmt = select(Fact.element_id, Fact.value, Fact.period_start, Fact.period_end).where(
    Fact.fact_scope == "in_scope",
    Fact.fact_type == "Numeric",
  )
  if fact_set_id is not None:
    stmt = stmt.where(Fact.fact_set_id == fact_set_id)
  else:
    # No actual FactSet exists, so this yields no balances. The actuals pin
    # still matters: the structure may carry forecast-only sets.
    stmt = stmt.where(
      Fact.structure_id == structure_id,
      Fact.fact_set_id.in_(_actual_set_ids(structure_id)),
    )
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


def _rollup_parent_variable(rule: Rule) -> dict | None:
  """Pick the ``rule_variables`` entry naming the RollUp's parent subtotal.

  The parent is the expression's LHS variable (as the frozen evaluator
  derives it), falling back to ``variables[0]`` (the machine producers'
  parent-first convention) when that can't be determined.
  """
  variables = rule.rule_variables or []
  if not variables:
    return None
  names = [v.get("variable_name", "") for v in variables]
  expression = rule.rule_expression if isinstance(rule.rule_expression, str) else ""
  try:
    parsed = parse_arithmetic_expression(expression, [n for n in names if n])
    lhs = lhs_variable_names(parsed)
  except InvalidRuleExpression:
    lhs = []
  if len(lhs) == 1 and lhs[0] in names:
    return variables[names.index(lhs[0])]
  return variables[0]


def _rollup_parent_element_id(
  session: Session, rule: Rule, cache: dict[str, str | None]
) -> tuple[str | None, str]:
  """Resolve a RollUp rule's parent (the expression LHS) to an element_id,
  preferring an explicit ``variable_element_id``; qname lookups are cached."""
  parent = _rollup_parent_variable(rule)
  if parent is None:
    return None, ""
  qname = parent.get("variable_qname", "") or ""
  explicit = parent.get("variable_element_id")
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
  """Evaluate a RollUp against the parent's DIRECT children in the live calc DAG.

  XBRL calculation semantics: each parent is checked against the weighted sum
  of its direct children's reported balances, not a global DAG resolution, so
  an element on another statement's calc path can't contaminate the sum.
  Using live arcs rather than the rule's frozen enumeration lets a subtotal
  foot over a sibling concept. Periods where no direct child is reported here
  are skipped.

  Returns ``None`` when the parent has no calc children, so the caller falls
  back to the frozen-expression evaluator.
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
  """Evaluate every rule scoped to ``structure_id`` (including element- and
  association-scoped ones) and persist one result per rule.

  A per-rule failure writes ``status='error'`` rather than propagating.
  Flushes; the caller owns the commit.
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

  # RollUp rules evaluate against the live calc DAG; ``global_calculations``
  # lets a caller evaluating many structures load it once.
  calculations: dict[str, list[tuple[str, float]]] = {}
  by_period: dict[
    tuple[date | None, date | None], tuple[dict[str, float], set[str]]
  ] = {}
  parent_id_cache: dict[str, str | None] = {}
  if any(r.rule_pattern == "RollUp" for r in rules):
    from robosystems.operations.roboledger.reports.calc_dag import (
      load_rs_gaap_calculations,
      merge_calculations,
    )

    if global_calculations is None:
      calculations = load_rs_gaap_calculations(session)
    else:
      calculations = dict(global_calculations)
    # Local arcs win: a disclosure note that decomposes a global calc parent
    # foots against its own members. merge_calculations is pure, so the
    # shared global dict isn't polluted.
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
    calculations = merge_calculations(calculations, local_calcs)
    by_period = _load_period_balances(
      session, structure_id, fact_set_id, period_start, period_end
    )

  results: list[VerificationResult] = []
  for rule_lite in rule_lites:
    rule = rule_map.get(rule_lite.id)
    if rule is None:
      continue
    # Derive rules compute values; they are not checks.
    if rule.rule_pattern == "Derive":
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
