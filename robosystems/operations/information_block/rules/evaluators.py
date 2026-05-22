"""Pattern dispatchers for the rule engine.

Maps each ``rule_pattern`` value to an evaluation strategy:

* ``EqualTo`` / ``RollForward`` — strict arithmetic equality via the
  safe AST parser; ``pass`` when ``abs(lhs - rhs) <= tolerance``; any
  unbound variable → ``skipped``.
* ``RollUp`` — ``$Parent = Σ children``; the parent subtotal (LHS) must
  be bound, but a missing RHS child is treated as 0 (the renderer sums
  only present children); ``skipped`` only when the parent is unbound.
* ``Exists`` — ``pass`` when the first variable's bound value is not
  ``None`` (i.e., a fact exists for that concept in the period).
* ``CoExists`` — ``pass`` when *all* variables are bound or *all* are
  ``None``; ``fail`` on a mixed binding (some present, some absent).
* Any other pattern — ``skipped`` with an "not yet implemented" message.

All handlers return an :class:`EvaluationOutcome` dataclass so the
engine can persist the result without inspecting internal state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from robosystems.operations.information_block.rules.expressions import (
  EQUALITY_TOLERANCE,
  InvalidRuleExpression,
  evaluate_equality,
  lhs_variable_names,
  parse_arithmetic_expression,
)


@dataclass
class EvaluationOutcome:
  """Result of evaluating one rule against a binding."""

  status: Literal["pass", "fail", "error", "skipped"]
  message: str | None = None
  detail: dict[str, Any] = field(default_factory=dict)


_EQUALITY_PATTERNS = frozenset({"EqualTo", "RollForward"})


def evaluate_rule(rule: Any, bindings: dict[str, float | None]) -> EvaluationOutcome:
  """Dispatch rule evaluation based on ``rule.rule_pattern``.

  ``rule`` is a :class:`~robosystems.models.extensions.Rule` ORM row or
  any object with the same attributes. ``bindings`` maps variable name →
  float (or ``None`` when the fact was not found for that concept).
  """
  pattern = rule.rule_pattern
  if pattern in _EQUALITY_PATTERNS:
    return _evaluate_equality_pattern(rule, bindings)
  if pattern == "RollUp":
    return _evaluate_rollup(rule, bindings)
  if pattern == "Exists":
    return _evaluate_exists(rule, bindings)
  if pattern == "CoExists":
    return _evaluate_coexists(rule, bindings)
  if pattern == "SumEquals":
    return _evaluate_sum_equals(rule, bindings)
  return EvaluationOutcome(
    status="skipped",
    message=f"pattern {pattern!r} not yet implemented",
    detail={},
  )


def _evaluate_equality_pattern(
  rule: Any, bindings: dict[str, float | None]
) -> EvaluationOutcome:
  variable_names = [v["variable_name"] for v in (rule.rule_variables or [])]
  missing = [n for n in variable_names if bindings.get(n) is None]
  if missing:
    return EvaluationOutcome(
      status="skipped",
      message=f"no fact bound for variables: {', '.join(missing)}",
      detail={"bindings": _serializable(bindings), "missing": missing},
    )
  tolerance = EQUALITY_TOLERANCE
  if rule.metadata_ and isinstance(rule.metadata_, dict):
    tolerance = float(rule.metadata_.get("tolerance", EQUALITY_TOLERANCE))

  bound: dict[str, float] = {n: float(bindings[n]) for n in variable_names}  # type: ignore[arg-type]
  try:
    parsed = parse_arithmetic_expression(rule.rule_expression, variable_names)
    passed, residual = evaluate_equality(parsed, bound, tolerance=tolerance)
  except InvalidRuleExpression as exc:
    return EvaluationOutcome(
      status="error",
      message=str(exc),
      detail={"expression": rule.rule_expression},
    )
  return EvaluationOutcome(
    status="pass" if passed else "fail",
    message=None if passed else f"equality failed; residual = {residual:.2f}",
    detail={
      "bindings": _serializable(bindings),
      "residual": residual,
      "tolerance": tolerance,
    },
  )


def _evaluate_rollup(rule: Any, bindings: dict[str, float | None]) -> EvaluationOutcome:
  """Evaluate a ``RollUp`` rule (``$Parent = Σ children``).

  Unlike strict :func:`_evaluate_equality_pattern`, a missing RHS child
  is treated as 0 — the renderer sums only the children that have facts
  (all-zero rows are dropped), so a report that presents two of a
  subtotal's six possible children must still satisfy the rollup. The
  parent subtotal (the LHS) MUST be bound; if it isn't, the subtotal
  wasn't reported, so there's nothing to verify → ``skipped``.
  """
  variable_names = [v["variable_name"] for v in (rule.rule_variables or [])]
  if not variable_names:
    return EvaluationOutcome(
      status="skipped", message="RollUp rule has no variables", detail={}
    )
  try:
    parsed = parse_arithmetic_expression(rule.rule_expression, variable_names)
    required = lhs_variable_names(parsed)
  except InvalidRuleExpression as exc:
    return EvaluationOutcome(
      status="error", message=str(exc), detail={"expression": rule.rule_expression}
    )

  missing_subtotal = [n for n in required if bindings.get(n) is None]
  if missing_subtotal:
    return EvaluationOutcome(
      status="skipped",
      message=f"no fact bound for subtotal: {', '.join(missing_subtotal)}",
      detail={
        "bindings": _serializable(bindings),
        "missing_subtotal": missing_subtotal,
      },
    )

  tolerance = EQUALITY_TOLERANCE
  if rule.metadata_ and isinstance(rule.metadata_, dict):
    tolerance = float(rule.metadata_.get("tolerance", EQUALITY_TOLERANCE))

  # RHS children with no fact default to 0 (sum-of-present-children).
  rhs_names = [n for n in variable_names if n not in required]
  defaulted = [n for n in rhs_names if bindings.get(n) is None]
  # Parent present but NONE of its rollup children reported here: the
  # rollup is vacuous (nothing to sum), which happens when an
  # element-scoped rule fires in a statement that lists the subtotal as a
  # standalone line but doesn't decompose it (e.g. NetIncomeLoss on the
  # Cash Flow / Equity statements, where it's an input/total, not a
  # rollup of ContinuingOps + Discontinued). Skip rather than report a
  # spurious failure — the rule still evaluates in the subtotal's home
  # statement where the children ARE presented.
  if rhs_names and len(defaulted) == len(rhs_names):
    return EvaluationOutcome(
      status="skipped",
      message="subtotal reported but none of its rollup children are present here",
      detail={"bindings": _serializable(bindings), "absent_children": defaulted},
    )
  bound: dict[str, float] = {
    n: (float(bindings[n]) if bindings.get(n) is not None else 0.0)
    for n in variable_names
  }
  try:
    passed, residual = evaluate_equality(parsed, bound, tolerance=tolerance)
  except InvalidRuleExpression as exc:
    return EvaluationOutcome(
      status="error", message=str(exc), detail={"expression": rule.rule_expression}
    )
  return EvaluationOutcome(
    status="pass" if passed else "fail",
    message=None if passed else f"rollup failed; residual = {residual:.2f}",
    detail={
      "bindings": _serializable(bindings),
      "residual": residual,
      "tolerance": tolerance,
      "children_defaulted_to_zero": defaulted,
    },
  )


def _evaluate_exists(rule: Any, bindings: dict[str, float | None]) -> EvaluationOutcome:
  """Pass when at least one variable is bound to a non-null value."""
  variable_names = [v["variable_name"] for v in (rule.rule_variables or [])]
  if not variable_names:
    return EvaluationOutcome(
      status="skipped",
      message="Exists rule has no variables",
      detail={},
    )
  bound_values = {n: bindings.get(n) for n in variable_names}
  exists = any(v is not None for v in bound_values.values())
  if exists:
    return EvaluationOutcome(
      status="pass",
      detail={"bindings": _serializable(bound_values)},
    )
  return EvaluationOutcome(
    status="fail",
    message=f"required concept(s) not present: {', '.join(variable_names)}",
    detail={"bindings": _serializable(bound_values)},
  )


def _evaluate_sum_equals(
  rule: Any, bindings: dict[str, float | None]
) -> EvaluationOutcome:
  """Pass when the aggregate sum bound for the first variable equals expected_total."""
  variable_names = [v["variable_name"] for v in (rule.rule_variables or [])]
  if not variable_names:
    return EvaluationOutcome(
      status="skipped",
      message="SumEquals rule has no variables",
      detail={},
    )
  actual = bindings.get(variable_names[0])
  if actual is None:
    return EvaluationOutcome(
      status="skipped",
      message=f"no sum bound for variable: {variable_names[0]}",
      detail={},
    )
  expected = float((rule.metadata_ or {}).get("expected_total", 0))
  tolerance = float((rule.metadata_ or {}).get("tolerance", EQUALITY_TOLERANCE))
  passed = abs(actual - expected) <= tolerance
  return EvaluationOutcome(
    status="pass" if passed else "fail",
    message=None if passed else f"sum {actual:.2f} != expected {expected:.2f}",
    detail={"actual_sum": actual, "expected_total": expected, "tolerance": tolerance},
  )


def _evaluate_coexists(
  rule: Any, bindings: dict[str, float | None]
) -> EvaluationOutcome:
  """Pass when all variables are bound or all are null; fail on mixed."""
  variable_names = [v["variable_name"] for v in (rule.rule_variables or [])]
  if not variable_names:
    return EvaluationOutcome(
      status="skipped",
      message="CoExists rule has no variables",
      detail={},
    )
  bound_values = {n: bindings.get(n) for n in variable_names}
  present = [n for n, v in bound_values.items() if v is not None]
  absent = [n for n, v in bound_values.items() if v is None]
  if not present or not absent:
    return EvaluationOutcome(
      status="pass",
      detail={"bindings": _serializable(bound_values)},
    )
  return EvaluationOutcome(
    status="fail",
    message=(
      f"co-existence violated: {', '.join(present)} present "
      f"but {', '.join(absent)} absent"
    ),
    detail={
      "bindings": _serializable(bound_values),
      "present": present,
      "absent": absent,
    },
  )


def _serializable(d: dict[str, Any]) -> dict[str, Any]:
  """Convert a bindings dict to JSON-safe types (float → float, None stays None)."""
  return {k: (float(v) if v is not None else None) for k, v in d.items()}


__all__ = [
  "EvaluationOutcome",
  "evaluate_rule",
]
