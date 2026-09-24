"""Pattern dispatchers for the rule engine.

Maps each ``rule_pattern`` value to an evaluation strategy:

* ``EqualTo`` / ``RollForward`` — strict arithmetic equality via the
  safe AST parser; ``pass`` when ``abs(lhs - rhs) <= tolerance``; any
  unbound variable → ``skipped``.
* ``RollUp`` — ``$Parent = Σ children``; the parent subtotal (LHS) must
  be bound, but a missing RHS child is treated as 0 (the renderer sums
  only present children); ``skipped`` only when the parent is unbound.
* ``Exists`` — ``pass`` when any variable is bound to a non-null value.
* ``CoExists`` — ``pass`` when *all* variables are bound or *all* are
  ``None``; ``fail`` on a mixed binding (some present, some absent).
* ``SumEquals`` — the aggregate sum bound for the first variable is
  compared against ``metadata_['expected_total']``.
* Any other pattern — ``skipped``.
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


def rule_tolerance(rule: Any) -> float:
  """The rule's ``metadata_['tolerance']`` override, else ``EQUALITY_TOLERANCE``."""
  metadata = rule.metadata_
  if isinstance(metadata, dict):
    return float(metadata.get("tolerance", EQUALITY_TOLERANCE))
  return EQUALITY_TOLERANCE


def evaluate_rule(rule: Any, bindings: dict[str, float | None]) -> EvaluationOutcome:
  """Dispatch rule evaluation based on ``rule.rule_pattern``. ``bindings`` maps
  variable name → value, ``None`` when no fact was found."""
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
  tolerance = rule_tolerance(rule)

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

  A missing RHS child counts as 0, as in the renderer's sum of present
  children. An unbound parent (LHS) means ``skipped``.
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

  tolerance = rule_tolerance(rule)

  rhs_names = [n for n in variable_names if n not in required]
  defaulted = [n for n in rhs_names if bindings.get(n) is None]
  # No children reported here (e.g. NetIncomeLoss as a standalone line on
  # the cash-flow statement): vacuous, so skip rather than fail.
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
  """Pass when the first variable's bound sum equals ``expected_total``."""
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
  tolerance = rule_tolerance(rule)
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
