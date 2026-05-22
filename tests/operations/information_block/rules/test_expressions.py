"""Tests for the safe AST expression parser and evaluator."""

from __future__ import annotations

import pytest

from robosystems.operations.information_block.rules.expressions import (
  EQUALITY_TOLERANCE,
  InvalidRuleExpression,
  evaluate_equality,
  lhs_variable_names,
  parse_arithmetic_expression,
)


class TestLhsVariableNames:
  def test_returns_only_left_side_variables(self) -> None:
    """The RollUp evaluator uses this to find the parent subtotal (LHS),
    which must be bound; RHS children default to 0 when missing."""
    parsed = parse_arithmetic_expression(
      "$Assets = ($AssetsCurrent + $AssetsNoncurrent)",
      ["Assets", "AssetsCurrent", "AssetsNoncurrent"],
    )
    assert lhs_variable_names(parsed) == ["Assets"]

  def test_raises_when_not_an_equality(self) -> None:
    parsed = parse_arithmetic_expression("$A + $B", ["A", "B"])
    with pytest.raises(InvalidRuleExpression):
      lhs_variable_names(parsed)


class TestParseArithmeticExpression:
  def test_simple_equality_parses(self) -> None:
    parsed = parse_arithmetic_expression(
      "$Assets = ($Liabilities + $Equity)",
      ["Assets", "Liabilities", "Equity"],
    )
    assert parsed.variable_names == ["Assets", "Liabilities", "Equity"]

  def test_variable_substitution_replaces_dollar_prefix(self) -> None:
    parsed = parse_arithmetic_expression("$X = $Y", ["X", "Y"])
    import ast

    assert isinstance(parsed.tree, ast.Expression)

  def test_raises_on_unbound_variable(self) -> None:
    with pytest.raises(InvalidRuleExpression, match="unbound"):
      parse_arithmetic_expression("$Assets = $UnknownVar", ["Assets"])

  def test_raises_on_syntax_error(self) -> None:
    with pytest.raises(InvalidRuleExpression, match="syntax error"):
      parse_arithmetic_expression("$A === $B", ["A", "B"])

  def test_raises_on_function_call(self) -> None:
    with pytest.raises(InvalidRuleExpression, match="disallowed"):
      parse_arithmetic_expression("abs($A) = $B", ["A", "B"])

  def test_raises_on_attribute_access(self) -> None:
    with pytest.raises(InvalidRuleExpression, match="disallowed"):
      parse_arithmetic_expression("$A.value = $B", ["A", "B"])

  def test_raises_on_subscript(self) -> None:
    with pytest.raises(InvalidRuleExpression, match="disallowed"):
      parse_arithmetic_expression("$A[0] = $B", ["A", "B"])

  def test_nested_arithmetic_parses(self) -> None:
    parsed = parse_arithmetic_expression(
      "$Total = ($A + $B + $C - $D)", ["Total", "A", "B", "C", "D"]
    )
    assert parsed is not None

  def test_empty_variable_list_with_no_dollar_signs(self) -> None:
    parsed = parse_arithmetic_expression("1 = 1", [])
    assert parsed is not None

  def test_raises_on_import_statement(self) -> None:
    with pytest.raises((InvalidRuleExpression, SyntaxError)):
      parse_arithmetic_expression("__import__('os')", [])


class TestEvaluateEquality:
  def test_passing_identity(self) -> None:
    parsed = parse_arithmetic_expression(
      "$Assets = ($Liabilities + $Equity)", ["Assets", "Liabilities", "Equity"]
    )
    passed, residual = evaluate_equality(
      parsed, {"Assets": 1000.0, "Liabilities": 600.0, "Equity": 400.0}
    )
    assert passed is True
    assert residual == pytest.approx(0.0)

  def test_failing_identity(self) -> None:
    parsed = parse_arithmetic_expression(
      "$Assets = ($Liabilities + $Equity)", ["Assets", "Liabilities", "Equity"]
    )
    passed, residual = evaluate_equality(
      parsed, {"Assets": 1001.0, "Liabilities": 600.0, "Equity": 400.0}
    )
    assert passed is False
    assert residual == pytest.approx(1.0)

  def test_within_tolerance_passes(self) -> None:
    parsed = parse_arithmetic_expression("$A = $B", ["A", "B"])
    passed, residual = evaluate_equality(parsed, {"A": 100.005, "B": 100.0})
    assert passed is True
    assert residual < EQUALITY_TOLERANCE

  def test_custom_tolerance(self) -> None:
    parsed = parse_arithmetic_expression("$A = $B", ["A", "B"])
    passed, _ = evaluate_equality(parsed, {"A": 100.5, "B": 100.0}, tolerance=1.0)
    assert passed is True

  def test_raises_on_non_equality_expression(self) -> None:
    parsed = parse_arithmetic_expression("$A + $B", ["A", "B"])
    with pytest.raises(InvalidRuleExpression, match="equality pattern"):
      evaluate_equality(parsed, {"A": 1.0, "B": 2.0})

  def test_unary_negation(self) -> None:
    parsed = parse_arithmetic_expression("$A = -$B", ["A", "B"])
    passed, residual = evaluate_equality(parsed, {"A": -5.0, "B": 5.0})
    assert passed is True
    assert residual == pytest.approx(0.0)
