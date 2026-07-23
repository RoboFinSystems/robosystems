"""Tests for the safe AST expression parser and evaluator."""

from __future__ import annotations

import pytest

from robosystems.operations.information_block.rules.expressions import (
  EQUALITY_TOLERANCE,
  InvalidRuleExpression,
  desugar_aggregates,
  evaluate_derivation,
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


class TestBuildRollupExpression:
  """Shared builder used by both the seed generator and tenant auto rules."""

  def test_unit_weights(self) -> None:
    from robosystems.operations.information_block.rules.expressions import (
      build_rollup_expression,
    )

    expr = build_rollup_expression("Assets", [("Current", 1.0), ("Noncurrent", 1.0)])
    assert expr == "$Assets = ($Current + $Noncurrent)"

  def test_first_term_negative_weight(self) -> None:
    from robosystems.operations.information_block.rules.expressions import (
      build_rollup_expression,
    )

    expr = build_rollup_expression("Net", [("Contra", -1.0), ("Gross", 1.0)])
    assert expr == "$Net = (-$Contra + $Gross)"

  def test_non_unit_weight_keeps_coefficient(self) -> None:
    from robosystems.operations.information_block.rules.expressions import (
      build_rollup_expression,
    )

    expr = build_rollup_expression("Total", [("Half", 0.5), ("Double", 2.0)])
    assert expr == "$Total = (($Half * 0.5) + ($Double * 2.0))"

  def test_single_child(self) -> None:
    from robosystems.operations.information_block.rules.expressions import (
      build_rollup_expression,
    )

    expr = build_rollup_expression("Total", [("Only", 1.0)])
    assert expr == "$Total = ($Only)"

  def test_output_round_trips_through_parser(self) -> None:
    from robosystems.operations.information_block.rules.expressions import (
      build_rollup_expression,
      lhs_variable_names,
      parse_arithmetic_expression,
    )

    names = ["Total", "Half", "Contra", "Plain"]
    expr = build_rollup_expression(
      "Total", [("Half", 0.5), ("Contra", -1.0), ("Plain", 1.0)]
    )
    parsed = parse_arithmetic_expression(expr, names)
    assert lhs_variable_names(parsed) == ["Total"]


class TestEvaluateDerivation:
  """The compute path for Derive rules — RHS evaluated to a value."""

  def test_evaluates_rhs_with_operand_values_only(self) -> None:
    parsed = parse_arithmetic_expression(
      "$CurrentRatio = ($AssetsCurrent / $LiabilitiesCurrent)",
      ["CurrentRatio", "AssetsCurrent", "LiabilitiesCurrent"],
    )
    value = evaluate_derivation(
      parsed, {"AssetsCurrent": 100.0, "LiabilitiesCurrent": 40.0}
    )
    assert value == pytest.approx(2.5)

  def test_nested_subtraction_and_division(self) -> None:
    parsed = parse_arithmetic_expression(
      "$QuickRatio = (($AssetsCurrent - $Inventory) / $LiabilitiesCurrent)",
      ["QuickRatio", "AssetsCurrent", "Inventory", "LiabilitiesCurrent"],
    )
    value = evaluate_derivation(
      parsed,
      {"AssetsCurrent": 100.0, "Inventory": 20.0, "LiabilitiesCurrent": 40.0},
    )
    assert value == pytest.approx(2.0)

  def test_division_by_zero_raises(self) -> None:
    parsed = parse_arithmetic_expression("$Ratio = ($A / $B)", ["Ratio", "A", "B"])
    with pytest.raises(InvalidRuleExpression, match="division by zero"):
      evaluate_derivation(parsed, {"A": 1.0, "B": 0.0})

  def test_missing_operand_raises(self) -> None:
    parsed = parse_arithmetic_expression("$Ratio = ($A / $B)", ["Ratio", "A", "B"])
    with pytest.raises(InvalidRuleExpression, match="unbound name"):
      evaluate_derivation(parsed, {"A": 1.0})

  def test_non_equality_expression_raises(self) -> None:
    parsed = parse_arithmetic_expression("$A + $B", ["A", "B"])
    with pytest.raises(InvalidRuleExpression, match="derivation expects"):
      evaluate_derivation(parsed, {"A": 1.0, "B": 2.0})


class TestDesugarAggregates:
  """avg($X) → synthesized $__avg_X — the pre-parse rewrite that keeps
  the AST whitelist call-free."""

  def test_rewrites_avg_to_synthesized_operand(self) -> None:
    expr, synth = desugar_aggregates(
      "$ReturnOnEquity = ($NetIncomeLoss / avg($StockholdersEquity))"
    )
    assert expr == "$ReturnOnEquity = ($NetIncomeLoss / $__avg_StockholdersEquity)"
    assert synth == {"__avg_StockholdersEquity": "StockholdersEquity"}

  def test_multiple_avg_calls_each_synthesize(self) -> None:
    expr, synth = desugar_aggregates("$EM = (avg($Assets) / avg($Equity))")
    assert expr == "$EM = ($__avg_Assets / $__avg_Equity)"
    assert synth == {"__avg_Assets": "Assets", "__avg_Equity": "Equity"}

  def test_plain_expression_is_untouched(self) -> None:
    original = "$WorkingCapital = ($AssetsCurrent - $LiabilitiesCurrent)"
    expr, synth = desugar_aggregates(original)
    assert expr == original
    assert synth == {}

  def test_whitespace_inside_call_is_tolerated(self) -> None:
    expr, synth = desugar_aggregates("$X = avg( $Y )")
    assert expr == "$X = $__avg_Y"
    assert synth == {"__avg_Y": "Y"}

  def test_desugared_expression_parses_and_evaluates(self) -> None:
    expr, synth = desugar_aggregates("$ROE = ($NI / avg($SE))")
    parsed = parse_arithmetic_expression(expr, ["ROE", "NI", "SE", *list(synth)])
    value = evaluate_derivation(parsed, {"NI": 30.0, "__avg_SE": 100.0})
    assert value == pytest.approx(0.3)

  def test_non_variable_argument_survives_and_is_rejected_downstream(self) -> None:
    """avg($A + $B) is not the aggregate form — it stays a genuine
    ast.Call and the whitelist rejects it."""
    expr, synth = desugar_aggregates("$X = avg($A + $B)")
    assert synth == {}
    with pytest.raises(InvalidRuleExpression, match="disallowed"):
      parse_arithmetic_expression(expr, ["X", "A", "B"])

  def test_other_function_names_are_not_desugared(self) -> None:
    expr, synth = desugar_aggregates("$X = median($A)")
    assert synth == {}
    with pytest.raises(InvalidRuleExpression, match="disallowed"):
      parse_arithmetic_expression(expr, ["X", "A"])
