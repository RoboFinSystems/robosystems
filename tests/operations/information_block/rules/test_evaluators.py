"""Tests for the per-pattern rule evaluators."""

from __future__ import annotations

from unittest.mock import MagicMock

from robosystems.operations.information_block.rules.evaluators import (
  evaluate_rule,
)


def _make_rule(
  *,
  pattern: str,
  expression: str = "$A = $B",
  variables: list[dict[str, str]] | None = None,
  metadata: dict | None = None,
) -> MagicMock:
  rule = MagicMock()
  rule.rule_pattern = pattern
  rule.rule_expression = expression
  rule.rule_variables = (
    variables
    if variables is not None
    else [
      {"variable_name": "A", "variable_qname": "fac:A"},
      {"variable_name": "B", "variable_qname": "fac:B"},
    ]
  )
  rule.metadata_ = metadata or {}
  return rule


class TestEqualToPattern:
  def test_pass_when_equal(self) -> None:
    rule = _make_rule(pattern="EqualTo")
    outcome = evaluate_rule(rule, {"A": 100.0, "B": 100.0})
    assert outcome.status == "pass"
    assert outcome.message is None

  def test_fail_when_not_equal(self) -> None:
    rule = _make_rule(pattern="EqualTo")
    outcome = evaluate_rule(rule, {"A": 101.0, "B": 100.0})
    assert outcome.status == "fail"
    assert "residual" in (outcome.message or "")
    assert outcome.detail["residual"] == 1.0

  def test_skipped_when_variable_missing(self) -> None:
    rule = _make_rule(pattern="EqualTo")
    outcome = evaluate_rule(rule, {"A": 100.0, "B": None})
    assert outcome.status == "skipped"
    assert "B" in (outcome.message or "")

  def test_error_on_invalid_expression(self) -> None:
    rule = _make_rule(pattern="EqualTo", expression="abs($A) = $B")
    outcome = evaluate_rule(rule, {"A": 100.0, "B": 100.0})
    assert outcome.status == "error"

  def test_custom_tolerance_from_metadata(self) -> None:
    rule = _make_rule(pattern="EqualTo", metadata={"tolerance": 5.0})
    outcome = evaluate_rule(rule, {"A": 103.0, "B": 100.0})
    assert outcome.status == "pass"


class TestRollUpPattern:
  def test_pass_when_sum_matches(self) -> None:
    rule = _make_rule(
      pattern="RollUp",
      expression="$Total = ($Part1 + $Part2)",
      variables=[
        {"variable_name": "Total", "variable_qname": "fac:Total"},
        {"variable_name": "Part1", "variable_qname": "fac:Part1"},
        {"variable_name": "Part2", "variable_qname": "fac:Part2"},
      ],
    )
    outcome = evaluate_rule(rule, {"Total": 1000.0, "Part1": 600.0, "Part2": 400.0})
    assert outcome.status == "pass"

  def test_fail_when_sum_does_not_match(self) -> None:
    rule = _make_rule(
      pattern="RollUp",
      expression="$Total = ($Part1 + $Part2)",
      variables=[
        {"variable_name": "Total", "variable_qname": "fac:Total"},
        {"variable_name": "Part1", "variable_qname": "fac:Part1"},
        {"variable_name": "Part2", "variable_qname": "fac:Part2"},
      ],
    )
    outcome = evaluate_rule(rule, {"Total": 1001.0, "Part1": 600.0, "Part2": 400.0})
    assert outcome.status == "fail"


class TestRollForwardPattern:
  def test_pass_when_equal(self) -> None:
    rule = _make_rule(
      pattern="RollForward",
      expression="$End = ($Begin + $Additions - $Disposals)",
      variables=[
        {"variable_name": "End", "variable_qname": "fac:End"},
        {"variable_name": "Begin", "variable_qname": "fac:Begin"},
        {"variable_name": "Additions", "variable_qname": "fac:Additions"},
        {"variable_name": "Disposals", "variable_qname": "fac:Disposals"},
      ],
    )
    outcome = evaluate_rule(
      rule, {"End": 900.0, "Begin": 1000.0, "Additions": 200.0, "Disposals": 300.0}
    )
    assert outcome.status == "pass"


class TestExistsPattern:
  def test_pass_when_variable_is_bound(self) -> None:
    rule = _make_rule(pattern="Exists", expression="exists($A)")
    outcome = evaluate_rule(rule, {"A": 100.0, "B": None})
    assert outcome.status == "pass"

  def test_fail_when_all_variables_missing(self) -> None:
    rule = _make_rule(pattern="Exists", expression="exists($A)")
    outcome = evaluate_rule(rule, {"A": None, "B": None})
    assert outcome.status == "fail"
    assert "not present" in (outcome.message or "")

  def test_skipped_when_no_variables(self) -> None:
    rule = _make_rule(pattern="Exists", expression="exists()", variables=[])
    outcome = evaluate_rule(rule, {})
    assert outcome.status == "skipped"


class TestCoExistsPattern:
  def test_pass_when_all_present(self) -> None:
    rule = _make_rule(pattern="CoExists", expression="coexists($A, $B)")
    outcome = evaluate_rule(rule, {"A": 100.0, "B": 200.0})
    assert outcome.status == "pass"

  def test_pass_when_all_absent(self) -> None:
    rule = _make_rule(pattern="CoExists", expression="coexists($A, $B)")
    outcome = evaluate_rule(rule, {"A": None, "B": None})
    assert outcome.status == "pass"

  def test_fail_on_mixed_binding(self) -> None:
    rule = _make_rule(pattern="CoExists", expression="coexists($A, $B)")
    outcome = evaluate_rule(rule, {"A": 100.0, "B": None})
    assert outcome.status == "fail"
    assert "co-existence" in (outcome.message or "")

  def test_skipped_when_no_variables(self) -> None:
    rule = _make_rule(pattern="CoExists", expression="coexists()", variables=[])
    outcome = evaluate_rule(rule, {})
    assert outcome.status == "skipped"


class TestSumEqualsPattern:
  def test_pass_when_sum_matches_expected_total(self) -> None:
    rule = _make_rule(
      pattern="SumEquals",
      expression="sum($periodic_amount) = 1200.0",
      variables=[
        {"variable_name": "periodic_amount", "variable_qname": "fac:DeprExpense"}
      ],
      metadata={"expected_total": 1200.0},
    )
    outcome = evaluate_rule(rule, {"periodic_amount": 1200.0})
    assert outcome.status == "pass"
    assert outcome.message is None

  def test_fail_when_sum_differs_from_expected(self) -> None:
    rule = _make_rule(
      pattern="SumEquals",
      expression="sum($periodic_amount) = 1200.0",
      variables=[
        {"variable_name": "periodic_amount", "variable_qname": "fac:DeprExpense"}
      ],
      metadata={"expected_total": 1200.0},
    )
    outcome = evaluate_rule(rule, {"periodic_amount": 1199.98})
    assert outcome.status == "fail"
    assert "1199.98" in (outcome.message or "")
    assert outcome.detail["actual_sum"] == 1199.98
    assert outcome.detail["expected_total"] == 1200.0

  def test_skipped_when_sum_not_bound(self) -> None:
    rule = _make_rule(
      pattern="SumEquals",
      variables=[
        {"variable_name": "periodic_amount", "variable_qname": "fac:DeprExpense"}
      ],
      metadata={"expected_total": 1200.0},
    )
    outcome = evaluate_rule(rule, {"periodic_amount": None})
    assert outcome.status == "skipped"
    assert "periodic_amount" in (outcome.message or "")

  def test_skipped_when_no_variables(self) -> None:
    rule = _make_rule(
      pattern="SumEquals", variables=[], metadata={"expected_total": 100.0}
    )
    outcome = evaluate_rule(rule, {})
    assert outcome.status == "skipped"

  def test_pass_within_custom_tolerance(self) -> None:
    rule = _make_rule(
      pattern="SumEquals",
      variables=[{"variable_name": "periodic_amount", "variable_qname": "fac:X"}],
      metadata={"expected_total": 1000.0, "tolerance": 1.0},
    )
    outcome = evaluate_rule(rule, {"periodic_amount": 1000.5})
    assert outcome.status == "pass"

  def test_fail_outside_custom_tolerance(self) -> None:
    rule = _make_rule(
      pattern="SumEquals",
      variables=[{"variable_name": "periodic_amount", "variable_qname": "fac:X"}],
      metadata={"expected_total": 1000.0, "tolerance": 0.01},
    )
    outcome = evaluate_rule(rule, {"periodic_amount": 999.0})
    assert outcome.status == "fail"


class TestUnimplementedPattern:
  def test_adjustment_returns_skipped(self) -> None:
    rule = _make_rule(pattern="Adjustment")
    outcome = evaluate_rule(rule, {"A": 1.0, "B": 1.0})
    assert outcome.status == "skipped"
    assert "not yet implemented" in (outcome.message or "")

  def test_variance_returns_skipped(self) -> None:
    rule = _make_rule(pattern="Variance")
    outcome = evaluate_rule(rule, {"A": 1.0, "B": 1.0})
    assert outcome.status == "skipped"

  def test_unknown_pattern_returns_skipped(self) -> None:
    rule = _make_rule(pattern="SomeNewPattern")
    outcome = evaluate_rule(rule, {})
    assert outcome.status == "skipped"
