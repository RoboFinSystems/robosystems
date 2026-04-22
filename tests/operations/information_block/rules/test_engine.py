"""Integration-style tests for the rule engine (MagicMock session)."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch


def _make_rule_orm(
  *,
  rule_id: str = "rule_1",
  pattern: str = "EqualTo",
  expression: str = "$Assets = ($Liabilities + $Equity)",
  variables: list[dict] | None = None,
) -> MagicMock:
  rule = MagicMock()
  rule.id = rule_id
  rule.rule_pattern = pattern
  rule.rule_expression = expression
  rule.rule_variables = variables or [
    {"variable_name": "Assets", "variable_qname": "fac:Assets"},
    {"variable_name": "Liabilities", "variable_qname": "fac:Liabilities"},
    {"variable_name": "Equity", "variable_qname": "fac:Equity"},
  ]
  rule.target_kind = "structure"
  rule.target_structure_id = "struct_bs"
  rule.target_element_id = None
  rule.target_association_id = None
  rule.metadata_ = {}
  rule.rule_category = "FundamentalAccountingConceptRelation"
  rule.rule_severity = "error"
  rule.rule_origin = "forked"
  rule.rule_message = None
  return rule


def _scalars(*rows):
  result = MagicMock()
  result.scalars.return_value.all.return_value = list(rows)
  return result


def _scalar(value):
  result = MagicMock()
  result.scalar.return_value = value
  return result


class TestBindVariables:
  def test_binds_value_when_element_and_fact_exist(self) -> None:
    from robosystems.operations.information_block.rules.engine import _bind_variables

    session = MagicMock()
    rule = _make_rule_orm(
      variables=[{"variable_name": "Assets", "variable_qname": "fac:Assets"}]
    )
    session.execute.side_effect = [
      _scalar("elem_assets"),
      _scalar(1000.0),
    ]
    bindings = _bind_variables(session, rule, "struct_bs", None, None)
    assert bindings == {"Assets": 1000.0}

  def test_binds_none_when_element_missing(self) -> None:
    from robosystems.operations.information_block.rules.engine import _bind_variables

    session = MagicMock()
    rule = _make_rule_orm(
      variables=[{"variable_name": "X", "variable_qname": "fac:Unknown"}]
    )
    session.execute.return_value = _scalar(None)
    bindings = _bind_variables(session, rule, "struct_bs", None, None)
    assert bindings == {"X": None}

  def test_binds_none_when_fact_missing(self) -> None:
    from robosystems.operations.information_block.rules.engine import _bind_variables

    session = MagicMock()
    rule = _make_rule_orm(
      variables=[{"variable_name": "Assets", "variable_qname": "fac:Assets"}]
    )
    session.execute.side_effect = [_scalar("elem_assets"), _scalar(None)]
    bindings = _bind_variables(session, rule, "struct_bs", None, None)
    assert bindings == {"Assets": None}

  def test_skips_empty_variable_name(self) -> None:
    from robosystems.operations.information_block.rules.engine import _bind_variables

    session = MagicMock()
    rule = _make_rule_orm(variables=[{"variable_name": "", "variable_qname": "fac:A"}])
    bindings = _bind_variables(session, rule, "struct_bs", None, None)
    assert bindings == {}
    session.execute.assert_not_called()

  def test_multiple_variables_bind_independently(self) -> None:
    from robosystems.operations.information_block.rules.engine import _bind_variables

    session = MagicMock()
    rule = _make_rule_orm(
      variables=[
        {"variable_name": "A", "variable_qname": "fac:A"},
        {"variable_name": "B", "variable_qname": "fac:B"},
      ]
    )
    session.execute.side_effect = [
      _scalar("elem_a"),
      _scalar(100.0),
      _scalar("elem_b"),
      _scalar(50.0),
    ]
    bindings = _bind_variables(session, rule, "struct_bs", None, None)
    assert bindings == {"A": 100.0, "B": 50.0}


class TestEvaluateRulesForStructure:
  def test_returns_empty_when_no_rules(self) -> None:
    from robosystems.operations.information_block.rules.engine import (
      evaluate_rules_for_structure,
    )

    session = MagicMock()
    session.get.return_value = MagicMock(id="struct_bs")
    session.execute.return_value = _scalars()

    with patch(
      "robosystems.operations.information_block.rules.engine.load_rules_for_structure",
      return_value=[],
    ):
      results = evaluate_rules_for_structure(session, "struct_bs")
    assert results == []
    session.flush.assert_not_called()

  def test_writes_error_status_on_engine_failure(self) -> None:
    from robosystems.operations.information_block.rules.engine import (
      evaluate_rules_for_structure,
    )

    session = MagicMock()
    session.get.return_value = MagicMock(id="struct_bs")

    rule_lite = MagicMock()
    rule_lite.id = "rule_crash"

    rule_orm = _make_rule_orm(rule_id="rule_crash")

    with (
      patch(
        "robosystems.operations.information_block.rules.engine.load_rules_for_structure",
        return_value=[rule_lite],
      ),
      patch(
        "robosystems.operations.information_block.rules.engine.select",
      ),
    ):
      assoc_result = _scalars()
      rule_result = _scalars(rule_orm)
      session.execute.side_effect = [assoc_result, rule_result, RuntimeError("boom")]

      results = evaluate_rules_for_structure(session, "struct_bs")

    assert len(results) == 1
    added = session.add.call_args_list[0][0][0]
    assert added.status == "error"
    assert "engine failure" in added.message

  def test_flushes_after_writing_results(self) -> None:
    from robosystems.operations.information_block.rules.engine import (
      evaluate_rules_for_structure,
    )

    session = MagicMock()
    session.get.return_value = MagicMock(id="struct_bs")

    rule_lite = MagicMock()
    rule_lite.id = "rule_eq"

    rule_orm = _make_rule_orm(
      rule_id="rule_eq",
      variables=[
        {"variable_name": "A", "variable_qname": "fac:A"},
        {"variable_name": "B", "variable_qname": "fac:B"},
      ],
      expression="$A = $B",
    )

    with (
      patch(
        "robosystems.operations.information_block.rules.engine.load_rules_for_structure",
        return_value=[rule_lite],
      ),
      patch(
        "robosystems.operations.information_block.rules.engine.select",
      ),
    ):
      session.execute.side_effect = [
        _scalars(),
        _scalars(rule_orm),
        _scalar("elem_a"),
        _scalar(100.0),
        _scalar("elem_b"),
        _scalar(100.0),
      ]
      evaluate_rules_for_structure(session, "struct_bs")

    session.flush.assert_called_once()

  def test_passes_period_args_through(self) -> None:
    from robosystems.operations.information_block.rules.engine import (
      evaluate_rules_for_structure,
    )

    session = MagicMock()
    session.get.return_value = MagicMock(id="struct_bs")
    session.execute.return_value = _scalars()

    with patch(
      "robosystems.operations.information_block.rules.engine.load_rules_for_structure",
      return_value=[],
    ):
      results = evaluate_rules_for_structure(
        session,
        "struct_bs",
        fact_set_id="fs_1",
        period_start=date(2025, 1, 1),
        period_end=date(2025, 12, 31),
        created_by="test_user",
      )
    assert results == []
