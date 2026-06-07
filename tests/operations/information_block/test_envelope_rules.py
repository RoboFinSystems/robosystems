"""Tests for rule packing in the Information Block envelope.

Exercises :func:`rule_to_lite` and :func:`load_rules_for_structure`
from :mod:`robosystems.operations.information_block.envelope` with a
mocked SQLAlchemy session — no database required.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from robosystems.models.api.information_block import RuleLite
from robosystems.operations.information_block.envelope import (
  load_rules_for_structure,
  rule_to_lite,
)


def _make_rule(
  *,
  rule_id: str = "rule_1",
  category: str = "FundamentalAccountingConceptRelation",
  pattern: str = "EqualTo",
  expression: str = "$Assets = ($Liabilities + $Equity)",
  target_kind: str | None = "structure",
  target_structure_id: str | None = "struct_bs",
  target_element_id: str | None = None,
  target_association_id: str | None = None,
  rule_variables: list[dict[str, str]] | None = None,
  message: str | None = "Balance Sheet identity.",
  severity: str = "error",
  origin: str = "forked",
) -> MagicMock:
  """Build a MagicMock shaped like a :class:`Rule` ORM row."""
  rule = MagicMock()
  rule.id = rule_id
  rule.rule_category = category
  rule.rule_pattern = pattern
  rule.rule_check_kind = None
  rule.rule_expression = expression
  rule.target_kind = target_kind
  rule.target_structure_id = target_structure_id
  rule.target_element_id = target_element_id
  rule.target_association_id = target_association_id
  rule.rule_variables = (
    rule_variables
    if rule_variables is not None
    else [
      {"variable_name": "Assets", "variable_qname": "fac:Assets"},
      {"variable_name": "Liabilities", "variable_qname": "fac:Liabilities"},
      {"variable_name": "Equity", "variable_qname": "fac:Equity"},
    ]
  )
  rule.rule_message = message
  rule.rule_severity = severity
  rule.rule_origin = origin
  return rule


def _exec_scalars(*, rows: list[Any]) -> MagicMock:
  result = MagicMock()
  result.scalars.return_value.all.return_value = rows
  return result


class TestRuleToLite:
  def test_projects_core_fields(self) -> None:
    lite = rule_to_lite(_make_rule())
    assert isinstance(lite, RuleLite)
    assert lite.id == "rule_1"
    assert lite.rule_category == "FundamentalAccountingConceptRelation"
    assert lite.rule_pattern == "EqualTo"
    assert lite.rule_expression == "$Assets = ($Liabilities + $Equity)"
    assert lite.rule_message == "Balance Sheet identity."
    assert lite.rule_severity == "error"
    assert lite.rule_origin == "forked"

  def test_structure_target_is_unpacked(self) -> None:
    lite = rule_to_lite(_make_rule())
    assert lite.rule_target is not None
    assert lite.rule_target.target_kind == "structure"
    assert lite.rule_target.target_ref_id == "struct_bs"

  def test_element_target_is_unpacked(self) -> None:
    rule = _make_rule(
      target_kind="element",
      target_structure_id=None,
      target_element_id="elem_assets",
    )
    lite = rule_to_lite(rule)
    assert lite.rule_target is not None
    assert lite.rule_target.target_kind == "element"
    assert lite.rule_target.target_ref_id == "elem_assets"

  def test_association_target_is_unpacked(self) -> None:
    rule = _make_rule(
      target_kind="association",
      target_structure_id=None,
      target_association_id="assoc_42",
    )
    lite = rule_to_lite(rule)
    assert lite.rule_target is not None
    assert lite.rule_target.target_kind == "association"
    assert lite.rule_target.target_ref_id == "assoc_42"

  def test_null_target_becomes_none(self) -> None:
    """A global rule (no target_kind, no FK) surfaces as
    ``rule_target=None`` — matches the CHECK constraint's "all nulls"
    branch."""
    rule = _make_rule(
      target_kind=None,
      target_structure_id=None,
      target_element_id=None,
      target_association_id=None,
    )
    lite = rule_to_lite(rule)
    assert lite.rule_target is None

  def test_variables_are_projected(self) -> None:
    lite = rule_to_lite(_make_rule())
    assert len(lite.rule_variables) == 3
    names = [v.variable_name for v in lite.rule_variables]
    qnames = [v.variable_qname for v in lite.rule_variables]
    assert names == ["Assets", "Liabilities", "Equity"]
    assert qnames == ["fac:Assets", "fac:Liabilities", "fac:Equity"]

  def test_missing_variables_become_empty_list(self) -> None:
    """A rule with ``rule_variables=None`` in the DB should project to
    ``[]`` — the JSONB default is ``'[]'`` but the SQLAlchemy attribute
    might be ``None`` on a freshly-instantiated row."""
    rule = _make_rule(rule_variables=None)
    rule.rule_variables = None
    lite = rule_to_lite(rule)
    assert lite.rule_variables == []


class TestLoadRulesForStructure:
  def test_returns_empty_list_when_no_rules_match(self) -> None:
    session = MagicMock()
    session.execute.return_value = _exec_scalars(rows=[])
    result = load_rules_for_structure(session, "struct_bs")
    assert result == []
    session.execute.assert_called_once()

  def test_packs_rule_list_from_query_rows(self) -> None:
    session = MagicMock()
    rows = [
      _make_rule(rule_id="rule_bs_identity"),
      _make_rule(
        rule_id="rule_bs_assets_breakdown",
        pattern="RollUp",
        expression="$Assets = ($CurrentAssets + $NoncurrentAssets)",
      ),
    ]
    session.execute.return_value = _exec_scalars(rows=rows)
    result = load_rules_for_structure(session, "struct_bs")
    assert len(result) == 2
    assert result[0].id == "rule_bs_identity"
    assert result[1].rule_pattern == "RollUp"

  def test_query_runs_once_even_with_element_and_association_scope(self) -> None:
    """Rules query is a single ``or_()`` across all three target buckets
    so the envelope builder doesn't issue three separate round-trips."""
    session = MagicMock()
    session.execute.return_value = _exec_scalars(rows=[])
    load_rules_for_structure(
      session,
      "struct_bs",
      element_ids=["elem_assets", "elem_liab"],
      association_ids=["assoc_1", "assoc_2"],
    )
    assert session.execute.call_count == 1
