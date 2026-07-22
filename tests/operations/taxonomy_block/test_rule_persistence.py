"""Tests for ``persist_tenant_rules`` — envelope rules[] → Rule rows."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from robosystems.models.api.taxonomy_block import TaxonomyBlockRuleRequest
from robosystems.models.extensions import Rule
from robosystems.operations.taxonomy_block.rule_persistence import persist_tenant_rules


def _fake_taxonomy(tid: str = "tax_1") -> MagicMock:
  taxonomy = MagicMock()
  taxonomy.id = tid
  return taxonomy


def _fake_element(element_id: str, qname: str) -> MagicMock:
  element = MagicMock()
  element.id = element_id
  element.qname = qname
  return element


def _fake_structure(structure_id: str, name: str) -> MagicMock:
  structure = MagicMock()
  structure.id = structure_id
  structure.name = name
  return structure


def test_target_taxonomy_self_maps_to_taxonomy_kind() -> None:
  session = MagicMock()
  taxonomy = _fake_taxonomy("tax_42")
  req = TaxonomyBlockRuleRequest(
    name="identity",
    rule_category="FundamentalAccountingConceptRelation",
    rule_pattern="EqualTo",
    expression="$a = $b",
    variables=[
      {"variable_name": "a", "variable_qname": "x:A"},
      {"variable_name": "b", "variable_qname": "x:B"},
    ],
    target_taxonomy_self=True,
  )
  rules = persist_tenant_rules(session, taxonomy, [req], {}, {}, "usr_1")
  assert len(rules) == 1
  rule = rules[0]
  assert isinstance(rule, Rule)
  assert rule.target_kind == "taxonomy"
  assert rule.target_taxonomy_id == "tax_42"
  assert rule.target_structure_id is None
  assert rule.target_element_id is None
  session.add.assert_called_once_with(rule)


def test_target_structure_ref_maps_to_structure_kind() -> None:
  session = MagicMock()
  taxonomy = _fake_taxonomy()
  structure = _fake_structure("struct_7", "main")
  req = TaxonomyBlockRuleRequest(
    name="on-structure",
    rule_category="ReportLevelModelStructureRule",
    rule_pattern="Exists",
    expression="$x = 1",
    variables=[{"variable_name": "x", "variable_qname": "x:X"}],
    target_structure_ref="main",
  )
  rules = persist_tenant_rules(
    session, taxonomy, [req], {}, {"main": structure}, "usr_1"
  )
  rule = rules[0]
  assert rule.target_kind == "structure"
  assert rule.target_structure_id == "struct_7"


def test_target_element_qname_maps_to_element_kind() -> None:
  session = MagicMock()
  taxonomy = _fake_taxonomy()
  element = _fake_element("el_3", "us-gaap:Assets")
  req = TaxonomyBlockRuleRequest(
    name="on-element",
    rule_category="XBRLTechnicalSyntaxRule",
    rule_pattern="Exists",
    expression="$x = 1",
    variables=[{"variable_name": "x", "variable_qname": "us-gaap:Assets"}],
    target_element_qname="us-gaap:Assets",
  )
  rules = persist_tenant_rules(
    session, taxonomy, [req], {"us-gaap:Assets": element}, {}, "usr_1"
  )
  rule = rules[0]
  assert rule.target_kind == "element"
  assert rule.target_element_id == "el_3"


def test_unresolved_target_structure_ref_raises() -> None:
  session = MagicMock()
  taxonomy = _fake_taxonomy()
  req = TaxonomyBlockRuleRequest(
    name="bad-struct",
    rule_category="ReportLevelModelStructureRule",
    rule_pattern="Exists",
    expression="$x = 1",
    variables=[{"variable_name": "x", "variable_qname": "x:X"}],
    target_structure_ref="ghost",
  )
  with pytest.raises(ValueError) as exc:
    persist_tenant_rules(session, taxonomy, [req], {}, {}, "usr_1")
  assert "ghost" in str(exc.value)


def test_name_and_description_preserved_in_metadata() -> None:
  session = MagicMock()
  taxonomy = _fake_taxonomy()
  req = TaxonomyBlockRuleRequest(
    name="named",
    description="A helpful rule",
    rule_category="FundamentalAccountingConceptRelation",
    rule_pattern="EqualTo",
    expression="$a = $b",
    variables=[
      {"variable_name": "a", "variable_qname": "x:A"},
      {"variable_name": "b", "variable_qname": "x:B"},
    ],
    target_taxonomy_self=True,
    metadata={"custom": "data"},
  )
  rules = persist_tenant_rules(session, taxonomy, [req], {}, {}, "usr_1")
  rule = rules[0]
  assert rule.metadata_["rule_name"] == "named"
  assert rule.metadata_["rule_description"] == "A helpful rule"
  assert rule.metadata_["custom"] == "data"


def test_rule_origin_native_for_tenant_authored() -> None:
  session = MagicMock()
  taxonomy = _fake_taxonomy()
  req = TaxonomyBlockRuleRequest(
    name="r",
    rule_category="FundamentalAccountingConceptRelation",
    rule_pattern="Exists",
    expression="$x = 1",
    variables=[{"variable_name": "x", "variable_qname": "x:X"}],
    target_taxonomy_self=True,
  )
  rules = persist_tenant_rules(session, taxonomy, [req], {}, {}, "usr_1")
  assert rules[0].rule_origin == "native"


def test_derive_pattern_accepted_and_persisted() -> None:
  """'Derive' is an envelope-authorable pattern — the tenant-metric path.

  A Derive rule targets the metric concept (compute-metrics requires an
  element target) and carries $Variable→qname bindings that resolve at
  compute time, so the envelope must accept it end to end.
  """
  session = MagicMock()
  taxonomy = _fake_taxonomy("tax_7")
  metric = _fake_element("elem_wc", "driftline:WorkingCapital")
  req = TaxonomyBlockRuleRequest(
    name="derive-working-capital",
    rule_category="ReportingSystemSpecificRule",
    rule_pattern="Derive",
    expression="$WorkingCapital = ($AssetsCurrent - $LiabilitiesCurrent)",
    variables=[
      {"variable_name": "WorkingCapital", "variable_qname": "driftline:WorkingCapital"},
      {"variable_name": "AssetsCurrent", "variable_qname": "rs-gaap:AssetsCurrent"},
      {
        "variable_name": "LiabilitiesCurrent",
        "variable_qname": "rs-gaap:LiabilitiesCurrent",
      },
    ],
    target_element_qname="driftline:WorkingCapital",
  )
  rules = persist_tenant_rules(
    session, taxonomy, [req], {"driftline:WorkingCapital": metric}, {}, "usr_1"
  )
  assert len(rules) == 1
  rule = rules[0]
  assert rule.rule_pattern == "Derive"
  assert rule.target_kind == "element"
  assert rule.target_element_id == "elem_wc"
  assert len(rule.rule_variables) == 3
