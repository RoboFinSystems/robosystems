"""Tests for the rs-gaap-rollup-rules/v1 L2 package + its generator.

The package is generated from rs-gaap-calculations/v1 by
``robosystems.taxonomy.scripts.generate_rollup_rules`` — one ``RollUp``
rule per calc-arc subtotal parent. These tests pin the committed
artifact's shape and the pure builder's transform.
"""

from __future__ import annotations

import pytest

from robosystems.taxonomy.discovery import framework_root
from robosystems.taxonomy.loader import (
  RULE_CATEGORY_VALUES,
  RULE_PATTERN_VALUES,
  load_taxonomy_package,
)
from robosystems.taxonomy.model import RuleSpec, TaxonomyPackage
from robosystems.taxonomy.scripts.generate_rollup_rules import build_rollup_rules

SEED_PATH = (
  framework_root("rs-gaap")
  / "packages"
  / "rs-gaap-rollup-rules"
  / "v1"
  / "taxonomy.jsonld"
)


@pytest.fixture(scope="module")
def package() -> TaxonomyPackage:
  return load_taxonomy_package(SEED_PATH)


@pytest.fixture(scope="module")
def rules(package: TaxonomyPackage) -> list[RuleSpec]:
  return package.rules


class TestPackageShape:
  def test_metadata(self, package: TaxonomyPackage) -> None:
    assert package.standard == "rs-gaap-rollup-rules"
    assert package.version == "v1"
    assert package.taxonomy_type == "rules"

  def test_one_rule_per_calc_parent(self, rules: list[RuleSpec]) -> None:
    """rs-gaap-calculations/v1 has 21 distinct subtotal parents."""
    assert len(rules) == 21

  def test_all_rollup_native_fac_relation(self, rules: list[RuleSpec]) -> None:
    for rule in rules:
      assert rule.rule_pattern == "RollUp"
      assert rule.rule_category == "FundamentalAccountingConceptRelation"
      assert rule.rule_origin == "native"
      assert rule.rule_category in RULE_CATEGORY_VALUES
      assert rule.rule_pattern in RULE_PATTERN_VALUES

  def test_element_scoped_to_rs_gaap_parent(self, rules: list[RuleSpec]) -> None:
    """Element-scoping to the parent (a literal rs-gaap qname) is what
    makes the rule fire across every equity-form statement variant."""
    for rule in rules:
      assert rule.rule_target is not None
      assert rule.rule_target.target_kind == "element"
      assert rule.rule_target.target_ref.startswith("rs-gaap:")
      # The parent (LHS / first variable) IS the element target.
      assert rule.rule_variables[0].variable_qname == rule.rule_target.target_ref

  def test_all_variable_qnames_are_rs_gaap(self, rules: list[RuleSpec]) -> None:
    for rule in rules:
      for variable in rule.rule_variables:
        assert variable.variable_qname.startswith("rs-gaap:")

  def test_balance_sheet_identity_present(self, rules: list[RuleSpec]) -> None:
    by_id = {r.id: r for r in rules}
    assets = by_id["rs-gaap-rollup-bs-assets"]
    assert assets.rule_expression == "$Assets = ($AssetsCurrent + $AssetsNoncurrent)"


class TestBuilder:
  def test_groups_arcs_into_one_rule_per_parent(self) -> None:
    graph = [
      {
        "@id": "_:net",
        "roleUri": "https://x/roles/calc/IS-GrossProfit",
        "structureName": "calc — rs-gaap:GrossProfit = rs-gaap:Revenues - rs-gaap:CostOfRevenue",
      },
      {
        "@id": "_:a1",
        "arcFrom": {"@id": "rs-gaap:GrossProfit"},
        "arcTo": {"@id": "rs-gaap:Revenues"},
        "arcAssociationType": "calculation",
        "arcRoleUri": "https://x/roles/calc/IS-GrossProfit",
        "arcWeight": 1.0,
        "arcOrder": 100.0,
      },
      {
        "@id": "_:a2",
        "arcFrom": {"@id": "rs-gaap:GrossProfit"},
        "arcTo": {"@id": "rs-gaap:CostOfRevenue"},
        "arcAssociationType": "calculation",
        "arcRoleUri": "https://x/roles/calc/IS-GrossProfit",
        "arcWeight": -1.0,
        "arcOrder": 200.0,
      },
    ]
    rules = build_rollup_rules(graph)
    assert len(rules) == 1
    rule = rules[0]
    assert rule["@id"] == "_:rs-gaap-rollup-is-grossprofit"
    assert rule["rulePattern"] == "RollUp"
    assert rule["ruleExpression"] == "$GrossProfit = ($Revenues - $CostOfRevenue)"
    assert rule["ruleTarget"] == {
      "targetKind": "element",
      "targetRef": "rs-gaap:GrossProfit",
    }
    # parent first, then children in arcOrder
    assert [v["variableQname"] for v in rule["ruleVariables"]] == [
      "rs-gaap:GrossProfit",
      "rs-gaap:Revenues",
      "rs-gaap:CostOfRevenue",
    ]

  def test_children_sorted_by_arc_order(self) -> None:
    graph = [
      {
        "@id": "_:b2",
        "arcFrom": {"@id": "rs-gaap:P"},
        "arcTo": {"@id": "rs-gaap:Second"},
        "arcAssociationType": "calculation",
        "arcRoleUri": "https://x/r/N",
        "arcWeight": 1.0,
        "arcOrder": 200.0,
      },
      {
        "@id": "_:b1",
        "arcFrom": {"@id": "rs-gaap:P"},
        "arcTo": {"@id": "rs-gaap:First"},
        "arcAssociationType": "calculation",
        "arcRoleUri": "https://x/r/N",
        "arcWeight": 1.0,
        "arcOrder": 100.0,
      },
    ]
    rule = build_rollup_rules(graph)[0]
    assert rule["ruleExpression"] == "$P = ($First + $Second)"

  def test_rejects_multiple_parents_in_one_network(self) -> None:
    graph = [
      {
        "@id": "_:c1",
        "arcFrom": {"@id": "rs-gaap:P1"},
        "arcTo": {"@id": "rs-gaap:X"},
        "arcAssociationType": "calculation",
        "arcRoleUri": "https://x/r/N",
        "arcWeight": 1.0,
        "arcOrder": 100.0,
      },
      {
        "@id": "_:c2",
        "arcFrom": {"@id": "rs-gaap:P2"},
        "arcTo": {"@id": "rs-gaap:Y"},
        "arcAssociationType": "calculation",
        "arcRoleUri": "https://x/r/N",
        "arcWeight": 1.0,
        "arcOrder": 200.0,
      },
    ]
    with pytest.raises(ValueError, match="multiple parents"):
      build_rollup_rules(graph)


class TestBuilderNonUnitWeights:
  def test_non_unit_weight_renders_explicit_coefficient(self) -> None:
    """Locks the shared-builder delegation: a 0.5-weighted arc keeps its
    coefficient as ``($var * 0.5)`` (byte-identical to the historical
    inline builder)."""
    graph = [
      {
        "@id": "_:a1",
        "arcFrom": {"@id": "rs-gaap:Total"},
        "arcTo": {"@id": "rs-gaap:HalfShare"},
        "arcAssociationType": "calculation",
        "arcRoleUri": "https://x/roles/calc/BS-Total",
        "arcWeight": 0.5,
        "arcOrder": 100.0,
      },
      {
        "@id": "_:a2",
        "arcFrom": {"@id": "rs-gaap:Total"},
        "arcTo": {"@id": "rs-gaap:Plain"},
        "arcAssociationType": "calculation",
        "arcRoleUri": "https://x/roles/calc/BS-Total",
        "arcWeight": 1.0,
        "arcOrder": 200.0,
      },
    ]
    rules = build_rollup_rules(graph)
    assert len(rules) == 1
    assert rules[0]["ruleExpression"] == "$Total = (($HalfShare * 0.5) + $Plain)"
