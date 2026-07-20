"""Tests for the rs-metric/v1 catalog package (Metrics M-1).

Pins the hand-authored artifact's shape: the Key Financial Metrics
container (element + structure), five metric concepts, five role-bound
presentation arcs, and five ``Derive`` rules whose expressions parse and
whose operands reference rs-gaap anchors that exist in the rs-gaap
package (the calc-DAG anchors the report pivot persists).
"""

from __future__ import annotations

import json

import pytest

from robosystems.operations.information_block.rules.expressions import (
  lhs_variable_names,
  parse_arithmetic_expression,
)
from robosystems.taxonomy.discovery import framework_root
from robosystems.taxonomy.loader import (
  RULE_CATEGORY_VALUES,
  RULE_PATTERN_VALUES,
  load_taxonomy_package,
)
from robosystems.taxonomy.model import RuleSpec, TaxonomyPackage

SEED_PATH = framework_root("rs-gaap") / "packages" / "rs-metric" / "v1"
ROLE_URI = "https://robosystems.ai/seattle/cm-roles/roles/metrics/KeyFinancialMetrics"

EXPECTED_CONCEPTS = {
  "rs-metric:WorkingCapital",
  "rs-metric:CurrentRatio",
  "rs-metric:QuickRatio",
  "rs-metric:DebtToEquity",
  "rs-metric:InterestCoverage",
}


@pytest.fixture(scope="module")
def package() -> TaxonomyPackage:
  return load_taxonomy_package(SEED_PATH / "taxonomy.jsonld")


@pytest.fixture(scope="module")
def rules(package: TaxonomyPackage) -> list[RuleSpec]:
  return package.rules


class TestPackageShape:
  def test_metadata(self, package: TaxonomyPackage) -> None:
    assert package.standard == "rs-metric"
    assert package.version == "v1"
    assert package.taxonomy_type == "reporting_extension"
    assert package.default_block_type == "metric"

  def test_elements(self, package: TaxonomyPackage) -> None:
    by_qname = {e.qname: e for e in package.elements}
    assert set(by_qname) == EXPECTED_CONCEPTS | {"rs-metric:KeyFinancialMetrics"}
    container = by_qname["rs-metric:KeyFinancialMetrics"]
    assert container.is_abstract
    for qname in EXPECTED_CONCEPTS:
      assert by_qname[qname].element_type == "concept"
    # Every element carries the namespace prefix as its source — the
    # value the check_element_source widening (0022 / provisioning)
    # admits and the #893 namespace protection reserves.
    assert {e.source for e in package.elements} == {"rs-metric"}

  def test_monetary_and_period_shape(self, package: TaxonomyPackage) -> None:
    by_qname = {e.qname: e for e in package.elements}
    assert by_qname["rs-metric:WorkingCapital"].is_monetary
    for ratio in ("CurrentRatio", "QuickRatio", "DebtToEquity", "InterestCoverage"):
      assert not by_qname[f"rs-metric:{ratio}"].is_monetary
    assert by_qname["rs-metric:InterestCoverage"].period_type == "duration"
    for instant in ("WorkingCapital", "CurrentRatio", "QuickRatio", "DebtToEquity"):
      assert by_qname[f"rs-metric:{instant}"].period_type == "instant"

  def test_structure(self, package: TaxonomyPackage) -> None:
    assert len(package.structures) == 1
    structure = package.structures[0]
    assert structure.block_type == "metric"
    assert structure.concept_arrangement == "arithmetic"
    assert structure.role_uri == ROLE_URI

  def test_presentation_arcs_bind_catalog_to_role(
    self, package: TaxonomyPackage
  ) -> None:
    arcs = package.associations
    assert len(arcs) == 5
    assert {a.to_qname for a in arcs} == EXPECTED_CONCEPTS
    for arc in arcs:
      assert arc.from_qname == "rs-metric:KeyFinancialMetrics"
      assert arc.association_type == "presentation"
      assert arc.role == ROLE_URI
    orders = sorted(a.order for a in arcs)
    assert orders == [1.0, 2.0, 3.0, 4.0, 5.0]


class TestDeriveRules:
  def test_five_derive_rules(self, rules: list[RuleSpec]) -> None:
    """Regression against the RULE_PATTERN_VALUES gate: an unknown
    pattern is silently skipped at load, so a missing 'Derive' entry
    would collapse this to zero."""
    assert len(rules) == 5
    for rule in rules:
      assert rule.rule_pattern == "Derive"
      assert rule.rule_pattern in RULE_PATTERN_VALUES
      assert rule.rule_category in RULE_CATEGORY_VALUES
      assert rule.rule_severity == "info"

  def test_element_scoped_to_metric_concepts(self, rules: list[RuleSpec]) -> None:
    targets = set()
    for rule in rules:
      assert rule.rule_target is not None
      assert rule.rule_target.target_kind == "element"
      targets.add(rule.rule_target.target_ref)
    assert targets == EXPECTED_CONCEPTS

  def test_expressions_parse_with_lhs_metric_first(self, rules: list[RuleSpec]) -> None:
    for rule in rules:
      names = [v.variable_name for v in rule.rule_variables]
      parsed = parse_arithmetic_expression(rule.rule_expression, names)
      lhs = lhs_variable_names(parsed)
      assert len(lhs) == 1
      # LHS variable is the target metric and sits first in the list.
      assert lhs[0] == names[0]
      assert rule.rule_variables[0].variable_qname == rule.rule_target.target_ref

  def test_operand_qnames_exist_in_rs_gaap_package(self, rules: list[RuleSpec]) -> None:
    """Operands must be anchors the report pivot actually persists —
    a typo'd qname soft-fails to 'skipped' at compute time, so catch it
    here instead."""
    rs_gaap_path = framework_root("rs-gaap") / "packages" / "rs-gaap" / "v1"
    raw = json.loads((rs_gaap_path / "taxonomy.jsonld").read_text())
    rs_gaap_ids = {
      node.get("@id") for node in raw.get("@graph", []) if isinstance(node, dict)
    }
    for rule in rules:
      for var in rule.rule_variables[1:]:
        assert var.variable_qname.startswith("rs-gaap:")
        assert var.variable_qname in rs_gaap_ids, (
          f"{rule.id}: operand {var.variable_qname} not found in rs-gaap/v1"
        )
