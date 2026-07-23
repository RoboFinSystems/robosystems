"""Tests for the rs-metric/v1 catalog package (Metrics M-1 + M-2).

Pins the hand-authored artifact's shape: the Key Financial Metrics
container (element + structure), ten metric concepts, ten role-bound
presentation arcs, and ten ``Derive`` rules whose expressions parse
(through the avg() desugar) and whose operands reference either rs-gaap
anchors that exist in the rs-gaap package (the calc-DAG anchors the
report pivot persists) or rs-metric concepts defined in this package
(the M-2 composition operands).
"""

from __future__ import annotations

import json

import pytest

from robosystems.operations.information_block.rules.expressions import (
  desugar_aggregates,
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

M1_CONCEPTS = {
  "rs-metric:WorkingCapital",
  "rs-metric:CurrentRatio",
  "rs-metric:QuickRatio",
  "rs-metric:DebtToEquity",
  "rs-metric:InterestCoverage",
}
M2_CONCEPTS = {
  "rs-metric:ReturnOnEquity",
  "rs-metric:NetProfitMargin",
  "rs-metric:AssetTurnover",
  "rs-metric:EquityMultiplier",
  "rs-metric:ReturnOnEquityDuPont",
}
EXPECTED_CONCEPTS = M1_CONCEPTS | M2_CONCEPTS

EXPECTED_ITEM_TYPES = {
  "rs-metric:WorkingCapital": "monetary",
  "rs-metric:CurrentRatio": "ratio",
  "rs-metric:QuickRatio": "ratio",
  "rs-metric:DebtToEquity": "ratio",
  "rs-metric:InterestCoverage": "multiple",
  "rs-metric:ReturnOnEquity": "percent",
  "rs-metric:NetProfitMargin": "percent",
  "rs-metric:AssetTurnover": "multiple",
  "rs-metric:EquityMultiplier": "multiple",
  "rs-metric:ReturnOnEquityDuPont": "percent",
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
    for qname in EXPECTED_CONCEPTS - {"rs-metric:WorkingCapital"}:
      assert not by_qname[qname].is_monetary
    for instant in ("WorkingCapital", "CurrentRatio", "QuickRatio", "DebtToEquity"):
      assert by_qname[f"rs-metric:{instant}"].period_type == "instant"
    # The M-2 concepts measure over the window (flows or period
    # averages), so they are all durations — like InterestCoverage.
    for duration in {"rs-metric:InterestCoverage"} | M2_CONCEPTS:
      assert by_qname[duration].period_type == "duration"

  def test_item_types(self, package: TaxonomyPackage) -> None:
    """Every catalog concept carries its format family — the M-2
    unit/format plumbing seeds from here."""
    by_qname = {e.qname: e for e in package.elements}
    for qname, expected in EXPECTED_ITEM_TYPES.items():
      assert by_qname[qname].item_type == expected, qname
    assert by_qname["rs-metric:KeyFinancialMetrics"].item_type is None

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
    assert len(arcs) == 10
    assert {a.to_qname for a in arcs} == EXPECTED_CONCEPTS
    for arc in arcs:
      assert arc.from_qname == "rs-metric:KeyFinancialMetrics"
      assert arc.association_type == "presentation"
      assert arc.role == ROLE_URI
    orders = sorted(a.order for a in arcs)
    assert orders == [float(i) for i in range(1, 11)]


class TestDeriveRules:
  def test_ten_derive_rules(self, rules: list[RuleSpec]) -> None:
    """Regression against the RULE_PATTERN_VALUES gate: an unknown
    pattern is silently skipped at load, so a missing 'Derive' entry
    would collapse this to zero."""
    assert len(rules) == 10
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
    """Every expression parses through the avg() desugar (the compute
    path's exact preprocessing) with the target metric as the sole LHS
    variable, listed first."""
    for rule in rules:
      names = [v.variable_name for v in rule.rule_variables]
      expression, synthesized = desugar_aggregates(rule.rule_expression)
      # Every avg() base must be a declared variable with a qname binding.
      for base in synthesized.values():
        assert base in names, f"{rule.id}: avg base {base!r} not declared"
      parsed = parse_arithmetic_expression(expression, names + list(synthesized))
      lhs = lhs_variable_names(parsed)
      assert len(lhs) == 1
      # LHS variable is the target metric and sits first in the list.
      assert lhs[0] == names[0]
      assert rule.rule_variables[0].variable_qname == rule.rule_target.target_ref

  def test_avg_and_composition_are_present(self, rules: list[RuleSpec]) -> None:
    """The M-2 grammar is exercised by the shipped catalog: at least one
    avg() rule and one metric-on-metric rule, and the DuPont composite's
    operands are exactly the three legs."""
    by_target = {r.rule_target.target_ref: r for r in rules if r.rule_target}
    _, roe_synth = desugar_aggregates(
      by_target["rs-metric:ReturnOnEquity"].rule_expression
    )
    assert roe_synth == {"__avg_StockholdersEquity": "StockholdersEquity"}
    dupont = by_target["rs-metric:ReturnOnEquityDuPont"]
    operand_qnames = {v.variable_qname for v in dupont.rule_variables[1:]}
    assert operand_qnames == {
      "rs-metric:NetProfitMargin",
      "rs-metric:AssetTurnover",
      "rs-metric:EquityMultiplier",
    }

  def test_operand_qnames_resolve_in_package_universe(
    self, rules: list[RuleSpec], package: TaxonomyPackage
  ) -> None:
    """Operands must be anchors the report pivot actually persists (a
    typo'd qname soft-fails to 'skipped' at compute time, so catch it
    here) — or, for M-2 composition, rs-metric concepts defined in this
    very package."""
    rs_gaap_path = framework_root("rs-gaap") / "packages" / "rs-gaap" / "v1"
    raw = json.loads((rs_gaap_path / "taxonomy.jsonld").read_text())
    rs_gaap_ids = {
      node.get("@id") for node in raw.get("@graph", []) if isinstance(node, dict)
    }
    own_qnames = {e.qname for e in package.elements}
    for rule in rules:
      for var in rule.rule_variables[1:]:
        qname = var.variable_qname
        if qname.startswith("rs-metric:"):
          assert qname in own_qnames, (
            f"{rule.id}: composition operand {qname} not defined in rs-metric/v1"
          )
        else:
          assert qname.startswith("rs-gaap:")
          assert qname in rs_gaap_ids, (
            f"{rule.id}: operand {qname} not found in rs-gaap/v1"
          )
