"""Tests for the rs-driver/v1 lever catalog package (Forecast F-1).

Pins the hand-authored artifact's shape: the Driver Catalog container
(element + reference structure, ``block_type='custom'`` so it never
renders), four lever concepts with format families, four role-bound
presentation arcs, and four ``Derive`` rules whose expressions parse
through the FULL forecast preprocessing (``desugar_priors`` then
``desugar_aggregates`` — the compute path's exact order) and whose
targets/operands are rs-gaap anchors present in the core rs-gaap
package (which the tenant keep-critical curation carries — the DSO
target is ``ReceivablesNetCurrent``, NOT the tenant-excluded
``AccountsReceivableNetCurrent``).
"""

from __future__ import annotations

import json

import pytest

from robosystems.operations.information_block.rules.expressions import (
  desugar_aggregates,
  desugar_priors,
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

SEED_PATH = framework_root("rs-gaap") / "packages" / "rs-driver" / "v1"
ROLE_URI = "https://robosystems.ai/seattle/cm-roles/roles/drivers/DriverCatalog"

EXPECTED_CONCEPTS = {
  "rs-driver:RevenueGrowthRate",
  "rs-driver:CostOfRevenueRate",
  "rs-driver:DaysSalesOutstanding",
  "rs-driver:DaysPayableOutstanding",
}

EXPECTED_ITEM_TYPES = {
  "rs-driver:RevenueGrowthRate": "percent",
  "rs-driver:CostOfRevenueRate": "percent",
  "rs-driver:DaysSalesOutstanding": "days",
  "rs-driver:DaysPayableOutstanding": "days",
}

# target rs-gaap anchor → the lever operand that activates the rule
EXPECTED_RULE_TARGETS = {
  "rs-gaap:Revenues": "rs-driver:RevenueGrowthRate",
  "rs-gaap:CostOfRevenue": "rs-driver:CostOfRevenueRate",
  "rs-gaap:ReceivablesNetCurrent": "rs-driver:DaysSalesOutstanding",
  "rs-gaap:AccountsPayableCurrent": "rs-driver:DaysPayableOutstanding",
}


@pytest.fixture(scope="module")
def package() -> TaxonomyPackage:
  return load_taxonomy_package(SEED_PATH / "taxonomy.jsonld")


@pytest.fixture(scope="module")
def rules(package: TaxonomyPackage) -> list[RuleSpec]:
  return package.rules


class TestPackageShape:
  def test_metadata(self, package: TaxonomyPackage) -> None:
    assert package.standard == "rs-driver"
    assert package.version == "v1"
    assert package.taxonomy_type == "reporting_extension"
    # 'custom' keeps the reference catalog out of the renderable block
    # surface — it is vocabulary + rules, not an Information Block.
    assert package.default_block_type == "custom"

  def test_elements(self, package: TaxonomyPackage) -> None:
    by_qname = {e.qname: e for e in package.elements}
    assert set(by_qname) == EXPECTED_CONCEPTS | {"rs-driver:DriverCatalog"}
    assert by_qname["rs-driver:DriverCatalog"].is_abstract
    for qname in EXPECTED_CONCEPTS:
      element = by_qname[qname]
      assert element.element_type == "concept"
      assert not element.is_monetary
      # Levers are per-month assertions — durations, never instants.
      assert element.period_type == "duration"
    # Namespace prefix as source — the value the 0024 / 0002 /
    # provisioning check_element_source widening admits and the #893
    # namespace protection auto-reserves.
    assert {e.source for e in package.elements} == {"rs-driver"}

  def test_item_types(self, package: TaxonomyPackage) -> None:
    by_qname = {e.qname: e for e in package.elements}
    for qname, expected in EXPECTED_ITEM_TYPES.items():
      assert by_qname[qname].item_type == expected, qname

  def test_structure(self, package: TaxonomyPackage) -> None:
    assert len(package.structures) == 1
    structure = package.structures[0]
    assert structure.block_type == "custom"
    assert structure.concept_arrangement == "set"
    assert structure.role_uri == ROLE_URI

  def test_presentation_arcs_bind_catalog_to_role(
    self, package: TaxonomyPackage
  ) -> None:
    arcs = package.associations
    assert len(arcs) == 4
    assert {a.to_qname for a in arcs} == EXPECTED_CONCEPTS
    for arc in arcs:
      assert arc.from_qname == "rs-driver:DriverCatalog"
      assert arc.association_type == "presentation"
      assert arc.role == ROLE_URI
    assert sorted(a.order for a in arcs) == [1.0, 2.0, 3.0, 4.0]


class TestDriverRules:
  def test_four_derive_rules(self, rules: list[RuleSpec]) -> None:
    """Regression against the RULE_PATTERN_VALUES gate: an unknown
    pattern is silently skipped at load."""
    assert len(rules) == 4
    for rule in rules:
      assert rule.rule_pattern == "Derive"
      assert rule.rule_pattern in RULE_PATTERN_VALUES
      assert rule.rule_category in RULE_CATEGORY_VALUES
      assert rule.rule_severity == "info"

  def test_targets_are_rs_gaap_anchors(self, rules: list[RuleSpec]) -> None:
    """Driver rules target the DRIVEN rs-gaap elements (cross-package,
    like rs-gaap-rollup-rules) — never the lever concepts themselves."""
    targets = {}
    for rule in rules:
      assert rule.rule_target is not None
      assert rule.rule_target.target_kind == "element"
      lever_operands = [
        v.variable_qname
        for v in rule.rule_variables
        if v.variable_qname.startswith("rs-driver:")
      ]
      assert len(lever_operands) == 1, rule.id
      targets[rule.rule_target.target_ref] = lever_operands[0]
    assert targets == EXPECTED_RULE_TARGETS

  def test_expressions_parse_with_lhs_target_first(self, rules: list[RuleSpec]) -> None:
    """Every expression parses through desugar_priors + desugar_aggregates
    (the compute-forecast preprocessing, in order) with the driven anchor
    as the sole LHS variable, listed first in ruleVariables."""
    for rule in rules:
      names = [v.variable_name for v in rule.rule_variables]
      expression, priors = desugar_priors(rule.rule_expression)
      expression, avgs = desugar_aggregates(expression)
      for base in list(priors.values()) + list(avgs.values()):
        assert base in names, f"{rule.id}: synthesized base {base!r} not declared"
      parsed = parse_arithmetic_expression(
        expression, names + list(priors) + list(avgs)
      )
      lhs = lhs_variable_names(parsed)
      assert len(lhs) == 1
      assert lhs[0] == names[0]
      assert rule.rule_variables[0].variable_qname == rule.rule_target.target_ref

  def test_compounding_rule_uses_prior_reference(self, rules: list[RuleSpec]) -> None:
    """GROWTH is the [t-1] consumer: Revenues[t] = Revenues[t-1] x (1+g)."""
    by_target = {r.rule_target.target_ref: r for r in rules if r.rule_target}
    _, priors = desugar_priors(by_target["rs-gaap:Revenues"].rule_expression)
    assert priors == {"__prior_Revenues": "Revenues"}
    # The other three rules are same-month mechanics — no prior refs.
    for target in ("rs-gaap:CostOfRevenue", "rs-gaap:ReceivablesNetCurrent"):
      _, other_priors = desugar_priors(by_target[target].rule_expression)
      assert other_priors == {}

  def test_qnames_resolve_in_core_rs_gaap_package(
    self, rules: list[RuleSpec], package: TaxonomyPackage
  ) -> None:
    """Targets AND rs-gaap operands must exist in the core rs-gaap
    package (the keep-critical tenant curation carries them — this is
    the guard that caught AccountsReceivableNetCurrent, which lives in
    the tenant-exclude list)."""
    rs_gaap_path = framework_root("rs-gaap") / "packages" / "rs-gaap" / "v1"
    raw = json.loads((rs_gaap_path / "taxonomy.jsonld").read_text())
    rs_gaap_ids = {
      node.get("@id") for node in raw.get("@graph", []) if isinstance(node, dict)
    }
    own_qnames = {e.qname for e in package.elements}
    for rule in rules:
      assert rule.rule_target is not None
      assert rule.rule_target.target_ref in rs_gaap_ids, (
        f"{rule.id}: target {rule.rule_target.target_ref} not in core rs-gaap/v1"
      )
      for var in rule.rule_variables:
        qname = var.variable_qname
        if qname.startswith("rs-driver:"):
          assert qname in own_qnames, (
            f"{rule.id}: lever operand {qname} not defined in rs-driver/v1"
          )
        else:
          assert qname.startswith("rs-gaap:")
          assert qname in rs_gaap_ids, (
            f"{rule.id}: operand {qname} not found in core rs-gaap/v1"
          )
