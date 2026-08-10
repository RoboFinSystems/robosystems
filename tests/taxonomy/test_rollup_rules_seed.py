"""Tests for the rs-gaap-rollup-rules/v1 L2 package.

One ``RollUp`` rule per calc-arc subtotal parent in rs-gaap-calculations/v1.
The package is **hand-maintained** — its generator was retired in #898 (the
calc source collapsed per-subtotal role URIs into three statement-level
Networks, and the rule ``@id`` slugs are hand-abbreviated). These tests pin
the committed artifact's shape and hold it honest against the live calc
source: :class:`TestCalcSourceCoverage` is the drift gate that fails when the
calc DAG gains or loses a subtotal parent the package doesn't cover.
"""

from __future__ import annotations

import json
from collections import defaultdict

import pytest

from robosystems.operations.information_block.rules.expressions import (
  build_rollup_expression,
)
from robosystems.taxonomy.discovery import framework_root
from robosystems.taxonomy.loader import (
  RULE_CATEGORY_VALUES,
  RULE_PATTERN_VALUES,
  load_taxonomy_package,
)
from robosystems.taxonomy.model import RuleSpec, TaxonomyPackage

SEED_PATH = (
  framework_root("rs-gaap")
  / "packages"
  / "rs-gaap-rollup-rules"
  / "v1"
  / "taxonomy.jsonld"
)

CALC_PATH = (
  framework_root("rs-gaap")
  / "packages"
  / "rs-gaap-calculations"
  / "v1"
  / "taxonomy.jsonld"
)


def _local(qname: str) -> str:
  return qname.split(":", 1)[1] if ":" in qname else qname


@pytest.fixture(scope="module")
def calc_children() -> dict[str, list[tuple[str, float]]]:
  """Live calc source: subtotal parent -> its children in arc order.

  Reads the current ``from`` / ``to`` / ``associationType`` / ``weight`` /
  ``order`` keys. ``derivation`` arcs are excluded — only ``calculation``
  arcs foot a subtotal.
  """
  graph = json.loads(CALC_PATH.read_text())["@graph"]
  ordered: dict[str, list[tuple[str, float, float]]] = defaultdict(list)
  for node in graph:
    if "from" not in node or node.get("associationType") != "calculation":
      continue
    ordered[node["from"]["@id"]].append(
      (
        node["to"]["@id"],
        float(node.get("weight", 1.0)),
        float(node.get("order", 0.0)),
      )
    )
  return {
    parent: [(child, weight) for child, weight, _ in sorted(arcs, key=lambda t: t[2])]
    for parent, arcs in ordered.items()
  }


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


class TestCalcSourceCoverage:
  """Drift gate: the package must cover exactly the live calc DAG's parents.

  This is what the retired generator used to guarantee by construction.
  Adding a subtotal to ``rs-gaap-calculations/v1`` without adding its rule
  here leaves that subtotal unverified in every tenant; removing one leaves
  a rule whose parent no longer foots.
  """

  def test_covers_exactly_the_calc_parents(
    self, rules: list[RuleSpec], calc_children: dict[str, list[tuple[str, float]]]
  ) -> None:
    covered = {r.rule_target.target_ref for r in rules if r.rule_target is not None}
    expected = set(calc_children)
    assert covered == expected, (
      f"rollup-rule drift — parents in calc source with no rule: "
      f"{sorted(expected - covered)}; rules with no calc parent: "
      f"{sorted(covered - expected)}"
    )

  def test_children_match_the_calc_arcs(
    self, rules: list[RuleSpec], calc_children: dict[str, list[tuple[str, float]]]
  ) -> None:
    """Each rule's frozen child enumeration matches the live arcs.

    The engine derives RollUp children from live calc arcs on the normal
    path, but falls back to this enumeration when the parent has no calc
    children in the merged DAG — where a stale list binds nothing, scores
    the missing children as 0, and reports a false failure.
    """
    for rule in rules:
      assert rule.rule_target is not None
      parent = rule.rule_target.target_ref
      expected = [child for child, _weight in calc_children[parent]]
      actual = [v.variable_qname for v in rule.rule_variables[1:]]
      assert actual == expected, f"{rule.id}: children drifted from the calc source"

  def test_expressions_match_the_shared_builder(
    self, rules: list[RuleSpec], calc_children: dict[str, list[tuple[str, float]]]
  ) -> None:
    """Hand-maintained expressions stay byte-identical to what tenant
    auto-rule emission produces for the same arcs, so a seeded rule and an
    authored one evaluate identically."""
    for rule in rules:
      assert rule.rule_target is not None
      parent = rule.rule_target.target_ref
      expected = build_rollup_expression(
        _local(parent),
        [(_local(child), weight) for child, weight in calc_children[parent]],
      )
      assert rule.rule_expression == expected, f"{rule.id}: expression drifted"
