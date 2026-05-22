"""Tests for the fac-rules/v1 JSON-LD seed.

Pure data tests — parses the seed via
:func:`robosystems.taxonomy.loader.load_taxonomy_package` and asserts the
shape Package II.a ships: L1 cross-tree consistency identities harvested
from Charlie Hoffman's PROOF and **rewritten to rs-gaap subtotal
concepts**, element-scoped so they fire against any rendered statement
that presents the subtotal (info-block §6.1). The prior FAC-keyed,
structure-scoped 14-rule seed (which evaluated to ``skipped`` because our
facts are rs-gaap-keyed) is superseded; rollup-shaped identities now live
in ``rs-gaap-rollup-rules/v1`` (L2).
"""

from __future__ import annotations

import re

import pytest

from robosystems.taxonomy.discovery import framework_root
from robosystems.taxonomy.loader import (
  RULE_CATEGORY_VALUES,
  RULE_PATTERN_VALUES,
  load_taxonomy_package,
)
from robosystems.taxonomy.model import RuleSpec, TaxonomyPackage

SEED_PATH = framework_root("fac") / "packages" / "fac-rules" / "v1" / "taxonomy.jsonld"

VARIABLE_PATTERN = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")


@pytest.fixture(scope="module")
def package() -> TaxonomyPackage:
  return load_taxonomy_package(SEED_PATH)


@pytest.fixture(scope="module")
def rules(package: TaxonomyPackage) -> list[RuleSpec]:
  return package.rules


class TestPackageMetadata:
  def test_standard_is_fac_rules(self, package: TaxonomyPackage) -> None:
    assert package.standard == "fac-rules"

  def test_version_is_v1(self, package: TaxonomyPackage) -> None:
    assert package.version == "v1"

  def test_taxonomy_type_is_rules(self, package: TaxonomyPackage) -> None:
    assert package.taxonomy_type == "rules"

  def test_package_carries_no_elements_or_associations(
    self, package: TaxonomyPackage
  ) -> None:
    """fac-rules/v1 is rule-only. Elements are seeded by rs-gaap/v1."""
    assert package.elements == []
    assert package.associations == []


class TestRuleContent:
  def test_has_three_cross_tree_identities(self, rules: list[RuleSpec]) -> None:
    """Only the genuine cross-tree identities are kept (BS0, BS01, IS04);
    rollup-shaped Consistency rules move to rs-gaap-rollup-rules (L2) and
    FAC/SFAC6-aggregate rules have no rs-gaap subtotal target."""
    assert len(rules) == 3
    assert {r.id for r in rules} == {
      "rs-gaap-rule-bs-identity",
      "rs-gaap-rule-bs-balances",
      "rs-gaap-rule-ci-identity",
    }

  def test_only_fac_relation_category(self, rules: list[RuleSpec]) -> None:
    assert {r.rule_category for r in rules} == {"FundamentalAccountingConceptRelation"}

  def test_only_equal_to_pattern(self, rules: list[RuleSpec]) -> None:
    """Cross-tree identities are strict equalities (all subtotals must be
    present); lenient rollups live in the L2 package."""
    assert {r.rule_pattern for r in rules} == {"EqualTo"}

  def test_all_categories_and_patterns_are_known(self, rules: list[RuleSpec]) -> None:
    for rule in rules:
      assert rule.rule_category in RULE_CATEGORY_VALUES
      assert rule.rule_pattern in RULE_PATTERN_VALUES

  def test_all_rules_are_forked(self, rules: list[RuleSpec]) -> None:
    """Each transcribes an upstream PROOF Consistency assertion."""
    for rule in rules:
      assert rule.rule_origin == "forked"

  def test_all_rules_default_to_error(self, rules: list[RuleSpec]) -> None:
    for rule in rules:
      assert rule.rule_severity == "error"


class TestRuleTargets:
  def test_every_rule_is_element_scoped(self, rules: list[RuleSpec]) -> None:
    """Element-scoping (to the LHS subtotal) makes a rule fire whenever a
    rendered statement presents that concept — across every equity-form
    Balance Sheet variant — without coupling to a presentation role-URI."""
    for rule in rules:
      assert rule.rule_target is not None, f"rule {rule.id} missing rule_target"
      assert rule.rule_target.target_kind == "element"

  def test_every_target_ref_is_an_rs_gaap_qname(self, rules: list[RuleSpec]) -> None:
    """The library creator resolves element targets by ``Element.qname``,
    so the ref must be the literal rs-gaap qname (un-expanded)."""
    for rule in rules:
      assert rule.rule_target is not None
      assert rule.rule_target.target_ref.startswith("rs-gaap:"), (
        f"rule {rule.id} target_ref {rule.rule_target.target_ref} is not rs-gaap"
      )


class TestRuleExpressionsBindVariables:
  def test_every_dollar_variable_is_bound(self, rules: list[RuleSpec]) -> None:
    """Every `$Variable` in `rule_expression` must appear in
    `rule_variables` — the engine looks bindings up by name."""
    for rule in rules:
      referenced = set(VARIABLE_PATTERN.findall(rule.rule_expression))
      bound = {v.variable_name for v in rule.rule_variables}
      missing = referenced - bound
      assert not missing, (
        f"rule {rule.id}: expression references {sorted(missing)} but "
        f"rule_variables bind only {sorted(bound)}"
      )

  def test_every_variable_qname_is_rs_gaap(self, rules: list[RuleSpec]) -> None:
    """Variables resolve against rs-gaap-keyed facts — the FAC->rs-gaap
    rewrite is the whole point of the Package II.a harvest."""
    for rule in rules:
      for variable in rule.rule_variables:
        assert variable.variable_qname.startswith("rs-gaap:"), (
          f"rule {rule.id}: variable {variable.variable_name} qname "
          f"{variable.variable_qname} must be rs-gaap"
        )


class TestRuleIdShape:
  def test_ids_have_known_prefix(self, rules: list[RuleSpec]) -> None:
    """Blank-node local ids prefix with `rs-gaap-rule-` (the rewritten
    L1 identities). The writer rewrites to a deterministic UUID5."""
    for rule in rules:
      assert rule.id.startswith("rs-gaap-rule-"), (
        f"rule id {rule.id!r} should start with 'rs-gaap-rule-'"
      )

  def test_ids_are_unique(self, rules: list[RuleSpec]) -> None:
    ids = [rule.id for rule in rules]
    assert len(ids) == len(set(ids))
