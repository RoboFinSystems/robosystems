"""Tests for the fac-rules/v1 JSON-LD seed.

Pure data tests — parses the seed via
:func:`robosystems.taxonomy.loader.load_taxonomy_package`
and asserts the shape Phase δ.3 ships: 14 forked rules drawn from three
rule categories and five rule patterns, every `$Variable` bound via
`rule_variables`, every rule scoped to a known fac-calculations /
fac-presentation Structure role URI.
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
    """fac-rules/v1 is rule-only. Elements and associations are seeded
    by the sibling fac-calculations/v1 and fac-presentation/v1 packages.
    """
    assert package.elements == []
    assert package.associations == []


class TestRuleCount:
  def test_has_fourteen_rules(self, rules: list[RuleSpec]) -> None:
    """Phase δ.3 expands the seed to 14 rules: 7 from Phase δ.2 plus
    2 harvested from XBRL formula linkbases (IS / CNA identities) and
    5 from disclosure-mechanics definition linkbases."""
    assert len(rules) == 14


class TestRuleEnums:
  def test_all_categories_are_known(self, rules: list[RuleSpec]) -> None:
    for rule in rules:
      assert rule.rule_category in RULE_CATEGORY_VALUES

  def test_all_patterns_are_known(self, rules: list[RuleSpec]) -> None:
    for rule in rules:
      assert rule.rule_pattern in RULE_PATTERN_VALUES

  def test_seed_covers_three_categories(self, rules: list[RuleSpec]) -> None:
    """Three of eight cm:VerificationRule categories seeded in Phase δ.3:
    the two from δ.2 plus DisclosureMechanicsRule harvested from the
    Seattle Method dm definition linkbases."""
    categories = {rule.rule_category for rule in rules}
    assert categories == {
      "FundamentalAccountingConceptRelation",
      "ReportLevelModelStructureRule",
      "DisclosureMechanicsRule",
    }

  def test_seed_covers_five_patterns(self, rules: list[RuleSpec]) -> None:
    """Five of ten cm:BusinessRulePattern mechanisms used in Phase δ.2."""
    patterns = {rule.rule_pattern for rule in rules}
    assert patterns == {
      "EqualTo",
      "RollForward",
      "RollUp",
      "Exists",
      "CoExists",
    }


class TestRuleOrigin:
  def test_all_rules_are_forked(self, rules: list[RuleSpec]) -> None:
    """Every seeded rule transcribes an upstream Seattle Method artifact
    — no 'native' authorship before Phase δ.3's engine ships."""
    for rule in rules:
      assert rule.rule_origin == "forked"


class TestRuleSeverity:
  def test_all_rules_default_to_error(self, rules: list[RuleSpec]) -> None:
    """The seed omits `ruleSeverity`; the Pydantic default populates all
    7 rules to `error` — these are hard constraints."""
    for rule in rules:
      assert rule.rule_severity == "error"


class TestRuleTargets:
  def test_every_rule_targets_a_structure(self, rules: list[RuleSpec]) -> None:
    for rule in rules:
      assert rule.rule_target is not None, (
        f"rule {rule.id} missing rule_target — Phase δ.2 seeds structure-scoped "
        f"rules only"
      )
      assert rule.rule_target.target_kind == "structure"

  def test_every_target_ref_is_a_fac_role_uri(self, rules: list[RuleSpec]) -> None:
    """Rules reference Structures seeded by fac-calculations/v1 and
    fac-presentation/v1 — their role URIs are under cm-roles/roles/{fac-*}/."""
    prefix = "http://www.xbrlsite.com/seattlemethod/conceptual-model/cm-roles/roles/"
    for rule in rules:
      assert rule.rule_target is not None
      assert rule.rule_target.target_ref.startswith(prefix), (
        f"rule {rule.id} has unexpected target_ref {rule.rule_target.target_ref}"
      )


class TestRuleExpressionsBindVariables:
  def test_every_dollar_variable_is_bound(self, rules: list[RuleSpec]) -> None:
    """Every `$Variable` in `rule_expression` must appear in
    `rule_variables` — Phase δ.3's engine looks bindings up by name, and
    an unbound variable is a seed bug that would only fail at runtime."""
    for rule in rules:
      referenced = set(VARIABLE_PATTERN.findall(rule.rule_expression))
      bound = {v.variable_name for v in rule.rule_variables}
      missing = referenced - bound
      assert not missing, (
        f"rule {rule.id}: expression references {sorted(missing)} but "
        f"rule_variables bind only {sorted(bound)}"
      )

  def test_every_bound_variable_has_a_qname(self, rules: list[RuleSpec]) -> None:
    for rule in rules:
      for variable in rule.rule_variables:
        assert variable.variable_qname, (
          f"rule {rule.id}: variable {variable.variable_name} missing qname"
        )
        assert ":" in variable.variable_qname, (
          f"rule {rule.id}: variable qname {variable.variable_qname} must be "
          f"prefix:local"
        )


class TestRuleIdShape:
  def test_ids_have_known_prefix(self, rules: list[RuleSpec]) -> None:
    """Blank-node local ids prefix with `fac-rule-` (Phase δ.2 rules) or
    `sfac6-rule-` (Phase δ.3 formula/dm harvested rules). The writer
    rewrites to a deterministic UUID5 at seed time."""
    for rule in rules:
      assert "fac-rule-" in rule.id or "sfac6-rule-" in rule.id, (
        f"rule id {rule.id!r} should contain 'fac-rule-' or 'sfac6-rule-'"
      )

  def test_ids_are_unique(self, rules: list[RuleSpec]) -> None:
    ids = [rule.id for rule in rules]
    assert len(ids) == len(set(ids))
