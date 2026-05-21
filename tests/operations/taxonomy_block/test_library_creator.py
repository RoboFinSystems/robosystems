"""Tests for the library_creator operations module (Phase 2.7).

All tests use MagicMock sessions — the module's three functions translate
TaxonomyPackage → session.execute(pg_insert(...)) calls. We verify:
  - Deterministic ID helpers produce stable, unique values.
  - Phase 1 writes the right number of rows for a given package.
  - Elements with disallowed sources are skipped.
  - Phase 2 DB-resolves qnames (via session.execute scalar) and skips
    unresolved arcs.
  - Phase 3 resolves polymorphic rule targets and skips unsupported kinds.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from robosystems.operations.taxonomy_block.library_creator import (
  _association_id,
  _element_id,
  _rule_id,
  _structure_id,
  _taxonomy_id,
  _trait_id,
  create_library_arcs,
  create_library_rules,
  create_library_taxonomy_elements,
)
from robosystems.taxonomy.model import (
  AssociationSpec,
  ElementSpec,
  LabelSpec,
  ReferenceSpec,
  RuleSpec,
  RuleTargetSpec,
  RuleVariableSpec,
  StructureSpec,
  TaxonomyPackage,
  TraitAssignmentSpec,
  TraitSpec,
)

# ── ID helper stability tests ─────────────────────────────────────────────────


class TestIdHelpers:
  def test_taxonomy_id_stable(self) -> None:
    id1 = _taxonomy_id("fac", "v1")
    id2 = _taxonomy_id("fac", "v1")
    assert id1 == id2

  def test_taxonomy_id_different_standard(self) -> None:
    assert _taxonomy_id("fac", "v1") != _taxonomy_id("rs-gaap", "v1")

  def test_element_id_stable(self) -> None:
    id1 = _element_id("http://example.com/fac#", "fac:Assets")
    id2 = _element_id("http://example.com/fac#", "fac:Assets")
    assert id1 == id2

  def test_element_id_different_qname(self) -> None:
    assert _element_id("http://x.com#", "fac:A") != _element_id(
      "http://x.com#", "fac:B"
    )

  def test_structure_id_stable(self) -> None:
    assert _structure_id("http://role/bs") == _structure_id("http://role/bs")

  def test_association_id_unique(self) -> None:
    id1 = _association_id("s1", "e1", "e2", "presentation")
    id2 = _association_id("s1", "e1", "e3", "presentation")
    assert id1 != id2

  def test_rule_id_stable(self) -> None:
    assert _rule_id("fac", "fac-rule-bs-identity") == _rule_id(
      "fac", "fac-rule-bs-identity"
    )

  def test_rule_id_different_standard(self) -> None:
    assert _rule_id("fac", "rule-1") != _rule_id("rs-gaap", "rule-1")

  def test_trait_id_stable(self) -> None:
    id1 = _trait_id("elementsOfFinancialStatements", "asset", "system")
    id2 = _trait_id("elementsOfFinancialStatements", "asset", "system")
    assert id1 == id2


# ── Helpers to build minimal TaxonomyPackage fixtures ─────────────────────────


def _make_element(
  qname: str = "fac:Assets",
  source: str = "fac",
  labels: int = 1,
  refs: int = 0,
) -> ElementSpec:
  return ElementSpec(
    qname=qname,
    namespace="fac",
    namespace_uri="http://fac.example.com/",
    name=qname.split(":")[1],
    balance_type="debit",
    period_type="instant",
    source=source,
    labels=[LabelSpec(role="standard", language="en", text=qname)] * labels,
    references=(
      [ReferenceSpec(citation="FASB ASC 210", ref_type="ASC")] * refs if refs else []
    ),
  )


def _make_package(
  *,
  elements: list[ElementSpec] | None = None,
  structures: list[StructureSpec] | None = None,
  associations: list[AssociationSpec] | None = None,
  traits: list[TraitSpec] | None = None,
  trait_assignments: list[TraitAssignmentSpec] | None = None,
  rules: list[RuleSpec] | None = None,
) -> TaxonomyPackage:
  return TaxonomyPackage(
    name="Test Taxonomy",
    standard="fac",
    version="v1",
    namespace_uri="http://fac.example.com/",
    taxonomy_type="reporting_standard",
    elements=elements or [],
    structures=structures or [],
    associations=associations or [],
    traits=traits or [],
    trait_assignments=trait_assignments or [],
    rules=rules or [],
  )


# ── Phase 1 tests ─────────────────────────────────────────────────────────────


class TestCreateLibraryTaxonomyElements:
  def test_writes_taxonomy_row(self) -> None:
    session = MagicMock()
    package = _make_package()
    tax_id, counts = create_library_taxonomy_elements(session, package)
    assert tax_id == _taxonomy_id("fac", "v1")
    assert counts["taxonomies"] == 1
    assert session.execute.called

  def test_writes_one_element_with_label(self) -> None:
    session = MagicMock()
    package = _make_package(elements=[_make_element(labels=1)])
    _, counts = create_library_taxonomy_elements(session, package)
    assert counts["elements"] == 1
    assert counts["labels"] == 1
    assert counts["references"] == 0

  def test_writes_element_with_reference(self) -> None:
    session = MagicMock()
    package = _make_package(elements=[_make_element(refs=1, labels=0)])
    _, counts = create_library_taxonomy_elements(session, package)
    assert counts["elements"] == 1
    assert counts["references"] == 1

  def test_skips_disallowed_source(self) -> None:
    session = MagicMock()
    allowed = _make_element(qname="fac:Assets", source="fac")
    disallowed = _make_element(qname="xbrl:item", source="xbrl-infrastructure")
    package = _make_package(elements=[allowed, disallowed])
    _, counts = create_library_taxonomy_elements(session, package)
    assert counts["elements"] == 1

  def test_writes_structures_and_default(self) -> None:
    session = MagicMock()
    structure = StructureSpec(
      name="Balance Sheet",
      role_uri="http://role/bs",
      block_type="balance_sheet",
    )
    package = _make_package(structures=[structure])
    _, counts = create_library_taxonomy_elements(session, package)
    assert counts["structures"] == 1

  def test_writes_classifications(self) -> None:
    session = MagicMock()
    cls = TraitSpec(
      category="elementsOfFinancialStatements",
      identifier="asset",
      source="fac-metamodel",
    )
    package = _make_package(traits=[cls])
    _, counts = create_library_taxonomy_elements(session, package)
    assert counts["traits"] == 1

  def test_empty_package_writes_only_taxonomy(self) -> None:
    session = MagicMock()
    package = _make_package()
    _, counts = create_library_taxonomy_elements(session, package)
    assert counts["elements"] == 0
    assert counts["labels"] == 0
    assert counts["structures"] == 0
    assert counts["traits"] == 0


# ── Phase 2 tests ─────────────────────────────────────────────────────────────


class TestCreateLibraryArcs:
  def _session_with_element_map(self, qname_to_id: dict[str, str]) -> MagicMock:
    """Mock session where _bulk_resolve_element_ids returns the given map.

    _bulk_resolve_element_ids calls session.execute(...).all() and expects a
    sequence of (qname, id) tuples.  _resolve_trait_id still calls
    session.execute(...).scalar_one_or_none(), so both can be configured
    independently on the same return_value mock.
    """
    session = MagicMock()
    session.execute.return_value.all.return_value = list(qname_to_id.items())
    return session

  def test_skips_unresolved_qnames(self) -> None:
    session = self._session_with_element_map({})
    assoc = AssociationSpec(
      from_qname="fac:Assets",
      to_qname="fac:CashAndCashEquivalents",
      association_type="presentation",
      arcrole="http://xbrl.org/arcrole/presentation",
    )
    package = _make_package(associations=[assoc])
    counts = create_library_arcs(session, package)
    assert counts["associations"] == 0
    assert counts["associations_skipped"] == 1

  def test_writes_association_when_resolved(self) -> None:
    session = self._session_with_element_map(
      {"fac:Assets": "elem_from_id", "fac:Cash": "elem_to_id"}
    )
    assoc = AssociationSpec(
      from_qname="fac:Assets",
      to_qname="fac:Cash",
      association_type="presentation",
      arcrole="http://xbrl.org/arcrole/presentation",
    )
    package = _make_package(associations=[assoc])
    counts = create_library_arcs(session, package)
    assert counts["associations"] == 1
    assert counts["associations_skipped"] == 0

  def test_skips_unresolved_classification_assignment(self) -> None:
    session = self._session_with_element_map({})
    asn = TraitAssignmentSpec(
      element_qname="fac:Assets",
      category="elementsOfFinancialStatements",
      identifier="asset",
    )
    package = _make_package(trait_assignments=[asn])
    counts = create_library_arcs(session, package)
    assert counts["trait_assignments"] == 0
    assert counts["trait_assignments_skipped"] == 1

  def test_writes_classification_assignment_when_resolved(self) -> None:
    session = self._session_with_element_map({"fac:Assets": "elem_id"})
    session.execute.return_value.scalar_one_or_none.return_value = "cls_id"
    asn = TraitAssignmentSpec(
      element_qname="fac:Assets",
      category="elementsOfFinancialStatements",
      identifier="asset",
    )
    package = _make_package(trait_assignments=[asn])
    counts = create_library_arcs(session, package)
    assert counts["trait_assignments"] == 1
    assert counts["trait_assignments_skipped"] == 0


# ── Phase 3 tests ─────────────────────────────────────────────────────────────


class TestCreateLibraryRules:
  def test_writes_global_rule(self) -> None:
    session = MagicMock()
    rule = RuleSpec(
      id="fac-rule-bs-identity",
      rule_category="FundamentalAccountingConceptRelation",
      rule_pattern="EqualTo",
      rule_expression="$Assets = $Liabilities + $Equity",
      rule_variables=[
        RuleVariableSpec(variable_name="Assets", variable_qname="fac:Assets"),
      ],
    )
    package = _make_package(rules=[rule])
    counts = create_library_rules(session, package)
    assert counts["rules"] == 1
    assert counts["rules_skipped"] == 0

  def test_writes_structure_targeted_rule_when_resolved(self) -> None:
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = "struct_id"
    rule = RuleSpec(
      id="fac-rule-bs-structure",
      rule_category="FundamentalAccountingConceptRelation",
      rule_pattern="RollUp",
      rule_expression="$Assets = $CurrentAssets + $NonCurrentAssets",
      rule_target=RuleTargetSpec(
        target_kind="structure",
        target_ref="http://role/bs",
      ),
    )
    package = _make_package(rules=[rule])
    counts = create_library_rules(session, package)
    assert counts["rules"] == 1

  def test_skips_structure_targeted_rule_when_unresolved(self) -> None:
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = None
    rule = RuleSpec(
      id="fac-rule-missing",
      rule_category="FundamentalAccountingConceptRelation",
      rule_pattern="EqualTo",
      rule_expression="$X = $Y",
      rule_target=RuleTargetSpec(
        target_kind="structure",
        target_ref="http://role/does-not-exist",
      ),
    )
    package = _make_package(rules=[rule])
    counts = create_library_rules(session, package)
    assert counts["rules"] == 0
    assert counts["rules_skipped"] == 1

  def test_skips_association_targeted_rule(self) -> None:
    session = MagicMock()
    rule = RuleSpec(
      id="fac-rule-assoc",
      rule_category="XBRLTechnicalSyntaxRule",
      rule_pattern="Exists",
      rule_expression="true",
      rule_target=RuleTargetSpec(
        target_kind="association",
        target_ref="http://some/arc",
      ),
    )
    package = _make_package(rules=[rule])
    counts = create_library_rules(session, package)
    assert counts["rules"] == 0
    assert counts["rules_skipped"] == 1
