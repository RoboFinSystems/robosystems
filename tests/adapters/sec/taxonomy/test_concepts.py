"""Tests for canonical taxonomy concepts."""


class TestTaxonomyStructure:
  """Test taxonomy structure and integrity."""

  def test_element_taxonomy_has_concepts(self):
    from robosystems.adapters.sec.taxonomy.balance_sheet import BALANCE_SHEET_CONCEPTS
    from robosystems.adapters.sec.taxonomy.cash_flow import CASH_FLOW_CONCEPTS
    from robosystems.adapters.sec.taxonomy.income_statement import (
      INCOME_STATEMENT_CONCEPTS,
    )

    assert len(INCOME_STATEMENT_CONCEPTS) >= 10
    assert len(BALANCE_SHEET_CONCEPTS) >= 10
    assert len(CASH_FLOW_CONCEPTS) >= 7

  def test_structure_taxonomy_has_concepts(self):
    from robosystems.adapters.sec.taxonomy.structures import STRUCTURE_CONCEPTS

    assert len(STRUCTURE_CONCEPTS) == 5

  def test_all_ids_unique(self):
    from robosystems.adapters.sec.taxonomy.balance_sheet import BALANCE_SHEET_CONCEPTS
    from robosystems.adapters.sec.taxonomy.cash_flow import CASH_FLOW_CONCEPTS
    from robosystems.adapters.sec.taxonomy.income_statement import (
      INCOME_STATEMENT_CONCEPTS,
    )
    from robosystems.adapters.sec.taxonomy.structures import STRUCTURE_CONCEPTS

    all_concepts = (
      INCOME_STATEMENT_CONCEPTS
      + BALANCE_SHEET_CONCEPTS
      + CASH_FLOW_CONCEPTS
      + STRUCTURE_CONCEPTS
    )
    ids = [c.id for c in all_concepts]
    assert len(ids) == len(set(ids)), (
      f"Duplicate IDs found: {[x for x in ids if ids.count(x) > 1]}"
    )

  def test_expected_elements_are_namespaced(self):
    """All expected_elements should have a namespace prefix like 'us-gaap:'."""
    from robosystems.adapters.sec.taxonomy.balance_sheet import BALANCE_SHEET_CONCEPTS
    from robosystems.adapters.sec.taxonomy.cash_flow import CASH_FLOW_CONCEPTS
    from robosystems.adapters.sec.taxonomy.income_statement import (
      INCOME_STATEMENT_CONCEPTS,
    )

    all_concepts = (
      INCOME_STATEMENT_CONCEPTS + BALANCE_SHEET_CONCEPTS + CASH_FLOW_CONCEPTS
    )
    for concept in all_concepts:
      for elem in concept.expected_elements:
        assert ":" in elem, (
          f"Element {elem} in concept {concept.id} missing namespace prefix"
        )

  def test_concept_categories_are_valid(self):
    from robosystems.adapters.sec.taxonomy.balance_sheet import BALANCE_SHEET_CONCEPTS
    from robosystems.adapters.sec.taxonomy.cash_flow import CASH_FLOW_CONCEPTS
    from robosystems.adapters.sec.taxonomy.income_statement import (
      INCOME_STATEMENT_CONCEPTS,
    )
    from robosystems.adapters.sec.taxonomy.structures import STRUCTURE_CONCEPTS

    valid_categories = {
      "income_statement",
      "balance_sheet",
      "cash_flow",
      "per_share",
      "structure",
    }
    all_concepts = (
      INCOME_STATEMENT_CONCEPTS
      + BALANCE_SHEET_CONCEPTS
      + CASH_FLOW_CONCEPTS
      + STRUCTURE_CONCEPTS
    )
    for concept in all_concepts:
      assert concept.category in valid_categories, (
        f"Concept {concept.id} has invalid category: {concept.category}"
      )

  def test_period_types_are_valid(self):
    from robosystems.adapters.sec.taxonomy.balance_sheet import BALANCE_SHEET_CONCEPTS
    from robosystems.adapters.sec.taxonomy.cash_flow import CASH_FLOW_CONCEPTS
    from robosystems.adapters.sec.taxonomy.income_statement import (
      INCOME_STATEMENT_CONCEPTS,
    )

    all_concepts = (
      INCOME_STATEMENT_CONCEPTS + BALANCE_SHEET_CONCEPTS + CASH_FLOW_CONCEPTS
    )
    for concept in all_concepts:
      assert concept.period_type in ("duration", "instant"), (
        f"Concept {concept.id} has invalid period_type: {concept.period_type}"
      )

  def test_element_taxonomy_embeddings(self):
    """Verify embeddings are computed correctly (loads model)."""
    from robosystems.adapters.sec.taxonomy import get_element_taxonomy

    taxonomy = get_element_taxonomy()
    assert len(taxonomy) >= 30
    for concept in taxonomy:
      assert concept.embedding is not None, f"Concept {concept.id} missing embedding"
      assert len(concept.embedding) == 384, (
        f"Concept {concept.id} embedding has wrong dimension: {len(concept.embedding)}"
      )

  def test_structure_taxonomy_embeddings(self):
    """Verify structure embeddings are computed correctly (loads model)."""
    from robosystems.adapters.sec.taxonomy import get_structure_taxonomy

    taxonomy = get_structure_taxonomy()
    assert len(taxonomy) == 5
    for concept in taxonomy:
      assert concept.embedding is not None
      assert len(concept.embedding) == 384


class TestCanonicalElementMap:
  """The curated qname table staging stamps onto Element."""

  def test_every_qname_is_a_standard_namespace(self):
    import re

    from robosystems.adapters.sec.taxonomy import canonical_element_map

    pattern = re.compile(r"^(us-gaap|ifrs-full):[A-Za-z][A-Za-z0-9]*$")
    bad = [q for q in canonical_element_map() if not pattern.match(q)]
    assert bad == []

  def test_every_element_concept_has_a_qname(self):
    from robosystems.adapters.sec.taxonomy import canonical_element_map
    from robosystems.adapters.sec.taxonomy.balance_sheet import BALANCE_SHEET_CONCEPTS
    from robosystems.adapters.sec.taxonomy.cash_flow import CASH_FLOW_CONCEPTS
    from robosystems.adapters.sec.taxonomy.income_statement import (
      INCOME_STATEMENT_CONCEPTS,
    )

    mapped = set(canonical_element_map().values())
    ids = {
      c.id
      for c in INCOME_STATEMENT_CONCEPTS + BALANCE_SHEET_CONCEPTS + CASH_FLOW_CONCEPTS
    }
    assert ids == mapped

  def test_a_qname_listed_twice_is_refused(self, monkeypatch):
    import pytest

    from robosystems.adapters.sec import taxonomy
    from robosystems.adapters.sec.taxonomy import cash_flow
    from robosystems.adapters.sec.taxonomy.concepts import CanonicalConcept

    duplicate = CanonicalConcept(
      id="duplicate",
      display_name="Duplicate",
      category="cash_flow",
      description="",
      expected_elements=("us-gaap:Assets",),
    )
    monkeypatch.setattr(
      cash_flow, "CASH_FLOW_CONCEPTS", (*cash_flow.CASH_FLOW_CONCEPTS, duplicate)
    )
    with pytest.raises(ValueError, match="us-gaap:Assets"):
      taxonomy.canonical_element_map()

  def test_equity_and_real_estate_lines_have_their_own_concepts(self):
    """Each of these was labelled as debt or cash before the table existed."""
    from robosystems.adapters.sec.taxonomy import canonical_element_map

    mapping = canonical_element_map()
    assert mapping["us-gaap:AdditionalPaidInCapital"] == "additional_paid_in_capital"
    assert mapping["us-gaap:CommonStockValue"] == "common_stock_value"
    assert mapping["us-gaap:MinorityInterest"] == "noncontrolling_interest"
    assert (
      mapping["us-gaap:RealEstateInvestmentPropertyAccumulatedDepreciation"]
      == "real_estate_accumulated_depreciation"
    )
    assert "us-gaap:Land" not in mapping
    assert "us-gaap:CommitmentsAndContingencies" not in mapping
