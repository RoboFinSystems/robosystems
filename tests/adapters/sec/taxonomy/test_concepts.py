"""Tests for canonical taxonomy concepts."""

import pytest


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

  @pytest.mark.slow
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

  @pytest.mark.slow
  def test_structure_taxonomy_embeddings(self):
    """Verify structure embeddings are computed correctly (loads model)."""
    from robosystems.adapters.sec.taxonomy import get_structure_taxonomy

    taxonomy = get_structure_taxonomy()
    assert len(taxonomy) == 5
    for concept in taxonomy:
      assert concept.embedding is not None
      assert len(concept.embedding) == 384
