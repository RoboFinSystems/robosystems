"""Tests for XBRL semantic enrichment module."""

import pytest

from robosystems.adapters.sec.enrichment import (
  camel_case_to_words,
  compose_element_text,
  compose_structure_text,
)


class TestCamelCaseToWords:
  def test_simple(self):
    assert camel_case_to_words("Revenue") == "Revenue"

  def test_multi_word(self):
    assert camel_case_to_words("NetIncomeLoss") == "Net Income Loss"

  def test_long_name(self):
    result = camel_case_to_words("RevenueFromContractWithCustomerExcludingAssessedTax")
    assert result == "Revenue From Contract With Customer Excluding Assessed Tax"

  def test_consecutive_uppercase(self):
    result = camel_case_to_words("EPSBasic")
    assert result == "EPS Basic"

  def test_empty_string(self):
    assert camel_case_to_words("") == ""


class TestComposeElementText:
  def test_full_metadata(self):
    result = compose_element_text(
      "Revenue",
      {"balance": "credit", "period_type": "duration", "classification": "us-gaap"},
    )
    assert "Name: Revenue" in result
    assert "Balance: credit" in result
    assert "Period: duration" in result
    assert "Classification: us-gaap" in result

  def test_minimal_metadata(self):
    result = compose_element_text("Revenue", {})
    assert result == "Name: Revenue"

  def test_partial_metadata(self):
    result = compose_element_text("Assets", {"balance": "debit"})
    assert "Name: Assets" in result
    assert "Balance: debit" in result
    assert "Period:" not in result


class TestComposeStructureText:
  def test_name_and_definition(self):
    result = compose_structure_text("Income Statement", "0001001 - Statement - Income")
    assert "Income Statement" in result
    assert "0001001" in result

  def test_name_only(self):
    result = compose_structure_text("Balance Sheet", None)
    assert result == "Balance Sheet"

  def test_empty(self):
    result = compose_structure_text(None, None)
    assert result == ""


class TestSemanticEnricher:
  @pytest.mark.slow
  def test_embed_batch(self):
    from robosystems.adapters.sec.enrichment import SemanticEnricher

    enricher = SemanticEnricher()
    embeddings = enricher.embed_batch(["revenue", "total assets"])
    assert len(embeddings) == 2
    assert len(embeddings[0]) == 384
    assert len(embeddings[1]) == 384

  @pytest.mark.slow
  def test_match_canonical_known_element(self):
    """Known elements should match with high confidence."""
    from robosystems.adapters.sec.enrichment import SemanticEnricher

    enricher = SemanticEnricher()
    emb = enricher.embed_batch(["Revenue"])[0]
    concept_id, confidence = enricher.match_canonical(
      emb, {"qname": "us-gaap:Revenues", "period_type": "duration", "balance": "credit"}
    )
    assert concept_id == "revenue"
    assert confidence >= 0.95  # Known-element override

  @pytest.mark.slow
  def test_match_canonical_below_threshold(self):
    """Unrelated text should not match."""
    from robosystems.adapters.sec.enrichment import SemanticEnricher

    enricher = SemanticEnricher()
    emb = enricher.embed_batch(
      ["this is completely unrelated text about cooking recipes"]
    )[0]
    concept_id, confidence = enricher.match_canonical(
      emb, {"qname": "custom:CookingRecipe", "period_type": "", "balance": ""}
    )
    # Should either not match or have very low confidence
    if concept_id is not None:
      assert confidence < 0.85

  @pytest.mark.slow
  def test_match_canonical_from_query(self):
    from robosystems.adapters.sec.enrichment import SemanticEnricher

    enricher = SemanticEnricher()
    emb = enricher.embed_batch(["total revenue"])[0]
    concept = enricher.match_canonical_from_query(emb)
    assert concept is not None
    assert concept.id == "revenue"
