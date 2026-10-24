"""Tests for XBRL semantic enrichment module."""

from robosystems.adapters.sec.enrichment import compose_structure_text


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
  def test_embed_batch(self):
    from robosystems.adapters.sec.enrichment import SemanticEnricher

    enricher = SemanticEnricher()
    embeddings = enricher.embed_batch(["revenue", "total assets"])
    assert len(embeddings) == 2
    assert len(embeddings[0]) == 384
    assert len(embeddings[1]) == 384

  def test_match_canonical_from_query(self):
    from robosystems.adapters.sec.enrichment import SemanticEnricher

    enricher = SemanticEnricher()
    emb = enricher.embed_batch(["total revenue"])[0]
    concept = enricher.match_canonical_from_query(emb)
    assert concept is not None
    assert concept.id == "revenue"
