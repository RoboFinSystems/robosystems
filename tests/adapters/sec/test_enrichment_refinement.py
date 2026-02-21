"""Tests for graph-based confidence refinement in SemanticEnricher.

Tests both element refinement (_refine_element_confidence) and
structure refinement (refine_structure_confidence) methods.
"""

import pytest

from robosystems.adapters.sec.enrichment import SemanticEnricher


@pytest.fixture()
def enricher():
  """Create a SemanticEnricher with mock artifact data."""
  e = SemanticEnricher()
  # Inject mock element knowledge
  e._element_knowledge = {
    "us-gaap:Revenue": {
      "primary_statement": "IncomeStatement",
      "bfs_depth": 1,
      "pagerank": 0.8,
      "core_number": 5,
      "neighborhood_agreement": 1.0,
      "filing_count": 500,
    },
    "us-gaap:Assets": {
      "primary_statement": "BalanceSheet",
      "bfs_depth": 0,
      "pagerank": 0.9,
      "core_number": 8,
      "neighborhood_agreement": 1.0,
      "filing_count": 500,
    },
    "custom:WeirdElement": {
      "primary_statement": "IncomeStatement",
      "bfs_depth": 3,
      "pagerank": 0.05,
      "core_number": 1,
      "neighborhood_agreement": 0.0,
      "filing_count": 2,
    },
    "us-gaap:MixedSignal": {
      "primary_statement": "IncomeStatement",
      "bfs_depth": 2,
      "pagerank": 0.3,
      "core_number": 3,
      "neighborhood_agreement": 0.5,
      "filing_count": 100,
    },
  }
  # Inject mock structure profiles
  e._structure_profiles = {
    "income_statement": {
      "us-gaap:Revenue": 0.94,
      "us-gaap:CostOfGoodsSold": 0.89,
      "us-gaap:GrossProfit": 0.86,
      "us-gaap:OperatingExpenses": 0.82,
    },
    "balance_sheet": {
      "us-gaap:Assets": 0.98,
      "us-gaap:Liabilities": 0.97,
      "us-gaap:StockholdersEquity": 0.96,
    },
  }
  # Inject mock structure consensus
  e._structure_consensus = {
    "abc123hash": {
      "canonical_type": "income_statement",
      "consensus_ratio": 0.98,
      "filing_count": 500,
    },
    "def456hash": {
      "canonical_type": "balance_sheet",
      "consensus_ratio": 0.55,
      "filing_count": 10,
    },
  }
  return e


class TestElementRefinement:
  def test_perfect_agreement_boosts(self, enricher):
    """Element at 0.85 with high agreement and PageRank gets boosted."""
    result = enricher._refine_element_confidence(0.85, "us-gaap:Revenue")
    # High PageRank (0.8) + high agreement (1.0) should boost above raw
    assert result > 0.85

  def test_full_disagreement_crushes(self, enricher):
    """Element at 0.85 with zero agreement gets crushed."""
    result = enricher._refine_element_confidence(0.85, "custom:WeirdElement")
    # Zero agreement + low PageRank → heavy penalty + quadratic
    assert result < 0.60

  def test_neutral_zone_preserves(self, enricher):
    """Element with 0.5 agreement stays near raw score."""
    result = enricher._refine_element_confidence(0.90, "us-gaap:MixedSignal")
    # Neutral zone (agreement=0.5), modest PageRank boost
    # Should be close to raw but slightly adjusted
    assert 0.80 < result < 1.0

  def test_artifact_missing_graceful(self):
    """When element_knowledge is None, raw score is returned unchanged."""
    e = SemanticEnricher()
    e._element_knowledge = None
    result = e._refine_element_confidence(0.85, "us-gaap:Revenue")
    assert result == 0.85

  def test_element_not_in_artifact(self, enricher):
    """Unknown qname falls through to raw score."""
    result = enricher._refine_element_confidence(0.85, "custom:NeverSeen")
    assert result == 0.85

  def test_result_capped_at_one(self, enricher):
    """Refined score should never exceed 1.0."""
    result = enricher._refine_element_confidence(0.99, "us-gaap:Assets")
    assert result <= 1.0

  def test_result_never_negative(self, enricher):
    """Refined score should never go below 0.0."""
    result = enricher._refine_element_confidence(0.80, "custom:WeirdElement")
    assert result >= 0.0

  def test_refinement_disabled_by_flag(self, enricher, monkeypatch):
    """When XBRL_GRAPH_REFINEMENT is False, match_canonical skips refinement."""
    monkeypatch.setattr("robosystems.adapters.sec.config.XBRL_GRAPH_REFINEMENT", False)
    # _refine_element_confidence itself doesn't check the flag,
    # but match_canonical gates on it. Test the flag integration
    # by calling _refine directly (it always refines).
    # The gating happens in match_canonical, tested via integration.
    result = enricher._refine_element_confidence(0.85, "us-gaap:Revenue")
    assert result != 0.85  # Direct call still refines


class TestStructureRefinement:
  def test_composition_boosts_matching_type(self, enricher):
    """Structure with income statement elements boosts income_statement confidence."""
    elements = ["us-gaap:Revenue", "us-gaap:CostOfGoodsSold", "us-gaap:GrossProfit"]
    result_type, result_conf = enricher.refine_structure_confidence(
      "income_statement", 0.85, elements, "no_match_hash"
    )
    assert result_type == "income_statement"
    assert result_conf > 0.85

  def test_composition_classifies_untyped_structure(self, enricher):
    """Structure with no heuristic match gets classified by composition."""
    elements = ["us-gaap:Assets", "us-gaap:Liabilities", "us-gaap:StockholdersEquity"]
    result_type, result_conf = enricher.refine_structure_confidence(
      None, 0.0, elements, "no_match_hash"
    )
    assert result_type == "balance_sheet"
    assert result_conf > 0.0

  def test_consensus_boosts_matching(self, enricher):
    """High consensus ratio boosts matching type."""
    elements = ["us-gaap:Revenue"]
    result_type, result_conf = enricher.refine_structure_confidence(
      "income_statement", 0.80, elements, "abc123hash"
    )
    assert result_type == "income_statement"
    assert result_conf > 0.80

  def test_element_vote_validates(self, enricher):
    """Elements' primary_statements validate the structure type."""
    elements = ["us-gaap:Revenue"]
    result_type, result_conf = enricher.refine_structure_confidence(
      "income_statement", 0.85, elements, "no_match_hash"
    )
    assert result_type == "income_statement"
    # Element vote (IncomeStatement → income_statement) should give small boost
    assert result_conf >= 0.85

  def test_no_artifacts_graceful(self):
    """When artifacts are absent, raw scores returned unchanged."""
    e = SemanticEnricher()
    e._element_knowledge = None
    e._structure_profiles = None
    e._structure_consensus = None
    result_type, result_conf = e.refine_structure_confidence(
      "income_statement", 0.85, ["us-gaap:Revenue"], "somehash"
    )
    assert result_type == "income_statement"
    assert result_conf == 0.85

  def test_empty_elements_graceful(self, enricher):
    """Structure with no elements returns raw scores unchanged."""
    result_type, result_conf = enricher.refine_structure_confidence(
      "income_statement", 0.85, [], "somehash"
    )
    assert result_type == "income_statement"
    assert result_conf == 0.85

  def test_flag_disabled_returns_raw(self, enricher, monkeypatch):
    """When XBRL_GRAPH_REFINEMENT is False, raw scores returned."""
    monkeypatch.setattr("robosystems.adapters.sec.config.XBRL_GRAPH_REFINEMENT", False)
    elements = ["us-gaap:Revenue", "us-gaap:CostOfGoodsSold"]
    result_type, result_conf = enricher.refine_structure_confidence(
      "income_statement", 0.85, elements, "abc123hash"
    )
    assert result_type == "income_statement"
    assert result_conf == 0.85
