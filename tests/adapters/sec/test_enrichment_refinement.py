"""Tests for graph-based structure confidence refinement in SemanticEnricher."""

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
      "disclosure_type": "RevenuefromExternalCustomersAttributedToForeignCountriesByGeographicArea",
    },
    "us-gaap:Assets": {
      "primary_statement": "BalanceSheet",
      "bfs_depth": 0,
      "pagerank": 0.9,
      "core_number": 8,
      "neighborhood_agreement": 1.0,
      "filing_count": 500,
      "disclosure_type": "AssetsRollUp",
    },
    "custom:WeirdElement": {
      "primary_statement": "IncomeStatement",
      "bfs_depth": 3,
      "pagerank": 0.05,
      "core_number": 1,
      "neighborhood_agreement": 0.0,
      "filing_count": 2,
      "disclosure_type": None,
    },
    "us-gaap:MixedSignal": {
      "primary_statement": "IncomeStatement",
      "bfs_depth": 2,
      "pagerank": 0.3,
      "core_number": 3,
      "neighborhood_agreement": 0.5,
      "filing_count": 100,
      "disclosure_type": None,
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
