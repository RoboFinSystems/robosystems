"""Tests for disclosure-level composition profile system.

Tests ArcExtractor.extract_disclosure_compositions(), DisclosureProfileBuilder,
SemanticEnricher disclosure classification methods, and DEI detection.
"""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from robosystems.adapters.sec.knowledge.artifact import DisclosureProfileBuilder
from robosystems.adapters.sec.knowledge.extractors import ArcExtractor

# ---------------------------------------------------------------------------
# ArcExtractor.extract_disclosure_compositions()
# ---------------------------------------------------------------------------


class TestExtractDisclosureCompositions:
  def test_returns_list_of_tuples(self, sec_duckdb):
    """Returns list of (structure_id, disclosure_type, def_hash, [qnames])."""
    extractor = ArcExtractor(sec_duckdb)
    compositions = extractor.extract_disclosure_compositions()
    assert isinstance(compositions, list)
    assert len(compositions) > 0
    for item in compositions:
      assert len(item) == 4
      sid, dtype, dhash, qnames = item
      assert isinstance(sid, str)
      assert isinstance(dtype, str)
      assert isinstance(dhash, str)
      assert isinstance(qnames, list)

  def test_groups_by_disclosure_type(self, sec_duckdb):
    """Compositions include disclosure type from disclosure_mechanics."""
    extractor = ArcExtractor(sec_duckdb)
    compositions = extractor.extract_disclosure_compositions()
    disclosure_types = {c[1] for c in compositions}
    # Our fixture has AssetsRollUp and GoodwillRollForward classifications
    assert (
      "AssetsRollUp" in disclosure_types or "GoodwillRollForward" in disclosure_types
    )

  def test_element_qnames_sorted(self, sec_duckdb):
    """Element qnames within each composition are sorted."""
    extractor = ArcExtractor(sec_duckdb)
    compositions = extractor.extract_disclosure_compositions()
    for _sid, _dtype, _dhash, qnames in compositions:
      assert qnames == sorted(qnames)

  def test_missing_classification_table_returns_empty(self, no_classification_duckdb):
    """Returns empty list when Classification table does not exist."""
    extractor = ArcExtractor(no_classification_duckdb)
    compositions = extractor.extract_disclosure_compositions()
    assert compositions == []

  def test_empty_database_returns_empty(self, empty_duckdb):
    """Returns empty list when no structures have classifications."""
    extractor = ArcExtractor(empty_duckdb)
    compositions = extractor.extract_disclosure_compositions()
    assert compositions == []


# ---------------------------------------------------------------------------
# DisclosureProfileBuilder
# ---------------------------------------------------------------------------


class TestDisclosureProfileBuilder:
  def test_build_produces_both_parquets(self, sec_duckdb, tmp_path, monkeypatch):
    """Builder creates both profile and consensus parquet files."""
    monkeypatch.setattr(
      "robosystems.config.storage.shared.get_artifact_path",
      lambda name: str(tmp_path / f"{name}.parquet"),
    )
    builder = DisclosureProfileBuilder()
    profiles_path, consensus_path = builder.build(sec_duckdb)
    assert Path(profiles_path).exists()
    assert Path(consensus_path).exists()

  def test_profile_schema(self, sec_duckdb, tmp_path, monkeypatch):
    """Profile parquet has correct columns."""
    monkeypatch.setattr(
      "robosystems.config.storage.shared.get_artifact_path",
      lambda name: str(tmp_path / f"{name}.parquet"),
    )
    builder = DisclosureProfileBuilder()
    profiles_path, _ = builder.build(sec_duckdb)
    with open(profiles_path, "rb") as f:
      table = pq.read_table(f)
    expected = {
      "disclosure_type",
      "qname",
      "frequency",
      "weighted_score",
      "structure_count",
    }
    assert set(table.column_names) == expected

  def test_consensus_schema(self, sec_duckdb, tmp_path, monkeypatch):
    """Consensus parquet has correct columns."""
    monkeypatch.setattr(
      "robosystems.config.storage.shared.get_artifact_path",
      lambda name: str(tmp_path / f"{name}.parquet"),
    )
    builder = DisclosureProfileBuilder()
    _, consensus_path = builder.build(sec_duckdb)
    with open(consensus_path, "rb") as f:
      table = pq.read_table(f)
    expected = {"definition_hash", "disclosure_type", "consensus_ratio", "filing_count"}
    assert set(table.column_names) == expected

  def test_profile_frequencies_bounded(self, sec_duckdb, tmp_path, monkeypatch):
    """Profile frequencies are between 0 and 1."""
    monkeypatch.setattr(
      "robosystems.config.storage.shared.get_artifact_path",
      lambda name: str(tmp_path / f"{name}.parquet"),
    )
    builder = DisclosureProfileBuilder()
    profiles_path, _ = builder.build(sec_duckdb)
    with open(profiles_path, "rb") as f:
      table = pq.read_table(f)
    freqs = table.column("frequency").to_pylist()
    for freq in freqs:
      assert 0.0 <= freq <= 1.0

  def test_weighted_score_uses_pagerank(self, sec_duckdb, tmp_path, monkeypatch):
    """Weighted scores incorporate PageRank when element_knowledge available."""
    # Write a mock element_knowledge with non-zero pagerank
    ek_path = tmp_path / "element_knowledge.parquet"
    ek_table = pa.table(
      {
        "qname": pa.array(
          ["us-gaap:Liabilities", "us-gaap:StockholdersEquity"], type=pa.string()
        ),
        "pagerank": pa.array([0.8, 0.3], type=pa.float64()),
      }
    )
    with open(ek_path, "wb") as f:
      pq.write_table(ek_table, f)

    monkeypatch.setattr(
      "robosystems.config.storage.shared.get_artifact_path",
      lambda name: str(tmp_path / f"{name}.parquet"),
    )
    builder = DisclosureProfileBuilder()
    profiles_path, _ = builder.build(sec_duckdb)
    with open(profiles_path, "rb") as f:
      table = pq.read_table(f)
    cols = table.to_pydict()

    # Check that weighted_score >= frequency for elements with pagerank >= 0
    for i in range(len(cols["qname"])):
      freq = cols["frequency"][i]
      ws = cols["weighted_score"][i]
      assert ws >= freq - 0.001

  def test_weighted_score_degrades_without_pagerank(
    self, sec_duckdb, tmp_path, monkeypatch
  ):
    """Weighted scores equal frequency when element_knowledge unavailable."""
    monkeypatch.setattr(
      "robosystems.config.storage.shared.get_artifact_path",
      lambda name: str(tmp_path / f"{name}.parquet"),
    )
    builder = DisclosureProfileBuilder()
    profiles_path, _ = builder.build(sec_duckdb)
    with open(profiles_path, "rb") as f:
      table = pq.read_table(f)
    cols = table.to_pydict()

    for i in range(len(cols["qname"])):
      assert abs(cols["weighted_score"][i] - cols["frequency"][i]) < 0.001

  def test_empty_database_handles_gracefully(self, empty_duckdb, tmp_path, monkeypatch):
    """Builder handles empty database without crashing."""
    monkeypatch.setattr(
      "robosystems.config.storage.shared.get_artifact_path",
      lambda name: str(tmp_path / f"{name}.parquet"),
    )
    builder = DisclosureProfileBuilder()
    profiles_path, consensus_path = builder.build(empty_duckdb)
    assert Path(profiles_path).exists()
    assert Path(consensus_path).exists()


# ---------------------------------------------------------------------------
# SemanticEnricher.classify_disclosure_by_composition()
# ---------------------------------------------------------------------------


class TestDisclosureCompositionClassification:
  def _make_enricher_with_profiles(self, profiles):
    """Create an enricher with pre-set disclosure profiles."""
    from robosystems.adapters.sec.enrichment import SemanticEnricher

    enricher = SemanticEnricher.__new__(SemanticEnricher)
    enricher._disclosure_profiles = profiles
    return enricher

  def test_classifies_matching_structure(self):
    """Structure with known disclosure elements gets classified."""
    profiles = {
      "AssetsRollUp": {"us-gaap:Assets": 1.0, "us-gaap:Liabilities": 0.9},
    }
    enricher = self._make_enricher_with_profiles(profiles)
    dtype, score = enricher.classify_disclosure_by_composition(
      ["us-gaap:Assets", "us-gaap:Liabilities"]
    )
    assert dtype == "AssetsRollUp"
    assert score > 0.0

  def test_returns_none_for_unknown_elements(self):
    """Structure with unknown elements returns (None, 0.0)."""
    profiles = {
      "AssetsRollUp": {"us-gaap:Assets": 1.0},
    }
    enricher = self._make_enricher_with_profiles(profiles)
    dtype, score = enricher.classify_disclosure_by_composition(
      ["custom:TotallyUnknown", "custom:AnotherUnknown"]
    )
    assert dtype is None
    assert score == 0.0

  def test_returns_none_when_no_profiles(self):
    """Returns (None, 0.0) when profiles artifact unavailable."""
    enricher = self._make_enricher_with_profiles(None)
    dtype, score = enricher.classify_disclosure_by_composition(["us-gaap:Assets"])
    assert dtype is None
    assert score == 0.0

  def test_best_type_wins(self):
    """Highest-scoring disclosure type is returned."""
    profiles = {
      "AssetsRollUp": {"us-gaap:Assets": 0.5},
      "GoodwillRollForward": {"us-gaap:Assets": 0.3, "us-gaap:Goodwill": 1.0},
    }
    enricher = self._make_enricher_with_profiles(profiles)
    dtype, score = enricher.classify_disclosure_by_composition(
      ["us-gaap:Assets", "us-gaap:Goodwill"]
    )
    assert dtype == "GoodwillRollForward"

  def test_score_normalized_by_structure_size(self):
    """Score is normalized by number of elements in structure."""
    profiles = {
      "AssetsRollUp": {"us-gaap:Assets": 1.0},
    }
    enricher = self._make_enricher_with_profiles(profiles)

    # 1 matching element out of 1 total
    _, score1 = enricher.classify_disclosure_by_composition(["us-gaap:Assets"])
    # 1 matching element out of 4 total
    _, score4 = enricher.classify_disclosure_by_composition(
      ["us-gaap:Assets", "custom:A", "custom:B", "custom:C"]
    )
    assert score1 > score4


# ---------------------------------------------------------------------------
# SemanticEnricher.detect_dei_structure()
# ---------------------------------------------------------------------------


class TestDEIDetection:
  def test_document_information(self):
    """Structure with document DEI elements returns DocumentInformation."""
    from robosystems.adapters.sec.enrichment import SemanticEnricher

    dtype, conf = SemanticEnricher.detect_dei_structure(
      ["dei:DocumentType", "dei:DocumentPeriodEndDate", "dei:DocumentFiscalYearFocus"]
    )
    assert dtype == "DocumentInformation"
    assert conf == 0.95

  def test_entity_information(self):
    """Structure with entity DEI elements returns EntityInformation."""
    from robosystems.adapters.sec.enrichment import SemanticEnricher

    dtype, conf = SemanticEnricher.detect_dei_structure(
      [
        "dei:EntityRegistrantName",
        "dei:EntityCentralIndexKey",
        "dei:EntityFileNumber",
      ]
    )
    assert dtype == "EntityInformation"
    assert conf == 0.95

  def test_non_dei_returns_none(self):
    """Structure without dei: elements returns (None, 0.0)."""
    from robosystems.adapters.sec.enrichment import SemanticEnricher

    dtype, conf = SemanticEnricher.detect_dei_structure(
      ["us-gaap:Assets", "us-gaap:Liabilities"]
    )
    assert dtype is None
    assert conf == 0.0

  def test_high_confidence(self):
    """DEI detection returns confidence >= 0.85."""
    from robosystems.adapters.sec.enrichment import SemanticEnricher

    _, conf = SemanticEnricher.detect_dei_structure(["dei:DocumentType"])
    assert conf >= 0.85

  def test_mixed_dei_elements(self):
    """Structure with both document and entity elements classifies by majority."""
    from robosystems.adapters.sec.enrichment import SemanticEnricher

    # More entity than document indicators
    dtype, _ = SemanticEnricher.detect_dei_structure(
      [
        "dei:EntityRegistrantName",
        "dei:EntityCentralIndexKey",
        "dei:EntityFileNumber",
        "dei:DocumentType",
      ]
    )
    assert dtype == "EntityInformation"


# ---------------------------------------------------------------------------
# SemanticEnricher._lookup_disclosure_consensus()
# ---------------------------------------------------------------------------


class TestDisclosureConsensusLookup:
  def _make_enricher_with_consensus(self, consensus):
    """Create an enricher with pre-set disclosure consensus."""
    from robosystems.adapters.sec.enrichment import SemanticEnricher

    enricher = SemanticEnricher.__new__(SemanticEnricher)
    enricher._disclosure_consensus = consensus
    return enricher

  def test_known_hash_returns_consensus(self):
    """Known definition hash returns consensus type and ratio."""
    consensus = {
      "abc123": {
        "disclosure_type": "AssetsRollUp",
        "consensus_ratio": 0.95,
        "filing_count": 10,
      }
    }
    enricher = self._make_enricher_with_consensus(consensus)
    dtype, ratio = enricher._lookup_disclosure_consensus("abc123")
    assert dtype == "AssetsRollUp"
    assert ratio == 0.95

  def test_unknown_hash_returns_none(self):
    """Unknown definition hash returns (None, 0.0)."""
    consensus = {
      "abc123": {
        "disclosure_type": "AssetsRollUp",
        "consensus_ratio": 0.95,
        "filing_count": 10,
      }
    }
    enricher = self._make_enricher_with_consensus(consensus)
    dtype, ratio = enricher._lookup_disclosure_consensus("unknown_hash")
    assert dtype is None
    assert ratio == 0.0

  def test_returns_none_when_no_consensus(self):
    """Returns (None, 0.0) when consensus artifact unavailable."""
    enricher = self._make_enricher_with_consensus(None)
    dtype, ratio = enricher._lookup_disclosure_consensus("any_hash")
    assert dtype is None
    assert ratio == 0.0


# ---------------------------------------------------------------------------
# SemanticEnricher.refine_disclosure_confidence()
# ---------------------------------------------------------------------------


class TestRefineDisclosureConfidence:
  def _make_enricher(self, consensus=None, element_knowledge=None):
    """Create an enricher with pre-set artifacts for refinement testing."""
    from robosystems.adapters.sec.enrichment import SemanticEnricher

    enricher = SemanticEnricher.__new__(SemanticEnricher)
    enricher._disclosure_consensus = consensus
    enricher._element_knowledge = element_knowledge
    return enricher

  def test_consensus_boosts_matching_type(self):
    """Consensus matching the raw type boosts confidence."""
    consensus = {
      "hash1": {
        "disclosure_type": "AssetsRollUp",
        "consensus_ratio": 0.95,
        "filing_count": 10,
      }
    }
    enricher = self._make_enricher(consensus=consensus)
    refined_type, refined_conf = enricher.refine_disclosure_confidence(
      "AssetsRollUp", 0.7, ["us-gaap:Assets"], "hash1"
    )
    assert refined_type == "AssetsRollUp"
    assert refined_conf > 0.7

  def test_consensus_overrides_weak_type(self):
    """Strong consensus overrides weak raw classification."""
    consensus = {
      "hash1": {
        "disclosure_type": "GoodwillRollForward",
        "consensus_ratio": 0.95,
        "filing_count": 20,
      }
    }
    enricher = self._make_enricher(consensus=consensus)
    refined_type, refined_conf = enricher.refine_disclosure_confidence(
      "AssetsRollUp", 0.5, ["us-gaap:Goodwill"], "hash1"
    )
    assert refined_type == "GoodwillRollForward"

  def test_element_vote_boosts_agreement(self):
    """Element disclosure_type vote boosting matching type."""
    ek = {
      "us-gaap:Assets": {"disclosure_type": "AssetsRollUp", "primary_statement": None},
    }
    enricher = self._make_enricher(element_knowledge=ek)
    _, refined_conf = enricher.refine_disclosure_confidence(
      "AssetsRollUp", 0.7, ["us-gaap:Assets"], "hash_no_match"
    )
    assert refined_conf > 0.7

  def test_empty_elements_returns_unchanged(self):
    """Empty elements list returns raw values unchanged."""
    enricher = self._make_enricher()
    refined_type, refined_conf = enricher.refine_disclosure_confidence(
      "AssetsRollUp", 0.8, [], "hash1"
    )
    assert refined_type == "AssetsRollUp"
    assert refined_conf == 0.8

  def test_no_artifacts_returns_unchanged(self):
    """When no artifacts are available, returns raw values."""
    enricher = self._make_enricher(consensus=None, element_knowledge=None)
    refined_type, refined_conf = enricher.refine_disclosure_confidence(
      "AssetsRollUp", 0.75, ["us-gaap:Assets"], "hash1"
    )
    assert refined_type == "AssetsRollUp"
    assert refined_conf == 0.75
