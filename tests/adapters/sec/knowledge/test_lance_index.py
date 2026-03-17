"""Tests for LanceDB vector search index builder and search module."""

import duckdb
import numpy as np
import pytest


@pytest.fixture()
def synthetic_duckdb_with_embeddings(tmp_path):
  """Create a minimal DuckDB with elements that have embeddings."""
  db_path = tmp_path / "test.duckdb"
  conn = duckdb.connect(str(db_path))

  conn.execute("""
    CREATE TABLE Element (
      identifier VARCHAR PRIMARY KEY,
      qname VARCHAR,
      name VARCHAR,
      period_type VARCHAR,
      balance VARCHAR,
      is_abstract BOOLEAN DEFAULT false,
      is_numeric BOOLEAN DEFAULT false,
      is_textblock BOOLEAN DEFAULT false,
      is_domain_member BOOLEAN DEFAULT false,
      is_dimension_item BOOLEAN DEFAULT false,
      is_hypercube_item BOOLEAN DEFAULT false,
      canonical_concept VARCHAR,
      canonical_confidence DOUBLE,
      classification VARCHAR,
      embedding DOUBLE[]
    )
  """)

  conn.execute("""
    CREATE TABLE FACT_HAS_ELEMENT (
      src VARCHAR,
      dst VARCHAR
    )
  """)

  # Insert test elements with random embeddings
  rng = np.random.default_rng(42)

  elements = [
    ("e1", "us-gaap:Revenues", "Revenues", True, "revenue", 0.95, "credit"),
    (
      "e2",
      "us-gaap:NetIncomeLoss",
      "NetIncomeLoss",
      True,
      "net_income",
      0.92,
      "credit",
    ),
    ("e3", "ext:CustomRevenue", "CustomRevenue", True, None, None, "credit"),
    ("e4", "us-gaap:Assets", "Assets", True, "total_assets", 0.98, "debit"),
    ("e5", "ext:TextBlock", "TextBlock", False, None, None, None),
    ("e6", "ext:AbstractItem", "AbstractItem", True, None, None, None),
  ]

  for eid, qname, name, is_numeric, canonical, confidence, balance in elements:
    embedding = rng.standard_normal(384).tolist()
    conn.execute(
      """
      INSERT INTO Element (identifier, qname, name, is_numeric, is_textblock,
        canonical_concept, canonical_confidence, balance, embedding)
      VALUES (?, ?, ?, ?, false, ?, ?, ?, ?)
      """,
      [eid, qname, name, is_numeric, canonical, confidence, balance, embedding],
    )

  # e5 is a textblock, e6 is abstract but still numeric
  conn.execute("UPDATE Element SET is_textblock = true WHERE identifier = 'e5'")

  # Create fact relationships (only e1-e4 are fact-linked)
  for eid in ["e1", "e2", "e3", "e4"]:
    conn.execute(
      "INSERT INTO FACT_HAS_ELEMENT (src, dst) VALUES (?, ?)", [f"f_{eid}", eid]
    )

  conn.close()
  return db_path


class TestLanceIndexBuilder:
  def test_build_creates_lance_directory(
    self, synthetic_duckdb_with_embeddings, tmp_path
  ):
    from robosystems.adapters.sec.knowledge.lance_index import LanceIndexBuilder

    builder = LanceIndexBuilder(memory_limit="256MB")
    output_dir = tmp_path / "lance_output"
    result = builder.build(synthetic_duckdb_with_embeddings, output_dir=output_dir)

    assert result == output_dir
    assert (output_dir / "elements.lance").is_dir()

  def test_build_filters_to_numeric_fact_linked(
    self, synthetic_duckdb_with_embeddings, tmp_path
  ):
    import lancedb

    from robosystems.adapters.sec.knowledge.lance_index import LanceIndexBuilder

    builder = LanceIndexBuilder(memory_limit="256MB")
    output_dir = tmp_path / "lance_output"
    builder.build(synthetic_duckdb_with_embeddings, output_dir=output_dir)

    db = lancedb.connect(str(output_dir))
    table = db.open_table("elements")

    # Should include e1-e4 (numeric, fact-linked) but not e5 (textblock) or e6 (no fact)
    rows = table.to_pandas()
    qnames = set(rows["qname"].tolist())
    assert "us-gaap:Revenues" in qnames
    assert "us-gaap:NetIncomeLoss" in qnames
    assert "ext:CustomRevenue" in qnames
    assert "us-gaap:Assets" in qnames
    assert "ext:TextBlock" not in qnames  # textblock excluded

  def test_build_and_tar(self, synthetic_duckdb_with_embeddings, tmp_path):
    from robosystems.adapters.sec.knowledge.lance_index import LanceIndexBuilder

    builder = LanceIndexBuilder(memory_limit="256MB")
    tar_path = tmp_path / "output.tar.gz"
    result = builder.build_and_tar(
      synthetic_duckdb_with_embeddings, output_tar=tar_path
    )

    assert result == tar_path
    assert tar_path.exists()
    assert tar_path.stat().st_size > 0

  def test_build_empty_db(self, tmp_path):
    """Builder handles a database with no matching elements."""
    from robosystems.adapters.sec.knowledge.lance_index import LanceIndexBuilder

    db_path = tmp_path / "empty.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
      CREATE TABLE Element (
        identifier VARCHAR, qname VARCHAR, name VARCHAR,
        is_numeric BOOLEAN DEFAULT false, is_textblock BOOLEAN DEFAULT false,
        canonical_concept VARCHAR, canonical_confidence DOUBLE,
        classification VARCHAR, balance VARCHAR, embedding DOUBLE[]
      )
    """)
    conn.execute("CREATE TABLE FACT_HAS_ELEMENT (src VARCHAR, dst VARCHAR)")
    conn.close()

    builder = LanceIndexBuilder(memory_limit="256MB")
    output_dir = tmp_path / "lance_empty"
    result = builder.build(db_path, output_dir=output_dir)
    # Should return without error, no elements.lance created
    assert result == output_dir


class TestLanceElementSearch:
  def test_get_instance_returns_none_when_missing(self, tmp_path, monkeypatch):
    from robosystems.adapters.sec.knowledge.lance_search import LanceElementSearch

    LanceElementSearch.reset()
    monkeypatch.setenv("LANCE_INDEX_PATH", str(tmp_path / "nonexistent"))
    result = LanceElementSearch.get_instance()
    assert result is None
    LanceElementSearch.reset()

  def test_search_returns_results(
    self, synthetic_duckdb_with_embeddings, tmp_path, monkeypatch
  ):

    from robosystems.adapters.sec.knowledge.lance_index import LanceIndexBuilder
    from robosystems.adapters.sec.knowledge.lance_search import LanceElementSearch

    # Build index
    builder = LanceIndexBuilder(memory_limit="256MB")
    output_dir = tmp_path / "lance_search_test"
    builder.build(synthetic_duckdb_with_embeddings, output_dir=output_dir)

    # Point env to it
    LanceElementSearch.reset()
    monkeypatch.setenv("LANCE_INDEX_PATH", str(output_dir))

    # Force re-resolve by creating instance directly
    search = LanceElementSearch(str(output_dir))

    # Search with a random embedding
    rng = np.random.default_rng(123)
    query = rng.standard_normal(384).tolist()
    results = search.search(query, limit=5)

    assert len(results) > 0
    assert "qname" in results[0]
    assert "_distance" in results[0]
    LanceElementSearch.reset()

  def test_search_graceful_failure(self, tmp_path):
    """Search returns empty list on error."""
    from robosystems.adapters.sec.knowledge.lance_search import LanceElementSearch

    # Create instance pointing to empty directory
    search = LanceElementSearch(str(tmp_path))
    results = search.search([0.0] * 384)
    assert results == []
    LanceElementSearch.reset()
