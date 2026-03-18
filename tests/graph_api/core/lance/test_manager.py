"""Tests for LanceManager — Graph API vector index management.

Tests build, search, export, delete, and edge cases for the LanceDB
IVF-PQ vector index manager that runs on graph instances.
"""

import tarfile

import duckdb
import numpy as np
import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def lance_base(tmp_path):
  """Temporary base directory for lance indexes."""
  base = tmp_path / "lance"
  base.mkdir()
  return base


@pytest.fixture()
def manager(lance_base):
  """LanceManager instance using tmp directory."""
  from robosystems.graph_api.core.lance import LanceManager

  return LanceManager(base_path=str(lance_base))


@pytest.fixture()
def duckdb_with_elements(tmp_path):
  """DuckDB staging database with Element table + embeddings + facts.

  Contains 6 elements:
    e1: us-gaap:Revenues (numeric, fact-linked, high confidence)
    e2: us-gaap:NetIncomeLoss (numeric, fact-linked)
    e3: ext:CustomRevenue (numeric, fact-linked, no canonical)
    e4: us-gaap:Assets (numeric, fact-linked)
    e5: ext:TextBlock (textblock, fact-linked — should be excluded by SEC query)
    e6: ext:NoFacts (numeric, NOT fact-linked — should be excluded by SEC query)
    e7: us-gaap:Revenues (duplicate qname, lower confidence — should be deduped)
  """
  db_path = tmp_path / "sec.duckdb"
  conn = duckdb.connect(str(db_path))
  rng = np.random.default_rng(42)

  conn.execute("""
    CREATE TABLE Element (
      identifier VARCHAR PRIMARY KEY,
      qname VARCHAR,
      name VARCHAR,
      is_numeric BOOLEAN DEFAULT false,
      is_textblock BOOLEAN DEFAULT false,
      canonical_concept VARCHAR,
      canonical_confidence DOUBLE,
      classification VARCHAR,
      balance VARCHAR,
      embedding DOUBLE[]
    )
  """)

  conn.execute("""
    CREATE TABLE FACT_HAS_ELEMENT (
      src VARCHAR,
      dst VARCHAR
    )
  """)

  elements = [
    (
      "e1",
      "us-gaap:Revenues",
      "Revenues",
      True,
      False,
      "revenue",
      0.95,
      "income",
      "credit",
    ),
    (
      "e2",
      "us-gaap:NetIncomeLoss",
      "NetIncomeLoss",
      True,
      False,
      "net_income",
      0.92,
      "income",
      "credit",
    ),
    (
      "e3",
      "ext:CustomRevenue",
      "CustomRevenue",
      True,
      False,
      None,
      None,
      None,
      "credit",
    ),
    (
      "e4",
      "us-gaap:Assets",
      "Assets",
      True,
      False,
      "total_assets",
      0.98,
      "balance_sheet",
      "debit",
    ),
    ("e5", "ext:TextBlock", "TextBlock", False, True, None, None, None, None),
    ("e6", "ext:NoFacts", "NoFacts", True, False, None, None, None, None),
    (
      "e7",
      "us-gaap:Revenues",
      "Revenues",
      True,
      False,
      "revenue",
      0.80,
      "income",
      "credit",
    ),
  ]

  for (
    eid,
    qname,
    name,
    is_numeric,
    is_textblock,
    canonical,
    confidence,
    classification,
    balance,
  ) in elements:
    embedding = rng.standard_normal(384).tolist()
    conn.execute(
      """
      INSERT INTO Element (identifier, qname, name, is_numeric, is_textblock,
        canonical_concept, canonical_confidence, classification, balance, embedding)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      """,
      [
        eid,
        qname,
        name,
        is_numeric,
        is_textblock,
        canonical,
        confidence,
        classification,
        balance,
        embedding,
      ],
    )

  # Fact links for e1-e5 (e6 has no facts)
  for eid in ["e1", "e2", "e3", "e4", "e5", "e7"]:
    conn.execute(
      "INSERT INTO FACT_HAS_ELEMENT (src, dst) VALUES (?, ?)", [f"f_{eid}", eid]
    )

  conn.close()
  return db_path


@pytest.fixture()
def duckdb_empty(tmp_path):
  """DuckDB with Element table but no rows."""
  db_path = tmp_path / "empty.duckdb"
  conn = duckdb.connect(str(db_path))
  conn.execute("""
    CREATE TABLE Element (
      identifier VARCHAR, qname VARCHAR, name VARCHAR,
      is_numeric BOOLEAN, is_textblock BOOLEAN,
      canonical_concept VARCHAR, canonical_confidence DOUBLE,
      classification VARCHAR, balance VARCHAR, embedding DOUBLE[]
    )
  """)
  conn.close()
  return db_path


@pytest.fixture()
def duckdb_no_embeddings(tmp_path):
  """DuckDB with Element table where all embeddings are NULL."""
  db_path = tmp_path / "null_embeddings.duckdb"
  conn = duckdb.connect(str(db_path))
  conn.execute("""
    CREATE TABLE Element (
      identifier VARCHAR, qname VARCHAR, name VARCHAR,
      embedding DOUBLE[]
    )
  """)
  conn.execute("INSERT INTO Element VALUES ('e1', 'test:Elem', 'Elem', NULL)")
  conn.close()
  return db_path


@pytest.fixture()
def sec_vector_query():
  """The SEC-specific query that filters to numeric, fact-linked elements."""
  return """
    SELECT
      e.qname,
      e.name,
      e.canonical_concept,
      e.canonical_confidence,
      e.classification,
      e.balance,
      e.embedding::FLOAT[384] AS vector
    FROM Element e
    WHERE e.is_numeric = true
      AND e.is_textblock = false
      AND e.embedding IS NOT NULL
      AND e.identifier IN (
        SELECT DISTINCT dst FROM FACT_HAS_ELEMENT
      )
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY e.qname
      ORDER BY e.canonical_confidence DESC NULLS LAST, e.identifier
    ) = 1
  """


@pytest.fixture()
def simple_vector_query():
  """A simple query that selects all rows with embeddings."""
  return """
    SELECT
      qname,
      name,
      embedding::FLOAT[384] AS vector
    FROM Element
    WHERE embedding IS NOT NULL
  """


# ---------------------------------------------------------------------------
# Build Tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLanceManagerBuild:
  def test_build_creates_lance_directory(
    self, manager, duckdb_with_elements, sec_vector_query
  ):
    """Build produces a lance directory on disk."""
    result = manager.build(
      graph_id="sec",
      table_name="Element",
      query=sec_vector_query,
      duckdb_path=str(duckdb_with_elements),
      memory_limit="256MB",
    )

    assert result["graph_id"] == "sec"
    assert result["table_name"] == "Element"
    assert result["row_count"] > 0
    assert result["index_size_bytes"] > 0
    assert manager.index_exists("sec", "Element")

  def test_build_sec_query_filters_correctly(
    self, manager, duckdb_with_elements, sec_vector_query
  ):
    """SEC query excludes textblocks, non-fact-linked, and deduplicates by qname."""
    import lancedb

    result = manager.build(
      graph_id="sec",
      table_name="Element",
      query=sec_vector_query,
      duckdb_path=str(duckdb_with_elements),
      memory_limit="256MB",
    )

    # Open the index and check contents
    db = lancedb.connect(str(manager._table_dir("sec", "Element")))
    table = db.open_table("Element")
    rows = table.to_pandas()
    qnames = set(rows["qname"].tolist())

    # Included: numeric, fact-linked elements
    assert "us-gaap:Revenues" in qnames
    assert "us-gaap:NetIncomeLoss" in qnames
    assert "ext:CustomRevenue" in qnames
    assert "us-gaap:Assets" in qnames

    # Excluded: textblock (e5)
    assert "ext:TextBlock" not in qnames

    # Excluded: no facts (e6)
    assert "ext:NoFacts" not in qnames

    # Deduplicated: e1 and e7 both have qname us-gaap:Revenues, only one row
    revenue_rows = rows[rows["qname"] == "us-gaap:Revenues"]
    assert len(revenue_rows) == 1

    # The kept row should have higher confidence (e1=0.95 > e7=0.80)
    assert revenue_rows.iloc[0]["canonical_confidence"] == pytest.approx(0.95)

    # Total: 4 unique qnames (Revenues, NetIncomeLoss, CustomRevenue, Assets)
    assert result["row_count"] == 4

  def test_build_simple_query_includes_all(
    self, manager, duckdb_with_elements, simple_vector_query
  ):
    """Simple query without SEC filters includes all rows with embeddings."""
    result = manager.build(
      graph_id="test",
      table_name="Element",
      query=simple_vector_query,
      duckdb_path=str(duckdb_with_elements),
      memory_limit="256MB",
    )

    # All 7 elements have embeddings, no dedup in simple query
    assert result["row_count"] == 7

  def test_build_requires_vector_column(self, manager, duckdb_with_elements):
    """Build fails if query doesn't return a 'vector' column."""
    bad_query = "SELECT qname, name FROM Element"

    with pytest.raises(ValueError, match='Query must return a "vector" column'):
      manager.build(
        graph_id="sec",
        table_name="Element",
        query=bad_query,
        duckdb_path=str(duckdb_with_elements),
      )

  def test_build_empty_results_raises(self, manager, duckdb_empty):
    """Build fails if query returns no rows."""
    query = (
      "SELECT embedding::FLOAT[384] AS vector FROM Element WHERE embedding IS NOT NULL"
    )

    with pytest.raises(ValueError, match="Query returned no rows"):
      manager.build(
        graph_id="sec",
        table_name="Element",
        query=query,
        duckdb_path=str(duckdb_empty),
      )

  def test_build_missing_duckdb_raises(self, manager, tmp_path):
    """Build fails if DuckDB file doesn't exist."""
    with pytest.raises(ValueError, match="DuckDB staging database not found"):
      manager.build(
        graph_id="sec",
        table_name="Element",
        query="SELECT 1 AS vector",
        duckdb_path=str(tmp_path / "nonexistent.duckdb"),
      )

  def test_build_atomic_swap(self, manager, duckdb_with_elements, sec_vector_query):
    """Second build replaces first without leaving stale files."""
    # First build
    result1 = manager.build(
      graph_id="sec",
      table_name="Element",
      query=sec_vector_query,
      duckdb_path=str(duckdb_with_elements),
      memory_limit="256MB",
    )
    assert result1["row_count"] == 4

    # Second build (same data, should replace cleanly)
    result2 = manager.build(
      graph_id="sec",
      table_name="Element",
      query=sec_vector_query,
      duckdb_path=str(duckdb_with_elements),
      memory_limit="256MB",
    )
    assert result2["row_count"] == 4

    # No .old or .building directories left
    graph_dir = manager._graph_dir("sec")
    assert not (graph_dir / "Element.old").exists()
    assert not (graph_dir / "Element.building").exists()
    assert (graph_dir / "Element").is_dir()

  def test_build_multiple_graphs(
    self, manager, duckdb_with_elements, simple_vector_query
  ):
    """Can build indexes for different graph IDs independently."""
    manager.build(
      graph_id="graph_a",
      table_name="Element",
      query=simple_vector_query,
      duckdb_path=str(duckdb_with_elements),
      memory_limit="256MB",
    )
    manager.build(
      graph_id="graph_b",
      table_name="Element",
      query=simple_vector_query,
      duckdb_path=str(duckdb_with_elements),
      memory_limit="256MB",
    )

    assert manager.index_exists("graph_a", "Element")
    assert manager.index_exists("graph_b", "Element")

  def test_build_multiple_tables(
    self, manager, duckdb_with_elements, simple_vector_query
  ):
    """Can build indexes for different tables in the same graph."""
    manager.build(
      graph_id="sec",
      table_name="Element",
      query=simple_vector_query,
      duckdb_path=str(duckdb_with_elements),
      memory_limit="256MB",
    )
    manager.build(
      graph_id="sec",
      table_name="Label",
      query=simple_vector_query,
      duckdb_path=str(duckdb_with_elements),
      memory_limit="256MB",
    )

    assert manager.index_exists("sec", "Element")
    assert manager.index_exists("sec", "Label")


# ---------------------------------------------------------------------------
# Search Tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLanceManagerSearch:
  def test_search_returns_results(
    self, manager, duckdb_with_elements, sec_vector_query
  ):
    """Search returns matching rows with distance scores."""
    manager.build(
      graph_id="sec",
      table_name="Element",
      query=sec_vector_query,
      duckdb_path=str(duckdb_with_elements),
      memory_limit="256MB",
    )

    rng = np.random.default_rng(123)
    query_embedding = rng.standard_normal(384).tolist()

    result = manager.search("sec", "Element", query_embedding, limit=10)

    assert result["total"] > 0
    assert result["execution_time_ms"] >= 0
    assert len(result["results"]) <= 10

    # Each result has metadata + distance
    first = result["results"][0]
    assert "qname" in first
    assert "distance" in first
    assert "vector" not in first  # vector column excluded from output

  def test_search_respects_limit(self, manager, duckdb_with_elements, sec_vector_query):
    """Search returns at most `limit` results."""
    manager.build(
      graph_id="sec",
      table_name="Element",
      query=sec_vector_query,
      duckdb_path=str(duckdb_with_elements),
      memory_limit="256MB",
    )

    rng = np.random.default_rng(123)
    query_embedding = rng.standard_normal(384).tolist()

    result = manager.search("sec", "Element", query_embedding, limit=2)
    assert len(result["results"]) == 2

  def test_search_select_columns(self, manager, duckdb_with_elements, sec_vector_query):
    """Search can restrict which columns are returned."""
    manager.build(
      graph_id="sec",
      table_name="Element",
      query=sec_vector_query,
      duckdb_path=str(duckdb_with_elements),
      memory_limit="256MB",
    )

    rng = np.random.default_rng(123)
    query_embedding = rng.standard_normal(384).tolist()

    result = manager.search(
      "sec", "Element", query_embedding, limit=5, select_columns=["qname", "name"]
    )

    first = result["results"][0]
    assert "qname" in first
    assert "name" in first
    assert "distance" in first  # always included
    # Other columns should not be present
    assert "classification" not in first

  def test_search_no_index_raises(self, manager):
    """Search fails with clear error when no index exists."""
    with pytest.raises(ValueError, match="No vector index"):
      manager.search("sec", "Element", [0.0] * 384)

  def test_search_wrong_table_raises(
    self, manager, duckdb_with_elements, sec_vector_query
  ):
    """Search fails when querying a table that wasn't indexed."""
    manager.build(
      graph_id="sec",
      table_name="Element",
      query=sec_vector_query,
      duckdb_path=str(duckdb_with_elements),
      memory_limit="256MB",
    )

    with pytest.raises(ValueError, match="No vector index"):
      manager.search("sec", "Label", [0.0] * 384)


# ---------------------------------------------------------------------------
# Export Tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLanceManagerExport:
  def test_export_creates_tar_gz(
    self, manager, duckdb_with_elements, sec_vector_query, tmp_path
  ):
    """Export produces a tar.gz file."""
    manager.build(
      graph_id="sec",
      table_name="Element",
      query=sec_vector_query,
      duckdb_path=str(duckdb_with_elements),
      memory_limit="256MB",
    )

    output = tmp_path / "export.tar.gz"
    result = manager.export("sec", "Element", output_path=output)

    assert output.exists()
    assert result["size_bytes"] > 0

  def test_export_archive_structure(
    self, manager, duckdb_with_elements, sec_vector_query, tmp_path
  ):
    """Export archive extracts to {graph_id}/{table_name}.lance/ structure."""
    manager.build(
      graph_id="sec",
      table_name="Element",
      query=sec_vector_query,
      duckdb_path=str(duckdb_with_elements),
      memory_limit="256MB",
    )

    output = tmp_path / "export.tar.gz"
    manager.export("sec", "Element", output_path=output)

    # Extract and verify structure
    extract_dir = tmp_path / "extracted"
    extract_dir.mkdir()
    with tarfile.open(str(output), "r:gz") as tar:
      tar.extractall(str(extract_dir))

    # Should have sec/Element/ directory (LanceDB creates Element.lance/ inside)
    assert (extract_dir / "sec" / "Element").is_dir()

  def test_export_no_index_raises(self, manager):
    """Export fails with clear error when no index exists."""
    with pytest.raises(ValueError, match="No vector index"):
      manager.export("sec", "Element")

  def test_export_default_path(self, manager, duckdb_with_elements, sec_vector_query):
    """Export uses default path when output_path not specified."""
    manager.build(
      graph_id="sec",
      table_name="Element",
      query=sec_vector_query,
      duckdb_path=str(duckdb_with_elements),
      memory_limit="256MB",
    )

    result = manager.export("sec", "Element")
    assert result["size_bytes"] > 0
    assert result["graph_id"] == "sec"
    assert result["table_name"] == "Element"


# ---------------------------------------------------------------------------
# Delete Tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLanceManagerDelete:
  def test_delete_single_table(
    self, manager, duckdb_with_elements, simple_vector_query
  ):
    """Delete removes a specific table's index."""
    manager.build(
      graph_id="sec",
      table_name="Element",
      query=simple_vector_query,
      duckdb_path=str(duckdb_with_elements),
      memory_limit="256MB",
    )
    manager.build(
      graph_id="sec",
      table_name="Label",
      query=simple_vector_query,
      duckdb_path=str(duckdb_with_elements),
      memory_limit="256MB",
    )

    manager.delete("sec", "Element")

    assert not manager.index_exists("sec", "Element")
    assert manager.index_exists("sec", "Label")  # unaffected

  def test_delete_all_for_graph(
    self, manager, duckdb_with_elements, simple_vector_query
  ):
    """Delete with no table_name removes all indexes for the graph."""
    manager.build(
      graph_id="sec",
      table_name="Element",
      query=simple_vector_query,
      duckdb_path=str(duckdb_with_elements),
      memory_limit="256MB",
    )
    manager.build(
      graph_id="sec",
      table_name="Label",
      query=simple_vector_query,
      duckdb_path=str(duckdb_with_elements),
      memory_limit="256MB",
    )

    manager.delete("sec")

    assert not manager.index_exists("sec", "Element")
    assert not manager.index_exists("sec", "Label")

  def test_delete_nonexistent_is_safe(self, manager):
    """Delete on nonexistent index doesn't raise."""
    result = manager.delete("sec", "Element")
    assert result["deleted"] == []


# ---------------------------------------------------------------------------
# Index Info Tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLanceManagerInfo:
  def test_get_index_info(self, manager, duckdb_with_elements, sec_vector_query):
    """get_index_info returns metadata about existing index."""
    manager.build(
      graph_id="sec",
      table_name="Element",
      query=sec_vector_query,
      duckdb_path=str(duckdb_with_elements),
      memory_limit="256MB",
    )

    info = manager.get_index_info("sec", "Element")
    assert info is not None
    assert info["graph_id"] == "sec"
    assert info["table_name"] == "Element"
    assert info["row_count"] == 4
    assert info["size_bytes"] > 0

  def test_get_index_info_missing(self, manager):
    """get_index_info returns None when no index exists."""
    assert manager.get_index_info("sec", "Element") is None

  def test_index_exists(self, manager, duckdb_with_elements, sec_vector_query):
    """index_exists correctly reports presence."""
    assert not manager.index_exists("sec", "Element")

    manager.build(
      graph_id="sec",
      table_name="Element",
      query=sec_vector_query,
      duckdb_path=str(duckdb_with_elements),
      memory_limit="256MB",
    )

    assert manager.index_exists("sec", "Element")
    assert not manager.index_exists("sec", "Label")
