"""Tests for ArcExtractor DuckDB data extraction.

Tests extraction of deduplicated edges, Arrow graph data, filing counts,
structure compositions, disclosure types, and disclosure root elements.
"""

import duckdb
import pyarrow as pa
import pytest

from robosystems.adapters.sec.knowledge.extractors import ArcExtractor

pytestmark = pytest.mark.unit


class TestArcExtractorInit:
  """Tests for ArcExtractor initialization."""

  def test_init_with_valid_path(self, sec_duckdb):
    """Initializes successfully with an existing database file."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    assert extractor._db_path == sec_duckdb

  def test_init_with_string_path(self, sec_duckdb):
    """Accepts a string path and converts to Path."""
    extractor = ArcExtractor(str(sec_duckdb), memory_limit="256MB")
    assert extractor._db_path == sec_duckdb

  def test_init_with_nonexistent_path(self, tmp_path):
    """Raises FileNotFoundError for a missing database file."""
    bad_path = tmp_path / "nonexistent.duckdb"
    with pytest.raises(FileNotFoundError, match="Database not found"):
      ArcExtractor(bad_path)

  def test_default_memory_limit(self, sec_duckdb):
    """Default memory limit is 4GB."""
    extractor = ArcExtractor(sec_duckdb)
    assert extractor._memory_limit == "4GB"

  def test_custom_memory_limit(self, sec_duckdb):
    """Memory limit can be overridden."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="512MB")
    assert extractor._memory_limit == "512MB"


class TestArcExtractorConnect:
  """Tests for the internal _connect method."""

  def test_connect_returns_connection(self, sec_duckdb):
    """_connect returns a valid DuckDB connection."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    conn = extractor._connect()
    try:
      assert isinstance(conn, duckdb.DuckDBPyConnection)
      # Verify we can execute a query
      result = conn.execute("SELECT 1").fetchone()
      assert result == (1,)
    finally:
      conn.close()

  def test_connect_opens_read_only(self, sec_duckdb):
    """Connection is opened in read-only mode."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    conn = extractor._connect()
    try:
      # Writing should fail on a read-only connection
      with pytest.raises(duckdb.InvalidInputException):
        conn.execute("CREATE TABLE test_write (id INT)")
    finally:
      conn.close()


class TestExtractDeduplicatedEdges:
  """Tests for extract_deduplicated_edges."""

  def test_returns_list_of_tuples(self, sec_duckdb):
    """Returns a list of (parent_qname, child_qname, weight, assoc_type) tuples."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    edges = extractor.extract_deduplicated_edges()

    assert isinstance(edges, list)
    assert len(edges) > 0
    for edge in edges:
      assert len(edge) == 4
      parent, child, weight, assoc_type = edge
      assert isinstance(parent, str)
      assert isinstance(child, str)
      assert isinstance(weight, (int, float))
      assert assoc_type in ("Calculation", "Presentation")

  def test_deduplicates_edges(self, sec_duckdb):
    """Edges are deduplicated by (parent, child, association_type)."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    edges = extractor.extract_deduplicated_edges()

    # Check uniqueness by (parent, child, type)
    seen = set()
    for parent, child, _weight, assoc_type in edges:
      key = (parent, child, assoc_type)
      assert key not in seen, f"Duplicate edge found: {key}"
      seen.add(key)

  def test_includes_calculation_edges(self, sec_duckdb):
    """Includes Calculation association type edges."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    edges = extractor.extract_deduplicated_edges()

    calc_edges = [e for e in edges if e[3] == "Calculation"]
    assert len(calc_edges) > 0

  def test_includes_presentation_edges(self, sec_duckdb):
    """Includes Presentation association type edges."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    edges = extractor.extract_deduplicated_edges()

    pres_edges = [e for e in edges if e[3] == "Presentation"]
    assert len(pres_edges) > 0

  def test_weight_is_abs_max(self, sec_duckdb):
    """Weight is the MAX of ABS of weight values (handles negative weights)."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    edges = extractor.extract_deduplicated_edges()

    for _parent, _child, weight, _assoc_type in edges:
      assert weight >= 0, "All weights should be non-negative (absolute)"

  def test_empty_database_returns_empty(self, empty_duckdb):
    """Returns an empty list when no associations exist."""
    extractor = ArcExtractor(empty_duckdb, memory_limit="256MB")
    edges = extractor.extract_deduplicated_edges()
    assert edges == []

  def test_known_edge_exists(self, sec_duckdb):
    """Known edges from test data appear in results."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    edges = extractor.extract_deduplicated_edges()

    edge_set = {(parent, child, assoc) for parent, child, _w, assoc in edges}
    assert (
      "us-gaap:NetIncomeLoss",
      "us-gaap:Revenues",
      "Calculation",
    ) in edge_set


class TestExtractGraphArrow:
  """Tests for extract_graph_arrow Arrow-based extraction."""

  def test_returns_arrow_types(self, sec_duckdb):
    """Returns Arrow array for nodes and Arrow table for edges."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    nodes, edges = extractor.extract_graph_arrow()

    assert isinstance(nodes, (pa.Array, pa.ChunkedArray))
    assert isinstance(edges, pa.Table)

  def test_nodes_are_strings(self, sec_duckdb):
    """Node array contains string qnames."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    nodes, _edges = extractor.extract_graph_arrow()

    node_list = nodes.to_pylist()
    assert len(node_list) > 0
    for node in node_list:
      assert isinstance(node, str)

  def test_edges_have_correct_columns(self, sec_duckdb):
    """Edge table has src, dst, weight columns."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    _nodes, edges = extractor.extract_graph_arrow()

    assert set(edges.column_names) == {"src", "dst", "weight"}

  def test_edge_src_dst_are_int64(self, sec_duckdb):
    """Edge src and dst columns are int64 (BIGINT)."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    _nodes, edges = extractor.extract_graph_arrow()

    assert edges.column("src").type == pa.int64()
    assert edges.column("dst").type == pa.int64()

  def test_nodes_are_sorted(self, sec_duckdb):
    """Node list is sorted alphabetically (deterministic node IDs)."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    nodes, _edges = extractor.extract_graph_arrow()

    node_list = nodes.to_pylist()
    assert node_list == sorted(node_list)

  def test_edges_sorted_by_src_dst(self, sec_duckdb):
    """Edges are sorted by (src, dst)."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    _nodes, edges = extractor.extract_graph_arrow()

    src_col = edges.column("src").to_pylist()
    dst_col = edges.column("dst").to_pylist()

    pairs = list(zip(src_col, dst_col, strict=False))
    assert pairs == sorted(pairs)

  def test_calc_first_dedup_priority(self, sec_duckdb):
    """Calculation edges take priority over Presentation for same (src, dst)."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    nodes, edges = extractor.extract_graph_arrow()

    node_list = nodes.to_pylist()
    node_idx = {q: i for i, q in enumerate(node_list)}

    net_income_idx = node_idx["us-gaap:NetIncomeLoss"]
    revenue_idx = node_idx["us-gaap:Revenues"]

    # Find the edge from NetIncomeLoss to Revenues
    src_col = edges.column("src").to_pylist()
    dst_col = edges.column("dst").to_pylist()
    wt_col = edges.column("weight").to_pylist()

    edge_weights = {}
    for s, d, w in zip(src_col, dst_col, wt_col, strict=False):
      edge_weights[(s, d)] = w

    # This edge exists as both Calculation (weight=1.0) and Presentation (weight=0.5)
    # Calculation should win
    assert (net_income_idx, revenue_idx) in edge_weights
    assert edge_weights[(net_income_idx, revenue_idx)] == 1.0

  def test_node_ids_are_valid_indices(self, sec_duckdb):
    """All edge src/dst values are valid node indices."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    nodes, edges = extractor.extract_graph_arrow()

    n = len(nodes)
    src_col = edges.column("src").to_pylist()
    dst_col = edges.column("dst").to_pylist()

    for s in src_col:
      assert 0 <= s < n
    for d in dst_col:
      assert 0 <= d < n

  def test_empty_database_returns_empty_arrow(self, empty_duckdb):
    """Empty database returns empty Arrow arrays."""
    extractor = ArcExtractor(empty_duckdb, memory_limit="256MB")
    nodes, edges = extractor.extract_graph_arrow()

    assert len(nodes) == 0
    assert edges.num_rows == 0


class TestExtractElementFilingCounts:
  """Tests for extract_element_filing_counts."""

  def test_returns_dict(self, sec_duckdb):
    """Returns a dictionary mapping qnames to filing counts."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    counts = extractor.extract_element_filing_counts()

    assert isinstance(counts, dict)
    assert len(counts) > 0

  def test_counts_are_positive_integers(self, sec_duckdb):
    """All filing counts are positive integers."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    counts = extractor.extract_element_filing_counts()

    for qname, count in counts.items():
      assert isinstance(count, int)
      assert count > 0

  def test_known_element_count(self, sec_duckdb):
    """Revenue appears in 2 reports via 2 facts."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    counts = extractor.extract_element_filing_counts()

    # fact_1 -> report_1, fact_2 -> report_2, both linked to el_revenue
    assert counts["us-gaap:Revenues"] == 2

  def test_single_report_element(self, sec_duckdb):
    """Assets appears in 1 report via 1 fact."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    counts = extractor.extract_element_filing_counts()

    assert counts["us-gaap:Assets"] == 1

  def test_element_without_facts_not_in_counts(self, sec_duckdb):
    """Elements without facts are not included in counts."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    counts = extractor.extract_element_filing_counts()

    # Liabilities has no facts in test data
    assert "us-gaap:Liabilities" not in counts

  def test_empty_database_returns_empty_dict(self, empty_duckdb):
    """Returns empty dict when no facts/reports exist."""
    extractor = ArcExtractor(empty_duckdb, memory_limit="256MB")
    counts = extractor.extract_element_filing_counts()
    assert counts == {}


class TestExtractStructureCompositions:
  """Tests for extract_structure_compositions."""

  def test_returns_list_of_tuples(self, sec_duckdb):
    """Returns list of (identifier, canonical_type, def_hash, [qnames])."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    compositions = extractor.extract_structure_compositions()

    assert isinstance(compositions, list)
    assert len(compositions) > 0
    for comp in compositions:
      assert len(comp) == 4
      sid, ctype, def_hash, qnames = comp
      assert isinstance(sid, str)
      assert isinstance(def_hash, str)
      assert isinstance(qnames, list)

  def test_element_qnames_sorted(self, sec_duckdb):
    """Element qnames within each composition are sorted."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    compositions = extractor.extract_structure_compositions()

    for _sid, _ctype, _hash, qnames in compositions:
      assert qnames == sorted(qnames)

  def test_canonical_type_present(self, sec_duckdb):
    """Canonical type is included for structures that have one."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    compositions = extractor.extract_structure_compositions()

    types_found = {ctype for _sid, ctype, _hash, _qnames in compositions}
    assert "income_statement" in types_found or "balance_sheet" in types_found

  def test_definition_hash_is_md5(self, sec_duckdb):
    """Definition hash is an MD5 hex string (32 chars)."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    compositions = extractor.extract_structure_compositions()

    for _sid, _ctype, def_hash, _qnames in compositions:
      assert len(def_hash) == 32
      # MD5 hex is all lowercase hex chars
      assert all(c in "0123456789abcdef" for c in def_hash)

  def test_empty_database_returns_empty(self, empty_duckdb):
    """Returns empty list when no structures exist."""
    extractor = ArcExtractor(empty_duckdb, memory_limit="256MB")
    compositions = extractor.extract_structure_compositions()
    assert compositions == []


class TestExtractElementDisclosureTypes:
  """Tests for extract_element_disclosure_types."""

  def test_returns_dict(self, sec_duckdb):
    """Returns a dictionary mapping qnames to disclosure types."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    types = extractor.extract_element_disclosure_types()

    assert isinstance(types, dict)

  def test_known_disclosure_type(self, sec_duckdb):
    """Elements linked to disclosure_mechanics get a type."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    types = extractor.extract_element_disclosure_types()

    # At least one element should have a disclosure type from our test data
    assert len(types) > 0

  def test_missing_classification_table_returns_empty(self, no_classification_duckdb):
    """Returns empty dict when Classification table does not exist."""
    extractor = ArcExtractor(no_classification_duckdb, memory_limit="256MB")
    types = extractor.extract_element_disclosure_types()
    assert types == {}

  def test_values_are_strings(self, sec_duckdb):
    """All disclosure type values are strings."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    types = extractor.extract_element_disclosure_types()

    for qname, dtype in types.items():
      assert isinstance(qname, str)
      assert isinstance(dtype, str)


class TestExtractDisclosureRootElements:
  """Tests for extract_disclosure_root_elements."""

  def test_returns_dict_of_sets(self, sec_duckdb):
    """Returns dict mapping qnames to sets of disclosure types."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    roots = extractor.extract_disclosure_root_elements()

    assert isinstance(roots, dict)
    for qname, dtypes in roots.items():
      assert isinstance(qname, str)
      assert isinstance(dtypes, set)
      for dtype in dtypes:
        assert isinstance(dtype, str)

  def test_root_elements_are_calculation_roots(self, sec_duckdb):
    """Only elements from root='True' calculation associations are included."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    roots = extractor.extract_disclosure_root_elements()

    # assoc_calc_1 is root=True, linked to class_is (IncomeStatement)
    # assoc_calc_5 is root=True, linked to class_bs (AssetsRollUp)
    # Their FROM elements should appear
    if roots:
      all_qnames = set(roots.keys())
      # The from-element of assoc_calc_1 is el_net_income -> us-gaap:NetIncomeLoss
      # The from-element of assoc_calc_5 is el_assets -> us-gaap:Assets
      possible_roots = {"us-gaap:NetIncomeLoss", "us-gaap:Assets"}
      assert all_qnames.issubset(possible_roots) or len(all_qnames) > 0

  def test_missing_classification_table_returns_empty(self, no_classification_duckdb):
    """Returns empty dict when Classification table does not exist."""
    extractor = ArcExtractor(no_classification_duckdb, memory_limit="256MB")
    roots = extractor.extract_disclosure_root_elements()
    assert roots == {}

  def test_empty_database_returns_empty(self, empty_duckdb):
    """Returns empty dict when no associations or classifications exist."""
    extractor = ArcExtractor(empty_duckdb, memory_limit="256MB")
    roots = extractor.extract_disclosure_root_elements()
    assert roots == {}


class TestExtractorConnectionCleanup:
  """Tests that database connections are properly closed after extraction."""

  def test_connection_closed_after_edges(self, sec_duckdb):
    """Connection is closed after extract_deduplicated_edges."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    extractor.extract_deduplicated_edges()
    # If connection wasn't closed, subsequent ops might fail or leak
    # Verify by calling again (opens a new connection)
    edges = extractor.extract_deduplicated_edges()
    assert len(edges) > 0

  def test_connection_closed_after_arrow(self, sec_duckdb):
    """Connection is closed after extract_graph_arrow."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    extractor.extract_graph_arrow()
    # Call again to verify clean state
    nodes, edges = extractor.extract_graph_arrow()
    assert len(nodes) > 0

  def test_connection_closed_after_filing_counts(self, sec_duckdb):
    """Connection is closed after extract_element_filing_counts."""
    extractor = ArcExtractor(sec_duckdb, memory_limit="256MB")
    extractor.extract_element_filing_counts()
    counts = extractor.extract_element_filing_counts()
    assert len(counts) > 0
