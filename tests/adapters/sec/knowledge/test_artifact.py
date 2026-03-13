"""Tests for knowledge artifact builders.

Tests ElementKnowledgeBuilder and StructureKnowledgeBuilder using
a small synthetic DuckDB database that mirrors the SEC staging schema.
"""

import duckdb
import pyarrow.parquet as pq
import pytest


def _read_parquet(path):
  """Read parquet via file handle to avoid pyarrow/DuckDB filesystem conflict."""
  with open(path, "rb") as f:
    return pq.read_table(f)


@pytest.fixture()
def synthetic_duckdb(tmp_path, monkeypatch):
  """Create a minimal DuckDB with SEC-like schema and test data."""
  db_path = tmp_path / "test.duckdb"
  conn = duckdb.connect(str(db_path))

  # Create tables matching SEC staging schema
  conn.execute("""
    CREATE TABLE Element (
      identifier VARCHAR PRIMARY KEY,
      qname VARCHAR,
      name VARCHAR,
      period_type VARCHAR,
      balance VARCHAR,
      is_abstract BOOLEAN DEFAULT false,
      is_numeric BOOLEAN DEFAULT false
    )
  """)

  conn.execute("""
    CREATE TABLE Association (
      identifier VARCHAR PRIMARY KEY,
      association_type VARCHAR,
      weight DOUBLE,
      order_value DOUBLE,
      root VARCHAR
    )
  """)

  conn.execute("""
    CREATE TABLE Structure (
      identifier VARCHAR PRIMARY KEY,
      name VARCHAR,
      definition VARCHAR,
      canonical_type VARCHAR,
      type VARCHAR
    )
  """)

  conn.execute("""
    CREATE TABLE Report (
      identifier VARCHAR PRIMARY KEY,
      filing_date VARCHAR
    )
  """)

  conn.execute("""
    CREATE TABLE Fact (
      identifier VARCHAR PRIMARY KEY,
      value VARCHAR
    )
  """)

  # Relationship tables
  conn.execute("""
    CREATE TABLE ASSOCIATION_HAS_FROM_ELEMENT (
      src VARCHAR,
      dst VARCHAR
    )
  """)

  conn.execute("""
    CREATE TABLE ASSOCIATION_HAS_TO_ELEMENT (
      src VARCHAR,
      dst VARCHAR
    )
  """)

  conn.execute("""
    CREATE TABLE STRUCTURE_HAS_ASSOCIATION (
      src VARCHAR,
      dst VARCHAR
    )
  """)

  conn.execute("""
    CREATE TABLE FACT_HAS_ELEMENT (
      src VARCHAR,
      dst VARCHAR
    )
  """)

  conn.execute("""
    CREATE TABLE REPORT_HAS_FACT (
      src VARCHAR,
      dst VARCHAR
    )
  """)

  # Insert test elements
  elements = [
    ("el_net_income", "us-gaap:NetIncomeLoss", "Net Income Loss", "duration", "credit"),
    ("el_revenue", "us-gaap:Revenues", "Revenues", "duration", "credit"),
    ("el_cogs", "us-gaap:CostOfGoodsSold", "Cost of Goods Sold", "duration", "debit"),
    ("el_gross_profit", "us-gaap:GrossProfit", "Gross Profit", "duration", "credit"),
    ("el_assets", "us-gaap:Assets", "Assets", "instant", "debit"),
    ("el_liabilities", "us-gaap:Liabilities", "Liabilities", "instant", "credit"),
    (
      "el_equity",
      "us-gaap:StockholdersEquity",
      "Stockholders Equity",
      "instant",
      "credit",
    ),
    (
      "el_op_expenses",
      "us-gaap:OperatingExpenses",
      "Operating Expenses",
      "duration",
      "debit",
    ),
  ]
  for eid, qname, name, pt, bal in elements:
    conn.execute(
      "INSERT INTO Element VALUES (?, ?, ?, ?, ?, false, true)",
      [eid, qname, name, pt, bal],
    )

  # Insert associations (calculation arcs): NetIncome -> Revenue, COGS, GrossProfit, OpExpenses
  calc_arcs = [
    ("assoc_1", "Calculation", "el_net_income", "el_revenue", 1.0),
    ("assoc_2", "Calculation", "el_net_income", "el_cogs", -1.0),
    ("assoc_3", "Calculation", "el_net_income", "el_gross_profit", 1.0),
    ("assoc_4", "Calculation", "el_net_income", "el_op_expenses", -1.0),
    ("assoc_5", "Calculation", "el_gross_profit", "el_revenue", 1.0),
    ("assoc_6", "Calculation", "el_gross_profit", "el_cogs", -1.0),
  ]
  for aid, atype, from_el, to_el, weight in calc_arcs:
    root = "True" if aid in ("assoc_1",) else "False"
    conn.execute(
      "INSERT INTO Association VALUES (?, ?, ?, NULL, ?)", [aid, atype, weight, root]
    )
    conn.execute(
      "INSERT INTO ASSOCIATION_HAS_FROM_ELEMENT VALUES (?, ?)", [aid, from_el]
    )
    conn.execute("INSERT INTO ASSOCIATION_HAS_TO_ELEMENT VALUES (?, ?)", [aid, to_el])

  # Insert structures
  conn.execute("""
    INSERT INTO Structure VALUES
    ('struct_1', 'Income Statement', '0001001 - Statement - CONSOLIDATED STATEMENTS OF INCOME', 'income_statement', 'Statement'),
    ('struct_2', 'Balance Sheet', '0001002 - Statement - CONSOLIDATED BALANCE SHEETS', 'balance_sheet', 'Statement')
  """)

  # Structure -> Association links
  conn.execute("""
    INSERT INTO STRUCTURE_HAS_ASSOCIATION VALUES
    ('struct_1', 'assoc_1'),
    ('struct_1', 'assoc_2'),
    ('struct_1', 'assoc_3'),
    ('struct_1', 'assoc_4'),
    ('struct_1', 'assoc_5'),
    ('struct_1', 'assoc_6')
  """)

  # Insert facts and reports for filing count testing
  conn.execute("INSERT INTO Report VALUES ('report_1', '2024-01-15')")
  conn.execute("INSERT INTO Fact VALUES ('fact_1', '1000000')")
  conn.execute("INSERT INTO Fact VALUES ('fact_2', '500000')")
  conn.execute("INSERT INTO FACT_HAS_ELEMENT VALUES ('fact_1', 'el_net_income')")
  conn.execute("INSERT INTO FACT_HAS_ELEMENT VALUES ('fact_2', 'el_revenue')")
  conn.execute("INSERT INTO REPORT_HAS_FACT VALUES ('report_1', 'fact_1')")
  conn.execute("INSERT INTO REPORT_HAS_FACT VALUES ('report_1', 'fact_2')")

  # Classification tables for disclosure mechanics
  conn.execute("""
    CREATE TABLE Classification (
      identifier VARCHAR PRIMARY KEY,
      type VARCHAR,
      source VARCHAR,
      confidence DOUBLE
    )
  """)

  conn.execute("""
    CREATE TABLE ASSOCIATION_HAS_CLASSIFICATION (
      src VARCHAR,
      dst VARCHAR
    )
  """)

  # IncomeStatement disclosure classification linked to calc root association
  conn.execute("""
    INSERT INTO Classification VALUES
    ('class_1', 'IncomeStatement', 'disclosure_mechanics', 1.0)
  """)

  # Link root calc association (assoc_1: NetIncomeLoss -> Revenue) to classification
  conn.execute("""
    INSERT INTO ASSOCIATION_HAS_CLASSIFICATION VALUES
    ('assoc_1', 'class_1'),
    ('assoc_2', 'class_1')
  """)

  conn.close()

  # Set ARTIFACT_PATH to tmp_path so artifacts are written there
  monkeypatch.setenv("ARTIFACT_PATH", str(tmp_path / "artifacts"))

  return db_path


class TestElementKnowledgeBuilder:
  def test_build_produces_parquet(self, synthetic_duckdb, tmp_path, monkeypatch):
    """Element builder creates a valid parquet file."""
    monkeypatch.setenv("ARTIFACT_PATH", str(tmp_path / "artifacts"))
    from robosystems.adapters.sec.knowledge.artifact import ElementKnowledgeBuilder

    builder = ElementKnowledgeBuilder(memory_limit="256MB")
    path = builder.build(synthetic_duckdb)

    assert path.exists()
    table = _read_parquet(path)
    assert table.num_rows > 0

    # Check schema
    col_names = set(table.column_names)
    assert col_names == {
      "qname",
      "primary_statement",
      "bfs_depth",
      "pagerank",
      "core_number",
      "neighborhood_agreement",
      "filing_count",
      "disclosure_type",
    }

  def test_element_classification(self, synthetic_duckdb, tmp_path, monkeypatch):
    """Elements connected to NetIncomeLoss get classified as IncomeStatement."""
    monkeypatch.setenv("ARTIFACT_PATH", str(tmp_path / "artifacts"))
    from robosystems.adapters.sec.knowledge.artifact import ElementKnowledgeBuilder

    builder = ElementKnowledgeBuilder(memory_limit="256MB")
    path = builder.build(synthetic_duckdb)

    table = _read_parquet(path)
    cols = table.to_pydict()

    # Find NetIncomeLoss
    idx = cols["qname"].index("us-gaap:NetIncomeLoss")
    assert cols["primary_statement"][idx] == "IncomeStatement"
    assert cols["bfs_depth"][idx] == 0  # Root element

    # Revenue should also be IncomeStatement (child of NetIncomeLoss)
    idx_rev = cols["qname"].index("us-gaap:Revenues")
    assert cols["primary_statement"][idx_rev] == "IncomeStatement"
    assert cols["bfs_depth"][idx_rev] == 1

  def test_pagerank_nonzero(self, synthetic_duckdb, tmp_path, monkeypatch):
    """PageRank scores should be non-zero for connected elements."""
    monkeypatch.setenv("ARTIFACT_PATH", str(tmp_path / "artifacts"))
    from robosystems.adapters.sec.knowledge.artifact import ElementKnowledgeBuilder

    builder = ElementKnowledgeBuilder(memory_limit="256MB")
    path = builder.build(synthetic_duckdb)

    table = _read_parquet(path)
    cols = table.to_pydict()

    # At least one element should have non-zero PageRank
    assert any(pr > 0 for pr in cols["pagerank"])

  def test_filing_counts(self, synthetic_duckdb, tmp_path, monkeypatch):
    """Filing counts should reflect the test data."""
    monkeypatch.setenv("ARTIFACT_PATH", str(tmp_path / "artifacts"))
    from robosystems.adapters.sec.knowledge.artifact import ElementKnowledgeBuilder

    builder = ElementKnowledgeBuilder(memory_limit="256MB")
    path = builder.build(synthetic_duckdb)

    table = _read_parquet(path)
    cols = table.to_pydict()

    # NetIncomeLoss has 1 fact in 1 report
    idx = cols["qname"].index("us-gaap:NetIncomeLoss")
    assert cols["filing_count"][idx] == 1

  def test_neighborhood_agreement(self, synthetic_duckdb, tmp_path, monkeypatch):
    """Neighborhood agreement should be computed for classified elements."""
    monkeypatch.setenv("ARTIFACT_PATH", str(tmp_path / "artifacts"))
    from robosystems.adapters.sec.knowledge.artifact import ElementKnowledgeBuilder

    builder = ElementKnowledgeBuilder(memory_limit="256MB")
    path = builder.build(synthetic_duckdb)

    table = _read_parquet(path)
    cols = table.to_pydict()

    # NetIncomeLoss neighbors should all be IncomeStatement
    idx = cols["qname"].index("us-gaap:NetIncomeLoss")
    assert cols["neighborhood_agreement"][idx] > 0

  def test_disclosure_type_column(self, synthetic_duckdb, tmp_path, monkeypatch):
    """Elements in disclosure structures get a disclosure_type value."""
    monkeypatch.setenv("ARTIFACT_PATH", str(tmp_path / "artifacts"))
    from robosystems.adapters.sec.knowledge.artifact import ElementKnowledgeBuilder

    builder = ElementKnowledgeBuilder(memory_limit="256MB")
    path = builder.build(synthetic_duckdb)

    table = _read_parquet(path)
    cols = table.to_pydict()

    assert "disclosure_type" in cols
    # At least one element should have a non-null disclosure type
    non_null = [dt for dt in cols["disclosure_type"] if dt is not None]
    assert len(non_null) > 0

  def test_build_without_classification_table(self, tmp_path, monkeypatch):
    """Build succeeds when Classification table doesn't exist (old data)."""
    monkeypatch.setenv("ARTIFACT_PATH", str(tmp_path / "artifacts"))

    # Create a DuckDB without Classification tables
    db_path = tmp_path / "old.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
      CREATE TABLE Element (
        identifier VARCHAR PRIMARY KEY, qname VARCHAR, name VARCHAR,
        period_type VARCHAR, balance VARCHAR, is_abstract BOOLEAN, is_numeric BOOLEAN
      )
    """)
    conn.execute("""
      CREATE TABLE Association (
        identifier VARCHAR PRIMARY KEY, association_type VARCHAR,
        weight DOUBLE, order_value DOUBLE, root VARCHAR
      )
    """)
    conn.execute("CREATE TABLE ASSOCIATION_HAS_FROM_ELEMENT (src VARCHAR, dst VARCHAR)")
    conn.execute("CREATE TABLE ASSOCIATION_HAS_TO_ELEMENT (src VARCHAR, dst VARCHAR)")
    conn.execute(
      "CREATE TABLE Report (identifier VARCHAR PRIMARY KEY, filing_date VARCHAR)"
    )
    conn.execute("CREATE TABLE Fact (identifier VARCHAR PRIMARY KEY, value VARCHAR)")
    conn.execute("CREATE TABLE FACT_HAS_ELEMENT (src VARCHAR, dst VARCHAR)")
    conn.execute("CREATE TABLE REPORT_HAS_FACT (src VARCHAR, dst VARCHAR)")

    # Insert minimal data (two elements to avoid self-loop in graph)
    conn.execute(
      "INSERT INTO Element VALUES ('e1', 'us-gaap:Assets', 'Assets', 'instant', 'debit', false, true)"
    )
    conn.execute(
      "INSERT INTO Element VALUES ('e2', 'us-gaap:AssetsCurrent', 'AssetsCurrent', 'instant', 'debit', false, true)"
    )
    conn.execute(
      "INSERT INTO Association VALUES ('a1', 'Calculation', 1.0, 1.0, 'True')"
    )
    conn.execute("INSERT INTO ASSOCIATION_HAS_FROM_ELEMENT VALUES ('a1', 'e1')")
    conn.execute("INSERT INTO ASSOCIATION_HAS_TO_ELEMENT VALUES ('a1', 'e2')")
    conn.close()

    from robosystems.adapters.sec.knowledge.artifact import ElementKnowledgeBuilder

    builder = ElementKnowledgeBuilder(memory_limit="256MB")
    path = builder.build(db_path)

    table = _read_parquet(path)
    cols = table.to_pydict()
    # disclosure_type should be all None
    assert all(dt is None for dt in cols["disclosure_type"])


class TestClassifyByStructureMembership:
  """Tests for ElementKnowledgeBuilder._classify_by_structure_membership."""

  def test_majority_vote_picks_highest_count(self):
    from robosystems.adapters.sec.knowledge.artifact import ElementKnowledgeBuilder

    membership = {
      "us-gaap:DepreciationDepletionAndAmortization": {
        "cash_flow_statement": 19000,
        "income_statement": 3200,
        "IncomeStatement": 2400,
      },
    }
    result = ElementKnowledgeBuilder._classify_by_structure_membership(membership)
    # 19000 CashFlow vs 5600 IncomeStatement (3200+2400)
    assert result["us-gaap:DepreciationDepletionAndAmortization"] == "CashFlow"

  def test_income_statement_variants_aggregate(self):
    from robosystems.adapters.sec.knowledge.artifact import ElementKnowledgeBuilder

    membership = {
      "us-gaap:NetIncomeLoss": {
        "income_statement": 46000,
        "IncomeStatement": 27000,
        "comprehensive_income": 42000,
        "cash_flow_statement": 26000,
      },
    }
    result = ElementKnowledgeBuilder._classify_by_structure_membership(membership)
    # IS: 46000+27000+42000=115000 vs CF: 26000
    assert result["us-gaap:NetIncomeLoss"] == "IncomeStatement"

  def test_balance_sheet_variants_aggregate(self):
    from robosystems.adapters.sec.knowledge.artifact import ElementKnowledgeBuilder

    membership = {
      "us-gaap:Assets": {
        "balance_sheet": 100000,
        "AssetsRollUp": 3000,
        "cash_flow_statement": 50,
      },
    }
    result = ElementKnowledgeBuilder._classify_by_structure_membership(membership)
    assert result["us-gaap:Assets"] == "BalanceSheet"

  def test_disclosure_only_types_ignored(self):
    from robosystems.adapters.sec.knowledge.artifact import ElementKnowledgeBuilder

    membership = {
      "us-gaap:SomeDisclosureElement": {
        "DocumentInformation": 50000,
        "NetBenefitCosts": 10000,
      },
    }
    result = ElementKnowledgeBuilder._classify_by_structure_membership(membership)
    # No statement-relevant types → element not classified
    assert "us-gaap:SomeDisclosureElement" not in result

  def test_empty_input(self):
    from robosystems.adapters.sec.knowledge.artifact import ElementKnowledgeBuilder

    result = ElementKnowledgeBuilder._classify_by_structure_membership({})
    assert result == {}

  def test_multiple_elements(self):
    from robosystems.adapters.sec.knowledge.artifact import ElementKnowledgeBuilder

    membership = {
      "us-gaap:NetCashProvidedByUsedInOperatingActivities": {
        "cash_flow_statement": 50000,
        "IncomeStatement": 900,
      },
      "us-gaap:StockholdersEquity": {
        "equity_statement": 69000,
        "balance_sheet": 102000,
      },
    }
    result = ElementKnowledgeBuilder._classify_by_structure_membership(membership)
    assert result["us-gaap:NetCashProvidedByUsedInOperatingActivities"] == "CashFlow"
    assert result["us-gaap:StockholdersEquity"] == "BalanceSheet"


class TestStructureKnowledgeBuilder:
  def test_build_produces_both_parquets(self, synthetic_duckdb, tmp_path, monkeypatch):
    """Structure builder creates both profile and consensus parquet files."""
    monkeypatch.setenv("ARTIFACT_PATH", str(tmp_path / "artifacts"))
    from robosystems.adapters.sec.knowledge.artifact import StructureKnowledgeBuilder

    builder = StructureKnowledgeBuilder(memory_limit="256MB")
    profiles_path, consensus_path = builder.build(synthetic_duckdb)

    assert profiles_path.exists()
    assert consensus_path.exists()

    profiles_table = _read_parquet(profiles_path)
    assert set(profiles_table.column_names) == {
      "canonical_type",
      "qname",
      "frequency",
      "structure_count",
    }

    consensus_table = _read_parquet(consensus_path)
    assert set(consensus_table.column_names) == {
      "definition_hash",
      "canonical_type",
      "consensus_ratio",
      "filing_count",
    }

  def test_profile_frequencies(self, synthetic_duckdb, tmp_path, monkeypatch):
    """Profile frequencies should be between 0 and 1."""
    monkeypatch.setenv("ARTIFACT_PATH", str(tmp_path / "artifacts"))
    from robosystems.adapters.sec.knowledge.artifact import StructureKnowledgeBuilder

    builder = StructureKnowledgeBuilder(memory_limit="256MB")
    profiles_path, _ = builder.build(synthetic_duckdb)

    table = _read_parquet(profiles_path)
    cols = table.to_pydict()

    for freq in cols["frequency"]:
      assert 0.0 <= freq <= 1.0


class TestArrowGraphPath:
  """Tests for the zero-copy DuckDB → Arrow → CSR graph construction path."""

  def test_extract_graph_arrow_returns_arrow(self, synthetic_duckdb):
    """extract_graph_arrow returns Arrow arrays, not Python lists."""
    import pyarrow as pa

    from robosystems.adapters.sec.knowledge.extractors import ArcExtractor

    extractor = ArcExtractor(synthetic_duckdb, memory_limit="256MB")
    nodes, edges = extractor.extract_graph_arrow()

    assert isinstance(nodes, pa.ChunkedArray)
    assert isinstance(edges, pa.Table)
    assert len(nodes) > 0
    assert edges.num_rows > 0
    assert set(edges.column_names) == {"src", "dst", "weight"}

  def test_arrow_graph_matches_legacy(self, synthetic_duckdb):
    """Arrow path produces the same graph structure as the legacy tuple path."""
    from robosystems.adapters.sec.knowledge.extractors import ArcExtractor
    from robosystems.adapters.sec.knowledge.graphs import (
      build_element_graph_from_arrow,
      build_element_graph_from_edges,
    )

    extractor = ArcExtractor(synthetic_duckdb, memory_limit="256MB")

    # Legacy path
    edges_legacy = extractor.extract_deduplicated_edges()
    graph_legacy = build_element_graph_from_edges(edges_legacy)

    # Arrow path
    nodes, edges_arrow = extractor.extract_graph_arrow()
    graph_arrow = build_element_graph_from_arrow(nodes, edges_arrow)

    # Same node count and edge count
    assert graph_arrow.num_nodes == graph_legacy.num_nodes
    assert graph_arrow.num_edges == graph_legacy.num_edges

    # Same elements (sorted in both)
    assert sorted(graph_arrow.elements) == sorted(graph_legacy.elements)

    # Same edge weights for each edge
    for qname in graph_legacy.elements:
      legacy_idx = graph_legacy.get_idx(qname)
      arrow_idx = graph_arrow.get_idx(qname)
      assert legacy_idx is not None
      assert arrow_idx is not None

      legacy_neighbors = sorted(graph_legacy.graph.iterNeighbors(legacy_idx))
      arrow_neighbors = sorted(graph_arrow.graph.iterNeighbors(arrow_idx))

      legacy_neighbor_qnames = sorted(
        graph_legacy.get_qname(n) for n in legacy_neighbors
      )
      arrow_neighbor_qnames = sorted(graph_arrow.get_qname(n) for n in arrow_neighbors)
      assert arrow_neighbor_qnames == legacy_neighbor_qnames

  def test_arrow_graph_pagerank(self, synthetic_duckdb):
    """PageRank runs correctly on Arrow-constructed graph."""
    import networkit as nk

    from robosystems.adapters.sec.knowledge.extractors import ArcExtractor
    from robosystems.adapters.sec.knowledge.graphs import build_element_graph_from_arrow

    extractor = ArcExtractor(synthetic_duckdb, memory_limit="256MB")
    nodes, edges = extractor.extract_graph_arrow()
    graph = build_element_graph_from_arrow(nodes, edges)

    pr = nk.centrality.PageRank(graph.graph, damp=0.85, tol=1e-6)
    pr.run()
    scores = pr.scores()

    assert len(scores) == graph.num_nodes
    assert any(s > 0 for s in scores)

  def test_arrow_graph_core_decomposition(self, synthetic_duckdb):
    """Core decomposition runs correctly on Arrow-constructed graph."""
    import networkit as nk

    from robosystems.adapters.sec.knowledge.extractors import ArcExtractor
    from robosystems.adapters.sec.knowledge.graphs import build_element_graph_from_arrow

    extractor = ArcExtractor(synthetic_duckdb, memory_limit="256MB")
    nodes, edges = extractor.extract_graph_arrow()
    graph = build_element_graph_from_arrow(nodes, edges)

    undirected = nk.graphtools.toUndirected(graph.graph)
    core = nk.centrality.CoreDecomposition(undirected)
    core.run()
    scores = core.scores()

    assert len(scores) == graph.num_nodes

  def test_calc_first_dedup(self, synthetic_duckdb):
    """Calculation edges take priority over presentation edges."""
    from robosystems.adapters.sec.knowledge.extractors import ArcExtractor
    from robosystems.adapters.sec.knowledge.graphs import build_element_graph_from_arrow

    extractor = ArcExtractor(synthetic_duckdb, memory_limit="256MB")
    nodes, edges = extractor.extract_graph_arrow()
    graph = build_element_graph_from_arrow(nodes, edges)

    # NetIncomeLoss->Revenue should have calc weight (1.0), not presentation (0.5)
    net_idx = graph.get_idx("us-gaap:NetIncomeLoss")
    rev_idx = graph.get_idx("us-gaap:Revenues")
    assert net_idx is not None
    assert rev_idx is not None
    assert graph.graph.weight(net_idx, rev_idx) == 1.0
