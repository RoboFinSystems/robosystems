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
      order_value DOUBLE
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
    conn.execute("INSERT INTO Association VALUES (?, ?, ?, NULL)", [aid, atype, weight])
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
