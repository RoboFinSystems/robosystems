"""Shared fixtures for SEC knowledge module tests."""

import duckdb
import pytest


@pytest.fixture()
def sec_duckdb(tmp_path):
  """Create a minimal DuckDB with SEC-like schema and test data.

  Provides a self-contained database with elements, associations,
  structures, facts, reports, and classification tables for testing
  extractors, graphs, and framework code.
  """
  db_path = tmp_path / "sec_test.duckdb"
  conn = duckdb.connect(str(db_path))

  # Core entity tables
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
  conn.execute("CREATE TABLE ASSOCIATION_HAS_FROM_ELEMENT (src VARCHAR, dst VARCHAR)")
  conn.execute("CREATE TABLE ASSOCIATION_HAS_TO_ELEMENT (src VARCHAR, dst VARCHAR)")
  conn.execute("CREATE TABLE STRUCTURE_HAS_ASSOCIATION (src VARCHAR, dst VARCHAR)")
  conn.execute("CREATE TABLE FACT_HAS_ELEMENT (src VARCHAR, dst VARCHAR)")
  conn.execute("CREATE TABLE REPORT_HAS_FACT (src VARCHAR, dst VARCHAR)")

  # Classification tables
  conn.execute("""
        CREATE TABLE Classification (
            identifier VARCHAR PRIMARY KEY,
            type VARCHAR,
            source VARCHAR,
            confidence DOUBLE
        )
    """)
  conn.execute("CREATE TABLE ASSOCIATION_HAS_CLASSIFICATION (src VARCHAR, dst VARCHAR)")

  # Insert elements
  elements = [
    ("el_assets", "us-gaap:Assets", "Assets", "instant", "debit"),
    ("el_liabilities", "us-gaap:Liabilities", "Liabilities", "instant", "credit"),
    ("el_equity", "us-gaap:StockholdersEquity", "Equity", "instant", "credit"),
    ("el_revenue", "us-gaap:Revenues", "Revenues", "duration", "credit"),
    ("el_cogs", "us-gaap:CostOfGoodsSold", "COGS", "duration", "debit"),
    ("el_net_income", "us-gaap:NetIncomeLoss", "Net Income", "duration", "credit"),
    ("el_gross_profit", "us-gaap:GrossProfit", "Gross Profit", "duration", "credit"),
  ]
  for eid, qname, name, pt, bal in elements:
    conn.execute(
      "INSERT INTO Element VALUES (?, ?, ?, ?, ?, false, true)",
      [eid, qname, name, pt, bal],
    )

  # Calculation arcs
  calc_arcs = [
    ("assoc_calc_1", "Calculation", "el_net_income", "el_revenue", 1.0, "True"),
    ("assoc_calc_2", "Calculation", "el_net_income", "el_cogs", -1.0, "False"),
    ("assoc_calc_3", "Calculation", "el_gross_profit", "el_revenue", 1.0, "False"),
    ("assoc_calc_4", "Calculation", "el_gross_profit", "el_cogs", -1.0, "False"),
    ("assoc_calc_5", "Calculation", "el_assets", "el_liabilities", 1.0, "True"),
    ("assoc_calc_6", "Calculation", "el_assets", "el_equity", 1.0, "False"),
  ]
  for aid, atype, from_el, to_el, weight, root in calc_arcs:
    conn.execute(
      "INSERT INTO Association VALUES (?, ?, ?, NULL, ?)",
      [aid, atype, weight, root],
    )
    conn.execute(
      "INSERT INTO ASSOCIATION_HAS_FROM_ELEMENT VALUES (?, ?)", [aid, from_el]
    )
    conn.execute("INSERT INTO ASSOCIATION_HAS_TO_ELEMENT VALUES (?, ?)", [aid, to_el])

  # Presentation arcs (some overlap with calc, some unique)
  pres_arcs = [
    ("assoc_pres_1", "Presentation", "el_net_income", "el_revenue", None, "False"),
    (
      "assoc_pres_2",
      "Presentation",
      "el_net_income",
      "el_gross_profit",
      None,
      "False",
    ),
  ]
  for aid, atype, from_el, to_el, weight, root in pres_arcs:
    conn.execute(
      "INSERT INTO Association VALUES (?, ?, ?, NULL, ?)",
      [aid, atype, weight, root],
    )
    conn.execute(
      "INSERT INTO ASSOCIATION_HAS_FROM_ELEMENT VALUES (?, ?)", [aid, from_el]
    )
    conn.execute("INSERT INTO ASSOCIATION_HAS_TO_ELEMENT VALUES (?, ?)", [aid, to_el])

  # Structures
  conn.execute("""
        INSERT INTO Structure VALUES
        ('struct_is', 'Income Statement', 'IS definition', 'income_statement', 'Statement'),
        ('struct_bs', 'Balance Sheet', 'BS definition', 'balance_sheet', 'Statement')
    """)

  # Link structures to associations
  conn.execute("""
        INSERT INTO STRUCTURE_HAS_ASSOCIATION VALUES
        ('struct_is', 'assoc_calc_1'),
        ('struct_is', 'assoc_calc_2'),
        ('struct_is', 'assoc_calc_3'),
        ('struct_is', 'assoc_calc_4'),
        ('struct_bs', 'assoc_calc_5'),
        ('struct_bs', 'assoc_calc_6')
    """)

  # Facts and reports for filing count testing
  conn.execute("INSERT INTO Report VALUES ('report_1', '2024-01-15')")
  conn.execute("INSERT INTO Report VALUES ('report_2', '2024-04-15')")
  conn.execute("INSERT INTO Fact VALUES ('fact_1', '1000000')")
  conn.execute("INSERT INTO Fact VALUES ('fact_2', '500000')")
  conn.execute("INSERT INTO Fact VALUES ('fact_3', '250000')")
  conn.execute("INSERT INTO FACT_HAS_ELEMENT VALUES ('fact_1', 'el_revenue')")
  conn.execute("INSERT INTO FACT_HAS_ELEMENT VALUES ('fact_2', 'el_revenue')")
  conn.execute("INSERT INTO FACT_HAS_ELEMENT VALUES ('fact_3', 'el_assets')")
  conn.execute("INSERT INTO REPORT_HAS_FACT VALUES ('report_1', 'fact_1')")
  conn.execute("INSERT INTO REPORT_HAS_FACT VALUES ('report_2', 'fact_2')")
  conn.execute("INSERT INTO REPORT_HAS_FACT VALUES ('report_1', 'fact_3')")

  # DEI elements
  conn.execute(
    "INSERT INTO Element VALUES "
    "('el_doc_type', 'dei:DocumentType', 'Document Type', 'duration', NULL, false, false)"
  )
  conn.execute(
    "INSERT INTO Element VALUES "
    "('el_doc_period', 'dei:DocumentPeriodEndDate', 'Document Period End Date', "
    "'duration', NULL, false, false)"
  )
  conn.execute(
    "INSERT INTO Element VALUES "
    "('el_entity_name', 'dei:EntityRegistrantName', 'Entity Registrant Name', "
    "'duration', NULL, false, false)"
  )

  # Disclosure structures
  conn.execute("""
        INSERT INTO Structure VALUES
        ('struct_assets_disc', 'Composition of Certain Financial Statement Captions',
         '0002001 - Disclosure - Composition of Certain Financial Statement Captions',
         NULL, 'Disclosure'),
        ('struct_goodwill_disc', 'Goodwill',
         '0002002 - Disclosure - Goodwill',
         NULL, 'Disclosure'),
        ('struct_dei', 'Document Entity Information',
         '0000001 - Document - Document and Entity Information',
         NULL, 'Document')
    """)

  # Associations for disclosure structures
  conn.execute(
    "INSERT INTO Association VALUES ('assoc_disc_1', 'Calculation', 1.0, NULL, 'True')"
  )
  conn.execute(
    "INSERT INTO Association VALUES "
    "('assoc_disc_2', 'Presentation', NULL, NULL, 'False')"
  )
  conn.execute(
    "INSERT INTO Association VALUES "
    "('assoc_disc_3', 'Presentation', NULL, NULL, 'False')"
  )
  conn.execute(
    "INSERT INTO Association VALUES "
    "('assoc_disc_4', 'Presentation', NULL, NULL, 'False')"
  )

  # Link disclosure structures to associations
  conn.execute("""
        INSERT INTO STRUCTURE_HAS_ASSOCIATION VALUES
        ('struct_assets_disc', 'assoc_disc_1'),
        ('struct_assets_disc', 'assoc_disc_2'),
        ('struct_goodwill_disc', 'assoc_disc_3'),
        ('struct_dei', 'assoc_disc_4')
    """)

  # Link disclosure associations to elements
  conn.execute(
    "INSERT INTO ASSOCIATION_HAS_TO_ELEMENT VALUES ('assoc_disc_1', 'el_liabilities')"
  )
  conn.execute(
    "INSERT INTO ASSOCIATION_HAS_TO_ELEMENT VALUES ('assoc_disc_2', 'el_equity')"
  )
  conn.execute(
    "INSERT INTO ASSOCIATION_HAS_TO_ELEMENT VALUES ('assoc_disc_3', 'el_equity')"
  )
  conn.execute(
    "INSERT INTO ASSOCIATION_HAS_TO_ELEMENT VALUES ('assoc_disc_4', 'el_doc_type')"
  )

  # Classification data
  conn.execute("""
        INSERT INTO Classification VALUES
        ('class_is', 'IncomeStatement', 'disclosure_mechanics', 1.0),
        ('class_bs', 'AssetsRollUp', 'disclosure_mechanics', 1.0),
        ('class_gw', 'GoodwillRollForward', 'disclosure_mechanics', 1.0)
    """)
  conn.execute("""
        INSERT INTO ASSOCIATION_HAS_CLASSIFICATION VALUES
        ('assoc_calc_1', 'class_is'),
        ('assoc_calc_5', 'class_bs'),
        ('assoc_disc_1', 'class_bs'),
        ('assoc_disc_3', 'class_gw')
    """)

  conn.close()
  return db_path


@pytest.fixture()
def empty_duckdb(tmp_path):
  """Create a DuckDB with the SEC schema but no data."""
  db_path = tmp_path / "empty.duckdb"
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
  conn.execute(
    "CREATE TABLE Structure (identifier VARCHAR PRIMARY KEY, name VARCHAR, "
    "definition VARCHAR, canonical_type VARCHAR, type VARCHAR)"
  )
  conn.execute("CREATE TABLE STRUCTURE_HAS_ASSOCIATION (src VARCHAR, dst VARCHAR)")
  conn.execute("""
        CREATE TABLE Classification (
            identifier VARCHAR PRIMARY KEY, type VARCHAR,
            source VARCHAR, confidence DOUBLE
        )
    """)
  conn.execute("CREATE TABLE ASSOCIATION_HAS_CLASSIFICATION (src VARCHAR, dst VARCHAR)")

  conn.close()
  return db_path


@pytest.fixture()
def no_classification_duckdb(tmp_path):
  """Create a DuckDB without Classification tables (backward compatibility)."""
  db_path = tmp_path / "no_class.duckdb"
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
  conn.execute(
    "CREATE TABLE Structure (identifier VARCHAR PRIMARY KEY, name VARCHAR, "
    "definition VARCHAR, canonical_type VARCHAR, type VARCHAR)"
  )
  conn.execute("CREATE TABLE STRUCTURE_HAS_ASSOCIATION (src VARCHAR, dst VARCHAR)")

  # Insert minimal data
  conn.execute(
    "INSERT INTO Element VALUES "
    "('e1', 'us-gaap:Assets', 'Assets', 'instant', 'debit', false, true)"
  )
  conn.execute(
    "INSERT INTO Element VALUES "
    "('e2', 'us-gaap:Liabilities', 'Liabilities', 'instant', 'credit', false, true)"
  )
  conn.execute(
    "INSERT INTO Association VALUES ('a1', 'Calculation', 1.0, NULL, 'True')"
  )
  conn.execute("INSERT INTO ASSOCIATION_HAS_FROM_ELEMENT VALUES ('a1', 'e1')")
  conn.execute("INSERT INTO ASSOCIATION_HAS_TO_ELEMENT VALUES ('a1', 'e2')")

  conn.close()
  return db_path
