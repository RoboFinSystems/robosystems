"""Tests for QuickBooks pipeline utility functions."""

from datetime import datetime, timedelta

import pytest


@pytest.mark.unit
class TestFlattenJournalLines:
  """Test journal entry line flattening."""

  def test_basic_flattening(self):
    from robosystems.adapters.quickbooks.pipeline.utils import flatten_journal_lines

    entries = [
      {
        "Id": "1",
        "Line": [
          {
            "Amount": 5000.0,
            "Description": "Payment received",
            "DetailType": "JournalEntryLineDetail",
            "JournalEntryLineDetail": {
              "PostingType": "Debit",
              "AccountRef": {"value": "1", "name": "Checking"},
            },
          },
          {
            "Amount": 5000.0,
            "Description": "Payment received",
            "DetailType": "JournalEntryLineDetail",
            "JournalEntryLineDetail": {
              "PostingType": "Credit",
              "AccountRef": {"value": "2", "name": "Revenue"},
            },
          },
        ],
      }
    ]

    lines = flatten_journal_lines(entries)
    assert len(lines) == 2
    assert lines[0]["journal_entry_id"] == "1"
    assert lines[0]["line_num"] == 1
    assert lines[0]["Amount"] == 5000.0
    assert lines[0]["PostingType"] == "Debit"
    assert lines[0]["AccountRef_value"] == "1"
    assert lines[0]["AccountRef_name"] == "Checking"
    assert lines[1]["line_num"] == 2
    assert lines[1]["PostingType"] == "Credit"

  def test_dimension_refs(self):
    from robosystems.adapters.quickbooks.pipeline.utils import flatten_journal_lines

    entries = [
      {
        "Id": "1",
        "Line": [
          {
            "Amount": 100.0,
            "DetailType": "JournalEntryLineDetail",
            "JournalEntryLineDetail": {
              "PostingType": "Debit",
              "AccountRef": {"value": "1", "name": "Expense"},
              "DepartmentRef": {"value": "10", "name": "Engineering"},
              "ClassRef": {"value": "20", "name": "R&D"},
              "LocationRef": None,
            },
          },
        ],
      }
    ]

    lines = flatten_journal_lines(entries)
    assert len(lines) == 1
    assert lines[0]["DepartmentRef_value"] == "10"
    assert lines[0]["DepartmentRef_name"] == "Engineering"
    assert lines[0]["ClassRef_value"] == "20"
    assert lines[0]["ClassRef_name"] == "R&D"
    assert lines[0]["LocationRef_value"] == ""
    assert lines[0]["LocationRef_name"] == ""

  def test_empty_entries(self):
    from robosystems.adapters.quickbooks.pipeline.utils import flatten_journal_lines

    assert flatten_journal_lines([]) == []

  def test_entry_with_no_lines(self):
    from robosystems.adapters.quickbooks.pipeline.utils import flatten_journal_lines

    entries = [{"Id": "1", "Line": []}]
    assert flatten_journal_lines(entries) == []

  def test_missing_detail_fields(self):
    from robosystems.adapters.quickbooks.pipeline.utils import flatten_journal_lines

    entries = [
      {
        "Id": "1",
        "Line": [
          {
            "Amount": 100.0,
            "DetailType": "JournalEntryLineDetail",
            "JournalEntryLineDetail": {
              "PostingType": "Debit",
            },
          },
        ],
      }
    ]

    lines = flatten_journal_lines(entries)
    assert len(lines) == 1
    assert lines[0]["AccountRef_value"] == ""
    assert lines[0]["AccountRef_name"] == ""


@pytest.mark.unit
class TestFlattenCompanyInfo:
  """Test company info flattening."""

  def test_with_dict(self):
    from robosystems.adapters.quickbooks.pipeline.utils import flatten_company_info

    info = [
      {
        "Id": "1",
        "CompanyName": "Acme Corp",
        "LegalName": "Acme Corp LLC",
        "CompanyAddr": {
          "Line1": "123 Main St",
          "City": "SF",
          "CountrySubDivisionCode": "CA",
          "PostalCode": "94105",
          "Country": "US",
        },
      }
    ]

    rows = flatten_company_info(info)
    assert len(rows) == 1
    assert rows[0]["CompanyName"] == "Acme Corp"
    assert rows[0]["LegalName"] == "Acme Corp LLC"
    assert rows[0]["CompanyAddr_City"] == "SF"

  def test_missing_address(self):
    from robosystems.adapters.quickbooks.pipeline.utils import flatten_company_info

    info = [{"Id": "1", "CompanyName": "NoAddr Inc"}]
    rows = flatten_company_info(info)
    assert len(rows) == 1
    assert rows[0]["CompanyAddr_Line1"] == ""
    assert rows[0]["CompanyAddr_Country"] == "US"

  def test_with_to_dict_object(self):
    from robosystems.adapters.quickbooks.pipeline.utils import flatten_company_info

    class MockCompanyInfo:
      def to_dict(self):
        return {"Id": "1", "CompanyName": "Mock Co"}

    rows = flatten_company_info([MockCompanyInfo()])
    assert rows[0]["CompanyName"] == "Mock Co"


@pytest.mark.unit
class TestFlattenJournalEntries:
  """Test journal entry header flattening."""

  def test_basic_flattening(self):
    from robosystems.adapters.quickbooks.pipeline.utils import flatten_journal_entries

    entries = [
      {
        "Id": "42",
        "TxnDate": "2024-03-15",
        "DocNumber": "JE-042",
        "TotalAmt": 1500.0,
        "PrivateNote": "Test entry",
        "Adjustment": False,
      }
    ]

    rows = flatten_journal_entries(entries)
    assert len(rows) == 1
    assert rows[0]["Id"] == "42"
    assert rows[0]["TxnDate"] == "2024-03-15"
    assert rows[0]["TotalAmt"] == 1500.0

  def test_missing_fields(self):
    from robosystems.adapters.quickbooks.pipeline.utils import flatten_journal_entries

    entries = [{"Id": "1"}]
    rows = flatten_journal_entries(entries)
    assert rows[0]["TxnDate"] == ""
    assert rows[0]["TotalAmt"] == 0.0


@pytest.mark.unit
class TestFilterEntriesByDate:
  """Test date filtering for incremental sync."""

  def test_filters_old_entries(self):
    from robosystems.adapters.quickbooks.pipeline.utils import filter_entries_by_date

    today = datetime.now().strftime("%Y-%m-%d")
    old_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    recent_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    entries = [
      {"Id": "1", "TxnDate": old_date},
      {"Id": "2", "TxnDate": recent_date},
      {"Id": "3", "TxnDate": today},
    ]

    filtered = filter_entries_by_date(entries, lookback_days=60)
    assert len(filtered) == 2
    assert filtered[0]["Id"] == "2"
    assert filtered[1]["Id"] == "3"

  def test_empty_list(self):
    from robosystems.adapters.quickbooks.pipeline.utils import filter_entries_by_date

    assert filter_entries_by_date([], lookback_days=60) == []

  def test_all_within_window(self):
    from robosystems.adapters.quickbooks.pipeline.utils import filter_entries_by_date

    today = datetime.now().strftime("%Y-%m-%d")
    entries = [
      {"Id": "1", "TxnDate": today},
      {"Id": "2", "TxnDate": today},
    ]

    filtered = filter_entries_by_date(entries, lookback_days=60)
    assert len(filtered) == 2


@pytest.mark.unit
class TestExportDuckdbTables:
  """Test DuckDB table export to parquet."""

  def test_export_tables(self, tmp_path):
    import duckdb

    db_path = tmp_path / "test.duckdb"
    output_dir = tmp_path / "output"

    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE entity (identifier VARCHAR, name VARCHAR)")
    con.execute("INSERT INTO entity VALUES ('id1', 'Test Corp')")
    con.execute("CREATE TABLE element (identifier VARCHAR, qname VARCHAR)")
    con.execute("INSERT INTO element VALUES ('id2', 'qb:Cash')")
    con.execute("INSERT INTO element VALUES ('id3', 'qb:Revenue')")
    con.close()

    from robosystems.adapters.quickbooks.pipeline.utils import export_duckdb_tables

    results = export_duckdb_tables(db_path, output_dir)

    assert results["entity"] == 1
    assert results["element"] == 2
    assert (output_dir / "qb_entity.parquet").exists()
    assert (output_dir / "qb_element.parquet").exists()

  def test_skips_missing_tables(self, tmp_path):
    import duckdb

    db_path = tmp_path / "test.duckdb"
    output_dir = tmp_path / "output"

    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE entity (identifier VARCHAR)")
    con.execute("INSERT INTO entity VALUES ('id1')")
    con.close()

    from robosystems.adapters.quickbooks.pipeline.utils import export_duckdb_tables

    results = export_duckdb_tables(db_path, output_dir)
    assert "entity" in results
    assert "transaction" not in results
