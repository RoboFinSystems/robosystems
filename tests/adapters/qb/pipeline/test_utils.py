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
class TestParseJournalReport:
  """Test JournalReport parsing into entries and lines."""

  def test_basic_report(self):
    from robosystems.adapters.quickbooks.pipeline.utils import parse_journal_report

    report = {
      "Rows": {
        "Row": [
          {
            "ColData": [
              {"value": "2024-03-15"},  # Date
              {"value": "Invoice", "id": "101"},  # Type + ID
              {"value": "INV-001"},  # Num
              {"value": "Customer A"},  # Name
              {"value": "Service revenue"},  # Memo
              {"value": "Accounts Receivable", "id": "1"},  # Account
              {"value": "500.00"},  # Debit
              {"value": ""},  # Credit
            ]
          },
          {
            "ColData": [
              {"value": ""},
              {"value": "", "id": ""},
              {"value": ""},
              {"value": ""},
              {"value": "Service revenue"},
              {"value": "Revenue", "id": "2"},
              {"value": ""},
              {"value": "500.00"},
            ]
          },
          {"Summary": True},  # End of transaction group
        ]
      }
    }

    entries, lines = parse_journal_report(report)

    assert len(entries) == 1
    assert entries[0]["Id"] == "Invoice_101"
    assert entries[0]["TxnDate"] == "2024-03-15"
    assert entries[0]["DocNumber"] == "INV-001"
    assert entries[0]["TotalAmt"] == 500.0

    assert len(lines) == 2
    assert lines[0]["journal_entry_id"] == "Invoice_101"
    assert lines[0]["PostingType"] == "Debit"
    assert lines[0]["Amount"] == 500.0
    assert lines[0]["AccountRef_value"] == "1"
    assert lines[1]["PostingType"] == "Credit"
    assert lines[1]["AccountRef_value"] == "2"

  def test_empty_report(self):
    from robosystems.adapters.quickbooks.pipeline.utils import parse_journal_report

    entries, lines = parse_journal_report({})
    assert entries == []
    assert lines == []

  def test_none_report(self):
    from robosystems.adapters.quickbooks.pipeline.utils import parse_journal_report

    entries, lines = parse_journal_report(None)
    assert entries == []
    assert lines == []

  def test_report_without_rows(self):
    from robosystems.adapters.quickbooks.pipeline.utils import parse_journal_report

    entries, lines = parse_journal_report({"Rows": {}})
    assert entries == []
    assert lines == []

  def test_short_col_data_skipped(self):
    from robosystems.adapters.quickbooks.pipeline.utils import parse_journal_report

    report = {
      "Rows": {
        "Row": [
          {"ColData": [{"value": "short"}]},
        ]
      }
    }

    entries, lines = parse_journal_report(report)
    assert entries == []
    assert lines == []

  def test_multiple_transactions(self):
    from robosystems.adapters.quickbooks.pipeline.utils import parse_journal_report

    def _make_row(date, tx_type, tx_id, account_id, debit, credit):
      return {
        "ColData": [
          {"value": date},
          {"value": tx_type, "id": tx_id},
          {"value": ""},
          {"value": ""},
          {"value": ""},
          {"value": "Account", "id": account_id},
          {"value": str(debit) if debit else ""},
          {"value": str(credit) if credit else ""},
        ]
      }

    report = {
      "Rows": {
        "Row": [
          _make_row("2024-01-01", "Bill", "1", "10", 100, 0),
          _make_row("", "", "", "20", 0, 100),
          {"Summary": True},
          _make_row("2024-02-01", "Payment", "2", "30", 200, 0),
          _make_row("", "", "", "40", 0, 200),
          {"Summary": True},
        ]
      }
    }

    entries, lines = parse_journal_report(report)

    assert len(entries) == 2
    assert entries[0]["Id"] == "Bill_1"
    assert entries[1]["Id"] == "Payment_2"
    assert len(lines) == 4

  def test_row_without_date_type_id_skipped(self):
    from robosystems.adapters.quickbooks.pipeline.utils import parse_journal_report

    report = {
      "Rows": {
        "Row": [
          {
            "ColData": [
              {"value": ""},  # No date
              {"value": "", "id": ""},  # No type/id
              {"value": ""},
              {"value": ""},
              {"value": ""},
              {"value": "", "id": ""},
              {"value": "100"},
              {"value": ""},
            ]
          },
        ]
      }
    }

    entries, lines = parse_journal_report(report)
    assert entries == []
    assert lines == []


@pytest.mark.unit
class TestGetPipelineWorkDir:
  def test_creates_directory(self, tmp_path):
    import tempfile
    from unittest.mock import patch

    from robosystems.adapters.quickbooks.pipeline.utils import get_pipeline_work_dir

    with patch.object(tempfile, "gettempdir", return_value=str(tmp_path)):
      work_dir = get_pipeline_work_dir("kg_test123")

    assert work_dir.exists()
    assert work_dir.name == "kg_test123"
    assert work_dir.parent.name == "qb_pipeline"


@pytest.mark.unit
class TestWriteExtractParquet:
  def test_writes_all_files(self, tmp_path):
    from unittest.mock import patch

    from robosystems.adapters.quickbooks.pipeline.utils import write_extract_parquet

    output_dir = tmp_path / "extract"

    accounts = [{"Id": "1", "Name": "Cash", "AccountType": "Bank"}]
    journal_entries = [
      {
        "Id": "1",
        "TxnDate": "2024-01-01",
        "DocNumber": "JE-1",
        "TotalAmt": 100.0,
        "PrivateNote": "",
        "Adjustment": False,
      }
    ]
    journal_lines = [
      {
        "journal_entry_id": "1",
        "line_num": 1,
        "Amount": 100.0,
        "PostingType": "Debit",
        "AccountRef_value": "1",
        "AccountRef_name": "Cash",
        "Description": "",
        "DetailType": "JournalEntryLineDetail",
        "DepartmentRef_value": "",
        "DepartmentRef_name": "",
        "ClassRef_value": "",
        "ClassRef_name": "",
        "LocationRef_value": "",
        "LocationRef_name": "",
      }
    ]
    company_info = [{"Id": "1", "CompanyName": "Acme"}]

    # Mock to_parquet to avoid pyarrow/DuckDB filesystem conflict in full suite
    with patch("pandas.core.frame.DataFrame.to_parquet") as mock_parquet:
      write_extract_parquet(
        output_dir, accounts, journal_entries, journal_lines, company_info
      )

    # Phase 2: 4 original files (accounts, journal_entries, journal_lines,
    # company_info) + 6 new (customers, vendors, employees, invoice_headers,
    # bill_headers, payment_headers) = 10 total.
    assert mock_parquet.call_count == 10
    assert output_dir.exists()

  def test_creates_output_directory(self, tmp_path):
    from unittest.mock import patch

    from robosystems.adapters.quickbooks.pipeline.utils import write_extract_parquet

    output_dir = tmp_path / "nested" / "extract"

    with patch("pandas.core.frame.DataFrame.to_parquet"):
      write_extract_parquet(
        output_dir,
        [{"Id": "1"}],
        [],
        [],
        [{"Id": "1"}],
      )

    assert output_dir.exists()


@pytest.mark.unit
class TestToDataframe:
  def test_with_data(self):
    from robosystems.adapters.quickbooks.pipeline.utils import _to_dataframe

    df = _to_dataframe([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}])
    assert len(df) == 2
    assert list(df.columns) == ["a", "b"]

  def test_empty_with_schema(self):
    from robosystems.adapters.quickbooks.pipeline.utils import _to_dataframe

    df = _to_dataframe([], schema={"col1": "str", "col2": "float64"})
    assert len(df) == 0
    assert "col1" in df.columns
    assert "col2" in df.columns

  def test_empty_without_schema(self):
    from robosystems.adapters.quickbooks.pipeline.utils import _to_dataframe

    df = _to_dataframe([])
    assert len(df) == 0
