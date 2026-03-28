"""Tests for OLTPLoader — generic OLTP loading from DuckDB to PostgreSQL."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mock_duckdb_data():
  """Sample dbt output data as lists of dicts."""
  accounts = [
    {
      "external_id": "1",
      "external_source": "quickbooks",
      "code": "Cash",
      "name": "Cash",
      "description": "Cash and equivalents",
      "classification": "asset",
      "sub_classification": "Bank",
      "balance_type": "debit",
      "external_parent_id": None,
      "depth": 0,
      "path": "",
      "currency": "USD",
      "is_active": True,
      "is_placeholder": False,
      "metadata": "{}",
    },
    {
      "external_id": "2",
      "external_source": "quickbooks",
      "code": "Revenue",
      "name": "Revenue",
      "description": "Service revenue",
      "classification": "revenue",
      "sub_classification": "ServiceFeeIncome",
      "balance_type": "credit",
      "external_parent_id": None,
      "depth": 0,
      "path": "",
      "currency": "USD",
      "is_active": True,
      "is_placeholder": False,
      "metadata": "{}",
    },
  ]

  transactions = [
    {
      "external_id": "JournalEntry_100",
      "number": "JE-100",
      "type": "JournalEntry",
      "category": None,
      "amount": 50000,
      "currency": "USD",
      "date": date(2026, 1, 15),
      "due_date": None,
      "merchant_name": None,
      "reference_number": "JE-100",
      "description": "Service revenue",
      "source": "quickbooks",
      "source_id": "JournalEntry_100",
      "status": "posted",
      "metadata": "{}",
    },
  ]

  entries = [
    {
      "external_id": "JournalEntry_100",
      "external_transaction_id": "JournalEntry_100",
      "number": "JE-100",
      "type": "standard",
      "posting_date": date(2026, 1, 15),
      "memo": "Service revenue",
      "status": "posted",
      "metadata": "{}",
    },
  ]

  line_items = [
    {
      "entry_external_id": "JournalEntry_100",
      "account_external_id": "1",
      "debit_amount": 50000,
      "credit_amount": 0,
      "description": "Cash received",
      "line_order": 1,
      "metadata": "{}",
    },
    {
      "entry_external_id": "JournalEntry_100",
      "account_external_id": "2",
      "debit_amount": 0,
      "credit_amount": 50000,
      "description": "Revenue earned",
      "line_order": 2,
      "metadata": "{}",
    },
  ]

  dimensions = [
    {
      "external_id": "1",
      "dimension_type": "department",
      "name": "Engineering",
      "value": "1",
    },
  ]

  return {
    "accounts": accounts,
    "transactions": transactions,
    "entries": entries,
    "line_items": line_items,
    "dimensions": dimensions,
  }


def _make_duckdb_mock(tables: dict[str, list[dict]]):
  """Create a DuckDB connection mock that returns the given tables."""
  mock_con = MagicMock()
  table_names = list(tables.keys())

  def execute_side_effect(query):
    result = MagicMock()
    if query == "SHOW TABLES":
      result.fetchall.return_value = [(t,) for t in table_names]
    else:
      for t in table_names:
        if f"SELECT * FROM {t}" in query:
          rows = tables[t]
          if rows:
            columns = list(rows[0].keys())
            result.description = [(c,) for c in columns]
            result.fetchall.return_value = [tuple(r.values()) for r in rows]
          else:
            result.description = []
            result.fetchall.return_value = []
          break
      else:
        result.description = []
        result.fetchall.return_value = []
    return result

  mock_con.execute.side_effect = execute_side_effect
  return mock_con


class TestOLTPLoader:
  """Test the OLTPLoader class."""

  @patch("robosystems.db.extensions.provision_tenant_schema")
  @patch("robosystems.db.extensions.extensions_session")
  @patch("duckdb.connect")
  def test_load_calls_provision_tenant_schema(
    self,
    mock_duckdb_connect,
    mock_ext_session,
    mock_provision,
    mock_duckdb_data,
  ):
    """Loader provisions the tenant schema before inserting."""
    from robosystems.operations.extensions.loader import OLTPLoader

    mock_duckdb_connect.return_value = _make_duckdb_mock({})

    mock_session = MagicMock()
    mock_ext_session.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_ext_session.return_value.__exit__ = MagicMock(return_value=False)

    loader = OLTPLoader()
    loader.load(
      graph_id="kg0123456789abcdef",
      source="quickbooks",
      connection_id="conn_123",
      duckdb_path="/tmp/test.duckdb",
      created_by="user_123",
    )

    mock_provision.assert_called_once_with("kg0123456789abcdef")

  @patch("robosystems.db.extensions.provision_tenant_schema")
  @patch("robosystems.db.extensions.extensions_session")
  @patch("duckdb.connect")
  def test_load_reads_all_tables(
    self,
    mock_duckdb_connect,
    mock_ext_session,
    mock_provision,
    mock_duckdb_data,
  ):
    """Loader reads all OLTP tables from DuckDB and reports counts."""
    from robosystems.operations.extensions.loader import OLTPLoader

    mock_duckdb_connect.return_value = _make_duckdb_mock(mock_duckdb_data)

    mock_session = MagicMock()
    mock_ext_session.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_ext_session.return_value.__exit__ = MagicMock(return_value=False)

    loader = OLTPLoader()
    result = loader.load(
      graph_id="kg0123456789abcdef",
      source="quickbooks",
      connection_id="conn_123",
      duckdb_path="/tmp/test.duckdb",
      created_by="user_123",
    )

    assert result.elements == 2
    assert result.transactions == 1
    assert result.entries == 1
    assert result.line_items == 2
    assert result.dimensions == 1
    assert result.total_rows == 7

  @patch("robosystems.db.extensions.provision_tenant_schema")
  @patch("robosystems.db.extensions.extensions_session")
  @patch("duckdb.connect")
  def test_load_deletes_before_insert(
    self,
    mock_duckdb_connect,
    mock_ext_session,
    mock_provision,
  ):
    """Loader deletes existing data for source + connection_id."""
    from robosystems.operations.extensions.loader import OLTPLoader

    mock_duckdb_connect.return_value = _make_duckdb_mock({})

    mock_session = MagicMock()
    mock_ext_session.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_ext_session.return_value.__exit__ = MagicMock(return_value=False)

    loader = OLTPLoader()
    loader.load(
      graph_id="kg0123456789abcdef",
      source="quickbooks",
      connection_id="conn_123",
      duckdb_path="/tmp/test.duckdb",
      created_by="user_123",
    )

    assert mock_session.query.called
    assert mock_session.flush.called

  @patch("robosystems.db.extensions.provision_tenant_schema")
  @patch("robosystems.db.extensions.extensions_session")
  @patch("duckdb.connect")
  def test_load_handles_missing_tables(
    self,
    mock_duckdb_connect,
    mock_ext_session,
    mock_provision,
  ):
    """Loader gracefully handles missing tables in DuckDB."""
    from robosystems.operations.extensions.loader import OLTPLoader

    only_accounts = [
      {
        "external_id": "1",
        "external_source": "quickbooks",
        "code": "Cash",
        "name": "Cash",
        "description": None,
        "classification": "asset",
        "sub_classification": None,
        "balance_type": "debit",
        "external_parent_id": None,
        "depth": 0,
        "path": "",
        "currency": "USD",
        "is_active": True,
        "is_placeholder": False,
        "metadata": "{}",
      }
    ]

    mock_duckdb_connect.return_value = _make_duckdb_mock({"accounts": only_accounts})

    mock_session = MagicMock()
    mock_ext_session.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_ext_session.return_value.__exit__ = MagicMock(return_value=False)

    loader = OLTPLoader()
    result = loader.load(
      graph_id="kg0123456789abcdef",
      source="quickbooks",
      connection_id="conn_123",
      duckdb_path="/tmp/test.duckdb",
      created_by="user_123",
    )

    assert result.elements == 1
    assert result.transactions == 0
    assert result.entries == 0
    assert result.line_items == 0

  @patch("robosystems.db.extensions.provision_tenant_schema")
  @patch("robosystems.db.extensions.extensions_session")
  @patch("duckdb.connect")
  def test_load_reports_unresolved_fk_errors(
    self,
    mock_duckdb_connect,
    mock_ext_session,
    mock_provision,
  ):
    """Loader reports errors for unresolved foreign key references."""
    from robosystems.operations.extensions.loader import OLTPLoader

    orphan_lines = [
      {
        "entry_external_id": "nonexistent_entry",
        "account_external_id": "nonexistent_account",
        "debit_amount": 10000,
        "credit_amount": 0,
        "description": "Test",
        "line_order": 1,
        "metadata": "{}",
      }
    ]

    mock_duckdb_connect.return_value = _make_duckdb_mock({"line_items": orphan_lines})

    mock_session = MagicMock()
    mock_ext_session.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_ext_session.return_value.__exit__ = MagicMock(return_value=False)

    loader = OLTPLoader()
    result = loader.load(
      graph_id="kg0123456789abcdef",
      source="quickbooks",
      connection_id="conn_123",
      duckdb_path="/tmp/test.duckdb",
      created_by="user_123",
    )

    assert len(result.errors) == 1
    assert "unknown entry" in result.errors[0]
    assert result.line_items == 0

  def test_load_result_total_rows(self):
    """LoadResult.total_rows sums all table counts."""
    from robosystems.operations.extensions.loader import LoadResult

    result = LoadResult(
      graph_id="kg123",
      source="quickbooks",
      connection_id="conn_1",
      elements=10,
      transactions=5,
      entries=5,
      line_items=20,
      dimensions=3,
    )
    assert result.total_rows == 43
