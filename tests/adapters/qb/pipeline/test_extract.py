"""Tests for QuickBooks extract Dagster asset."""

from unittest.mock import MagicMock, Mock, patch

import pytest
from dagster import build_asset_context

# These imports happen inside qb_extract's function body, so we patch at the source paths.
_PATCH_SESSION = "robosystems.database.SessionFactory"
_PATCH_CREDS = (
  "robosystems.models.core.connection.connection_credentials.ConnectionCredentials"
)
_PATCH_QB_CLIENT = "robosystems.adapters.quickbooks.client.QBClient"
# These are module-level in extract.py, so patch at the extract module
_PATCH_FLATTEN_CO = (
  "robosystems.adapters.quickbooks.pipeline.extract.flatten_company_info"
)
_PATCH_PARSE_JR = (
  "robosystems.adapters.quickbooks.pipeline.extract.parse_journal_report"
)
_PATCH_WORK_DIR = (
  "robosystems.adapters.quickbooks.pipeline.extract.get_pipeline_work_dir"
)
_PATCH_WRITE = "robosystems.adapters.quickbooks.pipeline.extract.write_extract_parquet"

# Phase 2 party + transaction-header flatteners — patched so the mock client's
# auto-generated Mock() return values for get_customers / get_vendors etc. don't
# break iteration in the flatten functions.
_PATCH_FLATTEN_CUSTOMERS = (
  "robosystems.adapters.quickbooks.pipeline.extract.flatten_customers"
)
_PATCH_FLATTEN_VENDORS = (
  "robosystems.adapters.quickbooks.pipeline.extract.flatten_vendors"
)
_PATCH_FLATTEN_EMPLOYEES = (
  "robosystems.adapters.quickbooks.pipeline.extract.flatten_employees"
)
_PATCH_FLATTEN_INVOICES = (
  "robosystems.adapters.quickbooks.pipeline.extract.flatten_invoice_headers"
)
_PATCH_FLATTEN_BILLS = (
  "robosystems.adapters.quickbooks.pipeline.extract.flatten_bill_headers"
)
_PATCH_FLATTEN_PAYMENTS = (
  "robosystems.adapters.quickbooks.pipeline.extract.flatten_payment_headers"
)


def _wire_phase2_client_methods(mock_client):
  """Wire all Phase 2 client methods on a Mock so iteration doesn't break."""
  mock_client.get_customers.return_value = []
  mock_client.get_vendors.return_value = []
  mock_client.get_employees.return_value = []
  mock_client.get_invoices.return_value = []
  mock_client.get_bills.return_value = []
  mock_client.get_payments.return_value = []
  return mock_client


def _make_config(
  graph_id="kg_test123",
  connection_id="conn_abc",
  user_id="user_xyz",
  realm_id="123456789",
  full_rebuild=False,
  lookback_days=60,
  since_date="",
):
  """Create a QBSyncConfig."""
  from robosystems.adapters.quickbooks.pipeline.configs import QBSyncConfig

  return QBSyncConfig(
    graph_id=graph_id,
    connection_id=connection_id,
    user_id=user_id,
    realm_id=realm_id,
    full_rebuild=full_rebuild,
    lookback_days=lookback_days,
    since_date=since_date,
  )


def _make_mock_session():
  """Create a context-manager-compatible mock DB session."""
  mock_session = MagicMock()
  mock_session.__enter__ = Mock(return_value=mock_session)
  mock_session.__exit__ = Mock(return_value=False)
  return mock_session


@pytest.mark.unit
class TestQbExtractSuccess:
  """Test successful execution paths for qb_extract."""

  def test_extract_incremental_returns_materialize_result(self, tmp_path):
    """Test incremental extract returns MaterializeResult with correct metadata."""
    from dagster import MaterializeResult

    from robosystems.adapters.quickbooks.pipeline.extract import qb_extract

    config = _make_config(full_rebuild=False, lookback_days=60)

    mock_creds = Mock()
    mock_creds.get_credentials.return_value = {
      "access_token": "tok",
      "refresh_token": "ref",
    }

    mock_client = Mock()
    mock_client.get_entity_info.return_value = [{"Id": "1", "CompanyName": "Acme"}]
    mock_client.get_accounts.return_value = [
      {"Id": "1", "Name": "Cash", "AccountType": "Bank"}
    ]
    mock_client.get_transactions.return_value = {}
    _wire_phase2_client_methods(mock_client)

    company_info_flat = [{"Id": "1", "CompanyName": "Acme"}]
    journal_entries = [
      {
        "Id": "Invoice_101",
        "TxnDate": "2024-01-15",
        "DocNumber": "INV-1",
        "TotalAmt": 500.0,
        "PrivateNote": "",
        "Adjustment": False,
      }
    ]
    journal_lines = [
      {
        "journal_entry_id": "Invoice_101",
        "line_num": 1,
        "Amount": 500.0,
        "PostingType": "Debit",
        "AccountRef_value": "10",
        "AccountRef_name": "AR",
        "Description": "Memo",
        "DetailType": "JournalEntryLineDetail",
        "DepartmentRef_value": "",
        "DepartmentRef_name": "",
        "ClassRef_value": "",
        "ClassRef_name": "",
        "LocationRef_value": "",
        "LocationRef_name": "",
      },
    ]

    mock_session = _make_mock_session()
    work_dir = tmp_path / "qb_pipeline" / config.graph_id
    extract_dir = work_dir / "extract"

    with (
      patch(_PATCH_SESSION, return_value=mock_session),
      patch(_PATCH_CREDS) as MockCreds,
      patch(_PATCH_QB_CLIENT, return_value=mock_client),
      patch(_PATCH_FLATTEN_CO, return_value=company_info_flat),
      patch(_PATCH_PARSE_JR, return_value=(journal_entries, journal_lines)),
      patch(_PATCH_FLATTEN_CUSTOMERS, return_value=[]),
      patch(_PATCH_FLATTEN_VENDORS, return_value=[]),
      patch(_PATCH_FLATTEN_EMPLOYEES, return_value=[]),
      patch(_PATCH_FLATTEN_INVOICES, return_value=[]),
      patch(_PATCH_FLATTEN_BILLS, return_value=[]),
      patch(_PATCH_FLATTEN_PAYMENTS, return_value=[]),
      patch(_PATCH_WORK_DIR, return_value=work_dir),
      patch(_PATCH_WRITE) as mock_write,
    ):
      MockCreds.get_by_connection_id.return_value = mock_creds
      context = build_asset_context()
      result = qb_extract(context, config)

    assert isinstance(result, MaterializeResult)
    assert result.metadata["graph_id"] == config.graph_id
    assert result.metadata["realm_id"] == config.realm_id
    assert result.metadata["accounts"] == 1
    assert result.metadata["journal_entries"] == 1
    assert result.metadata["journal_lines"] == 1
    assert result.metadata["full_rebuild"] is False
    assert str(extract_dir) in result.metadata["extract_path"]

    mock_write.assert_called_once()
    mock_client.get_entity_info.assert_called_once()
    mock_client.get_accounts.assert_called_once()
    mock_client.get_transactions.assert_called_once()

  def test_extract_full_rebuild_uses_2000_start_date(self, tmp_path):
    """Test full rebuild uses start_date='2000-01-01'."""
    from robosystems.adapters.quickbooks.pipeline.extract import qb_extract

    config = _make_config(full_rebuild=True)

    mock_creds = Mock()
    mock_creds.get_credentials.return_value = {
      "access_token": "t",
      "refresh_token": "r",
    }

    mock_client = Mock()
    mock_client.get_entity_info.return_value = []
    mock_client.get_accounts.return_value = []
    mock_client.get_transactions.return_value = {}
    _wire_phase2_client_methods(mock_client)

    mock_session = _make_mock_session()
    work_dir = tmp_path / "qb_pipeline" / config.graph_id

    with (
      patch(_PATCH_SESSION, return_value=mock_session),
      patch(_PATCH_CREDS) as MockCreds,
      patch(_PATCH_QB_CLIENT, return_value=mock_client),
      patch(_PATCH_FLATTEN_CO, return_value=[]),
      patch(_PATCH_PARSE_JR, return_value=([], [])),
      patch(_PATCH_FLATTEN_CUSTOMERS, return_value=[]),
      patch(_PATCH_FLATTEN_VENDORS, return_value=[]),
      patch(_PATCH_FLATTEN_EMPLOYEES, return_value=[]),
      patch(_PATCH_FLATTEN_INVOICES, return_value=[]),
      patch(_PATCH_FLATTEN_BILLS, return_value=[]),
      patch(_PATCH_FLATTEN_PAYMENTS, return_value=[]),
      patch(_PATCH_WORK_DIR, return_value=work_dir),
      patch(_PATCH_WRITE),
    ):
      MockCreds.get_by_connection_id.return_value = mock_creds
      context = build_asset_context()
      qb_extract(context, config)

    call_kwargs = mock_client.get_transactions.call_args
    assert call_kwargs[1]["start_date"] == "2000-01-01"

  def test_extract_incremental_uses_lookback_start_date(self, tmp_path):
    """Test incremental extract computes start_date from lookback_days."""
    from datetime import datetime, timedelta

    from robosystems.adapters.quickbooks.pipeline.extract import qb_extract

    config = _make_config(full_rebuild=False, lookback_days=30)

    mock_creds = Mock()
    mock_creds.get_credentials.return_value = {
      "access_token": "t",
      "refresh_token": "r",
    }

    mock_client = Mock()
    mock_client.get_entity_info.return_value = []
    mock_client.get_accounts.return_value = []
    mock_client.get_transactions.return_value = {}
    _wire_phase2_client_methods(mock_client)

    mock_session = _make_mock_session()
    work_dir = tmp_path / "qb_pipeline" / config.graph_id

    with (
      patch(_PATCH_SESSION, return_value=mock_session),
      patch(_PATCH_CREDS) as MockCreds,
      patch(_PATCH_QB_CLIENT, return_value=mock_client),
      patch(_PATCH_FLATTEN_CO, return_value=[]),
      patch(_PATCH_PARSE_JR, return_value=([], [])),
      patch(_PATCH_FLATTEN_CUSTOMERS, return_value=[]),
      patch(_PATCH_FLATTEN_VENDORS, return_value=[]),
      patch(_PATCH_FLATTEN_EMPLOYEES, return_value=[]),
      patch(_PATCH_FLATTEN_INVOICES, return_value=[]),
      patch(_PATCH_FLATTEN_BILLS, return_value=[]),
      patch(_PATCH_FLATTEN_PAYMENTS, return_value=[]),
      patch(_PATCH_WORK_DIR, return_value=work_dir),
      patch(_PATCH_WRITE),
    ):
      MockCreds.get_by_connection_id.return_value = mock_creds
      context = build_asset_context()
      qb_extract(context, config)

    call_kwargs = mock_client.get_transactions.call_args
    start_date = call_kwargs[1]["start_date"]
    expected = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    assert start_date == expected

  def test_extract_since_date_overrides_lookback(self, tmp_path):
    """since_date is used as start_date when full_rebuild=False."""
    from robosystems.adapters.quickbooks.pipeline.extract import qb_extract

    config = _make_config(full_rebuild=False, lookback_days=60, since_date="2024-01-15")

    mock_creds = Mock()
    mock_creds.get_credentials.return_value = {
      "access_token": "t",
      "refresh_token": "r",
    }

    mock_client = Mock()
    mock_client.get_entity_info.return_value = []
    mock_client.get_accounts.return_value = []
    mock_client.get_transactions.return_value = {}
    _wire_phase2_client_methods(mock_client)

    mock_session = _make_mock_session()
    work_dir = tmp_path / "qb_pipeline" / config.graph_id

    with (
      patch(_PATCH_SESSION, return_value=mock_session),
      patch(_PATCH_CREDS) as MockCreds,
      patch(_PATCH_QB_CLIENT, return_value=mock_client),
      patch(_PATCH_FLATTEN_CO, return_value=[]),
      patch(_PATCH_PARSE_JR, return_value=([], [])),
      patch(_PATCH_FLATTEN_CUSTOMERS, return_value=[]),
      patch(_PATCH_FLATTEN_VENDORS, return_value=[]),
      patch(_PATCH_FLATTEN_EMPLOYEES, return_value=[]),
      patch(_PATCH_FLATTEN_INVOICES, return_value=[]),
      patch(_PATCH_FLATTEN_BILLS, return_value=[]),
      patch(_PATCH_FLATTEN_PAYMENTS, return_value=[]),
      patch(_PATCH_WORK_DIR, return_value=work_dir),
      patch(_PATCH_WRITE),
    ):
      MockCreds.get_by_connection_id.return_value = mock_creds
      context = build_asset_context()
      qb_extract(context, config)

    assert mock_client.get_transactions.call_args[1]["start_date"] == "2024-01-15"

  def test_extract_full_rebuild_beats_since_date(self, tmp_path):
    """full_rebuild takes precedence over since_date when both are set."""
    from robosystems.adapters.quickbooks.pipeline.extract import qb_extract

    config = _make_config(full_rebuild=True, since_date="2024-01-15")

    mock_creds = Mock()
    mock_creds.get_credentials.return_value = {
      "access_token": "t",
      "refresh_token": "r",
    }

    mock_client = Mock()
    mock_client.get_entity_info.return_value = []
    mock_client.get_accounts.return_value = []
    mock_client.get_transactions.return_value = {}
    _wire_phase2_client_methods(mock_client)

    mock_session = _make_mock_session()
    work_dir = tmp_path / "qb_pipeline" / config.graph_id

    with (
      patch(_PATCH_SESSION, return_value=mock_session),
      patch(_PATCH_CREDS) as MockCreds,
      patch(_PATCH_QB_CLIENT, return_value=mock_client),
      patch(_PATCH_FLATTEN_CO, return_value=[]),
      patch(_PATCH_PARSE_JR, return_value=([], [])),
      patch(_PATCH_FLATTEN_CUSTOMERS, return_value=[]),
      patch(_PATCH_FLATTEN_VENDORS, return_value=[]),
      patch(_PATCH_FLATTEN_EMPLOYEES, return_value=[]),
      patch(_PATCH_FLATTEN_INVOICES, return_value=[]),
      patch(_PATCH_FLATTEN_BILLS, return_value=[]),
      patch(_PATCH_FLATTEN_PAYMENTS, return_value=[]),
      patch(_PATCH_WORK_DIR, return_value=work_dir),
      patch(_PATCH_WRITE),
    ):
      MockCreds.get_by_connection_id.return_value = mock_creds
      context = build_asset_context()
      qb_extract(context, config)

    assert mock_client.get_transactions.call_args[1]["start_date"] == "2000-01-01"

  def test_extract_passes_credentials_to_qb_client(self, tmp_path):
    """Test that QBClient receives credentials from ConnectionCredentials."""
    from robosystems.adapters.quickbooks.pipeline.extract import qb_extract

    config = _make_config(realm_id="realm_abc")

    credentials_dict = {"access_token": "my_tok", "refresh_token": "my_ref"}
    mock_creds = Mock()
    mock_creds.get_credentials.return_value = credentials_dict

    mock_client = Mock()
    mock_client.get_entity_info.return_value = []
    mock_client.get_accounts.return_value = []
    mock_client.get_transactions.return_value = {}
    _wire_phase2_client_methods(mock_client)

    mock_session = _make_mock_session()
    work_dir = tmp_path / "qb_pipeline" / config.graph_id

    with (
      patch(_PATCH_SESSION, return_value=mock_session),
      patch(_PATCH_CREDS) as MockCreds,
      patch(_PATCH_QB_CLIENT, return_value=mock_client) as MockQBClient,
      patch(_PATCH_FLATTEN_CO, return_value=[]),
      patch(_PATCH_PARSE_JR, return_value=([], [])),
      patch(_PATCH_FLATTEN_CUSTOMERS, return_value=[]),
      patch(_PATCH_FLATTEN_VENDORS, return_value=[]),
      patch(_PATCH_FLATTEN_EMPLOYEES, return_value=[]),
      patch(_PATCH_FLATTEN_INVOICES, return_value=[]),
      patch(_PATCH_FLATTEN_BILLS, return_value=[]),
      patch(_PATCH_FLATTEN_PAYMENTS, return_value=[]),
      patch(_PATCH_WORK_DIR, return_value=work_dir),
      patch(_PATCH_WRITE),
    ):
      MockCreds.get_by_connection_id.return_value = mock_creds
      context = build_asset_context()
      qb_extract(context, config)

    MockQBClient.assert_called_once_with(
      realm_id="realm_abc",
      qb_credentials=credentials_dict,
    )


@pytest.mark.unit
class TestQbExtractErrors:
  """Test error handling in qb_extract."""

  def test_extract_raises_when_no_credentials(self, tmp_path):
    """Test ValueError when no credentials found for connection."""
    from robosystems.adapters.quickbooks.pipeline.extract import qb_extract

    config = _make_config()
    mock_session = _make_mock_session()

    with (
      patch(_PATCH_SESSION, return_value=mock_session),
      patch(_PATCH_CREDS) as MockCreds,
    ):
      MockCreds.get_by_connection_id.return_value = None
      context = build_asset_context()

      with pytest.raises(ValueError, match="No credentials found"):
        qb_extract(context, config)

  def test_extract_raises_when_no_realm_id(self, tmp_path):
    """Test ValueError when realm_id is empty."""
    from robosystems.adapters.quickbooks.pipeline.extract import qb_extract

    config = _make_config(realm_id="")  # Empty realm_id

    mock_creds = Mock()
    mock_creds.get_credentials.return_value = {
      "access_token": "t",
      "refresh_token": "r",
    }

    mock_session = _make_mock_session()

    with (
      patch(_PATCH_SESSION, return_value=mock_session),
      patch(_PATCH_CREDS) as MockCreds,
    ):
      MockCreds.get_by_connection_id.return_value = mock_creds
      context = build_asset_context()

      with pytest.raises(ValueError, match="realm_id is required"):
        qb_extract(context, config)

  def test_extract_calls_get_by_connection_id_with_correct_id(self, tmp_path):
    """Test that credentials lookup uses the config connection_id."""
    from robosystems.adapters.quickbooks.pipeline.extract import qb_extract

    config = _make_config(connection_id="conn_specific_456")
    mock_session = _make_mock_session()

    with (
      patch(_PATCH_SESSION, return_value=mock_session),
      patch(_PATCH_CREDS) as MockCreds,
    ):
      MockCreds.get_by_connection_id.return_value = None
      context = build_asset_context()

      with pytest.raises(ValueError):
        qb_extract(context, config)

    MockCreds.get_by_connection_id.assert_called_once_with(
      "conn_specific_456", mock_session
    )

  def test_extract_passes_realm_id_to_qb_client(self, tmp_path):
    """Test that QBClient is initialized with the correct realm_id."""
    from robosystems.adapters.quickbooks.pipeline.extract import qb_extract

    config = _make_config(realm_id="realm_9876")

    mock_creds = Mock()
    credentials_dict = {"access_token": "t", "refresh_token": "r"}
    mock_creds.get_credentials.return_value = credentials_dict

    mock_client = Mock()
    mock_client.get_entity_info.return_value = []
    mock_client.get_accounts.return_value = []
    mock_client.get_transactions.return_value = {}
    _wire_phase2_client_methods(mock_client)

    mock_session = _make_mock_session()
    work_dir = tmp_path / "qb_pipeline" / config.graph_id

    with (
      patch(_PATCH_SESSION, return_value=mock_session),
      patch(_PATCH_CREDS) as MockCreds,
      patch(_PATCH_QB_CLIENT, return_value=mock_client) as MockQBClient,
      patch(_PATCH_FLATTEN_CO, return_value=[]),
      patch(_PATCH_PARSE_JR, return_value=([], [])),
      patch(_PATCH_FLATTEN_CUSTOMERS, return_value=[]),
      patch(_PATCH_FLATTEN_VENDORS, return_value=[]),
      patch(_PATCH_FLATTEN_EMPLOYEES, return_value=[]),
      patch(_PATCH_FLATTEN_INVOICES, return_value=[]),
      patch(_PATCH_FLATTEN_BILLS, return_value=[]),
      patch(_PATCH_FLATTEN_PAYMENTS, return_value=[]),
      patch(_PATCH_WORK_DIR, return_value=work_dir),
      patch(_PATCH_WRITE),
    ):
      MockCreds.get_by_connection_id.return_value = mock_creds
      context = build_asset_context()
      qb_extract(context, config)

    MockQBClient.assert_called_once_with(
      realm_id="realm_9876",
      qb_credentials=credentials_dict,
    )


@pytest.mark.unit
class TestQbExtractMetadata:
  """Test that qb_extract returns correct metadata values."""

  def test_extract_metadata_contains_extract_path(self, tmp_path):
    """Test extract_path in result metadata reflects the work directory."""
    from dagster import MaterializeResult

    from robosystems.adapters.quickbooks.pipeline.extract import qb_extract

    config = _make_config(graph_id="kg_meta_test")

    mock_creds = Mock()
    mock_creds.get_credentials.return_value = {
      "access_token": "t",
      "refresh_token": "r",
    }

    mock_client = Mock()
    mock_client.get_entity_info.return_value = []
    mock_client.get_accounts.return_value = [{"Id": "1"}, {"Id": "2"}]
    mock_client.get_transactions.return_value = {}
    _wire_phase2_client_methods(mock_client)

    mock_session = _make_mock_session()
    work_dir = tmp_path / "qb_pipeline" / config.graph_id

    with (
      patch(_PATCH_SESSION, return_value=mock_session),
      patch(_PATCH_CREDS) as MockCreds,
      patch(_PATCH_QB_CLIENT, return_value=mock_client),
      patch(_PATCH_FLATTEN_CO, return_value=[]),
      patch(_PATCH_PARSE_JR, return_value=([], [])),
      patch(_PATCH_FLATTEN_CUSTOMERS, return_value=[]),
      patch(_PATCH_FLATTEN_VENDORS, return_value=[]),
      patch(_PATCH_FLATTEN_EMPLOYEES, return_value=[]),
      patch(_PATCH_FLATTEN_INVOICES, return_value=[]),
      patch(_PATCH_FLATTEN_BILLS, return_value=[]),
      patch(_PATCH_FLATTEN_PAYMENTS, return_value=[]),
      patch(_PATCH_WORK_DIR, return_value=work_dir),
      patch(_PATCH_WRITE),
    ):
      MockCreds.get_by_connection_id.return_value = mock_creds
      context = build_asset_context()
      result = qb_extract(context, config)

    assert isinstance(result, MaterializeResult)
    assert "extract" in result.metadata["extract_path"]
    assert result.metadata["accounts"] == 2

  def test_extract_metadata_full_rebuild_flag(self, tmp_path):
    """Test full_rebuild flag is reflected in result metadata."""
    from dagster import MaterializeResult

    from robosystems.adapters.quickbooks.pipeline.extract import qb_extract

    config = _make_config(full_rebuild=True)

    mock_creds = Mock()
    mock_creds.get_credentials.return_value = {
      "access_token": "t",
      "refresh_token": "r",
    }

    mock_client = Mock()
    mock_client.get_entity_info.return_value = []
    mock_client.get_accounts.return_value = []
    mock_client.get_transactions.return_value = {}
    _wire_phase2_client_methods(mock_client)

    mock_session = _make_mock_session()
    work_dir = tmp_path / "qb_pipeline" / config.graph_id

    with (
      patch(_PATCH_SESSION, return_value=mock_session),
      patch(_PATCH_CREDS) as MockCreds,
      patch(_PATCH_QB_CLIENT, return_value=mock_client),
      patch(_PATCH_FLATTEN_CO, return_value=[]),
      patch(_PATCH_PARSE_JR, return_value=([], [])),
      patch(_PATCH_FLATTEN_CUSTOMERS, return_value=[]),
      patch(_PATCH_FLATTEN_VENDORS, return_value=[]),
      patch(_PATCH_FLATTEN_EMPLOYEES, return_value=[]),
      patch(_PATCH_FLATTEN_INVOICES, return_value=[]),
      patch(_PATCH_FLATTEN_BILLS, return_value=[]),
      patch(_PATCH_FLATTEN_PAYMENTS, return_value=[]),
      patch(_PATCH_WORK_DIR, return_value=work_dir),
      patch(_PATCH_WRITE),
    ):
      MockCreds.get_by_connection_id.return_value = mock_creds
      context = build_asset_context()
      result = qb_extract(context, config)

    assert isinstance(result, MaterializeResult)
    assert result.metadata["full_rebuild"] is True

  def test_extract_metadata_zero_entries_and_lines(self, tmp_path):
    """Test metadata correctly reports zeros when there are no transactions."""
    from dagster import MaterializeResult

    from robosystems.adapters.quickbooks.pipeline.extract import qb_extract

    config = _make_config()

    mock_creds = Mock()
    mock_creds.get_credentials.return_value = {
      "access_token": "t",
      "refresh_token": "r",
    }

    mock_client = Mock()
    mock_client.get_entity_info.return_value = []
    mock_client.get_accounts.return_value = []
    mock_client.get_transactions.return_value = {}
    _wire_phase2_client_methods(mock_client)

    mock_session = _make_mock_session()
    work_dir = tmp_path / "qb_pipeline" / config.graph_id

    with (
      patch(_PATCH_SESSION, return_value=mock_session),
      patch(_PATCH_CREDS) as MockCreds,
      patch(_PATCH_QB_CLIENT, return_value=mock_client),
      patch(_PATCH_FLATTEN_CO, return_value=[]),
      patch(_PATCH_PARSE_JR, return_value=([], [])),
      patch(_PATCH_FLATTEN_CUSTOMERS, return_value=[]),
      patch(_PATCH_FLATTEN_VENDORS, return_value=[]),
      patch(_PATCH_FLATTEN_EMPLOYEES, return_value=[]),
      patch(_PATCH_FLATTEN_INVOICES, return_value=[]),
      patch(_PATCH_FLATTEN_BILLS, return_value=[]),
      patch(_PATCH_FLATTEN_PAYMENTS, return_value=[]),
      patch(_PATCH_WORK_DIR, return_value=work_dir),
      patch(_PATCH_WRITE),
    ):
      MockCreds.get_by_connection_id.return_value = mock_creds
      context = build_asset_context()
      result = qb_extract(context, config)

    assert isinstance(result, MaterializeResult)
    assert result.metadata["journal_entries"] == 0
    assert result.metadata["journal_lines"] == 0
    assert result.metadata["accounts"] == 0
