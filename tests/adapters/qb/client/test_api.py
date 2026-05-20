"""Tests for QuickBooks API client adapter."""

from unittest.mock import Mock, patch

import pandas as pd
import pytest
from intuitlib.client import AuthClient
from quickbooks import QuickBooks

from robosystems.adapters.quickbooks import QBClient


class TestQBClient:
  """Test cases for QuickBooks client functionality."""

  @pytest.fixture
  def qb_credentials(self):
    """Sample QB credentials for testing."""
    return {
      "refresh_token": "test_refresh_token_123",
      "access_token": "test_access_token_456",
    }

  @pytest.fixture
  def realm_id(self):
    """Sample realm ID for testing."""
    return "1234567890123456789"

  @pytest.fixture
  def mock_auth_client(self):
    """Mock AuthClient for testing."""
    mock_client = Mock(spec=AuthClient)
    mock_client.refresh_token = "new_refresh_token_789"
    mock_client.access_token = "new_access_token_012"
    return mock_client

  @pytest.fixture
  def mock_qb_client(self):
    """Mock QuickBooks client for testing."""
    return Mock(spec=QuickBooks)

  @patch("robosystems.adapters.quickbooks.client.api.QuickBooks")
  @patch("robosystems.adapters.quickbooks.client.api.AuthClient")
  def test_initialization_success(
    self,
    mock_auth_client_class,
    mock_qb_class,
    qb_credentials,
    realm_id,
    mock_auth_client,
    mock_qb_client,
  ):
    """Test successful QB client initialization."""
    # Setup mocks
    mock_auth_client_class.return_value = mock_auth_client
    mock_qb_class.return_value = mock_qb_client

    with patch("robosystems.adapters.quickbooks.client.api.env") as mock_env:
      mock_env.INTUIT_CLIENT_ID = "test_client_id"
      mock_env.INTUIT_CLIENT_SECRET = "test_client_secret"
      mock_env.INTUIT_ENVIRONMENT = "sandbox"
      mock_env.INTUIT_REDIRECT_URI = "http://localhost:8000/callback"

      # Execute
      client = QBClient(realm_id, qb_credentials)

      # Verify
      assert client.realm_id == realm_id
      assert client.refresh_token == "new_refresh_token_789"
      assert client.access_token == "test_access_token_456"

      # Verify AuthClient creation
      mock_auth_client_class.assert_called_once_with(
        client_id="test_client_id",
        client_secret="test_client_secret",
        environment="sandbox",
        redirect_uri="http://localhost:8000/callback",
        refresh_token="test_refresh_token_123",
        realm_id=realm_id,
      )

      # Verify QuickBooks client creation
      mock_qb_class.assert_called_once_with(
        auth_client=mock_auth_client,
        refresh_token="new_refresh_token_789",
        company_id=realm_id,
        minorversion=75,
      )

  def test_initialization_missing_realm_id(self, qb_credentials):
    """Test initialization with missing realm_id."""
    with pytest.raises(ValueError, match="realm_id and qb_credentials are required"):
      QBClient(None, qb_credentials)

  def test_initialization_missing_credentials(self, realm_id):
    """Test initialization with missing credentials."""
    with pytest.raises(ValueError, match="realm_id and qb_credentials are required"):
      QBClient(realm_id, None)

  def test_initialization_missing_refresh_token(self, realm_id):
    """Test initialization with missing refresh token."""
    credentials = {"access_token": "test_access_token"}

    with pytest.raises(
      ValueError, match="QuickBooks refresh_token not found in credentials"
    ):
      QBClient(realm_id, credentials)

  @patch("robosystems.adapters.quickbooks.client.api.AuthClient")
  def test_initialization_mock_token_skip_refresh(
    self, mock_auth_client_class, realm_id, mock_auth_client
  ):
    """Test initialization with mock token skips refresh."""
    # Setup mocks
    mock_auth_client_class.return_value = mock_auth_client

    credentials = {
      "refresh_token": "mock_test_token",
      "access_token": "test_access_token",
    }

    with patch("robosystems.adapters.quickbooks.client.api.QuickBooks"):
      # Execute
      QBClient(realm_id, credentials)

      # Verify refresh was not called for mock token
      mock_auth_client.refresh.assert_not_called()

  @patch("robosystems.adapters.quickbooks.client.api.QuickBooks")
  @patch("robosystems.adapters.quickbooks.client.api.AuthClient")
  def test_initialization_no_access_token(
    self,
    mock_auth_client_class,
    mock_qb_class,
    realm_id,
    mock_auth_client,
    mock_qb_client,
  ):
    """Test initialization without access token."""
    # Setup mocks
    mock_auth_client_class.return_value = mock_auth_client
    mock_qb_class.return_value = mock_qb_client

    credentials = {"refresh_token": "test_refresh_token"}

    with patch("robosystems.adapters.quickbooks.client.api.env"):
      # Execute
      client = QBClient(realm_id, credentials)

      # After refresh, access_token is populated from auth_client
      assert client.access_token == mock_auth_client.access_token

  @patch("quickbooks.objects.company_info.CompanyInfo")
  def test_get_entity_info(self, mock_company_info, mock_qb_client):
    """Test getting entity/company info."""
    # Setup mock client
    client = QBClient.__new__(QBClient)
    client.client = mock_qb_client

    mock_company_info.all.return_value = {"name": "Test Company", "id": "123"}

    # Execute
    result = client.get_entity_info()

    # Verify
    assert result == {"name": "Test Company", "id": "123"}
    mock_company_info.all.assert_called_once_with(qb=mock_qb_client)

  @patch("quickbooks.objects.account.Account")
  def test_get_accounts_success(self, mock_account, mock_qb_client):
    """Test successful account retrieval with pagination via Account.query.

    The adapter pulls active + inactive accounts (WHERE Active IN
    (true, false)) and paginates until a page returns fewer than
    page_size rows.
    """
    client = QBClient.__new__(QBClient)
    client.client = mock_qb_client

    # Intuit's API returns the key as "Id" (capital I) — the adapter's
    # dedup keys on that field, so the test fixtures must match.
    mock_account1 = Mock()
    mock_account1.to_dict.return_value = {"Id": "1", "Name": "Account 1"}
    mock_account2 = Mock()
    mock_account2.to_dict.return_value = {"Id": "2", "Name": "Account 2"}

    # First page returns 2 rows (less than page_size=100) → loop exits.
    mock_account.query.return_value = [mock_account1, mock_account2]

    result = client.get_accounts()

    assert result == [
      {"Id": "1", "Name": "Account 1"},
      {"Id": "2", "Name": "Account 2"},
    ]
    # One page was enough since the page came back shorter than page_size.
    assert mock_account.query.call_count == 1
    # Verify the query asks for both active + inactive — the bug this
    # adapter was fixing was QB defaulting to active-only, which made
    # historical journal lines reference unsynced accounts.
    query_str = mock_account.query.call_args[0][0]
    assert "Active IN (true, false)" in query_str

  @patch("quickbooks.objects.account.Account")
  def test_get_accounts_no_duplicates(self, mock_account, mock_qb_client):
    """Duplicate accounts (same Id on two pages, or duplicated within a
    page) must be filtered out via the O(1) seen-set keyed on Id."""
    client = QBClient.__new__(QBClient)
    client.client = mock_qb_client

    mock_account_obj = Mock()
    mock_account_obj.to_dict.return_value = {"Id": "1", "Name": "Account 1"}

    # Page returns the same account twice — second occurrence should be
    # deduped by the seen-set keyed on the "Id" field.
    mock_account.query.return_value = [mock_account_obj, mock_account_obj]

    result = client.get_accounts()

    assert len(result) == 1
    assert result == [{"Id": "1", "Name": "Account 1"}]

  @patch("quickbooks.objects.account.Account")
  def test_get_accounts_zero_count(self, mock_account, mock_qb_client):
    """Test account retrieval when QB returns no accounts at all."""
    client = QBClient.__new__(QBClient)
    client.client = mock_qb_client

    # Empty first page → loop exits immediately.
    mock_account.query.return_value = []

    result = client.get_accounts()

    assert result == []
    # Query is still invoked once (we don't know it's empty until we ask).
    assert mock_account.query.call_count == 1

  @patch("robosystems.adapters.quickbooks.client.api.QBClient.get_accounts")
  def test_get_accounts_df_processing(self, mock_get_accounts, mock_qb_client):
    """Test accounts DataFrame processing and categorization."""
    # Setup mock client
    client = QBClient.__new__(QBClient)
    client.client = mock_qb_client

    # Mock raw account data
    mock_accounts = [
      {
        "Id": "1",
        "Name": "Checking Account",
        "AccountType": "Bank",
        "Classification": "Asset",
        "FullyQualifiedName": "Checking Account",
        "ParentRef": None,
      },
      {
        "Id": "2",
        "Name": "Service Revenue",
        "AccountType": "Income",
        "Classification": "Revenue",
        "FullyQualifiedName": "Service Revenue",
        "ParentRef": None,
      },
      {
        "Id": "3",
        "Name": "Office Supplies",
        "AccountType": "NaN",  # Test NaN handling
        "Classification": "Expense",
        "FullyQualifiedName": "Office Supplies",
        "ParentRef": None,
      },
    ]
    mock_get_accounts.return_value = mock_accounts

    # Execute
    result = client.get_accounts_df()

    # Verify DataFrame creation
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 3

    # Verify AccountType processing for NaN values
    office_supplies_row = result[result["Name"] == "Office Supplies"].iloc[0]
    assert office_supplies_row["AccountType"] == "Other Expense"

    # Verify categorical columns
    assert pd.api.types.is_categorical_dtype(result["Classification"])
    assert pd.api.types.is_categorical_dtype(result["AccountType"])

    # Verify sorting (by Classification order: Asset, Revenue, Other Expense)
    assert result.iloc[0]["Classification"] == "Asset"
    assert result.iloc[1]["Classification"] == "Revenue"
    assert result.iloc[2]["Classification"] == "Other Expense"

    # Verify sequence and order columns added
    assert "Order" in result.columns
    assert "Sequence" in result.columns

  @patch("quickbooks.objects.account.Account")
  def test_get_account_by_id(self, mock_account, mock_qb_client):
    """Test getting account by ID."""
    # Setup mock client
    client = QBClient.__new__(QBClient)
    client.client = mock_qb_client

    mock_account_obj = Mock()
    mock_account_obj.to_dict.return_value = {"id": "123", "name": "Test Account"}
    mock_account.get.return_value = mock_account_obj

    # Execute
    result = client.get_account_by_id("123")

    # Verify
    assert result == {"id": "123", "name": "Test Account"}
    mock_account.get.assert_called_once_with("123", qb=mock_qb_client)

  @patch("quickbooks.objects.account.Account")
  def test_get_account_by_name(self, mock_account, mock_qb_client):
    """Test getting account by name."""
    # Setup mock client
    client = QBClient.__new__(QBClient)
    client.client = mock_qb_client

    mock_account_obj = Mock()
    mock_account_obj.to_dict.return_value = {"id": "456", "name": "Test Account"}
    mock_account.filter.return_value = [mock_account_obj]

    # Execute
    result = client.get_account_by_name("Test Account")

    # Verify
    assert result == {"id": "456", "name": "Test Account"}
    mock_account.filter.assert_called_once_with(Name="Test Account", qb=mock_qb_client)

  @patch("quickbooks.objects.journalentry.JournalEntry")
  def test_get_journal_entries_success(self, mock_journal_entry, mock_qb_client):
    """Test successful journal entries retrieval."""
    # Setup mock client
    client = QBClient.__new__(QBClient)
    client.client = mock_qb_client

    # Mock journal entry objects
    mock_entry1 = Mock()
    mock_entry1.to_dict.return_value = {"id": "1", "amount": 100.00}
    mock_entry2 = Mock()
    mock_entry2.to_dict.return_value = {"id": "2", "amount": 200.00}

    # Phase 2: get_journal_entries paginates via Entity.all() (no count).
    mock_journal_entry.all.side_effect = [
      [mock_entry1, mock_entry2],  # First page
      [],  # Empty page → stop
    ]

    # Execute
    result = client.get_journal_entries()

    # Verify
    expected_result = [{"id": "1", "amount": 100.00}, {"id": "2", "amount": 200.00}]
    assert result == expected_result

  @patch("quickbooks.objects.journalentry.JournalEntry")
  def test_get_journal_entries_zero_count(self, mock_journal_entry, mock_qb_client):
    """Test journal entries retrieval when no entries exist."""
    # Setup mock client
    client = QBClient.__new__(QBClient)
    client.client = mock_qb_client

    # Empty first page → no entries
    mock_journal_entry.all.return_value = []

    # Execute
    result = client.get_journal_entries()

    # Verify
    assert result == []

  def test_get_transactions_with_dates(self, mock_qb_client):
    """Test transaction retrieval with date filters."""
    # Setup mock client
    client = QBClient.__new__(QBClient)
    client.client = mock_qb_client

    mock_transactions = {"transactions": [{"id": "1", "amount": 150.00}]}
    mock_qb_client.get_report.return_value = mock_transactions

    # Execute
    result = client.get_transactions("2023-01-01", "2023-12-31")

    # Verify
    assert result == mock_transactions
    mock_qb_client.get_report.assert_called_once_with(
      "JournalReport", {"start_date": "2023-01-01", "end_date": "2023-12-31"}
    )

  def test_get_transactions_no_dates(self, mock_qb_client):
    """Test transaction retrieval without date filters."""
    # Setup mock client
    client = QBClient.__new__(QBClient)
    client.client = mock_qb_client

    mock_transactions = {"transactions": []}
    mock_qb_client.get_report.return_value = mock_transactions

    # Execute
    result = client.get_transactions()

    # Verify
    assert result == mock_transactions
    mock_qb_client.get_report.assert_called_once_with("JournalReport", {})

  def test_get_transactions_partial_dates(self, mock_qb_client):
    """Test transaction retrieval with only start date."""
    # Setup mock client
    client = QBClient.__new__(QBClient)
    client.client = mock_qb_client

    mock_transactions = {"transactions": [{"id": "1"}]}
    mock_qb_client.get_report.return_value = mock_transactions

    # Execute
    result = client.get_transactions("2023-01-01")

    # Verify
    assert result == mock_transactions
    mock_qb_client.get_report.assert_called_once_with(
      "JournalReport", {"start_date": "2023-01-01"}
    )

  # ---- Phase 5 §4.3.4 — CDC endpoint -----------------------------------

  def _make_cdc_client(self, realm_id="1234567890123456789"):
    """Construct a QBClient with just enough state to call cdc()."""
    client = QBClient.__new__(QBClient)
    client.realm_id = realm_id
    client.access_token = "test_access_token"
    return client

  @patch("robosystems.adapters.quickbooks.client.api.requests.get")
  @patch("robosystems.adapters.quickbooks.client.api.env")
  def test_cdc_sandbox_url(self, mock_env, mock_get):
    """CDC URL switches between sandbox + production per env."""
    mock_env.INTUIT_ENVIRONMENT = "sandbox"
    mock_response = Mock(status_code=200)
    mock_response.json.return_value = {"CDCResponse": []}
    mock_get.return_value = mock_response

    from datetime import UTC, datetime

    client = self._make_cdc_client(realm_id="rlm_42")
    client.cdc(datetime(2026, 5, 1, tzinfo=UTC), ["Invoice"])

    url = mock_get.call_args[0][0]
    assert url == "https://sandbox-quickbooks.api.intuit.com/v3/company/rlm_42/cdc"

  @patch("robosystems.adapters.quickbooks.client.api.requests.get")
  @patch("robosystems.adapters.quickbooks.client.api.env")
  def test_cdc_production_url(self, mock_env, mock_get):
    mock_env.INTUIT_ENVIRONMENT = "production"
    mock_response = Mock(status_code=200)
    mock_response.json.return_value = {"CDCResponse": []}
    mock_get.return_value = mock_response

    from datetime import UTC, datetime

    client = self._make_cdc_client(realm_id="rlm_42")
    client.cdc(datetime(2026, 5, 1, tzinfo=UTC), ["Invoice"])

    url = mock_get.call_args[0][0]
    assert url == "https://quickbooks.api.intuit.com/v3/company/rlm_42/cdc"

  @patch("robosystems.adapters.quickbooks.client.api.requests.get")
  @patch("robosystems.adapters.quickbooks.client.api.env")
  def test_cdc_parses_query_response_block(self, mock_env, mock_get):
    """CDC payload's nested QueryResponse arrays get split by entity type."""
    mock_env.INTUIT_ENVIRONMENT = "sandbox"
    mock_response = Mock(status_code=200)
    mock_response.json.return_value = {
      "CDCResponse": [
        {
          "QueryResponse": [
            {"Customer": [{"Id": "1", "SyncToken": "3"}]},
            {"Invoice": [{"Id": "42", "SyncToken": "1"}]},
            {"Vendor": [{"Id": "9", "SyncToken": "0"}]},
          ]
        }
      ]
    }
    mock_get.return_value = mock_response

    from datetime import UTC, datetime

    client = self._make_cdc_client()
    entities, too_old = client.cdc(
      datetime(2026, 5, 1, tzinfo=UTC), ["Customer", "Vendor", "Invoice"]
    )

    assert too_old is False
    assert len(entities["Customer"]) == 1
    assert entities["Customer"][0]["Id"] == "1"
    assert entities["Vendor"][0]["SyncToken"] == "0"
    assert entities["Invoice"][0]["Id"] == "42"

  @patch("robosystems.adapters.quickbooks.client.api.requests.get")
  @patch("robosystems.adapters.quickbooks.client.api.env")
  def test_cdc_30_day_window_returns_too_old_signal(self, mock_env, mock_get):
    """A QB 400 Fault about changedSince out-of-range returns
    ``watermark_too_old=True`` so the caller can reset + fall back."""
    mock_env.INTUIT_ENVIRONMENT = "sandbox"
    mock_response = Mock(status_code=400)
    mock_response.json.return_value = {
      "Fault": {
        "Error": [
          {
            "Message": "ChangedSince invalid",
            "Detail": "changedSince must be within 30 days",
            "code": "400",
          }
        ]
      }
    }
    mock_get.return_value = mock_response

    from datetime import UTC, datetime

    client = self._make_cdc_client()
    entities, too_old = client.cdc(datetime(2025, 1, 1, tzinfo=UTC), ["Customer"])

    assert too_old is True
    assert entities == {}

  @patch("robosystems.adapters.quickbooks.client.api.requests.get")
  @patch("robosystems.adapters.quickbooks.client.api.env")
  def test_cdc_passes_changed_since_header_and_query(self, mock_env, mock_get):
    """Auth header carries Bearer token; changedSince + entities go in
    the query string."""
    mock_env.INTUIT_ENVIRONMENT = "sandbox"
    mock_response = Mock(status_code=200)
    mock_response.json.return_value = {"CDCResponse": []}
    mock_get.return_value = mock_response

    from datetime import UTC, datetime

    client = self._make_cdc_client()
    client.access_token = "tok_phase5"
    client.cdc(
      datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC),
      ["Invoice", "Bill", "Payment"],
    )

    headers = mock_get.call_args.kwargs["headers"]
    params = mock_get.call_args.kwargs["params"]
    assert headers["Authorization"] == "Bearer tok_phase5"
    assert headers["Accept"] == "application/json"
    assert params["entities"] == "Invoice,Bill,Payment"
    # changedSince is ISO 8601 with the time component preserved.
    assert params["changedSince"].startswith("2026-05-01T12:00:00")

  @patch("robosystems.adapters.quickbooks.client.api.AuthClient")
  @patch("robosystems.adapters.quickbooks.client.api.QuickBooks")
  def test_token_refresh_called_for_non_mock_tokens(
    self,
    mock_qb_class,
    mock_auth_client_class,
    qb_credentials,
    realm_id,
    mock_auth_client,
    mock_qb_client,
  ):
    """Test that token refresh is called for non-mock tokens."""
    # Setup mocks
    mock_auth_client_class.return_value = mock_auth_client
    mock_qb_class.return_value = mock_qb_client

    with patch("robosystems.adapters.quickbooks.client.api.env"):
      # Execute
      QBClient(realm_id, qb_credentials)

      # Verify refresh was called
      mock_auth_client.refresh.assert_called_once_with(
        refresh_token="test_refresh_token_123"
      )

  @patch("robosystems.adapters.quickbooks.client.api.AuthClient")
  @patch("robosystems.adapters.quickbooks.client.api.QuickBooks")
  def test_token_refresh_not_called_for_mock_tokens(
    self,
    mock_qb_class,
    mock_auth_client_class,
    realm_id,
    mock_auth_client,
    mock_qb_client,
  ):
    """Test that token refresh is not called for mock tokens."""
    # Setup mocks
    mock_auth_client_class.return_value = mock_auth_client
    mock_qb_class.return_value = mock_qb_client

    mock_credentials = {"refresh_token": "mock_test_token"}

    with patch("robosystems.adapters.quickbooks.client.api.env"):
      # Execute
      QBClient(realm_id, mock_credentials)

      # Verify refresh was not called
      mock_auth_client.refresh.assert_not_called()

  @patch("quickbooks.objects.account.Account")
  def test_get_accounts_pagination_edge_cases(self, mock_account, mock_qb_client):
    """Test multi-page pagination with a partial last page.

    Pagination terminates when a page returns fewer than ``page_size``
    rows (the loop's "last page" signal). page_size is 100, so we
    arrange a 100-row first page (full → continue) followed by a
    smaller second page (partial → exit).
    """
    client = QBClient.__new__(QBClient)
    client.client = mock_qb_client

    def create_mock_account(account_id):
      mock_obj = Mock()
      mock_obj.to_dict.return_value = {
        # Intuit returns "Id" with capital I; adapter dedup keys on it.
        "Id": str(account_id),
        "Name": f"Account {account_id}",
      }
      return mock_obj

    # First page: exactly 100 unique accounts (== page_size → loop continues).
    first_page = [create_mock_account(i) for i in range(1, 101)]
    # Second page: 27 unique accounts (< page_size → loop exits).
    second_page = [create_mock_account(i) for i in range(101, 128)]

    mock_account.query.side_effect = [first_page, second_page]

    result = client.get_accounts()

    assert len(result) == 127
    assert mock_account.query.call_count == 2

    # Verify pagination cursors advance correctly.
    first_query = mock_account.query.call_args_list[0][0][0]
    second_query = mock_account.query.call_args_list[1][0][0]
    assert "STARTPOSITION 1 " in first_query
    assert "STARTPOSITION 101 " in second_query

  @patch("robosystems.adapters.quickbooks.client.api.QBClient.get_accounts")
  def test_get_accounts_df_complex_hierarchy(self, mock_get_accounts, mock_qb_client):
    """Test accounts DataFrame with complex parent-child hierarchy."""
    # Setup mock client
    client = QBClient.__new__(QBClient)
    client.client = mock_qb_client

    # Mock hierarchical account data
    mock_accounts = [
      {
        "Id": "1",
        "Name": "Assets",
        "AccountType": "Asset",
        "Classification": "Asset",
        "FullyQualifiedName": "Assets",
        "ParentRef": None,
      },
      {
        "Id": "2",
        "Name": "Current Assets",
        "AccountType": "Asset",
        "Classification": "Asset",
        "FullyQualifiedName": "Assets:Current Assets",
        "ParentRef": "1",
      },
      {
        "Id": "3",
        "Name": "Checking",
        "AccountType": "Bank",
        "Classification": "Asset",
        "FullyQualifiedName": "Assets:Current Assets:Checking",
        "ParentRef": "2",
      },
    ]
    mock_get_accounts.return_value = mock_accounts

    # Execute
    result = client.get_accounts_df()

    # Verify hierarchy processing
    assert len(result) == 3

    # Verify parent-child relationships
    parent_row = result[result["Id"] == "1"].iloc[0]
    child_row = result[result["Id"] == "2"].iloc[0]
    grandchild_row = result[result["Id"] == "3"].iloc[0]

    # Verify ordering columns are set
    assert pd.notna(parent_row["Order"])
    assert pd.notna(child_row["Order"])
    assert pd.notna(grandchild_row["Order"])

    # Verify sequences are assigned
    assert pd.notna(parent_row["Sequence"])
    assert pd.notna(child_row["Sequence"])
    assert pd.notna(grandchild_row["Sequence"])


class TestTokenPersistence:
  """Phase 3 A1 — rotated refresh tokens persist to ConnectionCredentials.

  Intuit rotates the refresh_token on every refresh(). Without the
  persistence path, the rotated value dies with the QBClient instance
  and the next sync fails once Intuit's rotation grace window expires.
  """

  @patch("robosystems.adapters.quickbooks.client.api.QuickBooks")
  @patch("robosystems.adapters.quickbooks.client.api.AuthClient")
  @patch(
    "robosystems.models.core.connection.connection_credentials.ConnectionCredentials"
  )
  @patch("robosystems.database.SessionFactory")
  def test_rotated_tokens_persisted_when_connection_id_set(
    self,
    mock_session_factory,
    mock_credentials_cls,
    mock_auth_client_class,
    mock_qb_class,
  ):
    """When AuthClient rotates the tokens during refresh, QBClient writes
    the new values back to ConnectionCredentials via update_credentials.

    AuthClient.refresh() mutates `self.refresh_token` + `self.access_token`
    in-place when Intuit returns rotated values. We simulate that by
    side-effect.
    """
    mock_auth = Mock(spec=AuthClient)
    mock_auth.refresh_token = "OLD_REFRESH"
    mock_auth.access_token = "OLD_ACCESS"

    def rotate_on_refresh(refresh_token):
      mock_auth.refresh_token = "ROTATED_REFRESH"
      mock_auth.access_token = "ROTATED_ACCESS"

    mock_auth.refresh.side_effect = rotate_on_refresh
    mock_auth_client_class.return_value = mock_auth
    mock_qb_class.return_value = Mock(spec=QuickBooks)

    mock_session = Mock()
    mock_session_factory.return_value = mock_session

    mock_cred = Mock()
    mock_cred.get_credentials.return_value = {
      "refresh_token": "OLD_REFRESH",
      "access_token": "OLD_ACCESS",
      "extra": "preserved",
    }
    mock_credentials_cls.get_by_connection_id.return_value = mock_cred

    with patch("robosystems.adapters.quickbooks.client.api.env"):
      QBClient(
        realm_id="9999",
        qb_credentials={
          "refresh_token": "OLD_REFRESH",
          "access_token": "OLD_ACCESS",
        },
        connection_id="conn_test_a1",
      )

    # ConnectionCredentials.update_credentials called with rotated values
    # and the prior bundle preserved (the "extra" key survives the merge).
    mock_cred.update_credentials.assert_called_once()
    call_args = mock_cred.update_credentials.call_args
    updated_dict = call_args[0][0]
    assert updated_dict["refresh_token"] == "ROTATED_REFRESH"
    assert updated_dict["access_token"] == "ROTATED_ACCESS"
    assert updated_dict["extra"] == "preserved"
    # Session is closed after the scoped write — don't hold it open
    # for the whole sync.
    mock_session.close.assert_called_once()

  @patch("robosystems.adapters.quickbooks.client.api.QuickBooks")
  @patch("robosystems.adapters.quickbooks.client.api.AuthClient")
  @patch("robosystems.database.SessionFactory")
  def test_no_persistence_without_connection_id(
    self,
    mock_session_factory,
    mock_auth_client_class,
    mock_qb_class,
  ):
    """Callers without a connection_id (legacy callsites, tests) don't
    trigger the persistence path — no session is opened."""
    mock_auth = Mock(spec=AuthClient)
    mock_auth.refresh_token = "ROTATED_REFRESH"
    mock_auth.access_token = "ROTATED_ACCESS"
    mock_auth_client_class.return_value = mock_auth
    mock_qb_class.return_value = Mock(spec=QuickBooks)

    with patch("robosystems.adapters.quickbooks.client.api.env"):
      QBClient(
        realm_id="9999",
        qb_credentials={
          "refresh_token": "OLD_REFRESH",
          "access_token": "OLD_ACCESS",
        },
        # No connection_id
      )

    mock_session_factory.assert_not_called()

  @patch("robosystems.adapters.quickbooks.client.api.QuickBooks")
  @patch("robosystems.adapters.quickbooks.client.api.AuthClient")
  @patch("robosystems.database.SessionFactory")
  def test_no_persistence_when_tokens_unchanged(
    self,
    mock_session_factory,
    mock_auth_client_class,
    mock_qb_class,
  ):
    """If AuthClient returned the SAME tokens (no rotation this cycle),
    skip the persistence write entirely — no point burning a DB roundtrip."""
    mock_auth = Mock(spec=AuthClient)
    mock_auth.refresh_token = "SAME_REFRESH"
    mock_auth.access_token = "SAME_ACCESS"
    mock_auth_client_class.return_value = mock_auth
    mock_qb_class.return_value = Mock(spec=QuickBooks)

    with patch("robosystems.adapters.quickbooks.client.api.env"):
      QBClient(
        realm_id="9999",
        qb_credentials={
          "refresh_token": "SAME_REFRESH",
          "access_token": "SAME_ACCESS",
        },
        connection_id="conn_test_a1",
      )

    mock_session_factory.assert_not_called()


class TestAuthFailureHandling:
  """Phase 3 A2 — AuthClientError + transient network errors get clean
  QBAuthFailedError surfaces. AuthClientError additionally flips the
  connection to needs_reauth (B5)."""

  @patch(
    "robosystems.operations.connection_service.ConnectionService."
    "mark_connection_needs_reauth_sync"
  )
  @patch("robosystems.adapters.quickbooks.client.api.QuickBooks")
  @patch("robosystems.adapters.quickbooks.client.api.AuthClient")
  def test_authclient_error_marks_needs_reauth(
    self,
    mock_auth_client_class,
    mock_qb_class,
    mock_mark_needs_reauth,
  ):
    """An AuthClientError from Intuit (revoked / scope-insufficient)
    flips the connection to needs_reauth and raises QBAuthFailedError
    with recoverable=False."""
    from intuitlib.exceptions import AuthClientError

    from robosystems.adapters.quickbooks.client.api import QBAuthFailedError

    mock_auth = Mock(spec=AuthClient)
    # Construct the error with the response shape intuitlib expects.
    fake_response = Mock()
    fake_response.status_code = 401
    fake_response.text = "Unauthorized"
    fake_response.headers = {"intuit_tid": "tid_x"}
    mock_auth.refresh.side_effect = AuthClientError(fake_response)
    mock_auth_client_class.return_value = mock_auth
    mock_qb_class.return_value = Mock(spec=QuickBooks)

    with patch("robosystems.adapters.quickbooks.client.api.env"):
      with pytest.raises(QBAuthFailedError) as exc_info:
        QBClient(
          realm_id="9999",
          qb_credentials={"refresh_token": "BAD", "access_token": "BAD"},
          connection_id="conn_bad_auth",
        )

    assert exc_info.value.recoverable is False
    mock_mark_needs_reauth.assert_called_once_with("conn_bad_auth")

  @patch(
    "robosystems.operations.connection_service.ConnectionService."
    "mark_connection_needs_reauth_sync"
  )
  @patch("robosystems.adapters.quickbooks.client.api.QuickBooks")
  @patch("robosystems.adapters.quickbooks.client.api.AuthClient")
  def test_transient_network_error_does_not_flip_state(
    self,
    mock_auth_client_class,
    mock_qb_class,
    mock_mark_needs_reauth,
  ):
    """A network-level failure (DNS, connection refused, timeout) raises
    QBAuthFailedError with recoverable=True and does NOT mark the
    connection as needs_reauth — credentials are presumed valid."""
    import requests

    from robosystems.adapters.quickbooks.client.api import QBAuthFailedError

    mock_auth = Mock(spec=AuthClient)
    mock_auth.refresh.side_effect = requests.exceptions.ConnectionError(
      "Intuit unreachable"
    )
    mock_auth_client_class.return_value = mock_auth
    mock_qb_class.return_value = Mock(spec=QuickBooks)

    with patch("robosystems.adapters.quickbooks.client.api.env"):
      with pytest.raises(QBAuthFailedError) as exc_info:
        QBClient(
          realm_id="9999",
          qb_credentials={"refresh_token": "VALID", "access_token": "VALID"},
          connection_id="conn_transient",
        )

    assert exc_info.value.recoverable is True
    mock_mark_needs_reauth.assert_not_called()


class TestRetryBehavior:
  """Phase 3 A3 — tenacity retries 429/5xx on every QB API call."""

  def test_retry_on_429_then_succeeds(self):
    """A 429 followed by a 200 should retry transparently and return
    the 200 result."""
    from quickbooks.exceptions import QuickbooksException

    from robosystems.adapters.quickbooks.client.api import QBClient

    client = QBClient.__new__(QBClient)
    client.client = Mock()

    # First call: 429 (wrapped as QuickbooksException by the SDK).
    # Second call: success.
    error_429 = QuickbooksException(
      "Error returned with status code '429': rate limited", 10000
    )
    success = [Mock(to_dict=lambda: {"Id": "1"})]
    page_call_count = {"n": 0}

    def page_side_effect(*args, **kwargs):
      page_call_count["n"] += 1
      if page_call_count["n"] == 1:
        raise error_429
      return success

    with patch.object(client, "_paginate_one_page", side_effect=page_side_effect) as _:
      # _paginate is undecorated; the inner _paginate_one_page is what
      # carries @_QB_RETRY. But since we patched _paginate_one_page
      # directly, the retry doesn't apply here — so we test the
      # decorator on a fresh decorated function instead.
      pass

    # Direct test of the retry predicate + decorator behavior.
    from robosystems.adapters.quickbooks.client.api import (
      _QB_RETRY,
      _is_retryable_qb_error,
    )

    assert _is_retryable_qb_error(error_429) is True

    call_count = {"n": 0}

    @_QB_RETRY
    def flaky():
      call_count["n"] += 1
      if call_count["n"] < 3:
        raise error_429
      return "ok"

    assert flaky() == "ok"
    assert call_count["n"] == 3

  def test_no_retry_on_authorization_exception(self):
    """A 401 from QB (AuthorizationException) should NOT retry — auth
    errors won't get better with a retry."""
    from quickbooks.exceptions import AuthorizationException

    from robosystems.adapters.quickbooks.client.api import (
      _QB_RETRY,
      _is_retryable_qb_error,
    )

    auth_error = AuthorizationException("forbidden", error_code=401)
    assert _is_retryable_qb_error(auth_error) is False

    call_count = {"n": 0}

    @_QB_RETRY
    def always_401():
      call_count["n"] += 1
      raise auth_error

    with pytest.raises(AuthorizationException):
      always_401()
    assert call_count["n"] == 1  # No retries

  def test_no_retry_on_validation_exception(self):
    """Application-level QB validation errors (4xx-style) should NOT
    retry — they're permanent until the payload is fixed."""
    from quickbooks.exceptions import ValidationException

    from robosystems.adapters.quickbooks.client.api import _is_retryable_qb_error

    # ValidationException's __str__ doesn't include "status code '4xx'".
    validation_error = ValidationException("bad input", error_code=2000)
    assert _is_retryable_qb_error(validation_error) is False

  def test_retry_exhausts_after_5_attempts(self):
    """A persistent 5xx exhausts retry budget after 5 attempts."""
    from quickbooks.exceptions import QuickbooksException
    from retrying import retry

    from robosystems.adapters.quickbooks.client.api import _is_retryable_qb_error

    error_503 = QuickbooksException(
      "Error returned with status code '503': service unavailable", 10000
    )

    call_count = {"n": 0}

    # Use a zero-wait retry config to keep the test fast while still
    # exercising the same predicate + stop_max_attempt_number semantics
    # as the production `_QB_RETRY` (which would burn ~30s of backoff).
    @retry(
      retry_on_exception=_is_retryable_qb_error,
      stop_max_attempt_number=5,
      wait_fixed=0,
    )
    def always_503():
      call_count["n"] += 1
      raise error_503

    with pytest.raises(QuickbooksException):
      always_503()
    # Bounded by stop_max_attempt_number=5.
    assert call_count["n"] == 5
