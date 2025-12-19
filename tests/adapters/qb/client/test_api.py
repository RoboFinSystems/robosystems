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
        entity_id=realm_id,
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

      # Verify access token is None
      assert client.access_token is None

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
    """Test successful account retrieval."""
    # Setup mock client
    client = QBClient.__new__(QBClient)
    client.client = mock_qb_client

    # Mock account count and filtering
    mock_account.count.return_value = 50  # 50 total accounts

    # Mock account objects
    mock_account1 = Mock()
    mock_account1.to_dict.return_value = {"id": "1", "name": "Account 1"}
    mock_account2 = Mock()
    mock_account2.to_dict.return_value = {"id": "2", "name": "Account 2"}

    # Mock filter results - simulate pagination
    mock_account.filter.side_effect = [
      [mock_account1, mock_account2],  # First batch (items 0-24)
      [],  # Second batch (items 25-49, empty in this test)
    ]

    # Execute
    result = client.get_accounts()

    # Verify
    expected_result = [
      {"id": "1", "name": "Account 1"},
      {"id": "2", "name": "Account 2"},
    ]
    assert result == expected_result

    # Verify correct pagination calls
    # For 50 items with page size 25, we expect 2 calls: positions 0 and 25
    assert mock_account.filter.call_count == 2

  @patch("quickbooks.objects.account.Account")
  def test_get_accounts_no_duplicates(self, mock_account, mock_qb_client):
    """Test that duplicate accounts are filtered out."""
    # Setup mock client
    client = QBClient.__new__(QBClient)
    client.client = mock_qb_client

    mock_account.count.return_value = 10

    # Mock account objects - same account returned twice
    mock_account_obj = Mock()
    mock_account_obj.to_dict.return_value = {"id": "1", "name": "Account 1"}

    mock_account.filter.return_value = [mock_account_obj, mock_account_obj]

    # Execute
    result = client.get_accounts()

    # Verify only one instance returned
    assert len(result) == 1
    assert result == [{"id": "1", "name": "Account 1"}]

  @patch("quickbooks.objects.account.Account")
  def test_get_accounts_zero_count(self, mock_account, mock_qb_client):
    """Test account retrieval when count is zero."""
    # Setup mock client
    client = QBClient.__new__(QBClient)
    client.client = mock_qb_client

    mock_account.count.return_value = 0

    # Execute
    result = client.get_accounts()

    # Verify
    assert result == []
    mock_account.filter.assert_not_called()

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

    mock_journal_entry.count.return_value = 30

    # Mock journal entry objects
    mock_entry1 = Mock()
    mock_entry1.to_dict.return_value = {"id": "1", "amount": 100.00}
    mock_entry2 = Mock()
    mock_entry2.to_dict.return_value = {"id": "2", "amount": 200.00}

    # Mock filter results
    mock_journal_entry.filter.side_effect = [
      [mock_entry1, mock_entry2],  # First batch
      [],  # Second batch (empty)
    ]

    # Execute
    result = client.get_journal_entries()

    # Verify
    expected_result = [{"id": "1", "amount": 100.00}, {"id": "2", "amount": 200.00}]
    assert result == expected_result

  @patch("quickbooks.objects.journalentry.JournalEntry")
  def test_get_journal_entries_zero_count(self, mock_journal_entry, mock_qb_client):
    """Test journal entries retrieval when count is zero."""
    # Setup mock client
    client = QBClient.__new__(QBClient)
    client.client = mock_qb_client

    mock_journal_entry.count.return_value = 0

    # Execute
    result = client.get_journal_entries()

    # Verify
    assert result == []
    mock_journal_entry.filter.assert_not_called()

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
    """Test account pagination edge cases."""
    # Setup mock client
    client = QBClient.__new__(QBClient)
    client.client = mock_qb_client

    # Test with count that doesn't divide evenly by 25
    mock_account.count.return_value = 27

    # Create unique account objects to avoid deduplication
    def create_mock_account(account_id):
      mock_obj = Mock()
      mock_obj.to_dict.return_value = {
        "id": str(account_id),
        "name": f"Account {account_id}",
      }
      return mock_obj

    # Create 25 unique accounts for first batch
    first_batch = [create_mock_account(i) for i in range(1, 26)]
    # Create 2 unique accounts for second batch
    second_batch = [create_mock_account(i) for i in range(26, 28)]

    # Mock filter to return accounts for first two calls
    mock_account.filter.side_effect = [
      first_batch,  # First batch: 25 unique accounts
      second_batch,  # Second batch: 2 unique accounts
    ]

    # Execute
    result = client.get_accounts()

    # Verify
    assert len(result) == 27  # 25 + 2 unique accounts
    assert mock_account.filter.call_count == 2  # Two pagination calls for 27 items

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
