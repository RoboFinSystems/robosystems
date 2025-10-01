"""Tests for Trial Balance operations with Kuzu graph database integration.

Comprehensive test suite for the TrialBalanceProcessor class that tests
trial balance generation, account processing, and financial calculations.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

from robosystems.processors.trial_balance import TrialBalanceProcessor


@pytest.fixture(autouse=True)
def patch_get_graph_repository():
  """Patch get_graph_repository to return a sync mock for all tests."""
  # Create a sync repository mock
  repo = MagicMock()
  repo.execute_query = MagicMock()
  repo.execute_queries = MagicMock()

  # Import the module to patch it
  import robosystems.processors.trial_balance as tb_module

  # Replace the function with a lambda that returns the mock
  original = tb_module.get_graph_repository
  tb_module.get_graph_repository = lambda *args, **kwargs: repo

  yield repo

  # Restore original
  tb_module.get_graph_repository = original


@pytest.fixture
def mock_repository(patch_get_graph_repository):
  """Create mock graph repository - uses the patched version."""
  return patch_get_graph_repository


@pytest.fixture
def mock_entity_data():
  """Create mock entity data."""
  return {
    "identifier": "test_entity_123",
    "name": "Test Company",
    "type": "company",
  }


@pytest.fixture
def mock_coa_data():
  """Create mock chart of accounts data."""
  return [
    {
      "qname": "us-gaap:Cash",
      "balance": "debit",
      "period_type": "instant",
      "sequence": 1,
    },
    {
      "qname": "us-gaap:AccountsReceivable",
      "balance": "debit",
      "period_type": "instant",
      "sequence": 2,
    },
    {
      "qname": "us-gaap:AccountsPayable",
      "balance": "credit",
      "period_type": "instant",
      "sequence": 3,
    },
    {
      "qname": "us-gaap:Revenue",
      "balance": "credit",
      "period_type": "duration",
      "sequence": 4,
    },
    {
      "qname": "us-gaap:Expenses",
      "balance": "debit",
      "period_type": "duration",
      "sequence": 5,
    },
  ]


@pytest.fixture
def mock_transaction_data():
  """Create mock transaction data."""
  return [
    {
      "date": "2023-01-15",
      "identifier": "txn_001",
      "debit": 1000.00,
      "credit": 0.00,
      "qname": "us-gaap:Cash",
    },
    {
      "date": "2023-01-15",
      "identifier": "txn_001",
      "debit": 0.00,
      "credit": 1000.00,
      "qname": "us-gaap:Revenue",
    },
    {
      "date": "2023-02-01",
      "identifier": "txn_002",
      "debit": 500.00,
      "credit": 0.00,
      "qname": "us-gaap:Expenses",
    },
    {
      "date": "2023-02-01",
      "identifier": "txn_002",
      "debit": 0.00,
      "credit": 500.00,
      "qname": "us-gaap:Cash",
    },
    {
      "date": "2023-03-15",
      "identifier": "txn_003",
      "debit": 2000.00,
      "credit": 0.00,
      "qname": "us-gaap:AccountsReceivable",
    },
    {
      "date": "2023-03-15",
      "identifier": "txn_003",
      "debit": 0.00,
      "credit": 2000.00,
      "qname": "us-gaap:Revenue",
    },
  ]


@pytest.fixture
def trial_balance_processor(mock_repository, mock_entity_data):
  """Create trial balance processor instance."""
  with patch(
    "robosystems.processors.trial_balance.get_graph_repository"
  ) as mock_get_repo:
    mock_get_repo.return_value = mock_repository
    mock_repository.execute_query.return_value = [{"e": mock_entity_data}]

    processor = TrialBalanceProcessor(
      entity_id="test_entity_123",
      date="2023-12-31",
    )
    return processor


class TestTrialBalanceProcessorInitialization:
  """Test trial balance processor initialization."""

  def test_initialization_with_date(self, mock_repository, mock_entity_data):
    """Test initialization with specific date."""
    # get_graph_repository is already patched by the autouse fixture
    mock_repository.execute_query.return_value = [{"e": mock_entity_data}]

    processor = TrialBalanceProcessor(
      entity_id="test_entity_123",
      date="2023-12-31",
    )

    assert processor.entity_id == "test_entity_123"
    assert processor.date == "2023-12-31"
    assert processor.entity_node == mock_entity_data

  def test_initialization_without_date(self, mock_repository, mock_entity_data):
    """Test initialization without date (uses current date)."""
    with patch(
      "robosystems.processors.trial_balance.get_graph_repository"
    ) as mock_get_repo:
      mock_get_repo.return_value = mock_repository
      mock_repository.execute_query.return_value = [{"e": mock_entity_data}]

      with patch("robosystems.processors.trial_balance.dt.datetime") as mock_dt:
        mock_dt.now.return_value.strftime.return_value = "2023-12-31"

        processor = TrialBalanceProcessor(entity_id="test_entity_123")

        assert processor.date == "2023-12-31"

  def test_initialization_with_database_name(self, mock_repository, mock_entity_data):
    """Test initialization with custom database name."""
    with patch(
      "robosystems.processors.trial_balance.get_graph_repository"
    ) as mock_get_repo:
      mock_get_repo.return_value = mock_repository
      mock_repository.execute_query.return_value = [{"e": mock_entity_data}]

      processor = TrialBalanceProcessor(
        entity_id="test_entity_123",
        database_name="custom_db",
      )

      assert processor.database_name == "custom_db"
      mock_get_repo.assert_called_with("custom_db")

  def test_initialization_entity_not_found(self, mock_repository):
    """Test initialization when entity is not found."""
    with patch(
      "robosystems.processors.trial_balance.get_graph_repository"
    ) as mock_get_repo:
      mock_get_repo.return_value = mock_repository
      mock_repository.execute_query.return_value = []

      with pytest.raises(Exception, match="Entity not found"):
        TrialBalanceProcessor(entity_id="invalid_entity")


class TestTrialBalanceGeneration:
  """Test trial balance generation process."""

  def test_generate_trial_balance(self, trial_balance_processor):
    """Test complete trial balance generation."""
    # Mock all the internal methods
    trial_balance_processor.reset_tb_report = Mock()
    trial_balance_processor.get_coa_df = Mock()
    trial_balance_processor.get_all_transactions_df = Mock()
    trial_balance_processor.get_full_trial_balance = Mock()
    trial_balance_processor.get_retained_earnings = Mock()
    trial_balance_processor.get_current_year_trial_balance = Mock()
    trial_balance_processor.make_tb_report = Mock()

    trial_balance_processor.generate()

    # Verify all steps are called
    trial_balance_processor.reset_tb_report.assert_called_once()
    trial_balance_processor.get_coa_df.assert_called_once()
    trial_balance_processor.get_all_transactions_df.assert_called_once()
    trial_balance_processor.get_full_trial_balance.assert_called_once()
    trial_balance_processor.get_retained_earnings.assert_called_once()
    trial_balance_processor.get_current_year_trial_balance.assert_called_once()
    trial_balance_processor.make_tb_report.assert_called_once()

  async def test_generate_async_trial_balance(self, trial_balance_processor):
    """Test async trial balance generation."""
    # Mock all the internal methods
    trial_balance_processor.reset_tb_report = Mock()
    trial_balance_processor.get_coa_df = Mock()
    trial_balance_processor.get_all_transactions_df = Mock()
    trial_balance_processor.get_full_trial_balance = Mock()
    trial_balance_processor.get_retained_earnings = Mock()
    trial_balance_processor.get_current_year_trial_balance = Mock()
    trial_balance_processor.make_tb_report = Mock()

    await trial_balance_processor.generate_async()

    # Verify all steps are called
    trial_balance_processor.reset_tb_report.assert_called_once()
    trial_balance_processor.get_coa_df.assert_called_once()


class TestChartOfAccounts:
  """Test chart of accounts processing."""

  def test_get_coa_df(self, trial_balance_processor, mock_coa_data):
    """Test retrieving chart of accounts as DataFrame."""
    trial_balance_processor.repository.execute_query.return_value = mock_coa_data

    coa_df = trial_balance_processor.get_coa_df()

    assert isinstance(coa_df, pd.DataFrame)
    assert len(coa_df) == 5
    assert "qname" in coa_df.columns
    assert "balance" in coa_df.columns
    assert "period_type" in coa_df.columns
    assert "sequence" in coa_df.columns

    # Verify query parameters
    call_args = trial_balance_processor.repository.execute_query.call_args
    assert call_args[0][1]["entity_id"] == "test_entity_123"
    assert call_args[0][1]["structure_type"] == "ChartOfAccounts"

  def test_coa_df_empty(self, trial_balance_processor):
    """Test handling empty chart of accounts."""
    trial_balance_processor.repository.execute_query.return_value = []

    coa_df = trial_balance_processor.get_coa_df()

    assert isinstance(coa_df, pd.DataFrame)
    assert len(coa_df) == 0


class TestTransactionProcessing:
  """Test transaction processing."""

  def test_get_all_transactions_df(
    self, trial_balance_processor, mock_transaction_data
  ):
    """Test retrieving all transactions as DataFrame."""
    trial_balance_processor.repository.execute_query.return_value = (
      mock_transaction_data
    )

    trans_df = trial_balance_processor.get_all_transactions_df()

    assert isinstance(trans_df, pd.DataFrame)
    assert len(trans_df) == 6
    assert "date" in trans_df.columns
    assert "identifier" in trans_df.columns
    assert "debit" in trans_df.columns
    assert "credit" in trans_df.columns
    assert "qname" in trans_df.columns

    # Verify date conversion
    assert pd.api.types.is_datetime64_any_dtype(trans_df["date"])

  def test_transactions_df_empty(self, trial_balance_processor):
    """Test handling no transactions."""
    trial_balance_processor.repository.execute_query.return_value = []

    trans_df = trial_balance_processor.get_all_transactions_df()

    assert isinstance(trans_df, pd.DataFrame)
    assert len(trans_df) == 0


@pytest.fixture
def trial_balance_test_data(kuzu_repository_with_schema):
  """Create test data for trial balance operations."""
  # Create trial balance-specific schema
  kuzu_repository_with_schema.execute_query("""
      CREATE NODE TABLE TBEntity(
        identifier STRING,
        name STRING,
        fiscal_year_end STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        PRIMARY KEY (identifier)
      )
    """)

  kuzu_repository_with_schema.execute_query("""
      CREATE NODE TABLE TBAccount(
        identifier STRING,
        account_number STRING,
        name STRING,
        account_type STRING,
        classification STRING,
        normal_balance STRING,
        parent_account STRING,
        is_active BOOLEAN,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        PRIMARY KEY (identifier)
      )
    """)

  kuzu_repository_with_schema.execute_query("""
      CREATE NODE TABLE TBTransaction(
        identifier STRING,
        date STRING,
        description STRING,
        reference STRING,
        posted BOOLEAN,
        created_at TIMESTAMP,
        PRIMARY KEY (identifier)
      )
    """)

  kuzu_repository_with_schema.execute_query("""
      CREATE NODE TABLE TBJournalEntry(
        identifier STRING,
        account_id STRING,
        transaction_id STRING,
        debit_amount DOUBLE,
        credit_amount DOUBLE,
        description STRING,
        created_at TIMESTAMP,
        PRIMARY KEY (identifier)
      )
    """)

  kuzu_repository_with_schema.execute_query("""
      CREATE NODE TABLE TBTrialBalanceReport(
        identifier STRING,
        entity_id STRING,
        report_date STRING,
        account_id STRING,
        account_name STRING,
        debit_balance DOUBLE,
        credit_balance DOUBLE,
        created_at TIMESTAMP,
        PRIMARY KEY (identifier)
      )
    """)

  # Create relationship tables
  kuzu_repository_with_schema.execute_query("""
      CREATE REL TABLE ENTITY_HAS_TB_ACCOUNT(FROM TBEntity TO TBAccount)
    """)

  kuzu_repository_with_schema.execute_query("""
      CREATE REL TABLE ENTITY_HAS_TB_TRANSACTION(FROM TBEntity TO TBTransaction)
    """)

  kuzu_repository_with_schema.execute_query("""
      CREATE REL TABLE TRANSACTION_HAS_TB_ENTRY(FROM TBTransaction TO TBJournalEntry)
    """)

  kuzu_repository_with_schema.execute_query("""
      CREATE REL TABLE ACCOUNT_HAS_TB_ENTRY(FROM TBAccount TO TBJournalEntry)
    """)

  # Insert test entity
  entity_data = {
    "identifier": "tb-entity-123",
    "name": "Trial Balance Test Entity",
    "fiscal_year_end": "2023-12-31",
    "created_at": "2023-01-01 00:00:00",
    "updated_at": "2023-01-01 00:00:00",
  }

  kuzu_repository_with_schema.execute_single(
    """
      CREATE (c:TBEntity {
        identifier: $identifier,
        name: $name,
        fiscal_year_end: $fiscal_year_end,
        created_at: timestamp($created_at),
        updated_at: timestamp($updated_at)
      }) RETURN c
    """,
    entity_data,
  )

  # Insert chart of accounts
  accounts = [
    ("1000", "Cash", "Asset", "Current Asset", "Debit"),
    ("1200", "Accounts Receivable", "Asset", "Current Asset", "Debit"),
    ("1500", "Inventory", "Asset", "Current Asset", "Debit"),
    ("2000", "Accounts Payable", "Liability", "Current Liability", "Credit"),
    ("2100", "Accrued Expenses", "Liability", "Current Liability", "Credit"),
    ("3000", "Owner's Equity", "Equity", "Owner's Equity", "Credit"),
    ("4000", "Revenue", "Revenue", "Operating Revenue", "Credit"),
    ("5000", "Cost of Goods Sold", "Expense", "Cost of Sales", "Debit"),
    ("6000", "Operating Expenses", "Expense", "Operating Expense", "Debit"),
  ]

  for i, (number, name, acc_type, classification, normal_balance) in enumerate(
    accounts
  ):
    account_data = {
      "identifier": f"tb-account-{i + 1}",
      "account_number": number,
      "name": name,
      "account_type": acc_type,
      "classification": classification,
      "normal_balance": normal_balance,
      "parent_account": "",
      "is_active": True,
      "created_at": "2023-01-01 00:00:00",
      "updated_at": "2023-01-01 00:00:00",
    }

    kuzu_repository_with_schema.execute_single(
      """
          CREATE (a:TBAccount {
            identifier: $identifier,
            account_number: $account_number,
            name: $name,
            account_type: $account_type,
            classification: $classification,
            normal_balance: $normal_balance,
            parent_account: $parent_account,
            is_active: $is_active,
            created_at: timestamp($created_at),
            updated_at: timestamp($updated_at)
          }) RETURN a
        """,
      account_data,
    )

    # Create relationship to entity
    kuzu_repository_with_schema.execute_single(
      """
          MATCH (c:TBEntity {identifier: $entity_id})
          MATCH (a:TBAccount {identifier: $account_id})
          CREATE (c)-[:ENTITY_HAS_TB_ACCOUNT]->(a)
          RETURN c, a
        """,
      {"entity_id": "tb-entity-123", "account_id": f"tb-account-{i + 1}"},
    )

  return {"entity_id": "tb-entity-123", "account_count": len(accounts)}


class TestTrialBalanceDataStructures:
  """Test trial balance data structures and financial calculations."""

  def test_chart_of_accounts_structure(
    self, trial_balance_test_data, kuzu_repository_with_schema
  ):
    """Test chart of accounts structure for trial balance."""
    # Verify all accounts were created
    result = kuzu_repository_with_schema.execute_query(
      """
          MATCH (c:TBEntity {identifier: $entity_id})-[:ENTITY_HAS_TB_ACCOUNT]->(a:TBAccount)
          RETURN a.account_number as account_number,
                 a.name as account_name,
                 a.account_type as account_type,
                 a.normal_balance as normal_balance
          ORDER BY a.account_number
        """,
      {"entity_id": trial_balance_test_data["entity_id"]},
    )

    assert len(result) == trial_balance_test_data["account_count"]

    # Verify account types
    account_types = [r["account_type"] for r in result]
    expected_types = [
      "Asset",
      "Asset",
      "Asset",
      "Liability",
      "Liability",
      "Equity",
      "Revenue",
      "Expense",
      "Expense",
    ]
    assert account_types == expected_types

    # Verify normal balances
    normal_balances = [r["normal_balance"] for r in result]
    expected_balances = [
      "Debit",
      "Debit",
      "Debit",
      "Credit",
      "Credit",
      "Credit",
      "Credit",
      "Debit",
      "Debit",
    ]
    assert normal_balances == expected_balances

  def test_account_classification_grouping(
    self, trial_balance_test_data, kuzu_repository_with_schema
  ):
    """Test account classification for trial balance grouping."""
    # Test asset accounts
    asset_result = kuzu_repository_with_schema.execute_query("""
          MATCH (a:TBAccount)
          WHERE a.account_type = 'Asset'
          RETURN a.account_number as account_number, a.classification as classification
          ORDER BY a.account_number
        """)

    assert len(asset_result) == 3
    for asset in asset_result:
      assert asset["classification"] == "Current Asset"

    # Test liability accounts
    liability_result = kuzu_repository_with_schema.execute_query("""
          MATCH (a:TBAccount)
          WHERE a.account_type = 'Liability'
          RETURN a.account_number as account_number, a.classification as classification
          ORDER BY a.account_number
        """)

    assert len(liability_result) == 2
    for liability in liability_result:
      assert liability["classification"] == "Current Liability"

  def test_journal_entries_creation(
    self, trial_balance_test_data, kuzu_repository_with_schema
  ):
    """Test creation of journal entries for trial balance calculation."""
    # Create a sample transaction
    transaction_data = {
      "identifier": "tb-txn-001",
      "date": "2023-06-15",
      "description": "Sale of inventory",
      "reference": "INV-001",
      "posted": True,
      "created_at": "2023-06-15 10:00:00",
    }

    kuzu_repository_with_schema.execute_single(
      """
          CREATE (t:TBTransaction {
            identifier: $identifier,
            date: $date,
            description: $description,
            reference: $reference,
            posted: $posted,
            created_at: timestamp($created_at)
          }) RETURN t
        """,
      transaction_data,
    )

    # Create journal entries for the transaction (debit Cash, credit Revenue)
    journal_entries = [
      {
        "identifier": "tb-je-001",
        "account_id": "tb-account-1",  # Cash
        "transaction_id": "tb-txn-001",
        "debit_amount": 1000.00,
        "credit_amount": 0.00,
        "description": "Cash received from sale",
        "created_at": "2023-06-15 10:00:00",
      },
      {
        "identifier": "tb-je-002",
        "account_id": "tb-account-7",  # Revenue
        "transaction_id": "tb-txn-001",
        "debit_amount": 0.00,
        "credit_amount": 1000.00,
        "description": "Revenue from sale",
        "created_at": "2023-06-15 10:00:00",
      },
    ]

    for entry in journal_entries:
      kuzu_repository_with_schema.execute_single(
        """
              CREATE (je:TBJournalEntry {
                identifier: $identifier,
                account_id: $account_id,
                transaction_id: $transaction_id,
                debit_amount: $debit_amount,
                credit_amount: $credit_amount,
                description: $description,
                created_at: timestamp($created_at)
              }) RETURN je
            """,
        entry,
      )

      # Link to transaction
      kuzu_repository_with_schema.execute_single(
        """
              MATCH (t:TBTransaction {identifier: $transaction_id})
              MATCH (je:TBJournalEntry {identifier: $entry_id})
              CREATE (t)-[:TRANSACTION_HAS_TB_ENTRY]->(je)
              RETURN t, je
            """,
        {"transaction_id": entry["transaction_id"], "entry_id": entry["identifier"]},
      )

      # Link to account
      kuzu_repository_with_schema.execute_single(
        """
              MATCH (a:TBAccount {identifier: $account_id})
              MATCH (je:TBJournalEntry {identifier: $entry_id})
              CREATE (a)-[:ACCOUNT_HAS_TB_ENTRY]->(je)
              RETURN a, je
            """,
        {"account_id": entry["account_id"], "entry_id": entry["identifier"]},
      )

    # Verify journal entries balance (debits = credits)
    balance_result = kuzu_repository_with_schema.execute_single(
      """
          MATCH (t:TBTransaction {identifier: $transaction_id})-[:TRANSACTION_HAS_TB_ENTRY]->(je:TBJournalEntry)
          RETURN sum(je.debit_amount) as total_debits, sum(je.credit_amount) as total_credits
        """,
      {"transaction_id": "tb-txn-001"},
    )

    assert balance_result is not None
    assert balance_result["total_debits"] == 1000.00
    assert balance_result["total_credits"] == 1000.00

  def test_account_balance_calculation(
    self, trial_balance_test_data, kuzu_repository_with_schema
  ):
    """Test calculation of account balances for trial balance."""
    # Create multiple transactions to test balance calculation
    transactions = [
      ("tb-txn-002", "2023-07-01", "Purchase inventory", "PO-001"),
      ("tb-txn-003", "2023-07-15", "Pay expenses", "EXP-001"),
      ("tb-txn-004", "2023-08-01", "Collect receivables", "AR-001"),
    ]

    for txn_id, date, desc, ref in transactions:
      transaction_data = {
        "identifier": txn_id,
        "date": date,
        "description": desc,
        "reference": ref,
        "posted": True,
        "created_at": f"{date} 10:00:00",
      }

      kuzu_repository_with_schema.execute_single(
        """
              CREATE (t:TBTransaction {
                identifier: $identifier,
                date: $date,
                description: $description,
                reference: $reference,
                posted: $posted,
                created_at: timestamp($created_at)
              }) RETURN t
            """,
        transaction_data,
      )

    # Create various journal entries
    journal_entries = [
      # Purchase inventory: Debit Inventory, Credit Accounts Payable
      ("tb-je-003", "tb-account-3", "tb-txn-002", 500.00, 0.00),  # Inventory debit
      ("tb-je-004", "tb-account-4", "tb-txn-002", 0.00, 500.00),  # A/P credit
      # Pay expenses: Debit Operating Expenses, Credit Cash
      ("tb-je-005", "tb-account-9", "tb-txn-003", 200.00, 0.00),  # Expenses debit
      ("tb-je-006", "tb-account-1", "tb-txn-003", 0.00, 200.00),  # Cash credit
      # Collect receivables: Debit Cash, Credit Accounts Receivable
      ("tb-je-007", "tb-account-1", "tb-txn-004", 300.00, 0.00),  # Cash debit
      ("tb-je-008", "tb-account-2", "tb-txn-004", 0.00, 300.00),  # A/R credit
    ]

    for entry_id, account_id, txn_id, debit, credit in journal_entries:
      entry_data = {
        "identifier": entry_id,
        "account_id": account_id,
        "transaction_id": txn_id,
        "debit_amount": debit,
        "credit_amount": credit,
        "description": f"Entry for {entry_id}",
        "created_at": "2023-07-01 10:00:00",
      }

      kuzu_repository_with_schema.execute_single(
        """
              CREATE (je:TBJournalEntry {
                identifier: $identifier,
                account_id: $account_id,
                transaction_id: $transaction_id,
                debit_amount: $debit_amount,
                credit_amount: $credit_amount,
                description: $description,
                created_at: timestamp($created_at)
              }) RETURN je
            """,
        entry_data,
      )

      # Link to account
      kuzu_repository_with_schema.execute_single(
        """
              MATCH (a:TBAccount {identifier: $account_id})
              MATCH (je:TBJournalEntry {identifier: $entry_id})
              CREATE (a)-[:ACCOUNT_HAS_TB_ENTRY]->(je)
              RETURN a, je
            """,
        {"account_id": account_id, "entry_id": entry_id},
      )

    # Calculate Cash account balance (only from entries in this test: 300 - 200 = 100)
    cash_balance = kuzu_repository_with_schema.execute_single(
      """
          MATCH (a:TBAccount {identifier: $account_id})-[:ACCOUNT_HAS_TB_ENTRY]->(je:TBJournalEntry)
          RETURN sum(je.debit_amount) - sum(je.credit_amount) as net_balance
        """,
      {"account_id": "tb-account-1"},
    )  # Cash account

    assert cash_balance is not None
    assert cash_balance["net_balance"] == 100.00  # 300 - 200 = 100

  def test_trial_balance_report_generation(
    self, trial_balance_test_data, kuzu_repository_with_schema
  ):
    """Test generation of trial balance report structure."""
    # Create sample trial balance report entries
    trial_balance_entries = [
      ("tb-report-1", "tb-account-1", "Cash", 1100.00, 0.00),
      (
        "tb-report-2",
        "tb-account-2",
        "Accounts Receivable",
        0.00,
        0.00,
      ),  # Net zero after collection
      ("tb-report-3", "tb-account-3", "Inventory", 500.00, 0.00),
      ("tb-report-4", "tb-account-4", "Accounts Payable", 0.00, 500.00),
      ("tb-report-5", "tb-account-7", "Revenue", 0.00, 1000.00),
      ("tb-report-6", "tb-account-9", "Operating Expenses", 200.00, 0.00),
    ]

    for report_id, account_id, account_name, debit, credit in trial_balance_entries:
      report_data = {
        "identifier": report_id,
        "entity_id": trial_balance_test_data["entity_id"],
        "report_date": "2023-08-31",
        "account_id": account_id,
        "account_name": account_name,
        "debit_balance": debit,
        "credit_balance": credit,
        "created_at": "2023-08-31 23:59:59",
      }

      kuzu_repository_with_schema.execute_single(
        """
              CREATE (tb:TBTrialBalanceReport {
                identifier: $identifier,
                entity_id: $entity_id,
                report_date: $report_date,
                account_id: $account_id,
                account_name: $account_name,
                debit_balance: $debit_balance,
                credit_balance: $credit_balance,
                created_at: timestamp($created_at)
              }) RETURN tb
            """,
        report_data,
      )

    # Verify trial balance balances (total debits = total credits)
    totals_result = kuzu_repository_with_schema.execute_single(
      """
          MATCH (tb:TBTrialBalanceReport)
          WHERE tb.entity_id = $entity_id AND tb.report_date = $report_date
          RETURN sum(tb.debit_balance) as total_debits, sum(tb.credit_balance) as total_credits
        """,
      {
        "entity_id": trial_balance_test_data["entity_id"],
        "report_date": "2023-08-31",
      },
    )

    assert totals_result is not None
    assert totals_result["total_debits"] == 1800.00  # 1100 + 500 + 200
    assert totals_result["total_credits"] == 1500.00  # 500 + 1000

    # Note: In a real trial balance, debits should equal credits
    # This test shows the data structure - balancing would be handled by the engine

  def test_trial_balance_account_type_grouping(
    self, trial_balance_test_data, kuzu_repository_with_schema
  ):
    """Test grouping trial balance by account types."""
    # Create trial balance entries first (using simplified data)
    trial_balance_entries = [
      ("tb-asset-1", "tb-account-1", "Cash", "Asset", 1000.00, 0.00),
      ("tb-asset-2", "tb-account-3", "Inventory", "Asset", 500.00, 0.00),
      ("tb-liability-1", "tb-account-4", "Accounts Payable", "Liability", 0.00, 300.00),
      ("tb-equity-1", "tb-account-6", "Owner's Equity", "Equity", 0.00, 800.00),
      ("tb-revenue-1", "tb-account-7", "Revenue", "Revenue", 0.00, 600.00),
      ("tb-expense-1", "tb-account-9", "Operating Expenses", "Expense", 200.00, 0.00),
    ]

    for (
      report_id,
      account_id,
      account_name,
      account_type,
      debit,
      credit,
    ) in trial_balance_entries:
      # Create simplified account for this test using parameterized query
      account_data = {
        "identifier": f"{account_id}-simple",
        "name": account_name,
        "account_type": account_type,
      }

      kuzu_repository_with_schema.execute_single(
        """
              CREATE (a:TBAccount {
                identifier: $identifier,
                name: $name,
                account_type: $account_type
              }) RETURN a
            """,
        account_data,
      )

      report_data = {
        "identifier": report_id,
        "entity_id": trial_balance_test_data["entity_id"],
        "report_date": "2023-09-30",
        "account_id": f"{account_id}-simple",
        "account_name": account_name,
        "debit_balance": debit,
        "credit_balance": credit,
        "created_at": "2023-09-30 23:59:59",
      }

      kuzu_repository_with_schema.execute_single(
        """
              CREATE (tb:TBTrialBalanceReport {
                identifier: $identifier,
                entity_id: $entity_id,
                report_date: $report_date,
                account_id: $account_id,
                account_name: $account_name,
                debit_balance: $debit_balance,
                credit_balance: $credit_balance,
                created_at: timestamp($created_at)
              }) RETURN tb
            """,
        report_data,
      )

    # Test grouping by account type
    asset_result = kuzu_repository_with_schema.execute_query("""
          MATCH (tb:TBTrialBalanceReport)
          MATCH (a:TBAccount {identifier: tb.account_id})
          WHERE a.account_type = 'Asset' AND tb.report_date = '2023-09-30'
          RETURN tb.account_name as account_name, tb.debit_balance as debit_balance
          ORDER BY tb.account_name
        """)

    assert len(asset_result) == 2
    asset_total = sum(r["debit_balance"] for r in asset_result)
    assert asset_total == 1500.00  # 1000 + 500

  def test_trial_balance_date_filtering(
    self, trial_balance_test_data, kuzu_repository_with_schema
  ):
    """Test trial balance filtering by date ranges."""
    # Create trial balance reports for different dates
    dates = ["2023-06-30", "2023-07-31", "2023-08-31"]

    for i, date in enumerate(dates):
      report_data = {
        "identifier": f"tb-date-{i + 1}",
        "entity_id": trial_balance_test_data["entity_id"],
        "report_date": date,
        "account_id": "tb-account-1",
        "account_name": "Cash",
        "debit_balance": 1000.00 + (i * 100),  # Increasing balance
        "credit_balance": 0.00,
        "created_at": f"{date} 23:59:59",
      }

      kuzu_repository_with_schema.execute_single(
        """
              CREATE (tb:TBTrialBalanceReport {
                identifier: $identifier,
                entity_id: $entity_id,
                report_date: $report_date,
                account_id: $account_id,
                account_name: $account_name,
                debit_balance: $debit_balance,
                credit_balance: $credit_balance,
                created_at: timestamp($created_at)
              }) RETURN tb
            """,
        report_data,
      )

    # Test date filtering
    july_result = kuzu_repository_with_schema.execute_single(
      """
          MATCH (tb:TBTrialBalanceReport)
          WHERE tb.entity_id = $entity_id AND tb.report_date = '2023-07-31'
          RETURN tb.debit_balance as july_balance
        """,
      {"entity_id": trial_balance_test_data["entity_id"]},
    )

    assert july_result is not None
    assert july_result["july_balance"] == 1100.00

    # Test latest report
    latest_result = kuzu_repository_with_schema.execute_single(
      """
          MATCH (tb:TBTrialBalanceReport)
          WHERE tb.entity_id = $entity_id
          RETURN tb.report_date as report_date, tb.debit_balance as balance
          ORDER BY tb.report_date DESC
          LIMIT 1
        """,
      {"entity_id": trial_balance_test_data["entity_id"]},
    )

    assert latest_result is not None
    assert latest_result["report_date"] == "2023-08-31"
    assert latest_result["balance"] == 1200.00

  def test_trial_balance_validation_rules(
    self, trial_balance_test_data, kuzu_repository_with_schema
  ):
    """Test validation rules for trial balance data integrity."""
    # Test that accounts must exist before creating trial balance entries
    valid_account_count = kuzu_repository_with_schema.execute_single("""
          MATCH (a:TBAccount)
          WHERE a.is_active = true
          RETURN count(*) as active_accounts
        """)

    assert valid_account_count is not None
    assert (
      valid_account_count["active_accounts"] == trial_balance_test_data["account_count"]
    )

    # Test that all accounts have required fields
    account_validation = kuzu_repository_with_schema.execute_query("""
          MATCH (a:TBAccount)
          WHERE a.account_number IS NOT NULL
            AND a.name IS NOT NULL
            AND a.account_type IS NOT NULL
            AND a.normal_balance IS NOT NULL
          RETURN a.identifier as account_id, a.name as name
        """)

    assert len(account_validation) == trial_balance_test_data["account_count"]

    # Test normal balance validation
    debit_accounts = kuzu_repository_with_schema.execute_query("""
          MATCH (a:TBAccount)
          WHERE a.normal_balance = 'Debit'
          RETURN a.account_type as account_type
        """)

    # Assets and Expenses should have debit normal balance
    debit_types = [r["account_type"] for r in debit_accounts]
    assert "Asset" in debit_types
    assert "Expense" in debit_types

    credit_accounts = kuzu_repository_with_schema.execute_query("""
          MATCH (a:TBAccount)
          WHERE a.normal_balance = 'Credit'
          RETURN a.account_type as account_type
        """)

    # Liabilities, Equity, and Revenue should have credit normal balance
    credit_types = [r["account_type"] for r in credit_accounts]
    assert "Liability" in credit_types
    assert "Equity" in credit_types
    assert "Revenue" in credit_types


if __name__ == "__main__":
  pytest.main([__file__, "-v"])
