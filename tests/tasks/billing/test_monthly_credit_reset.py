"""Tests for monthly credit reset task."""

from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from decimal import Decimal

from robosystems.tasks.billing.monthly_credit_reset import (
  monthly_credit_reset,
  generate_monthly_usage_report,
  get_graphs_with_negative_balance,
  process_overage_invoice,
  cleanup_old_transactions,
)
from robosystems.models.iam.graph_credits import CreditTransactionType


class TestMonthlyCreditResetTask:
  """Test cases for monthly credit reset Celery task."""

  @patch("robosystems.tasks.billing.monthly_credit_reset.cleanup_old_transactions")
  @patch("robosystems.tasks.billing.monthly_credit_reset.process_overage_invoice")
  @patch(
    "robosystems.tasks.billing.monthly_credit_reset.get_graphs_with_negative_balance"
  )
  @patch("robosystems.tasks.billing.monthly_credit_reset.CreditService")
  @patch("robosystems.tasks.billing.monthly_credit_reset.get_celery_db_session")
  def test_successful_reset_no_overages(
    self,
    mock_get_session,
    mock_credit_service,
    mock_get_negative,
    mock_process_invoice,
    mock_cleanup,
  ):
    """Test successful monthly reset with no negative balances."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_get_negative.return_value = []

    mock_service = MagicMock()
    mock_credit_service.return_value = mock_service
    mock_service.bulk_allocate_monthly_credits.return_value = {
      "allocated_graphs": 10,
      "total_credits_allocated": 50000,
      "allocation_details": [],
    }

    mock_cleanup.return_value = {
      "deleted_transactions": 100,
      "total_processed": 100,
      "cutoff_date": "2024-11-23T00:00:00+00:00",
      "months_kept": 12,
    }

    result = monthly_credit_reset()  # type: ignore[call-arg]

    assert result["status"] == "success"
    assert result["graphs_processed"] == 10
    assert result["graphs_with_overage"] == 0
    assert result["total_overage_credits"] == 0.0
    assert result["processing_errors"] == 0
    assert len(result["overage_invoices"]) == 0

    mock_get_negative.assert_called_once()
    mock_process_invoice.assert_not_called()
    mock_service.bulk_allocate_monthly_credits.assert_called_once()
    mock_cleanup.assert_called_once_with(mock_session, months_to_keep=12)
    mock_session.close.assert_called_once()

  @patch("robosystems.tasks.billing.monthly_credit_reset.cleanup_old_transactions")
  @patch("robosystems.tasks.billing.monthly_credit_reset.process_overage_invoice")
  @patch(
    "robosystems.tasks.billing.monthly_credit_reset.get_graphs_with_negative_balance"
  )
  @patch("robosystems.tasks.billing.monthly_credit_reset.CreditService")
  @patch("robosystems.tasks.billing.monthly_credit_reset.get_celery_db_session")
  def test_successful_reset_with_overages(
    self,
    mock_get_session,
    mock_credit_service,
    mock_get_negative,
    mock_process_invoice,
    mock_cleanup,
  ):
    """Test successful monthly reset with negative balances (overages)."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_negative_graphs = [
      {
        "graph_id": "graph1",
        "user_id": "user1",
        "billing_admin_id": "admin1",
        "negative_balance": -50.0,
        "monthly_allocation": 1000.0,
        "graph_tier": "ladybug-standard",
        "overage_amount": 50.0,
      },
      {
        "graph_id": "graph2",
        "user_id": "user2",
        "billing_admin_id": "admin2",
        "negative_balance": -150.0,
        "monthly_allocation": 2000.0,
        "graph_tier": "ladybug-large",
        "overage_amount": 150.0,
      },
    ]
    mock_get_negative.return_value = mock_negative_graphs

    mock_invoices = [
      {
        "graph_id": "graph1",
        "user_id": "user1",
        "billing_admin_id": "admin1",
        "overage_credits": 50.0,
        "amount_usd": 0.25,
        "invoice_date": "2025-11-23T00:00:00+00:00",
        "status": "pending_payment",
      },
      {
        "graph_id": "graph2",
        "user_id": "user2",
        "billing_admin_id": "admin2",
        "overage_credits": 150.0,
        "amount_usd": 0.75,
        "invoice_date": "2025-11-23T00:00:00+00:00",
        "status": "pending_payment",
      },
    ]
    mock_process_invoice.side_effect = mock_invoices

    mock_service = MagicMock()
    mock_credit_service.return_value = mock_service
    mock_service.bulk_allocate_monthly_credits.return_value = {
      "allocated_graphs": 10,
      "total_credits_allocated": 50000,
      "allocation_details": [],
    }

    mock_cleanup.return_value = {
      "deleted_transactions": 100,
      "total_processed": 100,
    }

    result = monthly_credit_reset()  # type: ignore[call-arg]

    assert result["status"] == "success"
    assert result["graphs_processed"] == 10
    assert result["graphs_with_overage"] == 2
    assert result["total_overage_credits"] == 200.0
    assert result["processing_errors"] == 0
    assert len(result["overage_invoices"]) == 2

    assert mock_process_invoice.call_count == 2
    mock_session.close.assert_called_once()

  @patch("robosystems.tasks.billing.monthly_credit_reset.cleanup_old_transactions")
  @patch("robosystems.tasks.billing.monthly_credit_reset.process_overage_invoice")
  @patch(
    "robosystems.tasks.billing.monthly_credit_reset.get_graphs_with_negative_balance"
  )
  @patch("robosystems.tasks.billing.monthly_credit_reset.CreditService")
  @patch("robosystems.tasks.billing.monthly_credit_reset.get_celery_db_session")
  def test_partial_overage_processing_errors(
    self,
    mock_get_session,
    mock_credit_service,
    mock_get_negative,
    mock_process_invoice,
    mock_cleanup,
  ):
    """Test handling of errors during overage invoice processing."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_negative_graphs = [
      {
        "graph_id": "graph1",
        "user_id": "user1",
        "billing_admin_id": "admin1",
        "negative_balance": -50.0,
        "monthly_allocation": 1000.0,
        "graph_tier": "ladybug-standard",
        "overage_amount": 50.0,
      },
      {
        "graph_id": "graph2",
        "user_id": "user2",
        "billing_admin_id": "admin2",
        "negative_balance": -150.0,
        "monthly_allocation": 2000.0,
        "graph_tier": "ladybug-large",
        "overage_amount": 150.0,
      },
    ]
    mock_get_negative.return_value = mock_negative_graphs

    def invoice_side_effect(session, graph_info):
      if graph_info["graph_id"] == "graph1":
        return {
          "graph_id": "graph1",
          "overage_credits": 50.0,
          "amount_usd": 0.25,
        }
      else:
        raise RuntimeError("Invoice generation failed")

    mock_process_invoice.side_effect = invoice_side_effect

    mock_service = MagicMock()
    mock_credit_service.return_value = mock_service
    mock_service.bulk_allocate_monthly_credits.return_value = {
      "allocated_graphs": 10,
      "total_credits_allocated": 50000,
    }

    mock_cleanup.return_value = {"deleted_transactions": 0}

    result = monthly_credit_reset()  # type: ignore[call-arg]

    assert result["status"] == "success"
    assert result["graphs_processed"] == 10
    assert result["graphs_with_overage"] == 2
    assert result["processing_errors"] == 1
    assert len(result["overage_invoices"]) == 1

    assert mock_process_invoice.call_count == 2
    mock_session.close.assert_called_once()

  @patch(
    "robosystems.tasks.billing.monthly_credit_reset.get_graphs_with_negative_balance"
  )
  @patch("robosystems.tasks.billing.monthly_credit_reset.get_celery_db_session")
  def test_critical_failure(
    self,
    mock_get_session,
    mock_get_negative,
  ):
    """Test handling of critical failures."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_get_negative.side_effect = RuntimeError("Database connection failed")

    result = monthly_credit_reset()  # type: ignore[call-arg]

    assert result["status"] == "error"
    assert "Database connection failed" in result["error"]
    assert "timestamp" in result
    mock_session.close.assert_called_once()

  @patch("robosystems.tasks.billing.monthly_credit_reset.cleanup_old_transactions")
  @patch("robosystems.tasks.billing.monthly_credit_reset.process_overage_invoice")
  @patch(
    "robosystems.tasks.billing.monthly_credit_reset.get_graphs_with_negative_balance"
  )
  @patch("robosystems.tasks.billing.monthly_credit_reset.CreditService")
  @patch("robosystems.tasks.billing.monthly_credit_reset.get_celery_db_session")
  def test_allocation_includes_cleanup_result(
    self,
    mock_get_session,
    mock_credit_service,
    mock_get_negative,
    mock_process_invoice,
    mock_cleanup,
  ):
    """Test that cleanup result is included in response."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_get_negative.return_value = []

    mock_service = MagicMock()
    mock_credit_service.return_value = mock_service
    mock_service.bulk_allocate_monthly_credits.return_value = {
      "allocated_graphs": 5,
      "total_credits_allocated": 25000,
    }

    mock_cleanup.return_value = {
      "deleted_transactions": 500,
      "total_processed": 500,
      "cutoff_date": "2024-11-23T00:00:00+00:00",
      "months_kept": 12,
    }

    result = monthly_credit_reset()  # type: ignore[call-arg]

    assert result["status"] == "success"
    assert result["cleanup_result"]["deleted_transactions"] == 500
    assert result["cleanup_result"]["months_kept"] == 12


class TestGenerateMonthlyUsageReportTask:
  """Test cases for generate monthly usage report Celery task."""

  @patch("robosystems.tasks.billing.monthly_credit_reset.get_celery_db_session")
  def test_successful_report_with_specified_date(self, mock_get_session):
    """Test successful report generation with specified year and month."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_credits1 = MagicMock()
    mock_credits1.id = 1
    mock_credits1.graph_id = "graph1"
    mock_credits1.user_id = "user1"
    mock_credits1.graph_tier = "ladybug-standard"
    mock_credits1.monthly_allocation = 1000
    mock_credits1.current_balance = 500

    mock_credits2 = MagicMock()
    mock_credits2.id = 2
    mock_credits2.graph_id = "graph2"
    mock_credits2.user_id = "user2"
    mock_credits2.graph_tier = "ladybug-large"
    mock_credits2.monthly_allocation = 2000
    mock_credits2.current_balance = -50

    mock_session.query.return_value.all.return_value = [mock_credits1, mock_credits2]

    mock_transaction1 = MagicMock()
    mock_transaction1.transaction_type = CreditTransactionType.CONSUMPTION
    mock_transaction1.amount = Decimal("-100")

    mock_transaction2 = MagicMock()
    mock_transaction2.transaction_type = CreditTransactionType.ALLOCATION
    mock_transaction2.amount = Decimal("1000")

    mock_transaction3 = MagicMock()
    mock_transaction3.transaction_type = CreditTransactionType.CONSUMPTION
    mock_transaction3.amount = Decimal("-500")

    mock_transaction4 = MagicMock()
    mock_transaction4.transaction_type = CreditTransactionType.ALLOCATION
    mock_transaction4.amount = Decimal("2000")

    def query_side_effect(*args):
      mock_query = MagicMock()
      if mock_session.query.call_count <= 1:
        mock_query.all.return_value = [mock_credits1, mock_credits2]
      elif mock_session.query.call_count == 2:
        mock_query.filter.return_value.all.return_value = [
          mock_transaction1,
          mock_transaction2,
        ]
      else:
        mock_query.filter.return_value.all.return_value = [
          mock_transaction3,
          mock_transaction4,
        ]
      return mock_query

    mock_session.query.side_effect = query_side_effect

    result = generate_monthly_usage_report(year=2025, month=1)  # type: ignore[call-arg]

    assert result["status"] == "success"
    assert result["year"] == 2025
    assert result["month"] == 1
    assert result["summary"]["total_graphs"] == 2
    assert result["summary"]["graphs_with_overage"] == 1
    assert len(result["graph_reports"]) == 2

    mock_session.close.assert_called_once()

  @patch("robosystems.tasks.billing.monthly_credit_reset.get_celery_db_session")
  def test_report_defaults_to_last_month(self, mock_get_session):
    """Test that report defaults to last month if not specified."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_session.query.return_value.all.return_value = []

    result = generate_monthly_usage_report()  # type: ignore[call-arg]

    assert result["status"] == "success"
    assert "year" in result
    assert "month" in result

    last_month_date = datetime.now(timezone.utc).replace(day=1)
    last_month_date = last_month_date.replace(day=1)
    expected_month = last_month_date.month - 1 if last_month_date.month > 1 else 12

    assert result["month"] == expected_month
    mock_session.close.assert_called_once()

  @patch("robosystems.tasks.billing.monthly_credit_reset.get_celery_db_session")
  def test_report_no_graphs(self, mock_get_session):
    """Test report generation when no graphs exist."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_session.query.return_value.all.return_value = []

    result = generate_monthly_usage_report(year=2025, month=1)  # type: ignore[call-arg]

    assert result["status"] == "success"
    assert result["summary"]["total_graphs"] == 0
    assert result["summary"]["total_credits_consumed"] == 0.0
    assert result["summary"]["graphs_with_overage"] == 0
    assert len(result["graph_reports"]) == 0

  @patch("robosystems.tasks.billing.monthly_credit_reset.get_celery_db_session")
  def test_report_database_error(self, mock_get_session):
    """Test handling of database errors during report generation."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_session.query.side_effect = RuntimeError("Database connection lost")

    result = generate_monthly_usage_report(year=2025, month=1)  # type: ignore[call-arg]

    assert result["status"] == "error"
    assert "Database connection lost" in result["error"]
    assert result["year"] == 2025
    assert result["month"] == 1
    mock_session.close.assert_called_once()

  @patch("robosystems.tasks.billing.monthly_credit_reset.get_celery_db_session")
  def test_report_december_month_end_calculation(self, mock_get_session):
    """Test correct month end calculation for December."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_session.query.return_value.all.return_value = []

    result = generate_monthly_usage_report(year=2025, month=12)  # type: ignore[call-arg]

    assert result["status"] == "success"
    assert result["year"] == 2025
    assert result["month"] == 12


class TestHelperFunctions:
  """Test cases for helper functions."""

  def test_get_graphs_with_negative_balance(self):
    """Test retrieving graphs with negative balances."""
    mock_session = MagicMock()

    mock_row1 = MagicMock()
    mock_row1.graph_id = "graph1"
    mock_row1.user_id = "user1"
    mock_row1.billing_admin_id = "admin1"
    mock_row1.current_balance = -50.0
    mock_row1.monthly_allocation = 1000.0
    mock_row1.graph_tier = "ladybug-standard"

    mock_row2 = MagicMock()
    mock_row2.graph_id = "graph2"
    mock_row2.user_id = "user2"
    mock_row2.billing_admin_id = "admin2"
    mock_row2.current_balance = -150.0
    mock_row2.monthly_allocation = 2000.0
    mock_row2.graph_tier = "ladybug-large"

    mock_query = MagicMock()
    mock_query.filter.return_value.all.return_value = [mock_row1, mock_row2]
    mock_session.query.return_value = mock_query

    result = get_graphs_with_negative_balance(mock_session)

    assert len(result) == 2
    assert result[0]["graph_id"] == "graph1"
    assert result[0]["negative_balance"] == -50.0
    assert result[0]["overage_amount"] == 50.0
    assert result[1]["graph_id"] == "graph2"
    assert result[1]["negative_balance"] == -150.0
    assert result[1]["overage_amount"] == 150.0

  def test_get_graphs_with_negative_balance_none_found(self):
    """Test when no graphs have negative balances."""
    mock_session = MagicMock()

    mock_query = MagicMock()
    mock_query.filter.return_value.all.return_value = []
    mock_session.query.return_value = mock_query

    result = get_graphs_with_negative_balance(mock_session)

    assert len(result) == 0

  @patch("robosystems.tasks.billing.monthly_credit_reset.GraphCreditTransaction")
  @patch("robosystems.tasks.billing.monthly_credit_reset.GraphCredits")
  def test_process_overage_invoice(self, mock_graph_credits, mock_transaction):
    """Test processing overage invoice."""
    mock_session = MagicMock()

    graph_info = {
      "graph_id": "graph1",
      "user_id": "user1",
      "billing_admin_id": "admin1",
      "negative_balance": -100.0,
      "monthly_allocation": 1000.0,
      "graph_tier": "ladybug-standard",
    }

    mock_credits_record = MagicMock()
    mock_credits_record.id = 123
    mock_graph_credits.get_by_graph_id.return_value = mock_credits_record

    result = process_overage_invoice(mock_session, graph_info)

    assert result["graph_id"] == "graph1"
    assert result["user_id"] == "user1"
    assert result["billing_admin_id"] == "admin1"
    assert result["overage_credits"] == 100.0
    assert result["amount_usd"] == 0.5
    assert result["status"] == "pending_payment"

    mock_graph_credits.get_by_graph_id.assert_called_once_with("graph1", mock_session)
    mock_transaction.create_transaction.assert_called_once()
    mock_session.commit.assert_called_once()

  @patch("robosystems.tasks.billing.monthly_credit_reset.GraphCreditTransaction")
  @patch("robosystems.tasks.billing.monthly_credit_reset.GraphCredits")
  def test_process_overage_invoice_no_credits_record(
    self, mock_graph_credits, mock_transaction
  ):
    """Test processing overage invoice when credits record doesn't exist."""
    mock_session = MagicMock()

    graph_info = {
      "graph_id": "graph1",
      "user_id": "user1",
      "billing_admin_id": "admin1",
      "negative_balance": -50.0,
      "monthly_allocation": 1000.0,
      "graph_tier": "ladybug-standard",
    }

    mock_graph_credits.get_by_graph_id.return_value = None

    result = process_overage_invoice(mock_session, graph_info)

    assert result["overage_credits"] == 50.0
    assert result["amount_usd"] == 0.25

    mock_transaction.create_transaction.assert_not_called()
    mock_session.commit.assert_not_called()

  def test_cleanup_old_transactions(self):
    """Test cleanup of old transaction records."""
    mock_session = MagicMock()

    mock_query = MagicMock()
    mock_query.count.return_value = 250
    mock_query.delete.return_value = 250
    mock_session.query.return_value.filter.return_value = mock_query

    result = cleanup_old_transactions(mock_session, months_to_keep=12)

    assert result["deleted_transactions"] == 250
    assert result["total_processed"] == 250
    assert "cutoff_date" in result
    assert result["months_kept"] == 12

    mock_session.commit.assert_called_once()

  def test_cleanup_old_transactions_no_records(self):
    """Test cleanup when no old records exist."""
    mock_session = MagicMock()

    mock_query = MagicMock()
    mock_query.count.return_value = 0
    mock_session.query.return_value.filter.return_value = mock_query

    result = cleanup_old_transactions(mock_session, months_to_keep=12)

    assert result["deleted_transactions"] == 0
    assert result["total_processed"] == 0

    mock_query.delete.assert_not_called()
    mock_session.commit.assert_not_called()

  def test_cleanup_preserves_allocation_transactions(self):
    """Test that cleanup doesn't delete ALLOCATION transactions."""
    mock_session = MagicMock()

    mock_query = MagicMock()
    mock_query.count.return_value = 100
    mock_query.delete.return_value = 100
    mock_session.query.return_value.filter.return_value = mock_query

    result = cleanup_old_transactions(mock_session, months_to_keep=6)

    assert result["deleted_transactions"] == 100
    assert result["months_kept"] == 6
