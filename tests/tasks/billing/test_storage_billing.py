"""Tests for storage billing tasks."""

from unittest.mock import MagicMock, patch
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from robosystems.tasks.billing.storage_billing import (
  daily_storage_billing,
  monthly_storage_summary,
  get_graphs_with_storage_usage,
  calculate_daily_average_storage,
  cleanup_old_storage_records,
)


class TestDailyStorageBillingTask:
  """Test cases for daily storage billing Celery task."""

  @patch("robosystems.tasks.billing.storage_billing.cleanup_old_storage_records")
  @patch("robosystems.tasks.billing.storage_billing.CreditService")
  @patch("robosystems.tasks.billing.storage_billing.calculate_daily_average_storage")
  @patch("robosystems.tasks.billing.storage_billing.get_graphs_with_storage_usage")
  @patch("robosystems.tasks.billing.storage_billing.get_celery_db_session")
  def test_successful_billing(
    self,
    mock_get_session,
    mock_get_graphs,
    mock_calc_storage,
    mock_credit_service,
    mock_cleanup,
  ):
    """Test successful daily storage billing."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_graphs = [
      {
        "graph_id": "graph1",
        "user_id": "user1",
        "graph_tier": "standard",
        "measurement_count": 24,
      },
      {
        "graph_id": "graph2",
        "user_id": "user2",
        "graph_tier": "enterprise",
        "measurement_count": 24,
      },
    ]
    mock_get_graphs.return_value = mock_graphs

    mock_calc_storage.return_value = 2.5

    mock_service = MagicMock()
    mock_credit_service.return_value = mock_service
    mock_service.consume_storage_credits.return_value = {
      "success": True,
      "credits_consumed": 25.0,
      "remaining_balance": 975.0,
      "old_balance": 1000.0,
      "went_negative": False,
    }

    mock_cleanup.return_value = {
      "deleted_records": 100,
      "total_processed": 100,
      "cutoff_date": "2025-01-01T00:00:00+00:00",
    }

    result = daily_storage_billing(target_date="2025-01-15")  # type: ignore[call-arg]

    assert result["status"] == "success"
    assert result["billing_date"] == "2025-01-15"
    assert result["graphs_processed"] == 2
    assert result["total_credits_consumed"] == 50.0
    assert result["negative_balances"] == 0
    assert result["processing_errors"] == 0

    assert mock_get_graphs.call_count == 1
    assert mock_calc_storage.call_count == 2
    assert mock_service.consume_storage_credits.call_count == 2
    mock_session.close.assert_called_once()

  @patch("robosystems.tasks.billing.storage_billing.cleanup_old_storage_records")
  @patch("robosystems.tasks.billing.storage_billing.CreditService")
  @patch("robosystems.tasks.billing.storage_billing.calculate_daily_average_storage")
  @patch("robosystems.tasks.billing.storage_billing.get_graphs_with_storage_usage")
  @patch("robosystems.tasks.billing.storage_billing.get_celery_db_session")
  def test_billing_with_default_date(
    self,
    mock_get_session,
    mock_get_graphs,
    mock_calc_storage,
    mock_credit_service,
    mock_cleanup,
  ):
    """Test billing with default date (yesterday)."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_get_graphs.return_value = []
    mock_cleanup.return_value = {"deleted_records": 0}

    result = daily_storage_billing()  # type: ignore[call-arg]

    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    assert result["billing_date"] == yesterday.isoformat()
    mock_session.close.assert_called_once()

  @patch("robosystems.tasks.billing.storage_billing.get_celery_db_session")
  def test_invalid_date_format(self, mock_get_session):
    """Test handling of invalid date format."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    result = daily_storage_billing(target_date="invalid-date")  # type: ignore[call-arg]

    assert result["status"] == "error"
    assert "Invalid date format" in result["error"]

  @patch("robosystems.tasks.billing.storage_billing.cleanup_old_storage_records")
  @patch("robosystems.tasks.billing.storage_billing.CreditService")
  @patch("robosystems.tasks.billing.storage_billing.calculate_daily_average_storage")
  @patch("robosystems.tasks.billing.storage_billing.get_graphs_with_storage_usage")
  @patch("robosystems.tasks.billing.storage_billing.get_celery_db_session")
  def test_no_graphs_to_process(
    self,
    mock_get_session,
    mock_get_graphs,
    mock_calc_storage,
    mock_credit_service,
    mock_cleanup,
  ):
    """Test billing when no graphs have storage usage."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_get_graphs.return_value = []
    mock_cleanup.return_value = {"deleted_records": 0}

    result = daily_storage_billing(target_date="2025-01-15")  # type: ignore[call-arg]

    assert result["status"] == "success"
    assert result["graphs_processed"] == 0
    assert result["total_credits_consumed"] == 0.0

    mock_calc_storage.assert_not_called()

  @patch("robosystems.tasks.billing.storage_billing.cleanup_old_storage_records")
  @patch("robosystems.tasks.billing.storage_billing.CreditService")
  @patch("robosystems.tasks.billing.storage_billing.calculate_daily_average_storage")
  @patch("robosystems.tasks.billing.storage_billing.get_graphs_with_storage_usage")
  @patch("robosystems.tasks.billing.storage_billing.get_celery_db_session")
  def test_negative_balance_tracking(
    self,
    mock_get_session,
    mock_get_graphs,
    mock_calc_storage,
    mock_credit_service,
    mock_cleanup,
  ):
    """Test tracking of graphs that go negative."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_graphs = [
      {
        "graph_id": "graph1",
        "user_id": "user1",
        "graph_tier": "standard",
        "measurement_count": 24,
      }
    ]
    mock_get_graphs.return_value = mock_graphs

    mock_calc_storage.return_value = 5.0

    mock_service = MagicMock()
    mock_credit_service.return_value = mock_service
    mock_service.consume_storage_credits.return_value = {
      "success": True,
      "credits_consumed": 50.0,
      "remaining_balance": -25.0,
      "old_balance": 25.0,
      "went_negative": True,
    }

    mock_cleanup.return_value = {"deleted_records": 0}

    result = daily_storage_billing(target_date="2025-01-15")  # type: ignore[call-arg]

    assert result["status"] == "success"
    assert result["graphs_processed"] == 1
    assert result["negative_balances"] == 1

  @patch("robosystems.tasks.billing.storage_billing.cleanup_old_storage_records")
  @patch("robosystems.tasks.billing.storage_billing.CreditService")
  @patch("robosystems.tasks.billing.storage_billing.calculate_daily_average_storage")
  @patch("robosystems.tasks.billing.storage_billing.get_graphs_with_storage_usage")
  @patch("robosystems.tasks.billing.storage_billing.get_celery_db_session")
  def test_zero_storage_skipped(
    self,
    mock_get_session,
    mock_get_graphs,
    mock_calc_storage,
    mock_credit_service,
    mock_cleanup,
  ):
    """Test that graphs with zero storage are skipped."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_graphs = [
      {
        "graph_id": "graph1",
        "user_id": "user1",
        "graph_tier": "standard",
        "measurement_count": 24,
      }
    ]
    mock_get_graphs.return_value = mock_graphs

    mock_calc_storage.return_value = 0

    mock_service = MagicMock()
    mock_credit_service.return_value = mock_service

    mock_cleanup.return_value = {"deleted_records": 0}

    result = daily_storage_billing(target_date="2025-01-15")  # type: ignore[call-arg]

    assert result["status"] == "success"
    assert result["graphs_processed"] == 0

    mock_service.consume_storage_credits.assert_not_called()

  @patch("robosystems.tasks.billing.storage_billing.cleanup_old_storage_records")
  @patch("robosystems.tasks.billing.storage_billing.CreditService")
  @patch("robosystems.tasks.billing.storage_billing.calculate_daily_average_storage")
  @patch("robosystems.tasks.billing.storage_billing.get_graphs_with_storage_usage")
  @patch("robosystems.tasks.billing.storage_billing.get_celery_db_session")
  def test_partial_processing_errors(
    self,
    mock_get_session,
    mock_get_graphs,
    mock_calc_storage,
    mock_credit_service,
    mock_cleanup,
  ):
    """Test handling of partial processing errors."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_graphs = [
      {
        "graph_id": "graph1",
        "user_id": "user1",
        "graph_tier": "standard",
        "measurement_count": 24,
      },
      {
        "graph_id": "graph2",
        "user_id": "user2",
        "graph_tier": "enterprise",
        "measurement_count": 24,
      },
    ]
    mock_get_graphs.return_value = mock_graphs

    def calc_side_effect(session, graph_id, year, month, day):
      if graph_id == "graph1":
        return 2.5
      else:
        raise RuntimeError("Calculation failed")

    mock_calc_storage.side_effect = calc_side_effect

    mock_service = MagicMock()
    mock_credit_service.return_value = mock_service
    mock_service.consume_storage_credits.return_value = {
      "success": True,
      "credits_consumed": 25.0,
      "remaining_balance": 975.0,
      "old_balance": 1000.0,
      "went_negative": False,
    }

    mock_cleanup.return_value = {"deleted_records": 0}

    result = daily_storage_billing(target_date="2025-01-15")  # type: ignore[call-arg]

    assert result["status"] == "success"
    assert result["graphs_processed"] == 1
    assert result["processing_errors"] == 1

  @patch("robosystems.tasks.billing.storage_billing.cleanup_old_storage_records")
  @patch("robosystems.tasks.billing.storage_billing.CreditService")
  @patch("robosystems.tasks.billing.storage_billing.calculate_daily_average_storage")
  @patch("robosystems.tasks.billing.storage_billing.get_graphs_with_storage_usage")
  @patch("robosystems.tasks.billing.storage_billing.get_celery_db_session")
  def test_credit_service_failure(
    self,
    mock_get_session,
    mock_get_graphs,
    mock_calc_storage,
    mock_credit_service,
    mock_cleanup,
  ):
    """Test handling of credit service failures."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_graphs = [
      {
        "graph_id": "graph1",
        "user_id": "user1",
        "graph_tier": "standard",
        "measurement_count": 24,
      }
    ]
    mock_get_graphs.return_value = mock_graphs

    mock_calc_storage.return_value = 2.5

    mock_service = MagicMock()
    mock_credit_service.return_value = mock_service
    mock_service.consume_storage_credits.return_value = {
      "success": False,
      "error": "Insufficient credits",
    }

    mock_cleanup.return_value = {"deleted_records": 0}

    result = daily_storage_billing(target_date="2025-01-15")  # type: ignore[call-arg]

    assert result["status"] == "success"
    assert result["graphs_processed"] == 0
    assert result["processing_errors"] == 1

  @patch("robosystems.tasks.billing.storage_billing.get_graphs_with_storage_usage")
  @patch("robosystems.tasks.billing.storage_billing.get_celery_db_session")
  def test_database_error(self, mock_get_session, mock_get_graphs):
    """Test handling of database errors."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_get_graphs.side_effect = RuntimeError("Database error")

    result = daily_storage_billing(target_date="2025-01-15")  # type: ignore[call-arg]

    assert result["status"] == "error"
    assert "Database error" in result["error"]
    mock_session.close.assert_called_once()

  @patch("robosystems.tasks.billing.storage_billing.cleanup_old_storage_records")
  @patch("robosystems.tasks.billing.storage_billing.CreditService")
  @patch("robosystems.tasks.billing.storage_billing.calculate_daily_average_storage")
  @patch("robosystems.tasks.billing.storage_billing.get_graphs_with_storage_usage")
  @patch("robosystems.tasks.billing.storage_billing.get_celery_db_session")
  def test_cleanup_called(
    self,
    mock_get_session,
    mock_get_graphs,
    mock_calc_storage,
    mock_credit_service,
    mock_cleanup,
  ):
    """Test that cleanup is called after processing."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_get_graphs.return_value = []

    mock_cleanup.return_value = {
      "deleted_records": 50,
      "total_processed": 50,
      "cutoff_date": "2024-10-15T00:00:00+00:00",
    }

    result = daily_storage_billing(target_date="2025-01-15")  # type: ignore[call-arg]

    mock_cleanup.assert_called_once_with(mock_session, days_to_keep=90)
    assert result["cleanup_result"]["deleted_records"] == 50


class TestMonthlyStorageSummaryTask:
  """Test cases for monthly storage summary Celery task."""

  @patch("robosystems.tasks.billing.storage_billing.GraphUsage")
  @patch("robosystems.tasks.billing.storage_billing.get_celery_db_session")
  def test_successful_summary(self, mock_get_session, mock_usage_tracking):
    """Test successful monthly summary generation."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_summaries = {
      "graph1": {
        "user_id": "user1",
        "graph_id": "graph1",
        "total_gb_hours": 1800.0,
        "average_gb": 2.5,
        "days_tracked": 30,
      },
      "graph2": {
        "user_id": "user2",
        "graph_id": "graph2",
        "total_gb_hours": 3600.0,
        "average_gb": 5.0,
        "days_tracked": 30,
      },
    }
    mock_usage_tracking.get_monthly_storage_summary.return_value = mock_summaries

    result = monthly_storage_summary(year=2025, month=1)  # type: ignore[call-arg]

    assert result["status"] == "success"
    assert result["year"] == 2025
    assert result["month"] == 1
    assert result["total_graphs"] == 2
    assert result["total_gb_hours"] == 5400.0
    assert result["summaries"] == mock_summaries

    mock_usage_tracking.get_monthly_storage_summary.assert_called_once_with(
      user_id=None, year=2025, month=1, session=mock_session
    )
    mock_session.close.assert_called_once()

  @patch("robosystems.tasks.billing.storage_billing.GraphUsage")
  @patch("robosystems.tasks.billing.storage_billing.get_celery_db_session")
  def test_default_to_last_month(self, mock_get_session, mock_usage_tracking):
    """Test that task defaults to last month if not specified."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_usage_tracking.get_monthly_storage_summary.return_value = {}

    result = monthly_storage_summary()  # type: ignore[call-arg]

    assert result["status"] == "success"
    assert "year" in result
    assert "month" in result

    last_month = datetime.now(timezone.utc).replace(day=1) - timedelta(days=1)
    assert result["year"] == last_month.year
    assert result["month"] == last_month.month

    mock_session.close.assert_called_once()

  @patch("robosystems.tasks.billing.storage_billing.GraphUsage")
  @patch("robosystems.tasks.billing.storage_billing.get_celery_db_session")
  def test_no_summaries(self, mock_get_session, mock_usage_tracking):
    """Test handling when no summaries exist."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_usage_tracking.get_monthly_storage_summary.return_value = {}

    result = monthly_storage_summary(year=2025, month=1)  # type: ignore[call-arg]

    assert result["status"] == "success"
    assert result["total_graphs"] == 0
    assert result["total_gb_hours"] == 0

  @patch("robosystems.tasks.billing.storage_billing.GraphUsage")
  @patch("robosystems.tasks.billing.storage_billing.get_celery_db_session")
  def test_database_error(self, mock_get_session, mock_usage_tracking):
    """Test handling of database errors."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_usage_tracking.get_monthly_storage_summary.side_effect = RuntimeError(
      "Database error"
    )

    result = monthly_storage_summary(year=2025, month=1)  # type: ignore[call-arg]

    assert result["status"] == "error"
    assert "Database error" in result["error"]
    mock_session.close.assert_called_once()


class TestHelperFunctions:
  """Test cases for helper functions."""

  def test_get_graphs_with_storage_usage(self):
    """Test retrieving graphs with storage usage."""
    mock_session = MagicMock()

    mock_row1 = MagicMock()
    mock_row1.graph_id = "graph1"
    mock_row1.user_id = "user1"
    mock_row1.graph_tier = "kuzu-standard"
    mock_row1.measurement_count = 24

    mock_row2 = MagicMock()
    mock_row2.graph_id = "graph2"
    mock_row2.user_id = "user2"
    mock_row2.graph_tier = "kuzu-large"
    mock_row2.measurement_count = 48

    mock_query = MagicMock()
    mock_query.filter.return_value.group_by.return_value.all.return_value = [
      mock_row1,
      mock_row2,
    ]
    mock_session.query.return_value = mock_query

    result = get_graphs_with_storage_usage(mock_session, 2025, 1, 15)

    assert len(result) == 2
    assert result[0]["graph_id"] == "graph1"
    assert result[0]["measurement_count"] == 24
    assert result[1]["graph_id"] == "graph2"
    assert result[1]["measurement_count"] == 48

  def test_calculate_daily_average_storage(self):
    """Test calculating daily average storage."""
    mock_session = MagicMock()

    mock_query = MagicMock()
    mock_query.filter.return_value.scalar.return_value = Decimal("2.5")
    mock_session.query.return_value = mock_query

    result = calculate_daily_average_storage(mock_session, "graph1", 2025, 1, 15)

    assert result == 2.5

  def test_calculate_daily_average_storage_no_data(self):
    """Test calculating average when no data exists."""
    mock_session = MagicMock()

    mock_query = MagicMock()
    mock_query.filter.return_value.scalar.return_value = None
    mock_session.query.return_value = mock_query

    result = calculate_daily_average_storage(mock_session, "graph1", 2025, 1, 15)

    assert result is None

  def test_cleanup_old_storage_records(self):
    """Test cleanup of old storage records."""
    mock_session = MagicMock()

    mock_query = MagicMock()
    mock_query.count.return_value = 100
    mock_query.delete.return_value = 100
    mock_session.query.return_value.filter.return_value = mock_query

    result = cleanup_old_storage_records(mock_session, days_to_keep=90)

    assert result["deleted_records"] == 100
    assert result["total_processed"] == 100
    assert "cutoff_date" in result

    mock_session.commit.assert_called_once()

  def test_cleanup_old_storage_records_no_records(self):
    """Test cleanup when no old records exist."""
    mock_session = MagicMock()

    mock_query = MagicMock()
    mock_query.count.return_value = 0
    mock_session.query.return_value.filter.return_value = mock_query

    result = cleanup_old_storage_records(mock_session, days_to_keep=90)

    assert result["deleted_records"] == 0
    assert result["total_processed"] == 0

    mock_query.delete.assert_not_called()
    mock_session.commit.assert_not_called()
