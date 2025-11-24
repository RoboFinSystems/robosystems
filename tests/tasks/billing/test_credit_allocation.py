"""Tests for credit_allocation Celery tasks."""

import pytest
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

from robosystems.tasks.billing.credit_allocation import (
  allocate_graph_credits_for_user,
  check_graph_credit_health,
)


class TestAllocateGraphCreditsForUser:
  """Test cases for allocate_graph_credits_for_user task."""

  @patch("robosystems.tasks.billing.credit_allocation.get_celery_db_session")
  def test_successful_allocation_multiple_graphs(self, mock_get_db):
    """Test successful credit allocation for user with multiple graphs."""
    mock_db = MagicMock()
    mock_get_db.return_value = mock_db

    # Create mock graph credits
    mock_graph1 = MagicMock()
    mock_graph1.graph_id = "kg123456"
    mock_graph1.graph_tier = "ladybug-standard"
    mock_graph1.monthly_allocation = Decimal("50000.00")
    mock_graph1.current_balance = Decimal("52000.00")
    mock_graph1.allocate_monthly_credits.return_value = True

    mock_graph2 = MagicMock()
    mock_graph2.graph_id = "kg789012"
    mock_graph2.graph_tier = "ladybug-large"
    mock_graph2.monthly_allocation = Decimal("300000.00")
    mock_graph2.current_balance = Decimal("305000.00")
    mock_graph2.allocate_monthly_credits.return_value = True

    mock_db.query.return_value.filter.return_value.all.return_value = [
      mock_graph1,
      mock_graph2,
    ]

    result = allocate_graph_credits_for_user.apply(args=["user-123"]).get()

    assert result["user_id"] == "user-123"
    assert result["graphs_allocated"] == 2
    assert result["total_graphs"] == 2
    assert result["total_credits_allocated"] == 350000.00
    assert len(result["allocations"]) == 2
    assert result["allocations"][0]["graph_id"] == "kg123456"
    assert result["allocations"][0]["credits_allocated"] == 50000.00
    assert result["allocations"][1]["graph_id"] == "kg789012"
    assert result["allocations"][1]["credits_allocated"] == 300000.00

    mock_db.commit.assert_called_once()
    mock_db.close.assert_called_once()

  @patch("robosystems.tasks.billing.credit_allocation.get_celery_db_session")
  def test_successful_allocation_single_graph(self, mock_get_db):
    """Test successful credit allocation for user with single graph."""
    mock_db = MagicMock()
    mock_get_db.return_value = mock_db

    mock_graph = MagicMock()
    mock_graph.graph_id = "kg123456"
    mock_graph.graph_tier = "ladybug-standard"
    mock_graph.monthly_allocation = Decimal("50000.00")
    mock_graph.current_balance = Decimal("52000.00")
    mock_graph.allocate_monthly_credits.return_value = True

    mock_db.query.return_value.filter.return_value.all.return_value = [mock_graph]

    result = allocate_graph_credits_for_user.apply(args=["user-456"]).get()

    assert result["user_id"] == "user-456"
    assert result["graphs_allocated"] == 1
    assert result["total_graphs"] == 1
    assert result["total_credits_allocated"] == 50000.00
    assert len(result["allocations"]) == 1

  @patch("robosystems.tasks.billing.credit_allocation.get_celery_db_session")
  def test_user_with_no_graphs(self, mock_get_db):
    """Test credit allocation for user with no graphs."""
    mock_db = MagicMock()
    mock_get_db.return_value = mock_db

    mock_db.query.return_value.filter.return_value.all.return_value = []

    result = allocate_graph_credits_for_user.apply(args=["user-nographs"]).get()

    assert result["user_id"] == "user-nographs"
    assert result["graphs_allocated"] == 0
    assert result["total_graphs"] == 0
    assert result["total_credits_allocated"] == 0.0
    assert len(result["allocations"]) == 0

    mock_db.commit.assert_called_once()
    mock_db.close.assert_called_once()

  @patch("robosystems.tasks.billing.credit_allocation.get_celery_db_session")
  def test_partial_allocation_success(self, mock_get_db):
    """Test allocation where some graphs allocate and some don't."""
    mock_db = MagicMock()
    mock_get_db.return_value = mock_db

    # Graph 1 - allocates successfully
    mock_graph1 = MagicMock()
    mock_graph1.graph_id = "kg123456"
    mock_graph1.graph_tier = "ladybug-standard"
    mock_graph1.monthly_allocation = Decimal("50000.00")
    mock_graph1.current_balance = Decimal("52000.00")
    mock_graph1.allocate_monthly_credits.return_value = True

    # Graph 2 - doesn't allocate (e.g., not yet time)
    mock_graph2 = MagicMock()
    mock_graph2.graph_id = "kg789012"
    mock_graph2.graph_tier = "ladybug-large"
    mock_graph2.monthly_allocation = Decimal("300000.00")
    mock_graph2.current_balance = Decimal("280000.00")
    mock_graph2.allocate_monthly_credits.return_value = False

    mock_db.query.return_value.filter.return_value.all.return_value = [
      mock_graph1,
      mock_graph2,
    ]

    result = allocate_graph_credits_for_user.apply(args=["user-partial"]).get()

    assert result["user_id"] == "user-partial"
    assert result["graphs_allocated"] == 1
    assert result["total_graphs"] == 2
    assert result["total_credits_allocated"] == 50000.00
    assert len(result["allocations"]) == 1
    assert result["allocations"][0]["graph_id"] == "kg123456"

  @patch("robosystems.tasks.billing.credit_allocation.get_celery_db_session")
  def test_allocation_database_error(self, mock_get_db):
    """Test allocation handles database errors properly."""
    mock_db = MagicMock()
    mock_get_db.return_value = mock_db

    mock_db.query.side_effect = Exception("Database connection failed")

    with pytest.raises(Exception, match="Database connection failed"):
      allocate_graph_credits_for_user.apply(args=["user-error"]).get()

    mock_db.rollback.assert_called_once()
    mock_db.close.assert_called_once()

  @patch("robosystems.tasks.billing.credit_allocation.get_celery_db_session")
  def test_allocation_commit_error(self, mock_get_db):
    """Test allocation handles commit errors properly."""
    mock_db = MagicMock()
    mock_get_db.return_value = mock_db

    mock_graph = MagicMock()
    mock_graph.graph_id = "kg123456"
    mock_graph.graph_tier = "ladybug-standard"
    mock_graph.monthly_allocation = Decimal("50000.00")
    mock_graph.current_balance = Decimal("52000.00")
    mock_graph.allocate_monthly_credits.return_value = True

    mock_db.query.return_value.filter.return_value.all.return_value = [mock_graph]
    mock_db.commit.side_effect = Exception("Commit failed")

    with pytest.raises(Exception, match="Commit failed"):
      allocate_graph_credits_for_user.apply(args=["user-commit-fail"]).get()

    mock_db.rollback.assert_called_once()
    mock_db.close.assert_called_once()

  @patch("robosystems.tasks.billing.credit_allocation.get_celery_db_session")
  def test_allocation_with_different_tiers(self, mock_get_db):
    """Test allocation with various tier types."""
    mock_db = MagicMock()
    mock_get_db.return_value = mock_db

    mock_standard = MagicMock()
    mock_standard.graph_id = "kg-standard"
    mock_standard.graph_tier = "ladybug-standard"
    mock_standard.monthly_allocation = Decimal("50000.00")
    mock_standard.current_balance = Decimal("52000.00")
    mock_standard.allocate_monthly_credits.return_value = True

    mock_large = MagicMock()
    mock_large.graph_id = "kg-large"
    mock_large.graph_tier = "ladybug-large"
    mock_large.monthly_allocation = Decimal("300000.00")
    mock_large.current_balance = Decimal("305000.00")
    mock_large.allocate_monthly_credits.return_value = True

    mock_xlarge = MagicMock()
    mock_xlarge.graph_id = "kg-xlarge"
    mock_xlarge.graph_tier = "ladybug-xlarge"
    mock_xlarge.monthly_allocation = Decimal("600000.00")
    mock_xlarge.current_balance = Decimal("610000.00")
    mock_xlarge.allocate_monthly_credits.return_value = True

    mock_db.query.return_value.filter.return_value.all.return_value = [
      mock_standard,
      mock_large,
      mock_xlarge,
    ]

    result = allocate_graph_credits_for_user.apply(args=["user-tiers"]).get()

    assert result["graphs_allocated"] == 3
    assert result["total_credits_allocated"] == 950000.00
    assert {a["graph_tier"] for a in result["allocations"]} == {
      "ladybug-standard",
      "ladybug-large",
      "ladybug-xlarge",
    }


class TestCheckGraphCreditHealth:
  """Test cases for check_graph_credit_health task."""

  @patch("robosystems.tasks.billing.credit_allocation.datetime")
  @patch("robosystems.tasks.billing.credit_allocation.get_celery_db_session")
  def test_healthy_system(self, mock_get_db, mock_datetime):
    """Test health check with no issues."""
    mock_db = MagicMock()
    mock_get_db.return_value = mock_db

    now = datetime(2025, 11, 23, 12, 0, 0, tzinfo=timezone.utc)
    mock_datetime.now.return_value = now

    # No overdue allocations
    mock_db.query.return_value.filter.return_value.scalar.return_value = 0
    # No low balances
    mock_db.query.return_value.filter.return_value.all.return_value = []
    # Total count
    mock_db.query.return_value.scalar.return_value = 100

    result = check_graph_credit_health.apply().get()

    assert result["status"] == "healthy"
    assert result["checked_at"] == now.isoformat()
    assert result["total_graph_credit_pools"] == 100
    assert len(result["issues"]) == 0

    mock_db.close.assert_called_once()

  @patch("robosystems.tasks.billing.credit_allocation.datetime")
  @patch("robosystems.tasks.billing.credit_allocation.get_celery_db_session")
  def test_overdue_allocations_detected(self, mock_get_db, mock_datetime):
    """Test health check detects overdue allocations."""
    mock_db = MagicMock()
    mock_get_db.return_value = mock_db

    now = datetime(2025, 11, 23, 12, 0, 0, tzinfo=timezone.utc)
    mock_datetime.now.return_value = now

    # Setup query mock to return different values for different calls
    query_results = [5, [], 0, 100]  # overdue, low_balance, zero_alloc, total
    mock_db.query.return_value.filter.return_value.scalar.side_effect = query_results[
      ::2
    ]
    mock_db.query.return_value.filter.return_value.all.return_value = []
    mock_db.query.return_value.scalar.return_value = 100

    result = check_graph_credit_health.apply().get()

    assert result["status"] == "issues_found"
    assert len(result["issues"]) == 1
    assert result["issues"][0]["type"] == "overdue_allocations"
    assert result["issues"][0]["severity"] == "warning"
    assert result["issues"][0]["count"] == 5

  @patch("robosystems.tasks.billing.credit_allocation.datetime")
  @patch("robosystems.tasks.billing.credit_allocation.get_celery_db_session")
  def test_low_balances_detected(self, mock_get_db, mock_datetime):
    """Test health check detects low balances."""
    mock_db = MagicMock()
    mock_get_db.return_value = mock_db

    now = datetime(2025, 11, 23, 12, 0, 0, tzinfo=timezone.utc)
    mock_datetime.now.return_value = now

    # Create mock low balance credits
    mock_credit1 = MagicMock()
    mock_credit1.graph_id = "kg-low1"
    mock_credit1.current_balance = Decimal("1000.00")
    mock_credit1.monthly_allocation = Decimal("50000.00")

    mock_credit2 = MagicMock()
    mock_credit2.graph_id = "kg-low2"
    mock_credit2.current_balance = Decimal("2000.00")
    mock_credit2.monthly_allocation = Decimal("50000.00")

    # Mock query behavior
    mock_query = MagicMock()
    mock_filter = MagicMock()
    mock_query.filter.return_value = mock_filter

    # First call: overdue count
    # Second call: low balances
    # Third call: zero allocations
    # Fourth call: total count
    call_count = [0]

    def query_side_effect(*args):
      call_count[0] += 1
      if call_count[0] == 1:  # overdue count query
        result = MagicMock()
        result.filter.return_value.scalar.return_value = 0
        return result
      elif call_count[0] == 2:  # low balance query
        result = MagicMock()
        result.filter.return_value.all.return_value = [mock_credit1, mock_credit2]
        return result
      elif call_count[0] == 3:  # zero allocation count query
        result = MagicMock()
        result.filter.return_value.scalar.return_value = 0
        return result
      else:  # total count query
        result = MagicMock()
        result.scalar.return_value = 100
        return result

    mock_db.query.side_effect = query_side_effect

    result = check_graph_credit_health.apply().get()

    assert result["status"] == "issues_found"
    assert len(result["issues"]) == 1
    assert result["issues"][0]["type"] == "low_balances"
    assert result["issues"][0]["severity"] == "info"
    assert result["issues"][0]["count"] == 2
    assert len(result["issues"][0]["details"]) == 2

  @patch("robosystems.tasks.billing.credit_allocation.datetime")
  @patch("robosystems.tasks.billing.credit_allocation.get_celery_db_session")
  def test_zero_allocations_detected(self, mock_get_db, mock_datetime):
    """Test health check detects zero allocations."""
    mock_db = MagicMock()
    mock_get_db.return_value = mock_db

    now = datetime(2025, 11, 23, 12, 0, 0, tzinfo=timezone.utc)
    mock_datetime.now.return_value = now

    call_count = [0]

    def query_side_effect(*args):
      call_count[0] += 1
      if call_count[0] == 1:  # overdue count
        result = MagicMock()
        result.filter.return_value.scalar.return_value = 0
        return result
      elif call_count[0] == 2:  # low balances
        result = MagicMock()
        result.filter.return_value.all.return_value = []
        return result
      elif call_count[0] == 3:  # zero allocations
        result = MagicMock()
        result.filter.return_value.scalar.return_value = 3
        return result
      else:  # total count
        result = MagicMock()
        result.scalar.return_value = 100
        return result

    mock_db.query.side_effect = query_side_effect

    result = check_graph_credit_health.apply().get()

    assert result["status"] == "issues_found"
    assert len(result["issues"]) == 1
    assert result["issues"][0]["type"] == "zero_allocations"
    assert result["issues"][0]["severity"] == "error"
    assert result["issues"][0]["count"] == 3

  @patch("robosystems.tasks.billing.credit_allocation.datetime")
  @patch("robosystems.tasks.billing.credit_allocation.get_celery_db_session")
  def test_multiple_issues_detected(self, mock_get_db, mock_datetime):
    """Test health check detects multiple issue types."""
    mock_db = MagicMock()
    mock_get_db.return_value = mock_db

    now = datetime(2025, 11, 23, 12, 0, 0, tzinfo=timezone.utc)
    mock_datetime.now.return_value = now

    # Create mock low balance credit
    mock_credit = MagicMock()
    mock_credit.graph_id = "kg-low"
    mock_credit.current_balance = Decimal("1000.00")
    mock_credit.monthly_allocation = Decimal("50000.00")

    call_count = [0]

    def query_side_effect(*args):
      call_count[0] += 1
      if call_count[0] == 1:  # overdue count
        result = MagicMock()
        result.filter.return_value.scalar.return_value = 5
        return result
      elif call_count[0] == 2:  # low balances
        result = MagicMock()
        result.filter.return_value.all.return_value = [mock_credit]
        return result
      elif call_count[0] == 3:  # zero allocations
        result = MagicMock()
        result.filter.return_value.scalar.return_value = 2
        return result
      else:  # total count
        result = MagicMock()
        result.scalar.return_value = 100
        return result

    mock_db.query.side_effect = query_side_effect

    result = check_graph_credit_health.apply().get()

    assert result["status"] == "issues_found"
    assert len(result["issues"]) == 3

    issue_types = {issue["type"] for issue in result["issues"]}
    assert issue_types == {"overdue_allocations", "low_balances", "zero_allocations"}

  @patch("robosystems.tasks.billing.credit_allocation.datetime")
  @patch("robosystems.tasks.billing.credit_allocation.get_celery_db_session")
  def test_health_check_database_error(self, mock_get_db, mock_datetime):
    """Test health check handles database errors properly."""
    mock_db = MagicMock()
    mock_get_db.return_value = mock_db

    now = datetime(2025, 11, 23, 12, 0, 0, tzinfo=timezone.utc)
    mock_datetime.now.return_value = now

    mock_db.query.side_effect = Exception("Database query failed")

    with pytest.raises(Exception, match="Database query failed"):
      check_graph_credit_health.apply().get()

    mock_db.close.assert_called_once()

  @patch("robosystems.tasks.billing.credit_allocation.datetime")
  @patch("robosystems.tasks.billing.credit_allocation.get_celery_db_session")
  def test_low_balance_details_limited(self, mock_get_db, mock_datetime):
    """Test that low balance details are limited to 10 items."""
    mock_db = MagicMock()
    mock_get_db.return_value = mock_db

    now = datetime(2025, 11, 23, 12, 0, 0, tzinfo=timezone.utc)
    mock_datetime.now.return_value = now

    # Create 15 mock low balance credits
    low_credits = []
    for i in range(15):
      mock_credit = MagicMock()
      mock_credit.graph_id = f"kg-low{i}"
      mock_credit.current_balance = Decimal("1000.00")
      mock_credit.monthly_allocation = Decimal("50000.00")
      low_credits.append(mock_credit)

    call_count = [0]

    def query_side_effect(*args):
      call_count[0] += 1
      if call_count[0] == 1:  # overdue count
        result = MagicMock()
        result.filter.return_value.scalar.return_value = 0
        return result
      elif call_count[0] == 2:  # low balances
        result = MagicMock()
        result.filter.return_value.all.return_value = low_credits
        return result
      elif call_count[0] == 3:  # zero allocations
        result = MagicMock()
        result.filter.return_value.scalar.return_value = 0
        return result
      else:  # total count
        result = MagicMock()
        result.scalar.return_value = 100
        return result

    mock_db.query.side_effect = query_side_effect

    result = check_graph_credit_health.apply().get()

    assert result["status"] == "issues_found"
    assert len(result["issues"]) == 1
    assert result["issues"][0]["type"] == "low_balances"
    assert result["issues"][0]["count"] == 15
    # Details should be limited to 10
    assert len(result["issues"][0]["details"]) == 10
