"""Tests for shared credit allocation tasks."""

import pytest
from unittest.mock import MagicMock, patch
from sqlalchemy.exc import SQLAlchemyError

from robosystems.tasks.billing.shared_credit_allocation import (
  allocate_monthly_shared_credits,
  allocate_shared_credits_for_user,
  check_credit_allocation_health,
)


class TestAllocateMonthlySharedCreditsTask:
  """Test cases for monthly shared credits allocation Celery task."""

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  def test_successful_allocation(self, mock_get_session):
    """Test successful monthly credit allocation."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_addon1 = MagicMock()
    mock_addon1.id = "addon1"
    mock_addon1.user_id = "user1"
    mock_addon1.addon_type = "sec"
    mock_addon1.addon_tier = "standard"

    mock_pool1 = MagicMock()
    mock_pool1.id = "pool1"
    mock_pool1.is_active = True
    mock_pool1.monthly_allocation = 1000
    mock_pool1.current_balance = 1000
    mock_pool1.rollover_credits = 0
    mock_pool1.addon = mock_addon1
    mock_pool1.allocate_monthly_credits.return_value = True

    mock_addon2 = MagicMock()
    mock_addon2.id = "addon2"
    mock_addon2.user_id = "user2"
    mock_addon2.addon_type = "sec"
    mock_addon2.addon_tier = "enterprise"

    mock_pool2 = MagicMock()
    mock_pool2.id = "pool2"
    mock_pool2.is_active = True
    mock_pool2.monthly_allocation = 5000
    mock_pool2.current_balance = 5200
    mock_pool2.rollover_credits = 200
    mock_pool2.addon = mock_addon2
    mock_pool2.allocate_monthly_credits.return_value = True

    mock_query = MagicMock()
    mock_query.filter.return_value.all.return_value = [mock_pool1, mock_pool2]
    mock_session.query.return_value = mock_query

    result = allocate_monthly_shared_credits()  # type: ignore[call-arg]

    assert result["success"] is True
    assert result["allocations_performed"] == 2
    assert result["allocations_failed"] == 0
    assert result["total_credits_allocated"] == 6000
    assert len(result["allocation_details"]) == 2

    mock_session.commit.assert_called_once()
    mock_session.close.assert_called_once()

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  def test_no_pools_due_for_allocation(self, mock_get_session):
    """Test when no credit pools are due for allocation."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_query = MagicMock()
    mock_query.filter.return_value.all.return_value = []
    mock_session.query.return_value = mock_query

    result = allocate_monthly_shared_credits()  # type: ignore[call-arg]

    assert result["success"] is True
    assert result["total_pools_checked"] == 0
    assert result["allocations_performed"] == 0
    assert result["total_credits_allocated"] == 0

    mock_session.close.assert_called_once()

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  def test_pool_without_addon_skipped(self, mock_get_session):
    """Test that pools without addons are skipped."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_pool = MagicMock()
    mock_pool.id = "pool1"
    mock_pool.addon = None

    mock_query = MagicMock()
    mock_query.filter.return_value.all.return_value = [mock_pool]
    mock_session.query.return_value = mock_query

    result = allocate_monthly_shared_credits()  # type: ignore[call-arg]

    assert result["success"] is True
    assert result["allocations_performed"] == 0
    assert result["allocations_failed"] == 1

    mock_pool.allocate_monthly_credits.assert_not_called()

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  def test_pool_not_due_for_allocation(self, mock_get_session):
    """Test handling when pool.allocate_monthly_credits returns False."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_addon = MagicMock()
    mock_addon.id = "addon1"

    mock_pool = MagicMock()
    mock_pool.id = "pool1"
    mock_pool.addon = mock_addon
    mock_pool.allocate_monthly_credits.return_value = False

    mock_query = MagicMock()
    mock_query.filter.return_value.all.return_value = [mock_pool]
    mock_session.query.return_value = mock_query

    result = allocate_monthly_shared_credits()  # type: ignore[call-arg]

    assert result["success"] is True
    assert result["allocations_performed"] == 0
    assert len(result["allocation_details"]) == 0

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  def test_partial_allocation_errors(self, mock_get_session):
    """Test handling of partial allocation errors."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_addon1 = MagicMock()
    mock_addon1.id = "addon1"
    mock_addon1.user_id = "user1"
    mock_addon1.addon_type = "sec"
    mock_addon1.addon_tier = "standard"

    mock_pool1 = MagicMock()
    mock_pool1.id = "pool1"
    mock_pool1.addon = mock_addon1
    mock_pool1.monthly_allocation = 1000
    mock_pool1.current_balance = 1000
    mock_pool1.rollover_credits = 0
    mock_pool1.allocate_monthly_credits.return_value = True

    mock_addon2 = MagicMock()
    mock_addon2.id = "addon2"

    mock_pool2 = MagicMock()
    mock_pool2.id = "pool2"
    mock_pool2.addon = mock_addon2
    mock_pool2.allocate_monthly_credits.side_effect = SQLAlchemyError("Database error")

    mock_query = MagicMock()
    mock_query.filter.return_value.all.return_value = [mock_pool1, mock_pool2]
    mock_session.query.return_value = mock_query

    result = allocate_monthly_shared_credits()  # type: ignore[call-arg]

    assert result["success"] is True
    assert result["allocations_performed"] == 1
    assert result["allocations_failed"] == 1

    assert mock_session.rollback.call_count == 1
    mock_session.commit.assert_called_once()

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  def test_commit_failure(self, mock_get_session):
    """Test handling of commit failures."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_addon = MagicMock()
    mock_addon.id = "addon1"
    mock_addon.user_id = "user1"
    mock_addon.addon_type = "sec"
    mock_addon.addon_tier = "standard"

    mock_pool = MagicMock()
    mock_pool.id = "pool1"
    mock_pool.addon = mock_addon
    mock_pool.monthly_allocation = 1000
    mock_pool.current_balance = 1000
    mock_pool.rollover_credits = 0
    mock_pool.allocate_monthly_credits.return_value = True

    mock_query = MagicMock()
    mock_query.filter.return_value.all.return_value = [mock_pool]
    mock_session.query.return_value = mock_query

    mock_session.commit.side_effect = SQLAlchemyError("Commit failed")

    mock_task = MagicMock()
    mock_task.request.retries = 0
    mock_task.max_retries = 3

    result = allocate_monthly_shared_credits.apply(kwargs={}).get()  # type: ignore[attr-defined]

    assert result["success"] is False
    assert "error" in result

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  def test_database_connection_error(self, mock_get_session):
    """Test handling of database connection errors triggers retry."""
    mock_get_session.side_effect = RuntimeError("Connection failed")

    with patch.object(allocate_monthly_shared_credits, "retry") as mock_retry:
      mock_retry.side_effect = RuntimeError("Connection failed")

      with pytest.raises(RuntimeError) as exc_info:
        allocate_monthly_shared_credits.apply(kwargs={}).get()  # type: ignore[attr-defined]

      assert "Connection failed" in str(exc_info.value)

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  def test_rollover_credits_tracking(self, mock_get_session):
    """Test that rollover credits are properly tracked."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_addon = MagicMock()
    mock_addon.id = "addon1"
    mock_addon.user_id = "user1"
    mock_addon.addon_type = "sec"
    mock_addon.addon_tier = "standard"

    mock_pool = MagicMock()
    mock_pool.id = "pool1"
    mock_pool.addon = mock_addon
    mock_pool.monthly_allocation = 1000

    initial_rollover = 200
    new_rollover = 500
    mock_pool.rollover_credits = initial_rollover

    def allocate_side_effect(session):
      mock_pool.rollover_credits = new_rollover
      return True

    mock_pool.allocate_monthly_credits.side_effect = allocate_side_effect

    initial_balance = 800
    new_balance = 1500
    mock_pool.current_balance = initial_balance

    def balance_side_effect():
      return (
        new_balance if mock_pool.allocate_monthly_credits.called else initial_balance
      )

    type(mock_pool).current_balance = property(lambda self: balance_side_effect())

    mock_query = MagicMock()
    mock_query.filter.return_value.all.return_value = [mock_pool]
    mock_session.query.return_value = mock_query

    result = allocate_monthly_shared_credits()  # type: ignore[call-arg]

    assert result["success"] is True
    assert result["total_credits_rolled_over"] == (new_rollover - initial_rollover)


class TestAllocateSharedCreditsForUserTask:
  """Test cases for user-specific credit allocation Celery task."""

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  def test_successful_user_allocation(self, mock_get_session):
    """Test successful credit allocation for a specific user."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_repo = MagicMock()
    mock_repo.repository_name = "sec-filings"
    mock_repo.repository_type.value = "sec"
    mock_repo.repository_plan.value = "standard"

    mock_pool = MagicMock()
    mock_pool.id = "pool1"
    mock_pool.monthly_allocation = 1000
    mock_pool.current_balance = 1000
    mock_pool.user_repository = mock_repo
    mock_pool.allocate_monthly_credits.return_value = True

    mock_query = MagicMock()
    mock_query.join.return_value.filter.return_value.all.return_value = [mock_pool]
    mock_session.query.return_value = mock_query

    result = allocate_shared_credits_for_user(user_id="user1")  # type: ignore[call-arg]

    assert result["success"] is True
    assert result["user_id"] == "user1"
    assert result["allocations_performed"] == 1
    assert len(result["allocation_details"]) == 1
    assert result["allocation_details"][0]["repository_name"] == "sec-filings"

    mock_session.commit.assert_called_once()
    mock_session.close.assert_called_once()

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  def test_no_pools_for_user(self, mock_get_session):
    """Test when user has no credit pools."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_query = MagicMock()
    mock_query.join.return_value.filter.return_value.all.return_value = []
    mock_session.query.return_value = mock_query

    result = allocate_shared_credits_for_user(user_id="user1")  # type: ignore[call-arg]

    assert result["success"] is True
    assert result["allocations_performed"] == 0
    assert len(result["allocation_details"]) == 0

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  def test_multiple_pools_for_user(self, mock_get_session):
    """Test allocation for user with multiple credit pools."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_repo1 = MagicMock()
    mock_repo1.repository_name = "sec-filings"
    mock_repo1.repository_type.value = "sec"
    mock_repo1.repository_plan.value = "standard"

    mock_pool1 = MagicMock()
    mock_pool1.id = "pool1"
    mock_pool1.monthly_allocation = 1000
    mock_pool1.current_balance = 1000
    mock_pool1.user_repository = mock_repo1
    mock_pool1.allocate_monthly_credits.return_value = True

    mock_repo2 = MagicMock()
    mock_repo2.repository_name = "industry-data"
    mock_repo2.repository_type.value = "industry"
    mock_repo2.repository_plan.value = "enterprise"

    mock_pool2 = MagicMock()
    mock_pool2.id = "pool2"
    mock_pool2.monthly_allocation = 5000
    mock_pool2.current_balance = 5000
    mock_pool2.user_repository = mock_repo2
    mock_pool2.allocate_monthly_credits.return_value = True

    mock_query = MagicMock()
    mock_query.join.return_value.filter.return_value.all.return_value = [
      mock_pool1,
      mock_pool2,
    ]
    mock_session.query.return_value = mock_query

    result = allocate_shared_credits_for_user(user_id="user1")  # type: ignore[call-arg]

    assert result["success"] is True
    assert result["allocations_performed"] == 2
    assert len(result["allocation_details"]) == 2

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  def test_user_allocation_database_error(self, mock_get_session):
    """Test handling of database errors during user allocation."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_query = MagicMock()
    mock_query.join.return_value.filter.return_value.all.side_effect = SQLAlchemyError(
      "Database error"
    )
    mock_session.query.return_value = mock_query

    result = allocate_shared_credits_for_user(user_id="user1")  # type: ignore[call-arg]

    assert result["success"] is False
    assert "Database error" in result["error"]

    mock_session.rollback.assert_called_once()
    mock_session.close.assert_called_once()


class TestCheckCreditAllocationHealthTask:
  """Test cases for credit allocation health check Celery task."""

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  def test_healthy_system(self, mock_get_session):
    """Test health check when system is healthy."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_query_overdue = MagicMock()
    mock_query_overdue.filter.return_value.count.return_value = 0

    mock_query_negative = MagicMock()
    mock_query_negative.filter.return_value.count.return_value = 0

    mock_query_active = MagicMock()
    mock_query_active.filter.return_value.count.return_value = 10

    mock_query_upcoming = MagicMock()
    mock_query_upcoming.filter.return_value.count.return_value = 3

    def query_side_effect(*args):
      call_count = mock_session.query.call_count
      if call_count == 1:
        return mock_query_overdue
      elif call_count == 2:
        return mock_query_negative
      elif call_count == 3:
        return mock_query_active
      else:
        return mock_query_upcoming

    mock_session.query.side_effect = query_side_effect

    result = check_credit_allocation_health()  # type: ignore[call-arg]

    assert result["healthy"] is True
    assert result["overdue_allocations"] == 0
    assert result["negative_balance_pools"] == 0
    assert result["total_active_pools"] == 10
    assert result["upcoming_allocations_7_days"] == 3
    assert len(result["warnings"]) == 0

    mock_session.close.assert_called_once()

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  def test_overdue_allocations_warning(self, mock_get_session):
    """Test health check with overdue allocations."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_query_overdue = MagicMock()
    mock_query_overdue.filter.return_value.count.return_value = 5

    mock_query_negative = MagicMock()
    mock_query_negative.filter.return_value.count.return_value = 0

    mock_query_active = MagicMock()
    mock_query_active.filter.return_value.count.return_value = 10

    mock_query_upcoming = MagicMock()
    mock_query_upcoming.filter.return_value.count.return_value = 2

    def query_side_effect(*args):
      call_count = mock_session.query.call_count
      if call_count == 1:
        return mock_query_overdue
      elif call_count == 2:
        return mock_query_negative
      elif call_count == 3:
        return mock_query_active
      else:
        return mock_query_upcoming

    mock_session.query.side_effect = query_side_effect

    result = check_credit_allocation_health()  # type: ignore[call-arg]

    assert result["healthy"] is False
    assert result["overdue_allocations"] == 5
    assert len(result["warnings"]) == 1
    assert "5 credit pools have overdue allocations" in result["warnings"][0]

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  def test_negative_balance_warning(self, mock_get_session):
    """Test health check with negative balances."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_query_overdue = MagicMock()
    mock_query_overdue.filter.return_value.count.return_value = 0

    mock_query_negative = MagicMock()
    mock_query_negative.filter.return_value.count.return_value = 3

    mock_query_active = MagicMock()
    mock_query_active.filter.return_value.count.return_value = 10

    mock_query_upcoming = MagicMock()
    mock_query_upcoming.filter.return_value.count.return_value = 1

    def query_side_effect(*args):
      call_count = mock_session.query.call_count
      if call_count == 1:
        return mock_query_overdue
      elif call_count == 2:
        return mock_query_negative
      elif call_count == 3:
        return mock_query_active
      else:
        return mock_query_upcoming

    mock_session.query.side_effect = query_side_effect

    result = check_credit_allocation_health()  # type: ignore[call-arg]

    assert result["healthy"] is False
    assert result["negative_balance_pools"] == 3
    assert len(result["warnings"]) == 1
    assert "3 credit pools have negative balances" in result["warnings"][0]

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  def test_multiple_warnings(self, mock_get_session):
    """Test health check with multiple issues."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_query_overdue = MagicMock()
    mock_query_overdue.filter.return_value.count.return_value = 2

    mock_query_negative = MagicMock()
    mock_query_negative.filter.return_value.count.return_value = 1

    mock_query_active = MagicMock()
    mock_query_active.filter.return_value.count.return_value = 10

    mock_query_upcoming = MagicMock()
    mock_query_upcoming.filter.return_value.count.return_value = 5

    def query_side_effect(*args):
      call_count = mock_session.query.call_count
      if call_count == 1:
        return mock_query_overdue
      elif call_count == 2:
        return mock_query_negative
      elif call_count == 3:
        return mock_query_active
      else:
        return mock_query_upcoming

    mock_session.query.side_effect = query_side_effect

    result = check_credit_allocation_health()  # type: ignore[call-arg]

    assert result["healthy"] is False
    assert len(result["warnings"]) == 2

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  def test_health_check_database_error(self, mock_get_session):
    """Test health check handling of database errors."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_session.query.side_effect = SQLAlchemyError("Database error")

    result = check_credit_allocation_health()  # type: ignore[call-arg]

    assert result["healthy"] is False
    assert "Database error" in result["error"]

    mock_session.close.assert_called_once()
