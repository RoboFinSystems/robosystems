"""Tests for shared credit allocation tasks."""

import pytest
from unittest.mock import MagicMock, patch

from robosystems.tasks.billing.shared_credit_allocation import (
  allocate_monthly_shared_credits,
  allocate_shared_credits_for_user,
  check_credit_allocation_health,
)


class TestAllocateMonthlySharedCreditsTask:
  """Test cases for monthly shared credits allocation Celery task."""

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  @patch(
    "robosystems.tasks.billing.shared_credit_allocation.deactivate_canceled_subscription_credits"
  )
  def test_successful_allocation_no_subscriptions(
    self, mock_deactivate, mock_get_session
  ):
    """Test successful allocation when no subscriptions exist."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session
    mock_deactivate.return_value = 0

    mock_query = MagicMock()
    mock_query.filter.return_value.all.return_value = []
    mock_session.query.return_value = mock_query

    result = allocate_monthly_shared_credits()

    assert result["success"] is True
    assert result["active_subscriptions_found"] == 0
    assert result["allocations_performed"] == 0
    assert result["total_credits_allocated"] == 0

    mock_session.close.assert_called_once()

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  @patch(
    "robosystems.tasks.billing.shared_credit_allocation.deactivate_canceled_subscription_credits"
  )
  def test_deactivates_canceled_subscriptions(self, mock_deactivate, mock_get_session):
    """Test that canceled subscriptions are deactivated."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session
    mock_deactivate.return_value = 3

    mock_query = MagicMock()
    mock_query.filter.return_value.all.return_value = []
    mock_session.query.return_value = mock_query

    result = allocate_monthly_shared_credits()

    assert result["success"] is True
    assert result["credit_pools_deactivated"] == 3
    mock_deactivate.assert_called_once_with(mock_session)
    mock_session.close.assert_called_once()

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  @patch(
    "robosystems.tasks.billing.shared_credit_allocation.deactivate_canceled_subscription_credits"
  )
  def test_successful_allocation_with_subscriptions(
    self, mock_deactivate, mock_get_session
  ):
    """Test successful allocation with active subscriptions."""
    from decimal import Decimal

    mock_session = MagicMock()
    mock_get_session.return_value = mock_session
    mock_deactivate.return_value = 0

    # Mock subscription
    mock_sub = MagicMock()
    mock_sub.id = "sub_123"
    mock_sub.org_id = "org_456"
    mock_sub.resource_id = "sec"
    mock_sub.plan_name = "sec-starter"

    # Mock owner
    mock_owner = MagicMock()
    mock_owner.user_id = "user_789"

    # Mock UserRepository
    mock_user_repo = MagicMock()
    mock_user_repo.id = "repo_111"

    # Mock UserRepositoryCredits
    mock_credit_pool = MagicMock()
    mock_credit_pool.next_allocation_date = None
    mock_credit_pool.current_balance = Decimal("500.00")
    mock_credit_pool.rollover_credits = Decimal("100.00")
    mock_credit_pool.monthly_allocation = Decimal("1000.00")
    mock_credit_pool.allocate_monthly_credits.return_value = True

    # Setup query mocks
    def query_side_effect(model):
      if model.__name__ == "BillingSubscription":
        mock_q = MagicMock()
        mock_q.filter.return_value.all.return_value = [mock_sub]
        return mock_q
      elif model.__name__ == "OrgUser":
        mock_q = MagicMock()
        mock_q.filter.return_value.first.return_value = mock_owner
        return mock_q
      elif model.__name__ == "UserRepository":
        mock_q = MagicMock()
        mock_q.filter.return_value.first.return_value = mock_user_repo
        return mock_q
      return MagicMock()

    mock_session.query.side_effect = query_side_effect

    # Mock UserRepositoryCredits.get_user_repository_credits
    with patch(
      "robosystems.tasks.billing.shared_credit_allocation.UserRepositoryCredits.get_user_repository_credits"
    ) as mock_get_credits:
      mock_get_credits.return_value = mock_credit_pool

      result = allocate_monthly_shared_credits()

      assert result["success"] is True
      assert result["active_subscriptions_found"] == 1
      assert result["allocations_performed"] == 1
      assert result["total_credits_allocated"] == 1000.0

      mock_session.commit.assert_called_once()
      mock_session.close.assert_called_once()

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  @patch(
    "robosystems.tasks.billing.shared_credit_allocation.deactivate_canceled_subscription_credits"
  )
  def test_no_owner_found_for_org(self, mock_deactivate, mock_get_session):
    """Test handling when org has no owner."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session
    mock_deactivate.return_value = 0

    # Mock subscription
    mock_sub = MagicMock()
    mock_sub.id = "sub_123"
    mock_sub.org_id = "org_noowner"

    def query_side_effect(model):
      if model.__name__ == "BillingSubscription":
        mock_q = MagicMock()
        mock_q.filter.return_value.all.return_value = [mock_sub]
        return mock_q
      elif model.__name__ == "OrgUser":
        mock_q = MagicMock()
        mock_q.filter.return_value.first.return_value = None  # No owner
        return mock_q
      return MagicMock()

    mock_session.query.side_effect = query_side_effect

    result = allocate_monthly_shared_credits()

    assert result["success"] is True
    assert result["allocations_failed"] == 1

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  @patch(
    "robosystems.tasks.billing.shared_credit_allocation.deactivate_canceled_subscription_credits"
  )
  def test_creates_user_repository_when_missing(
    self, mock_deactivate, mock_get_session
  ):
    """Test creation of UserRepository when it doesn't exist."""
    from decimal import Decimal

    mock_session = MagicMock()
    mock_get_session.return_value = mock_session
    mock_deactivate.return_value = 0

    # Mock subscription
    mock_sub = MagicMock()
    mock_sub.id = "sub_123"
    mock_sub.org_id = "org_456"
    mock_sub.resource_id = "sec"
    mock_sub.plan_name = "sec-starter"

    # Mock owner
    mock_owner = MagicMock()
    mock_owner.user_id = "user_789"

    # Mock UserRepositoryCredits
    mock_credit_pool = MagicMock()
    mock_credit_pool.next_allocation_date = None
    mock_credit_pool.current_balance = Decimal("500.00")
    mock_credit_pool.rollover_credits = Decimal("100.00")
    mock_credit_pool.monthly_allocation = Decimal("1000.00")
    mock_credit_pool.allocate_monthly_credits.return_value = True

    def query_side_effect(model):
      if model.__name__ == "BillingSubscription":
        mock_q = MagicMock()
        mock_q.filter.return_value.all.return_value = [mock_sub]
        return mock_q
      elif model.__name__ == "OrgUser":
        mock_q = MagicMock()
        mock_q.filter.return_value.first.return_value = mock_owner
        return mock_q
      elif model.__name__ == "UserRepository":
        mock_q = MagicMock()
        mock_q.filter.return_value.first.return_value = None  # Doesn't exist
        return mock_q
      return MagicMock()

    mock_session.query.side_effect = query_side_effect

    with patch(
      "robosystems.tasks.billing.shared_credit_allocation.UserRepositoryCredits.get_user_repository_credits"
    ) as mock_get_credits:
      mock_get_credits.return_value = mock_credit_pool

      result = allocate_monthly_shared_credits()

      assert result["success"] is True
      mock_session.add.assert_called()

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  @patch(
    "robosystems.tasks.billing.shared_credit_allocation.deactivate_canceled_subscription_credits"
  )
  def test_invalid_repository_type_or_plan(self, mock_deactivate, mock_get_session):
    """Test handling of invalid repository type or plan."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session
    mock_deactivate.return_value = 0

    # Mock subscription with invalid plan
    mock_sub = MagicMock()
    mock_sub.id = "sub_123"
    mock_sub.org_id = "org_456"
    mock_sub.resource_id = "invalid_repo"
    mock_sub.plan_name = "invalid-plan"

    # Mock owner
    mock_owner = MagicMock()
    mock_owner.user_id = "user_789"

    def query_side_effect(model):
      if model.__name__ == "BillingSubscription":
        mock_q = MagicMock()
        mock_q.filter.return_value.all.return_value = [mock_sub]
        return mock_q
      elif model.__name__ == "OrgUser":
        mock_q = MagicMock()
        mock_q.filter.return_value.first.return_value = mock_owner
        return mock_q
      elif model.__name__ == "UserRepository":
        mock_q = MagicMock()
        mock_q.filter.return_value.first.return_value = None
        return mock_q
      return MagicMock()

    mock_session.query.side_effect = query_side_effect

    result = allocate_monthly_shared_credits()

    assert result["success"] is True
    assert result["allocations_failed"] == 1

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  @patch(
    "robosystems.tasks.billing.shared_credit_allocation.deactivate_canceled_subscription_credits"
  )
  def test_creates_credit_pool_when_missing(self, mock_deactivate, mock_get_session):
    """Test creation of UserRepositoryCredits when it doesn't exist."""

    mock_session = MagicMock()
    mock_get_session.return_value = mock_session
    mock_deactivate.return_value = 0

    # Mock subscription
    mock_sub = MagicMock()
    mock_sub.id = "sub_123"
    mock_sub.org_id = "org_456"
    mock_sub.resource_id = "sec"
    mock_sub.plan_name = "sec-starter"

    # Mock owner
    mock_owner = MagicMock()
    mock_owner.user_id = "user_789"

    # Mock UserRepository
    mock_user_repo = MagicMock()
    mock_user_repo.id = "repo_111"

    def query_side_effect(model):
      if model.__name__ == "BillingSubscription":
        mock_q = MagicMock()
        mock_q.filter.return_value.all.return_value = [mock_sub]
        return mock_q
      elif model.__name__ == "OrgUser":
        mock_q = MagicMock()
        mock_q.filter.return_value.first.return_value = mock_owner
        return mock_q
      elif model.__name__ == "UserRepository":
        mock_q = MagicMock()
        mock_q.filter.return_value.first.return_value = mock_user_repo
        return mock_q
      return MagicMock()

    mock_session.query.side_effect = query_side_effect

    with patch(
      "robosystems.tasks.billing.shared_credit_allocation.UserRepositoryCredits.get_user_repository_credits"
    ) as mock_get_credits:
      mock_get_credits.return_value = None  # Credit pool doesn't exist

      result = allocate_monthly_shared_credits()

      assert result["success"] is True
      assert result["credit_pools_created"] >= 1

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  @patch(
    "robosystems.tasks.billing.shared_credit_allocation.deactivate_canceled_subscription_credits"
  )
  def test_reactivates_inactive_credit_pool(self, mock_deactivate, mock_get_session):
    """Test reactivation of inactive credit pools."""
    from decimal import Decimal

    mock_session = MagicMock()
    mock_get_session.return_value = mock_session
    mock_deactivate.return_value = 0

    # Mock subscription
    mock_sub = MagicMock()
    mock_sub.id = "sub_123"
    mock_sub.org_id = "org_456"
    mock_sub.resource_id = "sec"
    mock_sub.plan_name = "sec-starter"

    # Mock owner
    mock_owner = MagicMock()
    mock_owner.user_id = "user_789"

    # Mock UserRepository
    mock_user_repo = MagicMock()
    mock_user_repo.id = "repo_111"

    # Mock inactive credit pool
    mock_credit_pool = MagicMock()
    mock_credit_pool.is_active = False
    mock_credit_pool.next_allocation_date = None
    mock_credit_pool.current_balance = Decimal("0.00")
    mock_credit_pool.rollover_credits = Decimal("0.00")
    mock_credit_pool.monthly_allocation = Decimal("1000.00")
    mock_credit_pool.allocate_monthly_credits.return_value = True

    def query_side_effect(model):
      if model.__name__ == "BillingSubscription":
        mock_q = MagicMock()
        mock_q.filter.return_value.all.return_value = [mock_sub]
        return mock_q
      elif model.__name__ == "OrgUser":
        mock_q = MagicMock()
        mock_q.filter.return_value.first.return_value = mock_owner
        return mock_q
      elif model.__name__ == "UserRepository":
        mock_q = MagicMock()
        mock_q.filter.return_value.first.return_value = mock_user_repo
        return mock_q
      return MagicMock()

    mock_session.query.side_effect = query_side_effect

    with patch(
      "robosystems.tasks.billing.shared_credit_allocation.UserRepositoryCredits.get_user_repository_credits"
    ) as mock_get_credits:
      mock_get_credits.return_value = mock_credit_pool

      result = allocate_monthly_shared_credits()

      assert result["success"] is True
      assert result["credit_pools_synced"] == 1
      assert mock_credit_pool.is_active is True

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  @patch(
    "robosystems.tasks.billing.shared_credit_allocation.deactivate_canceled_subscription_credits"
  )
  def test_skips_allocation_not_yet_due(self, mock_deactivate, mock_get_session):
    """Test skipping allocation when not yet due."""
    from datetime import datetime, timezone, timedelta
    from decimal import Decimal

    mock_session = MagicMock()
    mock_get_session.return_value = mock_session
    mock_deactivate.return_value = 0

    # Mock subscription
    mock_sub = MagicMock()
    mock_sub.id = "sub_123"
    mock_sub.org_id = "org_456"
    mock_sub.resource_id = "sec"
    mock_sub.plan_name = "sec-starter"

    # Mock owner
    mock_owner = MagicMock()
    mock_owner.user_id = "user_789"

    # Mock UserRepository
    mock_user_repo = MagicMock()
    mock_user_repo.id = "repo_111"

    # Mock credit pool - not yet due
    future_date = datetime.now(timezone.utc) + timedelta(days=10)
    mock_credit_pool = MagicMock()
    mock_credit_pool.next_allocation_date = future_date
    mock_credit_pool.current_balance = Decimal("500.00")

    def query_side_effect(model):
      if model.__name__ == "BillingSubscription":
        mock_q = MagicMock()
        mock_q.filter.return_value.all.return_value = [mock_sub]
        return mock_q
      elif model.__name__ == "OrgUser":
        mock_q = MagicMock()
        mock_q.filter.return_value.first.return_value = mock_owner
        return mock_q
      elif model.__name__ == "UserRepository":
        mock_q = MagicMock()
        mock_q.filter.return_value.first.return_value = mock_user_repo
        return mock_q
      return MagicMock()

    mock_session.query.side_effect = query_side_effect

    with patch(
      "robosystems.tasks.billing.shared_credit_allocation.UserRepositoryCredits.get_user_repository_credits"
    ) as mock_get_credits:
      mock_get_credits.return_value = mock_credit_pool

      result = allocate_monthly_shared_credits()

      assert result["success"] is True
      assert result["allocations_performed"] == 0  # Skipped

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  def test_database_connection_error(self, mock_get_session):
    """Test handling of database connection errors."""
    mock_get_session.side_effect = RuntimeError("Connection failed")

    with patch.object(allocate_monthly_shared_credits, "retry") as mock_retry:
      mock_retry.side_effect = RuntimeError("Connection failed")

      with pytest.raises(RuntimeError) as exc_info:
        allocate_monthly_shared_credits.apply(kwargs={}).get()  # type: ignore[misc]

      assert "Connection failed" in str(exc_info.value)

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  @patch(
    "robosystems.tasks.billing.shared_credit_allocation.deactivate_canceled_subscription_credits"
  )
  def test_commit_failure_returns_error(self, mock_deactivate, mock_get_session):
    """Test that commit failures return error status and trigger retry."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session
    mock_deactivate.return_value = 0

    mock_query = MagicMock()
    mock_query.filter.return_value.all.return_value = []
    mock_session.query.return_value = mock_query

    from sqlalchemy.exc import SQLAlchemyError
    from celery.exceptions import Retry

    mock_session.commit.side_effect = SQLAlchemyError("Commit failed")

    with patch.object(allocate_monthly_shared_credits, "retry") as mock_retry:
      mock_retry.return_value = Retry("Retrying due to commit failure")

      with pytest.raises(Retry):
        allocate_monthly_shared_credits.apply(kwargs={}).get()  # type: ignore[misc]

      mock_retry.assert_called_once()


class TestAllocateSharedCreditsForUserTask:
  """Test cases for user-specific credit allocation Celery task."""

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  def test_successful_user_allocation(self, mock_get_session):
    """Test successful credit allocation for a specific user."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_repo = MagicMock()
    mock_repo.repository_name = "sec"
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

    result = allocate_shared_credits_for_user(user_id="user1")

    assert result["success"] is True
    assert result["user_id"] == "user1"
    assert result["allocations_performed"] == 1
    assert len(result["allocation_details"]) == 1
    assert result["allocation_details"][0]["repository_name"] == "sec"

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

    result = allocate_shared_credits_for_user(user_id="user1")

    assert result["success"] is True
    assert result["allocations_performed"] == 0
    assert len(result["allocation_details"]) == 0

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  def test_multiple_pools_for_user(self, mock_get_session):
    """Test allocation for user with multiple credit pools."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    mock_repo1 = MagicMock()
    mock_repo1.repository_name = "sec"
    mock_repo1.repository_type.value = "sec"
    mock_repo1.repository_plan.value = "standard"

    mock_pool1 = MagicMock()
    mock_pool1.id = "pool1"
    mock_pool1.monthly_allocation = 1000
    mock_pool1.current_balance = 1000
    mock_pool1.user_repository = mock_repo1
    mock_pool1.allocate_monthly_credits.return_value = True

    mock_repo2 = MagicMock()
    mock_repo2.repository_name = "industry"
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

    result = allocate_shared_credits_for_user(user_id="user1")

    assert result["success"] is True
    assert result["allocations_performed"] == 2
    assert len(result["allocation_details"]) == 2

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  def test_user_allocation_database_error(self, mock_get_session):
    """Test handling of database errors during user allocation."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    from sqlalchemy.exc import SQLAlchemyError

    mock_query = MagicMock()
    mock_query.join.return_value.filter.return_value.all.side_effect = SQLAlchemyError(
      "Database error"
    )
    mock_session.query.return_value = mock_query

    result = allocate_shared_credits_for_user(user_id="user1")

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

    result = check_credit_allocation_health()

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

    result = check_credit_allocation_health()

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

    result = check_credit_allocation_health()

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

    result = check_credit_allocation_health()

    assert result["healthy"] is False
    assert len(result["warnings"]) == 2

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  def test_health_check_database_error(self, mock_get_session):
    """Test health check handling of database errors."""
    mock_session = MagicMock()
    mock_get_session.return_value = mock_session

    from sqlalchemy.exc import SQLAlchemyError

    mock_session.query.side_effect = SQLAlchemyError("Database error")

    result = check_credit_allocation_health()

    assert result["healthy"] is False
    assert "Database error" in result["error"]

    mock_session.close.assert_called_once()
