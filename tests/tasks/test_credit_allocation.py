"""Tests for credit allocation tasks."""

import pytest
from unittest.mock import Mock, MagicMock, patch

from robosystems.tasks.billing.credit_allocation import (
  allocate_monthly_graph_credits,
)
from robosystems.tasks.billing.shared_credit_allocation import (
  allocate_monthly_shared_credits,
)


class TestGraphCreditAllocation:
  """Test cases for graph credit allocation tasks."""

  @patch("robosystems.tasks.billing.credit_allocation.get_celery_db_session")
  @patch("robosystems.tasks.billing.credit_allocation.CreditService")
  def test_allocate_monthly_graph_credits(self, mock_credit_service_class, mock_get_db):
    """Test monthly graph credit allocation task."""
    # Setup mocks
    mock_db = MagicMock()
    mock_get_db.return_value = mock_db
    mock_credit_service = MagicMock()
    mock_credit_service_class.return_value = mock_credit_service

    # Mock bulk allocation result
    mock_credit_service.bulk_allocate_monthly_credits.return_value = {
      "allocated_count": 2,
      "total_credits_allocated": 3000.0,
      "failed_count": 1,
      "errors": ["Graph graph3 not found"],
    }

    # Run allocation
    result = allocate_monthly_graph_credits()

    # Verify results
    assert result["allocated_count"] == 2
    assert result["total_credits_allocated"] == 3000.0
    assert result["failed_count"] == 1
    assert len(result["errors"]) == 1

    # Verify service was called
    mock_credit_service.bulk_allocate_monthly_credits.assert_called_once()


class TestSharedCreditAllocation:
  """Test cases for shared repository credit allocation tasks."""

  @patch("robosystems.tasks.billing.shared_credit_allocation.get_celery_db_session")
  def test_allocate_monthly_shared_credits(self, mock_get_db):
    """Test monthly shared repository credit allocation."""
    # Setup mocks - direct session mock
    mock_db = MagicMock()
    mock_get_db.return_value = mock_db

    # Mock credit pools due for allocation
    credit_pools = [
      Mock(
        id="pool1",
        user_id="user1",
        repository_type="sec",
        monthly_allocation=100,
        current_balance=50,
        credit_balance=50,
        max_rollover=500,
        rollover_credits=0,
        addon=Mock(
          id="addon1",
          user_id="user1",
          addon_type="sec",
          addon_tier="standard",
          tier="standard",
          user=Mock(id="user1", email="user1@example.com"),
        ),
        allocate_monthly_credits=Mock(return_value=True),
      ),
      Mock(
        id="pool2",
        user_id="user2",
        repository_type="industry",
        monthly_allocation=200,
        current_balance=100,
        credit_balance=100,
        max_rollover=1000,
        rollover_credits=0,
        addon=Mock(
          id="addon2",
          user_id="user2",
          addon_type="industry",
          addon_tier="standard",
          tier="standard",
          user=Mock(id="user2", email="user2@example.com"),
        ),
        allocate_monthly_credits=Mock(return_value=True),
      ),
    ]

    # Mock query
    mock_db.query().filter().all.return_value = credit_pools

    # Run allocation
    result = allocate_monthly_shared_credits()

    # Verify results
    assert result["allocations_performed"] == 2
    assert result["total_credits_allocated"] == 300
    assert result["success"] is True

    # Verify each pool's allocate method was called
    for pool in credit_pools:
      pool.allocate_monthly_credits.assert_called_once()


class TestCreditAllocationIntegration:
  """Integration tests for credit allocation system."""

  @pytest.mark.integration
  @patch("robosystems.tasks.billing.credit_allocation.get_celery_db_session")
  @patch("robosystems.tasks.billing.credit_allocation.CreditService")
  def test_graph_allocation_error_handling(self, mock_service_class, mock_get_db):
    """Test error handling in graph credit allocation."""
    # Setup mocks
    mock_db = MagicMock()
    mock_get_db.return_value = mock_db
    mock_service = MagicMock()
    mock_service_class.return_value = mock_service

    # Mock allocation to raise an exception
    mock_service.bulk_allocate_monthly_credits.side_effect = Exception("Database error")

    # Run allocation - should raise the exception after logging
    with patch("robosystems.tasks.billing.credit_allocation.logger") as mock_logger:
      with pytest.raises(Exception) as exc_info:
        allocate_monthly_graph_credits()

      # Verify the exception message
      assert str(exc_info.value) == "Database error"

      # Verify error was logged
      mock_logger.error.assert_called()
      assert "Failed to allocate graph credits" in str(mock_logger.error.call_args)
