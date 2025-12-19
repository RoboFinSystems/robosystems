"""Tests for authentication maintenance and cleanup functions."""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.orm import Session

from robosystems.middleware.auth.maintenance import (
  cleanup_api_keys,
  cleanup_expired_api_keys,
  cleanup_inactive_api_keys,
  cleanup_jwt_cache_expired,
)
from robosystems.models.iam import UserAPIKey


class TestCleanupExpiredAPIKeys:
  """Test suite for cleanup_expired_api_keys function."""

  def test_no_expired_keys(self):
    """Test cleanup when no expired keys exist."""
    mock_session = MagicMock(spec=Session)
    mock_query = mock_session.query.return_value
    mock_filter = mock_query.filter.return_value
    mock_filter.all.return_value = []

    result = cleanup_expired_api_keys(mock_session)

    assert result["expired_sessions_deleted"] == 0
    assert result["expired_user_keys_deactivated"] == 0
    assert result["expired_by_date"] == 0
    # Function now makes only 1 query for expired keys
    assert mock_session.query.call_count == 1

  def test_single_expired_key(self):
    """Test deactivating a single API key that is expired."""
    mock_session = MagicMock(spec=Session)

    # Create a mock key that is expired
    mock_expired_key = MagicMock(spec=UserAPIKey)
    mock_expired_key.id = "expired-key"
    mock_expired_key.expires_at = datetime.now(UTC) - timedelta(days=1)

    mock_query = mock_session.query.return_value
    mock_filter = mock_query.filter.return_value
    mock_filter.all.return_value = [mock_expired_key]

    result = cleanup_expired_api_keys(mock_session)

    assert result["expired_sessions_deleted"] == 0
    assert result["expired_user_keys_deactivated"] == 1
    assert result["expired_by_date"] == 1
    mock_expired_key.deactivate.assert_called_once_with(mock_session)

  def test_multiple_expired_keys(self):
    """Test deactivating multiple expired API keys."""
    mock_session = MagicMock(spec=Session)

    # Create multiple mock keys that are expired
    mock_keys = []
    for i in range(3):
      mock_key = MagicMock(spec=UserAPIKey)
      mock_key.id = f"expired-key-{i}"
      mock_key.expires_at = datetime.now(UTC) - timedelta(days=i + 1)
      mock_keys.append(mock_key)

    mock_query = mock_session.query.return_value
    mock_filter = mock_query.filter.return_value
    mock_filter.all.return_value = mock_keys

    result = cleanup_expired_api_keys(mock_session)

    assert result["expired_sessions_deleted"] == 0
    assert result["expired_user_keys_deactivated"] == 3
    assert result["expired_by_date"] == 3
    for mock_key in mock_keys:
      mock_key.deactivate.assert_called_once_with(mock_session)

  def test_keys_without_expiry_not_affected(self):
    """Test that API keys without expiry dates are not affected."""
    mock_session = MagicMock(spec=Session)

    # The query filters out keys with no expires_at
    mock_query = mock_session.query.return_value
    mock_filter = mock_query.filter.return_value
    mock_filter.all.return_value = []  # Keys without expires_at won't appear in results

    result = cleanup_expired_api_keys(mock_session)

    assert result["expired_user_keys_deactivated"] == 0
    assert result["expired_by_date"] == 0

  def test_exception_handling(self):
    """Test exception handling during cleanup."""
    mock_session = MagicMock(spec=Session)
    mock_session.query.side_effect = Exception("Database error")

    with patch("robosystems.logger.logger") as mock_logger:
      with pytest.raises(Exception, match="Database error"):
        cleanup_expired_api_keys(mock_session)

      mock_logger.error.assert_called_once()
      error_call = mock_logger.error.call_args[0][0]
      assert "Error in cleanup_expired_api_keys" in error_call

  def test_logging_debug_messages(self):
    """Test that appropriate debug messages are logged."""
    mock_session = MagicMock(spec=Session)
    mock_api_key = MagicMock(spec=UserAPIKey)
    mock_api_key.id = "expired-key"
    mock_api_key.expires_at = datetime.now(UTC) - timedelta(days=1)

    mock_query = mock_session.query.return_value
    mock_filter = mock_query.filter.return_value
    mock_filter.all.return_value = [mock_api_key]

    with patch("robosystems.logger.logger") as mock_logger:
      cleanup_expired_api_keys(mock_session)

      # Check debug messages
      debug_calls = [call[0][0] for call in mock_logger.debug.call_args_list]
      assert any("session cleanup" in msg for msg in debug_calls)
      assert any("Cleaning up expired API keys" in msg for msg in debug_calls)
      assert any("Deactivated 1 expired API keys" in msg for msg in debug_calls)


class TestCleanupJWTCacheExpired:
  """Test suite for cleanup_jwt_cache_expired function."""

  @patch("robosystems.middleware.auth.cache.api_key_cache")
  def test_successful_cache_stats_retrieval(self, mock_cache):
    """Test successful retrieval of cache statistics."""
    mock_cache.get_cache_stats.return_value = {
      "cache_counts": {"jwt_tokens": 42, "jwt_blacklisted": 5}
    }

    result = cleanup_jwt_cache_expired()

    assert result["jwt_tokens_cached"] == 42
    assert result["jwt_blacklisted"] == 5
    assert result["cleanup_method"] == "automatic_ttl"
    mock_cache.get_cache_stats.assert_called_once()

  @patch("robosystems.middleware.auth.cache.api_key_cache")
  def test_empty_cache_stats(self, mock_cache):
    """Test handling of empty cache statistics."""
    mock_cache.get_cache_stats.return_value = {}

    result = cleanup_jwt_cache_expired()

    assert result["jwt_tokens_cached"] == 0
    assert result["jwt_blacklisted"] == 0
    assert result["cleanup_method"] == "automatic_ttl"

  @patch("robosystems.middleware.auth.cache.api_key_cache")
  def test_partial_cache_stats(self, mock_cache):
    """Test handling of partial cache statistics."""
    mock_cache.get_cache_stats.return_value = {
      "cache_counts": {
        "jwt_tokens": 10
        # jwt_blacklisted is missing
      }
    }

    result = cleanup_jwt_cache_expired()

    assert result["jwt_tokens_cached"] == 10
    assert result["jwt_blacklisted"] == 0
    assert result["cleanup_method"] == "automatic_ttl"

  @patch("robosystems.middleware.auth.cache.api_key_cache")
  def test_cache_error_handling(self, mock_cache):
    """Test error handling when cache operations fail."""
    mock_cache.get_cache_stats.side_effect = Exception("Redis connection failed")

    with patch("robosystems.logger.logger") as mock_logger:
      result = cleanup_jwt_cache_expired()

      assert result["jwt_tokens_cached"] == 0
      assert result["jwt_blacklisted"] == 0
      assert result["cleanup_method"] == "error"
      assert result["error"] == "Redis connection failed"

      mock_logger.error.assert_called_once()
      error_msg = mock_logger.error.call_args[0][0]
      assert "Error checking JWT cache" in error_msg

  @patch("robosystems.middleware.auth.cache.api_key_cache")
  def test_logging_debug_message(self, mock_cache):
    """Test that debug message is logged."""
    mock_cache.get_cache_stats.return_value = {"cache_counts": {}}

    with patch("robosystems.logger.logger") as mock_logger:
      cleanup_jwt_cache_expired()

      mock_logger.debug.assert_called_once()
      debug_msg = mock_logger.debug.call_args[0][0]
      assert "JWT cache cleanup handled automatically by Valkey TTL" in debug_msg

  @patch("robosystems.middleware.auth.cache.api_key_cache")
  def test_no_exception_raised_on_error(self, mock_cache):
    """Test that function returns error info instead of raising exception."""
    mock_cache.get_cache_stats.side_effect = ValueError("Invalid cache state")

    # Should not raise exception
    result = cleanup_jwt_cache_expired()

    assert result["cleanup_method"] == "error"
    assert "Invalid cache state" in result["error"]
    assert result["jwt_tokens_cached"] == 0
    assert result["jwt_blacklisted"] == 0


class TestBackwardCompatibility:
  """Test suite for backward compatibility functions."""

  def test_cleanup_inactive_api_keys_delegates(self):
    """Test that legacy cleanup_inactive_api_keys delegates to new function."""
    mock_session = MagicMock(spec=Session)

    with patch(
      "robosystems.middleware.auth.maintenance.cleanup_expired_api_keys"
    ) as mock_cleanup:
      mock_cleanup.return_value = {
        "expired_sessions_deleted": 0,
        "expired_user_keys_deactivated": 2,
        "expired_by_date": 2,
      }

      result = cleanup_inactive_api_keys(mock_session)

      assert result["expired_sessions_deleted"] == 0
      assert result["expired_user_keys_deactivated"] == 2
      assert result["expired_by_date"] == 2
      mock_cleanup.assert_called_once_with(mock_session)

  def test_cleanup_api_keys_delegates(self):
    """Test that legacy cleanup_api_keys delegates to new function."""
    mock_session = MagicMock(spec=Session)

    with patch(
      "robosystems.middleware.auth.maintenance.cleanup_expired_api_keys"
    ) as mock_cleanup:
      mock_cleanup.return_value = {
        "expired_sessions_deleted": 0,
        "expired_user_keys_deactivated": 3,
        "expired_by_date": 3,
      }

      result = cleanup_api_keys(mock_session)

      assert result["expired_sessions_deleted"] == 0
      assert result["expired_user_keys_deactivated"] == 3
      assert result["expired_by_date"] == 3
      mock_cleanup.assert_called_once_with(mock_session)


class TestIntegrationScenarios:
  """Test integration scenarios for maintenance functions."""

  def test_full_cleanup_workflow(self):
    """Test a complete cleanup workflow."""
    mock_session = MagicMock(spec=Session)

    # Setup mock API keys with various states
    active_key = MagicMock(spec=UserAPIKey)
    active_key.expires_at = datetime.now(UTC) + timedelta(
      days=30
    )  # Future expiry

    expired_key = MagicMock(spec=UserAPIKey)
    expired_key.id = "expired-key"
    expired_key.expires_at = datetime.now(UTC) - timedelta(
      days=1
    )  # Past expiry

    no_expiry_key = MagicMock(spec=UserAPIKey)
    no_expiry_key.expires_at = None  # No expiry set

    # Only the expired key should be in the query results
    mock_query = mock_session.query.return_value
    mock_filter = mock_query.filter.return_value
    mock_filter.all.return_value = [expired_key]

    result = cleanup_expired_api_keys(mock_session)

    assert result["expired_user_keys_deactivated"] == 1
    assert result["expired_by_date"] == 1
    expired_key.deactivate.assert_called_once()

  @patch("robosystems.middleware.auth.cache.api_key_cache")
  def test_combined_maintenance_operations(self, mock_cache):
    """Test running both API key and JWT cache cleanup."""
    mock_session = MagicMock(spec=Session)

    # Mock query to return empty results
    mock_query = mock_session.query.return_value
    mock_filter = mock_query.filter.return_value
    mock_filter.all.return_value = []

    mock_cache.get_cache_stats.return_value = {
      "cache_counts": {"jwt_tokens": 15, "jwt_blacklisted": 3}
    }

    # Run both cleanup operations
    api_key_result = cleanup_expired_api_keys(mock_session)
    jwt_result = cleanup_jwt_cache_expired()

    # Verify both ran successfully
    assert api_key_result["expired_user_keys_deactivated"] == 0
    assert api_key_result["expired_by_date"] == 0
    assert jwt_result["jwt_tokens_cached"] == 15
    assert jwt_result["jwt_blacklisted"] == 3
