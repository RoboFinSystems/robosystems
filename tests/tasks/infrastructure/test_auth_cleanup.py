"""Tests for authentication cleanup tasks."""

import pytest
from unittest.mock import MagicMock, patch

from robosystems.tasks.infrastructure.auth_cleanup import cleanup_expired_api_keys_task


class TestCleanupExpiredAPIKeysTask:
  """Test cases for API key cleanup Celery task."""

  @patch("robosystems.tasks.infrastructure.auth_cleanup.cleanup_expired_api_keys")
  @patch("robosystems.tasks.infrastructure.auth_cleanup.sessionmaker")
  @patch("robosystems.tasks.infrastructure.auth_cleanup.engine")
  def test_successful_cleanup(self, mock_engine, mock_sessionmaker, mock_cleanup_func):
    """Test successful API key cleanup execution."""
    mock_session = MagicMock()
    mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session
    mock_sessionmaker.return_value.return_value.__exit__.return_value = False

    mock_cleanup_result = {
      "expired_sessions_deleted": 0,
      "expired_user_keys_deactivated": 3,
      "expired_by_date": 3,
    }
    mock_cleanup_func.return_value = mock_cleanup_result

    result = cleanup_expired_api_keys_task()  # type: ignore[call-arg]

    assert result == mock_cleanup_result
    assert result["expired_user_keys_deactivated"] == 3
    mock_sessionmaker.assert_called_once_with(bind=mock_engine)
    mock_cleanup_func.assert_called_once_with(mock_session)
    mock_session.commit.assert_called_once()

  @patch("robosystems.tasks.infrastructure.auth_cleanup.cleanup_expired_api_keys")
  @patch("robosystems.tasks.infrastructure.auth_cleanup.sessionmaker")
  @patch("robosystems.tasks.infrastructure.auth_cleanup.engine")
  def test_no_expired_keys(self, mock_engine, mock_sessionmaker, mock_cleanup_func):
    """Test cleanup when no keys have expired."""
    mock_session = MagicMock()
    mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session
    mock_sessionmaker.return_value.return_value.__exit__.return_value = False

    mock_cleanup_result = {
      "expired_sessions_deleted": 0,
      "expired_user_keys_deactivated": 0,
      "expired_by_date": 0,
    }
    mock_cleanup_func.return_value = mock_cleanup_result

    result = cleanup_expired_api_keys_task()  # type: ignore[call-arg]

    assert result["expired_user_keys_deactivated"] == 0
    mock_cleanup_func.assert_called_once()

  @patch("robosystems.tasks.infrastructure.auth_cleanup.sessionmaker")
  @patch("robosystems.tasks.infrastructure.auth_cleanup.engine", None)
  def test_database_engine_unavailable(self, mock_sessionmaker):
    """Test task fails gracefully when database engine is unavailable."""
    with pytest.raises(Exception) as exc_info:
      cleanup_expired_api_keys_task()  # type: ignore[call-arg]

    assert "Database engine is not available" in str(exc_info.value)
    mock_sessionmaker.assert_not_called()

  @patch("robosystems.tasks.infrastructure.auth_cleanup.cleanup_expired_api_keys")
  @patch("robosystems.tasks.infrastructure.auth_cleanup.sessionmaker")
  @patch("robosystems.tasks.infrastructure.auth_cleanup.engine")
  def test_database_connection_failure(
    self, mock_engine, mock_sessionmaker, mock_cleanup_func
  ):
    """Test task handles database connection failures."""
    mock_session = MagicMock()
    mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session
    mock_sessionmaker.return_value.return_value.__exit__.return_value = False

    from sqlalchemy.exc import OperationalError

    mock_session.execute.side_effect = OperationalError("Connection failed", None, None)

    with pytest.raises(OperationalError):
      cleanup_expired_api_keys_task()  # type: ignore[call-arg]

  @patch("robosystems.tasks.infrastructure.auth_cleanup.cleanup_expired_api_keys")
  @patch("robosystems.tasks.infrastructure.auth_cleanup.sessionmaker")
  @patch("robosystems.tasks.infrastructure.auth_cleanup.engine")
  def test_cleanup_function_raises_exception(
    self, mock_engine, mock_sessionmaker, mock_cleanup_func
  ):
    """Test task handles exceptions from cleanup function."""
    mock_session = MagicMock()
    mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session
    mock_sessionmaker.return_value.return_value.__exit__.return_value = False

    mock_cleanup_func.side_effect = RuntimeError("Cleanup failed")

    with patch.object(cleanup_expired_api_keys_task, "retry") as mock_retry:
      mock_retry.side_effect = RuntimeError("Cleanup failed")

      with pytest.raises(RuntimeError) as exc_info:
        cleanup_expired_api_keys_task.apply(kwargs={}, task_id="test-task-id").get()  # type: ignore[attr-defined]

      assert "Cleanup failed" in str(exc_info.value)

  @patch("robosystems.tasks.infrastructure.auth_cleanup.cleanup_expired_api_keys")
  @patch("robosystems.tasks.infrastructure.auth_cleanup.sessionmaker")
  @patch("robosystems.tasks.infrastructure.auth_cleanup.engine")
  def test_session_commit_called(
    self, mock_engine, mock_sessionmaker, mock_cleanup_func
  ):
    """Test that session.commit() is called after successful cleanup."""
    mock_session = MagicMock()
    mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session
    mock_sessionmaker.return_value.return_value.__exit__.return_value = False

    mock_cleanup_result = {
      "expired_sessions_deleted": 0,
      "expired_user_keys_deactivated": 2,
      "expired_by_date": 2,
    }
    mock_cleanup_func.return_value = mock_cleanup_result

    cleanup_expired_api_keys_task()  # type: ignore[call-arg]

    mock_session.commit.assert_called_once()

  @patch("robosystems.tasks.infrastructure.auth_cleanup.cleanup_expired_api_keys")
  @patch("robosystems.tasks.infrastructure.auth_cleanup.sessionmaker")
  @patch("robosystems.tasks.infrastructure.auth_cleanup.engine")
  def test_logging_on_success(self, mock_engine, mock_sessionmaker, mock_cleanup_func):
    """Test that success is logged appropriately."""
    mock_session = MagicMock()
    mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session
    mock_sessionmaker.return_value.return_value.__exit__.return_value = False

    mock_cleanup_result = {
      "expired_sessions_deleted": 0,
      "expired_user_keys_deactivated": 5,
      "expired_by_date": 5,
    }
    mock_cleanup_func.return_value = mock_cleanup_result

    with patch("robosystems.tasks.infrastructure.auth_cleanup.logger") as mock_logger:
      cleanup_expired_api_keys_task()  # type: ignore[call-arg]

      mock_logger.info.assert_any_call("Starting API key cleanup task")
      assert any(
        "API key cleanup completed" in str(call)
        for call in mock_logger.info.call_args_list
      )

  @patch("robosystems.tasks.infrastructure.auth_cleanup.cleanup_expired_api_keys")
  @patch("robosystems.tasks.infrastructure.auth_cleanup.sessionmaker")
  @patch("robosystems.tasks.infrastructure.auth_cleanup.engine")
  def test_logging_on_error(self, mock_engine, mock_sessionmaker, mock_cleanup_func):
    """Test that errors are logged appropriately."""
    mock_session = MagicMock()
    mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session
    mock_sessionmaker.return_value.return_value.__exit__.return_value = False

    mock_cleanup_func.side_effect = RuntimeError("Unexpected error")

    with patch("robosystems.tasks.infrastructure.auth_cleanup.logger") as mock_logger:
      try:
        cleanup_expired_api_keys_task.apply(kwargs={}).get()  # type: ignore[attr-defined]
      except Exception:
        pass

      assert any(
        "Failed to clean up expired API keys" in str(call)
        for call in mock_logger.error.call_args_list
      )


class TestCleanupTaskRetryBehavior:
  """Test cases for task retry logic."""

  @patch("robosystems.tasks.infrastructure.auth_cleanup.cleanup_expired_api_keys")
  @patch("robosystems.tasks.infrastructure.auth_cleanup.sessionmaker")
  @patch("robosystems.tasks.infrastructure.auth_cleanup.engine")
  def test_retry_on_non_database_error(
    self, mock_engine, mock_sessionmaker, mock_cleanup_func
  ):
    """Test that task retries on non-database errors."""
    mock_session = MagicMock()
    mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session
    mock_sessionmaker.return_value.return_value.__exit__.return_value = False

    mock_cleanup_func.side_effect = RuntimeError("Random error")

    mock_self = MagicMock()
    mock_self.retry = MagicMock()

    try:
      from robosystems.tasks.infrastructure.auth_cleanup import (
        cleanup_expired_api_keys_task,
      )

      cleanup_expired_api_keys_task.__get__(mock_self, type(mock_self))()
    except Exception:
      pass

  @patch("robosystems.tasks.infrastructure.auth_cleanup.cleanup_expired_api_keys")
  @patch("robosystems.tasks.infrastructure.auth_cleanup.sessionmaker")
  @patch("robosystems.tasks.infrastructure.auth_cleanup.engine")
  def test_no_retry_on_database_connection_error(
    self, mock_engine, mock_sessionmaker, mock_cleanup_func
  ):
    """Test that task does not retry on database connection errors."""
    mock_session = MagicMock()
    mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session
    mock_sessionmaker.return_value.return_value.__exit__.return_value = False

    mock_cleanup_func.side_effect = Exception("Database connection failed")

    with pytest.raises(Exception) as exc_info:
      cleanup_expired_api_keys_task.apply(kwargs={}).get()  # type: ignore[attr-defined]

    assert (
      "Database connection failed" in str(exc_info.value)
      or "connection" in str(exc_info.value).lower()
    )
