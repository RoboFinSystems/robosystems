"""Tests for user limits service."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from sqlalchemy.orm import Session

from robosystems.operations.user_limits_service import UserLimitsService
from robosystems.models.iam.user_usage_tracking import UsageType


class TestUserLimitsService:
  """Test suite for UserLimitsService class."""

  def setup_method(self):
    """Set up test fixtures."""
    self.mock_session = MagicMock(spec=Session)
    self.service = UserLimitsService(self.mock_session)
    self.user_id = "test-user-123"

  def test_initialization(self):
    """Test service initialization."""
    assert self.service.session == self.mock_session


class TestCheckAndEnforceLimits:
  """Test suite for check_and_enforce_limits method."""

  def setup_method(self):
    """Set up test fixtures."""
    self.mock_session = MagicMock(spec=Session)
    self.service = UserLimitsService(self.mock_session)
    self.user_id = "test-user-123"

  @patch("robosystems.operations.user_limits_service.UserLimits")
  def test_create_user_graph_action(self, mock_user_limits_cls):
    """Test checking limits for create_user_graph action."""
    mock_user_limits = MagicMock()
    mock_user_limits.can_create_user_graph.return_value = (True, "Can create graph")
    mock_user_limits_cls.get_or_create_for_user.return_value = mock_user_limits

    result = self.service.check_and_enforce_limits(self.user_id, "create_user_graph")

    assert result == (True, "Can create graph")
    mock_user_limits_cls.get_or_create_for_user.assert_called_once_with(
      self.user_id, self.mock_session
    )
    mock_user_limits.can_create_user_graph.assert_called_once_with(self.mock_session)

  @patch("robosystems.operations.user_limits_service.UserLimits")
  def test_create_user_graph_limit_exceeded(self, mock_user_limits_cls):
    """Test when user graph creation limit is exceeded."""
    mock_user_limits = MagicMock()
    mock_user_limits.can_create_user_graph.return_value = (False, "Limit exceeded")
    mock_user_limits_cls.get_or_create_for_user.return_value = mock_user_limits

    result = self.service.check_and_enforce_limits(self.user_id, "create_user_graph")

    assert result == (False, "Limit exceeded")

  @patch("robosystems.operations.user_limits_service.UserLimits")
  def test_other_action_types(self, mock_user_limits_cls):
    """Test that other action types are handled by middleware."""
    mock_user_limits = MagicMock()
    mock_user_limits_cls.get_or_create_for_user.return_value = mock_user_limits

    # Test various other action types
    for action_type in ["api_call", "sec_import", "query_graph", "backup_graph"]:
      result = self.service.check_and_enforce_limits(self.user_id, action_type)

      assert result == (True, "Rate limiting handled by middleware")

    # UserLimits should still be fetched but no specific method called
    assert mock_user_limits_cls.get_or_create_for_user.call_count == 4

  @patch("robosystems.operations.user_limits_service.logger")
  @patch("robosystems.operations.user_limits_service.UserLimits")
  def test_logging(self, mock_user_limits_cls, mock_logger):
    """Test that appropriate logs are generated."""
    mock_user_limits = MagicMock()
    mock_user_limits_cls.get_or_create_for_user.return_value = mock_user_limits

    self.service.check_and_enforce_limits(self.user_id, "other_action")

    mock_logger.info.assert_called_once()
    info_msg = mock_logger.info.call_args[0][0]
    assert self.user_id in info_msg
    assert "other_action" in info_msg

    mock_logger.debug.assert_called_once()
    debug_msg = mock_logger.debug.call_args[0][0]
    assert "middleware" in debug_msg


class TestRecordAPICall:
  """Test suite for record_api_call method."""

  def setup_method(self):
    """Set up test fixtures."""
    self.mock_session = MagicMock(spec=Session)
    self.service = UserLimitsService(self.mock_session)
    self.user_id = "test-user-123"

  @patch("robosystems.operations.user_limits_service.UserUsageTracking")
  def test_record_api_call_basic(self, mock_tracking_cls):
    """Test recording a basic API call."""
    self.service.record_api_call(self.user_id)

    mock_tracking_cls.record_usage.assert_called_once_with(
      user_id=self.user_id,
      usage_type=UsageType.API_CALL,
      session=self.mock_session,
      endpoint=None,
      graph_id=None,
      auto_commit=False,
    )

  @patch("robosystems.operations.user_limits_service.UserUsageTracking")
  def test_record_api_call_with_endpoint(self, mock_tracking_cls):
    """Test recording an API call with endpoint."""
    self.service.record_api_call(self.user_id, endpoint="/v1/graphs/graph/query")

    mock_tracking_cls.record_usage.assert_called_once_with(
      user_id=self.user_id,
      usage_type=UsageType.API_CALL,
      session=self.mock_session,
      endpoint="/v1/graphs/graph/query",
      graph_id=None,
      auto_commit=False,
    )

  @patch("robosystems.operations.user_limits_service.UserUsageTracking")
  def test_record_api_call_with_graph_id(self, mock_tracking_cls):
    """Test recording an API call with graph ID."""
    self.service.record_api_call(self.user_id, graph_id="kg123456")

    mock_tracking_cls.record_usage.assert_called_once_with(
      user_id=self.user_id,
      usage_type=UsageType.API_CALL,
      session=self.mock_session,
      endpoint=None,
      graph_id="kg123456",
      auto_commit=False,
    )

  @patch("robosystems.operations.user_limits_service.UserUsageTracking")
  def test_record_api_call_with_all_params(self, mock_tracking_cls):
    """Test recording an API call with all parameters."""
    self.service.record_api_call(
      self.user_id, endpoint="/v1/graphs/kg123456/query", graph_id="kg123456"
    )

    mock_tracking_cls.record_usage.assert_called_once_with(
      user_id=self.user_id,
      usage_type=UsageType.API_CALL,
      session=self.mock_session,
      endpoint="/v1/graphs/kg123456/query",
      graph_id="kg123456",
      auto_commit=False,
    )

  @patch("robosystems.operations.user_limits_service.logger")
  @patch("robosystems.operations.user_limits_service.UserUsageTracking")
  def test_record_api_call_error_handling(self, mock_tracking_cls, mock_logger):
    """Test error handling in record_api_call."""
    mock_tracking_cls.record_usage.side_effect = Exception("Database error")

    # Should not raise exception
    self.service.record_api_call(self.user_id, endpoint="/v1/test")

    mock_logger.error.assert_called_once()
    error_msg = mock_logger.error.call_args[0][0]
    assert "Failed to record API call" in error_msg
    assert self.user_id in error_msg


class TestRecordSecImport:
  """Test suite for record_sec_import method."""

  def setup_method(self):
    """Set up test fixtures."""
    self.mock_session = MagicMock(spec=Session)
    self.service = UserLimitsService(self.mock_session)
    self.user_id = "test-user-123"

  @patch("robosystems.operations.user_limits_service.UserUsageTracking")
  def test_record_sec_import_basic(self, mock_tracking_cls):
    """Test recording a basic SEC import."""
    self.service.record_sec_import(self.user_id)

    mock_tracking_cls.record_usage.assert_called_once_with(
      user_id=self.user_id,
      usage_type=UsageType.SEC_IMPORT,
      session=self.mock_session,
      graph_id=None,
      resource_count=1,
      auto_commit=False,
    )

  @patch("robosystems.operations.user_limits_service.UserUsageTracking")
  def test_record_sec_import_with_graph_id(self, mock_tracking_cls):
    """Test recording SEC import with graph ID."""
    self.service.record_sec_import(self.user_id, graph_id="sec")

    mock_tracking_cls.record_usage.assert_called_once_with(
      user_id=self.user_id,
      usage_type=UsageType.SEC_IMPORT,
      session=self.mock_session,
      graph_id="sec",
      resource_count=1,
      auto_commit=False,
    )

  @patch("robosystems.operations.user_limits_service.UserUsageTracking")
  def test_record_sec_import_with_resource_count(self, mock_tracking_cls):
    """Test recording SEC import with multiple resources."""
    self.service.record_sec_import(self.user_id, resource_count=10)

    mock_tracking_cls.record_usage.assert_called_once_with(
      user_id=self.user_id,
      usage_type=UsageType.SEC_IMPORT,
      session=self.mock_session,
      graph_id=None,
      resource_count=10,
      auto_commit=False,
    )

  @patch("robosystems.operations.user_limits_service.logger")
  @patch("robosystems.operations.user_limits_service.UserUsageTracking")
  def test_record_sec_import_error_handling(self, mock_tracking_cls, mock_logger):
    """Test error handling in record_sec_import."""
    mock_tracking_cls.record_usage.side_effect = Exception("Database error")

    # Should not raise exception
    self.service.record_sec_import(self.user_id, resource_count=5)

    mock_logger.error.assert_called_once()
    error_msg = mock_logger.error.call_args[0][0]
    assert "Failed to record SEC import" in error_msg
    assert self.user_id in error_msg


class TestGetUserUsageStats:
  """Test suite for get_user_usage_stats method."""

  def setup_method(self):
    """Set up test fixtures."""
    self.mock_session = MagicMock(spec=Session)
    self.service = UserLimitsService(self.mock_session)
    self.user_id = "test-user-123"

  @patch("robosystems.operations.user_limits_service.UserUsageTracking")
  @patch("robosystems.operations.user_limits_service.UserLimits")
  def test_get_user_usage_stats_complete(self, mock_user_limits_cls, mock_tracking_cls):
    """Test getting complete usage statistics."""
    mock_user_limits = MagicMock()
    mock_user_limits.get_current_usage.return_value = {
      "user_graphs": 3,
      "max_user_graphs": 10,
      "api_calls_today": 150,
    }
    mock_user_limits_cls.get_or_create_for_user.return_value = mock_user_limits

    mock_tracking_cls.get_user_usage_stats.return_value = {
      "total_api_calls": 1500,
      "total_sec_imports": 25,
      "last_activity": "2024-01-01T12:00:00Z",
    }

    result = self.service.get_user_usage_stats(self.user_id)

    assert result["user_graphs"] == 3
    assert result["max_user_graphs"] == 10
    assert result["api_calls_today"] == 150
    assert result["usage_tracking"]["total_api_calls"] == 1500
    assert result["usage_tracking"]["total_sec_imports"] == 25

    mock_user_limits_cls.get_or_create_for_user.assert_called_once_with(
      self.user_id, self.mock_session
    )
    mock_user_limits.get_current_usage.assert_called_once_with(self.mock_session)
    mock_tracking_cls.get_user_usage_stats.assert_called_once_with(
      self.user_id, self.mock_session
    )

  @patch("robosystems.operations.user_limits_service.logger")
  @patch("robosystems.operations.user_limits_service.UserUsageTracking")
  @patch("robosystems.operations.user_limits_service.UserLimits")
  def test_get_user_usage_stats_logging(
    self, mock_user_limits_cls, mock_tracking_cls, mock_logger
  ):
    """Test logging in get_user_usage_stats."""
    mock_user_limits = MagicMock()
    mock_user_limits.get_current_usage.return_value = {}
    mock_user_limits_cls.get_or_create_for_user.return_value = mock_user_limits
    mock_tracking_cls.get_user_usage_stats.return_value = {}

    self.service.get_user_usage_stats(self.user_id)

    mock_logger.info.assert_called_once()
    info_msg = mock_logger.info.call_args[0][0]
    assert "Getting usage stats" in info_msg
    assert self.user_id in info_msg


class TestUpdateUserLimits:
  """Test suite for update_user_limits method."""

  def setup_method(self):
    """Set up test fixtures."""
    self.mock_session = MagicMock(spec=Session)
    self.service = UserLimitsService(self.mock_session)
    self.user_id = "test-user-123"

  @patch("robosystems.operations.user_limits_service.datetime")
  @patch("robosystems.operations.user_limits_service.UserLimits")
  def test_update_user_limits_success(self, mock_user_limits_cls, mock_datetime):
    """Test successful update of user limits."""
    mock_now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    mock_datetime.now.return_value = mock_now

    mock_user_limits = MagicMock()
    mock_user_limits_cls.get_or_create_for_user.return_value = mock_user_limits

    self.service.update_user_limits(self.user_id, max_user_graphs=20)

    # Verify limits were updated
    assert mock_user_limits.max_user_graphs == 20
    assert mock_user_limits.updated_at == mock_now

    # Verify database commit
    self.mock_session.commit.assert_called_once()

    # Verify get_or_create was called
    mock_user_limits_cls.get_or_create_for_user.assert_called_once_with(
      self.user_id, self.mock_session
    )

  @patch("robosystems.operations.user_limits_service.logger")
  @patch("robosystems.operations.user_limits_service.UserLimits")
  def test_update_user_limits_logging(self, mock_user_limits_cls, mock_logger):
    """Test logging in update_user_limits."""
    mock_user_limits = MagicMock()
    mock_user_limits_cls.get_or_create_for_user.return_value = mock_user_limits

    self.service.update_user_limits(self.user_id, max_user_graphs=15)

    # Check info logs
    assert mock_logger.info.call_count == 2
    first_log = mock_logger.info.call_args_list[0][0][0]
    second_log = mock_logger.info.call_args_list[1][0][0]

    assert "Updating max_user_graphs" in first_log
    assert self.user_id in first_log
    assert "15" in first_log

    assert "Successfully updated" in second_log
    assert self.user_id in second_log

  @patch("robosystems.operations.user_limits_service.UserLimits")
  def test_update_user_limits_zero_value(self, mock_user_limits_cls):
    """Test updating limits to zero (disable graph creation)."""
    mock_user_limits = MagicMock()
    mock_user_limits_cls.get_or_create_for_user.return_value = mock_user_limits

    self.service.update_user_limits(self.user_id, max_user_graphs=0)

    assert mock_user_limits.max_user_graphs == 0
    self.mock_session.commit.assert_called_once()


class TestIntegrationScenarios:
  """Test integration scenarios for UserLimitsService."""

  def setup_method(self):
    """Set up test fixtures."""
    self.mock_session = MagicMock(spec=Session)
    self.service = UserLimitsService(self.mock_session)
    self.user_id = "test-user-123"

  @patch("robosystems.operations.user_limits_service.UserUsageTracking")
  @patch("robosystems.operations.user_limits_service.UserLimits")
  def test_full_user_workflow(self, mock_user_limits_cls, mock_tracking_cls):
    """Test a complete user workflow."""
    # Setup
    mock_user_limits = MagicMock()
    mock_user_limits.can_create_user_graph.return_value = (True, "Can create")
    mock_user_limits.get_current_usage.return_value = {"graphs": 2}
    mock_user_limits_cls.get_or_create_for_user.return_value = mock_user_limits

    mock_tracking_cls.get_user_usage_stats.return_value = {"api_calls": 100}

    # 1. Check if user can create graph
    can_create, msg = self.service.check_and_enforce_limits(
      self.user_id, "create_user_graph"
    )
    assert can_create is True

    # 2. Record some API calls
    self.service.record_api_call(self.user_id, "/v1/graphs/graph/query")
    self.service.record_api_call(self.user_id, "/v1/graphs/graph/backup")

    # 3. Record SEC import
    self.service.record_sec_import(self.user_id, resource_count=5)

    # 4. Get usage stats
    stats = self.service.get_user_usage_stats(self.user_id)
    assert stats["graphs"] == 2
    assert stats["usage_tracking"]["api_calls"] == 100

    # 5. Update limits
    self.service.update_user_limits(self.user_id, max_user_graphs=5)

    # Verify all interactions
    assert mock_tracking_cls.record_usage.call_count == 3
    assert self.mock_session.commit.call_count == 1
