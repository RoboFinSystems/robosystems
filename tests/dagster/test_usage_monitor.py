"""Tests for graph usage monitor sensor."""

from unittest.mock import AsyncMock, MagicMock, patch

from dagster import build_sensor_context

from robosystems.dagster.sensors.usage_monitor import (
  _ALERT_DEDUP_TTL_SECONDS,
  graph_usage_monitor_sensor,
)


def _make_graph(graph_id="kg_test123", tier="ladybug-standard"):
  """Create a mock Graph object."""
  graph = MagicMock()
  graph.graph_id = graph_id
  graph.graph_tier = tier
  graph.is_repository = False
  graph.parent_graph_id = None
  graph.status = "active"
  return graph


class TestGraphUsageMonitorSensor:
  """Tests for graph usage monitor sensor."""

  @patch("robosystems.database.session")
  def test_skips_when_no_graphs(self, mock_session_factory):
    """Test sensor skips when no active user graphs exist."""
    mock_db = MagicMock()
    mock_session_factory.return_value = mock_db
    mock_db.query.return_value.filter.return_value.all.return_value = []

    context = build_sensor_context()
    result = graph_usage_monitor_sensor(context)

    assert result is not None  # Returns SkipReason
    mock_db.close.assert_called_once()

  @patch("robosystems.operations.aws.ses.ses_service")
  @patch("robosystems.config.valkey_registry.create_redis_client")
  @patch(
    "robosystems.middleware.graph.ingestion_limits.IngestionLimitChecker.check_instance_storage"
  )
  @patch("robosystems.database.session")
  def test_sends_alert_on_approaching(
    self,
    mock_session_factory,
    mock_check_storage,
    mock_redis_factory,
    mock_ses,
  ):
    """Test sensor sends email when storage is approaching limit."""
    mock_db = MagicMock()
    mock_session_factory.return_value = mock_db

    graph = _make_graph()
    mock_db.query.return_value.filter.return_value.all.return_value = [graph]

    # Graph user and user lookups
    mock_graph_user = MagicMock()
    mock_graph_user.user_id = "user_123"
    mock_graph_user.role = "admin"
    mock_user = MagicMock()
    mock_user.email = "user@example.com"
    mock_user.name = "Test User"
    mock_db.query.return_value.filter.return_value.first.side_effect = [
      mock_graph_user,
      mock_user,
    ]

    # Storage check returns approaching
    mock_check_storage.return_value = {
      "total_storage_gb": 17.0,
      "limit_gb": 20.0,
      "usage_percentage": 85.0,
      "status": "approaching",
      "databases": [{"graph_id": "kg_test123", "is_parent": True, "size_mb": 17000}],
    }

    # Valkey: no existing alert
    mock_redis = MagicMock()
    mock_redis_factory.return_value = mock_redis
    mock_redis.exists.return_value = False

    # SES: success
    mock_ses.send_capacity_warning_email = AsyncMock(return_value=True)

    context = build_sensor_context()
    graph_usage_monitor_sensor(context)

    # Verify dedup key was set
    mock_redis.setex.assert_called_once()
    dedup_call = mock_redis.setex.call_args
    assert dedup_call[0][0] == "usage_alert:kg_test123:approaching"
    assert dedup_call[0][1] == _ALERT_DEDUP_TTL_SECONDS

    mock_db.close.assert_called_once()

  @patch("robosystems.config.valkey_registry.create_redis_client")
  @patch(
    "robosystems.middleware.graph.ingestion_limits.IngestionLimitChecker.check_instance_storage"
  )
  @patch("robosystems.database.session")
  def test_skips_already_alerted(
    self,
    mock_session_factory,
    mock_check_storage,
    mock_redis_factory,
  ):
    """Test sensor skips graphs that were already alerted."""
    mock_db = MagicMock()
    mock_session_factory.return_value = mock_db
    mock_db.query.return_value.filter.return_value.all.return_value = [_make_graph()]

    mock_check_storage.return_value = {
      "total_storage_gb": 17.0,
      "limit_gb": 20.0,
      "usage_percentage": 85.0,
      "status": "approaching",
      "databases": [],
    }

    # Valkey: already alerted
    mock_redis = MagicMock()
    mock_redis_factory.return_value = mock_redis
    mock_redis.exists.return_value = True

    context = build_sensor_context()
    graph_usage_monitor_sensor(context)

    # No email sent — dedup key exists
    mock_redis.setex.assert_not_called()
    mock_db.close.assert_called_once()

  @patch("robosystems.config.valkey_registry.create_redis_client")
  @patch(
    "robosystems.middleware.graph.ingestion_limits.IngestionLimitChecker.check_instance_storage"
  )
  @patch("robosystems.database.session")
  def test_no_alert_when_healthy(
    self,
    mock_session_factory,
    mock_check_storage,
    mock_redis_factory,
  ):
    """Test sensor does nothing for healthy graphs."""
    mock_db = MagicMock()
    mock_session_factory.return_value = mock_db
    mock_db.query.return_value.filter.return_value.all.return_value = [_make_graph()]

    mock_check_storage.return_value = {
      "total_storage_gb": 5.0,
      "limit_gb": 20.0,
      "usage_percentage": 25.0,
      "status": "healthy",
      "databases": [],
    }

    mock_redis = MagicMock()
    mock_redis_factory.return_value = mock_redis

    context = build_sensor_context()
    graph_usage_monitor_sensor(context)

    # No dedup check needed for healthy graphs
    mock_redis.exists.assert_not_called()
    mock_db.close.assert_called_once()

  @patch("robosystems.config.valkey_registry.create_redis_client")
  @patch(
    "robosystems.middleware.graph.ingestion_limits.IngestionLimitChecker.check_instance_storage"
  )
  @patch("robosystems.database.session")
  def test_handles_storage_check_failure(
    self,
    mock_session_factory,
    mock_check_storage,
    mock_redis_factory,
  ):
    """Test sensor handles storage check failures gracefully."""
    mock_db = MagicMock()
    mock_session_factory.return_value = mock_db
    mock_db.query.return_value.filter.return_value.all.return_value = [_make_graph()]

    # Storage check fails
    mock_check_storage.side_effect = Exception("Connection refused")

    mock_redis = MagicMock()
    mock_redis_factory.return_value = mock_redis

    context = build_sensor_context()
    # Should not raise
    graph_usage_monitor_sensor(context)

    mock_db.close.assert_called_once()
