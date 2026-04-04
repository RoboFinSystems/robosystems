"""Tests for the stale graph materialization sensor."""

import json
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

from dagster import build_sensor_context

from robosystems.dagster.sensors.materialization import (
  stale_graph_materialization_sensor,
)


def _make_graph(graph_id, stale_at=None, stale_reason="schedule_created"):
  """Create a mock Graph object for sensor testing."""
  g = MagicMock()
  g.graph_id = graph_id
  g.graph_stale = stale_at is not None
  g.graph_stale_at = stale_at
  g.graph_stale_reason = stale_reason
  g.graph_type = "entity"
  g.status = "active"
  g.is_repository = False
  return g


class TestStaleGraphSensor:
  def test_no_stale_graphs_returns_empty(self):
    with patch(
      "robosystems.dagster.sensors.materialization.db_session_factory"
    ) as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.filter.return_value.all.return_value = []

      context = build_sensor_context()
      result = list(stale_graph_materialization_sensor(context))

    assert result == []

  def test_submits_run_for_stale_graph(self):
    stale_at = datetime.now(UTC) - timedelta(seconds=60)
    graphs = [_make_graph("kg123", stale_at=stale_at)]

    with patch(
      "robosystems.dagster.sensors.materialization.db_session_factory"
    ) as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.filter.return_value.all.return_value = graphs

      context = build_sensor_context()
      result = list(stale_graph_materialization_sensor(context))

    assert len(result) == 1
    assert "kg123" in result[0].run_key
    # run_key uses graph_stale_at, not now — enables Dagster deduplication
    assert stale_at.isoformat() in result[0].run_key

  def test_skips_in_progress_graph(self):
    stale_at = datetime.now(UTC) - timedelta(seconds=60)
    graphs = [_make_graph("kg123", stale_at=stale_at)]

    # JSON cursor with recent timestamp (not expired)
    cursor = json.dumps({"kg123": datetime.now(UTC).isoformat()})

    with patch(
      "robosystems.dagster.sensors.materialization.db_session_factory"
    ) as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.filter.return_value.all.return_value = graphs

      context = build_sensor_context(cursor=cursor)
      result = list(stale_graph_materialization_sensor(context))

    assert result == []

  def test_retries_after_cursor_expiry(self):
    """Graphs whose cursor entry has expired should be re-submitted."""
    stale_at = datetime.now(UTC) - timedelta(seconds=60)
    graphs = [_make_graph("kg123", stale_at=stale_at)]

    # Cursor entry from 3 hours ago (past 2-hour expiry)
    old_time = (datetime.now(UTC) - timedelta(hours=3)).isoformat()
    cursor = json.dumps({"kg123": old_time})

    with patch(
      "robosystems.dagster.sensors.materialization.db_session_factory"
    ) as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.filter.return_value.all.return_value = graphs

      context = build_sensor_context(cursor=cursor)
      result = list(stale_graph_materialization_sensor(context))

    assert len(result) == 1

  def test_handles_multiple_stale_graphs(self):
    stale_at = datetime.now(UTC) - timedelta(seconds=120)
    graphs = [
      _make_graph("kg111", stale_at=stale_at),
      _make_graph("kg222", stale_at=stale_at),
    ]

    with patch(
      "robosystems.dagster.sensors.materialization.db_session_factory"
    ) as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.filter.return_value.all.return_value = graphs

      context = build_sensor_context()
      result = list(stale_graph_materialization_sensor(context))

    assert len(result) == 2

  def test_closes_session_on_error(self):
    with patch(
      "robosystems.dagster.sensors.materialization.db_session_factory"
    ) as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.side_effect = Exception("DB error")

      context = build_sensor_context()
      result = list(stale_graph_materialization_sensor(context))

    assert result == []
    mock_session.close.assert_called_once()
