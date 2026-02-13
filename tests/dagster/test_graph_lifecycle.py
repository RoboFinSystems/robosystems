"""Tests for graph lifecycle Dagster jobs and sensors."""

from unittest.mock import MagicMock, patch

from dagster import build_sensor_context

from robosystems.dagster.sensors.graph_lifecycle import (
  suspended_graph_deprovisioning_sensor,
)


class TestSuspendedGraphDeprovisioningSensor:
  """Tests for the suspended_graph_deprovisioning_sensor."""

  def test_finds_ready_graphs(self):
    """Detects suspended graphs past the retention period."""
    mock_sub = MagicMock()
    mock_sub.resource_id = "kg_ready"

    with patch("robosystems.database.session") as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.join.return_value.filter.return_value.all.return_value = [
        mock_sub
      ]

      context = build_sensor_context()
      runs = list(suspended_graph_deprovisioning_sensor(context))

      assert len(runs) == 1
      config = runs[0].run_config["ops"]["deprovision_suspended_graphs"]["config"]
      assert config["graph_ids"] == ["kg_ready"]

  def test_skips_when_no_matches(self):
    """Returns nothing when no graphs are ready."""
    with patch("robosystems.database.session") as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.join.return_value.filter.return_value.all.return_value = []

      context = build_sensor_context()
      runs = list(suspended_graph_deprovisioning_sensor(context))

      assert len(runs) == 0

  def test_session_cleanup(self):
    """Database session is always cleaned up."""
    with patch("robosystems.database.session") as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.join.return_value.filter.return_value.all.return_value = []

      context = build_sensor_context()
      list(suspended_graph_deprovisioning_sensor(context))

      mock_db.remove.assert_called_once()

  def test_multiple_ready_graphs(self):
    """Handles multiple graphs ready for deprovisioning."""
    mock_sub1 = MagicMock()
    mock_sub1.resource_id = "kg_ready1"
    mock_sub2 = MagicMock()
    mock_sub2.resource_id = "kg_ready2"

    with patch("robosystems.database.session") as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.join.return_value.filter.return_value.all.return_value = [
        mock_sub1,
        mock_sub2,
      ]

      context = build_sensor_context()
      runs = list(suspended_graph_deprovisioning_sensor(context))

      assert len(runs) == 1
      config = runs[0].run_config["ops"]["deprovision_suspended_graphs"]["config"]
      assert config["graph_ids"] == ["kg_ready1", "kg_ready2"]
