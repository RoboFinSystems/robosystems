"""Tests for Dagster graph operation jobs.

Note: TestWaitAndCreateGraphConfig and TestWaitAndCreate removed —
wait-for-capacity graph creation replaced by worker task + sensor retry.
"""

from unittest.mock import MagicMock, patch

from robosystems.dagster.jobs.graph import (
  _emit_completion_sync,
  _emit_failure_sync,
  _emit_progress_sync,
)


class TestSyncSSEHelpers:
  """Test sync SSE emission helpers."""

  def test_emit_progress_sync(self):
    """Test progress emission from sync context."""
    with patch(
      "robosystems.middleware.sse.event_storage.SSEEventStorage"
    ) as MockStorage:
      mock_instance = MagicMock()
      MockStorage.return_value = mock_instance

      _emit_progress_sync("op123", "Creating graph...", 50.0)

      mock_instance.store_event_sync.assert_called_once()
      call_args = mock_instance.store_event_sync.call_args
      assert call_args.args[0] == "op123"
      assert call_args.args[2]["message"] == "Creating graph..."
      assert call_args.args[2]["progress_percent"] == 50.0

  def test_emit_failure_sync(self):
    """Test failure emission from sync context."""
    with patch(
      "robosystems.middleware.sse.event_storage.SSEEventStorage"
    ) as MockStorage:
      mock_instance = MagicMock()
      MockStorage.return_value = mock_instance

      _emit_failure_sync("op123", "Timeout exceeded")

      mock_instance.store_event_sync.assert_called_once()
      call_args = mock_instance.store_event_sync.call_args
      assert call_args.args[0] == "op123"
      assert "Timeout exceeded" in call_args.args[2]["error"]

  def test_emit_completion_sync(self):
    """Test completion emission from sync context."""
    with patch(
      "robosystems.middleware.sse.event_storage.SSEEventStorage"
    ) as MockStorage:
      mock_instance = MagicMock()
      MockStorage.return_value = mock_instance

      _emit_completion_sync("op123", {"graph_id": "kg123"})

      mock_instance.store_event_sync.assert_called_once()
      call_args = mock_instance.store_event_sync.call_args
      assert call_args.args[0] == "op123"
      assert call_args.args[2]["result"]["graph_id"] == "kg123"

  def test_emit_progress_handles_errors(self):
    """Test that SSE emission errors are swallowed."""
    with patch(
      "robosystems.middleware.sse.event_storage.SSEEventStorage"
    ) as MockStorage:
      MockStorage.return_value.store_event_sync.side_effect = Exception("Redis down")

      # Should not raise
      _emit_progress_sync("op123", "test", 50)
