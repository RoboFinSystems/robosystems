"""Tests for staleness tracking utility."""

from unittest.mock import MagicMock, patch

from robosystems.operations.extensions.staleness import mark_graph_stale


class TestMarkGraphStale:
  def test_marks_graph_stale(self):
    """Staleness utility calls graph.mark_stale with reason."""
    mock_graph = MagicMock()
    mock_session = MagicMock()

    with (
      patch(
        "robosystems.operations.extensions.staleness.SessionFactory",
        return_value=mock_session,
      ),
      patch("robosystems.operations.extensions.staleness.Graph") as mock_graph_cls,
    ):
      mock_graph_cls.get_by_id.return_value = mock_graph
      mark_graph_stale("kg123", "schedule_created")

    mock_graph_cls.get_by_id.assert_called_once_with("kg123", mock_session)
    mock_graph.mark_stale.assert_called_once_with(mock_session, "schedule_created")
    mock_session.close.assert_called_once()

  def test_handles_graph_not_found(self):
    """Staleness utility handles missing graph gracefully."""
    mock_session = MagicMock()

    with (
      patch(
        "robosystems.operations.extensions.staleness.SessionFactory",
        return_value=mock_session,
      ),
      patch("robosystems.operations.extensions.staleness.Graph") as mock_graph_cls,
    ):
      mock_graph_cls.get_by_id.return_value = None
      # Should not raise
      mark_graph_stale("nonexistent", "schedule_created")

    mock_session.close.assert_called_once()

  def test_handles_exception_gracefully(self):
    """Staleness utility logs warning on exception, does not raise."""
    with patch(
      "robosystems.operations.extensions.staleness.SessionFactory",
      side_effect=Exception("DB down"),
    ):
      # Should not raise
      mark_graph_stale("kg123", "schedule_created")

  def test_closes_session_on_mark_stale_failure(self):
    """Session is closed even if mark_stale raises."""
    mock_graph = MagicMock()
    mock_graph.mark_stale.side_effect = Exception("commit failed")
    mock_session = MagicMock()

    with (
      patch(
        "robosystems.operations.extensions.staleness.SessionFactory",
        return_value=mock_session,
      ),
      patch("robosystems.operations.extensions.staleness.Graph") as mock_graph_cls,
    ):
      mock_graph_cls.get_by_id.return_value = mock_graph
      # Should not raise (non-fatal)
      mark_graph_stale("kg123", "connector_sync")

    mock_session.close.assert_called_once()
