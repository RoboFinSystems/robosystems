"""Tests for staleness tracking utility."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from robosystems.operations.extensions.staleness import (
  graph_freshness,
  mark_graph_stale,
)


@pytest.fixture(autouse=True)
def _no_backoff():
  with patch("robosystems.operations.extensions.staleness.time.sleep"):
    yield


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

    # One session per attempt, each closed.
    assert mock_session.close.call_count == 3

  def test_retries_a_failed_mark(self):
    """A transient failure is retried rather than losing the mark."""
    mock_graph = MagicMock()
    mock_graph.mark_stale.side_effect = [Exception("connection reset"), None]
    mock_session = MagicMock()

    with (
      patch(
        "robosystems.operations.extensions.staleness.SessionFactory",
        return_value=mock_session,
      ),
      patch("robosystems.operations.extensions.staleness.Graph") as mock_graph_cls,
      patch("robosystems.operations.extensions.staleness.logger") as mock_logger,
    ):
      mock_graph_cls.get_by_id.return_value = mock_graph
      mark_graph_stale("kg123", "period_closed")

    assert mock_graph.mark_stale.call_count == 2
    mock_logger.error.assert_not_called()

  def test_final_failure_logs_an_alertable_event(self):
    """A mark lost after every attempt is an error event, not a warning."""
    with (
      patch(
        "robosystems.operations.extensions.staleness.SessionFactory",
        side_effect=Exception("DB down"),
      ),
      patch("robosystems.operations.extensions.staleness.logger") as mock_logger,
    ):
      mark_graph_stale("kg123", "period_closed")

    mock_logger.error.assert_called_once()
    assert mock_logger.error.call_args.args[0] == "graph_stale_mark_failed"
    assert mock_logger.error.call_args.kwargs["extra"]["graph_id"] == "kg123"


def _graph_row(**fields):
  row = MagicMock()
  row.graph_stale = fields.get("graph_stale", False)
  row.graph_stale_since = fields.get("graph_stale_since")
  row.graph_stale_at = fields.get("graph_stale_at")
  row.graph_stale_reason = fields.get("graph_stale_reason")
  row.graph_metadata = fields.get("graph_metadata", {})
  return row


class TestGraphFreshness:
  @pytest.mark.parametrize(
    "graph_id", ["sec", "sec_historical", "kg1a2b3c4d5e6f7a8b9c_dev"]
  )
  def test_not_applicable_to_shared_repositories_or_subgraphs(self, graph_id):
    with patch("robosystems.operations.extensions.staleness.SessionFactory") as factory:
      assert graph_freshness(graph_id) is None
    factory.assert_not_called()

  def test_stale_graph_reports_the_onset_not_the_latest_write(self):
    onset = datetime(2026, 10, 1, 9, 0, tzinfo=UTC)
    latest = datetime(2026, 10, 2, 17, 30, tzinfo=UTC)
    row = _graph_row(
      graph_stale=True,
      graph_stale_since=onset,
      graph_stale_at=latest,
      graph_stale_reason="period_closed",
      graph_metadata={"last_materialized_at": "2026-09-30T12:00:00+00:00"},
    )
    with (
      patch("robosystems.operations.extensions.staleness.SessionFactory"),
      patch("robosystems.operations.extensions.staleness.Graph") as mock_graph_cls,
    ):
      mock_graph_cls.get_by_id.return_value = row
      freshness = graph_freshness("kg1a2b3c4d5e6f7a8b9c")

    assert freshness is not None
    assert freshness.stale is True
    assert freshness.stale_since == onset.isoformat()
    assert freshness.stale_reason == "period_closed"
    assert freshness.last_materialized_at == "2026-09-30T12:00:00+00:00"

  def test_current_graph_carries_no_onset(self):
    row = _graph_row(
      graph_metadata={"last_materialized_at": "2026-10-01T00:00:00+00:00"}
    )
    with (
      patch("robosystems.operations.extensions.staleness.SessionFactory"),
      patch("robosystems.operations.extensions.staleness.Graph") as mock_graph_cls,
    ):
      mock_graph_cls.get_by_id.return_value = row
      freshness = graph_freshness("kg1a2b3c4d5e6f7a8b9c")

    assert freshness is not None
    assert freshness.stale is False
    assert freshness.stale_since is None
    assert freshness.last_materialized_at == "2026-10-01T00:00:00+00:00"

  def test_missing_graph_row(self):
    with (
      patch("robosystems.operations.extensions.staleness.SessionFactory"),
      patch("robosystems.operations.extensions.staleness.Graph") as mock_graph_cls,
    ):
      mock_graph_cls.get_by_id.return_value = None
      assert graph_freshness("kg1a2b3c4d5e6f7a8b9c") is None
