"""Tests for the stale graph materialization sensor."""

import json
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from dagster import build_sensor_context

from robosystems.config.defaults import MaterializationDefaults
from robosystems.dagster.sensors.materialization import (
  _graphs_being_written,
  _stale_windows,
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


@pytest.fixture(autouse=True)
def nothing_being_written():
  """No lock held and no run in flight unless a test says otherwise."""
  with patch(
    "robosystems.dagster.sensors.materialization._graphs_being_written",
    return_value=set(),
  ) as busy:
    yield busy


def _tick(graphs, cursor=None):
  with patch("robosystems.dagster.sensors.materialization.db_session_factory") as db:
    db.return_value.query.return_value.filter.return_value.all.return_value = graphs
    context = build_sensor_context(cursor=cursor)
    return list(stale_graph_materialization_sensor(context)), context.cursor


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
    # Keyed on the staleness event (plus the expiry window), so Dagster dedupes ticks
    assert stale_at.isoformat() in result[0].run_key

  def test_run_request_carries_per_graph_concurrency_tag(self):
    """dagster.yaml serializes runs on ``materialize_db`` (limit 1 per unique
    value). Without the tag, a stale-sensor run and a manual launch for the
    same graph could COPY into it concurrently."""
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

    assert result[0].tags["materialize_db"] == "kg123"
    assert result[0].tags["graph_id"] == "kg123"
    assert result[0].tags["trigger"] == "stale_sensor"

  def test_skips_a_graph_whose_event_was_just_submitted(self):
    """A run for this exact write was submitted moments ago (it failed, or
    the cursor outlived it): wait out the retry window."""
    stale_at = datetime.now(UTC) - timedelta(seconds=60)
    cursor = json.dumps(
      {
        "kg123": {
          "stale_at": stale_at.isoformat(),
          "submitted_at": datetime.now(UTC).isoformat(),
        }
      }
    )

    result, _ = _tick([_make_graph("kg123", stale_at=stale_at)], cursor=cursor)

    assert result == []

  def test_retries_after_the_retry_window(self):
    stale_at = datetime.now(UTC) - timedelta(hours=4)
    old_time = (datetime.now(UTC) - timedelta(hours=3)).isoformat()
    cursor = json.dumps(
      {"kg123": {"stale_at": stale_at.isoformat(), "submitted_at": old_time}}
    )

    result, _ = _tick([_make_graph("kg123", stale_at=stale_at)], cursor=cursor)

    assert len(result) == 1

  def test_a_write_during_the_last_run_resubmits_at_once(self):
    """The run finished but a write landed mid-build, so mark_fresh left the
    graph stale with a newer stale_at. That is a new event: no 2h wait."""
    submitted_for = datetime.now(UTC) - timedelta(minutes=5)
    newer = datetime.now(UTC) - timedelta(seconds=45)
    cursor = json.dumps(
      {
        "kg123": {
          "stale_at": submitted_for.isoformat(),
          "submitted_at": (datetime.now(UTC) - timedelta(minutes=4)).isoformat(),
        }
      }
    )

    result, new_cursor = _tick([_make_graph("kg123", stale_at=newer)], cursor=cursor)

    assert len(result) == 1
    assert newer.isoformat() in result[0].run_key
    assert json.loads(new_cursor)["kg123"]["stale_at"] == newer.isoformat()

  def test_a_graph_being_written_is_skipped_and_its_entry_kept(
    self, nothing_being_written
  ):
    """A manual run holds the lock (or a sensor run is queued): submitting
    would only launch a run that dies on the lock."""
    nothing_being_written.return_value = {"kg123"}
    stale_at = datetime.now(UTC) - timedelta(seconds=60)
    entry = {"stale_at": "x", "submitted_at": datetime.now(UTC).isoformat()}

    result, new_cursor = _tick(
      [_make_graph("kg123", stale_at=stale_at)],
      cursor=json.dumps({"kg123": entry}),
    )

    assert result == []
    assert json.loads(new_cursor) == {"kg123": entry}

  def test_a_cursor_from_the_previous_release_is_read(self):
    """The old cursor maps graph id to a bare submitted_at; it carries no
    staleness event, so the graph resubmits unless it is being written."""
    stale_at = datetime.now(UTC) - timedelta(seconds=60)
    cursor = json.dumps({"kg123": datetime.now(UTC).isoformat()})

    result, _ = _tick([_make_graph("kg123", stale_at=stale_at)], cursor=cursor)

    assert len(result) == 1

  def test_retry_after_expiry_uses_a_new_run_key(self):
    """Dagster dedupes on run_key, so a retry that reuses it never runs."""
    stale_at = datetime(2026, 9, 23, 12, 0, tzinfo=UTC)
    first_tick = stale_at + timedelta(minutes=1)
    retry_tick = first_tick + timedelta(hours=3)
    graphs = [_make_graph("kg123", stale_at=stale_at)]

    def tick(now, cursor=None):
      with (
        patch("robosystems.dagster.sensors.materialization.db_session_factory") as db,
        patch("robosystems.dagster.sensors.materialization._now", return_value=now),
      ):
        db.return_value.query.return_value.filter.return_value.all.return_value = graphs
        context = build_sensor_context(cursor=cursor)
        return list(stale_graph_materialization_sensor(context)), context.cursor

    (first,), cursor = tick(first_tick)
    (retry,), _ = tick(retry_tick, cursor)

    assert first.run_key != retry.run_key

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


class TestGraphsBeingWritten:
  """The busy set: a held lock, or a Dagster run in flight for the graph."""

  def _context(self, run_tags):
    context = MagicMock()
    context.instance.get_run_records.return_value = [
      MagicMock(dagster_run=MagicMock(tags=tags)) for tags in run_tags
    ]
    return context

  def test_held_locks_and_live_runs_are_busy(self):
    redis = MagicMock()
    redis.pipeline.return_value.execute.return_value = [1, 0, 0]
    context = self._context([{"materialize_db": "kg3"}, {"materialize_db": "kgX"}])

    with patch(
      "robosystems.config.valkey_registry.create_redis_client", return_value=redis
    ):
      busy = _graphs_being_written(context, ["kg1", "kg2", "kg3"])

    assert busy == {"kg1", "kg3"}
    redis.pipeline.return_value.exists.assert_any_call("materialize_lock:kg1")

  def test_an_unreadable_lock_service_counts_every_graph_busy(self):
    with patch(
      "robosystems.config.valkey_registry.create_redis_client",
      side_effect=ConnectionError("down"),
    ):
      busy = _graphs_being_written(self._context([]), ["kg1", "kg2"])

    assert busy == {"kg1", "kg2"}


class TestStaleWindows:
  def test_defaults_are_the_managed_cadence(self, monkeypatch):
    monkeypatch.delenv("TUNING_MATERIALIZATION_MIN_STALE_AGE", raising=False)
    monkeypatch.delenv("TUNING_MATERIALIZATION_MAX_STALE_WAIT", raising=False)
    assert _stale_windows() == (
      MaterializationDefaults.MIN_STALE_AGE,
      MaterializationDefaults.MAX_STALE_WAIT,
    )

  def test_a_tuning_override_is_read_on_the_tick(self, monkeypatch):
    monkeypatch.setenv("TUNING_MATERIALIZATION_MIN_STALE_AGE", "3600")
    monkeypatch.setenv("TUNING_MATERIALIZATION_MAX_STALE_WAIT", "86400")
    assert _stale_windows() == (3600, 86400)
