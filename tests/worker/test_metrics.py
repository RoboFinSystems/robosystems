"""Tests for the worker queue-depth publisher."""

from unittest.mock import MagicMock, patch

from robosystems.config import env
from robosystems.worker.metrics import (
  DLQ_KEY,
  INFLIGHT_KEY_PATTERN,
  QUEUE_KEY,
  QueueDepthPublisher,
)


def _queue_with(depth: int, inflight: dict[str, int], dlq: int) -> MagicMock:
  """A sync Valkey mock whose llen answers per key and whose scan finds the
  given per-worker inflight lists."""
  queue = MagicMock()
  lengths = {QUEUE_KEY: depth, DLQ_KEY: dlq, **inflight}
  queue.llen.side_effect = lambda key: lengths[key]
  queue.scan_iter.return_value = iter(inflight.keys())
  return queue


def test_publish_once_emits_queue_inflight_backlog_and_dlq_to_cloudwatch():
  """Outside dev, _publish_once publishes QueueDepth, InFlight, Backlog, DLQDepth."""
  publisher = QueueDepthPublisher()
  publisher._queue = _queue_with(
    depth=7, inflight={"worker:inflight:a": 1, "worker:inflight:b": 1}, dlq=2
  )
  cloudwatch = MagicMock()
  publisher._cloudwatch_client = cloudwatch

  with patch.object(env, "ENVIRONMENT", "prod"):
    publisher._publish_once()

  publisher._queue.llen.assert_any_call(QUEUE_KEY)
  publisher._queue.llen.assert_any_call(DLQ_KEY)
  publisher._queue.scan_iter.assert_called_once_with(
    match=INFLIGHT_KEY_PATTERN, count=100
  )
  cloudwatch.put_metric_data.assert_called_once()
  kwargs = cloudwatch.put_metric_data.call_args.kwargs
  assert kwargs["Namespace"] == "RoboSystems/Worker/prod"
  metrics = {m["MetricName"]: m for m in kwargs["MetricData"]}
  assert metrics["QueueDepth"]["Value"] == 7
  assert metrics["QueueDepth"]["Unit"] == "Count"
  assert metrics["InFlight"]["Value"] == 2
  assert metrics["Backlog"]["Value"] == 9
  assert metrics["DLQDepth"]["Value"] == 2
  assert metrics["DLQDepth"]["Unit"] == "Count"


def test_two_waiting_behind_one_busy_reads_as_backlog_three():
  """The scale-out alarm fires above 2; queue depth alone would report 2 here."""
  publisher = QueueDepthPublisher()
  publisher._queue = _queue_with(depth=2, inflight={"worker:inflight:a": 1}, dlq=0)
  cloudwatch = MagicMock()
  publisher._cloudwatch_client = cloudwatch

  with patch.object(env, "ENVIRONMENT", "prod"):
    publisher._publish_once()

  metrics = {
    m["MetricName"]: m["Value"]
    for m in cloudwatch.put_metric_data.call_args.kwargs["MetricData"]
  }
  assert metrics["QueueDepth"] == 2
  assert metrics["Backlog"] == 3


def test_publish_once_skips_cloudwatch_in_dev():
  """In dev, depths are read but never published to CloudWatch."""
  publisher = QueueDepthPublisher()
  publisher._queue = _queue_with(depth=3, inflight={}, dlq=1)
  cloudwatch = MagicMock()
  publisher._cloudwatch_client = cloudwatch

  with patch.object(env, "ENVIRONMENT", "dev"):
    publisher._publish_once()

  publisher._queue.llen.assert_any_call(QUEUE_KEY)
  cloudwatch.put_metric_data.assert_not_called()


def test_start_then_stop_runs_and_joins_cleanly():
  """The daemon thread starts, publishes at least once, and stops without error."""
  fake_queue = MagicMock()
  fake_queue.llen.return_value = 0

  with patch(
    "robosystems.config.valkey_registry.create_redis_client",
    return_value=fake_queue,
  ):
    publisher = QueueDepthPublisher()
    publisher.start()
    publisher.stop()

  # Thread joined within the stop timeout.
  assert not publisher._thread.is_alive()
  # It owned and closed its own sync client.
  fake_queue.llen.assert_any_call(QUEUE_KEY)
  fake_queue.close.assert_called_once()


def test_run_continues_after_publish_error():
  """A failing _publish_once is caught; the loop keeps publishing."""
  fake_queue = MagicMock()
  publisher = QueueDepthPublisher()
  publish_calls = []

  def publish_side_effect():
    publish_calls.append(1)
    if len(publish_calls) == 1:
      raise ValueError("transient CloudWatch error")

  with (
    patch(
      "robosystems.config.valkey_registry.create_redis_client",
      return_value=fake_queue,
    ),
    patch.object(publisher, "_publish_once", side_effect=publish_side_effect),
    patch.object(publisher._stop, "wait", side_effect=[False, True]),
  ):
    publisher._run()

  # Two iterations ran despite the first raising — the thread didn't die.
  assert len(publish_calls) == 2
  fake_queue.close.assert_called_once()
