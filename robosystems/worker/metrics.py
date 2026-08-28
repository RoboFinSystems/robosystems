"""Queue depth metric publishing via a background daemon thread.

Every worker runs its own publisher thread that reports the shared queue
depth (llen of "worker:tasks") to CloudWatch once per interval. The thread
is independent of the consume loop, so it keeps publishing even while the
worker is blocked processing a long-running task — the consume loop only
returns between tasks, which can be many minutes apart.

All workers publish the same global depth; the scale-out/scale-in alarms
aggregate with Statistic=Maximum, so concurrent publishers are correct
(scale out on the peak reading, scale in only when every reading is 0).

The metric powers:
1. CloudWatch alarms for queue backup detection
2. ECS auto-scaling policy for worker replicas
"""

import threading
from typing import Any

from robosystems.logger import get_logger

logger = get_logger(__name__)

# Publish queue depth once per interval (seconds).
PUBLISH_INTERVAL = 60

# Reliable-queue list that the consumer pops from.
QUEUE_KEY = "worker:tasks"

# Dead-letter list — tasks that exhausted retries. A growing DLQ is the
# canary for retry failures under routine scale-in.
DLQ_KEY = "worker:dlq"

# Per-worker inflight lists (BLMOVE destination). Their combined length is
# the number of tasks executing right now; queue depth alone counts only the
# waiting ones, so with one worker busy on a ten-minute close, two questions
# waiting behind it never crossed the scale-out threshold. `Backlog` (queued
# + in flight) is the metric the scaling alarms read.
INFLIGHT_KEY_PATTERN = "worker:inflight:*"

# Join timeout when stopping the thread on shutdown.
STOP_JOIN_TIMEOUT = 5


class QueueDepthPublisher:
  """Publishes worker queue depth to CloudWatch from a daemon thread.

  Start with start() at worker boot and stop() on shutdown. The thread owns
  its own synchronous Valkey and CloudWatch clients — it must not share the
  consume loop's async client across the thread boundary.
  """

  def __init__(self) -> None:
    self._stop = threading.Event()
    self._thread = threading.Thread(
      target=self._run, name="queue-depth-publisher", daemon=True
    )
    self._queue: Any = None
    self._cloudwatch_client: Any = None

  def start(self) -> None:
    self._thread.start()

  def stop(self) -> None:
    self._stop.set()
    self._thread.join(timeout=STOP_JOIN_TIMEOUT)
    if self._thread.is_alive():
      logger.warning(
        f"queue-depth-publisher thread did not stop within {STOP_JOIN_TIMEOUT}s"
      )

  def _run(self) -> None:
    """Publish immediately, then every interval until stopped."""
    from robosystems.config.valkey_registry import (
      ValkeyDatabase,
      create_redis_client,
    )

    self._queue = create_redis_client(
      ValkeyDatabase.WORKER_QUEUE, decode_responses=True
    )
    try:
      while True:
        try:
          self._publish_once()
        except Exception as e:
          # A transient Valkey/CloudWatch error must never kill the thread.
          logger.warning(f"Failed to publish queue depth metric: {e}")
        if self._stop.wait(PUBLISH_INTERVAL):
          break
    finally:
      try:
        self._queue.close()
      except Exception as e:
        logger.debug(f"Queue client close error on shutdown: {e}")

  def _in_flight(self) -> int:
    """Tasks currently executing: the sum over every worker's inflight list."""
    total = 0
    for key in self._queue.scan_iter(match=INFLIGHT_KEY_PATTERN, count=100):
      total += self._queue.llen(key)
    return total

  def _publish_once(self) -> None:
    """Read queue, in-flight and DLQ depth and publish them (skipped in dev)."""
    from robosystems.config import env

    depth = self._queue.llen(QUEUE_KEY)
    in_flight = self._in_flight()
    dlq_depth = self._queue.llen(DLQ_KEY)
    backlog = depth + in_flight

    if env.ENVIRONMENT == "dev":
      logger.debug(
        f"Queue depth: {depth}, in flight: {in_flight}, DLQ depth: {dlq_depth} "
        "(CloudWatch publish skipped in dev)"
      )
      return

    namespace = f"RoboSystems/Worker/{env.ENVIRONMENT}"
    self._get_cloudwatch_client().put_metric_data(
      Namespace=namespace,
      MetricData=[
        {"MetricName": "QueueDepth", "Value": depth, "Unit": "Count"},
        {"MetricName": "InFlight", "Value": in_flight, "Unit": "Count"},
        {"MetricName": "Backlog", "Value": backlog, "Unit": "Count"},
        {"MetricName": "DLQDepth", "Value": dlq_depth, "Unit": "Count"},
      ],
    )
    logger.debug(
      f"Published queue depth {depth}, in flight {in_flight}, DLQ depth "
      f"{dlq_depth} to {namespace}"
    )

  def _get_cloudwatch_client(self) -> Any:
    if self._cloudwatch_client is None:
      import boto3

      self._cloudwatch_client = boto3.client("cloudwatch")
    return self._cloudwatch_client
