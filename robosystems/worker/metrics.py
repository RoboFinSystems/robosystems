"""Queue depth metric publishing with leader election.

Only one worker publishes queue depth to CloudWatch, elected via a
Valkey lock with TTL. If the leader dies, the lock expires and another
worker takes over automatically.

The metric powers:
1. CloudWatch alarms for queue backup detection
2. ECS auto-scaling policy for worker replicas
"""

import time

from redis.asyncio import Redis

from robosystems.logger import get_logger

logger = get_logger(__name__)

# Publish at most once per interval (seconds)
METRIC_PUBLISH_INTERVAL = 60

# Leader lock TTL — must exceed METRIC_PUBLISH_INTERVAL so the lock
# doesn't expire between publishes during normal operation.
LEADER_LOCK_TTL = 90

LEADER_LOCK_KEY = "worker:metrics:leader"


class QueueDepthReporter:
  """Publishes worker queue depth to CloudWatch via leader election.

  Only the worker holding the leader lock publishes metrics. Others
  skip silently. Call maybe_publish() on every consumer loop iteration.
  """

  def __init__(self, queue: Redis, worker_id: str) -> None:
    self.queue = queue
    self.worker_id = worker_id
    self._last_publish_time: float = 0
    self._cloudwatch_client = None

  async def maybe_publish(self) -> None:
    """Publish queue depth if we're the leader and interval has elapsed."""
    now = time.time()
    if now - self._last_publish_time < METRIC_PUBLISH_INTERVAL:
      return

    # Try to acquire or confirm leader lock
    acquired = await self.queue.set(
      LEADER_LOCK_KEY, self.worker_id, nx=True, ex=LEADER_LOCK_TTL
    )
    if not acquired:
      current_leader = await self.queue.get(LEADER_LOCK_KEY)
      if current_leader != self.worker_id:
        return

    # We're the leader — refresh lock and publish
    await self.queue.set(LEADER_LOCK_KEY, self.worker_id, ex=LEADER_LOCK_TTL)

    depth = await self.queue.llen("worker:tasks")
    self._last_publish_time = now

    try:
      await self._publish_to_cloudwatch(depth)
    except Exception as e:
      logger.warning(f"Failed to publish queue depth metric: {e}")

  async def _publish_to_cloudwatch(self, depth: int) -> None:
    """Publish queue depth to CloudWatch. Skipped in dev."""
    from robosystems.config import env

    if env.ENVIRONMENT == "dev":
      logger.debug(f"Queue depth: {depth} (CloudWatch publish skipped in dev)")
      return

    import asyncio

    await asyncio.to_thread(self._publish_sync, depth, env.ENVIRONMENT)

  def _publish_sync(self, depth: int, environment: str) -> None:
    """Synchronous CloudWatch publish (runs in thread)."""
    if self._cloudwatch_client is None:
      import boto3

      self._cloudwatch_client = boto3.client("cloudwatch")

    namespace = f"RoboSystems/Worker/{environment}"
    self._cloudwatch_client.put_metric_data(
      Namespace=namespace,
      MetricData=[
        {
          "MetricName": "QueueDepth",
          "Value": depth,
          "Unit": "Count",
        },
      ],
    )
    logger.debug(f"Published queue depth {depth} to {namespace}")
