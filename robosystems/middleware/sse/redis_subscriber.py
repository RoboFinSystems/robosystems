"""
Redis pub/sub subscriber for SSE event distribution.

This module bridges the gap between worker processes that emit events
and the API process that streams them to clients via SSE.
"""

import asyncio
import json
from typing import Optional, Dict, Any
import redis.asyncio as redis

from robosystems.logger import logger
from robosystems.config.valkey_registry import ValkeyDatabase
from robosystems.config.valkey_registry import create_async_redis_client

from .event_storage import SSEEvent
from .streaming import get_connection_manager


class RedisEventSubscriber:
  """
  Subscribes to Redis pub/sub channels for SSE events and distributes
  them to connected clients through the connection manager.
  """

  def __init__(self):
    """Initialize the Redis subscriber."""
    self.redis_client: Optional[redis.Redis] = None
    self.pubsub: Optional[Any] = None
    self.subscriptions: Dict[str, bool] = {}
    self._running = False
    self._task: Optional[asyncio.Task] = None

  async def start(self):
    """Start the Redis subscriber."""
    if self._running:
      logger.warning("Redis subscriber already running")
      return

    logger.info("Starting Redis SSE event subscriber")

    # Create Redis client for pub/sub with proper ElastiCache support
    self.redis_client = create_async_redis_client(ValkeyDatabase.SSE_EVENTS)
    self.pubsub = self.redis_client.pubsub()

    self._running = True
    self._task = asyncio.create_task(self._listen_for_events())

  async def stop(self):
    """Stop the Redis subscriber."""
    if not self._running:
      return

    logger.info("Stopping Redis SSE event subscriber")
    self._running = False

    if self.pubsub:
      await self.pubsub.unsubscribe()
      await self.pubsub.close()

    if self.redis_client:
      await self.redis_client.close()

    if self._task:
      self._task.cancel()
      try:
        await self._task
      except asyncio.CancelledError:
        pass

  async def subscribe_to_operation(self, operation_id: str):
    """
    Subscribe to events for a specific operation.

    Args:
        operation_id: Operation to subscribe to
    """
    if not self.pubsub:
      logger.error("Redis subscriber not started")
      return

    channel = f"sse:events:{operation_id}"
    if channel not in self.subscriptions:
      await self.pubsub.subscribe(channel)
      self.subscriptions[channel] = True
      logger.info(f"Subscribed to Redis channel: {channel}")

  async def unsubscribe_from_operation(self, operation_id: str):
    """
    Unsubscribe from events for a specific operation.

    Args:
        operation_id: Operation to unsubscribe from
    """
    if not self.pubsub:
      return

    channel = f"sse:events:{operation_id}"
    if channel in self.subscriptions:
      await self.pubsub.unsubscribe(channel)
      del self.subscriptions[channel]
      logger.info(f"Unsubscribed from Redis channel: {channel}")

  async def _listen_for_events(self):
    """
    Main loop that listens for Redis pub/sub messages and distributes them.
    """
    connection_manager = get_connection_manager()
    logger.info(
      f"Redis event listener started with {len(self.subscriptions)} initial subscriptions"
    )

    while self._running:
      try:
        # Don't try to get messages if we have no subscriptions
        if not self.subscriptions:
          await asyncio.sleep(1.0)
          continue

        # Get message with timeout
        if self.pubsub is None:
          await asyncio.sleep(1.0)
          continue

        message = await asyncio.wait_for(
          self.pubsub.get_message(ignore_subscribe_messages=True), timeout=1.0
        )

        if message is None:
          continue

        logger.info(
          f"Received Redis message: type={message.get('type')}, channel={message.get('channel')}, data_len={len(str(message.get('data', '')))}"
        )

        # Parse the channel to get operation_id
        channel = message["channel"]
        if not channel.startswith("sse:events:"):
          logger.debug(f"Ignoring message from non-SSE channel: {channel}")
          continue

        operation_id = channel.replace("sse:events:", "")

        # Parse the event data
        try:
          event_data = json.loads(message["data"])
          event = SSEEvent.from_dict(event_data)

          logger.info(
            f"Parsed SSE event: type={event.event_type}, operation={operation_id}, seq={event.sequence_number}"
          )

          # Distribute to connected clients
          await connection_manager.broadcast_event(operation_id, event)

          logger.info(
            f"Distributed event {event.event_type} for operation {operation_id} "
            f"to connected clients"
          )

        except json.JSONDecodeError:
          logger.error(f"Failed to parse event data: {message['data']}")
        except Exception as e:
          logger.error(f"Error processing event: {e}")

      except asyncio.TimeoutError:
        # Normal timeout, continue listening
        continue
      except asyncio.CancelledError:
        # Task cancelled, exit cleanly
        break
      except Exception as e:
        logger.error(f"Error in Redis event listener: {e}")
        await asyncio.sleep(1)  # Brief pause before retrying


# Global subscriber instance
_redis_subscriber: Optional[RedisEventSubscriber] = None


def get_redis_subscriber() -> RedisEventSubscriber:
  """Get the global Redis subscriber instance."""
  global _redis_subscriber
  if _redis_subscriber is None:
    _redis_subscriber = RedisEventSubscriber()
  return _redis_subscriber


async def start_redis_subscriber():
  """Start the global Redis subscriber."""
  subscriber = get_redis_subscriber()
  await subscriber.start()


async def stop_redis_subscriber():
  """Stop the global Redis subscriber."""
  subscriber = get_redis_subscriber()
  await subscriber.stop()
