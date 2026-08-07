"""Redis pub/sub subscriber for SSE event distribution.

Bridges worker processes that emit events and the API process that
streams them to clients over SSE.
"""

import asyncio
import json
from typing import Any

import redis.asyncio as redis

from robosystems.config.valkey_registry import ValkeyDatabase, create_async_redis_client
from robosystems.logger import logger

from .event_storage import SSEEvent
from .streaming import get_connection_manager


class RedisEventSubscriber:
  """Relays `sse:events:{operation_id}` messages to the connection manager.

  One background task services every subscription; `subscribe_to_operation`
  and `unsubscribe_from_operation` adjust the channel set as SSE streams
  open and close.
  """

  def __init__(self):
    self.redis_client: redis.Redis | None = None
    self.pubsub: Any | None = None
    self.subscriptions: dict[str, bool] = {}
    self._running = False
    self._task: asyncio.Task | None = None

  async def start(self):
    """Start the Redis subscriber."""
    if self._running:
      logger.warning("Redis subscriber already running")
      return

    logger.info("Starting Redis SSE event subscriber")

    self.redis_client = create_async_redis_client(ValkeyDatabase.SSE)
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
    """Subscribe to one operation's event channel."""
    if not self.pubsub:
      logger.error("Redis subscriber not started")
      return

    channel = f"sse:events:{operation_id}"
    if channel not in self.subscriptions:
      await self.pubsub.subscribe(channel)
      self.subscriptions[channel] = True
      logger.info(f"Subscribed to Redis channel: {channel}")

  async def unsubscribe_from_operation(self, operation_id: str):
    """Unsubscribe from one operation's event channel."""
    if not self.pubsub:
      return

    channel = f"sse:events:{operation_id}"
    if channel in self.subscriptions:
      await self.pubsub.unsubscribe(channel)
      del self.subscriptions[channel]
      logger.info(f"Unsubscribed from Redis channel: {channel}")

  async def _listen_for_events(self):
    """Listen for pub/sub messages and broadcast them to local clients."""
    connection_manager = get_connection_manager()
    logger.info(
      f"Redis event listener started with {len(self.subscriptions)} initial subscriptions"
    )

    while self._running:
      try:
        if not self.subscriptions:
          await asyncio.sleep(1.0)
          continue

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

        channel = message["channel"]
        if not channel.startswith("sse:events:"):
          logger.debug(f"Ignoring message from non-SSE channel: {channel}")
          continue

        operation_id = channel.replace("sse:events:", "")

        try:
          event_data = json.loads(message["data"])
          event = SSEEvent.from_dict(event_data)

          logger.info(
            f"Parsed SSE event: type={event.event_type}, operation={operation_id}, seq={event.sequence_number}"
          )

          await connection_manager.broadcast_event(operation_id, event)

          logger.info(
            f"Distributed event {event.event_type} for operation {operation_id} "
            f"to connected clients"
          )

        except json.JSONDecodeError:
          logger.error(f"Failed to parse event data: {message['data']}")
        except Exception as e:
          logger.error(f"Error processing event: {e}")

      except TimeoutError:
        continue
      except asyncio.CancelledError:
        break
      except Exception as e:
        logger.error(f"Error in Redis event listener: {e}")
        await asyncio.sleep(1)  # Brief pause before retrying


# Global subscriber instance
_redis_subscriber: RedisEventSubscriber | None = None


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
