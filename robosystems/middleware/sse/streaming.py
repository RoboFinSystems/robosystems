"""Server-Sent Events streaming for operation monitoring.

Backs `GET /v1/operations/{operation_id}/stream`: replays stored events
from a requested sequence number, then streams live ones until the
operation reaches a terminal state.
"""

import asyncio
import json
from collections.abc import AsyncGenerator
from datetime import UTC
from typing import Any

from fastapi import HTTPException, Request
from sse_starlette.sse import EventSourceResponse

from robosystems.config.defaults import SSEDefaults
from robosystems.config.tuning import TuningConfig
from robosystems.logger import logger
from robosystems.middleware.sse.event_storage import (
  EventType,
  OperationStatus,
  SSEEvent,
  get_event_storage,
)


class SSEConnectionManager:
  """Tracks open SSE connections per operation and per user.

  Each connection gets a bounded queue; a queue that fills is dropped
  rather than allowed to block the broadcaster.
  """

  def __init__(self):
    self.connections: dict[str, set[str]] = {}  # operation_id -> set of connection_ids
    self.connection_queues: dict[
      str, asyncio.Queue
    ] = {}  # connection_key -> event queue
    self.user_connections: dict[str, set[str]] = {}  # user_id -> set of connection_keys
    self._lock = asyncio.Lock()

    # Runtime-tunable via SSM.
    self.max_connections_per_user = TuningConfig.get_sse_max_connections_per_user()
    self.queue_size = TuningConfig.get_sse_queue_size()

  async def add_connection(
    self, operation_id: str, connection_id: str, user_id: str
  ) -> asyncio.Queue:
    """Register a connection and return its event queue.

    Raises `HTTPException` 429 once the user is at
    `max_connections_per_user`.
    """
    async with self._lock:
      if user_id in self.user_connections:
        user_connection_count = len(self.user_connections[user_id])
        if user_connection_count >= self.max_connections_per_user:
          logger.warning(
            f"User {user_id} exceeded SSE connection limit ({user_connection_count}/{self.max_connections_per_user})"
          )
          try:
            from robosystems.middleware.otel.metrics import get_endpoint_metrics

            metrics = get_endpoint_metrics()
            metrics.record_sse_connection_rejected(user_id, "connection_limit_exceeded")
          except Exception:
            pass  # Don't fail if metrics aren't available

          raise HTTPException(
            status_code=429,
            detail=f"Too many concurrent SSE connections (limit: {self.max_connections_per_user})",
          )

      if operation_id not in self.connections:
        self.connections[operation_id] = set()
      self.connections[operation_id].add(connection_id)

      if user_id not in self.user_connections:
        self.user_connections[user_id] = set()
      queue_key = f"{operation_id}:{connection_id}"
      self.user_connections[user_id].add(queue_key)

      queue = asyncio.Queue(maxsize=self.queue_size)
      self.connection_queues[queue_key] = queue

      logger.debug(
        f"Added SSE connection {connection_id} for operation {operation_id} (user: {user_id})"
      )

      try:
        from robosystems.middleware.otel.metrics import get_endpoint_metrics

        metrics = get_endpoint_metrics()
        metrics.record_sse_connection_opened(user_id, operation_id)
      except Exception:
        pass  # Don't fail if metrics aren't available

      return queue

  async def remove_connection(
    self, operation_id: str, connection_id: str, user_id: str | None = None
  ):
    """Remove an SSE connection and its queue."""
    async with self._lock:
      if operation_id in self.connections:
        self.connections[operation_id].discard(connection_id)

        if not self.connections[operation_id]:
          del self.connections[operation_id]

      queue_key = f"{operation_id}:{connection_id}"
      if queue_key in self.connection_queues:
        del self.connection_queues[queue_key]

      if user_id and user_id in self.user_connections:
        self.user_connections[user_id].discard(queue_key)
        if not self.user_connections[user_id]:
          del self.user_connections[user_id]

      logger.debug(
        f"Removed SSE connection {connection_id} for operation {operation_id}"
      )

      try:
        from robosystems.middleware.otel.metrics import get_endpoint_metrics

        metrics = get_endpoint_metrics()
        metrics.record_sse_connection_closed(user_id or "unknown", operation_id)
      except Exception:
        pass  # Don't fail if metrics aren't available

  async def broadcast_event(self, operation_id: str, event: SSEEvent):
    """Fan an event out to every open connection for an operation.

    Connections whose queue is full are dropped: the broadcaster never
    blocks on a client that has stopped consuming.
    """
    failed_connections: list[str] = []

    async with self._lock:
      if operation_id not in self.connections:
        logger.info(
          f"No active connections for operation {operation_id} to broadcast event {event.event_type}"
        )
        return

      connection_count = len(self.connections[operation_id])
      logger.info(
        f"Broadcasting event {event.event_type} to {connection_count} connections for operation {operation_id}"
      )

      emitted_count = 0
      failed_count = 0
      for connection_id in self.connections[operation_id].copy():
        queue_key = f"{operation_id}:{connection_id}"
        if queue_key in self.connection_queues:
          try:
            self.connection_queues[queue_key].put_nowait(event)
            logger.debug(f"Event queued for connection {connection_id}")
            emitted_count += 1
          except asyncio.QueueFull:
            logger.warning(f"Queue full for connection {connection_id}, removing")
            failed_count += 1
            failed_connections.append(connection_id)
            try:
              from robosystems.middleware.otel.metrics import get_endpoint_metrics

              m = get_endpoint_metrics()
              m.record_sse_queue_overflow(operation_id, connection_id)
            except Exception:
              pass  # Metrics are best-effort, never break SSE delivery

      try:
        from robosystems.middleware.otel.metrics import get_endpoint_metrics

        m = get_endpoint_metrics()
        if emitted_count > 0:
          m.record_sse_event_emitted(operation_id, str(event.event_type))
        if failed_count > 0:
          m.record_sse_event_failed(operation_id, "queue_full")
      except Exception:
        pass  # Metrics are best-effort, never break SSE delivery

    # Clean up failed connections outside the lock to avoid deadlock
    for connection_id in failed_connections:
      await self._handle_connection_error(operation_id, connection_id, "Queue overflow")
      await self.remove_connection(operation_id, connection_id)

  async def get_active_connections(self, operation_id: str) -> int:
    """Get number of active connections for an operation."""
    async with self._lock:
      return len(self.connections.get(operation_id, []))

  async def get_user_connection_count(self, user_id: str) -> int:
    """Get number of active connections for a user."""
    async with self._lock:
      return len(self.user_connections.get(user_id, []))

  async def _handle_connection_error(
    self, operation_id: str, connection_id: str, error_message: str
  ):
    """Best-effort attempt to tell the client why its stream is closing."""
    queue_key = f"{operation_id}:{connection_id}"
    if queue_key in self.connection_queues:
      try:
        error_event = SSEEvent(
          event_type=EventType.OPERATION_ERROR,
          operation_id=operation_id,
          timestamp="",
          data={"error": error_message, "connection_closed": True},
          sequence_number=0,
        )
        self.connection_queues[queue_key].put_nowait(error_event)
      except Exception:
        pass  # Queue might be full or closed, best effort only


# Global connection manager
_connection_manager: SSEConnectionManager | None = None


def get_connection_manager() -> SSEConnectionManager:
  """Get the global connection manager instance."""
  global _connection_manager
  if _connection_manager is None:
    _connection_manager = SSEConnectionManager()
  return _connection_manager


async def create_sse_stream_starlette(
  operation_id: str,
  user_id: str,
  from_sequence: int = 0,
  request: Request | None = None,
) -> AsyncGenerator[dict[str, Any]]:
  """Yield sse-starlette event dicts for one operation stream.

  Emits `connected`, replays stored events from `from_sequence`, then
  streams live events until a terminal event or client disconnect, at
  which point it emits `stream_end`. Idle periods emit `keepalive`.

  A stream for an operation belonging to another user yields an `error`
  event and stops — the caller's identity is the access check.
  """
  import uuid

  connection_id = str(uuid.uuid4())
  event_storage = get_event_storage()
  connection_manager = get_connection_manager()

  try:
    metadata = await event_storage.get_operation_metadata(operation_id)
    if not metadata:
      yield {"event": "error", "data": json.dumps({"error": "Operation not found"})}
      return

    if metadata.user_id != user_id:
      yield {"event": "error", "data": json.dumps({"error": "Access denied"})}
      return

    yield {
      "event": "connected",
      "data": json.dumps(
        {
          "operation_id": operation_id,
          "connection_id": connection_id,
          "from_sequence": from_sequence,
        }
      ),
    }

    if from_sequence > 0:
      historical_events = await event_storage.get_events(operation_id, from_sequence)
      for event in historical_events:
        if request and await request.is_disconnected():
          return
        yield {
          "event": str(event.event_type),
          "data": json.dumps(
            {
              "operation_id": event.operation_id,
              "timestamp": event.timestamp,
              "sequence_number": event.sequence_number,
              **event.data,
            }
          ),
        }

    try:
      event_queue = await connection_manager.add_connection(
        operation_id, connection_id, user_id
      )
    except HTTPException as e:
      yield {"event": "error", "data": json.dumps({"error": e.detail})}
      return

    # Pub/sub relays events emitted by worker processes into this one.
    try:
      from .redis_subscriber import get_redis_subscriber

      subscriber = get_redis_subscriber()
      await subscriber.subscribe_to_operation(operation_id)
    except Exception as e:
      logger.warning(
        f"Failed to subscribe to Redis channel for operation {operation_id}: {e}"
      )

    if metadata.status in [
      OperationStatus.COMPLETED,
      OperationStatus.FAILED,
      OperationStatus.CANCELLED,
    ]:
      # Already terminal: replay the whole history and close.
      all_events = await event_storage.get_events(operation_id, from_sequence=0)
      for event in all_events:
        if request and await request.is_disconnected():
          return
        yield {
          "event": str(event.event_type),
          "data": json.dumps(
            {
              "operation_id": event.operation_id,
              "timestamp": event.timestamp,
              "sequence_number": event.sequence_number,
              **event.data,
            }
          ),
        }
        await asyncio.sleep(0.01)

      # Hold the connection open briefly so the client drains the replay.
      await asyncio.sleep(1.0)

      yield {
        "event": "stream_end",
        "data": json.dumps(
          {
            "status": metadata.status,
            "operation_id": operation_id,
            "message": "Stream ended normally",
          }
        ),
      }
      return

    while True:
      if request and await request.is_disconnected():
        break

      try:
        event = await asyncio.wait_for(
          event_queue.get(), timeout=SSEDefaults.KEEPALIVE_INTERVAL
        )

        yield {
          "event": str(event.event_type),
          "data": json.dumps(
            {
              "operation_id": event.operation_id,
              "timestamp": event.timestamp,
              "sequence_number": event.sequence_number,
              **event.data,
            }
          ),
        }

        if event.event_type in [
          EventType.OPERATION_COMPLETED,
          EventType.OPERATION_ERROR,
          EventType.OPERATION_CANCELLED,
        ]:
          yield {
            "event": "stream_end",
            "data": json.dumps(
              {
                "status": metadata.status,
                "operation_id": operation_id,
                "message": "Stream ended normally",
              }
            ),
          }
          break

      except TimeoutError:
        yield {
          "event": "keepalive",
          "data": json.dumps({"timestamp": str(asyncio.get_event_loop().time())}),
        }

  except Exception as e:
    logger.error(f"Error in SSE stream for operation {operation_id}: {e}")
    yield {"event": "error", "data": json.dumps({"error": str(e)})}

  finally:
    await connection_manager.remove_connection(operation_id, connection_id, user_id)

    try:
      from .redis_subscriber import get_redis_subscriber

      subscriber = get_redis_subscriber()
      await subscriber.unsubscribe_from_operation(operation_id)
    except Exception as e:
      logger.warning(f"Failed to unsubscribe from Redis channel: {e}")


async def emit_event_to_operation(
  operation_id: str,
  event_type: EventType,
  data: dict[str, Any],
  sequence_number: int | None = None,
) -> bool:
  """Broadcast an event to this process's open connections for an operation.

  Returns True when at least one connection was still attached. This does
  not persist the event — use `SSEEventStorage.store_event` for that.
  """
  from datetime import datetime

  connection_manager = get_connection_manager()

  event = SSEEvent(
    event_type=event_type,
    operation_id=operation_id,
    timestamp=datetime.now(UTC).isoformat(),
    data=data,
    sequence_number=sequence_number or 0,
  )

  await connection_manager.broadcast_event(operation_id, event)

  active_connections = await connection_manager.get_active_connections(operation_id)
  return active_connections > 0


DEFAULT_SSE_OPERATION_RESPONSE = {
  "operation_id": "string",
  "status": "pending",
  "operation_type": "graph_creation",
  "created_at": "2024-01-01T00:00:00Z",
  "message": "Operation started. Connect to stream endpoint for real-time updates.",
}


def create_sse_response_starlette(
  operation_id: str,
  user_id: str,
  from_sequence: int = 0,
  request: Request | None = None,
) -> EventSourceResponse:
  """Wrap `create_sse_stream_starlette` in an `EventSourceResponse`."""
  return EventSourceResponse(
    create_sse_stream_starlette(operation_id, user_id, from_sequence, request),
    headers={
      "Cache-Control": "no-cache",
      "X-Accel-Buffering": "no",  # Disable nginx buffering
      "Connection": "keep-alive",
    },
    ping=30,  # Automatic keep-alive ping every 30 seconds
  )
