"""
Server-Sent Events streaming for unified operation monitoring.

This module provides the SSE endpoint and streaming utilities for
real-time operation monitoring with event replay capability.
"""

import asyncio
import json
from collections.abc import AsyncGenerator
from datetime import UTC
from typing import Any

from fastapi import HTTPException, Request
from sse_starlette.sse import EventSourceResponse

from robosystems.config import env
from robosystems.logger import logger
from robosystems.middleware.sse.event_storage import (
  EventType,
  OperationStatus,
  SSEEvent,
  get_event_storage,
)


class SSEConnectionManager:
  """
  Manages Server-Sent Event connections for operations.

  Handles connection lifecycle, event distribution, and cleanup for SSE streams.
  """

  def __init__(self):
    """Initialize the connection manager."""
    self.connections: dict[str, set[str]] = {}  # operation_id -> set of connection_ids
    self.connection_queues: dict[
      str, asyncio.Queue
    ] = {}  # connection_key -> event queue
    self.user_connections: dict[str, set[str]] = {}  # user_id -> set of connection_keys
    self._lock = asyncio.Lock()

    # Configuration from environment
    self.max_connections_per_user = env.MAX_SSE_CONNECTIONS_PER_USER
    self.queue_size = env.SSE_QUEUE_SIZE
    self.sse_enabled = env.SSE_ENABLED

  async def add_connection(
    self, operation_id: str, connection_id: str, user_id: str
  ) -> asyncio.Queue:
    """
    Add a new SSE connection for an operation.

    Args:
        operation_id: Operation being monitored
        connection_id: Unique connection identifier
        user_id: User identifier for connection limits

    Returns:
        asyncio.Queue: Queue for sending events to this connection

    Raises:
        HTTPException: If user has exceeded connection limit
    """
    async with self._lock:
      # Check per-user connection limit
      if user_id in self.user_connections:
        user_connection_count = len(self.user_connections[user_id])
        if user_connection_count >= self.max_connections_per_user:
          logger.warning(
            f"User {user_id} exceeded SSE connection limit ({user_connection_count}/{self.max_connections_per_user})"
          )
          # Emit OpenTelemetry metric for connection limit exceeded
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

      # Add to connections set
      if operation_id not in self.connections:
        self.connections[operation_id] = set()
      self.connections[operation_id].add(connection_id)

      # Track user connections
      if user_id not in self.user_connections:
        self.user_connections[user_id] = set()
      queue_key = f"{operation_id}:{connection_id}"
      self.user_connections[user_id].add(queue_key)

      # Create event queue for this connection with configurable size
      queue = asyncio.Queue(maxsize=self.queue_size)
      self.connection_queues[queue_key] = queue

      logger.debug(
        f"Added SSE connection {connection_id} for operation {operation_id} (user: {user_id})"
      )

      # Emit OpenTelemetry metric for connection added
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
    """
    Remove an SSE connection.

    Args:
        operation_id: Operation being monitored
        connection_id: Connection identifier
        user_id: Optional user identifier for cleanup
    """
    async with self._lock:
      # Remove from connections set
      if operation_id in self.connections:
        self.connections[operation_id].discard(connection_id)

        # Clean up empty operation
        if not self.connections[operation_id]:
          del self.connections[operation_id]

      # Remove event queue
      queue_key = f"{operation_id}:{connection_id}"
      if queue_key in self.connection_queues:
        del self.connection_queues[queue_key]

      # Remove from user connections tracking
      if user_id and user_id in self.user_connections:
        self.user_connections[user_id].discard(queue_key)
        # Clean up empty user entry
        if not self.user_connections[user_id]:
          del self.user_connections[user_id]

      logger.debug(
        f"Removed SSE connection {connection_id} for operation {operation_id}"
      )

      # Emit OpenTelemetry metric for connection closed
      try:
        from robosystems.middleware.otel.metrics import get_endpoint_metrics

        metrics = get_endpoint_metrics()
        metrics.record_sse_connection_closed(user_id or "unknown", operation_id)
      except Exception:
        pass  # Don't fail if metrics aren't available

  async def broadcast_event(self, operation_id: str, event: SSEEvent):
    """
    Broadcast an event to all active connections for an operation.

    Args:
        operation_id: Operation identifier
        event: Event to broadcast
    """
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

      # Send to all active connections
      for connection_id in self.connections[operation_id].copy():
        queue_key = f"{operation_id}:{connection_id}"
        if queue_key in self.connection_queues:
          try:
            # Non-blocking put (connection should be consuming)
            self.connection_queues[queue_key].put_nowait(event)
            logger.debug(f"Event queued for connection {connection_id}")
          except asyncio.QueueFull:
            logger.warning(f"Queue full for connection {connection_id}, removing")
            # Try to notify client of error before removal
            await self._handle_connection_error(
              operation_id, connection_id, "Queue overflow"
            )
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
    """Handle connection error by trying to notify the client."""
    queue_key = f"{operation_id}:{connection_id}"
    if queue_key in self.connection_queues:
      try:
        # Try to send error event to client
        error_event = SSEEvent(
          event_type=EventType.OPERATION_ERROR,
          operation_id=operation_id,
          timestamp="",
          data={"error": error_message, "connection_closed": True},
          sequence_number=0,
        )
        # Use put_nowait to avoid blocking
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
  """
  Create an SSE stream for an operation using sse-starlette format.

  Args:
      operation_id: Operation to monitor
      user_id: User identifier (for access control)
      from_sequence: Start from this sequence number
      request: FastAPI request (for disconnect detection)

  Yields:
      Dict[str, Any]: Dictionaries with 'event' and 'data' keys for sse-starlette
  """
  import uuid

  connection_id = str(uuid.uuid4())
  event_storage = get_event_storage()
  connection_manager = get_connection_manager()

  try:
    # Verify operation exists and user has access
    metadata = await event_storage.get_operation_metadata(operation_id)
    if not metadata:
      yield {"event": "error", "data": json.dumps({"error": "Operation not found"})}
      return

    if metadata.user_id != user_id:
      yield {"event": "error", "data": json.dumps({"error": "Access denied"})}
      return

    # Send initial connection event
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

    # Replay missed events if requested
    if from_sequence > 0:
      historical_events = await event_storage.get_events(operation_id, from_sequence)
      for event in historical_events:
        if request and await request.is_disconnected():
          return
        # Convert to sse-starlette format
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

    # Add connection for real-time events with user tracking
    try:
      event_queue = await connection_manager.add_connection(
        operation_id, connection_id, user_id
      )
    except HTTPException as e:
      # Connection limit exceeded
      yield {"event": "error", "data": json.dumps({"error": e.detail})}
      return

    # Subscribe to Redis pub/sub for this operation
    try:
      from .redis_subscriber import get_redis_subscriber

      subscriber = get_redis_subscriber()
      await subscriber.subscribe_to_operation(operation_id)
    except Exception as e:
      logger.warning(
        f"Failed to subscribe to Redis channel for operation {operation_id}: {e}"
      )

    # Check if operation is already complete
    if metadata.status in [
      OperationStatus.COMPLETED,
      OperationStatus.FAILED,
      OperationStatus.CANCELLED,
    ]:
      # Operation already complete - replay all events
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
        # Small delay between events for client processing
        await asyncio.sleep(0.01)

      # Keep connection open briefly to ensure client processes events
      await asyncio.sleep(1.0)

      # Send explicit stream_end event
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

    # Stream real-time events
    while True:
      if request and await request.is_disconnected():
        break

      try:
        # Wait for next event with timeout
        event = await asyncio.wait_for(event_queue.get(), timeout=30.0)

        # Yield in sse-starlette format
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

        # Check if operation is complete
        if event.event_type in [
          EventType.OPERATION_COMPLETED,
          EventType.OPERATION_ERROR,
          EventType.OPERATION_CANCELLED,
        ]:
          # Send stream_end event
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
        # Send keepalive
        yield {
          "event": "keepalive",
          "data": json.dumps({"timestamp": str(asyncio.get_event_loop().time())}),
        }

  except Exception as e:
    logger.error(f"Error in SSE stream for operation {operation_id}: {e}")
    yield {"event": "error", "data": json.dumps({"error": str(e)})}

  finally:
    # Cleanup with user tracking
    await connection_manager.remove_connection(operation_id, connection_id, user_id)

    # Unsubscribe from Redis
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
  """
  Emit an event to all active SSE connections for an operation.

  This is used by internal services to send events to connected clients.

  Args:
      operation_id: Operation identifier
      event_type: Type of event
      data: Event data
      sequence_number: Optional sequence number

  Returns:
      bool: True if event was sent to at least one connection
  """
  from datetime import datetime

  connection_manager = get_connection_manager()

  # Create SSE event
  event = SSEEvent(
    event_type=event_type,
    operation_id=operation_id,
    timestamp=datetime.now(UTC).isoformat(),
    data=data,
    sequence_number=sequence_number or 0,
  )

  # Broadcast to all connections
  await connection_manager.broadcast_event(operation_id, event)

  # Check if any connections received it
  active_connections = await connection_manager.get_active_connections(operation_id)
  return active_connections > 0


# Initialize default SSE operation response
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
  """
  Create a FastAPI EventSourceResponse using sse-starlette.

  Args:
      operation_id: Operation to stream
      user_id: User identifier
      from_sequence: Start sequence number
      request: FastAPI request

  Returns:
      EventSourceResponse configured for SSE with automatic formatting
  """
  return EventSourceResponse(
    create_sse_stream_starlette(operation_id, user_id, from_sequence, request),
    headers={
      "Cache-Control": "no-cache",
      "X-Accel-Buffering": "no",  # Disable nginx buffering
      "Connection": "keep-alive",
    },
    ping=30,  # Automatic keep-alive ping every 30 seconds
  )
