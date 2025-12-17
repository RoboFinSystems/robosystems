"""
Redis-based event storage for Server-Sent Events operations.

This module provides persistent event storage with TTL cleanup for SSE operations,
enabling event replay for late connections and reliable operation monitoring.
"""

import json
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, cast
from dataclasses import dataclass, asdict
from enum import Enum

import redis.asyncio as redis_async
from redis import Redis

from robosystems.logger import logger


class EventType(str, Enum):
  """Standard event types for SSE operations."""

  OPERATION_STARTED = "operation_started"
  OPERATION_PROGRESS = "operation_progress"
  OPERATION_ERROR = "operation_error"
  OPERATION_COMPLETED = "operation_completed"
  OPERATION_CANCELLED = "operation_cancelled"

  # Custom event types for specific operations
  GRAPH_CREATION_PROGRESS = "graph_creation_progress"
  AGENT_ANALYSIS_PROGRESS = "agent_analysis_progress"
  BACKUP_PROGRESS = "backup_progress"
  SYNC_PROGRESS = "sync_progress"


class OperationStatus(str, Enum):
  """Operation status values."""

  PENDING = "pending"
  RUNNING = "running"
  COMPLETED = "completed"
  FAILED = "failed"
  CANCELLED = "cancelled"


@dataclass
class SSEEvent:
  """
  Structured event for SSE operations.

  All events are JSON-serializable and include standard metadata
  for consistent client handling.
  """

  event_type: EventType
  operation_id: str
  timestamp: str
  data: Dict[str, Any]
  sequence_number: int = 0

  def to_sse_format(self) -> str:
    """
    Convert to Server-Sent Events format.

    Note: This method is kept for backwards compatibility.
    New code should use sse-starlette's EventSourceResponse instead.
    """
    lines = []
    lines.append(f"event: {self.event_type.value}")
    # Ensure JSON is on a single line for SSE format
    data_json = json.dumps(
      {
        "operation_id": self.operation_id,
        "timestamp": self.timestamp,
        "sequence_number": self.sequence_number,
        **self.data,
      },
      separators=(",", ":"),
      ensure_ascii=False,
      default=str,
    )
    lines.append(f"data: {data_json}")
    # SSE format requires double newline to terminate event
    return "\n".join(lines) + "\n\n"

  def to_dict(self) -> Dict[str, Any]:
    """Convert to dictionary for JSON serialization."""
    return asdict(self)

  @classmethod
  def from_dict(cls, data: Dict[str, Any]) -> "SSEEvent":
    """Create SSEEvent from dictionary."""
    return cls(**data)


@dataclass
class OperationMetadata:
  """
  Metadata about an operation for tracking and management.
  """

  operation_id: str
  operation_type: str
  user_id: str
  graph_id: Optional[str]
  status: OperationStatus
  created_at: str
  updated_at: str
  error_message: Optional[str] = None
  result_data: Optional[Dict[str, Any]] = None

  def to_dict(self) -> Dict[str, Any]:
    """Convert to dictionary for JSON serialization."""
    return asdict(self)


class SSEEventStorage:
  """
  Redis-based storage for SSE events with automatic TTL cleanup.

  Provides event persistence for replay capability and operation tracking.
  Events are stored per operation with automatic expiration.
  """

  def __init__(
    self, redis_client: Optional[redis_async.Redis] = None, default_ttl: int = 3600
  ):
    """
    Initialize event storage.

    Args:
        redis_client: Async Redis client instance (uses default if None)
        default_ttl: Default TTL for events in seconds (1 hour default)
    """
    self._redis_client = redis_client
    self._async_redis = None
    self._sync_redis = None  # For sync methods (background tasks)
    self.default_ttl = default_ttl

    # Redis key prefixes
    self.event_prefix = "sse:operation:events:"
    self.metadata_prefix = "sse:operation:meta:"
    self.sequence_prefix = "sse:operation:seq:"

  async def _get_redis(self) -> redis_async.Redis:
    """Get async Redis client, creating if needed."""
    if self._async_redis is None:
      if self._redis_client:
        self._async_redis = self._redis_client
      else:
        self._async_redis = await self._get_default_async_redis()
    return self._async_redis

  async def _get_default_async_redis(self) -> redis_async.Redis:
    """Get default async Redis client from environment."""
    from robosystems.config.valkey_registry import ValkeyDatabase
    from robosystems.config.valkey_registry import create_async_redis_client

    # Use SSE events database from registry with proper ElastiCache support
    client = create_async_redis_client(ValkeyDatabase.SSE_EVENTS)
    # Test connection
    await client.ping()
    return client

  def _get_sync_redis(self) -> Redis:
    """Get synchronous Redis client for background tasks."""
    if self._sync_redis is None:
      from robosystems.config.valkey_registry import ValkeyDatabase
      from robosystems.config.valkey_registry import create_redis_client

      # Use SSE events database from registry with proper ElastiCache support
      self._sync_redis = create_redis_client(ValkeyDatabase.SSE_EVENTS)
      # Test connection
      self._sync_redis.ping()
    return self._sync_redis

  def generate_operation_id(self) -> str:
    """Generate a unique operation ID."""
    return str(uuid.uuid4())

  async def create_operation(
    self,
    operation_type: str,
    user_id: str,
    graph_id: Optional[str] = None,
    operation_id: Optional[str] = None,
    ttl: Optional[int] = None,
  ) -> str:
    """
    Create a new operation and return its ID.

    Args:
        operation_type: Type of operation (e.g., "graph_creation", "agent_analysis")
        user_id: User who initiated the operation
        graph_id: Graph database ID (if applicable)
        operation_id: Custom operation ID (generates one if None)
        ttl: Custom TTL for this operation (uses default if None)

    Returns:
        str: The operation ID
    """
    if operation_id is None:
      operation_id = self.generate_operation_id()

    ttl = ttl or self.default_ttl
    now = datetime.now(timezone.utc).isoformat()

    # Create operation metadata
    metadata = OperationMetadata(
      operation_id=operation_id,
      operation_type=operation_type,
      user_id=user_id,
      graph_id=graph_id,
      status=OperationStatus.PENDING,
      created_at=now,
      updated_at=now,
    )

    # Store metadata with TTL
    redis = await self._get_redis()
    metadata_key = f"{self.metadata_prefix}{operation_id}"
    await redis.setex(metadata_key, ttl, json.dumps(metadata.to_dict()))

    # Initialize sequence counter
    seq_key = f"{self.sequence_prefix}{operation_id}"
    await redis.setex(seq_key, ttl, "0")

    logger.info(
      f"Created SSE operation {operation_id} of type {operation_type} for user {user_id}"
    )

    return operation_id

  async def store_event(
    self,
    operation_id: str,
    event_type: EventType,
    data: Dict[str, Any],
    ttl: Optional[int] = None,
  ) -> SSEEvent:
    """
    Store an event for an operation.

    Args:
        operation_id: Operation identifier
        event_type: Type of event
        data: Event data
        ttl: Custom TTL (uses default if None)

    Returns:
        SSEEvent: The stored event

    Raises:
        ValueError: If operation doesn't exist
    """
    ttl = ttl or self.default_ttl

    # Check if operation exists
    redis = await self._get_redis()
    metadata_key = f"{self.metadata_prefix}{operation_id}"
    if not await redis.exists(metadata_key):
      raise ValueError(f"Operation {operation_id} not found")

    # Get and increment sequence number
    seq_key = f"{self.sequence_prefix}{operation_id}"
    sequence_number = await redis.incr(seq_key)

    # Create event
    event = SSEEvent(
      event_type=event_type,
      operation_id=operation_id,
      timestamp=datetime.now(timezone.utc).isoformat(),
      data=data,
      sequence_number=sequence_number,
    )

    # Store event in a sorted set by sequence number
    events_key = f"{self.event_prefix}{operation_id}"
    await redis.zadd(events_key, {json.dumps(event.to_dict()): sequence_number})

    # Set TTL on events set
    await redis.expire(events_key, ttl)

    # Publish event to Redis pub/sub channel for real-time notifications
    # This allows the API process to receive events from worker processes
    channel = f"sse:events:{operation_id}"
    await redis.publish(channel, json.dumps(event.to_dict()))

    # Update operation metadata timestamp and possibly status
    await self._update_operation_metadata(operation_id, event_type, data)

    logger.debug(
      f"Stored event {event_type} for operation {operation_id} (seq: {sequence_number}), published to {channel}"
    )

    return event

  def store_event_sync(
    self,
    operation_id: str,
    event_type: EventType,
    data: Dict[str, Any],
    ttl: Optional[int] = None,
  ) -> SSEEvent:
    """
    Synchronous version of store_event for use in background tasks.

    Args:
        operation_id: Operation identifier
        event_type: Type of event
        data: Event data
        ttl: Custom TTL (uses default if None)

    Returns:
        SSEEvent: The stored event

    Raises:
        ValueError: If operation doesn't exist
    """
    ttl = ttl or self.default_ttl

    # Check if operation exists
    redis = self._get_sync_redis()
    metadata_key = f"{self.metadata_prefix}{operation_id}"
    if not redis.exists(metadata_key):
      # For sync context, we might not have the operation created yet
      # Just log and continue
      logger.warning(
        f"Operation {operation_id} not found in metadata, continuing anyway"
      )

    # Get and increment sequence number
    seq_key = f"{self.sequence_prefix}{operation_id}"
    sequence_number = cast(int, redis.incr(seq_key))

    # Create event
    event = SSEEvent(
      event_type=event_type,
      operation_id=operation_id,
      timestamp=datetime.now(timezone.utc).isoformat(),
      data=data,
      sequence_number=sequence_number,
    )

    # Store event in a sorted set by sequence number
    events_key = f"{self.event_prefix}{operation_id}"
    redis.zadd(events_key, {json.dumps(event.to_dict()): sequence_number})

    # Set TTL on events set
    redis.expire(events_key, ttl)

    # Publish event to Redis pub/sub channel for real-time notifications
    channel = f"sse:events:{operation_id}"
    redis.publish(channel, json.dumps(event.to_dict()))

    # For sync version, update metadata directly
    self._update_operation_metadata_sync(operation_id, event_type, data)

    logger.debug(
      f"[SYNC] Stored event {event_type} for operation {operation_id} (seq: {sequence_number}), published to {channel}"
    )

    return event

  def _update_operation_metadata_sync(
    self, operation_id: str, event_type: EventType, data: Dict[str, Any]
  ):
    """Synchronous version to update operation metadata based on event type."""
    redis = self._get_sync_redis()
    metadata_key = f"{self.metadata_prefix}{operation_id}"
    metadata_json = redis.get(metadata_key)

    if not metadata_json:
      # Create minimal metadata if it doesn't exist
      metadata = OperationMetadata(
        operation_id=operation_id,
        operation_type="graph_creation",
        user_id=data.get("user_id", "unknown"),
        graph_id=data.get("graph_id"),
        status=OperationStatus.RUNNING,
        created_at=datetime.now(timezone.utc).isoformat(),
        updated_at=datetime.now(timezone.utc).isoformat(),
      )
      metadata_dict = metadata.to_dict()
    else:
      metadata_dict = json.loads(str(metadata_json))
      metadata = OperationMetadata(**metadata_dict)

    # Update metadata based on event type
    metadata.updated_at = datetime.now(timezone.utc).isoformat()

    if event_type == EventType.OPERATION_COMPLETED:
      metadata.status = OperationStatus.COMPLETED
      metadata.result_data = data
    elif event_type == EventType.OPERATION_ERROR:
      metadata.status = OperationStatus.FAILED
      metadata.error_message = data.get("error")
    elif event_type == EventType.OPERATION_CANCELLED:
      metadata.status = OperationStatus.CANCELLED
    elif event_type == EventType.OPERATION_STARTED:
      metadata.status = OperationStatus.RUNNING

    # Update in Redis
    redis.setex(
      metadata_key,
      self.default_ttl,
      json.dumps(metadata.to_dict()),
    )

  async def _update_operation_metadata(
    self, operation_id: str, event_type: EventType, data: Dict[str, Any]
  ):
    """Update operation metadata based on event type."""
    redis = await self._get_redis()
    metadata_key = f"{self.metadata_prefix}{operation_id}"
    metadata_json = await redis.get(metadata_key)

    if not metadata_json:
      return

    metadata_dict = json.loads(metadata_json)
    metadata = OperationMetadata(**metadata_dict)

    # Update timestamp
    metadata.updated_at = datetime.now(timezone.utc).isoformat()

    # Update status based on event type
    if event_type == EventType.OPERATION_STARTED:
      metadata.status = OperationStatus.RUNNING
    elif event_type == EventType.OPERATION_COMPLETED:
      metadata.status = OperationStatus.COMPLETED
      metadata.result_data = data.get("result")
    elif event_type == EventType.OPERATION_ERROR:
      metadata.status = OperationStatus.FAILED
      metadata.error_message = data.get("error", "Unknown error")
    elif event_type == EventType.OPERATION_CANCELLED:
      metadata.status = OperationStatus.CANCELLED

    # Store updated metadata
    ttl = await redis.ttl(metadata_key)
    if ttl > 0:
      await redis.setex(metadata_key, ttl, json.dumps(metadata.to_dict()))

  async def get_events(
    self, operation_id: str, from_sequence: int = 0, limit: Optional[int] = None
  ) -> List[SSEEvent]:
    """
    Retrieve events for an operation.

    Args:
        operation_id: Operation identifier
        from_sequence: Start from this sequence number (inclusive)
        limit: Maximum number of events to return

    Returns:
        List[SSEEvent]: Events in sequence order
    """
    redis = await self._get_redis()
    events_key = f"{self.event_prefix}{operation_id}"

    # Get events from sorted set
    if limit:
      raw_events = await redis.zrangebyscore(
        events_key, from_sequence, "+inf", start=0, num=limit
      )
    else:
      raw_events = await redis.zrangebyscore(events_key, from_sequence, "+inf")

    # Parse events
    events = []
    for raw_event in raw_events:
      try:
        event_dict = json.loads(raw_event)
        events.append(SSEEvent(**event_dict))
      except (json.JSONDecodeError, TypeError) as e:
        logger.warning(f"Failed to parse event for operation {operation_id}: {e}")
        continue

    return events

  async def get_operation_metadata(
    self, operation_id: str
  ) -> Optional[OperationMetadata]:
    """
    Get operation metadata.

    Args:
        operation_id: Operation identifier

    Returns:
        OperationMetadata or None if not found
    """
    redis = await self._get_redis()
    metadata_key = f"{self.metadata_prefix}{operation_id}"
    metadata_json = await redis.get(metadata_key)

    if not metadata_json:
      return None

    try:
      metadata_dict = json.loads(metadata_json)
      return OperationMetadata(**metadata_dict)
    except (json.JSONDecodeError, TypeError) as e:
      logger.warning(f"Failed to parse metadata for operation {operation_id}: {e}")
      return None

  async def cancel_operation(
    self, operation_id: str, reason: str = "Cancelled by user"
  ):
    """
    Cancel an operation by storing a cancellation event.

    Args:
        operation_id: Operation identifier
        reason: Cancellation reason
    """
    await self.store_event(
      operation_id, EventType.OPERATION_CANCELLED, {"reason": reason}
    )

  async def cleanup_expired_operations(self) -> int:
    """
    Clean up expired operations (Redis should handle TTL, but this is a backup).

    Returns:
        int: Number of operations cleaned up
    """
    # This is mainly for manual cleanup or operations that somehow didn't get TTL
    cleaned = 0
    redis = await self._get_redis()

    # Scan for expired metadata keys
    async for key in redis.scan_iter(match=f"{self.metadata_prefix}*"):
      if not await redis.exists(key):
        # Already expired
        continue

      ttl = await redis.ttl(key)
      if ttl == -1:  # No TTL set (shouldn't happen)
        # Set default TTL
        await redis.expire(key, self.default_ttl)
        cleaned += 1

    logger.info(f"Cleaned up {cleaned} expired operations")
    return cleaned


# Global instance (initialized lazily)
_event_storage: Optional[SSEEventStorage] = None


def get_event_storage() -> SSEEventStorage:
  """Get the global event storage instance."""
  global _event_storage
  if _event_storage is None:
    _event_storage = SSEEventStorage()
  return _event_storage
