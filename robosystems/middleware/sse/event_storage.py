"""Redis-based event storage for Server-Sent Events operations.

Events persist with a TTL so late-connecting clients can replay an
operation's history from any sequence number.
"""

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Any, cast

import redis.asyncio as redis_async
from redis import Redis
from redis.exceptions import WatchError

from robosystems.config.defaults import CacheDefaults
from robosystems.logger import logger


class EventType(str, Enum):
  OPERATION_STARTED = "operation_started"
  OPERATION_PROGRESS = "operation_progress"
  OPERATION_ERROR = "operation_error"
  OPERATION_COMPLETED = "operation_completed"
  OPERATION_CANCELLED = "operation_cancelled"
  # A run stopped at a checkpoint and needs a decision before it can go on;
  # the matching resume puts it back on the queue with the answer.
  OPERATION_AWAITING_INPUT = "operation_awaiting_input"
  OPERATION_RESUMED = "operation_resumed"

  GRAPH_CREATION_PROGRESS = "graph_creation_progress"
  AGENT_ANALYSIS_PROGRESS = "agent_analysis_progress"
  BACKUP_PROGRESS = "backup_progress"
  SYNC_PROGRESS = "sync_progress"


class OperationStatus(str, Enum):
  PENDING = "pending"
  RUNNING = "running"
  # Paused at a checkpoint, off the queue, waiting for a human answer.
  # Not terminal: a resume moves it back to RUNNING, a cancel ends it.
  AWAITING_INPUT = "awaiting_input"
  COMPLETED = "completed"
  FAILED = "failed"
  CANCELLED = "cancelled"


@dataclass
class SSEEvent:
  """A single SSE event: JSON-serializable payload plus standard metadata."""

  event_type: EventType
  operation_id: str
  timestamp: str
  data: dict[str, Any]
  sequence_number: int = 0

  def to_sse_format(self) -> str:
    """Render as a raw SSE frame; routes stream via `EventSourceResponse`."""
    lines = []
    lines.append(f"event: {self.event_type.value}")
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
    return "\n".join(lines) + "\n\n"

  def to_dict(self) -> dict[str, Any]:
    return asdict(self)

  @classmethod
  def from_dict(cls, data: dict[str, Any]) -> "SSEEvent":
    return cls(**data)


@dataclass
class OperationMetadata:
  """Tracking record for one operation: identity, status, and result."""

  operation_id: str
  operation_type: str
  user_id: str
  graph_id: str | None
  status: OperationStatus
  created_at: str
  updated_at: str
  error_message: str | None = None
  result_data: dict[str, Any] | None = None
  # Set while AWAITING_INPUT: the prompt for the human, the task's own
  # checkpoint, and the queue payload needed to re-enqueue it on resume.
  input_request: dict[str, Any] | None = None

  def to_dict(self) -> dict[str, Any]:
    return asdict(self)


_TERMINAL_FAILURE_STATUSES = frozenset(
  {OperationStatus.FAILED, OperationStatus.CANCELLED}
)
_TERMINAL_STATUSES = _TERMINAL_FAILURE_STATUSES | {OperationStatus.COMPLETED}

# Status only moves forward and the first terminal status is final, so a late
# duplicate error can't flip a completed operation or evict its idempotency
# envelope (a retry would then dispatch it again).
_STATUS_PRIORITY = {
  OperationStatus.PENDING: 0,
  OperationStatus.RUNNING: 1,
  # Same rung as RUNNING, so a run can pause and resume repeatedly.
  OperationStatus.AWAITING_INPUT: 1,
  OperationStatus.COMPLETED: 2,
  OperationStatus.FAILED: 2,
  OperationStatus.CANCELLED: 2,
}


def _next_status(event_type: EventType) -> OperationStatus | None:
  if event_type == EventType.OPERATION_STARTED:
    return OperationStatus.RUNNING
  if event_type == EventType.OPERATION_COMPLETED:
    return OperationStatus.COMPLETED
  if event_type == EventType.OPERATION_ERROR:
    return OperationStatus.FAILED
  if event_type == EventType.OPERATION_CANCELLED:
    return OperationStatus.CANCELLED
  if event_type == EventType.OPERATION_AWAITING_INPUT:
    return OperationStatus.AWAITING_INPUT
  if event_type == EventType.OPERATION_RESUMED:
    return OperationStatus.RUNNING
  return None


_STATUS_CHANGING_EVENTS = frozenset(
  {
    EventType.OPERATION_STARTED,
    EventType.OPERATION_COMPLETED,
    EventType.OPERATION_ERROR,
    EventType.OPERATION_CANCELLED,
    EventType.OPERATION_AWAITING_INPUT,
    EventType.OPERATION_RESUMED,
  }
)


def _apply_input_request(
  metadata: "OperationMetadata", event_type: EventType, data: dict[str, Any]
) -> None:
  """Record the pause's request on the metadata, and clear it once the run
  is going again — on the resume, and on a worker pickup (the two share a
  rung, so a started event can follow a pause directly)."""
  if event_type == EventType.OPERATION_AWAITING_INPUT:
    metadata.input_request = data.get("input_request")
  elif event_type in (EventType.OPERATION_RESUMED, EventType.OPERATION_STARTED):
    metadata.input_request = None


def _transition_allowed(current: OperationStatus, new: OperationStatus) -> bool:
  """Whether ``current`` → ``new`` is a forward move.

  Same-status re-writes are allowed (a completion event may arrive twice and
  merge result data); a different terminal status after a terminal one is not.
  """
  if current in _TERMINAL_STATUSES and new != current:
    return False
  return _STATUS_PRIORITY.get(new, 0) >= _STATUS_PRIORITY.get(current, 0)


async def _invalidate_idempotency(operation_id: str) -> None:
  """Evict the cached ``pending`` envelope of a failed or cancelled operation,
  so a retry dispatches again. Lazy import: this store must not need FastAPI.
  """
  from robosystems.middleware.operations import invalidate_operation_idempotency

  try:
    await invalidate_operation_idempotency(operation_id)
  except Exception as exc:
    logger.warning(
      f"Idempotency eviction failed for terminal operation {operation_id}: {exc}"
    )


def _invalidate_idempotency_sync(operation_id: str) -> None:
  """Sync counterpart to ``_invalidate_idempotency`` for ``store_event_sync``."""
  from robosystems.middleware.operations import (
    invalidate_operation_idempotency_sync,
  )

  try:
    invalidate_operation_idempotency_sync(operation_id)
  except Exception as exc:
    logger.warning(
      f"Idempotency eviction failed for terminal operation {operation_id}: {exc}"
    )


class SSEEventStorage:
  """Redis-backed SSE event store with TTL expiry.

  Events sit in a per-operation sorted set by sequence number for replay,
  and are published to `sse:events:{operation_id}` for other processes.
  """

  def __init__(
    self,
    redis_client: redis_async.Redis | None = None,
    default_ttl: int = CacheDefaults.LONG,
  ):
    self._redis_client = redis_client
    self._async_redis = None
    self._sync_redis = None
    self.default_ttl = default_ttl

    self.event_prefix = "sse:operation:events:"
    self.metadata_prefix = "sse:operation:meta:"
    self.sequence_prefix = "sse:operation:seq:"

  async def _get_redis(self) -> redis_async.Redis:
    if self._async_redis is None:
      if self._redis_client:
        self._async_redis = self._redis_client
      else:
        self._async_redis = await self._get_default_async_redis()
    return self._async_redis

  async def _get_default_async_redis(self) -> redis_async.Redis:
    from robosystems.config.valkey_registry import (
      ValkeyDatabase,
      create_async_redis_client,
    )

    client = create_async_redis_client(ValkeyDatabase.SSE)
    await client.ping()
    return client

  def _get_sync_redis(self) -> Redis:
    if self._sync_redis is None:
      from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client

      self._sync_redis = create_redis_client(ValkeyDatabase.SSE)
      self._sync_redis.ping()
    return self._sync_redis

  def generate_operation_id(self) -> str:
    from robosystems.utils.ulid import generate_prefixed_ulid

    return generate_prefixed_ulid("op")

  async def create_operation(
    self,
    operation_type: str,
    user_id: str,
    graph_id: str | None = None,
    operation_id: str | None = None,
    ttl: int | None = None,
  ) -> str:
    """Register a new operation and return its `op_`-prefixed ID."""
    if operation_id is None:
      operation_id = self.generate_operation_id()

    ttl = ttl or self.default_ttl
    now = datetime.now(UTC).isoformat()

    metadata = OperationMetadata(
      operation_id=operation_id,
      operation_type=operation_type,
      user_id=user_id,
      graph_id=graph_id,
      status=OperationStatus.PENDING,
      created_at=now,
      updated_at=now,
    )

    redis = await self._get_redis()
    metadata_key = f"{self.metadata_prefix}{operation_id}"
    await redis.setex(metadata_key, ttl, json.dumps(metadata.to_dict()))

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
    data: dict[str, Any],
    ttl: int | None = None,
  ) -> SSEEvent:
    """Append an event to an operation and publish it to subscribers.

    Raises `ValueError` if the operation was never created (or has expired).
    """
    ttl = ttl or self.default_ttl

    redis = await self._get_redis()
    metadata_key = f"{self.metadata_prefix}{operation_id}"
    if not await redis.exists(metadata_key):
      raise ValueError(f"Operation {operation_id} not found")

    seq_key = f"{self.sequence_prefix}{operation_id}"
    sequence_number = await redis.incr(seq_key)

    event = SSEEvent(
      event_type=event_type,
      operation_id=operation_id,
      timestamp=datetime.now(UTC).isoformat(),
      data=data,
      sequence_number=sequence_number,
    )

    events_key = f"{self.event_prefix}{operation_id}"
    await redis.zadd(events_key, {json.dumps(event.to_dict()): sequence_number})

    await redis.expire(events_key, ttl)

    # Pub/sub so the API process sees events emitted by worker processes.
    channel = f"sse:events:{operation_id}"
    await redis.publish(channel, json.dumps(event.to_dict()))

    await self._update_operation_metadata(operation_id, event_type, data)

    logger.debug(
      f"Stored event {event_type} for operation {operation_id} (seq: {sequence_number}), published to {channel}"
    )

    return event

  def store_event_sync(
    self,
    operation_id: str,
    event_type: EventType,
    data: dict[str, Any],
    ttl: int | None = None,
  ) -> SSEEvent:
    """Sync counterpart to `store_event` for background tasks.

    Unlike the async version this only warns on an unknown operation,
    since a worker may emit events before the API has registered them.
    """
    ttl = ttl or self.default_ttl

    redis = self._get_sync_redis()
    metadata_key = f"{self.metadata_prefix}{operation_id}"
    if not redis.exists(metadata_key):
      logger.warning(
        f"Operation {operation_id} not found in metadata, continuing anyway"
      )

    seq_key = f"{self.sequence_prefix}{operation_id}"
    sequence_number = cast(int, redis.incr(seq_key))

    event = SSEEvent(
      event_type=event_type,
      operation_id=operation_id,
      timestamp=datetime.now(UTC).isoformat(),
      data=data,
      sequence_number=sequence_number,
    )

    events_key = f"{self.event_prefix}{operation_id}"
    redis.zadd(events_key, {json.dumps(event.to_dict()): sequence_number})

    redis.expire(events_key, ttl)

    # Pub/sub so the API process sees events emitted by worker processes.
    channel = f"sse:events:{operation_id}"
    redis.publish(channel, json.dumps(event.to_dict()))

    self._update_operation_metadata_sync(operation_id, event_type, data)

    logger.debug(
      f"[SYNC] Stored event {event_type} for operation {operation_id} (seq: {sequence_number}), published to {channel}"
    )

    return event

  def _update_operation_metadata_sync(
    self, operation_id: str, event_type: EventType, data: dict[str, Any]
  ):
    """Sync counterpart to `_update_operation_metadata`."""
    if event_type not in _STATUS_CHANGING_EVENTS:
      return

    redis = self._get_sync_redis()
    metadata_key = f"{self.metadata_prefix}{operation_id}"
    terminal_failure = False

    with redis.pipeline() as pipe:
      try:
        pipe.watch(metadata_key)

        metadata_json = redis.get(metadata_key)
        if not metadata_json:
          pipe.unwatch()
          return

        metadata_dict = json.loads(str(metadata_json))
        metadata = OperationMetadata(**metadata_dict)

        new_status = _next_status(event_type)

        if new_status and _transition_allowed(metadata.status, new_status):
          # Evict only once this write lands: a lost CAS means another event,
          # possibly a completion, won the transition.
          entering_failure = (
            new_status in _TERMINAL_FAILURE_STATUSES
            and metadata.status not in _TERMINAL_STATUSES
          )
          metadata.status = new_status
          metadata.updated_at = datetime.now(UTC).isoformat()

          if event_type == EventType.OPERATION_COMPLETED:
            # Merge, so a graph_id recorded by the Dagster job survives.
            new_result = data.get("result") or {}
            if metadata.result_data:
              metadata.result_data.update(new_result)
            else:
              metadata.result_data = new_result
          elif event_type == EventType.OPERATION_ERROR:
            metadata.error_message = data.get("error", "Unknown error")
          else:
            _apply_input_request(metadata, event_type, data)

          pipe.multi()
          pipe.setex(metadata_key, self.default_ttl, json.dumps(metadata.to_dict()))
          pipe.execute()
          terminal_failure = entering_failure
        else:
          pipe.unwatch()

      except WatchError:
        # Another status-changing event won the transition.
        logger.debug(
          f"[SYNC] Metadata update skipped for {operation_id} due to concurrent modification"
        )
      except Exception as e:
        logger.warning(f"[SYNC] Metadata update failed for {operation_id}: {e}")

    if terminal_failure:
      _invalidate_idempotency_sync(operation_id)

  def update_operation_result_sync(
    self, operation_id: str, result: dict[str, Any]
  ) -> None:
    """Merge result data into an operation's metadata, so the completion
    event the monitor emits later carries it."""
    redis = self._get_sync_redis()
    metadata_key = f"{self.metadata_prefix}{operation_id}"
    metadata_json = redis.get(metadata_key)

    if not metadata_json:
      logger.warning(f"Operation {operation_id} not found, cannot update result")
      return

    metadata_dict = json.loads(str(metadata_json))
    metadata = OperationMetadata(**metadata_dict)

    metadata.updated_at = datetime.now(UTC).isoformat()

    if metadata.result_data:
      metadata.result_data.update(result)
    else:
      metadata.result_data = result

    if "graph_id" in result:
      metadata.graph_id = result["graph_id"]

    redis.setex(
      metadata_key,
      self.default_ttl,
      json.dumps(metadata.to_dict()),
    )

    logger.debug(
      f"Updated operation {operation_id} result data with keys: {list(result.keys())}"
    )

  def get_operation_result_sync(self, operation_id: str) -> dict[str, Any] | None:
    """Return an operation's stored result data, or None if absent."""
    redis = self._get_sync_redis()
    metadata_key = f"{self.metadata_prefix}{operation_id}"
    metadata_json = redis.get(metadata_key)

    if not metadata_json:
      return None

    try:
      metadata_dict = json.loads(str(metadata_json))
      metadata = OperationMetadata(**metadata_dict)
      return metadata.result_data
    except Exception as e:
      logger.warning(f"Failed to get operation result for {operation_id}: {e}")
      return None

  async def _update_operation_metadata(
    self, operation_id: str, event_type: EventType, data: dict[str, Any]
  ):
    """Advance operation status from a status-changing event.

    Progress events are skipped: they carry no status and would race a
    concurrent terminal write.
    """
    if event_type not in _STATUS_CHANGING_EVENTS:
      return

    redis = await self._get_redis()
    metadata_key = f"{self.metadata_prefix}{operation_id}"
    terminal_failure = False

    # WATCH must run on the pipeline: on the client it is a no-op in redis-py.
    async with redis.pipeline() as pipe:
      try:
        await pipe.watch(metadata_key)

        metadata_json = await pipe.get(metadata_key)
        if not metadata_json:
          await pipe.unwatch()
          return

        metadata_dict = json.loads(metadata_json)
        metadata = OperationMetadata(**metadata_dict)

        new_status = _next_status(event_type)

        if new_status and _transition_allowed(metadata.status, new_status):
          entering_failure = (
            new_status in _TERMINAL_FAILURE_STATUSES
            and metadata.status not in _TERMINAL_STATUSES
          )
          metadata.status = new_status
          metadata.updated_at = datetime.now(UTC).isoformat()

          if event_type == EventType.OPERATION_COMPLETED:
            new_result = data.get("result") or {}
            if metadata.result_data:
              metadata.result_data.update(new_result)
            else:
              metadata.result_data = new_result
          elif event_type == EventType.OPERATION_ERROR:
            metadata.error_message = data.get("error", "Unknown error")
          else:
            _apply_input_request(metadata, event_type, data)

          ttl = await pipe.ttl(metadata_key)
          if ttl > 0:
            pipe.multi()
            pipe.setex(metadata_key, ttl, json.dumps(metadata.to_dict()))
            await pipe.execute()
            terminal_failure = entering_failure
          else:
            await pipe.unwatch()
        else:
          await pipe.unwatch()

      except WatchError:
        # Another status-changing event won the transition.
        logger.debug(
          f"Metadata update skipped for {operation_id} due to concurrent modification"
        )
      except Exception as e:
        logger.warning(f"Metadata update failed for {operation_id}: {e}")

    if terminal_failure:
      await _invalidate_idempotency(operation_id)

  async def get_events(
    self, operation_id: str, from_sequence: int = 0, limit: int | None = None
  ) -> list[SSEEvent]:
    """Return an operation's events in sequence order, from `from_sequence`."""
    redis = await self._get_redis()
    events_key = f"{self.event_prefix}{operation_id}"

    if limit:
      raw_events = await redis.zrangebyscore(
        events_key, from_sequence, "+inf", start=0, num=limit
      )
    else:
      raw_events = await redis.zrangebyscore(events_key, from_sequence, "+inf")

    events = []
    for raw_event in raw_events:
      try:
        event_dict = json.loads(raw_event)
        events.append(SSEEvent(**event_dict))
      except (json.JSONDecodeError, TypeError) as e:
        logger.warning(f"Failed to parse event for operation {operation_id}: {e}")
        continue

    return events

  async def get_operation_metadata(self, operation_id: str) -> OperationMetadata | None:
    """Return an operation's metadata, or None if it is unknown or expired."""
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
    await self.store_event(
      operation_id, EventType.OPERATION_CANCELLED, {"reason": reason}
    )

  async def cleanup_expired_operations(self) -> int:
    """Backstop for operations whose keys ended up without a TTL.

    Redis expiry normally handles cleanup; this re-applies the default TTL
    to any metadata key missing one and returns how many it repaired.
    """
    cleaned = 0
    redis = await self._get_redis()

    async for key in redis.scan_iter(match=f"{self.metadata_prefix}*"):
      if not await redis.exists(key):
        continue

      ttl = await redis.ttl(key)
      if ttl == -1:
        await redis.expire(key, self.default_ttl)
        cleaned += 1

    logger.info(f"Cleaned up {cleaned} expired operations")
    return cleaned


_event_storage: SSEEventStorage | None = None


def get_event_storage() -> SSEEventStorage:
  global _event_storage
  if _event_storage is None:
    _event_storage = SSEEventStorage()
  return _event_storage
