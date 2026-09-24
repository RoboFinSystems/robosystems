"""Operation envelope, idempotency cache, and audit log shared by every
operation surface (extensions, graph lifecycle ops).

Operation IDs are `op_`-prefixed ULIDs, the pattern the
`/v1/operations/{operation_id}/stream` SSE endpoint accepts.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Generic, Literal, TypeVar

import anyio
from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, Field

from robosystems.config.valkey_registry import (
  ValkeyDatabase,
  create_async_redis_client,
  create_redis_client,
)
from robosystems.logger import logger
from robosystems.security.request_context import audit_context
from robosystems.utils.ulid import generate_prefixed_ulid

# `default=Any` (PEP 696) keeps an unparameterized `OperationEnvelope` loose.
TResult = TypeVar("TResult", default=Any)

OperationStatus = Literal["completed", "pending", "failed"]

IDEMPOTENCY_TTL_SECONDS = 24 * 60 * 60

# How long a key stays reserved while its first request runs: longer than
# the slowest operation (a close with QB writeback), short enough that a dead
# request doesn't block its own retry. An overrun just loses the guard.
IDEMPOTENCY_RESERVATION_TTL_SECONDS = 5 * 60

# Binds operation id -> pending envelope keys (a set: worker-side dedup can
# give two keys one operation), so a terminal failure can evict them and a
# retry dispatches again instead of replaying `pending`.
IDEMPOTENCY_BINDING_PREFIX = "op-idem:"


def _operation_binding_key(operation_id: str) -> str:
  return f"{IDEMPOTENCY_BINDING_PREFIX}{operation_id}"


def generate_operation_id() -> str:
  return generate_prefixed_ulid("op")


def _utcnow_iso() -> str:
  return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _result_to_payload(
  result: BaseModel | dict[str, Any] | list[Any] | None,
) -> dict[str, Any] | list[Any] | None:
  """Normalize a command return value into a JSON-safe payload."""
  if result is None:
    return None
  if isinstance(result, BaseModel):
    return result.model_dump(mode="json")
  if isinstance(result, (dict, list)):
    return result
  raise TypeError(
    "Operation result must be a Pydantic model, dict, list, or None; "
    f"got {type(result).__name__}"
  )


class OperationEnvelope(BaseModel, Generic[TResult]):
  """Uniform response shape for every operation endpoint.

  Every dispatch carries an `op_<ULID>` operation_id, which is the bridge
  to the monitoring surface: pass it to
  `GET /v1/operations/{operation_id}/stream` (see `routers/operations.py`)
  to subscribe to SSE progress events. Sync commands complete in the envelope
  itself (`status: "completed"`, HTTP 200); async commands
  (`status: "pending"`, HTTP 202) hand off to a background worker and stream
  their tail through that SSE endpoint. Failed dispatches still mint an
  `operation_id` so the audit log and any partial SSE events stay
  correlatable.

  `TResult` parameterizes `result` so per-op response shapes surface in
  OpenAPI. Operations that pin `OperationSpec.result_type` get
  `OperationEnvelope[YourEnvelope]` as their response model; the rest keep
  the default `Any` shape (`result: any | null` on the wire).
  """

  model_config = ConfigDict(populate_by_name=True)

  operation: str = Field(description="Kebab-case operation name")
  operation_id: str = Field(
    alias="operationId",
    description="op_-prefixed ULID for audit and SSE correlation",
  )
  status: OperationStatus = Field(description="Operation lifecycle state")
  result: TResult | None = Field(
    default=None, description="Command-specific result payload"
  )
  at: str = Field(description="ISO-8601 UTC timestamp")
  created_by: str | None = Field(
    default=None,
    alias="createdBy",
    description="User ID that initiated the operation (null for legacy callers)",
  )
  idempotent_replay: bool = Field(
    default=False,
    alias="idempotentReplay",
    description=(
      "True when this envelope came from the idempotency cache — the "
      "underlying command did not execute again. False on fresh executions."
    ),
  )


def wrap_completed(
  operation_name: str,
  result: BaseModel | dict[str, Any] | list[Any] | None,
  operation_id: str | None = None,
  created_by: str | None = None,
) -> OperationEnvelope:
  """Build a `status="completed"` envelope for a sync command result."""
  return OperationEnvelope(
    operation=operation_name,
    operationId=operation_id or generate_operation_id(),
    status="completed",
    result=_result_to_payload(result),
    at=_utcnow_iso(),
    createdBy=created_by,
  )


def wrap_pending(
  operation_name: str,
  operation_id: str,
  partial_result: BaseModel | dict[str, Any] | list[Any] | None = None,
  created_by: str | None = None,
) -> OperationEnvelope:
  """Build a `status="pending"` envelope for an async-dispatched command.

  `operation_id` is the one already registered for SSE streaming.
  """
  return OperationEnvelope(
    operation=operation_name,
    operationId=operation_id,
    status="pending",
    result=_result_to_payload(partial_result),
    at=_utcnow_iso(),
    createdBy=created_by,
  )


def wrap_failed(
  operation_name: str,
  error: str | dict[str, Any],
  operation_id: str | None = None,
  created_by: str | None = None,
) -> OperationEnvelope:
  """Build a `status="failed"` envelope (async commands; the REST dispatcher
  raises `HTTPException` instead)."""
  if isinstance(error, str):
    payload: dict[str, Any] = {"error": error}
  else:
    payload = {"error": error.get("error", "operation failed"), **error}
  return OperationEnvelope(
    operation=operation_name,
    operationId=operation_id or generate_operation_id(),
    status="failed",
    result=payload,
    at=_utcnow_iso(),
    createdBy=created_by,
  )


# ── Idempotency cache ────────────────────────────────────────────────────


class IdempotencyKeyConflictError(Exception):
  """An idempotency key reused with a different body; routes map it to 409.

  Stripe semantics: same key + same body replays, same key + different body
  conflicts, a different key executes independently.
  """

  def __init__(self, operation_name: str) -> None:
    super().__init__(
      f"Idempotency-Key was reused with a different request body for "
      f"operation {operation_name!r}. Use a fresh key for distinct payloads."
    )
    self.operation_name = operation_name


class IdempotencyInProgressError(IdempotencyKeyConflictError):
  """An idempotency key replayed while its first request is still running.

  Subclasses the conflict error so routes map it to 409 too.
  """

  def __init__(self, operation_name: str) -> None:
    Exception.__init__(
      self,
      f"A request with this Idempotency-Key for operation "
      f"{operation_name!r} is still in progress. Retry after it completes "
      "to receive its result.",
    )
    self.operation_name = operation_name


def compute_idempotency_cache_key(
  user_id: str,
  graph_id: str,
  operation_name: str,
  idempotency_key: str,
) -> str:
  """Valkey key for a `(user, graph, operation, key)` tuple.

  Scoped by user so callers can't replay each other's envelopes; user id and
  client key are hashed so neither appears verbatim in the keyspace.
  """
  digest = hashlib.sha256(f"{user_id}:{idempotency_key}".encode()).hexdigest()[:32]
  return f"idem:{graph_id}:{operation_name}:{digest}"


def fingerprint_body(body: Any) -> str:
  """SHA-256 of a request body (key order-insensitive), used to detect
  Idempotency-Key reuse with a different body."""
  if body is None:
    payload = "null"
  elif isinstance(body, BaseModel):
    payload = json.dumps(body.model_dump(mode="json"), sort_keys=True)
  elif isinstance(body, (dict, list)):
    payload = json.dumps(body, sort_keys=True, default=str)
  else:
    payload = json.dumps(str(body), sort_keys=True)
  return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class IdempotencyCache:
  """Async wrapper over the `OPERATION_IDEMPOTENCY` Valkey DB.

  Entries are ``{"envelope": ..., "body_fingerprint": ...}``, or
  ``{"pending": true, "body_fingerprint": ...}`` while reserved. Every cache
  failure is best-effort: logged, never raised.
  """

  def __init__(self, client: Any | None = None) -> None:
    self._client = client or create_async_redis_client(
      ValkeyDatabase.OPERATION_IDEMPOTENCY, decode_responses=True
    )

  async def get(
    self,
    user_id: str,
    graph_id: str,
    operation_name: str,
    idempotency_key: str,
    body_fingerprint: str,
  ) -> OperationEnvelope | None:
    """Return a cached envelope on (key + body) match, None on a miss.

    Raises `IdempotencyKeyConflictError` on a body mismatch and
    `IdempotencyInProgressError` while the key is reserved.
    """
    cache_key = compute_idempotency_cache_key(
      user_id, graph_id, operation_name, idempotency_key
    )
    try:
      raw = await self._client.get(cache_key)
    except Exception as exc:  # pragma: no cover - defensive
      logger.warning(
        "Idempotency cache read failed",
        extra={
          "cache_key": cache_key,
          "operation": operation_name,
          "graph_id": graph_id,
          "error": str(exc),
        },
      )
      return None
    if raw is None:
      return None
    try:
      stored = json.loads(raw)
      cached_fingerprint = stored["body_fingerprint"]
      if stored.get("pending"):
        if cached_fingerprint != body_fingerprint:
          raise IdempotencyKeyConflictError(operation_name)
        raise IdempotencyInProgressError(operation_name)
      cached_envelope = OperationEnvelope.model_validate(stored["envelope"])
    except IdempotencyKeyConflictError:
      raise
    except Exception as exc:  # pragma: no cover - defensive
      logger.warning(
        "Idempotency cache payload was invalid; evicting",
        extra={
          "cache_key": cache_key,
          "operation": operation_name,
          "graph_id": graph_id,
          "error": str(exc),
        },
      )
      await self._client.delete(cache_key)
      return None

    if cached_fingerprint != body_fingerprint:
      raise IdempotencyKeyConflictError(operation_name)

    return cached_envelope

  async def reserve(
    self,
    user_id: str,
    graph_id: str,
    operation_name: str,
    idempotency_key: str,
    body_fingerprint: str,
  ) -> bool:
    """Claim the key for an in-flight run with an atomic `SET NX`.

    False when another request holds it (re-read with `get` to learn which
    state). A cache outage answers True so a Valkey blip can't 5xx writes.
    """
    cache_key = compute_idempotency_cache_key(
      user_id, graph_id, operation_name, idempotency_key
    )
    payload = json.dumps({"pending": True, "body_fingerprint": body_fingerprint})
    try:
      claimed = await self._client.set(
        cache_key, payload, ex=IDEMPOTENCY_RESERVATION_TTL_SECONDS, nx=True
      )
    except Exception as exc:  # pragma: no cover - defensive
      logger.warning(
        "Idempotency cache reserve failed",
        extra={
          "cache_key": cache_key,
          "operation": operation_name,
          "graph_id": graph_id,
          "error": str(exc),
        },
      )
      return True
    return bool(claimed)

  async def release(
    self,
    user_id: str,
    graph_id: str,
    operation_name: str,
    idempotency_key: str,
  ) -> None:
    """Drop a pending marker after a failed run, so a retry can execute.
    Never removes a completed envelope."""
    cache_key = compute_idempotency_cache_key(
      user_id, graph_id, operation_name, idempotency_key
    )
    try:
      raw = await self._client.get(cache_key)
      if raw is not None and json.loads(raw).get("pending"):
        await self._client.delete(cache_key)
    except Exception as exc:  # pragma: no cover - defensive
      logger.warning(
        "Idempotency cache release failed",
        extra={
          "cache_key": cache_key,
          "operation": operation_name,
          "graph_id": graph_id,
          "error": str(exc),
        },
      )

  async def put(
    self,
    user_id: str,
    graph_id: str,
    operation_name: str,
    idempotency_key: str,
    envelope: OperationEnvelope,
    body_fingerprint: str,
    ttl_seconds: int = IDEMPOTENCY_TTL_SECONDS,
  ) -> None:
    cache_key = compute_idempotency_cache_key(
      user_id, graph_id, operation_name, idempotency_key
    )
    payload = json.dumps(
      {
        "envelope": envelope.model_dump(by_alias=True, mode="json"),
        "body_fingerprint": body_fingerprint,
      }
    )
    try:
      await self._client.set(cache_key, payload, ex=ttl_seconds)
    except Exception as exc:  # pragma: no cover - defensive
      logger.warning(
        "Idempotency cache write failed",
        extra={
          "cache_key": cache_key,
          "operation": operation_name,
          "graph_id": graph_id,
          "error": str(exc),
        },
      )

  async def bind_operation(self, operation_id: str, cache_key: str) -> None:
    """Record that `operation_id`'s pending envelope lives under `cache_key`."""
    binding_key = _operation_binding_key(operation_id)
    try:
      await self._client.sadd(binding_key, cache_key)
      await self._client.expire(binding_key, IDEMPOTENCY_TTL_SECONDS)
    except Exception as exc:  # pragma: no cover - defensive
      logger.warning(
        "Idempotency operation binding failed",
        extra={
          "operation_id": operation_id,
          "cache_key": cache_key,
          "error": str(exc),
        },
      )

  async def invalidate_operation(self, operation_id: str) -> int:
    """Evict every envelope bound to a failed or cancelled operation;
    returns how many."""
    binding_key = _operation_binding_key(operation_id)
    try:
      cache_keys = await self._client.smembers(binding_key)
      if not cache_keys:
        return 0
      await self._client.delete(*cache_keys, binding_key)
    except Exception as exc:  # pragma: no cover - defensive
      logger.warning(
        "Idempotency operation invalidation failed",
        extra={"operation_id": operation_id, "error": str(exc)},
      )
      return 0
    return len(cache_keys)


_sync_idempotency_client: Any | None = None


def _get_sync_idempotency_client() -> Any:
  """Sync client for callers with no event loop (Dagster, background threads)."""
  global _sync_idempotency_client
  if _sync_idempotency_client is None:
    _sync_idempotency_client = create_redis_client(
      ValkeyDatabase.OPERATION_IDEMPOTENCY, decode_responses=True
    )
  return _sync_idempotency_client


async def invalidate_operation_idempotency(operation_id: str) -> int:
  """Evict the pending envelopes bound to a failed or cancelled operation."""
  evicted = await get_idempotency_cache().invalidate_operation(operation_id)
  if evicted:
    logger.info(
      "Evicted idempotency envelope for terminal operation",
      extra={"operation_id": operation_id, "evicted": evicted},
    )
  return evicted


def invalidate_operation_idempotency_sync(operation_id: str) -> int:
  """Sync counterpart to `invalidate_operation_idempotency`."""
  binding_key = _operation_binding_key(operation_id)
  try:
    client = _get_sync_idempotency_client()
    cache_keys = client.smembers(binding_key)
    if not cache_keys:
      return 0
    client.delete(*cache_keys, binding_key)
  except Exception as exc:  # pragma: no cover - defensive
    logger.warning(
      "Idempotency operation invalidation failed",
      extra={"operation_id": operation_id, "error": str(exc)},
    )
    return 0
  logger.info(
    "Evicted idempotency envelope for terminal operation",
    extra={"operation_id": operation_id, "evicted": len(cache_keys)},
  )
  return len(cache_keys)


# ── Audit logging ────────────────────────────────────────────────────────


def log_operation_audit(
  *,
  operation_name: str,
  operation_id: str,
  user_id: str,
  graph_id: str,
  duration_ms: float,
  status: OperationStatus,
  idempotency_key: str | None = None,
  idempotent_replay: bool = False,
  error: str | None = None,
  event: str = "extensions.operation",
  surface: str = "rest",
) -> None:
  """Emit one structured audit-log line per operation call.

  `event` is `"extensions.operation"` or `"graph.operation"`; `surface` is
  `"rest"` or `"mcp"`. Inside a request the payload also carries the request
  id and the credential (`auth_method` / `api_key_prefix`), so a leaked key
  can be scoped to the credential rather than the account.
  """
  payload: dict[str, Any] = {
    "event": event,
    "operation": operation_name,
    "operation_id": operation_id,
    "user_id": user_id,
    "graph_id": graph_id,
    "surface": surface,
    "duration_ms": round(duration_ms, 2),
    "status": status,
    "idempotent_replay": idempotent_replay,
    **audit_context(),
  }
  if idempotency_key is not None:
    payload["idempotency_key_hash"] = hashlib.sha256(
      idempotency_key.encode("utf-8")
    ).hexdigest()[:16]
  if error is not None:
    payload["error"] = error

  if status == "failed":
    logger.error(event, extra={"audit": payload})
  else:
    logger.info(event, extra={"audit": payload})


# ── Operation dispatcher ─────────────────────────────────────────────────


@dataclass
class OperationContext:
  """Per-call context carried through `execute_operation`.

  `body_fingerprint` comes from `fingerprint_body(body)` in the route.
  """

  domain: str
  operation_name: str
  graph_id: str
  user_id: str
  idempotency_key: str | None = None
  body_fingerprint: str | None = None


# A zero-arg closure that opens its own session, calls the ops layer, and
# returns a response model; client errors are raised as HTTPException.
OperationRunner = Callable[[], BaseModel | dict[str, Any] | list[Any] | None]
AsyncOperationRunner = Callable[
  [], Awaitable[BaseModel | dict[str, Any] | list[Any] | None]
]


_idempotency_cache_singleton: IdempotencyCache | None = None


def get_idempotency_cache() -> IdempotencyCache:
  """Return the shared `IdempotencyCache` (one Valkey pool per process)."""
  global _idempotency_cache_singleton
  if _idempotency_cache_singleton is None:
    _idempotency_cache_singleton = IdempotencyCache()
  return _idempotency_cache_singleton


async def check_idempotency(
  cache: IdempotencyCache,
  user_id: str,
  graph_id: str,
  op_name: str,
  idempotency_key: str | None,
  body_fingerprint: str,
  event: str = "graph.operation",
) -> OperationEnvelope | None:
  """Check the idempotency cache for an async (pending) operation.

  Returns the cached envelope on a hit, None on a miss; raises 409 on a body
  mismatch or while the key is in progress. A miss reserves the key, so the
  caller must record or release it on every path: use `idempotent_dispatch`
  rather than calling this directly.
  """
  if idempotency_key is None:
    return None
  try:
    cached = await cache.get(
      user_id, graph_id, op_name, idempotency_key, body_fingerprint
    )
    if cached is None and not await cache.reserve(
      user_id, graph_id, op_name, idempotency_key, body_fingerprint
    ):
      # Lost the race for the key: replay its envelope, or report in progress.
      cached = await cache.get(
        user_id, graph_id, op_name, idempotency_key, body_fingerprint
      )
      if cached is None:
        raise IdempotencyInProgressError(op_name)
  except IdempotencyKeyConflictError as exc:
    log_operation_audit(
      operation_name=op_name,
      operation_id=generate_operation_id(),
      user_id=user_id,
      graph_id=graph_id,
      duration_ms=0.0,
      status="failed",
      idempotency_key=idempotency_key,
      error=str(exc),
      event=event,
    )
    raise HTTPException(status_code=409, detail=str(exc))
  if cached is not None:
    log_operation_audit(
      operation_name=op_name,
      operation_id=cached.operation_id,
      user_id=user_id,
      graph_id=graph_id,
      duration_ms=0.0,
      status=cached.status,
      idempotency_key=idempotency_key,
      idempotent_replay=True,
      event=event,
    )
    return cached.model_copy(update={"idempotent_replay": True})
  return None


class PendingDispatch:
  """Handle yielded by `idempotent_dispatch`.

  `replay` is the cached envelope when the key has already been used;
  `record` stores the envelope this request produced and, for a pending
  one, binds it to its operation so a terminal failure can evict it.
  """

  def __init__(
    self,
    cache: IdempotencyCache,
    *,
    user_id: str,
    graph_id: str,
    operation_name: str,
    idempotency_key: str | None,
    body_fingerprint: str,
    replay: OperationEnvelope | None,
  ) -> None:
    self._cache = cache
    self._user_id = user_id
    self._graph_id = graph_id
    self._operation_name = operation_name
    self._idempotency_key = idempotency_key
    self._body_fingerprint = body_fingerprint
    self.replay = replay
    self.recorded = False

  async def record(self, envelope: OperationEnvelope) -> None:
    """Cache `envelope` under the key and bind a pending one to its operation."""
    self.recorded = True
    if self._idempotency_key is None:
      return
    await self._cache.put(
      self._user_id,
      self._graph_id,
      self._operation_name,
      self._idempotency_key,
      envelope,
      self._body_fingerprint,
    )
    if envelope.status == "pending":
      await self._cache.bind_operation(
        envelope.operation_id,
        compute_idempotency_cache_key(
          self._user_id, self._graph_id, self._operation_name, self._idempotency_key
        ),
      )


@asynccontextmanager
async def idempotent_dispatch(
  cache: IdempotencyCache,
  user_id: str,
  graph_id: str,
  op_name: str,
  idempotency_key: str | None,
  body_fingerprint: str,
  event: str = "graph.operation",
) -> AsyncIterator[PendingDispatch]:
  """Idempotency guard for a route that enqueues work and returns `pending`.

  Enter it before any side effect. `replay` set means return it. Otherwise
  the key is reserved, and every exit that did not `record` an envelope
  releases it so the caller's retry can run.

      async with idempotent_dispatch(cache, user_id, graph_id, op, key, fp) as idem:
        if idem.replay is not None:
          return idem.replay
        ...validate, enqueue...
        envelope = wrap_pending(op, operation_id=..., created_by=user_id)
        await idem.record(envelope)
        return envelope
  """
  replay = await check_idempotency(
    cache, user_id, graph_id, op_name, idempotency_key, body_fingerprint, event
  )
  handle = PendingDispatch(
    cache,
    user_id=user_id,
    graph_id=graph_id,
    operation_name=op_name,
    idempotency_key=idempotency_key,
    body_fingerprint=body_fingerprint,
    replay=replay,
  )
  try:
    yield handle
  finally:
    if idempotency_key is not None and replay is None and not handle.recorded:
      await cache.release(user_id, graph_id, op_name, idempotency_key)


# ── Runner execution ─────────────────────────────────────────────────────

_runner_limiter: anyio.CapacityLimiter | None = None


def _get_runner_limiter() -> anyio.CapacityLimiter:
  """Bound on concurrent runner threads, sized to the extensions OLTP pool
  (each sync runner holds one session), so a burst queues here instead of
  failing on `pool_timeout`."""
  global _runner_limiter
  if _runner_limiter is None:
    from robosystems.config.tuning import TuningConfig

    total = (
      TuningConfig.get_extensions_pool_size()
      + TuningConfig.get_extensions_max_overflow()
    )
    _runner_limiter = anyio.CapacityLimiter(max(1, total))
  return _runner_limiter


async def run_off_loop(func: Callable[..., Any], *args: Any) -> Any:
  """Run `func` without blocking the event loop (the API runs one uvicorn
  worker).

  Coroutine functions are awaited; sync callables run in a worker thread
  with the request's contextvars under the runner limiter, and an awaitable
  they return is awaited on the loop.

  The thread run is shielded from asyncio cancellation: anyio would otherwise
  release the limiter token while the thread (and its DB connection) runs on,
  so a timeout means abandoned-but-running and the limiter stays honest.
  """
  if inspect.iscoroutinefunction(func):
    return await func(*args)
  work = asyncio.ensure_future(
    anyio.to_thread.run_sync(func, *args, limiter=_get_runner_limiter())
  )
  try:
    result = await asyncio.shield(work)
  except asyncio.CancelledError:
    # Nothing awaits `work` any more; log a late failure when it lands.
    work.add_done_callback(_log_abandoned_outcome)
    raise
  if inspect.isawaitable(result):
    result = await result
  return result


async def execute_operation(
  ctx: OperationContext,
  runner: OperationRunner | AsyncOperationRunner,
  idempotency_cache: IdempotencyCache | None = None,
  on_fresh_success: Callable[[OperationEnvelope], None | Awaitable[None]] | None = None,
) -> OperationEnvelope:
  """Run an operation and return its `OperationEnvelope`.

  1. Idempotency: replay a cached envelope without running; raise
     `IdempotencyKeyConflictError` (409) on a body mismatch; on a miss,
     reserve the key (released if the run fails).
  2. Run the runner off the loop, timed.
  3. `on_fresh_success(envelope)` runs only on a fresh execution and before
     caching, so a hook failure aborts rather than poisoning the cache. Use
     it for exactly-once effects (e.g. `mark_graph_stale`).
  4. Cache the envelope and emit exactly one audit line.
  """
  # A key without a fingerprint can't detect body reuse, so it's ignored.
  use_idempotency = (
    ctx.idempotency_key
    and ctx.body_fingerprint is not None
    and idempotency_cache is not None
  )
  if use_idempotency:
    try:
      cached = await idempotency_cache.get(
        ctx.user_id,
        ctx.graph_id,
        ctx.operation_name,
        ctx.idempotency_key,
        ctx.body_fingerprint,
      )
    except IdempotencyKeyConflictError as exc:
      log_operation_audit(
        operation_name=ctx.operation_name,
        operation_id=generate_operation_id(),
        user_id=ctx.user_id,
        graph_id=ctx.graph_id,
        duration_ms=0.0,
        status="failed",
        idempotency_key=ctx.idempotency_key,
        error=str(exc),
      )
      raise
    if cached is not None:
      # Copy: an in-memory cache may share the object across requests.
      cached = cached.model_copy(update={"idempotent_replay": True})
      log_operation_audit(
        operation_name=ctx.operation_name,
        operation_id=cached.operation_id,
        user_id=ctx.user_id,
        graph_id=ctx.graph_id,
        duration_ms=0.0,
        status=cached.status,
        idempotency_key=ctx.idempotency_key,
        idempotent_replay=True,
      )
      return cached
    # Lost the claim: re-read to replay its result or report in progress.
    if not await idempotency_cache.reserve(
      ctx.user_id,
      ctx.graph_id,
      ctx.operation_name,
      ctx.idempotency_key,
      ctx.body_fingerprint,
    ):
      raced = await idempotency_cache.get(
        ctx.user_id,
        ctx.graph_id,
        ctx.operation_name,
        ctx.idempotency_key,
        ctx.body_fingerprint,
      )
      if raced is not None:
        raced = raced.model_copy(update={"idempotent_replay": True})
        log_operation_audit(
          operation_name=ctx.operation_name,
          operation_id=raced.operation_id,
          user_id=ctx.user_id,
          graph_id=ctx.graph_id,
          duration_ms=0.0,
          status=raced.status,
          idempotency_key=ctx.idempotency_key,
          idempotent_replay=True,
        )
        return raced
      # The holder released between our reads; don't loop.
      raise IdempotencyInProgressError(ctx.operation_name)

  async def _run_and_record() -> OperationEnvelope:
    # Every failure is audited before re-raising, and the reservation is
    # released on any exit that did not store the envelope.
    start = time.monotonic()
    recorded = False
    try:
      try:
        result = await run_off_loop(runner)
      except HTTPException as exc:
        log_operation_audit(
          operation_name=ctx.operation_name,
          operation_id=generate_operation_id(),
          user_id=ctx.user_id,
          graph_id=ctx.graph_id,
          duration_ms=(time.monotonic() - start) * 1000,
          status="failed",
          idempotency_key=ctx.idempotency_key,
          error=str(exc.detail),
        )
        raise
      except Exception as exc:
        log_operation_audit(
          operation_name=ctx.operation_name,
          operation_id=generate_operation_id(),
          user_id=ctx.user_id,
          graph_id=ctx.graph_id,
          duration_ms=(time.monotonic() - start) * 1000,
          status="failed",
          idempotency_key=ctx.idempotency_key,
          error=f"{type(exc).__name__}: {exc}",
        )
        raise

      duration_ms = (time.monotonic() - start) * 1000

      envelope = wrap_completed(ctx.operation_name, result, created_by=ctx.user_id)
      if on_fresh_success is not None:
        try:
          await run_off_loop(on_fresh_success, envelope)
        except Exception as exc:
          log_operation_audit(
            operation_name=ctx.operation_name,
            operation_id=envelope.operation_id,
            user_id=ctx.user_id,
            graph_id=ctx.graph_id,
            duration_ms=(time.monotonic() - start) * 1000,
            status="failed",
            idempotency_key=ctx.idempotency_key,
            error=f"on_fresh_success: {type(exc).__name__}: {exc}",
          )
          raise
      if use_idempotency:
        await idempotency_cache.put(
          ctx.user_id,
          ctx.graph_id,
          ctx.operation_name,
          ctx.idempotency_key,
          envelope,
          ctx.body_fingerprint,
        )
      recorded = True
      log_operation_audit(
        operation_name=ctx.operation_name,
        operation_id=envelope.operation_id,
        user_id=ctx.user_id,
        graph_id=ctx.graph_id,
        duration_ms=duration_ms,
        status="completed",
        idempotency_key=ctx.idempotency_key,
      )
      return envelope
    finally:
      if use_idempotency and not recorded:
        await idempotency_cache.release(
          ctx.user_id, ctx.graph_id, ctx.operation_name, ctx.idempotency_key
        )

  # Shielded: the write commits in its thread regardless of the client, so
  # hook, cache and audit must finish too or a retry would re-execute it.
  work = asyncio.ensure_future(_run_and_record())
  try:
    return await asyncio.shield(work)
  except asyncio.CancelledError:
    # Nothing awaits `work` any more; log a late failure when it lands.
    work.add_done_callback(_log_abandoned_outcome)
    raise


def _log_abandoned_outcome(task: asyncio.Future) -> None:
  if task.cancelled():
    return
  exc = task.exception()
  if exc is not None:
    logger.warning(
      "operation finished after its request was cancelled and failed: %s: %s",
      type(exc).__name__,
      exc,
    )


__all__ = [
  "IDEMPOTENCY_BINDING_PREFIX",
  "IDEMPOTENCY_RESERVATION_TTL_SECONDS",
  "IDEMPOTENCY_TTL_SECONDS",
  "AsyncOperationRunner",
  "IdempotencyCache",
  "IdempotencyInProgressError",
  "IdempotencyKeyConflictError",
  "OperationContext",
  "OperationEnvelope",
  "OperationRunner",
  "OperationStatus",
  "PendingDispatch",
  "check_idempotency",
  "compute_idempotency_cache_key",
  "execute_operation",
  "fingerprint_body",
  "generate_operation_id",
  "get_idempotency_cache",
  "idempotent_dispatch",
  "invalidate_operation_idempotency",
  "invalidate_operation_idempotency_sync",
  "log_operation_audit",
  "run_off_loop",
  "wrap_completed",
  "wrap_failed",
  "wrap_pending",
]
