"""Operation envelope, idempotency cache, and audit helpers.

Shared dispatch infrastructure for every operation surface — extensions,
graph lifecycle ops, and any future command surface. Three concerns live
here so per-domain routers stay thin:

1. **Envelope** — wrap a command's Pydantic result in a uniform payload.
2. **Idempotency** — cache completed envelopes in Valkey keyed by the
   caller's ``Idempotency-Key`` header so retries are safe for 24 hours.
3. **Audit** — one structured log line per operation call carrying the
   durations and identifiers a SOC-2-style audit trail needs.

Operation IDs are ``op_``-prefixed ULIDs, matching the
``^op_[0-9A-Z]{26}$`` pattern the ``/v1/operations/{operation_id}/stream``
SSE endpoint accepts.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Generic, Literal, TypeVar

import anyio
from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, Field

from robosystems.config.valkey_registry import (
  ValkeyDatabase,
  create_async_redis_client,
)
from robosystems.logger import logger
from robosystems.utils.ulid import generate_prefixed_ulid

# `default=Any` (PEP 696) keeps unparameterized `OperationEnvelope` loose
# (`result: Any | None`) while `OperationEnvelope[PortfolioBlockEnvelope]`
# tightens it to a typed payload.
TResult = TypeVar("TResult", default=Any)

OperationStatus = Literal["completed", "pending", "failed"]

IDEMPOTENCY_TTL_SECONDS = 24 * 60 * 60

# How long a key stays reserved while its first request is still running.
# Long enough for the slowest legitimate operation (a close with QuickBooks
# writeback), short enough that a request that died mid-flight does not block
# its own retry for a day. A run that outlives the reservation simply loses
# the in-flight guard — the same behavior as before reservations existed.
IDEMPOTENCY_RESERVATION_TTL_SECONDS = 5 * 60


def generate_operation_id() -> str:
  """Return a fresh `op_`-prefixed ULID."""
  return generate_prefixed_ulid("op")


def _utcnow_iso() -> str:
  """ISO-8601 UTC timestamp with a `Z` suffix (seconds precision)."""
  return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _result_to_payload(
  result: BaseModel | dict[str, Any] | list[Any] | None,
) -> dict[str, Any] | list[Any] | None:
  """Normalize a command return value into a JSON-safe payload.

  Pydantic models are serialized via `model_dump(mode="json")` so nested
  types (datetime, Decimal, enums) are rendered in their wire form.
  """
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

  Every dispatch carries an ``op_<ULID>`` operation_id, which is the bridge
  to the monitoring surface: pass it to
  ``GET /v1/operations/{operation_id}/stream`` (see ``routers/operations.py``)
  to subscribe to SSE progress events. Sync commands complete in the envelope
  itself (``status: "completed"``, HTTP 200); async commands
  (``status: "pending"``, HTTP 202) hand off to a background worker and stream
  their tail through that SSE endpoint. Failed dispatches still mint an
  ``operation_id`` so the audit log and any partial SSE events stay
  correlatable.

  ``TResult`` parameterizes ``result`` so per-op response shapes surface in
  OpenAPI. Operations that pin ``OperationSpec.result_type`` get
  ``OperationEnvelope[YourEnvelope]`` as their response model; the rest keep
  the default ``Any`` shape (``result: any | null`` on the wire).
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
  """Build a `status="completed"` envelope for a sync command result.

  The dispatcher passes `created_by` from `ctx.user_id` so clients and
  audit consumers can correlate envelopes without reading the audit log.
  """
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

  `operation_id` is required: the caller already registered the operation
  with the SSE infrastructure (or the Dagster dispatcher) and needs the
  same ID in the response for streaming.
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
  """Build a `status="failed"` envelope.

  For error responses that should still carry the canonical envelope shape
  and an `operation_id` for audit correlation. The REST dispatcher normally
  raises `HTTPException` instead; async commands surface failure through
  the envelope.
  """
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


# ---------------------------------------------------------------------------
# Idempotency cache
# ---------------------------------------------------------------------------


class IdempotencyKeyConflictError(Exception):
  """Raised when a caller reuses an idempotency key with a different body.

  Mirrors Stripe / RFC draft idempotency semantics:

  - **Same key + same body** → return cached envelope (replay)
  - **Same key + different body** → 409 Conflict
  - **Different key** → independent execution

  Surfaced by `IdempotencyCache.get(...)` when a stored entry exists
  for the (user, graph, operation, key) tuple but the body fingerprint
  differs from the current request. The route layer translates this
  into HTTP 409 with a clear detail message.
  """

  def __init__(self, operation_name: str) -> None:
    super().__init__(
      f"Idempotency-Key was reused with a different request body for "
      f"operation {operation_name!r}. Use a fresh key for distinct payloads."
    )
    self.operation_name = operation_name


class IdempotencyInProgressError(IdempotencyKeyConflictError):
  """Raised when a caller replays an idempotency key whose first request is
  still executing.

  Without this, two requests with the same key that arrive inside the first
  one's run time both miss the cache and both execute — the double-submit
  the key exists to prevent. The route layer already maps the parent class
  to HTTP 409, so this needs no new mapping; the message says to retry.
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
  """Deterministic Valkey key for a `(user, graph, operation, key)` tuple.

  **Scoped by user_id** so different callers can't replay each other's
  envelopes by guessing or reusing the same idempotency key. The key
  is hashed so arbitrary client-supplied strings (UUIDs, random
  nonces, etc.) never become keyspace liabilities.

  The `user_id` is also hashed (rather than substituted verbatim) so
  PII / opaque IDs aren't surfaced in cache keys that may show up in
  Valkey monitoring tools.
  """
  digest = hashlib.sha256(f"{user_id}:{idempotency_key}".encode()).hexdigest()[:32]
  return f"idem:{graph_id}:{operation_name}:{digest}"


def fingerprint_body(body: Any) -> str:
  """SHA-256 of a request body, used to detect Idempotency-Key reuse.

  Serialization uses `sort_keys=True` so dict ordering doesn't cause
  spurious mismatches. Route handlers compute this once per request and
  put the result on `OperationContext.body_fingerprint`; the dispatcher
  and cache then treat the (key, body) pair as the idempotency identity.
  """
  if body is None:
    payload = "null"
  elif isinstance(body, BaseModel):
    payload = json.dumps(body.model_dump(mode="json"), sort_keys=True)
  elif isinstance(body, (dict, list)):
    payload = json.dumps(body, sort_keys=True, default=str)
  else:
    # Last-resort: stringify (covers primitives, dataclasses with __str__)
    payload = json.dumps(str(body), sort_keys=True)
  return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class IdempotencyCache:
  """Thin async wrapper over the `OPERATION_IDEMPOTENCY` Valkey DB.

  Stored shape (JSON):
  ```
  {
    "envelope": <OperationEnvelope>,
    "body_fingerprint": "<sha256 hex>"
  }
  ```

  The wrapper enforces three idempotency rules on `get()`:

  1. Cache miss → returns `None`, caller proceeds to execute.
  2. Cache hit + matching body fingerprint → returns the cached
     envelope (replay).
  3. Cache hit + mismatched body fingerprint → raises
     `IdempotencyKeyConflictError`, caller maps to HTTP 409.

  A fresh Redis client is created per instance to avoid sharing
  connection state across request lifetimes; callers typically use
  the module-level singleton from `get_idempotency_cache()`.
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
    """Return a cached envelope on (key + body) match.

    Raises `IdempotencyKeyConflictError` when the key matches an
    existing entry but the body has changed.
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
        # The first request with this key is still running.
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
    """Claim the key for an in-flight run.

    Atomic ``SET NX`` of a pending marker carrying the body fingerprint. Returns
    ``True`` when this call now holds the key, ``False`` when another request
    already holds it (pending or completed) — the caller re-reads with
    ``get`` to learn which. A cache outage answers ``True``: idempotency is
    best-effort here exactly as it is for ``get``/``put``, and a Valkey blip
    must not turn every write into a 5xx.
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
    """Drop a pending marker after the run failed, so a retry can execute.

    Removes the key only while it still holds a pending marker — never a
    completed envelope another request may have stored in the meantime.
    """
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
    """Cache an envelope + its body fingerprint for `ttl_seconds` (24h default)."""
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


# ---------------------------------------------------------------------------
# Audit logging
# ---------------------------------------------------------------------------


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

  The audit stream is consumed by the standard logging pipeline (CloudWatch
  in prod, stdout in dev). Fields are picked to satisfy a SOC-2-style
  "who did what, to which tenant, when, with what result" review.

  ``event`` names the event in the audit payload and log message:
  ``"extensions.operation"`` for extension ops, ``"graph.operation"`` for
  graph lifecycle ops. ``surface`` names the entry point — ``"rest"`` for
  the HTTP operation routes, ``"mcp"`` for tool calls — so the same command
  reached two ways is distinguishable in the stream.
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
  }
  if idempotency_key is not None:
    # Log only the hash prefix so keys aren't retained verbatim.
    payload["idempotency_key_hash"] = hashlib.sha256(
      idempotency_key.encode("utf-8")
    ).hexdigest()[:16]
  if error is not None:
    payload["error"] = error

  if status == "failed":
    logger.error(event, extra={"audit": payload})
  else:
    logger.info(event, extra={"audit": payload})


# ---------------------------------------------------------------------------
# Operation dispatcher — generic execute_operation() helper used by every
# operation surface (extensions, graph ops, etc.).
# ---------------------------------------------------------------------------


@dataclass
class OperationContext:
  """Per-call context carried through `execute_operation`.

  Everything the idempotency cache, the audit log, and any async Dagster
  dispatch need to identify a single operation call.

  `body_fingerprint` comes from the route layer via `fingerprint_body(body)`
  and is pinned to the cached envelope, so reusing an `Idempotency-Key`
  with a different body raises `409 Conflict` instead of silently replaying.
  """

  domain: str
  operation_name: str
  graph_id: str
  user_id: str
  idempotency_key: str | None = None
  body_fingerprint: str | None = None


# Runner signature: a zero-arg callable (usually a closure over the
# request body) that opens its own session, calls the ops layer, and
# returns a Pydantic response model. Raising HTTPException is the
# canonical way to surface client-facing errors.
OperationRunner = Callable[[], BaseModel | dict[str, Any] | list[Any] | None]
AsyncOperationRunner = Callable[
  [], Awaitable[BaseModel | dict[str, Any] | list[Any] | None]
]


# Module-level singleton — created lazily on first access. Tests override
# via `app.dependency_overrides[get_idempotency_cache]` or by passing an
# explicit instance to `execute_operation`.
_idempotency_cache_singleton: IdempotencyCache | None = None


def get_idempotency_cache() -> IdempotencyCache:
  """Return the shared `IdempotencyCache`.

  A module-level singleton so every request reuses one Valkey connection
  pool. Tests override via `dependency_overrides`.
  """
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
  """Check idempotency cache for async (pending) operations.

  Returns a cached envelope (with ``idempotent_replay=True``) on a cache hit,
  ``None`` on a miss. Raises ``HTTPException 409`` when the key is reused with
  a different body.

  Use this before enqueueing any async operation that cannot go through
  ``execute_operation`` — i.e. ops that return ``wrap_pending`` rather than
  ``wrap_completed``. The ``event`` parameter is forwarded to
  ``log_operation_audit``; pass ``"extensions.operation"`` for extension ops.
  """
  if idempotency_key is None:
    return None
  try:
    cached = await cache.get(
      user_id, graph_id, op_name, idempotency_key, body_fingerprint
    )
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


# ── Runner execution ─────────────────────────────────────────────────────

_runner_limiter: anyio.CapacityLimiter | None = None


def _get_runner_limiter() -> anyio.CapacityLimiter:
  """Bound on operation runners executing concurrently in worker threads.

  Sized to the extensions OLTP pool (``pool_size + max_overflow``): every
  sync runner opens one tenant session, so a wider limiter would only turn
  a burst into ``QueuePool limit ... reached`` after ``pool_timeout`` where
  the pre-threadpool code merely queued the request on the event loop.
  Excess runners wait on the limiter instead — same latency shape as before,
  without freezing the loop while they wait.
  """
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
  """Run ``func`` without blocking the event loop.

  A coroutine function is awaited directly. Anything else is a sync callable
  doing database or network work — it runs in a worker thread (with the
  request's contextvars, so the platform request-scoped session resolves the
  same way it does on the loop) under the runner limiter. A sync callable that
  hands back an awaitable gets that awaited on the loop.

  This is what keeps ``/v1/status`` answering while a close posts to
  QuickBooks or a bounded lock wait sits on a busy row: the API runs one
  uvicorn worker, and before this every operation's SQL and HTTP ran inline
  on the loop.

  The worker-thread run is **shielded** from native asyncio cancellation.
  ``anyio.to_thread.run_sync`` shields only anyio-flavoured cancellation;
  ``asyncio.wait_for`` / ``asyncio.timeout`` / ``Task.cancel()`` — which the
  MCP transports and the worker's operator budget use — raise straight
  through it, and anyio then releases the ``CapacityLimiter`` token while the
  thread (and its still-open DB connection) runs on to completion. That
  uncouples the limiter from the real thread count: under timeouts the pool
  overflows into ``pool_timeout`` failures, and a "timed out" write still
  commits later. Shielding ties the token's lifetime to the thread's, not the
  caller's — a timeout means *abandoned, still running*, and the limiter keeps
  bounding runner threads. (A Python thread cannot be cancelled anyway, so
  this only makes the accounting honest.) Matches ``execute_operation``'s use
  of ``asyncio.shield``.
  """
  if inspect.iscoroutinefunction(func):
    return await func(*args)
  work = asyncio.ensure_future(
    anyio.to_thread.run_sync(func, *args, limiter=_get_runner_limiter())
  )
  try:
    result = await asyncio.shield(work)
  except asyncio.CancelledError:
    # The worker thread keeps running to completion (a Python thread cannot be
    # cancelled), and nothing awaits `work` any more — retrieve its outcome
    # when it lands so a late failure reaches the structured logger rather than
    # surfacing as an "exception was never retrieved" stderr traceback from a
    # task nobody holds. Same handling as `execute_operation`.
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

  Order of operations:

  1. **Idempotency lookup** — when `ctx.idempotency_key` and a cache are
     both present. On match, return the cached envelope with an
     `idempotent_replay=True` audit line and never call the runner. On
     fingerprint mismatch, raise `IdempotencyKeyConflictError` for the
     route to map to HTTP 409. On a miss, **reserve** the key: a second
     request with the same key that arrives while this one runs gets
     `IdempotencyInProgressError` (also 409) instead of executing too. The
     reservation is released if the run fails, so a retry can execute.
  2. **Run + time** the runner; wall-clock duration excludes the lookup.
  3. **Side-effect hook** — `on_fresh_success(envelope)` runs ONLY on a
     fresh execution, and BEFORE the envelope is cached, so a hook failure
     aborts the request rather than poisoning the cache. Use it for
     effects that must happen exactly once (e.g. `mark_graph_stale`).
  4. **Cache + audit** — store the envelope with its body fingerprint so
     retries within the TTL return it unchanged, and emit exactly one
     audit line (`"completed"`, or `"failed"` if anything raised).

  The `runner` opens its own database session, performs business
  validation beyond FastAPI's parsing, calls the ops layer, and
  translates domain exceptions into `HTTPException`. Sync and async
  runners are both accepted; a sync runner (and a sync `on_fresh_success`
  hook) executes in a worker thread so its database and network work never
  blocks the event loop.
  """
  # Requires both a key AND a fingerprint so a route handler can't request
  # idempotency without the fingerprint that protects against key reuse.
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
      # Conflicts produce a failed audit line + propagate to the route.
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
      # Copy rather than mutate: an in-memory cache implementation that
      # shares object references across requests would otherwise corrupt
      # previously returned envelopes.
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
    # Miss: claim the key before running, so a second request with the same
    # key that lands during this run is told "in progress" (409) instead of
    # executing alongside it. Losing the claim means another request took
    # the key between our read and our write — read again to replay its
    # result or report it in progress.
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
      # The other holder released between our two reads (its run failed);
      # treat it like the in-progress case rather than looping.
      raise IdempotencyInProgressError(ctx.operation_name)

  async def _run_and_record() -> OperationEnvelope:
    # HTTPException and bare Exception are caught separately: the former
    # propagates with its original status/detail, the latter still produces a
    # failed audit line before re-raising, so a buggy command can never 500
    # with no audit record. The reservation is released on every exit that
    # did not store the envelope — including a hook failure and a
    # cancellation — so the caller's retry can run.
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

      # The hook runs before caching: if it raises, the failure aborts the
      # request without poisoning the idempotency cache with a stuck
      # envelope. A write whose command succeeded but whose post-hook failed
      # is still audited, as failed, so it never reaches the client as a 500
      # with no record.
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

  # Shielded from cancellation. The runner's write commits in its worker
  # thread whether or not the request is still connected (a disconnected
  # client cannot un-commit it), so the recording half — hook, cache, audit
  # — must finish too: otherwise a client that dropped mid-write leaves a
  # committed operation with no cached envelope, and its retry with the same
  # key either re-executes the write or is refused as in progress. The
  # cancellation is honored the moment the shielded work completes.
  work = asyncio.ensure_future(_run_and_record())
  try:
    return await asyncio.shield(work)
  except asyncio.CancelledError:
    # Nothing awaits `work` any more; retrieve its outcome when it lands so
    # a late failure is logged rather than reported as never retrieved.
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
  "check_idempotency",
  "compute_idempotency_cache_key",
  "execute_operation",
  "fingerprint_body",
  "generate_operation_id",
  "get_idempotency_cache",
  "log_operation_audit",
  "run_off_loop",
  "wrap_completed",
  "wrap_failed",
  "wrap_pending",
]
