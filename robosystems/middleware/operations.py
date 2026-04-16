"""Operation envelope, idempotency cache, and audit helpers.

Shared dispatch infrastructure for any operation surface — extensions,
graph lifecycle ops, or future command surfaces.  Three concerns live
in this module so per-domain routers stay thin:

1. **Envelope** — wrap a command's Pydantic result in a uniform payload.
2. **Idempotency** — cache completed envelopes in Valkey keyed by the
   caller's ``Idempotency-Key`` header so retries are safe for 24 hours.
3. **Audit** — structured log per operation call with the durations and
   identifiers a SOC-2-style audit trail needs.

Operation IDs reuse ``robosystems.utils.ulid.generate_prefixed_ulid("op")``
so the existing ``/v1/operations/{operation_id}/stream`` SSE endpoint
(which already matches ``^op_[0-9A-Z]{26}$``) accepts async operation
IDs without any regex changes.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, Field

from robosystems.config.valkey_registry import (
  ValkeyDatabase,
  create_async_redis_client,
)
from robosystems.logger import logger
from robosystems.utils.ulid import generate_prefixed_ulid

OperationStatus = Literal["completed", "pending", "failed"]

# 24 hours — matches the plan's idempotency retention window.
IDEMPOTENCY_TTL_SECONDS = 24 * 60 * 60


def generate_operation_id() -> str:
  """Return a fresh `op_`-prefixed ULID.

  Delegates to the repo-wide prefixed-ULID helper so the format stays
  aligned with `/v1/operations/{operation_id}/stream` and the existing
  `SSEEventStorage.create_operation` path.
  """
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


class OperationEnvelope(BaseModel):
  """Uniform response shape for every operation endpoint.

  Every dispatch through an operation surface returns an envelope carrying
  an ``op_<ULID>`` operation_id.  That id is the bridge to the platform's
  monitoring surface: pass it to
  ``GET /v1/operations/{operation_id}/stream`` (see ``routers/operations.py``)
  to subscribe to SSE progress events.  Sync commands complete in the
  envelope itself; async commands (``status: "pending"``, HTTP 202) hand
  off to a background worker and stream their tail through the same SSE
  endpoint until completion.  Failed dispatches still mint an
  ``operation_id`` so the audit log and any partial SSE events stay
  correlatable.

  Fields:
  - ``operation``: kebab-case command name (e.g. ``close-period``)
  - ``operation_id``: ``op_``-prefixed ULID; always present, usable for
    audit correlation and — for async commands — SSE subscription via
    ``/v1/operations/{operation_id}/stream``
  - ``status``: ``"completed"`` (sync, HTTP 200), ``"pending"``
    (async, HTTP 202), or ``"failed"`` (error responses)
  - ``result``: the domain-specific payload (the original Pydantic
    response) or ``None`` for async/failed cases
  - ``at``: ISO-8601 UTC timestamp of when the envelope was minted
  - ``created_by``: user ID of the caller who initiated this operation
  - ``idempotent_replay``: ``True`` when the dispatcher returned this
    envelope from the idempotency cache (the underlying command did NOT
    execute again)
  """

  model_config = ConfigDict(populate_by_name=True)

  operation: str = Field(description="Kebab-case operation name")
  operation_id: str = Field(
    alias="operationId",
    description="op_-prefixed ULID for audit and SSE correlation",
  )
  status: OperationStatus = Field(description="Operation lifecycle state")
  result: dict[str, Any] | list[Any] | None = Field(
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

  `created_by` is the user ID that initiated the operation; the
  dispatcher passes it from `ctx.user_id` so clients and audit
  consumers can correlate envelopes without reading the audit log.
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

  `operation_id` is required here because the caller already registered
  the operation with the SSE infrastructure (or the Dagster dispatcher)
  and needs the same ID in the response for streaming.

  `created_by` should be the user ID that enqueued the job so the
  pending envelope carries the same provenance as a completed one.
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

  Reserved for error responses where we still want the client to see the
  canonical envelope shape with an `operation_id` for audit correlation.
  The REST dispatcher normally raises `HTTPException` instead, but async
  commands may surface failure through the envelope itself.
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

  Pydantic models are serialized via `model_dump(mode="json")`,
  dicts/lists serialized directly, and `None` produces a stable
  sentinel. JSON serialization uses `sort_keys=True` so dict ordering
  doesn't cause spurious mismatches.

  Route handlers compute this once per request and put the result
  on `OperationContext.body_fingerprint`. The dispatcher and cache
  then enforce the (key, body) pair as the idempotency identity.
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
      cached_envelope = OperationEnvelope.model_validate(stored["envelope"])
      cached_fingerprint = stored["body_fingerprint"]
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
) -> None:
  """Emit one structured audit-log line per operation call.

  The audit stream is consumed by the standard logging pipeline (CloudWatch
  in prod, stdout in dev). Fields are picked to satisfy a SOC-2-style
  "who did what, to which tenant, when, with what result" review.

  The ``event`` parameter controls the event name in the audit payload
  and log message.  Defaults to ``"extensions.operation"`` for backward
  compatibility; graph ops pass ``"graph.operation"``.
  """
  payload: dict[str, Any] = {
    "event": event,
    "operation": operation_name,
    "operation_id": operation_id,
    "user_id": user_id,
    "graph_id": graph_id,
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

  Captures the identifying tuple for a single operation call —
  everything needed by the idempotency cache, the audit log, and any
  async Dagster dispatch that needs user + graph provenance.

  `body_fingerprint` is computed by the route layer from the typed
  request body via `fingerprint_body(body)` and pinned to the cached
  envelope so that reusing an `Idempotency-Key` with a different body
  raises a `409 Conflict` instead of silently replaying.
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
  """FastAPI dependency that returns a shared `IdempotencyCache` instance.

  Kept as a module-level singleton so every request shares one Valkey
  connection pool. Tests override via `dependency_overrides`.
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


async def execute_operation(
  ctx: OperationContext,
  runner: OperationRunner | AsyncOperationRunner,
  idempotency_cache: IdempotencyCache | None = None,
  on_fresh_success: Callable[[OperationEnvelope], None] | None = None,
) -> OperationEnvelope:
  """Run an operation and return its `OperationEnvelope`.

  Responsibilities:

  1. **Idempotency lookup** — if `ctx.idempotency_key` is set and a
     cache is provided, check for a cached envelope. On match, return
     it and emit an `idempotent_replay=True` audit line. On
     fingerprint mismatch (key reused with different body), raise
     `IdempotencyKeyConflictError` for the route to map to HTTP 409.
  2. **Timing** — wall-clock duration of the runner call (excludes
     idempotency lookup).
  3. **Audit logging** — emit exactly one structured audit line with
     `status` set to `"completed"` (happy path) or `"failed"`
     (any exception raised).
  4. **Envelope wrapping** — `wrap_completed(result)` on success.
  5. **Side-effect hook** — `on_fresh_success(envelope)` runs ONLY on
     a fresh execution, not on idempotent replay. Use this for things
     that must happen exactly once (e.g. `mark_graph_stale`).
  6. **Idempotency store** — cache the successful envelope (with body
     fingerprint) so retries within the TTL return it unchanged.

  The `runner` callable is responsible for:
  - Opening its own database session
  - Validating inputs (body already parsed by FastAPI; further
    business validation like "no fields to update" goes here)
  - Calling the ops layer
  - Translating domain exceptions (`None` returns, `LookupError`,
    `ValueError`, etc.) into `HTTPException(...)`

  Both sync and async runners are supported so simple CRUD commands
  can stay sync and heavier commands can await Dagster jobs. Detection
  uses `inspect.iscoroutine()` on the runner result.
  """
  import inspect

  # 1. Idempotency cache lookup. Requires both a key AND a fingerprint
  # so a route handler can't accidentally request idempotency without
  # supplying the body fingerprint that protects against key reuse.
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
      # Return a copy with `idempotent_replay=True` rather than mutating
      # `cached` in place. The production `IdempotencyCache` deserializes
      # a fresh instance per call (so mutation would be safe), but the
      # dispatcher contract should not depend on that — a future
      # in-memory cache implementation that shares object references
      # across requests would silently corrupt prior envelopes. Pydantic
      # `model_copy` is O(1) for the envelope's small field set.
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

  # 2. Run + time + audit any failure (HTTPException OR otherwise)
  #
  # We catch HTTPException and bare Exception separately so that:
  #   - HTTPException still propagates with its original status code +
  #     detail (FastAPI converts to a JSON error response).
  #   - Any other exception (RuntimeError, IntegrityError, KeyError, …)
  #     also produces a failed audit line before re-raising. Without
  #     this, a buggy command would 500 with no audit record at all.
  start = time.monotonic()
  try:
    maybe_result = runner()
    if inspect.isawaitable(maybe_result):
      result = await maybe_result
    else:
      result = maybe_result
  except HTTPException as exc:
    duration_ms = (time.monotonic() - start) * 1000
    log_operation_audit(
      operation_name=ctx.operation_name,
      operation_id=generate_operation_id(),
      user_id=ctx.user_id,
      graph_id=ctx.graph_id,
      duration_ms=duration_ms,
      status="failed",
      idempotency_key=ctx.idempotency_key,
      error=str(exc.detail),
    )
    raise
  except Exception as exc:
    duration_ms = (time.monotonic() - start) * 1000
    log_operation_audit(
      operation_name=ctx.operation_name,
      operation_id=generate_operation_id(),
      user_id=ctx.user_id,
      graph_id=ctx.graph_id,
      duration_ms=duration_ms,
      status="failed",
      idempotency_key=ctx.idempotency_key,
      error=f"{type(exc).__name__}: {exc}",
    )
    raise

  duration_ms = (time.monotonic() - start) * 1000

  # 3. Wrap envelope, fire side-effect hook, then cache + audit.
  #
  # The hook runs BEFORE caching so that if it raises (e.g. database
  # failure marking the graph stale), the failure aborts the request
  # without poisoning the idempotency cache with a stuck envelope.
  envelope = wrap_completed(ctx.operation_name, result, created_by=ctx.user_id)
  if on_fresh_success is not None:
    on_fresh_success(envelope)
  if use_idempotency:
    await idempotency_cache.put(
      ctx.user_id,
      ctx.graph_id,
      ctx.operation_name,
      ctx.idempotency_key,
      envelope,
      ctx.body_fingerprint,
    )
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


__all__ = [
  "IDEMPOTENCY_TTL_SECONDS",
  "AsyncOperationRunner",
  "IdempotencyCache",
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
  "wrap_completed",
  "wrap_failed",
  "wrap_pending",
]
