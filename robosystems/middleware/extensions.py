"""Operation envelope, idempotency cache, and audit helpers for extensions.

Every `POST /extensions/{domain}/{graph_id}/operations/{operation_name}`
endpoint shares the `OperationEnvelope` response shape built here. Three
concerns live in this module so the per-domain routers stay thin:

1. **Envelope** — wrap a command's Pydantic result in a uniform payload.
2. **Idempotency** — cache completed envelopes in Valkey keyed by the
   caller's `Idempotency-Key` header so retries are safe for 24 hours.
3. **Audit** — structured log per operation call with the durations and
   identifiers a SOC-2-style audit trail needs.

Operation IDs reuse `robosystems.utils.ulid.generate_prefixed_ulid("op")`
so the existing `/v1/operations/{operation_id}/stream` SSE endpoint (which
already matches `^op_[0-9A-Z]{26}$`) accepts async operation IDs without
any regex changes.
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
  """Uniform response shape for every extensions operation endpoint.

  - `operation`: kebab-case command name (e.g. `close-period`)
  - `operation_id`: `op_`-prefixed ULID; always present, usable for audit
    correlation and — for async commands — SSE subscription via
    `/v1/operations/{operation_id}/stream`
  - `status`: `"completed"` (sync, HTTP 200), `"pending"` (async, HTTP 202),
    or `"failed"` (error responses)
  - `result`: the domain-specific payload (the original Pydantic response)
    or `None` for async/failed cases
  - `at`: ISO-8601 UTC timestamp of when the envelope was minted
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


def wrap_completed(
  operation_name: str,
  result: BaseModel | dict[str, Any] | list[Any] | None,
  operation_id: str | None = None,
) -> OperationEnvelope:
  """Build a `status="completed"` envelope for a sync command result."""
  return OperationEnvelope(
    operation=operation_name,
    operationId=operation_id or generate_operation_id(),
    status="completed",
    result=_result_to_payload(result),
    at=_utcnow_iso(),
  )


def wrap_pending(
  operation_name: str,
  operation_id: str,
  partial_result: BaseModel | dict[str, Any] | list[Any] | None = None,
) -> OperationEnvelope:
  """Build a `status="pending"` envelope for an async-dispatched command.

  `operation_id` is required here because the caller already registered
  the operation with the SSE infrastructure (or the Dagster dispatcher)
  and needs the same ID in the response for streaming.
  """
  return OperationEnvelope(
    operation=operation_name,
    operationId=operation_id,
    status="pending",
    result=_result_to_payload(partial_result),
    at=_utcnow_iso(),
  )


def wrap_failed(
  operation_name: str,
  error: str | dict[str, Any],
  operation_id: str | None = None,
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
  )


# ---------------------------------------------------------------------------
# Idempotency cache
# ---------------------------------------------------------------------------


def compute_idempotency_cache_key(
  graph_id: str, operation_name: str, idempotency_key: str
) -> str:
  """Deterministic Valkey key for a `(graph_id, operation, key)` triple.

  The idempotency key is hashed so arbitrary client-supplied strings
  (including UUIDs, random nonces, or whatever the SDK produces) never
  become keyspace liabilities.
  """
  digest = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()[:32]
  return f"idem:{graph_id}:{operation_name}:{digest}"


class IdempotencyCache:
  """Thin async wrapper over the `OPERATION_IDEMPOTENCY` Valkey DB.

  Stores the full `OperationEnvelope` (serialized as JSON) keyed by the
  hashed `(graph_id, operation_name, idempotency_key)` triple. A fresh
  client is created per instance to avoid sharing connection state across
  request lifetimes; callers typically instantiate one per request.
  """

  def __init__(self, client: Any | None = None) -> None:
    self._client = client or create_async_redis_client(
      ValkeyDatabase.OPERATION_IDEMPOTENCY, decode_responses=True
    )

  async def get(
    self, graph_id: str, operation_name: str, idempotency_key: str
  ) -> OperationEnvelope | None:
    """Return a cached envelope, or `None` if nothing is stored."""
    cache_key = compute_idempotency_cache_key(graph_id, operation_name, idempotency_key)
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
      return OperationEnvelope.model_validate(json.loads(raw))
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

  async def put(
    self,
    graph_id: str,
    operation_name: str,
    idempotency_key: str,
    envelope: OperationEnvelope,
    ttl_seconds: int = IDEMPOTENCY_TTL_SECONDS,
  ) -> None:
    """Cache an envelope for `ttl_seconds` (default 24 hours)."""
    cache_key = compute_idempotency_cache_key(graph_id, operation_name, idempotency_key)
    payload = envelope.model_dump_json(by_alias=True)
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
) -> None:
  """Emit one structured audit-log line per operation call.

  The audit stream is consumed by the standard logging pipeline (CloudWatch
  in prod, stdout in dev). Fields are picked to satisfy a SOC-2-style
  "who did what, to which tenant, when, with what result" review.
  """
  payload: dict[str, Any] = {
    "event": "extensions.operation",
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
    logger.error("extensions.operation", extra={"audit": payload})
  else:
    logger.info("extensions.operation", extra={"audit": payload})


# ---------------------------------------------------------------------------
# Operation dispatcher — generic execute_operation() helper used by every
# `POST /extensions/{domain}/{graph_id}/operations/{operation_name}` route.
# ---------------------------------------------------------------------------


@dataclass
class OperationContext:
  """Per-call context carried through `execute_operation`.

  Captures the identifying tuple for a single operation call —
  everything needed by the idempotency cache, the audit log, and any
  async Dagster dispatch that needs user + graph provenance.
  """

  domain: str
  operation_name: str
  graph_id: str
  user_id: str
  idempotency_key: str | None = None


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


async def execute_operation(
  ctx: OperationContext,
  runner: OperationRunner | AsyncOperationRunner,
  idempotency_cache: IdempotencyCache | None = None,
) -> OperationEnvelope:
  """Run an operation and return its `OperationEnvelope`.

  Responsibilities:

  1. **Idempotency lookup** — if `ctx.idempotency_key` is set and a
     cache is provided, check for a cached envelope; on hit, return
     it and emit an `idempotent_replay=True` audit line.
  2. **Timing** — wall-clock duration of the runner call (excludes
     idempotency lookup).
  3. **Audit logging** — emit exactly one structured audit line with
     `status` set to `"completed"` (happy path) or `"failed"`
     (HTTPException raised).
  4. **Envelope wrapping** — `wrap_completed(result)` on success.
  5. **Idempotency store** — cache the successful envelope so retries
     within the TTL return it unchanged.

  The `runner` callable is responsible for:
  - Opening `extensions_session(graph_id)`
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

  # 1. Idempotency cache lookup
  if ctx.idempotency_key and idempotency_cache is not None:
    cached = await idempotency_cache.get(
      ctx.graph_id, ctx.operation_name, ctx.idempotency_key
    )
    if cached is not None:
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

  # 3. Wrap + cache + audit
  envelope = wrap_completed(ctx.operation_name, result)
  if ctx.idempotency_key and idempotency_cache is not None:
    await idempotency_cache.put(
      ctx.graph_id, ctx.operation_name, ctx.idempotency_key, envelope
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
  "OperationContext",
  "OperationEnvelope",
  "OperationRunner",
  "OperationStatus",
  "compute_idempotency_cache_key",
  "execute_operation",
  "generate_operation_id",
  "get_idempotency_cache",
  "log_operation_audit",
  "wrap_completed",
  "wrap_failed",
  "wrap_pending",
]
