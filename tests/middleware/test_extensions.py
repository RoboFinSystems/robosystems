"""Tests for the extensions operation envelope + idempotency + audit helpers.

Exercised before any real command is wired into the dispatcher so envelope
shape, idempotency semantics, and audit logging regress immediately if a
future change breaks them.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from pydantic import BaseModel

from robosystems.middleware import extensions as middleware_module
from robosystems.middleware.extensions import (
  IDEMPOTENCY_TTL_SECONDS,
  IdempotencyCache,
  IdempotencyKeyConflictError,
  OperationContext,
  OperationEnvelope,
  compute_idempotency_cache_key,
  execute_operation,
  fingerprint_body,
  generate_operation_id,
  log_operation_audit,
  wrap_completed,
  wrap_failed,
  wrap_pending,
)


class _SampleResult(BaseModel):
  """Stand-in for a domain Pydantic response model used in envelope tests."""

  id: str
  amount: int


# ---------------------------------------------------------------------------
# Operation ID generator
# ---------------------------------------------------------------------------


class TestGenerateOperationId:
  def test_has_op_prefix_and_ulid_suffix(self) -> None:
    op_id = generate_operation_id()
    assert op_id.startswith("op_")
    suffix = op_id.split("_", 1)[1]
    # Canonical ULID: 26 chars, Crockford base32 (0-9, A-Z minus I L O U).
    assert len(suffix) == 26
    assert suffix.isalnum()
    assert suffix.isupper() or any(c.isdigit() for c in suffix)

  def test_matches_sse_endpoint_regex(self) -> None:
    """The SSE endpoint accepts `^op_[0-9A-Z]{26}$` — this must match."""
    import re

    pattern = re.compile(r"^op_[0-9A-Z]{26}$")
    for _ in range(50):
      assert pattern.match(generate_operation_id()) is not None

  def test_unique_per_call(self) -> None:
    ids = {generate_operation_id() for _ in range(200)}
    assert len(ids) == 200


# ---------------------------------------------------------------------------
# Envelope wrappers
# ---------------------------------------------------------------------------


class TestWrapCompleted:
  def test_pydantic_result_serialized_to_dict(self) -> None:
    envelope = wrap_completed("update-entity", _SampleResult(id="ent_1", amount=42))
    assert envelope.operation == "update-entity"
    assert envelope.status == "completed"
    assert envelope.result == {"id": "ent_1", "amount": 42}
    assert envelope.operation_id.startswith("op_")
    assert envelope.at.endswith("Z")

  def test_dict_result_passes_through(self) -> None:
    envelope = wrap_completed("noop", {"ok": True})
    assert envelope.result == {"ok": True}

  def test_list_result_passes_through(self) -> None:
    envelope = wrap_completed("list-things", [{"id": "a"}, {"id": "b"}])
    assert envelope.result == [{"id": "a"}, {"id": "b"}]

  def test_none_result_is_permitted(self) -> None:
    envelope = wrap_completed("delete-thing", None)
    assert envelope.result is None
    assert envelope.status == "completed"

  def test_caller_supplied_operation_id_is_honored(self) -> None:
    envelope = wrap_completed("x", None, operation_id="op_01ARZ3NDEKTSV4RRFFQ69G5FAV")
    assert envelope.operation_id == "op_01ARZ3NDEKTSV4RRFFQ69G5FAV"

  def test_invalid_result_type_raises(self) -> None:
    with pytest.raises(TypeError):
      wrap_completed("bad", 123)  # type: ignore[arg-type]

  def test_serialization_uses_camel_case_operation_id(self) -> None:
    """The on-the-wire JSON must use `operationId`, not `operation_id`.

    Downstream SDKs unwrap the envelope via camelCase GraphQL/REST
    conventions; a snake_case leak would break them silently.
    """
    envelope = wrap_completed("noop", None, created_by="usr_42")
    payload = json.loads(envelope.model_dump_json(by_alias=True))
    assert "operationId" in payload
    assert "operation_id" not in payload
    assert "createdBy" in payload
    assert "created_by" not in payload
    assert "idempotentReplay" in payload
    assert "idempotent_replay" not in payload
    assert payload["createdBy"] == "usr_42"
    assert payload["idempotentReplay"] is False
    assert payload["status"] == "completed"
    assert set(payload.keys()) == {
      "operation",
      "operationId",
      "status",
      "result",
      "at",
      "createdBy",
      "idempotentReplay",
    }


class TestWrapPending:
  def test_pending_requires_explicit_operation_id(self) -> None:
    envelope = wrap_pending(
      "auto-map-elements",
      operation_id="op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
    )
    assert envelope.status == "pending"
    assert envelope.operation_id == "op_01ARZ3NDEKTSV4RRFFQ69G5FAV"
    assert envelope.result is None

  def test_pending_can_carry_partial_result(self) -> None:
    envelope = wrap_pending(
      "create-report",
      operation_id="op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
      partial_result={"report_id": "rpt_123", "queued": True},
    )
    assert envelope.result == {"report_id": "rpt_123", "queued": True}


class TestWrapFailed:
  def test_string_error_becomes_error_dict(self) -> None:
    envelope = wrap_failed("close-period", "period already closed")
    assert envelope.status == "failed"
    assert envelope.result == {"error": "period already closed"}

  def test_dict_error_preserves_details(self) -> None:
    envelope = wrap_failed(
      "close-period",
      {"error": "validation failed", "code": "INVALID_PERIOD", "field": "start"},
    )
    assert envelope.result is not None
    assert envelope.result["error"] == "validation failed"
    assert envelope.result["code"] == "INVALID_PERIOD"
    assert envelope.result["field"] == "start"


# ---------------------------------------------------------------------------
# Idempotency cache
# ---------------------------------------------------------------------------


class TestComputeIdempotencyCacheKey:
  """The cache key is now scoped by (user_id, graph_id, operation, key)."""

  def test_shape(self) -> None:
    key = compute_idempotency_cache_key("usr_abc", "kg123", "close-period", "abc-xyz")
    assert key.startswith("idem:kg123:close-period:")
    # 32-char hex suffix
    suffix = key.rsplit(":", 1)[1]
    assert len(suffix) == 32
    assert all(c in "0123456789abcdef" for c in suffix)

  def test_same_tuple_produces_same_key(self) -> None:
    a = compute_idempotency_cache_key("usr_a", "kg1", "op", "k1")
    b = compute_idempotency_cache_key("usr_a", "kg1", "op", "k1")
    assert a == b

  def test_different_user_produces_different_key(self) -> None:
    """Two users using the same idempotency key must NOT collide."""
    a = compute_idempotency_cache_key("usr_a", "kg1", "op", "k1")
    b = compute_idempotency_cache_key("usr_b", "kg1", "op", "k1")
    assert a != b, "Same idempotency key under different users must not collide"

  def test_different_graph_produces_different_key(self) -> None:
    a = compute_idempotency_cache_key("usr_a", "kg1", "op", "k1")
    b = compute_idempotency_cache_key("usr_a", "kg2", "op", "k1")
    assert a != b

  def test_different_operation_produces_different_key(self) -> None:
    a = compute_idempotency_cache_key("usr_a", "kg1", "op-a", "k1")
    b = compute_idempotency_cache_key("usr_a", "kg1", "op-b", "k1")
    assert a != b

  def test_different_idempotency_key_produces_different_cache_key(self) -> None:
    a = compute_idempotency_cache_key("usr_a", "kg1", "op", "k1")
    b = compute_idempotency_cache_key("usr_a", "kg1", "op", "k2")
    assert a != b

  def test_user_id_not_in_cleartext(self) -> None:
    """The user_id is hashed with the key — never appears verbatim."""
    cache_key = compute_idempotency_cache_key("usr_super_secret", "kg1", "op", "k1")
    assert "usr_super_secret" not in cache_key


class TestFingerprintBody:
  def test_pydantic_model_stable_across_field_order(self) -> None:
    a = _SampleResult(id="x", amount=1)
    b = _SampleResult(amount=1, id="x")
    assert fingerprint_body(a) == fingerprint_body(b)

  def test_dict_stable_across_key_order(self) -> None:
    assert fingerprint_body({"a": 1, "b": 2}) == fingerprint_body({"b": 2, "a": 1})

  def test_different_payloads_different_fingerprints(self) -> None:
    assert fingerprint_body({"a": 1}) != fingerprint_body({"a": 2})

  def test_none_has_stable_fingerprint(self) -> None:
    assert fingerprint_body(None) == fingerprint_body(None)

  def test_pydantic_vs_dict_match(self) -> None:
    """A model and its model_dump should fingerprint the same."""
    model = _SampleResult(id="x", amount=1)
    assert fingerprint_body(model) == fingerprint_body(model.model_dump(mode="json"))


class TestIdempotencyCache:
  def _make_cache(
    self, fake_store: dict[str, str] | None = None
  ) -> tuple[IdempotencyCache, AsyncMock, dict[str, str]]:
    """Build a cache backed by an in-memory dict via AsyncMock."""
    store = fake_store if fake_store is not None else {}

    mock_client = AsyncMock()

    async def _get(key: str) -> str | None:
      return store.get(key)

    async def _set(key: str, value: str, ex: int | None = None) -> None:
      store[key] = value

    async def _delete(key: str) -> None:
      store.pop(key, None)

    mock_client.get = AsyncMock(side_effect=_get)
    mock_client.set = AsyncMock(side_effect=_set)
    mock_client.delete = AsyncMock(side_effect=_delete)

    return IdempotencyCache(client=mock_client), mock_client, store

  @pytest.mark.asyncio
  async def test_miss_returns_none(self) -> None:
    cache, _client, _store = self._make_cache()
    result = await cache.get("usr_a", "kg1", "close-period", "key-1", "fp1")
    assert result is None

  @pytest.mark.asyncio
  async def test_put_then_get_roundtrips_envelope(self) -> None:
    cache, _client, _store = self._make_cache()
    envelope = wrap_completed(
      "close-period",
      _SampleResult(id="ent_1", amount=42),
      operation_id="op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
    )

    await cache.put("usr_a", "kg1", "close-period", "key-1", envelope, "fp1")
    cached = await cache.get("usr_a", "kg1", "close-period", "key-1", "fp1")

    assert cached is not None
    assert isinstance(cached, OperationEnvelope)
    assert cached.operation == "close-period"
    assert cached.operation_id == "op_01ARZ3NDEKTSV4RRFFQ69G5FAV"
    assert cached.status == "completed"
    assert cached.result == {"id": "ent_1", "amount": 42}

  @pytest.mark.asyncio
  async def test_put_uses_expected_ttl(self) -> None:
    cache, client, _store = self._make_cache()
    envelope = wrap_completed("op", None)
    await cache.put("usr_a", "kg1", "op", "key-1", envelope, "fp1")
    client.set.assert_awaited_once()
    _args, kwargs = client.set.call_args
    assert kwargs["ex"] == IDEMPOTENCY_TTL_SECONDS

  @pytest.mark.asyncio
  async def test_isolation_by_graph_and_operation(self) -> None:
    cache, _client, _store = self._make_cache()
    env_a = wrap_completed("op-a", {"a": 1})
    env_b = wrap_completed("op-b", {"b": 2})
    env_other_graph = wrap_completed("op-a", {"a": 99})

    await cache.put("usr_a", "kg1", "op-a", "same-key", env_a, "fp")
    await cache.put("usr_a", "kg1", "op-b", "same-key", env_b, "fp")
    await cache.put("usr_a", "kg2", "op-a", "same-key", env_other_graph, "fp")

    cached_a = await cache.get("usr_a", "kg1", "op-a", "same-key", "fp")
    cached_b = await cache.get("usr_a", "kg1", "op-b", "same-key", "fp")
    cached_other = await cache.get("usr_a", "kg2", "op-a", "same-key", "fp")

    assert cached_a is not None and cached_a.result == {"a": 1}
    assert cached_b is not None and cached_b.result == {"b": 2}
    assert cached_other is not None and cached_other.result == {"a": 99}

  @pytest.mark.asyncio
  async def test_isolation_by_user(self) -> None:
    """Two users with the same key must not see each other's envelopes."""
    cache, _client, _store = self._make_cache()
    env_a = wrap_completed("create-portfolio", {"id": "pf_a"})
    env_b = wrap_completed("create-portfolio", {"id": "pf_b"})

    await cache.put("usr_a", "kg1", "create-portfolio", "shared-key", env_a, "fp")
    await cache.put("usr_b", "kg1", "create-portfolio", "shared-key", env_b, "fp")

    cached_a = await cache.get("usr_a", "kg1", "create-portfolio", "shared-key", "fp")
    cached_b = await cache.get("usr_b", "kg1", "create-portfolio", "shared-key", "fp")

    assert cached_a is not None and cached_a.result == {"id": "pf_a"}
    assert cached_b is not None and cached_b.result == {"id": "pf_b"}

  @pytest.mark.asyncio
  async def test_key_reuse_with_different_body_raises_conflict(self) -> None:
    """Same idempotency key + different body → IdempotencyKeyConflictError."""
    cache, _client, _store = self._make_cache()
    envelope = wrap_completed("create-portfolio", {"id": "pf_1"})

    await cache.put(
      "usr_a", "kg1", "create-portfolio", "retry-key", envelope, "fp_first"
    )

    with pytest.raises(IdempotencyKeyConflictError):
      await cache.get("usr_a", "kg1", "create-portfolio", "retry-key", "fp_DIFFERENT")

  @pytest.mark.asyncio
  async def test_key_reuse_with_same_body_returns_cached(self) -> None:
    """Same idempotency key + same body → replay (no error)."""
    cache, _client, _store = self._make_cache()
    envelope = wrap_completed("create-portfolio", {"id": "pf_1"})

    await cache.put(
      "usr_a", "kg1", "create-portfolio", "retry-key", envelope, "fp_same"
    )
    replayed = await cache.get(
      "usr_a", "kg1", "create-portfolio", "retry-key", "fp_same"
    )

    assert replayed is not None
    assert replayed.result == {"id": "pf_1"}

  @pytest.mark.asyncio
  async def test_corrupt_payload_is_evicted(self) -> None:
    cache, _client, store = self._make_cache()
    # Pre-seed the store with non-JSON garbage at the canonical key.
    cache_key = compute_idempotency_cache_key("usr_a", "kg1", "op", "k1")
    store[cache_key] = "{not-json"

    result = await cache.get("usr_a", "kg1", "op", "k1", "fp")

    assert result is None
    assert cache_key not in store  # eviction happened


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------


class TestLogOperationAudit:
  """Verifies the audit log payload shape.

  The project-wide `robosystems.logger` is a structured logger that wraps
  the stdlib logger and does not propagate `extra=` through pytest's
  `caplog` fixture. We assert against the call instead.
  """

  def _get_audit_payload(self, mock_logger_fn: Any) -> dict[str, Any]:
    mock_logger_fn.assert_called_once()
    call = mock_logger_fn.call_args
    assert call.args[0] == "extensions.operation"
    extra = call.kwargs["extra"]
    assert "audit" in extra
    payload: dict[str, Any] = extra["audit"]
    return payload

  def test_completed_logs_at_info_with_expected_fields(self) -> None:
    with patch.object(middleware_module.logger, "info") as info_mock:
      log_operation_audit(
        operation_name="close-period",
        operation_id="op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        user_id="usr_abc",
        graph_id="kg1",
        duration_ms=123.456,
        status="completed",
        idempotency_key="caller-provided-key",
      )

    audit = self._get_audit_payload(info_mock)
    assert audit["event"] == "extensions.operation"
    assert audit["operation"] == "close-period"
    assert audit["operation_id"] == "op_01ARZ3NDEKTSV4RRFFQ69G5FAV"
    assert audit["user_id"] == "usr_abc"
    assert audit["graph_id"] == "kg1"
    assert audit["duration_ms"] == 123.46  # rounded
    assert audit["status"] == "completed"
    assert audit["idempotent_replay"] is False
    # Idempotency key is hashed, never retained verbatim.
    assert "idempotency_key" not in audit
    assert "idempotency_key_hash" in audit
    assert audit["idempotency_key_hash"] != "caller-provided-key"
    assert len(audit["idempotency_key_hash"]) == 16

  def test_failed_logs_at_error_with_error_field(self) -> None:
    with (
      patch.object(middleware_module.logger, "info") as info_mock,
      patch.object(middleware_module.logger, "error") as error_mock,
    ):
      log_operation_audit(
        operation_name="close-period",
        operation_id="op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        user_id="usr_abc",
        graph_id="kg1",
        duration_ms=5.0,
        status="failed",
        error="period already closed",
      )

    info_mock.assert_not_called()
    audit = self._get_audit_payload(error_mock)
    assert audit["status"] == "failed"
    assert audit["error"] == "period already closed"

  def test_idempotent_replay_flag_propagates(self) -> None:
    with patch.object(middleware_module.logger, "info") as info_mock:
      log_operation_audit(
        operation_name="noop",
        operation_id="op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        user_id="usr_abc",
        graph_id="kg1",
        duration_ms=0.1,
        status="completed",
        idempotent_replay=True,
      )

    audit = self._get_audit_payload(info_mock)
    assert audit["idempotent_replay"] is True

  def test_no_idempotency_key_omits_hash(self) -> None:
    with patch.object(middleware_module.logger, "info") as info_mock:
      log_operation_audit(
        operation_name="noop",
        operation_id="op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        user_id="usr_abc",
        graph_id="kg1",
        duration_ms=0.1,
        status="completed",
      )

    audit = self._get_audit_payload(info_mock)
    assert "idempotency_key_hash" not in audit


# ---------------------------------------------------------------------------
# execute_operation — the dispatch helper used by every REST operation route
# ---------------------------------------------------------------------------


class _FakeIdempotencyCache:
  """In-memory cache standing in for the real Valkey-backed one.

  Mirrors the real `IdempotencyCache` API so dispatcher tests can
  exercise the (user, graph, operation, key, fingerprint) tuple
  without spinning up Redis.
  """

  def __init__(self) -> None:
    self.store: dict[tuple[str, str, str, str], tuple[OperationEnvelope, str]] = {}

  async def get(
    self,
    user_id: str,
    graph_id: str,
    operation_name: str,
    idempotency_key: str,
    body_fingerprint: str,
  ) -> OperationEnvelope | None:
    entry = self.store.get((user_id, graph_id, operation_name, idempotency_key))
    if entry is None:
      return None
    cached_envelope, cached_fingerprint = entry
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
    self.store[(user_id, graph_id, operation_name, idempotency_key)] = (
      envelope,
      body_fingerprint,
    )


def _make_ctx(**overrides: Any) -> OperationContext:
  defaults: dict[str, Any] = {
    "domain": "roboinvestor",
    "operation_name": "create-portfolio",
    "graph_id": "kg1",
    "user_id": "usr_1",
    "idempotency_key": None,
    "body_fingerprint": None,
  }
  defaults.update(overrides)
  return OperationContext(**defaults)


class TestExecuteOperationHappyPath:
  @pytest.mark.asyncio
  async def test_wraps_pydantic_result_in_envelope(self) -> None:
    ctx = _make_ctx()
    result = _SampleResult(id="pf_1", amount=42)

    with patch.object(middleware_module.logger, "info"):
      envelope = await execute_operation(ctx, lambda: result)

    assert envelope.operation == "create-portfolio"
    assert envelope.status == "completed"
    assert envelope.result == {"id": "pf_1", "amount": 42}
    assert envelope.operation_id.startswith("op_")
    # created_by propagates from the OperationContext.user_id so the
    # envelope carries caller provenance without audit-log lookups.
    assert envelope.created_by == "usr_1"

  @pytest.mark.asyncio
  async def test_supports_async_runner(self) -> None:
    ctx = _make_ctx(operation_name="slow-op")

    async def _async_runner():
      return _SampleResult(id="x", amount=1)

    with patch.object(middleware_module.logger, "info"):
      envelope = await execute_operation(ctx, _async_runner)

    assert envelope.result == {"id": "x", "amount": 1}

  @pytest.mark.asyncio
  async def test_none_result_produces_null_envelope_result(self) -> None:
    ctx = _make_ctx(operation_name="delete-thing")
    with patch.object(middleware_module.logger, "info"):
      envelope = await execute_operation(ctx, lambda: None)
    assert envelope.status == "completed"
    assert envelope.result is None

  @pytest.mark.asyncio
  async def test_audit_log_emitted_on_success(self) -> None:
    ctx = _make_ctx(idempotency_key="key-1", body_fingerprint="fp1")
    with patch.object(middleware_module.logger, "info") as info_mock:
      await execute_operation(
        ctx, lambda: {"ok": True}, idempotency_cache=_FakeIdempotencyCache()
      )

    info_mock.assert_called()
    # Find the audit call (there may be other info logs)
    audit_calls = [
      c for c in info_mock.call_args_list if c.kwargs.get("extra", {}).get("audit")
    ]
    assert len(audit_calls) == 1
    audit = audit_calls[0].kwargs["extra"]["audit"]
    assert audit["status"] == "completed"
    assert audit["operation"] == "create-portfolio"
    assert audit["idempotent_replay"] is False
    assert audit["duration_ms"] >= 0


class TestExecuteOperationFailurePath:
  @pytest.mark.asyncio
  async def test_http_exception_re_raised(self) -> None:
    ctx = _make_ctx()

    def _runner():
      raise HTTPException(status_code=404, detail="Not found")

    with (
      patch.object(middleware_module.logger, "error"),
      pytest.raises(HTTPException) as exc_info,
    ):
      await execute_operation(ctx, _runner)

    assert exc_info.value.status_code == 404

  @pytest.mark.asyncio
  async def test_failure_logged_as_error_with_status_failed(self) -> None:
    ctx = _make_ctx(operation_name="update-portfolio")

    def _runner():
      raise HTTPException(status_code=409, detail="Conflict")

    with (
      patch.object(middleware_module.logger, "error") as error_mock,
      pytest.raises(HTTPException),
    ):
      await execute_operation(ctx, _runner)

    audit_calls = [
      c for c in error_mock.call_args_list if c.kwargs.get("extra", {}).get("audit")
    ]
    assert len(audit_calls) == 1
    audit = audit_calls[0].kwargs["extra"]["audit"]
    assert audit["status"] == "failed"
    assert audit["error"] == "Conflict"

  @pytest.mark.asyncio
  async def test_failed_operation_not_cached(self) -> None:
    ctx = _make_ctx(idempotency_key="key-fail", body_fingerprint="fp")
    cache = _FakeIdempotencyCache()

    def _runner():
      raise HTTPException(status_code=500, detail="Boom")

    with patch.object(middleware_module.logger, "error"), pytest.raises(HTTPException):
      await execute_operation(ctx, _runner, idempotency_cache=cache)

    assert cache.store == {}  # failures must not be cached

  @pytest.mark.asyncio
  async def test_unexpected_exception_audited_and_re_raised(self) -> None:
    """Non-HTTPException failures must still produce a failed audit line.

    Regression: an earlier version only caught HTTPException, so a bare
    `RuntimeError` (or `IntegrityError` from SQLAlchemy, or `KeyError`,
    etc.) would 500 with no audit record at all.
    """
    ctx = _make_ctx(operation_name="buggy-op")

    def _runner():
      raise RuntimeError("kaboom")

    with (
      patch.object(middleware_module.logger, "error") as error_mock,
      pytest.raises(RuntimeError) as exc_info,
    ):
      await execute_operation(ctx, _runner)

    assert "kaboom" in str(exc_info.value)
    audit_calls = [
      c for c in error_mock.call_args_list if c.kwargs.get("extra", {}).get("audit")
    ]
    assert len(audit_calls) == 1
    audit = audit_calls[0].kwargs["extra"]["audit"]
    assert audit["status"] == "failed"
    assert audit["operation"] == "buggy-op"
    # The error string includes the exception type for triage
    assert "RuntimeError" in audit["error"]
    assert "kaboom" in audit["error"]

  @pytest.mark.asyncio
  async def test_unexpected_exception_not_cached(self) -> None:
    """Bare exceptions must not poison the idempotency cache either."""
    ctx = _make_ctx(idempotency_key="key-bare-fail", body_fingerprint="fp")
    cache = _FakeIdempotencyCache()

    def _runner():
      raise KeyError("missing")

    with (
      patch.object(middleware_module.logger, "error"),
      pytest.raises(KeyError),
    ):
      await execute_operation(ctx, _runner, idempotency_cache=cache)

    assert cache.store == {}


class TestExecuteOperationIdempotency:
  @pytest.mark.asyncio
  async def test_idempotent_replay_returns_cached_envelope(self) -> None:
    ctx = _make_ctx(idempotency_key="key-abc", body_fingerprint="fp1")
    cache = _FakeIdempotencyCache()

    # First call populates the cache
    with patch.object(middleware_module.logger, "info"):
      first = await execute_operation(
        ctx, lambda: {"version": 1}, idempotency_cache=cache
      )

    # Second call with same key + same fingerprint should hit the cache
    runner_called = [False]

    def _should_not_be_called():
      runner_called[0] = True
      return {"version": 2}

    with patch.object(middleware_module.logger, "info") as info_mock:
      second = await execute_operation(
        ctx, _should_not_be_called, idempotency_cache=cache
      )

    assert runner_called[0] is False
    assert second.operation_id == first.operation_id
    assert second.result == {"version": 1}
    # Fresh execution sets idempotent_replay=False; cache hit flips it to
    # True so clients and the metrics decorator can distinguish them.
    assert first.idempotent_replay is False
    assert second.idempotent_replay is True

    # Replay should log with idempotent_replay=True
    audit_calls = [
      c for c in info_mock.call_args_list if c.kwargs.get("extra", {}).get("audit")
    ]
    assert any(
      c.kwargs["extra"]["audit"]["idempotent_replay"] is True for c in audit_calls
    )

  @pytest.mark.asyncio
  async def test_key_reuse_with_different_body_raises_409(self) -> None:
    """Key reused with a different body must raise IdempotencyKeyConflictError."""
    cache = _FakeIdempotencyCache()

    # First call: populate with body fingerprint "fp_first"
    ctx_first = _make_ctx(idempotency_key="retry-key", body_fingerprint="fp_first")
    with patch.object(middleware_module.logger, "info"):
      await execute_operation(ctx_first, lambda: {"v": 1}, idempotency_cache=cache)

    # Second call: same key, DIFFERENT body fingerprint
    ctx_second = _make_ctx(idempotency_key="retry-key", body_fingerprint="fp_DIFFERENT")
    runner_called = [False]

    def _runner():
      runner_called[0] = True
      return {"v": 2}

    with (
      patch.object(middleware_module.logger, "error"),
      patch.object(middleware_module.logger, "info"),
      pytest.raises(IdempotencyKeyConflictError),
    ):
      await execute_operation(ctx_second, _runner, idempotency_cache=cache)

    # Conflict short-circuits — the runner never runs
    assert runner_called[0] is False

  @pytest.mark.asyncio
  async def test_no_idempotency_key_means_no_cache_hit(self) -> None:
    ctx = _make_ctx()  # no idempotency_key, no fingerprint
    cache = _FakeIdempotencyCache()

    counter = [0]

    def _runner():
      counter[0] += 1
      return {"count": counter[0]}

    with patch.object(middleware_module.logger, "info"):
      r1 = await execute_operation(ctx, _runner, idempotency_cache=cache)
      r2 = await execute_operation(ctx, _runner, idempotency_cache=cache)

    # Runner was called twice — no cache involvement
    assert counter[0] == 2
    assert r1.result == {"count": 1}
    assert r2.result == {"count": 2}

  @pytest.mark.asyncio
  async def test_idempotency_key_without_fingerprint_skips_cache(self) -> None:
    """Defensive: a key without a fingerprint must not enable caching.

    The dispatcher requires BOTH so a route handler that forgets to
    set `body_fingerprint` doesn't accidentally enable cross-payload
    replay. Two calls with the same key but no fingerprint should
    both execute the runner.
    """
    ctx = _make_ctx(idempotency_key="key-no-fp")  # fingerprint=None
    cache = _FakeIdempotencyCache()
    counter = [0]

    def _runner():
      counter[0] += 1
      return {}

    with patch.object(middleware_module.logger, "info"):
      await execute_operation(ctx, _runner, idempotency_cache=cache)
      await execute_operation(ctx, _runner, idempotency_cache=cache)

    assert counter[0] == 2
    assert cache.store == {}  # nothing was cached

  @pytest.mark.asyncio
  async def test_no_cache_instance_skips_idempotency(self) -> None:
    """Even with a key, if no cache is supplied, the runner still runs."""
    ctx = _make_ctx(idempotency_key="key-no-cache", body_fingerprint="fp")
    counter = [0]

    def _runner():
      counter[0] += 1
      return {}

    with patch.object(middleware_module.logger, "info"):
      await execute_operation(ctx, _runner, idempotency_cache=None)
      await execute_operation(ctx, _runner, idempotency_cache=None)

    assert counter[0] == 2

  @pytest.mark.asyncio
  async def test_different_keys_get_separate_cache_entries(self) -> None:
    ctx_a = _make_ctx(idempotency_key="key-a", body_fingerprint="fp")
    ctx_b = _make_ctx(idempotency_key="key-b", body_fingerprint="fp")
    cache = _FakeIdempotencyCache()

    with patch.object(middleware_module.logger, "info"):
      a = await execute_operation(ctx_a, lambda: {"k": "a"}, idempotency_cache=cache)
      b = await execute_operation(ctx_b, lambda: {"k": "b"}, idempotency_cache=cache)

    assert a.operation_id != b.operation_id
    assert a.result == {"k": "a"}
    assert b.result == {"k": "b"}

  @pytest.mark.asyncio
  async def test_different_users_with_same_key_isolated(self) -> None:
    """The cache must scope by user — same key from two users must not collide."""
    ctx_alice = _make_ctx(
      user_id="usr_alice", idempotency_key="shared-key", body_fingerprint="fp"
    )
    ctx_bob = _make_ctx(
      user_id="usr_bob", idempotency_key="shared-key", body_fingerprint="fp"
    )
    cache = _FakeIdempotencyCache()

    with patch.object(middleware_module.logger, "info"):
      env_alice = await execute_operation(
        ctx_alice, lambda: {"who": "alice"}, idempotency_cache=cache
      )
      env_bob = await execute_operation(
        ctx_bob, lambda: {"who": "bob"}, idempotency_cache=cache
      )

    assert env_alice.result == {"who": "alice"}
    assert env_bob.result == {"who": "bob"}
    assert env_alice.operation_id != env_bob.operation_id


class TestExecuteOperationFreshSuccessHook:
  """Verify on_fresh_success runs only on fresh execution, not on replay.

  This is the contract that lets route handlers safely move side
  effects like `mark_graph_stale` into a hook without firing them on
  every retry.
  """

  @pytest.mark.asyncio
  async def test_hook_runs_on_fresh_success(self) -> None:
    ctx = _make_ctx(idempotency_key="key-1", body_fingerprint="fp")
    cache = _FakeIdempotencyCache()
    fired: list[OperationEnvelope] = []

    with patch.object(middleware_module.logger, "info"):
      await execute_operation(
        ctx,
        lambda: {"ok": True},
        idempotency_cache=cache,
        on_fresh_success=lambda env: fired.append(env),
      )

    assert len(fired) == 1
    assert fired[0].status == "completed"

  @pytest.mark.asyncio
  async def test_hook_does_not_run_on_replay(self) -> None:
    ctx = _make_ctx(idempotency_key="key-replay", body_fingerprint="fp")
    cache = _FakeIdempotencyCache()
    fired: list[OperationEnvelope] = []

    # First call — hook fires
    with patch.object(middleware_module.logger, "info"):
      await execute_operation(
        ctx,
        lambda: {"ok": True},
        idempotency_cache=cache,
        on_fresh_success=lambda env: fired.append(env),
      )

    assert len(fired) == 1

    # Second call (replay) — hook MUST NOT fire
    with patch.object(middleware_module.logger, "info"):
      await execute_operation(
        ctx,
        lambda: {"ok": True},
        idempotency_cache=cache,
        on_fresh_success=lambda env: fired.append(env),
      )

    assert len(fired) == 1, "on_fresh_success fired on replay — side effect leaked"

  @pytest.mark.asyncio
  async def test_hook_does_not_run_on_failure(self) -> None:
    ctx = _make_ctx()
    fired: list[OperationEnvelope] = []

    def _runner():
      raise HTTPException(status_code=500, detail="boom")

    with (
      patch.object(middleware_module.logger, "error"),
      pytest.raises(HTTPException),
    ):
      await execute_operation(
        ctx,
        _runner,
        on_fresh_success=lambda env: fired.append(env),
      )

    assert fired == []

  @pytest.mark.asyncio
  async def test_hook_exception_aborts_caching(self) -> None:
    """If the hook raises, the failure must not leave a stale cache entry."""
    ctx = _make_ctx(idempotency_key="key-hook-fail", body_fingerprint="fp")
    cache = _FakeIdempotencyCache()

    def _bad_hook(_env):
      raise RuntimeError("staleness DB down")

    with (
      patch.object(middleware_module.logger, "info"),
      pytest.raises(RuntimeError),
    ):
      await execute_operation(
        ctx,
        lambda: {"ok": True},
        idempotency_cache=cache,
        on_fresh_success=_bad_hook,
      )

    assert cache.store == {}, "Hook failure should not poison the cache"
