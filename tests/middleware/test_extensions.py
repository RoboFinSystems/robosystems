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

from robosystems.middleware import operations as middleware_module
from robosystems.middleware.operations import (
  IDEMPOTENCY_TTL_SECONDS,
  IdempotencyCache,
  IdempotencyInProgressError,
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

    async def _set(
      key: str, value: str, ex: int | None = None, nx: bool = False
    ) -> bool | None:
      if nx and key in store:
        return None
      store[key] = value
      return True

    async def _delete(*keys: str) -> int:
      return sum(1 for key in keys if store.pop(key, None) is not None)

    # Sets back the operation → envelope-key bindings; a set is stored in the
    # same dict so `store` stays the single view of what the cache holds.
    async def _sadd(key: str, *members: str) -> int:
      current = store.setdefault(key, set())
      assert isinstance(current, set)
      added = len(set(members) - current)
      current.update(members)
      return added

    async def _smembers(key: str) -> set[str]:
      current = store.get(key)
      return set(current) if isinstance(current, set) else set()

    async def _expire(key: str, seconds: int) -> bool:
      return key in store

    mock_client.get = AsyncMock(side_effect=_get)
    mock_client.set = AsyncMock(side_effect=_set)
    mock_client.delete = AsyncMock(side_effect=_delete)
    mock_client.sadd = AsyncMock(side_effect=_sadd)
    mock_client.smembers = AsyncMock(side_effect=_smembers)
    mock_client.expire = AsyncMock(side_effect=_expire)

    return IdempotencyCache(client=mock_client), mock_client, store

  @pytest.mark.asyncio
  async def test_reserve_claims_once_and_get_reports_in_progress(self) -> None:
    cache, client, store = self._make_cache()
    assert await cache.reserve("u", "g", "op", "k", "fp") is True
    # SET NX with the short reservation TTL, not the 24h envelope TTL.
    _, kwargs = client.set.call_args
    assert kwargs["nx"] is True
    assert kwargs["ex"] == middleware_module.IDEMPOTENCY_RESERVATION_TTL_SECONDS
    # A second claim loses; a read with the same body says "in progress",
    # a read with a different body is the usual conflict.
    assert await cache.reserve("u", "g", "op", "k", "fp") is False
    with pytest.raises(IdempotencyInProgressError):
      await cache.get("u", "g", "op", "k", "fp")
    with pytest.raises(IdempotencyKeyConflictError) as exc_info:
      await cache.get("u", "g", "op", "k", "other")
    assert not isinstance(exc_info.value, IdempotencyInProgressError)

  @pytest.mark.asyncio
  async def test_release_drops_only_a_pending_marker(self) -> None:
    cache, _client, store = self._make_cache()
    await cache.reserve("u", "g", "op", "k", "fp")
    await cache.release("u", "g", "op", "k")
    assert store == {}
    # A completed envelope is never removed by release.
    env = wrap_completed("op", {"ok": True})
    await cache.put("u", "g", "op", "k", env, "fp")
    await cache.release("u", "g", "op", "k")
    assert len(store) == 1
    assert (await cache.get("u", "g", "op", "k", "fp")) is not None

  @pytest.mark.asyncio
  async def test_put_replaces_the_reservation(self) -> None:
    cache, _client, _store = self._make_cache()
    await cache.reserve("u", "g", "op", "k", "fp")
    env = wrap_completed("op", {"ok": True})
    await cache.put("u", "g", "op", "k", env, "fp")
    replay = await cache.get("u", "g", "op", "k", "fp")
    assert replay is not None
    assert replay.result == {"ok": True}

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


class TestIdempotencyCacheOperationBinding:
  """`bind_operation` / `invalidate_operation` — the link between an async
  operation and the pending envelope its route cached, which the SSE
  terminal-status hook uses to evict that envelope on failure."""

  def _make_cache(self):
    return TestIdempotencyCache()._make_cache()

  @pytest.mark.asyncio
  async def test_invalidate_evicts_every_bound_envelope_and_the_binding(
    self,
  ) -> None:
    cache, client, store = self._make_cache()
    env = wrap_pending("create-backup", operation_id="op_1")
    # Two callers, two keys, one deduplicated worker task: both envelopes hang
    # off the same operation and both must go when it fails.
    await cache.put("u1", "g", "create-backup", "k1", env, "fp")
    await cache.put("u2", "g", "create-backup", "k2", env, "fp")
    key1 = compute_idempotency_cache_key("u1", "g", "create-backup", "k1")
    key2 = compute_idempotency_cache_key("u2", "g", "create-backup", "k2")
    await cache.bind_operation("op_1", key1)
    await cache.bind_operation("op_1", key2)
    # The binding lives as long as the envelope it points at.
    assert client.expire.call_args.args[1] == IDEMPOTENCY_TTL_SECONDS

    assert await cache.invalidate_operation("op_1") == 2
    assert store == {}
    assert await cache.get("u1", "g", "create-backup", "k1", "fp") is None
    assert await cache.get("u2", "g", "create-backup", "k2", "fp") is None

  @pytest.mark.asyncio
  async def test_invalidate_unknown_operation_is_a_noop(self) -> None:
    cache, client, store = self._make_cache()
    env = wrap_pending("materialize", operation_id="op_other")
    await cache.put("u", "g", "materialize", "k", env, "fp")
    assert await cache.invalidate_operation("op_never_bound") == 0
    # An unrelated envelope is untouched and nothing was deleted.
    assert len(store) == 1
    client.delete.assert_not_called()

  @pytest.mark.asyncio
  async def test_module_level_invalidate_uses_the_shared_cache(self) -> None:
    cache, _client, store = self._make_cache()
    env = wrap_pending("create-graph", operation_id="op_2")
    await cache.put("u", "new", "create-graph", "k", env, "fp")
    await cache.bind_operation(
      "op_2", compute_idempotency_cache_key("u", "new", "create-graph", "k")
    )
    with patch.object(middleware_module, "get_idempotency_cache", return_value=cache):
      assert await middleware_module.invalidate_operation_idempotency("op_2") == 1
    assert store == {}

  def test_sync_invalidate_mirrors_the_async_path(self) -> None:
    from unittest.mock import MagicMock

    store: dict[str, Any] = {"idem:g:op:abc": "{}", "op-idem:op_3": {"idem:g:op:abc"}}
    client = MagicMock()
    client.smembers = MagicMock(side_effect=lambda k: set(store.get(k, set())))
    client.delete = MagicMock(
      side_effect=lambda *keys: sum(1 for k in keys if store.pop(k, None))
    )
    with patch.object(
      middleware_module, "_get_sync_idempotency_client", return_value=client
    ):
      assert middleware_module.invalidate_operation_idempotency_sync("op_3") == 1
      assert store == {}
      assert middleware_module.invalidate_operation_idempotency_sync("op_3") == 0


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
    # Outside a request there is no correlation or credential to attribute.
    assert "request_id" not in audit
    assert "api_key_prefix" not in audit

  def test_inside_a_request_the_credential_and_request_id_are_attributed(
    self,
  ) -> None:
    from robosystems.security.request_context import (
      RequestPrincipal,
      bind_principal,
      bind_request_id,
      reset_principal,
      reset_request_id,
    )

    rid = bind_request_id("req_audit")
    ptoken = bind_principal(RequestPrincipal("usr_abc", "api_key", "rfs_abcd"))
    try:
      with patch.object(middleware_module.logger, "info") as info_mock:
        log_operation_audit(
          operation_name="close-period",
          operation_id="op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
          user_id="usr_abc",
          graph_id="kg1",
          duration_ms=1.0,
          status="completed",
        )
    finally:
      reset_principal(ptoken)
      reset_request_id(rid)

    audit = self._get_audit_payload(info_mock)
    assert audit["request_id"] == "req_audit"
    assert audit["auth_method"] == "api_key"
    assert audit["api_key_prefix"] == "rfs_abcd"

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
    # Keys reserved by an in-flight run: key → body fingerprint.
    self.pending: dict[tuple[str, str, str, str], str] = {}
    # operation_id → envelope cache keys bound to it.
    self.bindings: dict[str, set[str]] = {}

  async def get(
    self,
    user_id: str,
    graph_id: str,
    operation_name: str,
    idempotency_key: str,
    body_fingerprint: str,
  ) -> OperationEnvelope | None:
    k = (user_id, graph_id, operation_name, idempotency_key)
    if k in self.pending:
      if self.pending[k] != body_fingerprint:
        raise IdempotencyKeyConflictError(operation_name)
      raise IdempotencyInProgressError(operation_name)
    entry = self.store.get(k)
    if entry is None:
      return None
    cached_envelope, cached_fingerprint = entry
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
    k = (user_id, graph_id, operation_name, idempotency_key)
    if k in self.pending or k in self.store:
      return False
    self.pending[k] = body_fingerprint
    return True

  async def release(
    self,
    user_id: str,
    graph_id: str,
    operation_name: str,
    idempotency_key: str,
  ) -> None:
    self.pending.pop((user_id, graph_id, operation_name, idempotency_key), None)

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
    k = (user_id, graph_id, operation_name, idempotency_key)
    self.pending.pop(k, None)
    self.store[k] = (envelope, body_fingerprint)

  async def bind_operation(self, operation_id: str, cache_key: str) -> None:
    self.bindings.setdefault(operation_id, set()).add(cache_key)


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


# ---------------------------------------------------------------------------
# check_idempotency / idempotent_dispatch — the async (pending) route guard
# ---------------------------------------------------------------------------


class TestCheckIdempotency:
  """`check_idempotency` claims the key on a miss, exactly as the sync
  dispatcher does, so two identical async requests cannot both enqueue."""

  @pytest.mark.asyncio
  async def test_miss_reserves_the_key(self) -> None:
    cache = _FakeIdempotencyCache()
    with patch.object(middleware_module.logger, "info"):
      result = await middleware_module.check_idempotency(
        cache, "usr_1", "kg1", "create-backup", "key-1", "fp"
      )
    assert result is None
    assert ("usr_1", "kg1", "create-backup", "key-1") in cache.pending

  @pytest.mark.asyncio
  async def test_concurrent_second_call_is_409_in_progress(self) -> None:
    cache = _FakeIdempotencyCache()
    with patch.object(middleware_module.logger, "info"):
      await middleware_module.check_idempotency(
        cache, "usr_1", "kg1", "create-backup", "key-1", "fp"
      )
    with (
      patch.object(middleware_module.logger, "info"),
      patch.object(middleware_module.logger, "error"),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await middleware_module.check_idempotency(
          cache, "usr_1", "kg1", "create-backup", "key-1", "fp"
        )
    assert exc_info.value.status_code == 409
    assert "still in progress" in str(exc_info.value.detail)

  @pytest.mark.asyncio
  async def test_lost_reservation_replays_a_recorded_envelope(self) -> None:
    """The other holder recorded its envelope between our read and our claim."""
    cache = _FakeIdempotencyCache()
    recorded = wrap_pending("create-backup", operation_id="op_first")

    original_reserve = cache.reserve

    async def _lose_the_race(*args: Any, **kwargs: Any) -> bool:
      await cache.put("usr_1", "kg1", "create-backup", "key-1", recorded, "fp")
      return False

    cache.reserve = _lose_the_race  # type: ignore[method-assign]
    try:
      with patch.object(middleware_module.logger, "info"):
        replay = await middleware_module.check_idempotency(
          cache, "usr_1", "kg1", "create-backup", "key-1", "fp"
        )
    finally:
      cache.reserve = original_reserve  # type: ignore[method-assign]
    assert replay is not None
    assert replay.operation_id == "op_first"
    assert replay.idempotent_replay is True

  @pytest.mark.asyncio
  async def test_lost_reservation_without_envelope_is_409(self) -> None:
    """The other holder released between our two reads (its dispatch failed)."""
    cache = _FakeIdempotencyCache()

    async def _lose_the_race(*args: Any, **kwargs: Any) -> bool:
      return False

    cache.reserve = _lose_the_race  # type: ignore[method-assign]
    with (
      patch.object(middleware_module.logger, "info"),
      patch.object(middleware_module.logger, "error"),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await middleware_module.check_idempotency(
          cache, "usr_1", "kg1", "create-backup", "key-1", "fp"
        )
    assert exc_info.value.status_code == 409

  @pytest.mark.asyncio
  async def test_no_key_neither_reads_nor_reserves(self) -> None:
    cache = _FakeIdempotencyCache()
    assert (
      await middleware_module.check_idempotency(
        cache, "usr_1", "kg1", "create-backup", None, "fp"
      )
      is None
    )
    assert cache.pending == {}


class TestIdempotentDispatch:
  """The context manager every async route wraps its enqueue in."""

  def _args(self, cache: _FakeIdempotencyCache, key: str | None = "key-1"):
    return (cache, "usr_1", "kg1", "create-backup", key, "fp")

  @pytest.mark.asyncio
  async def test_records_and_binds_a_pending_envelope(self) -> None:
    cache = _FakeIdempotencyCache()
    with patch.object(middleware_module.logger, "info"):
      async with middleware_module.idempotent_dispatch(*self._args(cache)) as idem:
        assert idem.replay is None
        envelope = wrap_pending("create-backup", operation_id="op_1")
        await idem.record(envelope)
    k = ("usr_1", "kg1", "create-backup", "key-1")
    assert k not in cache.pending
    assert cache.store[k][0].operation_id == "op_1"
    assert cache.bindings == {
      "op_1": {compute_idempotency_cache_key("usr_1", "kg1", "create-backup", "key-1")}
    }

  @pytest.mark.asyncio
  async def test_completed_envelope_is_recorded_but_not_bound(self) -> None:
    """A dry-run or inline path completes in the envelope; there is no
    operation whose failure could later evict it."""
    cache = _FakeIdempotencyCache()
    with patch.object(middleware_module.logger, "info"):
      async with middleware_module.idempotent_dispatch(*self._args(cache)) as idem:
        await idem.record(wrap_completed("create-backup", {"ok": True}))
    assert len(cache.store) == 1
    assert cache.bindings == {}

  @pytest.mark.asyncio
  async def test_dispatch_failure_releases_the_reservation(self) -> None:
    cache = _FakeIdempotencyCache()
    with patch.object(middleware_module.logger, "info"):
      with pytest.raises(RuntimeError, match="queue down"):
        async with middleware_module.idempotent_dispatch(*self._args(cache)):
          raise RuntimeError("queue down")
    assert cache.pending == {}
    assert cache.store == {}
    # ...so the retry executes rather than being told "in progress".
    with patch.object(middleware_module.logger, "info"):
      async with middleware_module.idempotent_dispatch(*self._args(cache)) as idem:
        assert idem.replay is None

  @pytest.mark.asyncio
  async def test_validation_4xx_before_dispatch_releases_too(self) -> None:
    cache = _FakeIdempotencyCache()
    with patch.object(middleware_module.logger, "info"):
      with pytest.raises(HTTPException):
        async with middleware_module.idempotent_dispatch(*self._args(cache)):
          raise HTTPException(status_code=403, detail="not admin")
    assert cache.pending == {}

  @pytest.mark.asyncio
  async def test_replay_path_does_not_touch_the_key(self) -> None:
    cache = _FakeIdempotencyCache()
    recorded = wrap_pending("create-backup", operation_id="op_first")
    await cache.put("usr_1", "kg1", "create-backup", "key-1", recorded, "fp")
    with patch.object(middleware_module.logger, "info"):
      async with middleware_module.idempotent_dispatch(*self._args(cache)) as idem:
        assert idem.replay is not None
        assert idem.replay.operation_id == "op_first"
        assert idem.replay.idempotent_replay is True
    # Still cached, still not reserved by anyone.
    assert len(cache.store) == 1
    assert cache.pending == {}

  @pytest.mark.asyncio
  async def test_no_key_is_a_pass_through(self) -> None:
    cache = _FakeIdempotencyCache()
    async with middleware_module.idempotent_dispatch(
      *self._args(cache, key=None)
    ) as idem:
      assert idem.replay is None
      await idem.record(wrap_pending("create-backup", operation_id="op_1"))
    assert cache.store == {}
    assert cache.pending == {}
    assert cache.bindings == {}


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


class TestExecuteOperationRunsOffTheLoop:
  """The API runs one uvicorn worker. A sync runner doing SQL or HTTP used
  to execute inline on the event loop, freezing every other request —
  including the ALB health check — for its whole duration."""

  @pytest.mark.asyncio
  async def test_sync_runner_does_not_block_the_event_loop(self) -> None:
    import asyncio
    import time

    ctx = _make_ctx(operation_name="slow-op")
    ticks: list[float] = []

    async def _ticker() -> None:
      for _ in range(3):
        await asyncio.sleep(0.02)
        ticks.append(time.monotonic())

    def _blocking_runner():
      time.sleep(0.25)
      return {"slept": True}

    with patch.object(middleware_module.logger, "info"):
      ticker = asyncio.ensure_future(_ticker())
      envelope = await execute_operation(ctx, _blocking_runner)
      runner_returned_at = time.monotonic()
      await ticker

    assert envelope.result == {"slept": True}
    # All three ticks landed while the runner was still sleeping — the loop
    # kept turning. On the old inline path the ticker could not run until
    # the runner returned, so every tick came after it.
    assert len(ticks) == 3
    assert all(t < runner_returned_at for t in ticks)

  @pytest.mark.asyncio
  async def test_sync_hook_runs_off_loop_and_async_hook_is_awaited(self) -> None:
    import threading

    ctx = _make_ctx(operation_name="hooked-op")
    seen: dict[str, Any] = {}

    def _sync_hook(envelope: OperationEnvelope) -> None:
      seen["sync_thread"] = threading.current_thread()
      seen["sync_env"] = envelope

    async def _async_hook(envelope: OperationEnvelope) -> None:
      seen["async_env"] = envelope

    with patch.object(middleware_module.logger, "info"):
      await execute_operation(ctx, lambda: {"a": 1}, on_fresh_success=_sync_hook)
      await execute_operation(ctx, lambda: {"a": 2}, on_fresh_success=_async_hook)

    assert seen["sync_thread"] is not threading.main_thread()
    assert seen["sync_env"].result == {"a": 1}
    assert seen["async_env"].result == {"a": 2}

  @pytest.mark.asyncio
  async def test_runner_sees_the_request_contextvars(self) -> None:
    """The platform request-scoped session resolves through a ContextVar;
    the worker thread must see the same context or the runner would get a
    different Session than the dependencies that ran on the loop."""
    import contextvars

    marker: contextvars.ContextVar[str | None] = contextvars.ContextVar(
      "op_marker", default=None
    )
    ctx = _make_ctx(operation_name="ctx-op")
    token = marker.set("from-request")
    try:
      with patch.object(middleware_module.logger, "info"):
        envelope = await execute_operation(ctx, lambda: {"marker": marker.get()})
    finally:
      marker.reset(token)
    assert envelope.result == {"marker": "from-request"}

  @pytest.mark.asyncio
  async def test_sync_runner_exception_still_propagates(self) -> None:
    ctx = _make_ctx(operation_name="boom-op")

    def _runner():
      raise HTTPException(status_code=409, detail="locked")

    with patch.object(middleware_module.logger, "info"):
      with pytest.raises(HTTPException) as exc_info:
        await execute_operation(ctx, _runner)
    assert exc_info.value.status_code == 409


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


class TestExecuteOperationReservation:
  """The cache had no in-flight state: two requests with the same key that
  arrived inside the first one's run time both missed and both executed."""

  @pytest.mark.asyncio
  async def test_same_key_during_the_run_is_409_not_a_second_execution(
    self,
  ) -> None:
    import asyncio

    cache = _FakeIdempotencyCache()
    ctx = _make_ctx(idempotency_key="k1", body_fingerprint="fp1")
    calls = 0
    release_runner = asyncio.Event()

    def _slow_runner():
      nonlocal calls
      calls += 1
      # Block the worker thread until the test lets the run finish.
      asyncio.run_coroutine_threadsafe(release_runner.wait(), loop).result()
      return {"n": calls}

    loop = asyncio.get_running_loop()
    with patch.object(middleware_module.logger, "info"):
      first = asyncio.ensure_future(
        execute_operation(ctx, _slow_runner, idempotency_cache=cache)
      )
      # Let the first request reach the runner and hold the reservation.
      while calls == 0:
        await asyncio.sleep(0.005)
      with pytest.raises(IdempotencyInProgressError):
        await execute_operation(ctx, _slow_runner, idempotency_cache=cache)
      release_runner.set()
      envelope = await first

    assert calls == 1
    assert envelope.result == {"n": 1}
    # Once the first completes, the same key replays its envelope.
    with patch.object(middleware_module.logger, "info"):
      replay = await execute_operation(ctx, _slow_runner, idempotency_cache=cache)
    assert replay.idempotent_replay is True
    assert replay.result == {"n": 1}
    assert calls == 1

  @pytest.mark.asyncio
  async def test_failed_run_releases_the_key_so_a_retry_executes(self) -> None:
    cache = _FakeIdempotencyCache()
    ctx = _make_ctx(idempotency_key="k2", body_fingerprint="fp2")
    attempts = 0

    def _flaky_runner():
      nonlocal attempts
      attempts += 1
      if attempts == 1:
        raise HTTPException(status_code=502, detail="upstream")
      return {"attempt": attempts}

    with (
      patch.object(middleware_module.logger, "info"),
      patch.object(middleware_module.logger, "error"),
    ):
      with pytest.raises(HTTPException):
        await execute_operation(ctx, _flaky_runner, idempotency_cache=cache)
      assert cache.pending == {}
      envelope = await execute_operation(ctx, _flaky_runner, idempotency_cache=cache)
    assert envelope.result == {"attempt": 2}

  @pytest.mark.asyncio
  async def test_failing_hook_releases_the_key(self) -> None:
    cache = _FakeIdempotencyCache()
    ctx = _make_ctx(idempotency_key="k3", body_fingerprint="fp3")

    def _hook(_env):
      raise RuntimeError("hook exploded")

    with patch.object(middleware_module.logger, "info"):
      with pytest.raises(RuntimeError):
        await execute_operation(
          ctx, lambda: {"ok": True}, idempotency_cache=cache, on_fresh_success=_hook
        )
    assert cache.pending == {}
    assert cache.store == {}

  @pytest.mark.asyncio
  async def test_cancelled_request_still_records_and_releases(self) -> None:
    """The runner's write commits in its worker thread whether or not the
    client is still there. Before shielding, a disconnect mid-run skipped
    both `except` arms: the envelope was never cached and the reservation
    stayed pending for its whole TTL, so the client's retry got 409 for a
    write that had already landed."""
    import asyncio
    import threading

    cache = _FakeIdempotencyCache()
    ctx = _make_ctx(idempotency_key="k4", body_fingerprint="fp4")
    started = threading.Event()
    release = threading.Event()
    calls = 0

    def _slow_runner():
      nonlocal calls
      calls += 1
      started.set()
      release.wait(timeout=5)
      return {"n": calls}

    with patch.object(middleware_module.logger, "info"):
      request = asyncio.ensure_future(
        execute_operation(ctx, _slow_runner, idempotency_cache=cache)
      )
      while not started.is_set():
        await asyncio.sleep(0.005)
      request.cancel()
      with pytest.raises(asyncio.CancelledError):
        await request
      # The client is gone; the work finishes anyway and is recorded.
      release.set()
      for _ in range(200):
        if cache.store:
          break
        await asyncio.sleep(0.01)
      assert cache.pending == {}
      assert len(cache.store) == 1
      # The retry replays instead of re-executing or being refused.
      replay = await execute_operation(ctx, _slow_runner, idempotency_cache=cache)
    assert replay.idempotent_replay is True
    assert calls == 1


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
