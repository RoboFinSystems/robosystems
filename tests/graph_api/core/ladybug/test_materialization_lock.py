"""Tests for per-graph materialization lock."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from robosystems.graph_api.core.ladybug.materialization_lock import (
  MaterializationLock,
  _resolve_base_graph_id,
)


class TestResolveBaseGraphId:
  def test_strips_wip_suffix(self):
    assert _resolve_base_graph_id("kg123-wip") == "kg123"

  def test_strips_prev_suffix(self):
    assert _resolve_base_graph_id("kg123-prev") == "kg123"

  def test_leaves_base_id_unchanged(self):
    assert _resolve_base_graph_id("kg123") == "kg123"

  def test_strips_wip_from_subgraph(self):
    assert _resolve_base_graph_id("kg123_dev-wip") == "kg123_dev"

  def test_does_not_strip_wip_in_middle(self):
    # -wip should only be stripped from the end
    assert _resolve_base_graph_id("kg-wip-123") == "kg-wip-123"


class TestMaterializationLock:
  def test_lock_key_uses_base_graph_id(self):
    redis = MagicMock()
    lock = MaterializationLock(redis, "kg123-wip")
    assert lock.lock_key == "materialize_lock:kg123"

  def test_lock_key_for_base_id(self):
    redis = MagicMock()
    lock = MaterializationLock(redis, "kg123")
    assert lock.lock_key == "materialize_lock:kg123"

  def test_wip_and_base_share_lock_key(self):
    redis = MagicMock()
    lock_base = MaterializationLock(redis, "kg123")
    lock_wip = MaterializationLock(redis, "kg123-wip")
    assert lock_base.lock_key == lock_wip.lock_key

  @pytest.mark.asyncio
  async def test_acquire_success(self):
    redis = AsyncMock()
    redis.set = AsyncMock(return_value=True)
    lock = MaterializationLock(redis, "kg123")

    result = await lock.acquire(timeout_seconds=1)

    assert result is True
    assert lock.acquired is True

  @pytest.mark.asyncio
  async def test_acquire_timeout(self):
    redis = AsyncMock()
    redis.set = AsyncMock(return_value=False)
    lock = MaterializationLock(redis, "kg123")

    result = await lock.acquire(timeout_seconds=0.1)

    assert result is False
    assert lock.acquired is False

  @pytest.mark.asyncio
  async def test_release_success(self):
    redis = AsyncMock()
    redis.set = AsyncMock(return_value=True)
    redis.eval = AsyncMock(return_value=1)
    lock = MaterializationLock(redis, "kg123")
    await lock.acquire(timeout_seconds=1)

    result = await lock.release()

    assert result is True
    assert lock.acquired is False

  @pytest.mark.asyncio
  async def test_release_without_acquire(self):
    redis = AsyncMock()
    lock = MaterializationLock(redis, "kg123")

    result = await lock.release()

    assert result is False

  @pytest.mark.asyncio
  async def test_is_locked(self):
    redis = AsyncMock()
    redis.exists = AsyncMock(return_value=1)
    lock = MaterializationLock(redis, "kg123")

    result = await lock.is_locked()
    assert result is True

  def test_from_token(self):
    redis = MagicMock()
    lock = MaterializationLock.from_trusted_token(redis, "kg123", "my-token-123")

    assert lock.token == "my-token-123"
    assert lock.acquired is True
    assert lock.lock_key == "materialize_lock:kg123"


class TestAcquireBackendErrorSignal:
  """``acquire`` returning False must be distinguishable between "held by
  another run" and "the lock service is unavailable" — the caller fails closed
  either way, but the message it surfaces differs."""

  @pytest.mark.asyncio
  async def test_held_by_another_run_leaves_no_backend_error(self):
    redis = AsyncMock()
    redis.set = AsyncMock(return_value=False)
    lock = MaterializationLock(redis, "kg123")

    assert await lock.acquire(timeout_seconds=0.05) is False
    assert lock.last_backend_error is None

  @pytest.mark.asyncio
  async def test_backend_outage_records_last_error(self):
    redis = AsyncMock()
    redis.set = AsyncMock(side_effect=ConnectionError("Connection refused"))
    lock = MaterializationLock(redis, "kg123")

    assert await lock.acquire(timeout_seconds=0.05) is False
    assert lock.acquired is False
    assert lock.last_backend_error == "Connection refused"

  @pytest.mark.asyncio
  async def test_success_after_blip_clears_error(self):
    redis = AsyncMock()
    redis.set = AsyncMock(side_effect=[ConnectionError("blip"), True])
    lock = MaterializationLock(redis, "kg123")

    assert await lock.acquire(timeout_seconds=1) is True
    assert lock.last_backend_error is None


class TestExtend:
  @pytest.mark.asyncio
  async def test_extend_with_matching_token_refreshes_ttl(self):
    redis = AsyncMock()
    redis.set = AsyncMock(return_value=True)
    redis.eval = AsyncMock(return_value=1)
    lock = MaterializationLock(redis, "kg123", ttl_seconds=1234)
    await lock.acquire(timeout_seconds=1)

    assert await lock.extend() is True
    assert lock.acquired is True
    args = redis.eval.await_args.args
    # Compare-and-extend: script, 1 key, our key, our token, the full TTL.
    assert args[1] == 1
    assert args[2] == "materialize_lock:kg123"
    assert args[3] == lock.token
    assert args[4] == "1234"

  @pytest.mark.asyncio
  async def test_extend_with_wrong_token_reports_lock_lost(self):
    """The Lua script returns 0 when the stored value is not our token — the
    lock expired and was re-acquired by another process. The instance must
    then consider itself no longer the holder so a later release is a no-op."""
    redis = AsyncMock()
    redis.set = AsyncMock(return_value=True)
    redis.eval = AsyncMock(return_value=0)
    lock = MaterializationLock(redis, "kg123")
    await lock.acquire(timeout_seconds=1)

    assert await lock.extend() is False
    assert lock.acquired is False

    # release() after a lost lock does not touch the key.
    redis.eval.reset_mock()
    assert await lock.release() is False
    redis.eval.assert_not_awaited()

  @pytest.mark.asyncio
  async def test_extend_without_acquire_is_false(self):
    redis = AsyncMock()
    lock = MaterializationLock(redis, "kg123")

    assert await lock.extend() is False
    redis.eval.assert_not_awaited()

  @pytest.mark.asyncio
  async def test_extend_backend_error_propagates(self):
    """Unlike acquire/release, extend raises on a backend error: the caller
    decides whether the remaining TTL makes a blip tolerable."""
    redis = AsyncMock()
    redis.set = AsyncMock(return_value=True)
    redis.eval = AsyncMock(side_effect=ConnectionError("blip"))
    lock = MaterializationLock(redis, "kg123")
    await lock.acquire(timeout_seconds=1)

    with pytest.raises(ConnectionError):
      await lock.extend()
    # Still considered held: nothing proved the token was lost.
    assert lock.acquired is True

  @pytest.mark.asyncio
  async def test_extend_script_compares_token(self):
    """Pin the Lua: get == token guards the expire (a stale holder must not
    push out someone else's TTL)."""
    from robosystems.graph_api.core.ladybug.materialization_lock import (
      _EXTEND_SCRIPT,
    )

    assert 'redis.call("get", KEYS[1]) == ARGV[1]' in _EXTEND_SCRIPT
    assert 'redis.call("expire", KEYS[1], ARGV[2])' in _EXTEND_SCRIPT
