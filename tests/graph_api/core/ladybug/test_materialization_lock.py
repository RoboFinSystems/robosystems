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
