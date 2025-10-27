"""Tests for GenericTaskManager Redis-backed task lifecycle."""

import json
from unittest.mock import AsyncMock

import pytest

from robosystems.graph_api.core.task_manager import GenericTaskManager, TaskStatus


@pytest.fixture
def redis_client():
  client = AsyncMock()
  client.setex = AsyncMock()
  client.get = AsyncMock()
  return client


@pytest.fixture
def task_manager(monkeypatch, redis_client):
  monkeypatch.setattr(
    "robosystems.config.valkey_registry.create_async_redis_client",
    lambda *args, **kwargs: redis_client,
  )
  manager = GenericTaskManager(task_prefix="test")
  # Ensure we start with a clean cached client per test
  manager._redis_client = None
  return manager


@pytest.mark.asyncio
async def test_create_task_persists_initial_record(task_manager, redis_client):
  task_id = await task_manager.create_task(
    task_type="backup", metadata={"graph_id": "graph-123"}, estimated_size=42
  )

  assert task_id.startswith("test_backup_")
  redis_client.setex.assert_awaited_once()
  key, ttl, payload = redis_client.setex.await_args.args
  assert key == f"kuzu:task:{task_id}"
  assert ttl == 86400

  task_data = json.loads(payload)
  assert task_data["task_id"] == task_id
  assert task_data["status"] == TaskStatus.PENDING.value
  assert task_data["metadata"] == {"graph_id": "graph-123"}
  assert task_data["estimated_size"] == 42


@pytest.mark.asyncio
async def test_update_task_mutates_and_restores_ttl(task_manager, redis_client):
  # Seed initial task payload
  task_id = await task_manager.create_task("restore", metadata={"graph": "abc"})
  initial_payload = redis_client.setex.await_args.args[2]

  redis_client.setex.reset_mock()
  redis_client.get.reset_mock()
  redis_client.get.return_value = initial_payload

  await task_manager.update_task(
    task_id, status=TaskStatus.RUNNING.value, progress_percent=55
  )

  redis_client.get.assert_awaited_once_with(f"kuzu:task:{task_id}")
  redis_client.setex.assert_awaited_once()
  _, ttl, updated_payload = redis_client.setex.await_args.args
  assert ttl == 86400

  updated_data = json.loads(updated_payload)
  assert updated_data["status"] == TaskStatus.RUNNING.value
  assert updated_data["progress_percent"] == 55
  assert updated_data["last_heartbeat"] >= json.loads(initial_payload)["last_heartbeat"]


@pytest.mark.asyncio
async def test_update_task_raises_when_missing(task_manager, redis_client):
  redis_client.get.return_value = None

  with pytest.raises(ValueError):
    await task_manager.update_task("missing-task", status="running")


@pytest.mark.asyncio
async def test_get_task_returns_parsed_payload(task_manager, redis_client):
  payload = {"task_id": "abc", "status": "pending"}
  redis_client.get.return_value = json.dumps(payload)

  result = await task_manager.get_task("abc")

  assert result == payload
  redis_client.get.assert_awaited_once_with("kuzu:task:abc")


@pytest.mark.asyncio
async def test_fail_task_sets_error_status(task_manager, redis_client):
  task_id = await task_manager.create_task("backup")
  base_payload = redis_client.setex.await_args.args[2]
  redis_client.get.reset_mock()
  redis_client.setex.reset_mock()
  redis_client.get.return_value = base_payload

  await task_manager.fail_task(task_id, error="boom")

  updated_data = json.loads(redis_client.setex.await_args.args[2])
  assert updated_data["status"] == TaskStatus.FAILED.value
  assert updated_data["error"] == "boom"
  assert updated_data["completed_at"] is not None
