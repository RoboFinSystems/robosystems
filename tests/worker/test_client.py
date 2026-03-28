"""Tests for the worker enqueue client."""

import json
from unittest.mock import AsyncMock, patch

import pytest

from robosystems.worker.client import enqueue_task


@pytest.fixture
def mock_operation_response():
  return {
    "operation_id": "task_01TESTID",
    "status": "pending",
    "operation_type": "agent_mapping",
    "created_at": "2026-03-28T12:00:00Z",
    "graph_id": "kg0123456789abcdef",
    "_links": {
      "stream": "/v1/operations/task_01TESTID/stream",
      "status": "/v1/operations/task_01TESTID/status",
      "cancel": "/v1/operations/task_01TESTID",
    },
    "message": "Operation agent_mapping queued.",
  }


@pytest.mark.asyncio
async def test_enqueue_task_creates_operation_and_pushes(mock_operation_response):
  mock_queue = AsyncMock()

  with (
    patch(
      "robosystems.worker.client.create_operation_response",
      new_callable=AsyncMock,
      return_value=mock_operation_response,
    ) as mock_create,
    patch(
      "robosystems.worker.client.create_async_redis_client",
      return_value=mock_queue,
    ),
    patch(
      "robosystems.worker.client.generate_prefixed_ulid",
      return_value="task_01TESTID",
    ),
  ):
    result = await enqueue_task(
      task_type="agent_mapping",
      graph_id="kg0123456789abcdef",
      user_id="user_01TEST",
      params={"confidence": 0.7},
    )

  # Verify SSE operation created
  mock_create.assert_called_once_with(
    operation_type="agent_mapping",
    user_id="user_01TEST",
    graph_id="kg0123456789abcdef",
    operation_id="task_01TESTID",
  )

  # Verify task pushed to queue
  mock_queue.lpush.assert_called_once()
  pushed_key = mock_queue.lpush.call_args[0][0]
  pushed_json = mock_queue.lpush.call_args[0][1]
  assert pushed_key == "worker:tasks"

  payload = json.loads(pushed_json)
  assert payload["task_id"] == "task_01TESTID"
  assert payload["task_type"] == "agent_mapping"
  assert payload["graph_id"] == "kg0123456789abcdef"
  assert payload["user_id"] == "user_01TEST"
  assert payload["params"] == {"confidence": 0.7}

  # Verify queue closed
  mock_queue.aclose.assert_called_once()

  # Verify response returned
  assert result == mock_operation_response


@pytest.mark.asyncio
async def test_enqueue_task_default_params(mock_operation_response):
  mock_queue = AsyncMock()

  with (
    patch(
      "robosystems.worker.client.create_operation_response",
      new_callable=AsyncMock,
      return_value=mock_operation_response,
    ),
    patch(
      "robosystems.worker.client.create_async_redis_client",
      return_value=mock_queue,
    ),
    patch(
      "robosystems.worker.client.generate_prefixed_ulid",
      return_value="task_01TESTID",
    ),
  ):
    await enqueue_task(
      task_type="graph_creation",
      graph_id="kg0123456789abcdef",
      user_id="user_01TEST",
    )

  pushed_json = mock_queue.lpush.call_args[0][1]
  payload = json.loads(pushed_json)
  assert payload["params"] == {}


@pytest.mark.asyncio
async def test_enqueue_task_closes_queue_on_error(mock_operation_response):
  mock_queue = AsyncMock()
  mock_queue.lpush.side_effect = ConnectionError("Valkey down")

  with (
    patch(
      "robosystems.worker.client.create_operation_response",
      new_callable=AsyncMock,
      return_value=mock_operation_response,
    ),
    patch(
      "robosystems.worker.client.create_async_redis_client",
      return_value=mock_queue,
    ),
    patch(
      "robosystems.worker.client.generate_prefixed_ulid",
      return_value="task_01TESTID",
    ),
  ):
    with pytest.raises(ConnectionError):
      await enqueue_task(
        task_type="agent_mapping",
        graph_id="kg0123456789abcdef",
        user_id="user_01TEST",
      )

  # Queue should still be closed even on error
  mock_queue.aclose.assert_called_once()
