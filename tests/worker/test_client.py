"""Tests for the worker enqueue client."""

import hashlib
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.worker.client import enqueue_task


def _params_hash(params: dict | None = None) -> str:
  """Compute the same params hash the client uses."""
  return hashlib.md5(json.dumps(params or {}, sort_keys=True).encode()).hexdigest()[:8]


@pytest.fixture
def mock_operation_response():
  return {
    "operation_id": "op_01TESTID",
    "status": "pending",
    "operation_type": "agent_mapping",
    "created_at": "2026-03-28T12:00:00Z",
    "graph_id": "kg0123456789abcdef",
    "_links": {
      "stream": "/v1/operations/op_01TESTID/stream",
      "status": "/v1/operations/op_01TESTID/status",
      "cancel": "/v1/operations/op_01TESTID",
    },
    "message": "Operation agent_mapping queued.",
  }


def _make_mock_queue(*, dedup_exists: bool = False, dedup_task_id: str = ""):
  """Create a mock async Redis client with pipeline support."""
  mock_queue = AsyncMock()
  mock_queue.get.return_value = dedup_task_id if dedup_exists else None

  # pipeline() is a sync method on redis.asyncio.Redis that returns a Pipeline.
  # Pipeline.set/rpush are sync, Pipeline.execute() is async.
  mock_pipe = MagicMock()
  mock_pipe.execute = AsyncMock(return_value=[True, 1])
  mock_queue.pipeline = MagicMock(return_value=mock_pipe)

  return mock_queue, mock_pipe


@pytest.mark.asyncio
async def test_enqueue_task_creates_operation_and_pushes(mock_operation_response):
  mock_queue, mock_pipe = _make_mock_queue()

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
      return_value="op_01TESTID",
    ),
  ):
    result = await enqueue_task(
      task_type="operator_mapping",
      graph_id="kg0123456789abcdef",
      user_id="user_01TEST",
      params={"confidence": 0.7},
    )

  # Verify SSE operation created
  mock_create.assert_called_once_with(
    operation_type="operator_mapping",
    user_id="user_01TEST",
    graph_id="kg0123456789abcdef",
    operation_id="op_01TESTID",
  )

  # Verify pipeline used atomically (dedup key + enqueue)
  mock_queue.pipeline.assert_called_once_with(transaction=True)
  mock_pipe.set.assert_called_once()
  mock_pipe.rpush.assert_called_once()
  mock_pipe.execute.assert_called_once()

  # Verify dedup key includes user_id and params hash
  dedup_args = mock_pipe.set.call_args[0]
  expected_hash = _params_hash({"confidence": 0.7})
  assert (
    dedup_args[0]
    == f"worker:dedup:operator_mapping:kg0123456789abcdef:user_01TEST:{expected_hash}"
  )
  assert dedup_args[1] == "op_01TESTID"

  # Verify task payload
  rpush_args = mock_pipe.rpush.call_args[0]
  assert rpush_args[0] == "worker:tasks"
  payload = json.loads(rpush_args[1])
  assert payload["task_id"] == "op_01TESTID"
  assert payload["task_type"] == "operator_mapping"
  assert payload["graph_id"] == "kg0123456789abcdef"
  assert payload["user_id"] == "user_01TEST"
  assert payload["attempt"] == 1
  assert payload["params"] == {"confidence": 0.7}

  # Verify queue closed
  mock_queue.aclose.assert_called_once()

  # Verify response returned
  assert result == mock_operation_response


@pytest.mark.asyncio
async def test_enqueue_task_default_params(mock_operation_response):
  mock_queue, mock_pipe = _make_mock_queue()

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
      return_value="op_01TESTID",
    ),
  ):
    await enqueue_task(
      task_type="graph_creation",
      graph_id="kg0123456789abcdef",
      user_id="user_01TEST",
    )

  rpush_args = mock_pipe.rpush.call_args[0]
  payload = json.loads(rpush_args[1])
  assert payload["params"] == {}
  assert payload["attempt"] == 1


@pytest.mark.asyncio
async def test_enqueue_task_closes_queue_on_error(mock_operation_response):
  mock_queue, mock_pipe = _make_mock_queue()
  mock_pipe.execute = AsyncMock(side_effect=ConnectionError("Valkey down"))

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
      return_value="op_01TESTID",
    ),
  ):
    with pytest.raises(ConnectionError):
      await enqueue_task(
        task_type="operator_mapping",
        graph_id="kg0123456789abcdef",
        user_id="user_01TEST",
      )

  # Queue should still be closed even on error
  mock_queue.aclose.assert_called_once()


@pytest.mark.asyncio
async def test_enqueue_task_dedup_returns_existing():
  """Duplicate enqueue within dedup window returns existing operation."""
  mock_queue, _ = _make_mock_queue(dedup_exists=True, dedup_task_id="op_01EXISTING")

  with patch(
    "robosystems.worker.client.create_async_redis_client",
    return_value=mock_queue,
  ):
    result = await enqueue_task(
      task_type="graph_materialization",
      graph_id="kg0123456789abcdef",
      user_id="user_01TEST",
    )

  # Should return dedup response, not create a new operation
  assert result["operation_id"] == "op_01EXISTING"
  assert result["deduplicated"] is True
  assert result["_links"]["stream"] == "/v1/operations/op_01EXISTING/stream"

  # Should NOT create pipeline or enqueue
  mock_queue.pipeline.assert_not_called()
  mock_queue.aclose.assert_called_once()


@pytest.mark.asyncio
async def test_enqueue_task_different_params_bypass_dedup(mock_operation_response):
  """Same task_type+graph_id+user but different params should NOT dedup."""
  mock_queue, mock_pipe = _make_mock_queue()

  # queue.get returns None because the dedup key (with params hash) won't match
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
      return_value="op_01TESTID",
    ),
  ):
    result = await enqueue_task(
      task_type="operator",
      graph_id="kg0123456789abcdef",
      user_id="user_01TEST",
      params={"operator_type": "mapping", "mapping_id": "struct_abc"},
    )

  # Should create a new operation, not dedup
  assert result["operation_id"] == "op_01TESTID"
  assert "deduplicated" not in result
  mock_queue.pipeline.assert_called_once()


@pytest.mark.asyncio
async def test_enqueue_task_none_graph_id_skips_dedup(mock_operation_response):
  """graph_id=None (new graph creation) should never dedup."""
  mock_queue, mock_pipe = _make_mock_queue()

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
      return_value="op_01TESTID",
    ),
  ):
    result = await enqueue_task(
      task_type="graph_creation",
      graph_id=None,
      user_id="user_01TEST",
      params={"tier": "ladybug-standard"},
    )

  # Should not even check dedup
  mock_queue.get.assert_not_called()

  # Should still enqueue but NOT set a dedup key
  assert result["operation_id"] == "op_01TESTID"
  mock_pipe.rpush.assert_called_once()
  mock_pipe.set.assert_not_called()
