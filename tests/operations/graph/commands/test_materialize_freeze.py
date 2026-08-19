"""Freeze test — the `materialize` source=extensions path (RoboLedger, live).

The content-ops cutover touches the graph operations router; this pins that
`materialize_cmd` for an entity/extensions graph still enqueues the
`extensions_materialize` worker task (tenant OLTP→OLAP). If a cutover change
breaks this routing, this test fails loudly before it can reach production.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.middleware.auth.distributed_lock import LockAcquisitionResult
from robosystems.models.api.graphs.operations import MaterializeOp

GRAPH = "kgentity00000001"


def _acquired_lock(lock_id: str = "lock-id-1"):
  lock = MagicMock()
  lock.lock_id = lock_id
  lock.acquire.return_value = LockAcquisitionResult(
    acquired=True, lock_id=lock_id, holder_id=lock_id, ttl_remaining=3600
  )
  return lock


async def test_materialize_extensions_routes_to_extensions_worker():
  from robosystems.operations.graph.commands.materialize import materialize_cmd

  graph = MagicMock()
  graph.graph_type = "entity"
  graph.graph_tier = "ladybug-standard"

  user = MagicMock()
  user.id = "user_1"
  db = MagicMock()

  with (
    patch(
      "robosystems.middleware.billing.enforcement.require_graph_access",
      return_value=graph,
    ),
    patch("robosystems.middleware.robustness.CircuitBreakerManager") as cb,
    patch(
      "robosystems.config.shared_repositories.is_shared_repository_or_subgraph",
      return_value=False,
    ),
    patch(
      "robosystems.config.valkey_registry.create_redis_client",
      return_value=MagicMock(),
    ),
    patch(
      "robosystems.middleware.auth.distributed_lock.DistributedLock",
      return_value=_acquired_lock(),
    ),
    patch(
      "robosystems.middleware.graph.ingestion_limits.IngestionLimitChecker.check_materialization_limits",
      new=AsyncMock(return_value={"allowed": True}),
    ),
    patch(
      "robosystems.worker.client.enqueue_task",
      new=AsyncMock(return_value={"operation_id": "op_test"}),
    ) as enqueue,
  ):
    cb.return_value.check_circuit.return_value = None
    result = await materialize_cmd(GRAPH, MaterializeOp(source="extensions"), user, db)

  enqueue.assert_awaited_once()
  kwargs = enqueue.await_args.kwargs
  assert kwargs["task_type"] == "extensions_materialize"
  assert kwargs["graph_id"] == GRAPH
  # The worker releases the API-side lock by compare-and-delete, so it needs
  # the lock_id as well as the key.
  assert kwargs["params"]["lock_key"] == f"graph_materialize:{GRAPH}"
  assert kwargs["params"]["lock_id"] == "lock-id-1"
  assert result["status"] == "queued"
  assert result["operation_id"] == "op_test"


class TestMaterializeLockFailsClosed:
  """The API-side lock used to degrade to an unlocked run when Valkey was
  unreachable. A materialization is retryable; an unlocked double-writer
  duplicates edges silently — so the lock failing is a 503, never a bypass."""

  def _common_patches(self):
    graph = MagicMock()
    graph.graph_type = "entity"
    graph.graph_tier = "ladybug-standard"
    return (
      patch(
        "robosystems.middleware.billing.enforcement.require_graph_access",
        return_value=graph,
      ),
      patch("robosystems.middleware.robustness.CircuitBreakerManager"),
      patch(
        "robosystems.config.shared_repositories.is_shared_repository_or_subgraph",
        return_value=False,
      ),
      patch(
        "robosystems.worker.client.enqueue_task",
        new=AsyncMock(return_value={"operation_id": "op_test"}),
      ),
    )

  async def _run(self, lock_patch):
    from robosystems.operations.graph.commands.materialize import materialize_cmd

    user = MagicMock()
    user.id = "user_1"
    patches = self._common_patches()
    with patches[0], patches[1] as cb, patches[2], patches[3] as enqueue, lock_patch:
      cb.return_value.check_circuit.return_value = None
      with pytest.raises(HTTPException) as exc_info:
        await materialize_cmd(
          GRAPH, MaterializeOp(source="extensions"), user, MagicMock()
        )
    enqueue.assert_not_awaited()
    return exc_info.value

  @pytest.mark.asyncio
  async def test_redis_client_failure_is_503_with_retry_after(self):
    exc = await self._run(
      patch(
        "robosystems.config.valkey_registry.create_redis_client",
        side_effect=RuntimeError("no redis"),
      )
    )
    assert exc.status_code == 503
    assert exc.headers is not None and exc.headers.get("Retry-After") == "30"
    assert "lock service unavailable" in str(exc.detail)

  @pytest.mark.asyncio
  async def test_lock_backend_error_is_503(self):
    """DistributedLock.acquire swallows RedisError into a not-acquired result;
    that must read as 'service unavailable', not 'already in progress'."""
    lock = MagicMock()
    lock.acquire.return_value = LockAcquisitionResult(
      acquired=False,
      lock_id=None,
      holder_id=None,
      ttl_remaining=None,
      error_message="Redis error: Connection refused",
      backend_error=True,
    )
    with patch(
      "robosystems.config.valkey_registry.create_redis_client",
      return_value=MagicMock(),
    ):
      exc = await self._run(
        patch(
          "robosystems.middleware.auth.distributed_lock.DistributedLock",
          return_value=lock,
        )
      )
    assert exc.status_code == 503
    assert exc.headers is not None and exc.headers.get("Retry-After") == "30"

  @pytest.mark.asyncio
  async def test_lock_held_is_409(self):
    lock = MagicMock()
    lock.acquire.return_value = LockAcquisitionResult(
      acquired=False,
      lock_id=None,
      holder_id="someone-else",
      ttl_remaining=100,
      error_message="Lock is currently held by another process",
    )
    with patch(
      "robosystems.config.valkey_registry.create_redis_client",
      return_value=MagicMock(),
    ):
      exc = await self._run(
        patch(
          "robosystems.middleware.auth.distributed_lock.DistributedLock",
          return_value=lock,
        )
      )
    assert exc.status_code == 409
