"""Freeze test — the `materialize` source=extensions path (RoboLedger, live).

The content-ops cutover touches the graph operations router; this pins that
`materialize_cmd` for an entity/extensions graph still enqueues the
`extensions_materialize` worker task (tenant OLTP→OLAP). If a cutover change
breaks this routing, this test fails loudly before it can reach production.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.models.api.graphs.operations import MaterializeOp

GRAPH = "kgentity00000001"


def _redis(set_result=True, set_error: Exception | None = None) -> AsyncMock:
  """An async Valkey client whose ``SET NX`` wins, loses, or errors."""
  redis = AsyncMock()
  redis.set = AsyncMock(side_effect=set_error, return_value=set_result)
  return redis


async def test_materialize_extensions_routes_to_extensions_worker():
  from robosystems.operations.graph.commands.materialize import materialize_cmd

  graph = MagicMock()
  graph.graph_type = "entity"
  graph.graph_tier = "ladybug-standard"

  user = MagicMock()
  user.id = "user_1"
  db = MagicMock()
  redis = _redis()

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
      "robosystems.config.valkey_registry.create_async_redis_client",
      return_value=redis,
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
  # One lock, the one the sensor's runs take too: the worker adopts this
  # token rather than acquiring again.
  set_args = redis.set.await_args
  assert set_args.args[0] == f"materialize_lock:{GRAPH}"
  assert set_args.kwargs["nx"] is True
  assert kwargs["params"]["materialization_lock_token"] == set_args.args[1]
  assert "lock_key" not in kwargs["params"]
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
        "robosystems.config.valkey_registry.create_async_redis_client",
        side_effect=RuntimeError("no redis"),
      )
    )
    assert exc.status_code == 503
    assert exc.headers is not None and exc.headers.get("Retry-After") == "30"
    assert "lock service unavailable" in str(exc.detail)

  @pytest.mark.asyncio
  async def test_lock_backend_error_is_503(self):
    """The acquire swallows a Valkey error into not-acquired; that must read
    as 'service unavailable', not 'already in progress'."""
    exc = await self._run(
      patch(
        "robosystems.config.valkey_registry.create_async_redis_client",
        return_value=_redis(set_error=ConnectionError("Connection refused")),
      )
    )
    assert exc.status_code == 503
    assert exc.headers is not None and exc.headers.get("Retry-After") == "30"

  @pytest.mark.asyncio
  async def test_lock_held_is_409_at_once(self):
    """Held by anyone, the stale-graph sensor's run included: one try, then a
    clean 409 rather than a queued job that dies on the lock later."""
    redis = _redis(set_result=False)
    exc = await self._run(
      patch(
        "robosystems.config.valkey_registry.create_async_redis_client",
        return_value=redis,
      )
    )
    assert exc.status_code == 409
    redis.set.assert_awaited_once()
