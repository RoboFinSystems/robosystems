"""Freeze test — the `materialize` source=extensions path (RoboLedger, live).

The content-ops cutover touches the graph operations router; this pins that
`materialize_cmd` for an entity/extensions graph still enqueues the
`extensions_materialize` worker task (tenant OLTP→OLAP). If a cutover change
breaks this routing, this test fails loudly before it can reach production.
"""

from unittest.mock import AsyncMock, MagicMock, patch

from robosystems.models.api.graphs.operations import MaterializeOp

GRAPH = "kgentity00000001"


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
    # Force the degraded lock path (no redis in tests) — caught, lock=None, proceeds.
    patch(
      "robosystems.config.valkey_registry.create_redis_client",
      side_effect=RuntimeError("no redis in test"),
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
  assert result["status"] == "queued"
  assert result["operation_id"] == "op_test"
