"""Materialization MCP tools for graph sync awareness.

Two tools for AI materialization workflow:
1. get-graph-sync-status: Check if the graph is current with OLTP data
2. materialize-graph: Trigger a full graph rebuild and get an operation ID
"""

from datetime import UTC, datetime
from typing import Any

from robosystems.logger import logger


class GetGraphSyncStatusTool:
  """Check if the graph database is in sync with OLTP data."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-graph-sync-status",
      "description": """Check if the graph database is in sync with OLTP data.

**WHEN TO USE:**
- Before querying financial data, to verify the graph is current
- After creating schedules or closing entries, to check if rematerialization is needed
- When the user asks about data freshness

**RETURNS:**
- sync_status: "fresh" | "stale"
- stale_since: ISO timestamp (null if fresh)
- stale_duration_minutes: how long it's been stale
- stale_reason: what caused the staleness
- last_materialized_at: when the graph was last rebuilt
- hours_since_materialization: convenience field""",
      "inputSchema": {
        "type": "object",
        "properties": {},
        "required": [],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    """Check graph sync status from the platform database."""
    from robosystems.database import SessionFactory
    from robosystems.models.core import Graph

    graph_id = self.client.graph_id

    try:
      session = SessionFactory()
      try:
        graph = Graph.get_by_id(graph_id, session)
        if not graph:
          return {"error": f"Graph {graph_id} not found"}

        is_stale = graph.graph_stale or False
        stale_reason = graph.graph_stale_reason
        stale_at = graph.graph_stale_at

        metadata = graph.graph_metadata or {}
        last_materialized_at = metadata.get("last_materialized_at")
        materialization_count = metadata.get("materialization_count", 0)

        # Compute durations
        stale_duration_minutes = None
        if is_stale and stale_at:
          stale_dt = stale_at
          if stale_dt.tzinfo is None:
            stale_dt = stale_dt.replace(tzinfo=UTC)
          delta = datetime.now(UTC) - stale_dt
          stale_duration_minutes = round(delta.total_seconds() / 60, 1)

        hours_since_materialization = None
        if last_materialized_at:
          try:
            from dateutil import parser as date_parser

            last_mat_dt = date_parser.isoparse(last_materialized_at)
            delta = datetime.now(UTC) - last_mat_dt
            hours_since_materialization = round(delta.total_seconds() / 3600, 1)
          except Exception as exc:
            logger.warning(
              f"Could not parse last_materialized_at '{last_materialized_at}': {exc}"
            )

        sync_status = "stale" if is_stale else "fresh"

        return {
          "sync_status": sync_status,
          "stale_since": stale_at.isoformat() if stale_at else None,
          "stale_duration_minutes": stale_duration_minutes,
          "stale_reason": stale_reason,
          "last_materialized_at": last_materialized_at,
          "hours_since_materialization": hours_since_materialization,
          "materialization_count": materialization_count,
        }
      finally:
        session.close()
    except Exception as e:
      logger.error(f"Failed to get sync status for {graph_id}: {e}")
      return {"error": f"Failed to retrieve sync status: {e!s}"}


class MaterializeGraphTool:
  """Trigger a full graph rebuild from OLTP data."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "materialize-graph",
      "description": """Trigger a full graph rebuild from OLTP data.

**WHEN TO USE:**
- When get-graph-sync-status shows "stale" and fresh data is needed
- After a batch of OLTP writes (schedules, entries) before querying results
- When the user explicitly asks to refresh the graph

**IMPORTANT:**
- Returns an operation_id for progress tracking via SSE
- Fails with 409 if another materialization is already running
- The graph remains queryable during the rebuild
- Wait for completion before querying fresh data

**PARAMETERS:**
- force: boolean (default false) — materialize even if graph is not stale""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "force": {
            "type": "boolean",
            "description": "Materialize even if graph is not stale",
            "default": False,
          },
        },
        "required": [],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    """Trigger materialization by enqueuing a worker task directly."""
    from robosystems.config.constants import INGESTION_LOCK_TTL
    from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client
    from robosystems.database import SessionFactory
    from robosystems.middleware.auth.distributed_lock import DistributedLock
    from robosystems.models.core import Graph
    from robosystems.worker.client import enqueue_task

    graph_id = self.client.graph_id
    force = arguments.get("force", False)

    # Check staleness (skip if force=True)
    if not force:
      try:
        session = SessionFactory()
        try:
          graph = Graph.get_by_id(graph_id, session)
          if graph and not graph.graph_stale:
            return {
              "status": "not_stale",
              "message": "Graph is already fresh. Use force=true to rebuild anyway.",
            }
        finally:
          session.close()
      except Exception as e:
        logger.warning(f"Could not check staleness for {graph_id}: {e}")

    # Acquire distributed lock to prevent concurrent materialization
    lock = None
    try:
      redis_client = create_redis_client(ValkeyDatabase.LOCKS)
      lock = DistributedLock(
        redis_client, f"graph_materialize:{graph_id}", ttl_seconds=INGESTION_LOCK_TTL
      )
      lock_result = lock.acquire(blocking=False)
      if not lock_result.acquired:
        return {
          "status": "conflict",
          "message": "Another materialization is already in progress for this graph. "
          "Wait for it to complete.",
        }
    except Exception as e:
      logger.warning(f"Could not acquire distributed lock for {graph_id}: {e}")
      # Proceed without lock if Valkey is unavailable (degraded mode)
      lock = None

    # Enqueue the materialization task
    try:
      run_config = {
        "ops": {
          "materialize_extensions_to_graph": {
            "config": {
              "graph_id": graph_id,
              "rebuild": False,
            }
          }
        }
      }

      lock_key = f"graph_materialize:{graph_id}" if lock else None
      response = await enqueue_task(
        task_type="dagster_job_monitor",
        graph_id=graph_id,
        user_id=f"mcp:{graph_id}",
        params={
          "job_name": "extensions_materialize_job",
          "run_config": run_config,
          "lock_key": lock_key,
        },
      )

      return {
        "status": "queued",
        "operation_id": response["operation_id"],
        "message": f"Materialization queued. Monitor progress via SSE stream at "
        f"/v1/operations/{response['operation_id']}/stream",
      }

    except Exception as e:
      # Release lock since the background task was never dispatched
      if lock is not None:
        try:
          lock.release()
        except Exception:
          pass
      logger.error(f"Failed to enqueue materialization for {graph_id}: {e}")
      return {
        "status": "error",
        "message": f"Failed to trigger materialization: {e!s}",
      }
