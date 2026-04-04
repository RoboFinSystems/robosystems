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
- sync_status: "fresh" | "stale" | "materializing" | "failed"
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
          except Exception:
            pass

        # Determine sync status
        if is_stale:
          sync_status = "stale"
        else:
          sync_status = "fresh"

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
- The graph remains queryable during the rebuild (blue-green swap)
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
    """Trigger materialization via the internal API endpoint."""
    import httpx

    from robosystems.config import env

    graph_id = self.client.graph_id
    force = arguments.get("force", False)

    # Call the materialize endpoint internally
    api_base = (
      env.API_BASE_URL if hasattr(env, "API_BASE_URL") else "http://localhost:8000"
    )
    url = f"{api_base}/v1/graphs/{graph_id}/materialize"

    try:
      async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
          url,
          json={"force": force, "source": "extensions"},
          headers={
            "X-Internal-Service": "mcp-tools",
            "Content-Type": "application/json",
          },
        )

        if response.status_code == 409:
          return {
            "status": "conflict",
            "message": "Another materialization is already in progress for this graph. Wait for it to complete.",
          }
        elif response.status_code == 400:
          return {
            "status": "not_stale",
            "message": "Graph is already fresh. Use force=true to rebuild anyway.",
          }
        elif response.status_code >= 400:
          return {
            "status": "error",
            "message": f"Materialization request failed: {response.text}",
          }

        data = response.json()
        return {
          "status": data.get("status", "queued"),
          "operation_id": data.get("operation_id"),
          "message": data.get(
            "message",
            "Materialization started. Monitor via SSE stream.",
          ),
        }

    except Exception as e:
      logger.error(f"Failed to trigger materialization for {graph_id}: {e}")
      return {
        "status": "error",
        "message": f"Failed to trigger materialization: {e!s}",
      }
