"""Graph database resource for Dagster.

Wraps the graph client factory so Dagster jobs and assets reach LadybugDB
through the same routing, auth, and circuit-breaker path as the API.
"""

from typing import Any

from dagster import ConfigurableResource

from robosystems.config import env
from robosystems.logger import logger


class GraphResource(ConfigurableResource):
  """LadybugDB graph resource for Dagster operations.

  Defaults to env.GRAPH_API_URL when ``graph_api_url`` is left empty.
  """

  graph_api_url: str = ""

  @property
  def api_url(self) -> str:
    """Get the Graph API URL."""
    return self.graph_api_url or env.GRAPH_API_URL

  async def get_client(self, graph_id: str, operation_type: str = "read"):
    """Get a graph client for the specified graph.

    ``operation_type`` is "read" or "write" and selects how the graph
    middleware routes the request.
    """
    from robosystems.middleware.graph import get_universal_repository

    return await get_universal_repository(graph_id, operation_type)

  async def execute_query(
    self, graph_id: str, query: str, params: dict[str, Any] | None = None
  ) -> list[dict[str, Any]]:
    """Execute a read-routed Cypher query against a graph."""
    client = await self.get_client(graph_id, "read")
    async with client:
      result = await client.execute_query(query, params or {})
      return result

  async def get_graph_info(self, graph_id: str) -> dict[str, Any]:
    """Return node and relationship counts, or an ``error`` key on failure."""
    try:
      client = await self.get_client(graph_id, "read")
      async with client:
        node_count = await client.execute_single(
          "MATCH (n) RETURN count(n) as count", {}
        )
        rel_count = await client.execute_single(
          "MATCH ()-[r]->() RETURN count(r) as count", {}
        )

        return {
          "graph_id": graph_id,
          "node_count": node_count.get("count", 0) if node_count else 0,
          "relationship_count": rel_count.get("count", 0) if rel_count else 0,
        }
    except Exception as e:
      logger.error(f"Failed to get graph info for {graph_id}: {e}")
      return {"graph_id": graph_id, "error": str(e)}

  async def materialize_table(
    self,
    graph_id: str,
    table_name: str,
    file_ids: list[str],
  ) -> dict[str, Any]:
    """Materialize the named staging table into the graph (write-routed)."""
    client = await self.get_client(graph_id, "write")
    async with client:
      result = await client.materialize_table(
        graph_id=graph_id,
        table_name=table_name,
        file_ids=file_ids,
      )
      return result
