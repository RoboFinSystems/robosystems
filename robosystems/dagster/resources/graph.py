"""Graph database resource for Dagster.

Provides LadybugDB graph operations for Dagster jobs and assets,
wrapping the existing GraphClientFactory patterns.
"""

from typing import Any

from dagster import ConfigurableResource

from robosystems.config import env
from robosystems.logger import logger


class GraphResource(ConfigurableResource):
  """LadybugDB graph resource for Dagster operations.

  This resource provides graph database operations using the
  existing GraphClientFactory infrastructure, ensuring consistency
  with the rest of the RoboSystems codebase.
  """

  graph_api_url: str = ""

  @property
  def api_url(self) -> str:
    """Get the Graph API URL."""
    return self.graph_api_url or env.GRAPH_API_URL

  async def get_client(self, graph_id: str, operation_type: str = "read"):
    """Get a graph client for the specified graph.

    Args:
        graph_id: The graph ID to connect to
        operation_type: Either "read" or "write"

    Returns:
        GraphClient instance
    """
    from robosystems.middleware.graph import get_universal_repository

    return await get_universal_repository(graph_id, operation_type)

  async def execute_query(
    self, graph_id: str, query: str, params: dict[str, Any] | None = None
  ) -> list[dict[str, Any]]:
    """Execute a Cypher query against a graph.

    Args:
        graph_id: Target graph ID
        query: Cypher query string
        params: Query parameters

    Returns:
        List of result dictionaries
    """
    client = await self.get_client(graph_id, "read")
    async with client:
      result = await client.execute_query(query, params or {})
      return result

  async def get_graph_info(self, graph_id: str) -> dict[str, Any]:
    """Get information about a graph database.

    Args:
        graph_id: Target graph ID

    Returns:
        Dictionary with graph metadata
    """
    try:
      client = await self.get_client(graph_id, "read")
      async with client:
        # Get basic graph statistics
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
    """Materialize a staging table to the graph.

    Args:
        graph_id: Target graph ID
        table_name: Name of the staging table
        file_ids: List of file IDs to materialize

    Returns:
        Materialization result
    """
    client = await self.get_client(graph_id, "write")
    async with client:
      result = await client.materialize_table(
        graph_id=graph_id,
        table_name=table_name,
        file_ids=file_ids,
      )
      return result
