"""
Simplified Kuzu-only Graph Router

This router provides graph database access exclusively through Kuzu,
using the enhanced client factory for all routing decisions.

Key features:
- Delegates all routing to the enhanced KuzuClientFactory
- Supports direct file access for development (KUZU_ACCESS_PATTERN=direct_file)
- Adds streaming support to all Kuzu clients
- Maintains backward compatibility with existing code
"""

from typing import Dict, Any, Union
import asyncio

from robosystems.config import env
from .engine import Repository
from .types import InstanceTier
from .clusters import ClusterConfig, load_cluster_configs
from robosystems.logger import logger


class GraphRouter:
  """
  Simplified router for graph database access.

  This router now delegates all routing decisions to the enhanced
  KuzuClientFactory which handles:
  - Entity graphs: Private databases for individual companies
  - Shared repositories: SEC, industry, economic data
  - Dev environment: Single local Kuzu instance
  - Production: Distributed instances with tier-based routing
  """

  def __init__(self):
    """Initialize the graph router."""
    logger.info("Initialized graph router (using enhanced client factory)")

  async def get_repository(
    self,
    graph_id: str,
    operation_type: str = "write",
    tier: InstanceTier = InstanceTier.STANDARD,
  ) -> Union[Repository, Any]:
    """
    Get a repository for the specified graph.

    Args:
        graph_id: Database identifier (entity ID or "sec")
        operation_type: "read" or "write"
        tier: Instance tier for routing

    Returns:
        Configured Repository instance
    """
    # Check KUZU_ACCESS_PATTERN to force direct file access if requested
    access_pattern = env.KUZU_ACCESS_PATTERN

    if access_pattern == "direct_file":
      # Force direct file access regardless of cluster configuration
      from .engine import Repository

      db_path = env.KUZU_DATABASE_PATH
      database_path = f"{db_path}/{graph_id}"
      logger.debug(f"Creating direct file Repository for {graph_id}: {database_path}")
      return Repository(database_path)
    else:
      # Use the new enhanced client factory for all routing
      from robosystems.kuzu_api.client.factory import get_kuzu_client
      from .streaming_wrapper import add_streaming_support

      logger.debug(f"Using enhanced client factory for {graph_id}")

      # The new factory handles all routing logic internally - use async version
      client = await get_kuzu_client(
        graph_id=graph_id,
        operation_type=operation_type,
        environment=env.ENVIRONMENT,
        tier=tier,
      )

      # Set graph ID on client for compatibility
      client.graph_id = graph_id

      # Add streaming support
      client = add_streaming_support(client)

      return client

  async def get_health_status(self) -> Dict[str, Any]:
    """Get health status of all clusters."""
    health_status = {"status": "healthy", "clusters": {}, "errors": []}

    try:
      # Check clusters
      clusters = load_cluster_configs()
      for cluster_name, cluster_config in clusters.items():
        try:
          # Test connectivity to cluster
          test_repo = self._create_test_repository(cluster_config)

          # Handle both sync and async repositories
          if hasattr(test_repo, "execute_query"):
            if asyncio.iscoroutinefunction(test_repo.execute_query):
              test_result = await test_repo.execute_query(
                "MATCH (n) RETURN count(n) as node_count LIMIT 1"
              )
            else:
              test_result = test_repo.execute_query(
                "MATCH (n) RETURN count(n) as node_count LIMIT 1"
              )
          else:
            # Fallback for repositories without execute_query
            test_result = [{"node_count": 0}]

          health_status["clusters"][cluster_name] = {
            "status": "healthy",
            "type": "graph",
            "endpoint": cluster_config.alb_endpoint or "local",
            "test_result": test_result,
          }
        except Exception as e:
          health_status["clusters"][cluster_name] = {
            "status": "unhealthy",
            "error": str(e),
          }
          health_status["errors"].append(f"Cluster {cluster_name}: {e}")

    except Exception as e:
      health_status["clusters"]["cluster_error"] = str(e)
      health_status["errors"].append(f"Cluster loading error: {e}")

    if health_status["errors"]:
      health_status["status"] = "degraded"

    return health_status

  def _create_test_repository(self, cluster_config: ClusterConfig):
    """Create a test repository for health checking."""
    if cluster_config.alb_endpoint:
      # Use unified API key for test repository from centralized config
      from robosystems.config import env

      api_key = env.KUZU_API_KEY

      from robosystems.kuzu_api.client import KuzuClient

      client = KuzuClient(base_url=cluster_config.alb_endpoint, api_key=api_key)
      client.graph_id = "test"
      return client
    else:
      return Repository("test")


# Global router instance
_graph_router = None


def get_graph_router() -> GraphRouter:
  """Get the global graph router instance."""
  global _graph_router
  if _graph_router is None:
    _graph_router = GraphRouter()
  return _graph_router


async def get_graph_repository(
  graph_id: str,
  operation_type: str = "write",
  tier: InstanceTier = InstanceTier.STANDARD,
) -> Union[Repository, Any]:
  """
  Get a graph repository for the specified database.

  This is the main entry point for graph database access.

  Args:
      graph_id: Database identifier (entity ID or "sec")
      operation_type: "read" or "write"
      tier: Instance tier for routing

  Returns:
      Configured repository instance
  """
  router = get_graph_router()
  return await router.get_repository(graph_id, operation_type, tier)


async def get_universal_repository(
  graph_id: str,
  operation_type: str = "write",
  tier: InstanceTier = InstanceTier.STANDARD,
):
  """
  Get a universal repository wrapper for the specified database.

  This provides a unified interface that handles both sync and async repositories
  automatically, eliminating the need for conditional awaiting in application code.

  Args:
      graph_id: Database identifier (entity ID or "sec")
      operation_type: "read" or "write"
      tier: Instance tier for routing

  Returns:
      UniversalRepository instance
  """
  from .repository import UniversalRepository

  repository = await get_graph_repository(graph_id, operation_type, tier)
  return UniversalRepository(repository)
