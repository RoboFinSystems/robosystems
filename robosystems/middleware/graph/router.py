"""
Multi-Backend Graph Router

This router provides graph database access with pluggable backend support,
using the enhanced client factory for all routing decisions.

Supported backends:
- LadybugDB: Embedded graph database (ladybug-standard, ladybug-large, ladybug-xlarge tiers)
- Neo4j Community: Client-server architecture (neo4j-community-large tier)
- Neo4j Enterprise: Full enterprise features (neo4j-enterprise-xlarge tier)

Key features:
- Backend-agnostic routing based on tier and allocation
- Delegates routing to the enhanced client factory
- Supports direct file access for development (LadybugDB only)
- Maintains backward compatibility with existing code
"""

from typing import Dict, Any, Union

from robosystems.config import env
from robosystems.graph_api.core.ladybug import Repository
from .types import GraphTier
from robosystems.logger import logger
from robosystems.graph_api.client import GraphClient
from robosystems.graph_api.client.factory import GraphClientFactory


class GraphRouter:
  """
  Multi-backend router for graph database access.

  This router now delegates all routing decisions to the enhanced
  client factory which handles:
  - Entity graphs: Private databases for individual companies
  - Shared repositories: SEC, industry, economic data
  - Dev environment: Single local backend instance
  - Production: Distributed instances with tier-based routing
  - Backend selection: Automatic based on tier (LadybugDB/Neo4j)
  """

  def __init__(self):
    """Initialize the graph router."""
    logger.info(
      f"Initialized multi-backend graph router (backend: {env.GRAPH_BACKEND_TYPE})"
    )

  async def get_repository(
    self,
    graph_id: str,
    operation_type: str = "write",
    tier: GraphTier = GraphTier.LADYBUG_STANDARD,
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
    # Check LBUG_ACCESS_PATTERN to force direct file access if requested
    access_pattern = env.LBUG_ACCESS_PATTERN

    if access_pattern == "direct_file":
      # Force direct file access regardless of cluster configuration
      db_path = env.LBUG_DATABASE_PATH
      database_path = f"{db_path}/{graph_id}"
      logger.debug(f"Creating direct file Repository for {graph_id}: {database_path}")
      return Repository(database_path)
    else:
      # Use the new enhanced client factory for all routing
      from .streaming_wrapper import add_streaming_support

      logger.debug(f"Using enhanced client factory for {graph_id}")

      # The new factory handles all routing logic internally - use async version
      client = await GraphClientFactory.create_client(
        graph_id=graph_id,
        operation_type=operation_type,
        tier=tier,
      )

      # Set graph ID on client for compatibility
      client.graph_id = graph_id

      # Add streaming support
      client = add_streaming_support(client)

      return client

  async def get_health_status(self) -> Dict[str, Any]:
    """Get health status of the graph backend."""
    health_status = {"status": "healthy", "backend": {}, "errors": []}

    try:
      # Check the configured graph API endpoint
      graph_api_url = env.GRAPH_API_URL
      api_key = env.GRAPH_API_KEY

      client = GraphClient(base_url=graph_api_url, api_key=api_key)

      # Try to get health from the graph API
      try:
        health_response = await client.health()
        health_status["backend"] = {
          "status": "healthy",
          "endpoint": graph_api_url,
          "response": health_response,
        }
      except Exception as e:
        health_status["backend"] = {
          "status": "unhealthy",
          "endpoint": graph_api_url,
          "error": str(e),
        }
        health_status["errors"].append(f"Graph API health check failed: {e}")

    except Exception as e:
      health_status["backend"] = {"status": "error", "error": str(e)}
      health_status["errors"].append(f"Health check error: {e}")

    if health_status["errors"]:
      health_status["status"] = "degraded"

    return health_status


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
  tier: GraphTier = GraphTier.LADYBUG_STANDARD,
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
  tier: GraphTier = GraphTier.LADYBUG_STANDARD,
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
