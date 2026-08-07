"""Entry point for obtaining a graph repository.

Routing decisions belong to `GraphClientFactory`; this module picks between
that path and direct file access, which `LBUG_ACCESS_PATTERN=direct_file`
forces for local development.
"""

from typing import Any

from robosystems.config import env
from robosystems.graph_api.client import GraphClient
from robosystems.graph_api.core.ladybug import Repository
from robosystems.logger import logger

from .types import GraphTier


class GraphRouter:
  """Hands out repositories, delegating routing to `GraphClientFactory`.

  The factory covers entity graphs, shared repositories (SEC and friends),
  the single local instance in dev, and tier-based routing across
  distributed instances in production.
  """

  def __init__(self):
    logger.info(f"Initialized graph router (backend: {env.GRAPH_BACKEND_TYPE})")

  async def get_repository(
    self,
    graph_id: str,
    operation_type: str = "write",
    tier: GraphTier = GraphTier.LADYBUG_STANDARD,
  ) -> Repository | Any:
    """Return a repository for a graph.

    `LBUG_ACCESS_PATTERN=direct_file` bypasses the factory and opens the
    database file directly, regardless of cluster configuration.
    """
    access_pattern = env.LBUG_ACCESS_PATTERN

    if access_pattern == "direct_file":
      db_path = env.LBUG_DATABASE_PATH
      database_path = f"{db_path}/{graph_id}"
      logger.debug(f"Creating direct file Repository for {graph_id}: {database_path}")
      return Repository(database_path)
    else:
      # Lazy import to avoid a circular dependency.
      from robosystems.graph_api.client.factory import GraphClientFactory

      from .streaming_wrapper import add_streaming_support

      logger.debug(f"Using enhanced client factory for {graph_id}")

      client = await GraphClientFactory.create_client(
        graph_id=graph_id,
        operation_type=operation_type,
        tier=tier,
      )

      client.graph_id = graph_id

      client = add_streaming_support(client)

      return client

  async def get_health_status(self) -> dict[str, Any]:
    """Get health status of the graph backend."""
    health_status = {"status": "healthy", "backend": {}, "errors": []}

    try:
      graph_api_url = env.GRAPH_API_URL
      api_key = env.GRAPH_API_KEY

      client = GraphClient(base_url=graph_api_url, api_key=api_key)

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
) -> Repository | Any:
  """The main entry point for graph database access."""
  router = get_graph_router()
  return await router.get_repository(graph_id, operation_type, tier)


async def get_universal_repository(
  graph_id: str,
  operation_type: str = "write",
  tier: GraphTier = GraphTier.LADYBUG_STANDARD,
):
  """Like `get_graph_repository`, wrapped so callers need not know whether
  the underlying repository is sync or async.
  """
  from .repository import UniversalRepository

  repository = await get_graph_repository(graph_id, operation_type, tier)
  return UniversalRepository(repository)
