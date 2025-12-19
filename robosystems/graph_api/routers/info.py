"""
Information endpoints for graph database nodes.

This module provides endpoints for retrieving detailed information
about the node and its capabilities.
"""

from fastapi import APIRouter, Depends

from robosystems.config import env
from robosystems.graph_api.core.ladybug import get_ladybug_service
from robosystems.graph_api.models.cluster import ClusterInfoResponse

router = APIRouter(tags=["Info"])


def _get_service_for_info():
  """Get the appropriate service based on backend configuration."""
  backend_type = env.GRAPH_BACKEND_TYPE
  if backend_type in ["neo4j_community", "neo4j_enterprise"]:
    from robosystems.graph_api.core.neo4j import Neo4jService

    return Neo4jService()
  else:
    return get_ladybug_service()


@router.get("/info", response_model=ClusterInfoResponse)
async def get_info(
  service=Depends(_get_service_for_info),
) -> ClusterInfoResponse:
  """
  Get detailed node information.

  Returns comprehensive information about the node including:
  - Node identification and type
  - Software version
  - Database capacity and current usage
  - List of databases on this node
  - Uptime and operational status
  """
  import inspect

  result = service.get_cluster_info()
  if inspect.iscoroutine(result):
    return await result
  return result
