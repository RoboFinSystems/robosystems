"""Node information endpoint: identity, capacity, and hosted databases."""

from fastapi import APIRouter, Depends

from robosystems.graph_api.core.ladybug import get_ladybug_service
from robosystems.graph_api.models.cluster import ClusterInfoResponse

router = APIRouter(tags=["Cluster Info"])


@router.get("/info", response_model=ClusterInfoResponse)
async def get_info(
  service=Depends(get_ladybug_service),
) -> ClusterInfoResponse:
  """
  Get detailed node information.

  Returns information about the node including:
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
