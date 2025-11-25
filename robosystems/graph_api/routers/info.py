"""
Information endpoints for LadybugDB nodes.

This module provides endpoints for retrieving detailed information
about the node and its capabilities.
"""

from fastapi import APIRouter, Depends

from robosystems.graph_api.models.cluster import ClusterInfoResponse
from robosystems.graph_api.core.ladybug import get_ladybug_service

router = APIRouter(tags=["Info"])


@router.get("/info", response_model=ClusterInfoResponse)
async def get_info(
  ladybug_service=Depends(get_ladybug_service),
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
  return ladybug_service.get_cluster_info()
