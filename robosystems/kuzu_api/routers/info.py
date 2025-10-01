"""
Cluster information endpoints for Kuzu nodes.

This module provides endpoints for retrieving detailed information
about the cluster node and its capabilities.
"""

from fastapi import APIRouter, Depends

from robosystems.kuzu_api.models.cluster import ClusterInfoResponse
from robosystems.kuzu_api.core.cluster_manager import get_cluster_service

router = APIRouter(tags=["Cluster Info"])


@router.get("/info", response_model=ClusterInfoResponse)
async def get_cluster_info(
  cluster_service=Depends(get_cluster_service),
) -> ClusterInfoResponse:
  """
  Get detailed cluster information.

  Returns comprehensive information about the cluster node including:
  - Node identification and type
  - Software version
  - Database capacity and current usage
  - List of databases on this node
  - Uptime and operational status
  """
  return cluster_service.get_cluster_info()
