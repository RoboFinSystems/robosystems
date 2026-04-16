"""
Subgraph management routers.

Read endpoints: list, info, quota.
Write operations (create, delete) live at
``POST /v1/graphs/{graph_id}/operations/{create-subgraph,delete-subgraph}``.
"""

from fastapi import APIRouter

from .info import router as info_router
from .main import router as subgraph_router
from .quota import router as quota_router

# Create main subgraphs router — reads only
router = APIRouter(
  tags=["Subgraphs"],
  responses={
    401: {"description": "Not authenticated"},
    403: {"description": "Insufficient permissions"},
    404: {"description": "Graph not found"},
  },
)

# Subgraph router has operations with empty paths - merge directly
for route in subgraph_router.routes:
  if not hasattr(route, "tags") or not route.tags:
    route.tags = ["Subgraphs"]
  router.routes.append(route)

# Include other sub-routers
# Note: delete_router removed — delete lives at POST .../operations/delete-subgraph
router.include_router(info_router)
router.include_router(quota_router)

__all__ = ["router"]
