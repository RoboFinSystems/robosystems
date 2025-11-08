"""
Subgraph management routers.

This module contains routers for subgraph operations including creation,
listing, deletion, quota management, and detailed information.
"""

from .main import router as subgraph_router
from .delete import router as delete_router
from .quota import router as quota_router
from .info import router as info_router

from fastapi import APIRouter

# Create main subgraphs router
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

# Include other sub-routers (tags already set on parent router)
router.include_router(delete_router)
router.include_router(info_router)
router.include_router(quota_router)

__all__ = ["router"]
