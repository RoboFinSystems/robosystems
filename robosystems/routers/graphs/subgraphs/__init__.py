"""Subgraph read routers (list, info); create/delete are graph operations.

Subgraph quota is reported by ``GET /v1/graphs/{graph_id}/limits``: a literal
path here would be shadowed by ``GET /{subgraph_name}``.
"""

from fastapi import APIRouter

from .info import router as info_router
from .main import router as subgraph_router

router = APIRouter(
  tags=["Subgraphs"],
  responses={
    401: {"description": "Not authenticated"},
    403: {"description": "Insufficient permissions"},
    404: {"description": "Graph not found"},
  },
)

# Routes with empty paths can't go through include_router; merge directly.
for route in subgraph_router.routes:
  if not hasattr(route, "tags") or not route.tags:
    route.tags = ["Subgraphs"]
  router.routes.append(route)

router.include_router(info_router)

__all__ = ["router"]
