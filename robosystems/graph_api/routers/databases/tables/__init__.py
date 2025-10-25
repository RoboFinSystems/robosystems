from fastapi import APIRouter

from . import management, query, ingest

router = APIRouter(
  tags=["Tables"],
  responses={
    400: {"description": "Invalid request"},
    404: {"description": "Graph or table not found"},
    500: {"description": "Internal server error"},
  },
)

for route in management.router.routes:
  if not hasattr(route, "tags") or not route.tags:
    route.tags = ["Tables"]
  router.routes.append(route)

for route in ingest.router.routes:
  if not hasattr(route, "tags") or not route.tags:
    route.tags = ["Tables"]
  router.routes.append(route)

for route in query.router.routes:
  if not hasattr(route, "tags") or not route.tags:
    route.tags = ["Tables"]
  router.routes.append(route)

__all__ = ["router"]
