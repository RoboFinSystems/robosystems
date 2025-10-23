from fastapi import APIRouter

from . import main, query, upload, ingest, files

router = APIRouter(
  tags=["Tables"],
  responses={
    401: {"description": "Not authenticated"},
    403: {"description": "Access denied to graph"},
    404: {"description": "Graph or table not found"},
  },
)

for route in main.router.routes:
  if not hasattr(route, "tags") or not route.tags:
    route.tags = ["Tables"]
  router.routes.append(route)

for route in upload.router.routes:
  if not hasattr(route, "tags") or not route.tags:
    route.tags = ["Tables"]
  router.routes.append(route)

for route in query.router.routes:
  if not hasattr(route, "tags") or not route.tags:
    route.tags = ["Tables"]
  router.routes.append(route)

for route in ingest.router.routes:
  if not hasattr(route, "tags") or not route.tags:
    route.tags = ["Tables"]
  router.routes.append(route)

for route in files.router.routes:
  if not hasattr(route, "tags") or not route.tags:
    route.tags = ["Tables"]
  router.routes.append(route)

__all__ = ["router"]
