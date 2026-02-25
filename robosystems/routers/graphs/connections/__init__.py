"""
Connection management routers.

This module contains routers for connection operations including options,
CRUD management, sync, and OAuth authentication.
"""

from fastapi import APIRouter
from fastapi.routing import APIRoute

from .management import router as management_router
from .oauth import router as oauth_router
from .options import router as options_router
from .sync import router as sync_router


def sort_routes_for_docs_and_matching(routes: list[APIRoute]) -> list[APIRoute]:
  """Sort routes by priority for correct FastAPI matching and documentation order."""
  return sorted(routes, key=lambda r: (getattr(r, "priority", 999), r.path))


# Create main connections router
router = APIRouter(tags=["Connections"])

# Include operation routers
router.include_router(sync_router)
router.include_router(options_router)
router.include_router(oauth_router)

# Add management routes manually (has empty paths so needs manual addition)
for route in management_router.routes:
  if not hasattr(route, "tags") or not route.tags:
    route.tags = ["Connections"]
  router.routes.append(route)

# Set priorities on all routes
for route in router.routes:
  has_params = "{" in route.path and "}" in route.path

  if route.path in [""]:
    route.priority = 10 if not has_params else 20
  elif "/options" in route.path:
    route.priority = 12 if not has_params else 22
  elif "/oauth" in route.path:
    route.priority = 14 if not has_params else 24
  elif "/{connection_id}" in route.path:
    route.priority = 15 if not has_params else 25
  elif "/sync" in route.path:
    route.priority = 16 if not has_params else 26
  else:
    route.priority = 999

router.routes = sort_routes_for_docs_and_matching(router.routes)

__all__ = ["router"]
