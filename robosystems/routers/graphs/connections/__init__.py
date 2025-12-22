"""
Connection management routers.

This module contains routers for connection operations including options,
CRUD management, sync, link tokens, and OAuth authentication.
"""

from fastapi import APIRouter
from fastapi.routing import APIRoute

from .link_token import router as link_token_router
from .management import router as management_router
from .oauth import router as oauth_router
from .options import router as options_router
from .sync import router as sync_router


def sort_routes_for_docs_and_matching(routes: list[APIRoute]) -> list[APIRoute]:
  """
  Sort routes by priority to ensure correct FastAPI matching and logical documentation order.

  Two-tier priority system:
  - Tier 1 (10-19): ALL literal paths, grouped by operation type
    * Management: 10, Sync: 11, Options: 12, OAuth: 13, Link: 14
  - Tier 2 (20-29): ALL param paths, grouped by operation type
    * Management: 20, Sync: 21, Options: 22, OAuth: 23, Link: 24

  This ensures:
  1. All literal paths come before param paths (correct FastAPI route matching)
  2. Within each tier, operations are grouped logically (clean documentation)

  Returns sorted list of routes.
  """
  return sorted(routes, key=lambda r: (getattr(r, "priority", 999), r.path))


# Create main connections router
router = APIRouter(tags=["Connections"])

# Include operation routers (these have prefixes so can use include_router)
router.include_router(sync_router)
router.include_router(options_router)
router.include_router(oauth_router)
router.include_router(link_token_router)

# Add management routes manually (has empty paths so needs manual addition)
for route in management_router.routes:
  if not hasattr(route, "tags") or not route.tags:
    route.tags = ["Connections"]
  router.routes.append(route)

# Set priorities on all routes by identifying them by path pattern
# Two-tier priority system:
# - Tier 1 (10-19): ALL literal paths, grouped by operation type
# - Tier 2 (20-29): ALL param paths, grouped by operation type
# This ensures correct FastAPI matching (literals before params) while grouping operations logically
for route in router.routes:
  has_params = "{" in route.path and "}" in route.path

  # Management routes - CRUD operations
  if route.path in [""]:
    route.priority = 10 if not has_params else 20
  # Options routes
  elif "/options" in route.path:
    route.priority = 12 if not has_params else 22
  # Link token routes
  elif "/link" in route.path:
    route.priority = 13 if not has_params else 23
  # OAuth routes
  elif "/oauth" in route.path:
    route.priority = 14 if not has_params else 24
  # Connection routes
  elif "/{connection_id}" in route.path:
    route.priority = 15 if not has_params else 25
  # Sync routes
  elif "/sync" in route.path:
    route.priority = 16 if not has_params else 26
  # Options routes
  else:
    route.priority = 999  # Unknown routes appear last

# Sort routes for documentation order while maintaining correct matching
# This ensures management routes appear first in docs, but literal paths
# like /options still match before /{connection_id}
router.routes = sort_routes_for_docs_and_matching(router.routes)

__all__ = ["router"]
