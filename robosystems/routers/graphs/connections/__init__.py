"""
Connection management routers.

This module contains routers for connection operations including options,
CRUD management, sync, link tokens, and OAuth authentication.
"""

from .options import router as options_router
from .management import router as management_router
from .sync import router as sync_router
from .link_token import router as link_token_router
from .oauth import router as oauth_router

from fastapi import APIRouter

# Create main connections router
router = APIRouter(tags=["Connections"])

# Include sub-routers with specific paths first (more specific routes have priority)
router.include_router(options_router)
router.include_router(sync_router)
router.include_router(link_token_router)
router.include_router(oauth_router)

# Management router has operations with empty paths - merge last to avoid conflicts
for route in management_router.routes:
  if not hasattr(route, "tags") or not route.tags:
    route.tags = ["Connections"]
  router.routes.append(route)

__all__ = ["router"]
