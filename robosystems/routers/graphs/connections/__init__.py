"""Connection routers: provider options, CRUD, sync, and OAuth."""

from fastapi import APIRouter

from .management import router as management_router
from .oauth import router as oauth_router
from .options import router as options_router
from .sync import router as sync_router

# Create main connections router
router = APIRouter(tags=["Connections"])

# Include the operation routers (static paths) before the management routes.
# FastAPI matches static path segments ahead of path parameters, so a request
# to /options resolves to the options handler rather than /{connection_id}.
# Declaration order (static routers first) reinforces this — no manual route
# priority sorting is needed. (FastAPI <0.137 required the sort; 0.137 made
# include_router lazy and added native static-over-parameter matching.)
router.include_router(sync_router)
router.include_router(options_router)
router.include_router(oauth_router)

# Management routes use empty paths, so include_router can't prefix them
# cleanly — append them directly (after the static routers) and tag them.
for route in management_router.routes:
  if not hasattr(route, "tags") or not route.tags:
    route.tags = ["Connections"]
  router.routes.append(route)

__all__ = ["router"]
