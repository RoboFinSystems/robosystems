"""Connection routers: provider options, CRUD, sync, and OAuth."""

from fastapi import APIRouter

from .management import router as management_router
from .oauth import router as oauth_router
from .options import router as options_router
from .sync import router as sync_router

router = APIRouter(tags=["Connections"])

# Static routers first so /options etc. resolve before /{connection_id}.
router.include_router(sync_router)
router.include_router(options_router)
router.include_router(oauth_router)

# Management routes use empty paths, so they are appended directly and tagged.
for route in management_router.routes:
  if not hasattr(route, "tags") or not route.tags:
    route.tags = ["Connections"]
  router.routes.append(route)

__all__ = ["router"]
