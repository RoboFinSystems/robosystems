"""Connection routers: provider options, CRUD, sync, and OAuth."""

from fastapi import APIRouter
from fastapi.routing import APIRoute

from .management import router as management_router
from .oauth import router as oauth_router
from .options import router as options_router
from .sync import router as sync_router

# The API reference lists operations in registration order, so this is the
# documented order: discover providers, list and create, authorize, then the
# per-connection operations. The static /options route must stay ahead of
# /{connection_id}, which would otherwise capture it.
DOCUMENTED_ORDER = (
  "getConnectionOptions",
  "listConnections",
  "createConnection",
  "initOAuth",
  "oauthCallback",
  "getConnection",
  "deleteConnection",
  "syncConnection",
  "setConnectionWritePolicy",
)

_routes_by_operation = {
  route.operation_id: route
  for sub_router in (options_router, management_router, oauth_router, sync_router)
  for route in sub_router.routes
  if isinstance(route, APIRoute)
}
if set(_routes_by_operation) != set(DOCUMENTED_ORDER):
  raise RuntimeError(
    "Connections routes and DOCUMENTED_ORDER disagree on: "
    f"{sorted(map(str, set(_routes_by_operation) ^ set(DOCUMENTED_ORDER)))}"
  )

# Routes are appended rather than included because the collection routes use
# an empty path, which include_router refuses; tagging happens here for the
# same reason.
router = APIRouter(tags=["Connections"])
for operation_id in DOCUMENTED_ORDER:
  route = _routes_by_operation[operation_id]
  route.tags = [*router.tags, *route.tags]
  router.routes.append(route)

__all__ = ["router"]
