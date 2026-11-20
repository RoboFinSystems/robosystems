"""``/admin/v1/webhooks/`` is reachable without an admin key by construction:
a provider cannot hold one. So every route under it must refuse a request the
provider did not sign."""

import pytest
from fastapi.routing import iter_route_contexts
from fastapi.testclient import TestClient

from main import app

pytestmark = pytest.mark.unit

PREFIX = "/admin/v1/webhooks/"


def _webhook_routes() -> list[tuple[str, str]]:
  # `iter_route_contexts` flattens the lazily included routers; `app.routes`
  # alone would find nothing and pass vacuously.
  return sorted(
    (method, context.path)
    for context in iter_route_contexts(app.routes)
    if str(getattr(context, "path", "")).startswith(PREFIX)
    for method in getattr(context, "methods", None) or ()
  )


def test_the_prefix_holds_routes_to_guard():
  assert ("POST", f"{PREFIX}stripe") in _webhook_routes()


@pytest.mark.parametrize(("method", "path"), _webhook_routes())
def test_an_unsigned_request_is_refused(method, path):
  response = TestClient(app).request(method, path, content=b"{}")
  assert response.status_code in (400, 401, 403), (
    f"{method} {path} answered {response.status_code} to a request with no "
    "provider signature"
  )
