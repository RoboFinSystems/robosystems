"""Regression guard against shadowed routes.

FastAPI matches routes in registration order and stops at the first full
match; path params are validated only *after* that route is chosen. So a
literal-segment route registered after a param-segment sibling is unreachable
— every call lands on the sibling, which 404s on a resource named for the
literal. The param's ``pattern=`` does not save it: a mismatch there is a 422
on the sibling, never a fall-through to the route that meant to serve it.

Three routes shipped this way and reached the published OpenAPI spec and both
1.x SDKs before anyone noticed (``/operator/batch``, ``/operator/recommend``,
``/subgraphs/quota``). The class is invisible in review — both routes look
correct in isolation, and the spec cheerfully documents the unreachable one —
so it is asserted here instead.

``iter_route_contexts`` is what flattens ``include_router``'s lazy branches
into the order the dispatcher actually walks, with prefixes applied. Reading
``app.routes`` directly sees only the handful of routes registered on the app
itself and would pass vacuously.
"""

import pytest
from fastapi.routing import iter_route_contexts

from main import app

# Cannot match a literal segment; any ``{param}`` placeholder matches it.
# Substituted into a route's own path to build the concrete request path that
# route exists to serve.
_PARAM_SENTINEL = "zzshadowprobezz"


def _concrete_path(path: str) -> str:
  """Build the request path a route template is meant to serve."""
  return "/".join(
    _PARAM_SENTINEL if segment.startswith("{") and segment.endswith("}") else segment
    for segment in path.split("/")
  )


def _http_routes():
  """Every effective HTTP route, in the order the dispatcher tries them."""
  routes = []
  for context in iter_route_contexts(app.routes):
    path = getattr(context, "path", None)
    methods = getattr(context, "methods", None)
    if path and methods and getattr(context, "path_regex", None) is not None:
      routes.append(context)
  return routes


def _find_shadowed(routes) -> list[tuple[object, object]]:
  """Return (shadowed, shadowing) pairs — routes an earlier route swallows."""
  shadowed = []

  for index, route in enumerate(routes):
    probe = _concrete_path(route.path)
    for earlier in routes[:index]:
      if earlier.path == route.path:
        continue
      if not set(earlier.methods) & set(route.methods):
        continue
      if earlier.path_regex.fullmatch(probe):
        shadowed.append((route, earlier))
        break

  return shadowed


@pytest.mark.unit
def test_no_shadowed_routes():
  """Every registered route must be reachable at its own path."""
  routes = _http_routes()
  assert len(routes) > 100, (
    f"Only {len(routes)} routes enumerated — the flattening broke and this "
    "test would pass vacuously."
  )

  shadowed = _find_shadowed(routes)

  assert not shadowed, (
    "Unreachable routes — an earlier route matches first:\n"
    + "\n".join(
      f"  {sorted(route.methods)} {route.path} "
      f"(shadowed by {sorted(earlier.methods)} {earlier.path})"
      for route, earlier in shadowed
    )
  )


@pytest.mark.unit
def test_detector_fires_on_the_historical_shadowings():
  """The detector must actually fire — a green suite proves nothing otherwise.

  Re-registers the three routes this defect class produced and asserts each
  is reported against the sibling that swallowed it.
  """
  from fastapi import APIRouter

  probe = APIRouter()

  @probe.get("/v1/graphs/{graph_id}/subgraphs/quota")
  async def quota(graph_id: str): ...  # pragma: no cover - routing fixture

  @probe.post("/v1/graphs/{graph_id}/operator/batch")
  async def batch(graph_id: str): ...  # pragma: no cover - routing fixture

  @probe.post("/v1/graphs/{graph_id}/operator/recommend")
  async def recommend(graph_id: str): ...  # pragma: no cover - routing fixture

  routes = _http_routes() + list(iter_route_contexts(probe.routes))
  found = {route.path: earlier.path for route, earlier in _find_shadowed(routes)}

  assert found == {
    "/v1/graphs/{graph_id}/subgraphs/quota": "/v1/graphs/{graph_id}/subgraphs/{subgraph_name}",
    "/v1/graphs/{graph_id}/operator/batch": "/v1/graphs/{graph_id}/operator/{operator_type}",
    "/v1/graphs/{graph_id}/operator/recommend": "/v1/graphs/{graph_id}/operator/{operator_type}",
  }
