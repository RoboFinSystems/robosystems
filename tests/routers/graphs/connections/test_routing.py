"""
Route-ordering guardrail for the connections router.

The static ``/options`` route must resolve ahead of the parameterized
``/{connection_id}`` route (both are GET on the same path segment), otherwise
``GET .../connections/options`` would be captured as ``connection_id="options"``.

Prior to FastAPI 0.137 this was enforced with a manual ``.priority`` sort over
``router.routes``. FastAPI 0.137 made ``include_router`` lazy (child routes live
inside an ``_IncludedRouter`` wrapper) and added native static-over-parameter
matching, so the sort was removed. This test locks the invariant in place so a
future refactor can't silently reintroduce the ambiguity.
"""

from robosystems.routers.graphs.connections import router


def _entry_paths(entry) -> list[str]:
  """The path(s) an entry in ``router.routes`` represents.

  A plain ``APIRoute`` has its own ``path``; an ``_IncludedRouter`` (lazy
  include) exposes its children via ``original_router.routes``.
  """
  if hasattr(entry, "path"):
    return [entry.path]
  original = getattr(entry, "original_router", None)
  return [getattr(child, "path", "") for child in getattr(original, "routes", [])]


def _first_index(paths_by_entry: list[list[str]], target: str) -> int:
  for i, paths in enumerate(paths_by_entry):
    if target in paths:
      return i
  raise AssertionError(f"{target} not found among connections routes")


def test_options_resolves_before_connection_id():
  paths_by_entry = [_entry_paths(e) for e in router.routes]
  assert _first_index(paths_by_entry, "/options") < _first_index(
    paths_by_entry, "/{connection_id}"
  )


def test_connections_router_exposes_expected_paths():
  all_paths = {p for e in router.routes for p in _entry_paths(e)}
  for expected in (
    "/options",
    "/{connection_id}",
    "/{connection_id}/sync",
    "/oauth/init",
  ):
    assert expected in all_paths
