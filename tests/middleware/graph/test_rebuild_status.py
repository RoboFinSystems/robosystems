"""The rebuild flag a Graph API caller checks before sending a query."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from robosystems.middleware.graph import rebuild_status
from robosystems.middleware.graph.rebuild_status import is_graph_rebuilding

GRAPH = "kg0123456789abcdef01"


@pytest.fixture(autouse=True)
def _fresh_cache():
  rebuild_status.clear_rebuild_status_cache()
  yield
  rebuild_status.clear_rebuild_status_cache()


def _graph_lookup(result):
  """Patch the platform lookup; returns the ``Graph.get_by_id`` mock."""
  get_by_id = MagicMock(side_effect=result if callable(result) else None)
  if not callable(result):
    get_by_id.return_value = result
  return (
    patch("robosystems.database.SessionFactory", MagicMock()),
    patch("robosystems.models.core.Graph.get_by_id", get_by_id),
    get_by_id,
  )


@pytest.mark.parametrize(
  ("graph", "expected"),
  [
    (SimpleNamespace(is_rebuilding=True), True),
    (SimpleNamespace(is_rebuilding=False), False),
    (None, False),
  ],
  ids=["rebuilding", "idle", "unknown-graph"],
)
async def test_reads_the_flag_from_the_graph_row(graph, expected):
  factory, lookup, _ = _graph_lookup(graph)
  with factory, lookup:
    assert await is_graph_rebuilding(GRAPH) is expected


async def test_answer_is_cached_per_graph():
  factory, lookup, get_by_id = _graph_lookup(SimpleNamespace(is_rebuilding=True))
  with factory, lookup:
    assert await is_graph_rebuilding(GRAPH) is True
    assert await is_graph_rebuilding(GRAPH) is True
    assert get_by_id.call_count == 1

    assert await is_graph_rebuilding("kgfedcba9876543210ff") is True
    assert get_by_id.call_count == 2


async def test_cache_expires(monkeypatch):
  factory, lookup, get_by_id = _graph_lookup(SimpleNamespace(is_rebuilding=True))
  clock = SimpleNamespace(now=1000.0)
  monkeypatch.setattr(rebuild_status.time, "monotonic", lambda: clock.now)
  with factory, lookup:
    await is_graph_rebuilding(GRAPH)
    clock.now += rebuild_status.REBUILD_CACHE_TTL_SECONDS + 1
    await is_graph_rebuilding(GRAPH)
  assert get_by_id.call_count == 2


async def test_database_failure_fails_open_and_is_remembered_briefly(monkeypatch):
  def boom(*_args, **_kwargs):
    raise RuntimeError("database unreachable")

  factory, lookup, get_by_id = _graph_lookup(boom)
  clock = SimpleNamespace(now=1000.0)
  monkeypatch.setattr(rebuild_status.time, "monotonic", lambda: clock.now)
  with factory, lookup:
    assert await is_graph_rebuilding(GRAPH) is False
    # An unreachable database is not asked again by the next query...
    assert await is_graph_rebuilding(GRAPH) is False
    assert get_by_id.call_count == 1

    # ...but far sooner than a real answer would be.
    clock.now += rebuild_status.FAILED_LOOKUP_TTL_SECONDS + 1
    assert await is_graph_rebuilding(GRAPH) is False
    assert get_by_id.call_count == 2
