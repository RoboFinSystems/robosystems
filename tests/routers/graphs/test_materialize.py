from types import SimpleNamespace
from datetime import datetime, timezone

import pytest

from robosystems.routers.graphs import materialize as materialize_router


@pytest.mark.asyncio
async def test_get_materialization_status_fresh_graph(monkeypatch):
  async def fake_repo(*args, **kwargs):
    return SimpleNamespace()

  monkeypatch.setattr(
    materialize_router,
    "get_universal_repository",
    fake_repo,
  )

  last_materialized_at = datetime.now(timezone.utc)

  graph_record = SimpleNamespace(
    graph_id="graph-123",
    graph_stale=False,
    graph_stale_reason=None,
    graph_stale_at=None,
    graph_metadata={
      "last_materialized_at": last_materialized_at.isoformat(),
      "materialization_count": 5,
    },
  )

  monkeypatch.setattr(
    materialize_router.Graph,
    "get_by_id",
    classmethod(lambda cls, graph_id, session: graph_record),
  )

  fake_db = SimpleNamespace()

  result = await materialize_router.get_materialization_status(
    graph_id="graph-123",
    current_user=SimpleNamespace(id="user-123"),
    db=fake_db,
  )

  assert result.graph_id == "graph-123"
  assert result.is_stale is False
  assert result.materialization_count == 5
  assert result.hours_since_materialization is not None
  assert result.hours_since_materialization >= 0
  assert "fresh" in result.message.lower() or "Last materialized" in result.message


@pytest.mark.asyncio
async def test_get_materialization_status_stale_graph(monkeypatch):
  async def fake_repo(*args, **kwargs):
    return SimpleNamespace()

  monkeypatch.setattr(
    materialize_router,
    "get_universal_repository",
    fake_repo,
  )

  last_materialized_at = datetime.now(timezone.utc)
  stale_at = datetime.now(timezone.utc)

  graph_record = SimpleNamespace(
    graph_id="graph-123",
    graph_stale=True,
    graph_stale_reason="File deletion",
    graph_stale_at=stale_at,
    graph_metadata={
      "last_materialized_at": last_materialized_at.isoformat(),
      "materialization_count": 3,
    },
  )

  monkeypatch.setattr(
    materialize_router.Graph,
    "get_by_id",
    classmethod(lambda cls, graph_id, session: graph_record),
  )

  fake_db = SimpleNamespace()

  result = await materialize_router.get_materialization_status(
    graph_id="graph-123",
    current_user=SimpleNamespace(id="user-123"),
    db=fake_db,
  )

  assert result.graph_id == "graph-123"
  assert result.is_stale is True
  assert result.stale_reason == "File deletion"
  assert result.materialization_count == 3
  assert result.hours_since_materialization is not None
  assert "stale" in result.message.lower()
  assert "recommended" in result.message.lower()


@pytest.mark.asyncio
async def test_get_materialization_status_never_materialized(monkeypatch):
  async def fake_repo(*args, **kwargs):
    return SimpleNamespace()

  monkeypatch.setattr(
    materialize_router,
    "get_universal_repository",
    fake_repo,
  )

  graph_record = SimpleNamespace(
    graph_id="graph-123",
    graph_stale=False,
    graph_stale_reason=None,
    graph_stale_at=None,
    graph_metadata={},
  )

  monkeypatch.setattr(
    materialize_router.Graph,
    "get_by_id",
    classmethod(lambda cls, graph_id, session: graph_record),
  )

  fake_db = SimpleNamespace()

  result = await materialize_router.get_materialization_status(
    graph_id="graph-123",
    current_user=SimpleNamespace(id="user-123"),
    db=fake_db,
  )

  assert result.graph_id == "graph-123"
  assert result.is_stale is False
  assert result.last_materialized_at is None
  assert result.materialization_count == 0
  assert result.hours_since_materialization is None


@pytest.mark.asyncio
async def test_get_materialization_status_graph_not_found(monkeypatch):
  async def fake_repo(*args, **kwargs):
    return None

  monkeypatch.setattr(
    materialize_router,
    "get_universal_repository",
    fake_repo,
  )

  fake_db = SimpleNamespace()

  with pytest.raises(Exception) as exc:
    await materialize_router.get_materialization_status(
      graph_id="graph-404",
      current_user=SimpleNamespace(id="user-123"),
      db=fake_db,
    )

  assert getattr(exc.value, "status_code", None) == 404
