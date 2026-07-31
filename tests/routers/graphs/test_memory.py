"""Tests for the semantic-memory read router.

Covers list / get / recall plus the three gates each handler runs before
touching a graph: the `SEMANTIC_MEMORY_ENABLED` flag, the shared-repository
block, and per-graph access enforcement. Also pins the writer-client
lifecycle -- every handler must close its client even when the body raises.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.config import env as env_config
from robosystems.models.api.memory import (
  MemoryListResponse,
  MemoryRecallRequest,
  MemoryRecord,
)
from robosystems.routers.graphs.memory import (
  get_memory,
  list_memories,
  recall_memory,
)

MODULE = "robosystems.routers.graphs.memory"

NOW = datetime(2026, 4, 1, tzinfo=UTC)


def _mock_user():
  user = MagicMock()
  user.id = "usr_1"
  return user


def _record(**overrides):
  defaults = {
    "id": "mem_abc123",
    "text": "Q2 close ran two days late",
    "source": "api",
    "memory_type": "note",
    "tags": ["close"],
    "created_at": NOW,
    "updated_at": NOW,
  }
  defaults.update(overrides)
  return MemoryRecord(**defaults)


@pytest.fixture
def gates():
  """Open all three gates; individual tests re-close the one under test."""
  with (
    patch(f"{MODULE}._require_memory_enabled"),
    patch(f"{MODULE}._block_shared_repository"),
    patch(f"{MODULE}._enforce_graph_access"),
  ):
    yield


@pytest.fixture
def writer_client():
  """Stub the writer-only graph client and assert it gets closed."""
  client = MagicMock()
  client.close = AsyncMock()
  with patch(f"{MODULE}._get_writer_client", AsyncMock(return_value=client)):
    yield client


@pytest.fixture
def service():
  svc = MagicMock()
  svc.list_memories = AsyncMock()
  svc.get_memory = AsyncMock()
  svc.recall = AsyncMock()
  with patch(f"{MODULE}._require_service", return_value=svc):
    yield svc


@pytest.mark.unit
class TestListMemories:
  @pytest.mark.asyncio
  async def test_returns_memories_and_closes_client(
    self, gates, writer_client, service
  ):
    service.list_memories.return_value = MemoryListResponse(
      total=2, memories=[_record(), _record(id="mem_def456")], graph_id="kg_test"
    )

    result = await list_memories(graph_id="kg_test", current_user=_mock_user())

    assert result.total == 2
    assert result.graph_id == "kg_test"
    writer_client.close.assert_awaited_once()

  @pytest.mark.asyncio
  async def test_forwards_filters_and_pagination(self, gates, writer_client, service):
    service.list_memories.return_value = MemoryListResponse(
      total=0, memories=[], graph_id="kg_test"
    )

    await list_memories(
      graph_id="kg_test",
      memory_type="decision",
      source="operator",
      limit=25,
      offset=50,
      current_user=_mock_user(),
    )

    service.list_memories.assert_awaited_once_with(
      "kg_test",
      memory_type="decision",
      source="operator",
      limit=25,
      offset=50,
    )

  @pytest.mark.asyncio
  async def test_closes_client_when_service_raises(self, gates, writer_client, service):
    """A leaked writer client holds a connection against the master node."""
    service.list_memories.side_effect = RuntimeError("store unavailable")

    with pytest.raises(RuntimeError):
      await list_memories(graph_id="kg_test", current_user=_mock_user())

    writer_client.close.assert_awaited_once()


@pytest.mark.unit
class TestGetMemory:
  @pytest.mark.asyncio
  async def test_returns_record(self, gates, writer_client, service):
    service.get_memory.return_value = _record()

    result = await get_memory(
      graph_id="kg_test", memory_id="mem_abc123", current_user=_mock_user()
    )

    assert result.id == "mem_abc123"
    service.get_memory.assert_awaited_once_with("kg_test", "mem_abc123")
    writer_client.close.assert_awaited_once()

  @pytest.mark.asyncio
  async def test_missing_memory_is_404_not_null(self, gates, writer_client, service):
    service.get_memory.return_value = None

    with pytest.raises(HTTPException) as exc:
      await get_memory(
        graph_id="kg_test", memory_id="mem_missing", current_user=_mock_user()
      )

    assert exc.value.status_code == 404
    writer_client.close.assert_awaited_once(), "client must close on the 404 path"


@pytest.mark.unit
class TestRecallMemory:
  @pytest.mark.asyncio
  async def test_forwards_request_and_returns_hits(self, gates, writer_client, service):
    expected = MagicMock()
    service.recall.return_value = expected
    request = MemoryRecallRequest(query="why did close slip", k=5)

    result = await recall_memory(
      graph_id="kg_test", request=request, current_user=_mock_user()
    )

    assert result is expected
    service.recall.assert_awaited_once_with("kg_test", request)
    writer_client.close.assert_awaited_once()

  @pytest.mark.asyncio
  async def test_closes_client_when_recall_raises(self, gates, writer_client, service):
    service.recall.side_effect = RuntimeError("index missing")

    with pytest.raises(RuntimeError):
      await recall_memory(
        graph_id="kg_test",
        request=MemoryRecallRequest(query="anything"),
        current_user=_mock_user(),
      )

    writer_client.close.assert_awaited_once()


@pytest.mark.unit
class TestGates:
  """Each gate must fire before any graph work happens -- a handler that
  reached `_get_writer_client` would have already opened a connection to a
  graph the caller may not be entitled to."""

  @pytest.mark.asyncio
  @pytest.mark.parametrize(
    "handler,kwargs",
    [
      (list_memories, {}),
      (get_memory, {"memory_id": "mem_1"}),
      (recall_memory, {"request": MemoryRecallRequest(query="q")}),
    ],
  )
  async def test_disabled_flag_returns_503(self, handler, kwargs):
    with (
      patch.object(env_config, "SEMANTIC_MEMORY_ENABLED", False),
      patch(f"{MODULE}._get_writer_client", AsyncMock()) as mock_client,
    ):
      with pytest.raises(HTTPException) as exc:
        await handler(graph_id="kg_test", current_user=_mock_user(), **kwargs)

    assert exc.value.status_code == 503
    mock_client.assert_not_awaited()

  @pytest.mark.asyncio
  @pytest.mark.parametrize(
    "handler,kwargs",
    [
      (list_memories, {}),
      (get_memory, {"memory_id": "mem_1"}),
      (recall_memory, {"request": MemoryRecallRequest(query="q")}),
    ],
  )
  async def test_shared_repository_is_forbidden(self, handler, kwargs):
    with (
      patch(f"{MODULE}._require_memory_enabled"),
      patch(f"{MODULE}._get_writer_client", AsyncMock()) as mock_client,
    ):
      with pytest.raises(HTTPException) as exc:
        await handler(graph_id="sec", current_user=_mock_user(), **kwargs)

    assert exc.value.status_code == 403
    mock_client.assert_not_awaited()

  @pytest.mark.asyncio
  @pytest.mark.parametrize(
    "handler,kwargs",
    [
      (list_memories, {}),
      (get_memory, {"memory_id": "mem_1"}),
      (recall_memory, {"request": MemoryRecallRequest(query="q")}),
    ],
  )
  async def test_graph_access_is_enforced(self, handler, kwargs):
    denied = HTTPException(status_code=403, detail="No access to graph")
    with (
      patch(f"{MODULE}._require_memory_enabled"),
      patch(f"{MODULE}._block_shared_repository"),
      patch(f"{MODULE}._enforce_graph_access", side_effect=denied),
      patch(f"{MODULE}._get_writer_client", AsyncMock()) as mock_client,
    ):
      with pytest.raises(HTTPException) as exc:
        await handler(graph_id="kg_other", current_user=_mock_user(), **kwargs)

    assert exc.value.status_code == 403
    mock_client.assert_not_awaited()


@pytest.mark.unit
class TestGateHelpers:
  def test_require_memory_enabled_passes_when_flag_set(self):
    from robosystems.routers.graphs.memory import _require_memory_enabled

    with patch.object(env_config, "SEMANTIC_MEMORY_ENABLED", True):
      _require_memory_enabled()  # must not raise

  def test_block_shared_repository_allows_customer_graph(self):
    from robosystems.routers.graphs.memory import _block_shared_repository

    _block_shared_repository("kg1a2b3c4d5e6f78")  # must not raise

  def test_enforce_graph_access_closes_its_session(self):
    """The helper opens a SessionFactory directly; a leak here exhausts the
    platform-DB pool under load."""
    from robosystems.routers.graphs.memory import _enforce_graph_access

    session = MagicMock()
    with (
      patch(f"{MODULE}.SessionFactory", return_value=session),
      patch(
        "robosystems.middleware.billing.enforcement.require_graph_access"
      ) as mock_require,
    ):
      _enforce_graph_access("kg_test", require_write=True)

    mock_require.assert_called_once_with("kg_test", session, require_write=True)
    session.close.assert_called_once()

  def test_enforce_graph_access_closes_session_on_denial(self):
    from robosystems.routers.graphs.memory import _enforce_graph_access

    session = MagicMock()
    with (
      patch(f"{MODULE}.SessionFactory", return_value=session),
      patch(
        "robosystems.middleware.billing.enforcement.require_graph_access",
        side_effect=HTTPException(status_code=403, detail="denied"),
      ),
    ):
      with pytest.raises(HTTPException):
        _enforce_graph_access("kg_test")

    session.close.assert_called_once()
