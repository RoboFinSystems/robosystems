"""Tests for MCP materialization tools (get-graph-sync-status, materialize-graph)."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.materialization_tools import (
  GetGraphSyncStatusTool,
  MaterializeGraphTool,
)


@pytest.fixture
def mock_client():
  client = MagicMock()
  client.graph_id = "kg_test123"
  return client


class TestGetGraphSyncStatusTool:
  def test_tool_definition(self, mock_client):
    tool = GetGraphSyncStatusTool(mock_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "get-graph-sync-status"
    assert "inputSchema" in defn

  @pytest.mark.asyncio
  async def test_returns_fresh_status(self, mock_client):
    """Fresh graph returns sync_status=fresh."""
    mock_graph = MagicMock()
    mock_graph.graph_stale = False
    mock_graph.graph_stale_reason = None
    mock_graph.graph_stale_at = None
    mock_graph.graph_metadata = {
      "last_materialized_at": datetime.now(UTC).isoformat(),
      "materialization_count": 3,
    }

    mock_session = MagicMock()

    with (
      patch(
        "robosystems.database.SessionFactory",
        return_value=mock_session,
      ),
      patch("robosystems.models.core.Graph") as mock_graph_cls,
    ):
      mock_graph_cls.get_by_id.return_value = mock_graph
      tool = GetGraphSyncStatusTool(mock_client)
      result = await tool.execute({})

    assert result["sync_status"] == "fresh"
    assert result["stale_since"] is None
    assert result["stale_duration_minutes"] is None
    assert result["materialization_count"] == 3

  @pytest.mark.asyncio
  async def test_returns_stale_status(self, mock_client):
    """Stale graph returns sync_status=stale with duration."""
    stale_at = datetime.now(UTC) - timedelta(minutes=15)
    mock_graph = MagicMock()
    mock_graph.graph_stale = True
    mock_graph.graph_stale_reason = "schedule_created"
    mock_graph.graph_stale_at = stale_at
    mock_graph.graph_metadata = {}

    mock_session = MagicMock()

    with (
      patch(
        "robosystems.database.SessionFactory",
        return_value=mock_session,
      ),
      patch("robosystems.models.core.Graph") as mock_graph_cls,
    ):
      mock_graph_cls.get_by_id.return_value = mock_graph
      tool = GetGraphSyncStatusTool(mock_client)
      result = await tool.execute({})

    assert result["sync_status"] == "stale"
    assert result["stale_reason"] == "schedule_created"
    assert result["stale_duration_minutes"] >= 14

  @pytest.mark.asyncio
  async def test_returns_error_for_missing_graph(self, mock_client):
    """Missing graph returns error dict."""
    mock_session = MagicMock()

    with (
      patch(
        "robosystems.database.SessionFactory",
        return_value=mock_session,
      ),
      patch("robosystems.models.core.Graph") as mock_graph_cls,
    ):
      mock_graph_cls.get_by_id.return_value = None
      tool = GetGraphSyncStatusTool(mock_client)
      result = await tool.execute({})

    assert "error" in result


class TestMaterializeGraphTool:
  def test_tool_definition(self, mock_client):
    tool = MaterializeGraphTool(mock_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "materialize-graph"
    assert "force" in defn["inputSchema"]["properties"]

  @pytest.mark.asyncio
  async def test_not_stale_without_force(self, mock_client):
    """Returns not_stale when graph is fresh and force=false."""
    mock_graph = MagicMock()
    mock_graph.graph_stale = False

    mock_session = MagicMock()

    with (
      patch(
        "robosystems.database.SessionFactory",
        return_value=mock_session,
      ),
      patch("robosystems.models.core.Graph") as mock_graph_cls,
    ):
      mock_graph_cls.get_by_id.return_value = mock_graph
      tool = MaterializeGraphTool(mock_client)
      result = await tool.execute({"force": False})

    assert result["status"] == "not_stale"

  @pytest.mark.asyncio
  async def test_enqueues_when_stale(self, mock_client):
    """Enqueues materialization task when graph is stale."""
    mock_graph = MagicMock()
    mock_graph.graph_stale = True

    mock_session = MagicMock()
    mock_lock_result = MagicMock()
    mock_lock_result.acquired = True

    with (
      patch(
        "robosystems.database.SessionFactory",
        return_value=mock_session,
      ),
      patch("robosystems.models.core.Graph") as mock_graph_cls,
      patch("robosystems.config.valkey_registry.create_redis_client"),
      patch(
        "robosystems.middleware.auth.distributed_lock.DistributedLock"
      ) as mock_lock_cls,
      patch(
        "robosystems.worker.client.enqueue_task",
        new_callable=AsyncMock,
      ) as mock_enqueue,
    ):
      mock_graph_cls.get_by_id.return_value = mock_graph
      mock_lock_cls.return_value.acquire.return_value = mock_lock_result
      mock_enqueue.return_value = {"operation_id": "op_abc123"}

      tool = MaterializeGraphTool(mock_client)
      result = await tool.execute({})

    assert result["status"] == "queued"
    assert result["operation_id"] == "op_abc123"
    mock_enqueue.assert_called_once()
    call_kwargs = mock_enqueue.call_args
    assert call_kwargs.kwargs["task_type"] == "dagster_job_monitor"
    assert call_kwargs.kwargs["graph_id"] == "kg_test123"

  @pytest.mark.asyncio
  async def test_enqueues_when_force(self, mock_client):
    """Enqueues materialization with force=true, skipping staleness check."""
    mock_lock_result = MagicMock()
    mock_lock_result.acquired = True

    with (
      patch("robosystems.config.valkey_registry.create_redis_client"),
      patch(
        "robosystems.middleware.auth.distributed_lock.DistributedLock"
      ) as mock_lock_cls,
      patch(
        "robosystems.worker.client.enqueue_task",
        new_callable=AsyncMock,
      ) as mock_enqueue,
    ):
      mock_lock_cls.return_value.acquire.return_value = mock_lock_result
      mock_enqueue.return_value = {"operation_id": "op_forced"}

      tool = MaterializeGraphTool(mock_client)
      result = await tool.execute({"force": True})

    assert result["status"] == "queued"
    assert result["operation_id"] == "op_forced"

  @pytest.mark.asyncio
  async def test_lock_conflict(self, mock_client):
    """Returns conflict when lock is already held."""
    mock_graph = MagicMock()
    mock_graph.graph_stale = True

    mock_session = MagicMock()
    mock_lock_result = MagicMock()
    mock_lock_result.acquired = False

    with (
      patch(
        "robosystems.database.SessionFactory",
        return_value=mock_session,
      ),
      patch("robosystems.models.core.Graph") as mock_graph_cls,
      patch("robosystems.config.valkey_registry.create_redis_client"),
      patch(
        "robosystems.middleware.auth.distributed_lock.DistributedLock"
      ) as mock_lock_cls,
    ):
      mock_graph_cls.get_by_id.return_value = mock_graph
      mock_lock_cls.return_value.acquire.return_value = mock_lock_result

      tool = MaterializeGraphTool(mock_client)
      result = await tool.execute({})

    assert result["status"] == "conflict"

  @pytest.mark.asyncio
  async def test_releases_lock_on_enqueue_failure(self, mock_client):
    """Releases lock if enqueue_task raises."""
    mock_lock_result = MagicMock()
    mock_lock_result.acquired = True
    mock_lock_instance = MagicMock()
    mock_lock_instance.acquire.return_value = mock_lock_result

    with (
      patch("robosystems.config.valkey_registry.create_redis_client"),
      patch(
        "robosystems.middleware.auth.distributed_lock.DistributedLock",
        return_value=mock_lock_instance,
      ),
      patch(
        "robosystems.worker.client.enqueue_task",
        new_callable=AsyncMock,
        side_effect=RuntimeError("worker unavailable"),
      ),
    ):
      tool = MaterializeGraphTool(mock_client)
      result = await tool.execute({"force": True})

    assert result["status"] == "error"
    mock_lock_instance.release.assert_called_once()
