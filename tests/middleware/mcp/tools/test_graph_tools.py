"""Tests for Phase 4 graph-lifecycle MCP tools.

Covers the six tools in `middleware/mcp/tools/graph_tools.py`:

1. `CreateSubgraphTool`
2. `DeleteSubgraphTool`
3. `ListSubgraphsTool`
4. `MaterializeTool`
5. `CreateBackupTool`
6. `GetGraphSyncStatusTool`

Plus the client-side sentinel `SwitchWorkspaceTool`.

Shared-repo gate coverage (the primary defense-in-depth concern) lives
in `tests/routers/graphs/test_ops_shared_repo_gates.py`. This file
focuses on definition shape + non-repo error paths + happy path so the
Phase 4 refactor is exercised beyond manager-level wiring.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.graph_tools import (
  CreateBackupTool,
  CreateSubgraphTool,
  DeleteSubgraphTool,
  GetGraphSyncStatusTool,
  ListSubgraphsTool,
  MaterializeTool,
  SwitchWorkspaceTool,
)

MODULE = "robosystems.middleware.mcp.tools.graph_tools"
GRAPH_ID = "kg01234567890abcdef"


def _user() -> MagicMock:
  u = MagicMock()
  u.id = "usr_test"
  return u


def _client(graph_id: str = GRAPH_ID, user: MagicMock | None = None) -> MagicMock:
  c = MagicMock()
  c.graph_id = graph_id
  c.user = user if user is not None else _user()
  return c


def _session_cm(mock_session: MagicMock):
  cm = MagicMock()
  cm.__enter__ = MagicMock(return_value=mock_session)
  cm.__exit__ = MagicMock(return_value=False)
  return cm


@pytest.fixture
def mock_db():
  """Patch `_open_platform_session` to return a fake session."""
  mock_session = MagicMock()
  with patch(
    f"{MODULE}._open_platform_session",
    return_value=(mock_session, lambda: None),
  ):
    yield mock_session


# ══════════════════════════════════════════════════════════════════════════
# CreateSubgraphTool
# ══════════════════════════════════════════════════════════════════════════


class TestCreateSubgraphDefinition:
  def test_definition_shape(self) -> None:
    defn = CreateSubgraphTool(_client()).get_tool_definition()
    assert defn["name"] == "create-subgraph"
    assert "name" in defn["inputSchema"]["required"]
    assert defn["inputSchema"]["additionalProperties"] is False


class TestCreateSubgraphExecute:
  @pytest.mark.asyncio
  @pytest.mark.parametrize("bad_name", ["dev-test", "a" * 21, "", "has_underscore"])
  async def test_invalid_name_rejected_pre_db(self, bad_name: str) -> None:
    tool = CreateSubgraphTool(_client())
    result = await tool.execute({"name": bad_name})
    assert result["error"] == "invalid_name"
    assert "alphanumeric" in result["message"]

  @pytest.mark.asyncio
  async def test_authentication_required(self) -> None:
    client = _client()
    client.user = None
    tool = CreateSubgraphTool(client)
    result = await tool.execute({"name": "dev"})
    assert result["error"] == "authentication_required"

  @pytest.mark.asyncio
  async def test_parent_not_found(self, mock_db: MagicMock) -> None:
    mock_db.query.return_value.filter.return_value.first.return_value = None
    result = await CreateSubgraphTool(_client()).execute({"name": "dev"})
    assert result["error"] == "parent_not_found"

  @pytest.mark.asyncio
  async def test_happy_path_delegates_to_subgraph_service(
    self, mock_db: MagicMock
  ) -> None:
    parent = MagicMock()
    mock_db.query.return_value.filter.return_value.first.return_value = parent

    with patch(
      "robosystems.operations.graph.subgraph_service.SubgraphService"
    ) as mock_svc_class:
      mock_svc = AsyncMock()
      mock_svc.create_subgraph.return_value = {
        "graph_id": f"{GRAPH_ID}_dev",
        "operation_id": "op_01",
      }
      mock_svc_class.return_value = mock_svc

      result = await CreateSubgraphTool(_client()).execute(
        {"name": "dev", "fork_parent": True}
      )

    assert result["subgraph_id"] == f"{GRAPH_ID}_dev"
    assert result["forked_from_parent"] is True
    assert result["operation_id"] == "op_01"
    # Service was handed the parent row + auth user
    call_kwargs = mock_svc.create_subgraph.call_args.kwargs
    assert call_kwargs["parent_graph"] is parent
    assert call_kwargs["fork_parent"] is True


# ══════════════════════════════════════════════════════════════════════════
# DeleteSubgraphTool
# ══════════════════════════════════════════════════════════════════════════


class TestDeleteSubgraphDefinition:
  def test_definition_shape(self) -> None:
    defn = DeleteSubgraphTool(_client()).get_tool_definition()
    assert defn["name"] == "delete-subgraph"
    assert "subgraph_id" in defn["inputSchema"]["required"]


class TestDeleteSubgraphExecute:
  @pytest.mark.asyncio
  async def test_invalid_subgraph_id(self) -> None:
    tool = DeleteSubgraphTool(_client())
    result = await tool.execute({"subgraph_id": "not-a-real-id"})
    assert result["error"] == "invalid_subgraph_id"

  @pytest.mark.asyncio
  async def test_cross_tenant_rejected(self) -> None:
    """Subgraph id encodes parent graph — mismatching the client's
    `graph_id` must 403-equivalent."""
    client = _client(graph_id=GRAPH_ID)  # parent = GRAPH_ID
    tool = DeleteSubgraphTool(client)
    result = await tool.execute(
      {"subgraph_id": "kgfffffffffffffffff_dev"}  # valid id, different parent
    )
    assert result["error"] == "authorization_failed"

  @pytest.mark.asyncio
  async def test_missing_user(self) -> None:
    client = _client()
    client.user = None
    result = await DeleteSubgraphTool(client).execute(
      {"subgraph_id": f"{GRAPH_ID}_dev"}
    )
    assert result["error"] == "authentication_required"

  @pytest.mark.asyncio
  async def test_subgraph_not_found(self, mock_db: MagicMock) -> None:
    mock_db.query.return_value.filter.return_value.first.return_value = None
    result = await DeleteSubgraphTool(_client()).execute(
      {"subgraph_id": f"{GRAPH_ID}_dev"}
    )
    assert result["error"] == "subgraph_not_found"

  @pytest.mark.asyncio
  async def test_not_admin_on_parent(self, mock_db: MagicMock) -> None:
    subgraph = MagicMock()
    subgraph.is_subgraph = True
    # First query(Graph).filter(...).first() → subgraph found
    # Second query(GraphUser).filter(...).first() → non-admin
    non_admin = MagicMock()
    non_admin.role = "member"
    filter_chain = MagicMock()
    filter_chain.first.side_effect = [subgraph, non_admin]
    mock_db.query.return_value.filter.return_value = filter_chain

    result = await DeleteSubgraphTool(_client()).execute(
      {"subgraph_id": f"{GRAPH_ID}_dev"}
    )
    assert result["error"] == "insufficient_permissions"

  @pytest.mark.asyncio
  async def test_happy_path(self, mock_db: MagicMock) -> None:
    subgraph = MagicMock()
    subgraph.is_subgraph = True
    admin = MagicMock()
    admin.role = "admin"
    filter_chain = MagicMock()
    filter_chain.first.side_effect = [subgraph, admin]
    mock_db.query.return_value.filter.return_value = filter_chain

    with patch(
      "robosystems.operations.graph.subgraph_service.SubgraphService"
    ) as mock_svc_class:
      mock_svc = AsyncMock()
      mock_svc_class.return_value = mock_svc

      result = await DeleteSubgraphTool(_client()).execute(
        {"subgraph_id": f"{GRAPH_ID}_dev", "force": True, "backup_first": True}
      )

    assert result == {
      "deleted": True,
      "subgraph_id": f"{GRAPH_ID}_dev",
      "backup_created": True,
    }
    mock_svc.delete_subgraph_database.assert_awaited_once()
    mock_db.delete.assert_called_once_with(subgraph)
    mock_db.commit.assert_called_once()


# ══════════════════════════════════════════════════════════════════════════
# ListSubgraphsTool
# ══════════════════════════════════════════════════════════════════════════


class TestListSubgraphsExecute:
  def test_definition(self) -> None:
    assert (
      ListSubgraphsTool(_client()).get_tool_definition()["name"] == "list-subgraphs"
    )

  @pytest.mark.asyncio
  async def test_primary_plus_subgraphs(self, mock_db: MagicMock) -> None:
    parent = MagicMock()
    parent.graph_name = "Main"
    sg = MagicMock()
    sg.graph_id = f"{GRAPH_ID}_dev"
    sg.subgraph_name = "dev"
    sg.graph_name = "Development"
    sg.created_at = datetime(2025, 4, 1, tzinfo=UTC)

    # query(Graph).filter(...).order_by(...).all() for subgraphs
    subgraph_q = MagicMock()
    subgraph_q.filter.return_value.order_by.return_value.all.return_value = [sg]
    # query(Graph).filter(...).first() for parent
    parent_q = MagicMock()
    parent_q.filter.return_value.first.return_value = parent

    mock_db.query.side_effect = [subgraph_q, parent_q]

    result = await ListSubgraphsTool(_client()).execute({})

    assert result["primary_graph_id"] == GRAPH_ID
    assert result["total_subgraphs"] == 1
    names = [entry["subgraph_id"] for entry in result["subgraphs"]]
    assert GRAPH_ID in names  # primary row
    assert f"{GRAPH_ID}_dev" in names


# ══════════════════════════════════════════════════════════════════════════
# MaterializeTool
# ══════════════════════════════════════════════════════════════════════════


class TestMaterializeExecute:
  def test_definition(self) -> None:
    defn = MaterializeTool(_client()).get_tool_definition()
    assert defn["name"] == "materialize"
    assert "dry_run" in defn["inputSchema"]["properties"]

  @pytest.mark.asyncio
  async def test_authentication_required(self) -> None:
    client = _client()
    client.user = None
    result = await MaterializeTool(client).execute({})
    assert result["error"] == "authentication_required"

  @pytest.mark.asyncio
  async def test_invalid_arguments_caught(self) -> None:
    """Unknown enum value for `source` must return an error envelope
    rather than a Pydantic trace."""
    result = await MaterializeTool(_client()).execute({"source": "bogus"})
    assert result["error"] == "invalid_arguments"

  @pytest.mark.asyncio
  async def test_happy_path_delegates_to_cmd(self, mock_db: MagicMock) -> None:
    with patch(
      "robosystems.operations.graph.commands.materialize.materialize_cmd",
      new=AsyncMock(),
    ) as mock_cmd:
      mock_cmd.return_value = {"status": "pending", "operation_id": "op_abc"}
      result = await MaterializeTool(_client()).execute(
        {"rebuild": True, "ignore_errors": True}
      )
    assert result["status"] == "pending"
    assert result["operation_id"] == "op_abc"
    mock_cmd.assert_awaited_once()

  @pytest.mark.asyncio
  async def test_http_exception_from_cmd_translated(self, mock_db: MagicMock) -> None:
    from fastapi import HTTPException

    with patch(
      "robosystems.operations.graph.commands.materialize.materialize_cmd",
      new=AsyncMock(side_effect=HTTPException(status_code=413, detail="too much data")),
    ):
      result = await MaterializeTool(_client()).execute({})
    assert result["error"] == "materialize_rejected"
    assert result["status"] == 413
    assert "too much data" in result["message"]


# ══════════════════════════════════════════════════════════════════════════
# CreateBackupTool
# ══════════════════════════════════════════════════════════════════════════


class TestCreateBackupExecute:
  def test_definition(self) -> None:
    defn = CreateBackupTool(_client()).get_tool_definition()
    assert defn["name"] == "create-backup"

  @pytest.mark.asyncio
  async def test_authentication_required(self) -> None:
    client = _client()
    client.user = None
    result = await CreateBackupTool(client).execute({})
    assert result["error"] == "authentication_required"

  @pytest.mark.asyncio
  async def test_feature_flag_off_returns_disabled(self, mock_db: MagicMock) -> None:
    from robosystems.config import env as env_module

    with patch.object(env_module, "BACKUP_CREATION_ENABLED", False):
      result = await CreateBackupTool(_client()).execute({})
    assert result["error"] == "backup_disabled"

  @pytest.mark.asyncio
  async def test_non_admin_rejected(self, mock_db: MagicMock) -> None:
    from robosystems.config import env as env_module

    non_admin = MagicMock()
    non_admin.role = "member"
    mock_db.query.return_value.filter.return_value.first.return_value = non_admin

    with patch.object(env_module, "BACKUP_CREATION_ENABLED", True):
      result = await CreateBackupTool(_client()).execute({})

    assert result["error"] == "insufficient_permissions"

  @pytest.mark.asyncio
  async def test_happy_path_enqueues_dagster_job(self, mock_db: MagicMock) -> None:
    from robosystems.config import env as env_module

    admin = MagicMock()
    admin.role = "admin"
    mock_db.query.return_value.filter.return_value.first.return_value = admin

    graph = MagicMock()
    graph.graph_tier = "ladybug-standard"

    with (
      patch.object(env_module, "BACKUP_CREATION_ENABLED", True),
      patch(
        "robosystems.models.core.Graph.get_by_id",
        return_value=graph,
      ),
      patch(
        "robosystems.config.graph_tier.GraphTierConfig.get_backup_limits",
        return_value={"backup_retention_days": 30},
      ),
      patch(
        "robosystems.middleware.sse.build_graph_job_config",
        return_value={"config": {}},
      ),
      patch(
        "robosystems.worker.client.enqueue_task",
        new=AsyncMock(return_value={"operation_id": "op_backup_01"}),
      ),
    ):
      result = await CreateBackupTool(_client()).execute(
        {"retention_days": 999, "encryption": True}
      )

    assert result["status"] == "accepted"
    assert result["operation_id"] == "op_backup_01"
    # Retention capped at the tier max (30), not the caller's 999
    assert result["retention_days"] == 30
    assert result["encryption"] is True


# ══════════════════════════════════════════════════════════════════════════
# GetGraphSyncStatusTool
# ══════════════════════════════════════════════════════════════════════════


class TestGetGraphSyncStatusExecute:
  def test_definition(self) -> None:
    assert (
      GetGraphSyncStatusTool(_client()).get_tool_definition()["name"]
      == "get-graph-sync-status"
    )

  @pytest.mark.asyncio
  async def test_graph_not_found(self, mock_db: MagicMock) -> None:
    with patch("robosystems.models.core.Graph.get_by_id", return_value=None):
      result = await GetGraphSyncStatusTool(_client()).execute({})
    assert result["error"] == "graph_not_found"

  @pytest.mark.asyncio
  async def test_fresh_graph(self, mock_db: MagicMock) -> None:
    graph = MagicMock()
    graph.graph_stale = False
    graph.graph_stale_reason = None
    graph.graph_stale_at = None
    graph.graph_metadata = {
      "last_materialized_at": "2026-04-16T00:00:00Z",
      "materialization_count": 7,
    }
    with patch("robosystems.models.core.Graph.get_by_id", return_value=graph):
      result = await GetGraphSyncStatusTool(_client()).execute({})
    assert result["sync_status"] == "fresh"
    assert result["materialization_count"] == 7
    assert result["hours_since_materialization"] is not None

  @pytest.mark.asyncio
  async def test_stale_graph(self, mock_db: MagicMock) -> None:
    graph = MagicMock()
    graph.graph_stale = True
    graph.graph_stale_reason = "schedule_created"
    graph.graph_stale_at = datetime(2026, 4, 16, tzinfo=UTC)
    graph.graph_metadata = {}

    with patch("robosystems.models.core.Graph.get_by_id", return_value=graph):
      result = await GetGraphSyncStatusTool(_client()).execute({})

    assert result["sync_status"] == "stale"
    assert result["stale_reason"] == "schedule_created"
    assert result["stale_since"] is not None
    assert result["stale_duration_minutes"] is not None


# ══════════════════════════════════════════════════════════════════════════
# SwitchWorkspaceTool (client-side sentinel)
# ══════════════════════════════════════════════════════════════════════════


class TestSwitchWorkspace:
  def test_definition(self) -> None:
    defn = SwitchWorkspaceTool(_client()).get_tool_definition()
    assert defn["name"] == "switch-workspace"
    assert "workspace_id" in defn["inputSchema"]["required"]

  @pytest.mark.asyncio
  async def test_server_side_execute_is_a_no_op_error(self) -> None:
    """The MCP client should intercept before calling the server. If the
    server executes, we return an explicit error — this is load-bearing
    for the "client-side tool" contract documented in the description."""
    result = await SwitchWorkspaceTool(_client()).execute({"workspace_id": "primary"})
    assert result["error"] == "client_side_tool"
