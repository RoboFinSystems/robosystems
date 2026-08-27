"""Tests for the graph-lifecycle MCP tools.

Covers the six tools in `middleware/mcp/tools/graph_tools.py`:

1. `CreateSubgraphTool`
2. `DeleteSubgraphTool`
3. `ListSubgraphsTool`
4. `MaterializeTool`
5. `CreateBackupTool`
6. `GetGraphSyncStatusTool`

Plus the platform-DB
connection tools `SetWritePolicyTool` and `SyncConnectionTool`.

Shared-repo gate coverage (the primary defense-in-depth concern) lives
in `tests/routers/graphs/test_ops_shared_repo_gates.py`. This file
focuses on definition shape + non-repo error paths + happy path so the
tools are exercised beyond manager-level wiring.
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
  SetWritePolicyTool,
  SyncConnectionTool,
  _subgraph_credential_hint,
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


def _live_graph_row(graph_id: str = GRAPH_ID) -> MagicMock:
  """A `Graph` projection row as `GraphUser.get_effective_role` reads it —
  the resolver first checks the graph is live (exists, not deprovisioned, no
  `deleted_at`) before consulting the membership row."""
  row = MagicMock()
  row.graph_id = graph_id
  row.org_id = None
  row.status = "active"
  row.deleted_at = None
  return row


def _script_admin_role(mock_db: MagicMock, role: str = "admin") -> None:
  """Script the two queries `get_effective_role` runs against the fake
  session: a live Graph row (`.all()`), then the caller's GraphUser row
  (`.first()`)."""
  membership = MagicMock()
  membership.role = role
  chain = mock_db.query.return_value.filter.return_value
  chain.all.return_value = [_live_graph_row()]
  chain.first.return_value = membership


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

  # The tool mirrors the REST create-subgraph gates by calling the same
  # helpers from routers/graphs/subgraphs/utils.py; these tests patch those
  # helpers at their source module (the tool imports them lazily).
  UTILS = "robosystems.routers.graphs.subgraphs.utils"

  def _gates(self, parent: MagicMock):
    from contextlib import ExitStack

    stack = ExitStack()
    stack.enter_context(
      patch(f"{self.UTILS}.verify_parent_graph_access", return_value=parent)
    )
    stack.enter_context(patch(f"{self.UTILS}.verify_subgraph_tier_support"))
    stack.enter_context(patch(f"{self.UTILS}.verify_parent_graph_active"))
    stack.enter_context(
      patch(f"{self.UTILS}.check_subgraph_quota", return_value=(0, 3, []))
    )
    stack.enter_context(patch(f"{self.UTILS}.validate_subgraph_name_unique"))
    return stack

  @pytest.mark.asyncio
  async def test_parent_not_found(self, mock_db: MagicMock) -> None:
    from fastapi import HTTPException

    with patch(
      f"{self.UTILS}.verify_parent_graph_access",
      side_effect=HTTPException(status_code=404, detail="Graph not found."),
    ):
      result = await CreateSubgraphTool(_client()).execute({"name": "dev"})
    assert result["error"] == "parent_not_found"

  @pytest.mark.asyncio
  async def test_feature_flag_off_returns_disabled(self, mock_db: MagicMock) -> None:
    from robosystems.config import env as env_module

    with patch.object(env_module, "SUBGRAPH_CREATION_ENABLED", False):
      result = await CreateSubgraphTool(_client()).execute({"name": "dev"})
    assert result["error"] == "subgraph_creation_disabled"

  @pytest.mark.asyncio
  async def test_member_without_admin_rejected(self, mock_db: MagicMock) -> None:
    """REST requires admin on the parent (`verify_parent_graph_access(...,
    "admin")`); the tool asks the same helper for the same role, so a member
    who is not admin gets the REST answer over MCP too."""
    from fastapi import HTTPException

    with patch(
      f"{self.UTILS}.verify_parent_graph_access",
      side_effect=HTTPException(
        status_code=403,
        detail="Admin access to parent graph required for this operation",
      ),
    ) as gate:
      result = await CreateSubgraphTool(_client()).execute({"name": "dev"})
    assert result["error"] == "insufficient_permissions"
    assert gate.call_args.args[3] == "admin"

  @pytest.mark.asyncio
  async def test_lifecycle_and_tier_gates_are_mirrored(
    self, mock_db: MagicMock
  ) -> None:
    from fastapi import HTTPException

    parent = MagicMock()
    with (
      patch(f"{self.UTILS}.verify_parent_graph_access", return_value=parent),
      patch(
        f"{self.UTILS}.verify_subgraph_tier_support",
        side_effect=HTTPException(
          status_code=403, detail="Subgraphs are not available for this tier."
        ),
      ),
      patch(
        "robosystems.operations.graph.subgraph_service.SubgraphService"
      ) as mock_svc_class,
    ):
      result = await CreateSubgraphTool(_client()).execute({"name": "dev"})
    assert result["error"] == "subgraph_not_allowed"
    mock_svc_class.assert_not_called()

  @pytest.mark.asyncio
  async def test_malformed_name_from_gate_codes_as_invalid_name(
    self, mock_db: MagicMock
  ) -> None:
    """A 400 from a gate helper (e.g. the REST name validator) codes as
    invalid_name, not the subgraph_not_allowed default. Unreachable for real
    input — validate_subgraph_name() catches a bad name before the gates — but
    the mapping is kept exhaustive so the error code can't drift."""
    from fastapi import HTTPException

    parent = MagicMock()
    with self._gates(parent):
      with patch(
        f"{self.UTILS}.validate_subgraph_name_unique",
        side_effect=HTTPException(
          status_code=400,
          detail="Subgraph name must be alphanumeric and 1-20 characters",
        ),
      ):
        result = await CreateSubgraphTool(_client()).execute({"name": "dev"})
    assert result["error"] == "invalid_name"

  @pytest.mark.asyncio
  async def test_name_collision_reported(self, mock_db: MagicMock) -> None:
    from fastapi import HTTPException

    parent = MagicMock()
    with self._gates(parent):
      with patch(
        f"{self.UTILS}.validate_subgraph_name_unique",
        side_effect=HTTPException(
          status_code=409, detail="Subgraph name 'dev' already exists."
        ),
      ):
        result = await CreateSubgraphTool(_client()).execute({"name": "dev"})
    assert result["error"] == "name_taken"

  @pytest.mark.asyncio
  async def test_happy_path_delegates_to_subgraph_service(
    self, mock_db: MagicMock
  ) -> None:
    parent = MagicMock()

    with (
      self._gates(parent),
      patch(
        "robosystems.operations.graph.subgraph_service.SubgraphService"
      ) as mock_svc_class,
    ):
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
    filter_chain = MagicMock()
    filter_chain.first.side_effect = [subgraph]
    mock_db.query.return_value.filter.return_value = filter_chain

    with patch(
      "robosystems.models.core.graph.graph_user.GraphUser.user_has_admin_access",
      return_value=False,
    ):
      result = await DeleteSubgraphTool(_client()).execute(
        {"subgraph_id": f"{GRAPH_ID}_dev"}
      )
    assert result["error"] == "insufficient_permissions"

  @pytest.mark.asyncio
  async def test_happy_path(self, mock_db: MagicMock) -> None:
    subgraph = MagicMock()
    subgraph.is_subgraph = True
    filter_chain = MagicMock()
    filter_chain.first.side_effect = [subgraph]
    mock_db.query.return_value.filter.return_value = filter_chain

    with (
      patch(
        "robosystems.models.core.graph.graph_user.GraphUser.user_has_admin_access",
        return_value=True,
      ),
      patch(
        "robosystems.operations.graph.subgraph_service.SubgraphService"
      ) as mock_svc_class,
    ):
      mock_svc = AsyncMock()
      mock_svc.delete_subgraph_database.return_value = {
        "status": "deleted",
        "backup_created": True,
        "backup_location": "graph-backups/kg_x/dev.lbug.zip",
        "backup_id": "gbk_01",
      }
      mock_svc_class.return_value = mock_svc

      result = await DeleteSubgraphTool(_client()).execute(
        {"subgraph_id": f"{GRAPH_ID}_dev", "force": True, "backup_first": True}
      )

    assert result == {
      "deleted": True,
      "subgraph_id": f"{GRAPH_ID}_dev",
      "backup_created": True,
      "backup_location": "graph-backups/kg_x/dev.lbug.zip",
      "backup_id": "gbk_01",
    }
    mock_svc.delete_subgraph_database.assert_awaited_once()
    assert mock_svc.delete_subgraph_database.call_args.kwargs["create_backup"] is True
    mock_db.delete.assert_called_once_with(subgraph)
    mock_db.commit.assert_called_once()

  @pytest.mark.asyncio
  async def test_backup_created_reports_what_happened_not_what_was_asked(
    self, mock_db: MagicMock
  ) -> None:
    """The result used to echo the `backup_first` flag while no backup was
    taken. It now carries the service's own account of the backup."""
    subgraph = MagicMock()
    subgraph.is_subgraph = True
    filter_chain = MagicMock()
    filter_chain.first.side_effect = [subgraph]
    mock_db.query.return_value.filter.return_value = filter_chain

    with (
      patch(
        "robosystems.models.core.graph.graph_user.GraphUser.user_has_admin_access",
        return_value=True,
      ),
      patch(
        "robosystems.operations.graph.subgraph_service.SubgraphService"
      ) as mock_svc_class,
    ):
      mock_svc = AsyncMock()
      mock_svc.delete_subgraph_database.return_value = {
        "status": "deleted",
        "backup_created": False,
        "backup_location": None,
        "backup_id": None,
      }
      mock_svc_class.return_value = mock_svc

      result = await DeleteSubgraphTool(_client()).execute(
        {"subgraph_id": f"{GRAPH_ID}_dev", "force": True, "backup_first": True}
      )

    assert result["deleted"] is True
    assert result["backup_created"] is False
    assert result["backup_location"] is None


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
    # The carriage-specific credential answer rides on the result.
    assert "credential" in result


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
      result = await MaterializeTool(_client()).execute({"rebuild": True})
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

    _script_admin_role(mock_db, role="member")

    with patch.object(env_module, "BACKUP_CREATION_ENABLED", True):
      result = await CreateBackupTool(_client()).execute({})

    assert result["error"] == "insufficient_permissions"

  @pytest.mark.asyncio
  async def test_happy_path_enqueues_dagster_job(self, mock_db: MagicMock) -> None:
    from robosystems.config import env as env_module

    _script_admin_role(mock_db)

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
        return_value={"backup_retention_days": 30, "max_backups_per_day": 10},
      ),
      patch(
        "robosystems.models.core.GraphBackup.count_user_initiated_today",
        return_value=0,
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
      result = await CreateBackupTool(_client()).execute({"retention_days": 999})

    assert result["status"] == "accepted"
    assert result["operation_id"] == "op_backup_01"
    # Retention capped at the tier max (30), not the caller's 999
    assert result["retention_days"] == 30

  @pytest.mark.asyncio
  async def test_daily_limit_is_enforced_on_this_path_too(
    self, mock_db: MagicMock
  ) -> None:
    """The MCP path enqueues the same job, so the limit has to live here too.

    A quota enforced on only one of two entry points is not a quota — the same
    shape as the retention clamp this path was missing before #1108.
    """
    from robosystems.config import env as env_module

    _script_admin_role(mock_db)

    graph = MagicMock()
    graph.graph_tier = "ladybug-standard"

    with (
      patch.object(env_module, "BACKUP_CREATION_ENABLED", True),
      patch("robosystems.models.core.Graph.get_by_id", return_value=graph),
      patch(
        "robosystems.config.graph_tier.GraphTierConfig.get_backup_limits",
        return_value={"backup_retention_days": 7, "max_backups_per_day": 2},
      ),
      patch(
        "robosystems.models.core.GraphBackup.count_user_initiated_today",
        return_value=2,
      ),
    ):
      result = await CreateBackupTool(_client()).execute({})

    assert result["error"] == "daily_backup_limit_reached"
    assert "2/2" in result["message"]

  @pytest.mark.asyncio
  async def test_unlimited_tier_skips_the_limit(self, mock_db: MagicMock) -> None:
    """A negative limit means unlimited, not "zero allowed"."""
    from robosystems.config import env as env_module

    _script_admin_role(mock_db)

    graph = MagicMock()
    graph.graph_tier = "ladybug-xlarge"

    with (
      patch.object(env_module, "BACKUP_CREATION_ENABLED", True),
      patch("robosystems.models.core.Graph.get_by_id", return_value=graph),
      patch(
        "robosystems.config.graph_tier.GraphTierConfig.get_backup_limits",
        return_value={"backup_retention_days": 90, "max_backups_per_day": -1},
      ),
      patch(
        "robosystems.models.core.GraphBackup.count_user_initiated_today",
        return_value=500,
      ),
      patch(
        "robosystems.middleware.sse.build_graph_job_config",
        return_value={"config": {}},
      ),
      patch(
        "robosystems.worker.client.enqueue_task",
        new=AsyncMock(return_value={"operation_id": "op_backup_02"}),
      ),
    ):
      result = await CreateBackupTool(_client()).execute({})

    assert result["status"] == "accepted"


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
    mock_db.query.return_value.filter.return_value.all.return_value = []
    with patch("robosystems.models.core.Graph.get_by_id", return_value=graph):
      result = await GetGraphSyncStatusTool(_client()).execute({})
    assert result["sync_status"] == "fresh"
    assert result["materialization_count"] == 7
    assert result["hours_since_materialization"] is not None
    assert result["connections"] == []

  @pytest.mark.asyncio
  async def test_stale_graph(self, mock_db: MagicMock) -> None:
    graph = MagicMock()
    graph.graph_stale = True
    graph.graph_stale_reason = "schedule_created"
    graph.graph_stale_at = datetime(2026, 4, 16, tzinfo=UTC)
    graph.graph_metadata = {}
    mock_db.query.return_value.filter.return_value.all.return_value = []

    with patch("robosystems.models.core.Graph.get_by_id", return_value=graph):
      result = await GetGraphSyncStatusTool(_client()).execute({})

    assert result["sync_status"] == "stale"
    assert result["stale_reason"] == "schedule_created"
    assert result["stale_since"] is not None
    assert result["stale_duration_minutes"] is not None

  @pytest.mark.asyncio
  async def test_connection_health_and_last_sync_result_ride_along(
    self, mock_db: MagicMock
  ) -> None:
    """The source→OLTP edge is legible — connection status (incl.
    needs_reauth) and the last sync attempt's outcome summary."""
    graph = MagicMock()
    graph.graph_stale = False
    graph.graph_stale_reason = None
    graph.graph_stale_at = None
    graph.graph_metadata = {}

    conn = MagicMock()
    conn.id = "conn_qb1"
    conn.provider = "quickbooks"
    conn.status = "needs_reauth"
    conn.last_sync = datetime(2026, 8, 6, tzinfo=UTC)
    conn.last_sync_result = {
      "status": "succeeded",
      "counts": {"events_captured": 46, "reconciling_items": 11},
    }
    mock_db.query.return_value.filter.return_value.all.return_value = [conn]

    with patch("robosystems.models.core.Graph.get_by_id", return_value=graph):
      result = await GetGraphSyncStatusTool(_client()).execute({})

    assert result["connections"] == [
      {
        "connection_id": "conn_qb1",
        "provider": "quickbooks",
        "status": "needs_reauth",
        "last_sync_at": "2026-08-06T00:00:00+00:00",
        "last_sync_result": {
          "status": "succeeded",
          "counts": {"events_captured": 46, "reconciling_items": 11},
        },
      }
    ]


# ══════════════════════════════════════════════════════════════════════════
# ResolveSubgraphTool (client-side sentinel)
# ══════════════════════════════════════════════════════════════════════════


# ══════════════════════════════════════════════════════════════════════════
# SetWritePolicyTool
# ══════════════════════════════════════════════════════════════════════════

CONN_MODULE = "robosystems.operations.connection_service"


class TestSetWritePolicyDefinition:
  def test_definition_shape(self) -> None:
    defn = SetWritePolicyTool(_client()).get_tool_definition()
    assert defn["name"] == "set-write-policy"
    required = defn["inputSchema"]["required"]
    assert "connection_id" in required
    assert "write_policy" in required
    assert defn["inputSchema"]["properties"]["write_policy"]["enum"] == [
      "native",
      "qb_authoritative",
    ]


class TestSetWritePolicyExecute:
  @pytest.mark.asyncio
  async def test_missing_user(self) -> None:
    client = _client()
    client.user = None
    result = await SetWritePolicyTool(client).execute(
      {"connection_id": "conn_1", "write_policy": "native"}
    )
    assert result["error"] == "authentication_required"

  @pytest.mark.asyncio
  async def test_invalid_policy_rejected_pre_service(self) -> None:
    result = await SetWritePolicyTool(_client()).execute(
      {"connection_id": "conn_1", "write_policy": "hybrid"}
    )
    assert result["error"] == "invalid_arguments"

  @pytest.mark.asyncio
  async def test_missing_connection_id(self) -> None:
    result = await SetWritePolicyTool(_client()).execute({"write_policy": "native"})
    assert result["error"] == "invalid_arguments"

  @pytest.mark.asyncio
  async def test_happy_path_delegates_to_service(self) -> None:
    updated = {
      "connection_id": "conn_1",
      "provider": "quickbooks",
      "status": "connected",
      "write_policy": "qb_authoritative",
    }
    with patch(
      f"{CONN_MODULE}.ConnectionService.set_write_policy",
      new=AsyncMock(return_value=updated),
    ) as mock_set:
      result = await SetWritePolicyTool(_client()).execute(
        {"connection_id": "conn_1", "write_policy": "qb_authoritative"}
      )

    assert result["write_policy"] == "qb_authoritative"
    assert result["connection_id"] == "conn_1"
    # graph_id from the client is passed through for scoping.
    assert mock_set.call_args.kwargs["graph_id"] == GRAPH_ID
    assert mock_set.call_args.kwargs["connection_id"] == "conn_1"

  @pytest.mark.asyncio
  async def test_connection_not_found(self) -> None:
    with patch(
      f"{CONN_MODULE}.ConnectionService.set_write_policy",
      new=AsyncMock(return_value=None),
    ):
      result = await SetWritePolicyTool(_client()).execute(
        {"connection_id": "conn_x", "write_policy": "native"}
      )
    assert result["error"] == "connection_not_found"


# ══════════════════════════════════════════════════════════════════════════
# SyncConnectionTool
# ══════════════════════════════════════════════════════════════════════════

_DISPATCH_RESULT = {
  "connection_id": "conn_1",
  "provider": "quickbooks",
  "dispatched": True,
  "task_id": "run_123",
  "message": None,
  "full_rebuild": False,
  "since_date": None,
  "last_sync_before_dispatch": "2026-07-31T00:00:00+00:00",
}

_NO_OP_DISPATCH_RESULT = {
  "connection_id": "conn_1",
  "provider": "external",
  "dispatched": False,
  "task_id": None,
  "message": "External connections are push-based; there is nothing to sync.",
  "full_rebuild": False,
  "since_date": None,
  "last_sync_before_dispatch": None,
}


class TestSyncConnectionDefinition:
  def test_definition_shape(self) -> None:
    defn = SyncConnectionTool(_client()).get_tool_definition()
    assert defn["name"] == "sync-connection"
    props = defn["inputSchema"]["properties"]
    assert set(props) == {"connection_id", "provider", "full_rebuild", "since_date"}
    # No required params — connection auto-resolution is the default path.
    assert "required" not in defn["inputSchema"]
    assert props["full_rebuild"]["default"] is False


class TestSyncConnectionExecute:
  @pytest.mark.asyncio
  async def test_missing_user(self) -> None:
    client = _client()
    client.user = None
    result = await SyncConnectionTool(client).execute({})
    assert result["error"] == "authentication_required"

  @pytest.mark.asyncio
  async def test_invalid_since_date(self) -> None:
    result = await SyncConnectionTool(_client()).execute({"since_date": "07/01/2026"})
    assert result["error"] == "invalid_arguments"

  @pytest.mark.asyncio
  async def test_auto_resolves_single_connection(self) -> None:
    with (
      patch(
        f"{CONN_MODULE}.resolve_sync_connection",
        new=AsyncMock(return_value={"connection_id": "conn_1"}),
      ) as mock_resolve,
      patch(
        f"{CONN_MODULE}.dispatch_connection_sync",
        new=AsyncMock(return_value=_DISPATCH_RESULT),
      ) as mock_dispatch,
    ):
      result = await SyncConnectionTool(_client()).execute({})

    assert result["status"] == "accepted"
    assert result["task_id"] == "run_123"
    assert mock_resolve.call_args.args[0] == GRAPH_ID
    assert mock_dispatch.call_args.kwargs["connection_id"] == "conn_1"
    assert mock_dispatch.call_args.kwargs["full_rebuild"] is False

  @pytest.mark.asyncio
  async def test_explicit_connection_id_skips_resolution(self) -> None:
    with (
      patch(
        f"{CONN_MODULE}.resolve_sync_connection",
        new=AsyncMock(),
      ) as mock_resolve,
      patch(
        f"{CONN_MODULE}.dispatch_connection_sync",
        new=AsyncMock(return_value=_DISPATCH_RESULT),
      ) as mock_dispatch,
    ):
      result = await SyncConnectionTool(_client()).execute(
        {"connection_id": "conn_9", "full_rebuild": True, "since_date": "2026-07-01"}
      )

    assert result["status"] == "accepted"
    mock_resolve.assert_not_called()
    kwargs = mock_dispatch.call_args.kwargs
    assert kwargs["connection_id"] == "conn_9"
    assert kwargs["full_rebuild"] is True
    assert kwargs["since_date"].isoformat() == "2026-07-01"

  @pytest.mark.asyncio
  async def test_no_op_provider_is_not_reported_as_accepted(self) -> None:
    """A provider that dispatched nothing must not tell the operator to poll.

    `accepted` plus the polling instruction would send the agent into a wait
    on a run that was never started.
    """
    with patch(
      f"{CONN_MODULE}.dispatch_connection_sync",
      new=AsyncMock(return_value=_NO_OP_DISPATCH_RESULT),
    ):
      result = await SyncConnectionTool(_client()).execute({"connection_id": "conn_1"})

    assert result["status"] == "no_op"
    assert result["task_id"] is None
    assert "nothing to poll for" in result["message"]

  @pytest.mark.asyncio
  async def test_ambiguous_resolution_lists_candidates(self) -> None:
    from robosystems.operations.connection_service import (
      AmbiguousSyncConnectionError,
    )

    candidates = [
      {"connection_id": "conn_1", "provider": "quickbooks", "status": "connected"},
      {"connection_id": "conn_2", "provider": "external", "status": "connected"},
    ]
    with patch(
      f"{CONN_MODULE}.resolve_sync_connection",
      new=AsyncMock(side_effect=AmbiguousSyncConnectionError(candidates)),
    ):
      result = await SyncConnectionTool(_client()).execute({})

    assert result["error"] == "ambiguous_sync_connection"
    assert result["candidates"] == candidates

  @pytest.mark.asyncio
  async def test_no_sync_connection(self) -> None:
    from robosystems.operations.connection_service import NoSyncConnectionError

    with patch(
      f"{CONN_MODULE}.resolve_sync_connection",
      new=AsyncMock(side_effect=NoSyncConnectionError("none")),
    ):
      result = await SyncConnectionTool(_client()).execute({})
    assert result["error"] == "no_sync_connection"

  @pytest.mark.asyncio
  async def test_sync_in_progress_surfaces_holder(self) -> None:
    from robosystems.operations.connection_service import SyncInProgressError

    with patch(
      f"{CONN_MODULE}.dispatch_connection_sync",
      new=AsyncMock(side_effect=SyncInProgressError("conn_1", "holder_abc", 1200)),
    ):
      result = await SyncConnectionTool(_client()).execute({"connection_id": "conn_1"})

    assert result["error"] == "sync_in_progress"
    assert result["holder_id"] == "holder_abc"
    assert result["ttl_remaining_seconds"] == 1200


# ══════════════════════════════════════════════════════════════════════════
# Subgraph credential hint — carriage-aware
# ══════════════════════════════════════════════════════════════════════════


class TestSubgraphCredentialHint:
  """A graph-scoped key covers the parent's subgraphs; an OAuth grant is
  bound to exactly one URL. The hint must say which applies to the calling
  connector — "reuse your key" sends an OAuth connector into a 401-and-
  refresh loop at the subgraph."""

  @staticmethod
  def _hint_as(auth_method: str | None) -> str:
    from robosystems.security.request_context import (
      RequestPrincipal,
      bind_principal,
      reset_principal,
    )

    principal = (
      RequestPrincipal(user_id="u1", auth_method=auth_method) if auth_method else None
    )
    token = bind_principal(principal)
    try:
      return _subgraph_credential_hint(GRAPH_ID)
    finally:
      reset_principal(token)

  def test_key_carriage_reuses_the_key(self) -> None:
    hint = self._hint_as("api_key")
    assert "Reuse this connector's API key" in hint
    assert GRAPH_ID in hint

  def test_oauth_carriage_authorizes_the_new_url(self) -> None:
    hint = self._hint_as("oauth")
    assert "bound to exactly one URL" in hint
    assert "authorize it" in hint
    assert "Reuse" not in hint

  def test_no_principal_defaults_to_the_key_answer(self) -> None:
    assert "Reuse this connector's API key" in self._hint_as(None)
