"""Tests for MCP workspace and data operation tools."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.data_tools import BuildFactGridTool
from robosystems.middleware.mcp.tools.workspace import (
  CreateWorkspaceTool,
  DeleteWorkspaceTool,
  ListWorkspacesTool,
  SwitchWorkspaceTool,
)


@pytest.fixture
def mock_graph_client():
  """Mock GraphMCPClient for tool initialization."""
  client = MagicMock()
  client.graph_id = "kg1234567890abcdef"
  client.user = MagicMock()
  client.user.id = "user123"
  return client


@pytest.fixture
def mock_db_session():
  """Mock database session."""
  with patch("robosystems.database.get_db_session") as mock:
    session = MagicMock()
    mock.return_value = iter([session])
    yield session


@pytest.fixture
def mock_graph_model():
  """Mock Graph model."""
  graph = MagicMock()
  graph.graph_id = "kg1234567890abcdef"
  graph.graph_name = "Test Graph"
  graph.parent_graph_id = None
  graph.created_at = datetime(2025, 1, 1, tzinfo=UTC)
  return graph


@pytest.fixture
def mock_subgraph_model():
  """Mock subgraph Graph model."""
  subgraph = MagicMock()
  subgraph.graph_id = "kg1234567890abcdef_dev"
  subgraph.graph_name = "dev"
  subgraph.parent_graph_id = "kg1234567890abcdef"
  subgraph.created_at = datetime(2025, 1, 2, tzinfo=UTC)
  return subgraph


class TestCreateWorkspaceTool:
  """Tests for CreateWorkspaceTool."""

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_create_workspace_success(
    self, mock_graph_client, mock_db_session, mock_graph_model
  ):
    """Test successful workspace creation."""
    tool = CreateWorkspaceTool(mock_graph_client)

    # Mock database query
    mock_db_session.query.return_value.filter.return_value.first.return_value = (
      mock_graph_model
    )

    # Mock SubgraphService
    with patch(
      "robosystems.middleware.mcp.tools.workspace.SubgraphService"
    ) as mock_service_class:
      mock_service = AsyncMock()
      mock_service_class.return_value = mock_service
      mock_service.create_subgraph.return_value = {"graph_id": "kg1234567890abcdef_dev"}

      result = await tool.execute(
        {"name": "dev", "description": "Development workspace", "fork_parent": False}
      )

    assert result["success"] is True
    assert result["workspace_id"] == "kg1234567890abcdef_dev"
    assert result["name"] == "dev"
    mock_service.create_subgraph.assert_called_once()

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_create_workspace_with_fork(
    self, mock_graph_client, mock_db_session, mock_graph_model
  ):
    """Test workspace creation with parent fork."""
    tool = CreateWorkspaceTool(mock_graph_client)

    mock_db_session.query.return_value.filter.return_value.first.return_value = (
      mock_graph_model
    )

    with patch(
      "robosystems.middleware.mcp.tools.workspace.SubgraphService"
    ) as mock_service_class:
      mock_service = AsyncMock()
      mock_service_class.return_value = mock_service
      mock_service.create_subgraph.return_value = {
        "graph_id": "kg1234567890abcdef_staging"
      }

      result = await tool.execute(
        {
          "name": "staging",
          "description": "Staging workspace",
          "fork_parent": True,
        }
      )

    assert result["success"] is True
    assert result["forked_from_parent"] is True
    call_args = mock_service.create_subgraph.call_args[1]
    assert call_args["fork_parent"] is True

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_create_workspace_parent_not_found(
    self, mock_graph_client, mock_db_session
  ):
    """Test workspace creation when parent graph not found."""
    tool = CreateWorkspaceTool(mock_graph_client)

    mock_db_session.query.return_value.filter.return_value.first.return_value = None

    result = await tool.execute({"name": "dev"})

    assert result["error"] == "parent_not_found"
    assert "not found" in result["message"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_create_workspace_invalid_name_format(self, mock_graph_client):
    """Test workspace creation with invalid name format."""
    tool = CreateWorkspaceTool(mock_graph_client)

    result = await tool.execute({"name": "dev-test"})

    assert result["error"] == "invalid_name"
    assert "alphanumeric" in result["message"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_create_workspace_name_too_long(self, mock_graph_client):
    """Test workspace creation with name too long."""
    tool = CreateWorkspaceTool(mock_graph_client)

    result = await tool.execute({"name": "a" * 21})

    assert result["error"] == "invalid_name"
    assert "1-20 characters" in result["message"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_create_workspace_service_error(
    self, mock_graph_client, mock_db_session, mock_graph_model
  ):
    """Test workspace creation when SubgraphService fails."""
    tool = CreateWorkspaceTool(mock_graph_client)

    mock_db_session.query.return_value.filter.return_value.first.return_value = (
      mock_graph_model
    )

    with patch(
      "robosystems.middleware.mcp.tools.workspace.SubgraphService"
    ) as mock_service_class:
      mock_service = AsyncMock()
      mock_service_class.return_value = mock_service
      mock_service.create_subgraph.side_effect = Exception("Database error")

      result = await tool.execute({"name": "dev"})

    assert result["error"] == "creation_failed"
    assert "Database error" in result["message"]


class TestDeleteWorkspaceTool:
  """Tests for DeleteWorkspaceTool."""

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_delete_workspace_success(
    self, mock_graph_client, mock_db_session, mock_subgraph_model
  ):
    """Test successful workspace deletion."""
    tool = DeleteWorkspaceTool(mock_graph_client)

    # Mock subgraph_info
    from robosystems.middleware.graph.utils import SubgraphInfo

    mock_subgraph_info = SubgraphInfo(
      graph_id="kg1234567890abcdef_dev",
      parent_graph_id="kg1234567890abcdef",
      subgraph_name="dev",
      database_name="kg1234567890abcdef_dev",
    )

    # Mock is_subgraph property
    mock_subgraph_model.is_subgraph = True

    # Mock GraphUser for authorization
    mock_user_graph = MagicMock()
    mock_user_graph.role = "admin"

    # Setup query mock to return different values on consecutive calls
    # First call: query Graph for workspace
    # Second call: query GraphUser for authorization
    mock_graph_query = MagicMock()
    mock_graph_query.filter.return_value.first.return_value = mock_subgraph_model

    mock_user_query = MagicMock()
    mock_user_query.filter.return_value.first.return_value = mock_user_graph

    # Setup multiple database sessions (authorization check + deletion)
    # First session: for authorization checks (2 queries)
    mock_db_session.query.side_effect = [mock_graph_query, mock_user_query]

    # Second session: for deletion from PostgreSQL
    mock_db_session2 = MagicMock()
    mock_delete_query = MagicMock()
    mock_delete_query.filter.return_value.first.return_value = mock_subgraph_model
    mock_db_session2.query.return_value = mock_delete_query

    with (
      patch(
        "robosystems.middleware.mcp.tools.workspace.SubgraphService"
      ) as mock_service_class,
      patch(
        "robosystems.middleware.graph.utils.parse_subgraph_id",
        return_value=mock_subgraph_info,
      ),
      patch("robosystems.database.get_db_session") as mock_get_db,
    ):
      # Mock get_db_session to return two different sessions
      mock_get_db.side_effect = [iter([mock_db_session]), iter([mock_db_session2])]

      mock_service = AsyncMock()
      mock_service_class.return_value = mock_service

      result = await tool.execute(
        {"workspace_id": "kg1234567890abcdef_dev", "force": False}
      )

    assert result["success"] is True
    assert result["deleted"] == "kg1234567890abcdef_dev"
    mock_service.delete_subgraph_database.assert_called_once_with(
      subgraph_id="kg1234567890abcdef_dev", force=False, create_backup=False
    )

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_delete_workspace_with_force(
    self, mock_graph_client, mock_db_session, mock_subgraph_model
  ):
    """Test workspace deletion with force flag."""
    tool = DeleteWorkspaceTool(mock_graph_client)

    # Mock subgraph_info
    from robosystems.middleware.graph.utils import SubgraphInfo

    mock_subgraph_info = SubgraphInfo(
      graph_id="kg1234567890abcdef_dev",
      parent_graph_id="kg1234567890abcdef",
      subgraph_name="dev",
      database_name="kg1234567890abcdef_dev",
    )

    # Mock is_subgraph property
    mock_subgraph_model.is_subgraph = True

    # Mock GraphUser for authorization
    mock_user_graph = MagicMock()
    mock_user_graph.role = "admin"

    # Setup query mock to return different values on consecutive calls
    # First call: query Graph for workspace
    # Second call: query GraphUser for authorization
    mock_graph_query = MagicMock()
    mock_graph_query.filter.return_value.first.return_value = mock_subgraph_model

    mock_user_query = MagicMock()
    mock_user_query.filter.return_value.first.return_value = mock_user_graph

    # Setup multiple database sessions (authorization check + deletion)
    # First session: for authorization checks (2 queries)
    mock_db_session.query.side_effect = [mock_graph_query, mock_user_query]

    # Second session: for deletion from PostgreSQL
    mock_db_session2 = MagicMock()
    mock_delete_query = MagicMock()
    mock_delete_query.filter.return_value.first.return_value = mock_subgraph_model
    mock_db_session2.query.return_value = mock_delete_query

    with (
      patch(
        "robosystems.middleware.mcp.tools.workspace.SubgraphService"
      ) as mock_service_class,
      patch(
        "robosystems.middleware.graph.utils.parse_subgraph_id",
        return_value=mock_subgraph_info,
      ),
      patch("robosystems.database.get_db_session") as mock_get_db,
    ):
      # Mock get_db_session to return two different sessions
      mock_get_db.side_effect = [iter([mock_db_session]), iter([mock_db_session2])]

      mock_service = AsyncMock()
      mock_service_class.return_value = mock_service

      result = await tool.execute(
        {"workspace_id": "kg1234567890abcdef_dev", "force": True}
      )

    assert result["success"] is True
    call_args = mock_service.delete_subgraph_database.call_args[1]
    assert call_args["force"] is True

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_delete_workspace_is_primary_graph(self, mock_graph_client):
    """Test deletion fails with invalid format when trying to delete primary graph."""
    tool = DeleteWorkspaceTool(mock_graph_client)

    result = await tool.execute({"workspace_id": "kg1234567890abcdef"})

    assert result["error"] == "invalid_workspace_id"
    assert "Invalid workspace ID format" in result["message"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_delete_workspace_invalid_format(self, mock_graph_client):
    """Test deletion fails with invalid workspace ID format."""
    tool = DeleteWorkspaceTool(mock_graph_client)

    result = await tool.execute({"workspace_id": "invalid"})

    assert result["error"] == "invalid_workspace_id"
    assert "format" in result["message"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_delete_workspace_not_found(self, mock_graph_client, mock_db_session):
    """Test deletion fails when workspace not found."""
    tool = DeleteWorkspaceTool(mock_graph_client)

    # Mock parse_subgraph_id to return valid info
    from robosystems.middleware.graph.utils import SubgraphInfo

    mock_subgraph_info = SubgraphInfo(
      graph_id="kg1234567890abcdef_missing",
      parent_graph_id="kg1234567890abcdef",
      subgraph_name="missing",
      database_name="kg1234567890abcdef_missing",
    )

    # Mock database query to return None (workspace not found)
    mock_graph_query = MagicMock()
    mock_graph_query.filter.return_value.first.return_value = None
    mock_db_session.query.return_value = mock_graph_query

    with (
      patch(
        "robosystems.middleware.mcp.tools.workspace.SubgraphService"
      ) as mock_service_class,
      patch(
        "robosystems.middleware.graph.utils.parse_subgraph_id",
        return_value=mock_subgraph_info,
      ),
      patch(
        "robosystems.database.get_db_session",
        return_value=iter([mock_db_session]),
      ),
    ):
      mock_service = AsyncMock()
      mock_service_class.return_value = mock_service

      result = await tool.execute({"workspace_id": "kg1234567890abcdef_missing"})

    # Now returns workspace_not_found instead of deletion_failed (improved error handling)
    assert result["error"] == "workspace_not_found"
    assert "not found" in result["message"].lower()


class TestListWorkspacesTool:
  """Tests for ListWorkspacesTool."""

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_list_workspaces_includes_primary(
    self, mock_graph_client, mock_db_session, mock_graph_model
  ):
    """Test listing workspaces includes primary graph."""
    tool = ListWorkspacesTool(mock_graph_client)

    mock_db_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
    mock_db_session.query.return_value.filter.return_value.first.return_value = (
      mock_graph_model
    )

    result = await tool.execute({})

    assert len(result["workspaces"]) == 1
    assert result["workspaces"][0]["type"] == "primary"
    assert result["workspaces"][0]["workspace_id"] == "kg1234567890abcdef"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_list_workspaces_includes_subgraphs(
    self, mock_graph_client, mock_db_session, mock_graph_model, mock_subgraph_model
  ):
    """Test listing workspaces includes subgraphs."""
    tool = ListWorkspacesTool(mock_graph_client)

    mock_db_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [
      mock_subgraph_model
    ]
    mock_db_session.query.return_value.filter.return_value.first.return_value = (
      mock_graph_model
    )

    result = await tool.execute({})

    assert len(result["workspaces"]) == 2
    assert result["workspaces"][0]["type"] == "primary"
    assert result["workspaces"][1]["type"] == "workspace"
    assert result["workspaces"][1]["workspace_id"] == "kg1234567890abcdef_dev"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_list_workspaces_parent_not_found(
    self, mock_graph_client, mock_db_session
  ):
    """Test listing workspaces when parent not found (returns generic description)."""
    tool = ListWorkspacesTool(mock_graph_client)

    mock_db_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
    mock_db_session.query.return_value.filter.return_value.first.return_value = None

    result = await tool.execute({})

    assert len(result["workspaces"]) == 1
    assert result["workspaces"][0]["type"] == "primary"
    assert result["workspaces"][0]["description"] == "Primary graph"


class TestSwitchWorkspaceTool:
  """Tests for SwitchWorkspaceTool."""

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_switch_workspace_returns_client_side_error(self, mock_graph_client):
    """Test that switch-workspace returns client-side error (tool is client-only)."""
    tool = SwitchWorkspaceTool(mock_graph_client)

    result = await tool.execute({"workspace_id": "kg1234567890abcdef_dev"})

    assert result["error"] == "client_side_tool"
    assert "client-side" in result["message"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_switch_to_primary_returns_client_side_error(self, mock_graph_client):
    """Test that switch to primary also returns client-side error."""
    tool = SwitchWorkspaceTool(mock_graph_client)

    result = await tool.execute({"workspace_id": "kg1234567890abcdef"})

    assert result["error"] == "client_side_tool"
    assert "MCP client" in result["message"]


class TestBuildFactGridTool:
  """Tests for BuildFactGridTool."""

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_build_fact_grid_success(self, mock_graph_client):
    """Test successful fact grid building."""
    tool = BuildFactGridTool(mock_graph_client)

    with patch(
      "robosystems.operations.roboledger.views.fact_grid_builder.FactGridBuilder"
    ) as mock_builder_class:
      mock_graph_client.execute_query = AsyncMock(
        return_value=[
          {
            "element_id": "us-gaap:Assets",
            "period_end": "2023-12-31",
            "value": 1000000,
          }
        ]
      )

      mock_builder = MagicMock()
      mock_builder_class.return_value = mock_builder
      mock_grid = MagicMock()
      mock_grid.metadata.fact_count = 1
      mock_grid.metadata.dimension_count = 2
      mock_grid.metadata.construction_time_ms = 50
      mock_grid.dimensions = []
      mock_builder.build.return_value = mock_grid

      result = await tool.execute(
        {"elements": ["us-gaap:Assets"], "periods": ["2023-12-31"]}
      )

    assert result["success"] is True
    assert result["fact_count"] == 1
    assert result["dimension_count"] == 2

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_build_fact_grid_missing_elements(self, mock_graph_client):
    """Test fact grid building fails without elements."""
    tool = BuildFactGridTool(mock_graph_client)

    result = await tool.execute({"periods": ["2023-12-31"]})

    assert result["error"] == "missing_elements"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_build_fact_grid_missing_period_filter(self, mock_graph_client):
    """Test fact grid building fails without any period scoping."""
    tool = BuildFactGridTool(mock_graph_client)

    result = await tool.execute({"elements": ["us-gaap:Assets"]})

    assert result["error"] == "missing_period_filter"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_build_fact_grid_query_failure(self, mock_graph_client):
    """Test fact grid building handles query errors."""
    tool = BuildFactGridTool(mock_graph_client)

    mock_graph_client.execute_query = AsyncMock(side_effect=Exception("Query failed"))

    result = await tool.execute(
      {"elements": ["us-gaap:Assets"], "periods": ["2023-12-31"]}
    )

    assert result["error"] == "construction_failed"
    assert "Query failed" in result["message"]
