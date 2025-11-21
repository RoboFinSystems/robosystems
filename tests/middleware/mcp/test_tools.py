"""Tests for MCP workspace and data operation tools."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone

from robosystems.middleware.mcp.tools.workspace import (
  CreateWorkspaceTool,
  DeleteWorkspaceTool,
  ListWorkspacesTool,
  SwitchWorkspaceTool,
)
from robosystems.middleware.mcp.tools.data_tools import (
  BuildFactGridTool,
  QueryStagingTool,
  MaterializeGraphTool,
  MapElementsTool,
  IngestFileTool,
)


@pytest.fixture
def mock_kuzu_client():
  """Mock KuzuMCPClient for tool initialization."""
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
  graph.created_at = datetime(2025, 1, 1, tzinfo=timezone.utc)
  return graph


@pytest.fixture
def mock_subgraph_model():
  """Mock subgraph Graph model."""
  subgraph = MagicMock()
  subgraph.graph_id = "kg1234567890abcdef_dev"
  subgraph.graph_name = "dev"
  subgraph.parent_graph_id = "kg1234567890abcdef"
  subgraph.created_at = datetime(2025, 1, 2, tzinfo=timezone.utc)
  return subgraph


class TestCreateWorkspaceTool:
  """Tests for CreateWorkspaceTool."""

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_create_workspace_success(
    self, mock_kuzu_client, mock_db_session, mock_graph_model
  ):
    """Test successful workspace creation."""
    tool = CreateWorkspaceTool(mock_kuzu_client)

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
    self, mock_kuzu_client, mock_db_session, mock_graph_model
  ):
    """Test workspace creation with parent fork."""
    tool = CreateWorkspaceTool(mock_kuzu_client)

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
    self, mock_kuzu_client, mock_db_session
  ):
    """Test workspace creation when parent graph not found."""
    tool = CreateWorkspaceTool(mock_kuzu_client)

    mock_db_session.query.return_value.filter.return_value.first.return_value = None

    result = await tool.execute({"name": "dev"})

    assert result["error"] == "parent_not_found"
    assert "not found" in result["message"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_create_workspace_invalid_name_format(self, mock_kuzu_client):
    """Test workspace creation with invalid name format."""
    tool = CreateWorkspaceTool(mock_kuzu_client)

    result = await tool.execute({"name": "dev-test"})

    assert result["error"] == "invalid_name"
    assert "alphanumeric" in result["message"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_create_workspace_name_too_long(self, mock_kuzu_client):
    """Test workspace creation with name too long."""
    tool = CreateWorkspaceTool(mock_kuzu_client)

    result = await tool.execute({"name": "a" * 21})

    assert result["error"] == "invalid_name"
    assert "1-20 characters" in result["message"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_create_workspace_service_error(
    self, mock_kuzu_client, mock_db_session, mock_graph_model
  ):
    """Test workspace creation when SubgraphService fails."""
    tool = CreateWorkspaceTool(mock_kuzu_client)

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
    self, mock_kuzu_client, mock_db_session, mock_subgraph_model
  ):
    """Test successful workspace deletion."""
    tool = DeleteWorkspaceTool(mock_kuzu_client)

    mock_db_session.query.return_value.filter.return_value.first.return_value = (
      mock_subgraph_model
    )

    with patch(
      "robosystems.middleware.mcp.tools.workspace.SubgraphService"
    ) as mock_service_class:
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
    self, mock_kuzu_client, mock_db_session, mock_subgraph_model
  ):
    """Test workspace deletion with force flag."""
    tool = DeleteWorkspaceTool(mock_kuzu_client)

    mock_db_session.query.return_value.filter.return_value.first.return_value = (
      mock_subgraph_model
    )

    with patch(
      "robosystems.middleware.mcp.tools.workspace.SubgraphService"
    ) as mock_service_class:
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
  async def test_delete_workspace_is_primary_graph(self, mock_kuzu_client):
    """Test deletion fails with invalid format when trying to delete primary graph."""
    tool = DeleteWorkspaceTool(mock_kuzu_client)

    result = await tool.execute({"workspace_id": "kg1234567890abcdef"})

    assert result["error"] == "invalid_workspace_id"
    assert "Invalid workspace ID format" in result["message"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_delete_workspace_invalid_format(self, mock_kuzu_client):
    """Test deletion fails with invalid workspace ID format."""
    tool = DeleteWorkspaceTool(mock_kuzu_client)

    result = await tool.execute({"workspace_id": "invalid"})

    assert result["error"] == "invalid_workspace_id"
    assert "format" in result["message"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_delete_workspace_not_found(self, mock_kuzu_client, mock_db_session):
    """Test deletion fails when workspace not found."""
    tool = DeleteWorkspaceTool(mock_kuzu_client)

    mock_db_session.query.return_value.filter.return_value.first.return_value = None

    with patch(
      "robosystems.middleware.mcp.tools.workspace.SubgraphService"
    ) as mock_service_class:
      mock_service = AsyncMock()
      mock_service_class.return_value = mock_service
      mock_service.delete_subgraph_database.side_effect = Exception(
        "Workspace not found"
      )

      result = await tool.execute({"workspace_id": "kg1234567890abcdef_missing"})

    assert result["error"] == "deletion_failed"
    assert "not found" in result["message"].lower()


class TestListWorkspacesTool:
  """Tests for ListWorkspacesTool."""

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_list_workspaces_includes_primary(
    self, mock_kuzu_client, mock_db_session, mock_graph_model
  ):
    """Test listing workspaces includes primary graph."""
    tool = ListWorkspacesTool(mock_kuzu_client)

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
    self, mock_kuzu_client, mock_db_session, mock_graph_model, mock_subgraph_model
  ):
    """Test listing workspaces includes subgraphs."""
    tool = ListWorkspacesTool(mock_kuzu_client)

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
    self, mock_kuzu_client, mock_db_session
  ):
    """Test listing workspaces when parent not found (returns generic description)."""
    tool = ListWorkspacesTool(mock_kuzu_client)

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
  async def test_switch_workspace_returns_client_side_error(self, mock_kuzu_client):
    """Test that switch-workspace returns client-side error (tool is client-only)."""
    tool = SwitchWorkspaceTool(mock_kuzu_client)

    result = await tool.execute({"workspace_id": "kg1234567890abcdef_dev"})

    assert result["error"] == "client_side_tool"
    assert "client-side" in result["message"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_switch_to_primary_returns_client_side_error(self, mock_kuzu_client):
    """Test that switch to primary also returns client-side error."""
    tool = SwitchWorkspaceTool(mock_kuzu_client)

    result = await tool.execute({"workspace_id": "kg1234567890abcdef"})

    assert result["error"] == "client_side_tool"
    assert "MCP client" in result["message"]


class TestBuildFactGridTool:
  """Tests for BuildFactGridTool."""

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_build_fact_grid_success(self, mock_kuzu_client):
    """Test successful fact grid building."""
    tool = BuildFactGridTool(mock_kuzu_client)

    with (
      patch("robosystems.middleware.graph.get_universal_repository") as mock_repo,
      patch(
        "robosystems.operations.views.fact_grid_builder.FactGridBuilder"
      ) as mock_builder_class,
    ):
      mock_repository = AsyncMock()
      mock_repo.return_value = mock_repository
      mock_repository.execute_query.return_value = [
        {
          "element_id": "us-gaap:Assets",
          "period_end": "2023-12-31",
          "value": 1000000,
        }
      ]

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
  async def test_build_fact_grid_missing_elements(self, mock_kuzu_client):
    """Test fact grid building fails without elements."""
    tool = BuildFactGridTool(mock_kuzu_client)

    result = await tool.execute({"periods": ["2023-12-31"]})

    assert result["error"] == "missing_elements"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_build_fact_grid_missing_periods(self, mock_kuzu_client):
    """Test fact grid building fails without periods."""
    tool = BuildFactGridTool(mock_kuzu_client)

    result = await tool.execute({"elements": ["us-gaap:Assets"]})

    assert result["error"] == "missing_periods"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_build_fact_grid_query_failure(self, mock_kuzu_client):
    """Test fact grid building handles query errors."""
    tool = BuildFactGridTool(mock_kuzu_client)

    with patch("robosystems.middleware.graph.get_universal_repository") as mock_repo:
      mock_repository = AsyncMock()
      mock_repo.return_value = mock_repository
      mock_repository.execute_query.side_effect = Exception("Query failed")

      result = await tool.execute(
        {"elements": ["us-gaap:Assets"], "periods": ["2023-12-31"]}
      )

    assert result["error"] == "construction_failed"
    assert "Query failed" in result["message"]


class TestQueryStagingTool:
  """Tests for QueryStagingTool."""

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_query_staging_success(self, mock_kuzu_client):
    """Test successful staging query."""
    tool = QueryStagingTool(mock_kuzu_client)

    with patch(
      "robosystems.graph_api.client.factory.get_graph_client"
    ) as mock_client_factory:
      mock_client = AsyncMock()
      mock_client_factory.return_value = mock_client
      mock_client.query_table.return_value = {
        "columns": ["id", "name"],
        "rows": [[1, "Test"]],
        "execution_time_ms": 10,
      }

      result = await tool.execute({"sql": "SELECT * FROM staging_table"})

    assert result["success"] is True
    assert result["columns"] == ["id", "name"]
    assert len(result["rows"]) == 1
    assert result["row_count"] == 1

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_query_staging_with_limit(self, mock_kuzu_client):
    """Test staging query with custom limit."""
    tool = QueryStagingTool(mock_kuzu_client)

    with patch(
      "robosystems.graph_api.client.factory.get_graph_client"
    ) as mock_client_factory:
      mock_client = AsyncMock()
      mock_client_factory.return_value = mock_client
      mock_client.query_table.return_value = {
        "columns": ["id"],
        "rows": [[1], [2], [3]],
        "execution_time_ms": 5,
      }

      result = await tool.execute({"sql": "SELECT id FROM staging_table", "limit": 50})

    assert result["success"] is True
    assert result["row_count"] == 3

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_query_staging_auto_limit_injection(self, mock_kuzu_client):
    """Test staging query auto-adds LIMIT if missing."""
    tool = QueryStagingTool(mock_kuzu_client)

    with patch(
      "robosystems.graph_api.client.factory.get_graph_client"
    ) as mock_client_factory:
      mock_client = AsyncMock()
      mock_client_factory.return_value = mock_client
      mock_client.query_table.return_value = {"columns": [], "rows": []}

      await tool.execute({"sql": "SELECT * FROM staging_table"})

      call_args = mock_client.query_table.call_args[0]
      assert "LIMIT 100" in call_args[1]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_query_staging_missing_sql(self, mock_kuzu_client):
    """Test staging query fails without SQL."""
    tool = QueryStagingTool(mock_kuzu_client)

    result = await tool.execute({})

    assert result["error"] == "missing_sql"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_query_staging_query_failure(self, mock_kuzu_client):
    """Test staging query handles execution errors."""
    tool = QueryStagingTool(mock_kuzu_client)

    with patch(
      "robosystems.graph_api.client.factory.get_graph_client"
    ) as mock_client_factory:
      mock_client = AsyncMock()
      mock_client_factory.return_value = mock_client
      mock_client.query_table.side_effect = Exception("Query error")

      result = await tool.execute({"sql": "SELECT * FROM staging_table"})

    assert result["error"] == "query_failed"


class TestMaterializeGraphTool:
  """Tests for MaterializeGraphTool."""

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_materialize_single_file(self, mock_kuzu_client):
    """Test materializing a single file."""
    tool = MaterializeGraphTool(mock_kuzu_client)

    with (
      patch("robosystems.database.get_db_session") as mock_db,
      patch(
        "robosystems.tasks.table_operations.graph_materialization.materialize_file_to_graph"
      ) as mock_task,
    ):
      mock_session = MagicMock()
      mock_db.return_value = iter([mock_session])

      mock_table = MagicMock()
      mock_table.id = "table123"
      mock_table.graph_id = "kg1234567890abcdef"
      mock_session.query.return_value.filter.return_value.first.return_value = (
        mock_table
      )

      mock_file = MagicMock()
      mock_file.id = "file123"

      with patch("robosystems.models.iam.GraphFile") as mock_file_class:
        mock_file_class.get_by_id.return_value = mock_file

        mock_celery_task = MagicMock()
        mock_celery_task.id = "task123"
        mock_task.delay.return_value = mock_celery_task

        result = await tool.execute(
          {"table_name": "financial_data", "file_id": "file123"}
        )

    assert result["success"] is True
    assert result["task_id"] == "task123"
    assert result["file_id"] == "file123"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_materialize_all_files(self, mock_kuzu_client):
    """Test materializing all files for a table."""
    tool = MaterializeGraphTool(mock_kuzu_client)

    with (
      patch("robosystems.database.get_db_session") as mock_db,
      patch(
        "robosystems.tasks.table_operations.graph_materialization.materialize_file_to_graph"
      ) as mock_task,
    ):
      mock_session = MagicMock()
      mock_db.return_value = iter([mock_session])

      mock_table = MagicMock()
      mock_table.id = "table123"
      mock_session.query.return_value.filter.return_value.first.return_value = (
        mock_table
      )

      mock_file1 = MagicMock()
      mock_file1.id = "file1"
      mock_file1.duckdb_status = "staged"
      mock_file2 = MagicMock()
      mock_file2.id = "file2"
      mock_file2.upload_status = "uploaded"

      with patch("robosystems.models.iam.GraphFile") as mock_file_class:
        mock_file_class.get_all_for_table.return_value = [mock_file1, mock_file2]

        mock_celery_task = MagicMock()
        mock_celery_task.id = "task123"
        mock_task.delay.return_value = mock_celery_task

        result = await tool.execute({"table_name": "financial_data"})

    assert result["success"] is True
    assert result["file_count"] == 2
    assert len(result["task_ids"]) == 2

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_materialize_table_not_found(self, mock_kuzu_client):
    """Test materialization fails when table not found."""
    tool = MaterializeGraphTool(mock_kuzu_client)

    with patch("robosystems.database.get_db_session") as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = iter([mock_session])
      mock_session.query.return_value.filter.return_value.first.return_value = None

      result = await tool.execute({"table_name": "missing_table"})

    assert result["error"] == "table_not_found"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_materialize_no_staged_files(self, mock_kuzu_client):
    """Test materialization fails when no staged files exist."""
    tool = MaterializeGraphTool(mock_kuzu_client)

    with patch("robosystems.database.get_db_session") as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = iter([mock_session])

      mock_table = MagicMock()
      mock_table.id = "table123"
      mock_session.query.return_value.filter.return_value.first.return_value = (
        mock_table
      )

      with patch("robosystems.models.iam.GraphFile") as mock_file_class:
        mock_file_class.get_all_for_table.return_value = []

        result = await tool.execute({"table_name": "financial_data"})

    assert result["error"] == "no_files_to_materialize"


class TestMapElementsTool:
  """Tests for MapElementsTool."""

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_map_elements_retrieve_structure(self, mock_kuzu_client):
    """Test retrieving existing mapping structure."""
    tool = MapElementsTool(mock_kuzu_client)

    with patch(
      "robosystems.operations.views.element_mapping.get_mapping_structure"
    ) as mock_get_mapping:
      mock_mapping = MagicMock()
      mock_mapping.association_count = 10
      mock_mapping.structure.associations = [
        MagicMock(
          source_element="Revenue",
          target_element="us-gaap:Revenue",
          aggregation_method=MagicMock(value="sum"),
          weight=1.0,
        )
      ]
      mock_get_mapping.return_value = mock_mapping

      result = await tool.execute({"structure_id": "mapping_123"})

    assert result["success"] is True
    assert result["association_count"] == 10
    assert len(result["associations"]) == 1

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_map_elements_structure_not_found(self, mock_kuzu_client):
    """Test retrieving non-existent mapping structure."""
    tool = MapElementsTool(mock_kuzu_client)

    with patch(
      "robosystems.operations.views.element_mapping.get_mapping_structure"
    ) as mock_get_mapping:
      mock_get_mapping.return_value = None

      result = await tool.execute({"structure_id": "missing_mapping"})

    assert result["error"] == "mapping_not_found"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_map_elements_suggests_client_sdk(self, mock_kuzu_client):
    """Test tool suggests client SDK for mapping creation."""
    tool = MapElementsTool(mock_kuzu_client)

    result = await tool.execute(
      {
        "source_elements": ["Revenue", "COGS"],
        "target_taxonomy": "us-gaap",
      }
    )

    assert result["info"] == "mapping_creation_requires_client"
    assert "robosystems-python-client" in result["message"]


class TestIngestFileTool:
  """Tests for IngestFileTool."""

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_ingest_file_suggests_client_sdk(self, mock_kuzu_client):
    """Test tool suggests client SDK for file upload."""
    tool = IngestFileTool(mock_kuzu_client)

    result = await tool.execute(
      {
        "file_path": "/path/to/data.csv",
        "table_name": "financial_data",
        "ingest_to_graph": False,
      }
    )

    assert result["error"] == "client_sdk_required"
    assert "robosystems-python-client" in result["message"]
    assert "upload_file" in result["example"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_ingest_file_missing_file_path(self, mock_kuzu_client):
    """Test ingest fails without file path."""
    tool = IngestFileTool(mock_kuzu_client)

    result = await tool.execute({"table_name": "financial_data"})

    assert result["error"] == "missing_file_path"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_ingest_file_missing_table_name(self, mock_kuzu_client):
    """Test ingest fails without table name."""
    tool = IngestFileTool(mock_kuzu_client)

    result = await tool.execute({"file_path": "/path/to/data.csv"})

    assert result["error"] == "missing_table_name"
