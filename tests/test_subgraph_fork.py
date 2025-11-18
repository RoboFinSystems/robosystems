"""Test subgraph fork functionality and write permissions."""

import pytest
from unittest.mock import Mock, patch, AsyncMock
from robosystems.security.cypher_analyzer import is_write_operation
from robosystems.middleware.graph.subgraph_utils import parse_subgraph_id


def test_parse_subgraph_id():
  """Test that subgraph IDs are correctly parsed."""
  # Test main graph (no underscore)
  assert parse_subgraph_id("kg1234567890abcdef") is None

  # Test subgraph (with underscore)
  result = parse_subgraph_id("kg1234567890abcdef_dev")
  assert result is not None
  assert result.parent_graph_id == "kg1234567890abcdef"
  assert result.subgraph_name == "dev"
  assert result.database_name == "kg1234567890abcdef_dev"


def test_write_operation_detection():
  """Test that write operations are correctly detected."""
  # Read operations
  assert not is_write_operation("MATCH (n) RETURN n")
  assert not is_write_operation("MATCH (n:Entity) WHERE n.type = $type RETURN n")

  # Write operations
  assert is_write_operation("CREATE (n:Entity {name: 'test'})")
  assert is_write_operation("MERGE (n:Entity {id: $id})")
  assert is_write_operation("MATCH (n) SET n.updated = true")
  assert is_write_operation("MATCH (n) DELETE n")


@pytest.mark.asyncio
async def test_fork_parent_data():
  """Test the fork_parent_data method."""
  from robosystems.operations.graph.subgraph_service import SubgraphService
  from robosystems.config import env

  service = SubgraphService()

  # Patch the GraphClient where it's imported in the fork_parent_data function
  with patch("robosystems.graph_api.client.GraphClient") as mock_client_class:
    # Setup the mock client instance
    mock_client = AsyncMock()
    mock_client_class.return_value = mock_client
    mock_client.fork_from_parent.return_value = {
      "status": "success",
      "tables_copied": ["Element", "Transaction"],
      "total_rows": 1000,
    }

    # Save original env value and set to local mode for simpler testing
    original_url = env.GRAPH_API_URL
    env.GRAPH_API_URL = "http://localhost:8001"

    try:
      # Test fork
      result = await service.fork_parent_data(
        parent_graph_id="kg1234567890abcdef",
        subgraph_id="kg1234567890abcdef_dev",
        options={"tables": ["Element", "Transaction"], "exclude_patterns": ["Report*"]},
      )

      assert result["status"] == "success"
      assert result["row_count"] == 1000
      assert len(result["tables_copied"]) == 2
      assert result["parent_graph_id"] == "kg1234567890abcdef"
      assert result["subgraph_id"] == "kg1234567890abcdef_dev"

      # Verify fork_from_parent was called with correct parameters
      mock_client.fork_from_parent.assert_called_once()
      call_args = mock_client.fork_from_parent.call_args
      assert call_args[1]["parent_graph_id"] == "kg1234567890abcdef"
      assert call_args[1]["subgraph_id"] == "kg1234567890abcdef_dev"
      assert call_args[1]["tables"] == ["Element", "Transaction"]
      assert call_args[1]["ignore_errors"] is True
    finally:
      # Restore original env
      env.GRAPH_API_URL = original_url


@pytest.mark.asyncio
async def test_create_subgraph_with_fork():
  """Test creating a subgraph with fork_parent=True."""
  from robosystems.operations.graph.subgraph_service import SubgraphService

  service = SubgraphService()

  # Mock graph and user
  mock_graph = Mock(
    graph_id="kg1234567890abcdef",
    graph_name="Test Graph",
    graph_tier="kuzu-large",
    graph_type="kuzu",
    base_schema="entity",
    schema_extensions=["roboledger"],
    graph_instance_id="i-12345",
    graph_cluster_region="us-east-1",
    org_id="org123",
  )

  mock_user = Mock(id="user123")

  with patch.object(service, "create_subgraph_database") as mock_create_db:
    with patch.object(service, "fork_parent_data") as mock_fork:
      with patch("robosystems.database.get_db_session") as mock_get_db:
        # Setup mocks
        mock_create_db.return_value = {"status": "created", "instance_id": "i-12345"}

        mock_fork.return_value = {
          "status": "success",
          "tables_copied": ["Element", "Transaction"],
          "row_count": 1000,
        }

        mock_db = Mock()
        mock_get_db.return_value = iter([mock_db])
        mock_db.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
        mock_db.commit = Mock()
        mock_db.refresh = Mock()
        mock_db.add = Mock()
        mock_db.close = Mock()

        # Create subgraph with fork
        result = await service.create_subgraph(
          parent_graph=mock_graph,
          user=mock_user,
          name="dev",
          description="Development Environment",
          fork_parent=True,
          fork_options={"exclude_patterns": ["Report*"]},
        )

        # Verify subgraph was created
        assert result["graph_id"] == "kg1234567890abcdef_dev"
        assert result["status"] == "active"
        assert result["database_created"] is True

        # Verify fork was called (now with progress_callback parameter)
        mock_fork.assert_called_once()
        call_args = mock_fork.call_args
        assert call_args[1]["parent_graph_id"] == "kg1234567890abcdef"
        assert call_args[1]["subgraph_id"] == "kg1234567890abcdef_dev"
        assert call_args[1]["options"] == {"exclude_patterns": ["Report*"]}

        # Verify fork status in result
        assert result["fork_status"] is not None
        assert result["fork_status"]["status"] == "success"
