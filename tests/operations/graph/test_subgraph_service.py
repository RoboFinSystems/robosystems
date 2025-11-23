"""
Comprehensive tests for SubgraphService.

Tests the critical subgraph service that manages subgraph operations for
Enterprise and Premium tier graphs, including creation, deletion, and management.
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock

from robosystems.operations.graph.subgraph_service import SubgraphService
from robosystems.exceptions import GraphAllocationError


class TestSubgraphService:
  """Tests for SubgraphService class."""

  @pytest.fixture
  def valid_parent_graph_id(self):
    """A valid parent graph ID for testing."""
    # This matches the pattern kg[a-f0-9]{16,} used throughout tests
    return "kg5f2e5e0da65d45d69645"

  @pytest.fixture
  def service(self):
    """Create a SubgraphService instance."""
    with patch(
      "robosystems.operations.graph.subgraph_service.LadybugAllocationManager"
    ) as mock_alloc:
      service = SubgraphService()
      service.allocation_manager = mock_alloc(environment="test")
      return service

  @pytest.fixture
  def mock_allocation_manager(self, service):
    """Mock allocation manager."""
    mock = AsyncMock()
    mock.find_database_location = AsyncMock()
    service.allocation_manager = mock
    return mock

  @pytest.fixture
  def mock_lbug_client(self):
    """Create a mock LadybugDB client."""
    client = AsyncMock()
    client.list_databases = AsyncMock(return_value={"databases": []})
    client.create_database = AsyncMock()
    client.delete_database = AsyncMock()
    client.install_schema = AsyncMock()
    client.execute = AsyncMock()
    client.get_database = AsyncMock()
    client.backup = AsyncMock()
    return client

  @pytest.fixture
  def mock_parent_location(self):
    """Mock parent location object."""
    location = Mock()
    location.instance_id = "i-1234567890abcdef0"
    location.private_ip = "10.0.1.100"
    return location

  @pytest.mark.asyncio
  async def test_create_subgraph_success(
    self,
    service,
    mock_allocation_manager,
    mock_lbug_client,
    mock_parent_location,
    valid_parent_graph_id,
    db_session,
  ):
    """Test successful subgraph creation."""
    from robosystems.models.iam.graph import Graph
    from robosystems.config.graph_tier import GraphTier

    existing_graph = (
      db_session.query(Graph).filter(Graph.graph_id == valid_parent_graph_id).first()
    )
    if not existing_graph:
      parent_graph = Graph(
        graph_id=valid_parent_graph_id,
        graph_name="Parent Graph",
        graph_type="generic",
        graph_tier=GraphTier.LADYBUG_LARGE.value,
      )
      db_session.add(parent_graph)
      db_session.commit()

    mock_allocation_manager.find_database_location.return_value = mock_parent_location

    with patch(
      "robosystems.operations.graph.subgraph_service.get_graph_client_for_instance"
    ) as mock_get_client:
      mock_get_client.return_value = mock_lbug_client

      result = await service.create_subgraph_database(
        parent_graph_id=valid_parent_graph_id,
        subgraph_name="analysis",
        schema_extensions=None,
      )

      assert result["status"] == "created"
      assert result["graph_id"] == "kg5f2e5e0da65d45d69645_analysis"
      assert result["database_name"] == "kg5f2e5e0da65d45d69645_analysis"
      assert result["parent_graph_id"] == "kg5f2e5e0da65d45d69645"
      assert result["instance_id"] == "i-1234567890abcdef0"
      assert result["instance_ip"] == "10.0.1.100"
      assert "created_at" in result

      # Verify calls - Both find_database_location and get_graph_client_for_instance
      # are called twice:
      # 1. During tier limit enforcement (listing existing subgraphs)
      # 2. During actual subgraph creation
      assert mock_allocation_manager.find_database_location.call_count == 2
      mock_allocation_manager.find_database_location.assert_called_with(
        "kg5f2e5e0da65d45d69645"
      )
      assert mock_get_client.call_count == 2
      mock_get_client.assert_called_with("10.0.1.100")
      mock_lbug_client.create_database.assert_called_once_with(
        graph_id="kg5f2e5e0da65d45d69645_analysis",
        schema_type="custom",
        custom_schema_ddl=None,
        is_subgraph=True,
      )
      mock_lbug_client.install_schema.assert_called_once()

  @pytest.mark.asyncio
  async def test_create_subgraph_already_exists(
    self,
    service,
    mock_allocation_manager,
    mock_lbug_client,
    mock_parent_location,
    db_session,
    valid_parent_graph_id,
  ):
    """Test creating a subgraph that already exists."""
    from robosystems.models.iam.graph import Graph
    from robosystems.config.graph_tier import GraphTier

    # Check if parent graph already exists (from previous test)
    existing_graph = (
      db_session.query(Graph).filter(Graph.graph_id == valid_parent_graph_id).first()
    )
    if not existing_graph:
      parent_graph = Graph(
        graph_id=valid_parent_graph_id,
        graph_name="Parent Graph",
        graph_type="generic",
        graph_tier=GraphTier.LADYBUG_LARGE.value,
      )
      db_session.add(parent_graph)
      db_session.commit()

    mock_allocation_manager.find_database_location.return_value = mock_parent_location
    mock_lbug_client.list_databases.return_value = {
      "databases": [{"graph_id": f"{valid_parent_graph_id}_analysis"}]
    }

    with patch(
      "robosystems.operations.graph.subgraph_service.get_graph_client_for_instance"
    ) as mock_get_client:
      mock_get_client.return_value = mock_lbug_client

      result = await service.create_subgraph_database(
        parent_graph_id=valid_parent_graph_id, subgraph_name="analysis"
      )

      assert result["status"] == "exists"
      assert result["message"] == "Subgraph database already exists"
      # Should not attempt to create
      mock_lbug_client.create_database.assert_not_called()

  @pytest.mark.asyncio
  async def test_create_subgraph_with_extensions(
    self,
    service,
    mock_allocation_manager,
    mock_lbug_client,
    mock_parent_location,
    db_session,
    valid_parent_graph_id,
  ):
    """Test creating a subgraph with schema extensions."""
    from robosystems.models.iam.graph import Graph
    from robosystems.config.graph_tier import GraphTier

    existing_graph = (
      db_session.query(Graph).filter(Graph.graph_id == valid_parent_graph_id).first()
    )
    if not existing_graph:
      parent_graph = Graph(
        graph_id=valid_parent_graph_id,
        graph_name="Parent Graph",
        graph_type="generic",
        graph_tier=GraphTier.LADYBUG_LARGE.value,
      )
      db_session.add(parent_graph)
      db_session.commit()

    mock_allocation_manager.find_database_location.return_value = mock_parent_location

    with patch(
      "robosystems.operations.graph.subgraph_service.get_graph_client_for_instance"
    ) as mock_get_client:
      mock_get_client.return_value = mock_lbug_client

      # Mock the DDL generation to avoid needing real schema extensions
      with patch.object(
        service, "_generate_schema_ddl", AsyncMock(return_value="CREATE (n:Node)")
      ):
        result = await service.create_subgraph_database(
          parent_graph_id=valid_parent_graph_id,
          subgraph_name="extended",
          schema_extensions=["analytics", "ml"],
        )

        assert result["status"] == "created"
        # Should install schema once with combined DDL
        mock_lbug_client.install_schema.assert_called_once()

  @pytest.mark.asyncio
  async def test_create_subgraph_invalid_parent(self, service):
    """Test creating a subgraph with invalid parent ID."""
    with pytest.raises(ValueError) as exc_info:
      await service.create_subgraph_database(
        parent_graph_id="invalid!@#", subgraph_name="test"
      )

    assert "Invalid parent graph ID" in str(exc_info.value)

  @pytest.mark.asyncio
  async def test_create_subgraph_shared_repository_parent(self, service):
    """Test that shared repositories cannot have subgraphs."""
    with pytest.raises(ValueError) as exc_info:
      await service.create_subgraph_database(
        parent_graph_id="sec", subgraph_name="test"
      )

    # Shared repositories fail validation as invalid parent IDs
    assert "Invalid parent graph ID" in str(exc_info.value)

  @pytest.mark.asyncio
  async def test_create_subgraph_invalid_name(self, service):
    """Test creating a subgraph with invalid name."""
    with pytest.raises(ValueError) as exc_info:
      await service.create_subgraph_database(
        parent_graph_id="kg5f2e5e0da65d45d69645", subgraph_name="invalid-name!"
      )

    assert "Invalid subgraph name" in str(exc_info.value)
    assert "Must be alphanumeric" in str(exc_info.value)

  @pytest.mark.asyncio
  async def test_create_subgraph_parent_not_found(
    self, service, mock_allocation_manager
  ):
    """Test creating a subgraph when parent doesn't exist."""
    mock_allocation_manager.find_database_location.return_value = None

    with pytest.raises(GraphAllocationError) as exc_info:
      await service.create_subgraph_database(
        parent_graph_id="kg5f2e5e0da65d45d69645", subgraph_name="orphan"
      )

    assert "Parent graph kg5f2e5e0da65d45d69645 not found" in str(exc_info.value)

  @pytest.mark.asyncio
  async def test_create_subgraph_creation_failure(
    self,
    service,
    mock_allocation_manager,
    mock_lbug_client,
    mock_parent_location,
    db_session,
    valid_parent_graph_id,
  ):
    """Test handling of database creation failure."""
    from robosystems.models.iam.graph import Graph
    from robosystems.config.graph_tier import GraphTier

    existing_graph = (
      db_session.query(Graph).filter(Graph.graph_id == valid_parent_graph_id).first()
    )
    if not existing_graph:
      parent_graph = Graph(
        graph_id=valid_parent_graph_id,
        graph_name="Parent Graph",
        graph_type="generic",
        graph_tier=GraphTier.LADYBUG_LARGE.value,
      )
      db_session.add(parent_graph)
      db_session.commit()

    mock_allocation_manager.find_database_location.return_value = mock_parent_location
    mock_lbug_client.create_database.side_effect = Exception("Database creation failed")

    with patch(
      "robosystems.operations.graph.subgraph_service.get_graph_client_for_instance"
    ) as mock_get_client:
      mock_get_client.return_value = mock_lbug_client

      with pytest.raises(GraphAllocationError) as exc_info:
        await service.create_subgraph_database(
          parent_graph_id=valid_parent_graph_id, subgraph_name="failed"
        )

      assert "Failed to create subgraph" in str(exc_info.value)

  @pytest.mark.asyncio
  async def test_delete_subgraph_success(
    self, service, mock_allocation_manager, mock_lbug_client, mock_parent_location
  ):
    """Test successful subgraph deletion."""
    mock_allocation_manager.find_database_location.return_value = mock_parent_location
    mock_lbug_client.list_databases.return_value = {
      "databases": [{"graph_id": "kg5f2e5e0da65d45d69645_analysis"}]
    }
    mock_lbug_client.execute.return_value = [{"node_count": 0}]  # No data

    with patch(
      "robosystems.operations.graph.subgraph_service.get_graph_client_for_instance"
    ) as mock_get_client:
      with patch("robosystems.graph_api.client.GraphClient") as mock_graph_client_class:
        mock_graph_client_class.return_value = mock_lbug_client
        mock_get_client.return_value = mock_lbug_client

        result = await service.delete_subgraph_database(
          "kg5f2e5e0da65d45d69645_analysis"
        )

        assert result["status"] == "deleted"
        assert result["graph_id"] == "kg5f2e5e0da65d45d69645_analysis"
        assert "deleted_at" in result

        mock_lbug_client.delete_database.assert_called_once_with(
          "kg5f2e5e0da65d45d69645_analysis"
        )

  @pytest.mark.asyncio
  async def test_delete_subgraph_not_found(
    self, service, mock_allocation_manager, mock_lbug_client, mock_parent_location
  ):
    """Test deleting a non-existent subgraph."""
    mock_allocation_manager.find_database_location.return_value = mock_parent_location
    mock_lbug_client.list_databases.return_value = {
      "databases": []
    }  # Database doesn't exist

    with patch(
      "robosystems.operations.graph.subgraph_service.get_graph_client_for_instance"
    ) as mock_get_client:
      with patch("robosystems.graph_api.client.GraphClient") as mock_graph_client_class:
        mock_graph_client_class.return_value = mock_lbug_client
        mock_get_client.return_value = mock_lbug_client

        result = await service.delete_subgraph_database(
          "kg5f2e5e0da65d45d69645_missing"
        )

        assert result["status"] == "not_found"
        assert result["message"] == "Subgraph database does not exist"
        mock_lbug_client.delete_database.assert_not_called()

  @pytest.mark.asyncio
  async def test_delete_subgraph_with_data_no_force(
    self, service, mock_allocation_manager, mock_lbug_client, mock_parent_location
  ):
    """Test deletion fails when subgraph has data and force=False."""
    mock_allocation_manager.find_database_location.return_value = mock_parent_location
    mock_lbug_client.list_databases.return_value = {
      "databases": [{"graph_id": "kg5f2e5e0da65d45d69645_analysis"}]
    }
    mock_lbug_client.execute.return_value = [{"node_count": 100}]  # Has data

    with patch(
      "robosystems.operations.graph.subgraph_service.get_graph_client_for_instance"
    ) as mock_get_client:
      with patch("robosystems.graph_api.client.GraphClient") as mock_graph_client_class:
        mock_graph_client_class.return_value = mock_lbug_client
        mock_get_client.return_value = mock_lbug_client

        with pytest.raises(GraphAllocationError) as exc_info:
          await service.delete_subgraph_database(
            "kg5f2e5e0da65d45d69645_analysis", force=False
          )

        assert "contains data" in str(exc_info.value)
        assert "force=True" in str(exc_info.value)
        mock_lbug_client.delete_database.assert_not_called()

  @pytest.mark.asyncio
  async def test_delete_subgraph_with_data_force(
    self, service, mock_allocation_manager, mock_lbug_client, mock_parent_location
  ):
    """Test forced deletion of subgraph with data."""
    mock_allocation_manager.find_database_location.return_value = mock_parent_location
    mock_lbug_client.list_databases.return_value = {
      "databases": [{"graph_id": "kg5f2e5e0da65d45d69645_analysis"}]
    }
    mock_lbug_client.execute.return_value = [{"node_count": 100}]  # Has data

    with patch(
      "robosystems.operations.graph.subgraph_service.get_graph_client_for_instance"
    ) as mock_get_client:
      with patch("robosystems.graph_api.client.GraphClient") as mock_graph_client_class:
        mock_graph_client_class.return_value = mock_lbug_client
        mock_get_client.return_value = mock_lbug_client

        result = await service.delete_subgraph_database(
          "kg5f2e5e0da65d45d69645_analysis", force=True
        )

        assert result["status"] == "deleted"
        mock_lbug_client.delete_database.assert_called_once()

  @pytest.mark.asyncio
  async def test_delete_subgraph_with_backup(
    self, service, mock_allocation_manager, mock_lbug_client, mock_parent_location
  ):
    """Test deletion with backup creation."""
    mock_allocation_manager.find_database_location.return_value = mock_parent_location
    mock_lbug_client.list_databases.return_value = {
      "databases": [{"graph_id": "kg5f2e5e0da65d45d69645_analysis"}]
    }
    mock_lbug_client.execute.return_value = [{"node_count": 0}]
    mock_lbug_client.backup.return_value = {"location": "s3://backup/location"}

    with patch(
      "robosystems.operations.graph.subgraph_service.get_graph_client_for_instance"
    ) as mock_get_client:
      with patch("robosystems.graph_api.client.GraphClient") as mock_graph_client_class:
        mock_graph_client_class.return_value = mock_lbug_client
        mock_get_client.return_value = mock_lbug_client

        result = await service.delete_subgraph_database(
          "kg5f2e5e0da65d45d69645_analysis", create_backup=True
        )

        assert result["status"] == "deleted"
        assert result["backup_location"] is not None
        mock_lbug_client.backup.assert_called_once()

  @pytest.mark.asyncio
  async def test_delete_invalid_subgraph_id(self, service):
    """Test deletion with invalid subgraph ID."""
    with pytest.raises(ValueError) as exc_info:
      await service.delete_subgraph_database("not_a_subgraph")

    assert "Invalid subgraph ID" in str(exc_info.value)

  @pytest.mark.asyncio
  async def test_list_subgraphs_success(
    self, service, mock_allocation_manager, mock_lbug_client, mock_parent_location
  ):
    """Test listing subgraphs for a parent."""
    mock_allocation_manager.find_database_location.return_value = mock_parent_location
    mock_lbug_client.list_databases.return_value = {
      "databases": [
        {"graph_id": "kg5f2e5e0da65d45d69645"},  # Parent itself
        {"graph_id": "kg5f2e5e0da65d45d69645_analysis"},  # Subgraph
        {"graph_id": "kg5f2e5e0da65d45d69645_reporting"},  # Subgraph
        {"graph_id": "other_database"},  # Unrelated
      ]
    }

    with patch(
      "robosystems.operations.graph.subgraph_service.get_graph_client_for_instance"
    ) as mock_get_client:
      with patch("robosystems.graph_api.client.GraphClient") as mock_graph_client_class:
        mock_graph_client_class.return_value = mock_lbug_client
        mock_get_client.return_value = mock_lbug_client

        result = await service.list_subgraph_databases("kg5f2e5e0da65d45d69645")

        assert len(result) == 2
        assert result[0]["graph_id"] == "kg5f2e5e0da65d45d69645_analysis"
        assert result[0]["subgraph_name"] == "analysis"
        assert result[1]["graph_id"] == "kg5f2e5e0da65d45d69645_reporting"
        assert result[1]["subgraph_name"] == "reporting"

  @pytest.mark.asyncio
  async def test_list_subgraphs_parent_not_found(
    self, service, mock_allocation_manager
  ):
    """Test listing subgraphs when parent doesn't exist."""
    mock_allocation_manager.find_database_location.return_value = None

    result = await service.list_subgraph_databases("nonexistent")

    assert result == []

  @pytest.mark.asyncio
  async def test_list_subgraphs_error_handling(
    self, service, mock_allocation_manager, mock_parent_location
  ):
    """Test error handling in list_subgraphs."""
    mock_allocation_manager.find_database_location.return_value = mock_parent_location

    with patch(
      "robosystems.operations.graph.subgraph_service.get_graph_client_for_instance"
    ) as mock_get_client:
      mock_get_client.side_effect = Exception("Connection failed")

      result = await service.list_subgraph_databases("kg5f2e5e0da65d45d69645")

      assert result == []  # Returns empty list on error

  @pytest.mark.asyncio
  async def test_get_subgraph_info_success(
    self, service, mock_allocation_manager, mock_lbug_client, mock_parent_location
  ):
    """Test getting detailed subgraph information."""
    mock_allocation_manager.find_database_location.return_value = mock_parent_location
    mock_lbug_client.list_databases.return_value = {
      "databases": [{"graph_id": "kg5f2e5e0da65d45d69645_analysis"}]
    }
    mock_lbug_client.execute.side_effect = [
      [{"count": 100}],  # Node count
      [{"count": 50}],  # Edge count
    ]
    mock_lbug_client.get_database.return_value = {
      "size_mb": 10.5,
      "last_modified": "2024-01-01T00:00:00Z",
    }

    with patch(
      "robosystems.operations.graph.subgraph_service.get_graph_client_for_instance"
    ) as mock_get_client:
      with patch("robosystems.graph_api.client.GraphClient") as mock_graph_client_class:
        mock_graph_client_class.return_value = mock_lbug_client
        mock_get_client.return_value = mock_lbug_client

        result = await service.get_subgraph_info("kg5f2e5e0da65d45d69645_analysis")

        assert result is not None
        assert result["graph_id"] == "kg5f2e5e0da65d45d69645_analysis"
        assert result["subgraph_name"] == "analysis"
        assert result["statistics"]["node_count"] == 100
        assert result["statistics"]["edge_count"] == 50
        assert result["statistics"]["size_mb"] == 10.5

  @pytest.mark.asyncio
  async def test_get_subgraph_info_not_found(
    self, service, mock_allocation_manager, mock_lbug_client, mock_parent_location
  ):
    """Test getting info for non-existent subgraph."""
    mock_allocation_manager.find_database_location.return_value = mock_parent_location
    mock_lbug_client.list_databases.return_value = {
      "databases": []
    }  # Subgraph doesn't exist

    with patch(
      "robosystems.operations.graph.subgraph_service.get_graph_client_for_instance"
    ) as mock_get_client:
      with patch("robosystems.graph_api.client.GraphClient") as mock_graph_client_class:
        mock_graph_client_class.return_value = mock_lbug_client
        mock_get_client.return_value = mock_lbug_client

        result = await service.get_subgraph_info("kg5f2e5e0da65d45d69645_missing")

        assert result is None

  @pytest.mark.asyncio
  async def test_get_subgraph_info_invalid_id(self, service):
    """Test getting info with invalid subgraph ID."""
    result = await service.get_subgraph_info("not_a_subgraph")

    assert result is None

  @pytest.mark.asyncio
  async def test_private_install_schema_with_extensions(
    self, service, mock_lbug_client
  ):
    """Test private method _install_schema_with_extensions."""
    await service._install_schema_with_extensions(
      mock_lbug_client, "test_db", ["ext1", "ext2"]
    )

    # Should call install_schema once with base_schema and extensions
    mock_lbug_client.install_schema.assert_called_once_with(
      graph_id="test_db", base_schema="entity", extensions=["ext1", "ext2"]
    )

  @pytest.mark.asyncio
  async def test_private_install_base_schema(self, service, mock_lbug_client):
    """Test private method _install_base_schema."""
    await service._install_base_schema(mock_lbug_client, "test_db")

    mock_lbug_client.install_schema.assert_called_once_with(
      graph_id="test_db", base_schema="entity", extensions=[]
    )

  @pytest.mark.asyncio
  async def test_private_check_database_has_data(self, service, mock_lbug_client):
    """Test private method _check_database_has_data."""
    mock_lbug_client.execute.return_value = [{"node_count": 50}]

    result = await service._check_database_has_data(mock_lbug_client, "test_db")

    assert result is True

    # Test with no data
    mock_lbug_client.execute.return_value = [{"node_count": 0}]

    result = await service._check_database_has_data(mock_lbug_client, "test_db")

    assert result is False

  @pytest.mark.asyncio
  async def test_private_create_backup(self, service, mock_lbug_client):
    """Test private method _create_backup."""
    mock_lbug_client.backup.return_value = {"location": "s3://backup/test.backup"}

    result = await service._create_backup(mock_lbug_client, "test_db", "i-123456")

    assert "s3://backup" in result
    mock_lbug_client.backup.assert_called_once()

  @pytest.mark.asyncio
  async def test_private_create_backup_not_implemented(self, service, mock_lbug_client):
    """Test backup when method not implemented."""
    del mock_lbug_client.backup  # Remove the backup method

    result = await service._create_backup(mock_lbug_client, "test_db", "i-123456")

    # Should return None when backup not implemented
    assert result is None

  @pytest.mark.asyncio
  async def test_private_get_database_stats(self, service, mock_lbug_client):
    """Test private method _get_database_stats."""
    mock_lbug_client.execute.side_effect = [
      [{"count": 100}],  # Node count
      [{"count": 50}],  # Edge count
    ]
    mock_lbug_client.get_database.return_value = {
      "size_mb": 25.5,
      "last_modified": "2024-01-01T00:00:00Z",
    }

    result = await service._get_database_stats(mock_lbug_client, "test_db")

    assert result["node_count"] == 100
    assert result["edge_count"] == 50
    assert result["size_mb"] == 25.5
    assert result["last_modified"] == "2024-01-01T00:00:00Z"

  @pytest.mark.asyncio
  async def test_private_get_database_stats_error(self, service, mock_lbug_client):
    """Test stats retrieval with errors."""
    mock_lbug_client.execute.side_effect = Exception("Query failed")

    result = await service._get_database_stats(mock_lbug_client, "test_db")

    assert result["node_count"] is None
    assert result["edge_count"] is None


class TestSubgraphServiceIntegration:
  """Integration tests for SubgraphService."""

  @pytest.mark.asyncio
  @pytest.mark.integration
  async def test_full_subgraph_lifecycle(self, db_session):
    """Test complete subgraph lifecycle: create, list, get info, delete."""
    from robosystems.models.iam.graph import Graph
    from robosystems.config.graph_tier import GraphTier

    parent_graph_id = "kg5f2e5e0da65d45d69645"

    # Create parent graph in database
    existing_graph = (
      db_session.query(Graph).filter(Graph.graph_id == parent_graph_id).first()
    )
    if not existing_graph:
      parent_graph = Graph(
        graph_id=parent_graph_id,
        graph_name="Parent Graph",
        graph_type="generic",
        graph_tier=GraphTier.LADYBUG_LARGE.value,
      )
      db_session.add(parent_graph)
      db_session.commit()

    service = SubgraphService()

    # Mock the dependencies
    mock_location = Mock()
    mock_location.instance_id = "i-test123"
    mock_location.private_ip = "10.0.1.100"

    mock_client = AsyncMock()
    mock_client.list_databases = AsyncMock()
    mock_client.create_database = AsyncMock()
    mock_client.install_schema = AsyncMock()
    mock_client.execute = AsyncMock()
    mock_client.get_database = AsyncMock()
    mock_client.delete_database = AsyncMock()

    # Set up mock returns for lifecycle - need more calls now due to tier limit enforcement
    mock_client.list_databases.side_effect = [
      {"databases": []},  # Tier limit check - empty
      {"databases": []},  # Creation check - empty
      {
        "databases": [{"graph_id": f"{parent_graph_id}_test"}]
      },  # List check - has subgraph
      {
        "databases": [{"graph_id": f"{parent_graph_id}_test"}]
      },  # Get info check - exists
      {"databases": [{"graph_id": f"{parent_graph_id}_test"}]},  # Delete check - exists
    ]
    mock_client.execute.return_value = [{"node_count": 0}]

    with patch.object(
      service.allocation_manager, "find_database_location"
    ) as mock_find:
      mock_find.return_value = mock_location

      with patch(
        "robosystems.operations.graph.subgraph_service.get_graph_client_for_instance"
      ) as mock_get_client:
        with patch(
          "robosystems.graph_api.client.GraphClient"
        ) as mock_graph_client_class:
          mock_graph_client_class.return_value = mock_client
          mock_get_client.return_value = mock_client

          # Create
          create_result = await service.create_subgraph_database(
            parent_graph_id, "test"
          )
          assert create_result["status"] == "created"

          # List
          list_result = await service.list_subgraph_databases(parent_graph_id)
          assert len(list_result) == 1

          # Get Info
          info_result = await service.get_subgraph_info(f"{parent_graph_id}_test")
          assert info_result is not None

          # Delete
          delete_result = await service.delete_subgraph_database(
            f"{parent_graph_id}_test"
          )
          assert delete_result["status"] == "deleted"
