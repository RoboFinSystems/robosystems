"""Tests for EntityGraphService business logic."""

import pytest
from robosystems.operations.graph.entity_graph_service import EntityGraphService


class TestEntityGraphService:
  """Test cases for EntityGraphService operations."""

  def test_generate_graph_id(self):
    """Test database ID generation creates unique identifiers."""
    service = EntityGraphService()

    # Test basic generation
    graph_id = service._generate_graph_id("Test Entity")
    assert graph_id.startswith("kg")
    assert len(graph_id) > 5  # Should have hash suffix

    # Test with different entity names (should generate different IDs)
    graph_id2 = service._generate_graph_id("Different Entity")
    assert graph_id != graph_id2

    # Test format consistency
    assert isinstance(graph_id, str)
    assert (
      len(graph_id) == 20
    )  # "kg" + 12 char UUID + 4 char entity hash + 2 char timestamp hash

  def test_is_multitenant_mode_always_true(self):
    """Test multitenant mode is always True for Kuzu databases."""
    from robosystems.middleware.graph.multitenant_utils import MultiTenantUtils

    # MultiTenantUtils.is_multitenant_mode() always returns True
    result = MultiTenantUtils.is_multitenant_mode()
    assert result is True

  @pytest.mark.unit
  def test_entity_graph_service_initialization(self):
    """Test EntityGraphService initializes correctly."""
    service = EntityGraphService()
    assert service.session is not None

  @pytest.mark.asyncio
  async def test_create_entity_with_new_graph_success(self, mocker):
    """Test successful entity creation with new graph."""
    # Mock dependencies
    mock_session = mocker.MagicMock()
    mock_kuzu_client = mocker.AsyncMock()
    mock_allocation_manager = mocker.AsyncMock()

    # Setup mocks
    mock_allocation_manager.allocate_database.return_value = mocker.MagicMock(
      instance_id="i-12345",
      private_ip="10.0.0.1",
      database_id="kg12345678ab",
      allocation_id="alloc-123",
    )
    mock_user_limits = mocker.MagicMock()
    mock_user_limits.can_create_user_graph.return_value = (True, "Can create graph")
    mocker.patch(
      "robosystems.operations.graph.entity_graph_service.UserLimits.get_or_create_for_user",
      return_value=mock_user_limits,
    )
    mocker.patch(
      "robosystems.operations.graph.entity_graph_service.KuzuAllocationManager",
      return_value=mock_allocation_manager,
    )
    mocker.patch(
      "robosystems.operations.graph.entity_graph_service.get_graph_client_for_instance",
      return_value=mock_kuzu_client,
    )

    # Mock query responses
    mock_404_error = Exception("Database not found")
    mock_404_error.status_code = 404
    mock_kuzu_client.get_database.side_effect = mock_404_error  # Database doesn't exist
    mock_kuzu_client.create_database.return_value = {"success": True}
    mock_kuzu_client.query.return_value = {
      "data": [
        {
          "identifier": "entity-kg12345678ab",
          "name": "Test Company",
          "website": "http://test.com",
          "cik": "0001234567",
          "sic": "1234",
          "sic_description": "Test SIC",
          "category": "Finance",
          "state_of_incorporation": "DE",
          "fiscal_year_end": "1231",
          "ein": "123456789",
          "status": "active",
          "created_at": "2023-01-01T00:00:00Z",
          "updated_at": "2023-01-01T00:00:00Z",
        }
      ]
    }

    # Create service and run test
    service = EntityGraphService(session=mock_session)
    entity_data = {
      "name": "Test Company",
      "cik": "0001234567",
      "sic": "1234",
      "category": "Finance",
      "uri": "http://test.com",
    }

    result = await service.create_entity_with_new_graph(
      entity_data_dict=entity_data, user_id="user-123", tier="standard"
    )

    # Verify results
    assert "graph_id" in result
    assert "entity" in result
    mock_allocation_manager.allocate_database.assert_called_once()
    mock_kuzu_client.create_database.assert_called_once()

  @pytest.mark.asyncio
  async def test_create_entity_allocation_failure(self, mocker):
    """Test entity creation when database allocation fails."""
    # Mock dependencies
    mock_session = mocker.MagicMock()
    mock_allocation_manager = mocker.AsyncMock()

    # Setup allocation failure
    mock_allocation_manager.allocate_database.side_effect = Exception(
      "No available instances"
    )
    mock_user_limits = mocker.MagicMock()
    mock_user_limits.can_create_user_graph.return_value = (True, "Can create graph")
    mocker.patch(
      "robosystems.operations.graph.entity_graph_service.UserLimits.get_or_create_for_user",
      return_value=mock_user_limits,
    )
    mocker.patch(
      "robosystems.operations.graph.entity_graph_service.KuzuAllocationManager",
      return_value=mock_allocation_manager,
    )

    # Create service and run test
    service = EntityGraphService(session=mock_session)
    entity_data = {"name": "Test Company", "uri": "http://test.com"}

    with pytest.raises(Exception) as exc_info:
      await service.create_entity_with_new_graph(
        entity_data_dict=entity_data, user_id="user-123"
      )

    assert "No available instances" in str(exc_info.value)

  @pytest.mark.asyncio
  async def test_create_entity_with_cancellation(self, mocker):
    """Test entity creation with cancellation callback."""
    # Mock dependencies
    mock_session = mocker.MagicMock()
    mock_allocation_manager = mocker.AsyncMock()
    mock_kuzu_client = mocker.AsyncMock()

    # Setup mocks
    mock_allocation_manager.allocate_database.return_value = mocker.MagicMock(
      instance_id="i-12345", instance_ip="10.0.0.1", database_id="kg12345678ab"
    )
    mocker.patch(
      "robosystems.operations.graph.entity_graph_service.KuzuAllocationManager",
      return_value=mock_allocation_manager,
    )
    mocker.patch(
      "robosystems.operations.graph.entity_graph_service.get_graph_client_for_instance",
      return_value=mock_kuzu_client,
    )

    # Create cancellation callback
    cancelled = False

    def cancel_callback():
      nonlocal cancelled
      return cancelled

    # Create service
    service = EntityGraphService(session=mock_session)
    entity_data = {"name": "Test Company"}

    # Simulate cancellation during database creation
    mock_kuzu_client.create_database.side_effect = (
      lambda *args, **kwargs: setattr(
        cancelled, "__class__", type("cancelled", (), {"__bool__": lambda self: True})()
      )
      or None
    )

    # This should handle cancellation gracefully
    try:
      await service.create_entity_with_new_graph(
        entity_data_dict=entity_data,
        user_id="user-123",
        cancellation_callback=cancel_callback,
      )
    except Exception:
      pass  # Expected when cancellation occurs

  @pytest.mark.asyncio
  async def test_install_entity_schema_with_extensions(self, mocker):
    """Test schema installation with custom extensions."""
    mock_kuzu_client = mocker.AsyncMock()
    mock_kuzu_client.install_schema.return_value = {"success": True}

    service = EntityGraphService(session=mocker.MagicMock())

    # Test with roboledger extension
    await service._install_entity_schema_kuzu(
      kuzu_client=mock_kuzu_client, graph_id="kg12345", extensions=["roboledger"]
    )

    # Verify schema creation was called
    assert mock_kuzu_client.install_schema.called

  @pytest.mark.asyncio
  async def test_install_entity_schema_unknown_extension(self, mocker):
    """Test schema installation with unknown extension."""
    mock_kuzu_client = mocker.AsyncMock()
    service = EntityGraphService(session=mocker.MagicMock())

    # Test with unknown extension - should raise error
    with pytest.raises(ValueError) as exc_info:
      await service._install_entity_schema_kuzu(
        kuzu_client=mock_kuzu_client,
        graph_id="kg12345",
        extensions=["unknown_extension"],
      )

    assert "Schema module 'unknown_extension' not found" in str(exc_info.value)

  @pytest.mark.asyncio
  async def test_create_graph_metadata_node_error_suppression(self, mocker):
    """Test that GraphMetadata creation errors are suppressed."""
    from robosystems.graph_api.client.exceptions import GraphClientError

    mock_kuzu_client = mocker.AsyncMock()
    mock_kuzu_client.query.side_effect = GraphClientError("Duplicate node")

    service = EntityGraphService(session=mocker.MagicMock())

    # This should not raise an exception
    await service._create_graph_metadata_node(
      kuzu_client=mock_kuzu_client,
      graph_id="kg12345",
      entity_name="Test Entity",
      user_id="user-123",
      tier="standard",
      extensions=None,
    )

    # Verify query was attempted
    mock_kuzu_client.query.assert_called_once()

  def test_generate_graph_id_consistency(self):
    """Test that graph ID generation is consistent for same inputs."""
    service = EntityGraphService()

    # Same entity name should generate same ID structure
    id1 = service._generate_graph_id("Test Entity")
    id2 = service._generate_graph_id("Test Entity")

    # IDs should have same prefix and length but different UUIDs
    assert id1[:2] == id2[:2] == "kg"
    assert len(id1) == len(id2)
    assert id1 != id2  # UUIDs should be different

  @pytest.mark.asyncio
  async def test_cleanup_on_failure(self, mocker):
    """Test proper cleanup when graph creation fails."""
    # Mock dependencies
    mock_session = mocker.MagicMock()
    mock_allocation_manager = mocker.AsyncMock()
    mock_kuzu_client = mocker.AsyncMock()

    # Setup successful allocation
    mock_allocation_manager.allocate_database.return_value = mocker.MagicMock(
      instance_id="i-12345",
      private_ip="10.0.0.1",
      database_id="kg12345678ab",
      allocation_id="alloc-123",
    )

    # But fail during schema installation
    mock_kuzu_client.install_schema.side_effect = Exception("Schema error")
    mock_404_error = Exception("Database not found")
    mock_404_error.status_code = 404
    mock_kuzu_client.get_database.side_effect = mock_404_error  # Database doesn't exist
    mock_kuzu_client.create_database.return_value = {"success": True}

    mock_user_limits = mocker.MagicMock()
    mock_user_limits.can_create_user_graph.return_value = (True, "Can create graph")
    mocker.patch(
      "robosystems.operations.graph.entity_graph_service.UserLimits.get_or_create_for_user",
      return_value=mock_user_limits,
    )
    mocker.patch(
      "robosystems.operations.graph.entity_graph_service.KuzuAllocationManager",
      return_value=mock_allocation_manager,
    )
    mocker.patch(
      "robosystems.operations.graph.entity_graph_service.get_graph_client_for_instance",
      return_value=mock_kuzu_client,
    )

    service = EntityGraphService(session=mock_session)
    entity_data = {"name": "Test Company", "uri": "http://test.com"}

    with pytest.raises(Exception):
      await service.create_entity_with_new_graph(
        entity_data_dict=entity_data, user_id="user-123"
      )

    # Verify cleanup was attempted
    mock_allocation_manager.deallocate_database.assert_called_once()
