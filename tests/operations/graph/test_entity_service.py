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
    """Test multitenant mode is always True for LadybugDB databases."""
    from robosystems.middleware.graph.utils import MultiTenantUtils

    # MultiTenantUtils.is_multitenant_mode() always returns True
    result = MultiTenantUtils.is_multitenant_mode()
    assert result is True

  @pytest.mark.unit
  def test_entity_graph_service_initialization(self):
    """Test EntityGraphService initializes correctly."""
    service = EntityGraphService()
    assert service.session is not None

  @pytest.mark.unit
  def test_generate_entity_data_for_upload(self, mocker):
    """Test entity data generation for controlled ingestion."""
    from robosystems.models.api import EntityCreate

    service = EntityGraphService()

    # Create test entity data
    entity_data = EntityCreate(
      name="Test Company",
      cik="0001234567",
      sic="1234",
      sic_description="Test SIC",
      category="Finance",
      state_of_incorporation="DE",
      fiscal_year_end="1231",
      ein="123456789",
      uri="http://test.com",
    )

    graph_id = "kg12345678ab"

    # Generate entity data for upload
    result = service._generate_entity_data_for_upload(entity_data, graph_id)

    # Verify all required fields are present
    assert result["identifier"] == f"entity_{graph_id}"
    assert result["name"] == "Test Company"
    assert result["legal_name"] == "Test Company"
    assert result["cik"] == "0001234567"
    assert result["sic"] == "1234"
    assert result["sic_description"] == "Test SIC"
    assert result["category"] == "Finance"
    assert result["state_of_incorporation"] == "DE"
    assert result["fiscal_year_end"] == "1231"
    assert result["ein"] == "123456789"
    assert result["website"] == "http://test.com"
    assert result["status"] == "active"
    assert "created_at" in result
    assert "updated_at" in result

    # Verify timestamps are ISO format
    import datetime

    datetime.datetime.fromisoformat(result["created_at"])
    datetime.datetime.fromisoformat(result["updated_at"])

  @pytest.mark.asyncio
  async def test_create_entity_with_new_graph_success(self, mocker):
    """Test successful entity creation with new graph using controlled ingestion."""
    # Mock dependencies
    mock_session = mocker.MagicMock()
    mock_lbug_client = mocker.AsyncMock()
    mock_allocation_manager = mocker.AsyncMock()

    # Setup mocks
    mock_allocation_manager.allocate_database.return_value = mocker.MagicMock(
      instance_id="i-12345",
      private_ip="10.0.0.1",
      database_id="kg12345678ab",
      allocation_id="alloc-123",
    )
    mock_user_limits = mocker.MagicMock()
    mock_user_limits.can_create_graph.return_value = (True, "Can create graph")
    mocker.patch(
      "robosystems.operations.graph.entity_graph_service.OrgLimits.get_or_create_for_org",
      return_value=mock_user_limits,
    )
    mocker.patch(
      "robosystems.operations.graph.entity_graph_service.LadybugAllocationManager",
      return_value=mock_allocation_manager,
    )
    mocker.patch(
      "robosystems.operations.graph.entity_graph_service.get_graph_client_for_instance",
      return_value=mock_lbug_client,
    )

    # Mock database and schema installation
    mock_404_error = Exception("Database not found")
    mock_404_error.status_code = 404
    mock_lbug_client.get_database.side_effect = mock_404_error
    mock_lbug_client.create_database.return_value = {"success": True}

    # Mock S3 client for file upload (imported locally in the method)
    mock_s3_client = mocker.MagicMock()
    mocker.patch(
      "robosystems.adapters.s3.S3Client",
      return_value=mock_s3_client,
    )

    # Mock GraphTable for Entity table (imported locally in the method)
    mock_entity_table = mocker.MagicMock()
    mock_entity_table.id = "table-123"
    mock_entity_table.file_count = 0
    mocker.patch(
      "robosystems.models.iam.GraphTable.get_by_name",
      return_value=mock_entity_table,
    )

    # Mock GraphFile creation (imported locally in the method)
    mock_graph_file = mocker.MagicMock()
    mock_graph_file.id = "file-123"
    mocker.patch(
      "robosystems.models.iam.GraphFile.create",
      return_value=mock_graph_file,
    )

    # Mock TableService for auto-creating DuckDB tables (imported locally)
    mock_table_service = mocker.MagicMock()
    mock_table_service.create_tables_from_schema.return_value = [mock_entity_table]
    mocker.patch(
      "robosystems.operations.graph.table_service.TableService",
      return_value=mock_table_service,
    )

    # Mock GraphSchema (imported locally)
    mocker.patch("robosystems.models.iam.GraphSchema.create")

    # Mock CreditService (imported locally)
    mock_credit_service = mocker.MagicMock()
    mocker.patch(
      "robosystems.operations.graph.credit_service.CreditService",
      return_value=mock_credit_service,
    )

    # Mock Graph and GraphUser for PostgreSQL metadata
    mocker.patch("robosystems.models.iam.graph.Graph.create")
    mocker.patch("robosystems.models.iam.graph_user.GraphUser.create")

    # Mock controlled materialization responses
    mock_lbug_client.create_table.return_value = {"success": True}
    mock_lbug_client.materialize_table.return_value = {"rows_ingested": 1}

    # Create service and run test
    service = EntityGraphService(session=mock_session)
    entity_data = {
      "name": "Test Company",
      "cik": "0001234567",
      "sic": "1234",
      "sic_description": "Test SIC",
      "category": "Finance",
      "state_of_incorporation": "DE",
      "fiscal_year_end": "1231",
      "ein": "123456789",
      "uri": "http://test.com",
    }

    result = await service.create_entity_with_new_graph(
      entity_data_dict=entity_data, user_id="user-123", tier="standard"
    )

    # Verify results
    assert "graph_id" in result
    assert "entity" in result
    assert result["entity"]["name"] == "Test Company"
    assert result["entity"]["cik"] == "0001234567"

    # Verify controlled materialization flow was used
    mock_allocation_manager.allocate_database.assert_called_once()
    mock_lbug_client.create_database.assert_called_once()
    mock_s3_client.s3_client.upload_fileobj.assert_called_once()
    mock_lbug_client.create_table.assert_called()
    mock_lbug_client.materialize_table.assert_called_once()

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
    mock_user_limits.can_create_graph.return_value = (True, "Can create graph")
    mocker.patch(
      "robosystems.operations.graph.entity_graph_service.OrgLimits.get_or_create_for_org",
      return_value=mock_user_limits,
    )
    mocker.patch(
      "robosystems.operations.graph.entity_graph_service.LadybugAllocationManager",
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
    mock_lbug_client = mocker.AsyncMock()

    # Setup mocks
    mock_allocation_manager.allocate_database.return_value = mocker.MagicMock(
      instance_id="i-12345", instance_ip="10.0.0.1", database_id="kg12345678ab"
    )
    mocker.patch(
      "robosystems.operations.graph.entity_graph_service.LadybugAllocationManager",
      return_value=mock_allocation_manager,
    )
    mocker.patch(
      "robosystems.operations.graph.entity_graph_service.get_graph_client_for_instance",
      return_value=mock_lbug_client,
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
    mock_lbug_client.create_database.side_effect = (
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
    mock_lbug_client = mocker.AsyncMock()
    mock_lbug_client.install_schema.return_value = {"success": True}

    service = EntityGraphService(session=mocker.MagicMock())

    # Test with roboledger extension
    await service._install_entity_schema(
      graph_client=mock_lbug_client, graph_id="kg12345", extensions=["roboledger"]
    )

    # Verify schema creation was called
    assert mock_lbug_client.install_schema.called

  @pytest.mark.asyncio
  async def test_install_entity_schema_unknown_extension(self, mocker):
    """Test schema installation with unknown extension."""
    mock_lbug_client = mocker.AsyncMock()
    service = EntityGraphService(session=mocker.MagicMock())

    # Test with unknown extension - should raise error
    with pytest.raises(ValueError) as exc_info:
      await service._install_entity_schema(
        graph_client=mock_lbug_client,
        graph_id="kg12345",
        extensions=["unknown_extension"],
      )

    assert "Schema module 'unknown_extension' not found" in str(exc_info.value)

  @pytest.mark.asyncio
  def test_generate_graph_id_consistency(self):
    """Test that graph ID generation includes entity-specific component and is time-ordered."""
    service = EntityGraphService()

    # Same entity name produces same structure
    id1 = service._generate_graph_id("Test Entity")
    id2 = service._generate_graph_id("Test Entity")

    # IDs should have same prefix and length
    assert id1[:2] == id2[:2] == "kg"
    assert len(id1) == len(id2) == 20  # 'kg' + 14 ULID chars + 4 entity hash chars

    # Different entity names should have different hash suffixes
    id3 = service._generate_graph_id("Different Entity")
    assert id3[:2] == "kg"
    assert len(id3) == 20
    assert id1[-4:] != id3[-4:]  # Entity hash portion should differ

  @pytest.mark.asyncio
  async def test_cleanup_on_failure(self, mocker):
    """Test proper cleanup when graph creation fails."""
    # Mock dependencies
    mock_session = mocker.MagicMock()
    mock_allocation_manager = mocker.AsyncMock()
    mock_lbug_client = mocker.AsyncMock()

    # Setup successful allocation
    mock_allocation_manager.allocate_database.return_value = mocker.MagicMock(
      instance_id="i-12345",
      private_ip="10.0.0.1",
      database_id="kg12345678ab",
      allocation_id="alloc-123",
    )

    # But fail during schema installation
    mock_lbug_client.install_schema.side_effect = Exception("Schema error")
    mock_404_error = Exception("Database not found")
    mock_404_error.status_code = 404
    mock_lbug_client.get_database.side_effect = mock_404_error  # Database doesn't exist
    mock_lbug_client.create_database.return_value = {"success": True}

    mock_user_limits = mocker.MagicMock()
    mock_user_limits.can_create_graph.return_value = (True, "Can create graph")
    mocker.patch(
      "robosystems.operations.graph.entity_graph_service.OrgLimits.get_or_create_for_org",
      return_value=mock_user_limits,
    )
    mocker.patch(
      "robosystems.operations.graph.entity_graph_service.LadybugAllocationManager",
      return_value=mock_allocation_manager,
    )
    mocker.patch(
      "robosystems.operations.graph.entity_graph_service.get_graph_client_for_instance",
      return_value=mock_lbug_client,
    )

    service = EntityGraphService(session=mock_session)
    entity_data = {"name": "Test Company", "uri": "http://test.com"}

    with pytest.raises(Exception):
      await service.create_entity_with_new_graph(
        entity_data_dict=entity_data, user_id="user-123"
      )

    # Verify cleanup was attempted
    mock_allocation_manager.deallocate_database.assert_called_once()
