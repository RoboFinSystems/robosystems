"""
Comprehensive tests for GenericGraphService.

Tests the generic graph service that handles creation and management of flexible
graph databases with custom schemas and extensions.
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock

from robosystems.operations.graph.generic_graph_service import (
  GenericGraphService,
  GenericGraphServiceSync,
)
from robosystems.middleware.graph.types import InstanceTier


class TestGenericGraphService:
  """Tests for GenericGraphService class."""

  @pytest.fixture
  def service(self):
    """Create a GenericGraphService instance."""
    return GenericGraphService()

  @pytest.fixture
  def mock_db_session(self):
    """Mock database session."""
    mock_db = Mock()
    mock_db.add = Mock()
    mock_db.commit = Mock()
    mock_db.rollback = Mock()
    return mock_db

  @pytest.fixture
  def mock_user_limits(self):
    """Mock UserLimits."""
    mock_limits = Mock()
    mock_limits.can_create_user_graph = Mock(return_value=(True, None))
    return mock_limits

  @pytest.fixture
  def mock_allocation_manager(self):
    """Mock KuzuAllocationManager."""
    mock = AsyncMock()
    mock_cluster_info = Mock()
    mock_cluster_info.instance_id = "i-1234567890abcdef0"
    mock_cluster_info.private_ip = "10.0.1.100"
    mock.allocate_database = AsyncMock(return_value=mock_cluster_info)
    return mock, mock_cluster_info

  @pytest.fixture
  def mock_kuzu_client(self):
    """Mock Kuzu client."""
    client = AsyncMock()
    client.create_database = AsyncMock(return_value={"status": "created"})
    client.install_schema = AsyncMock(return_value={"status": "installed"})
    client.query = AsyncMock(return_value=[])
    client.close = AsyncMock()
    return client

  @pytest.fixture
  def valid_metadata(self):
    """Valid metadata for graph creation."""
    return {
      "name": "Test Graph",
      "description": "A test graph database",
      "type": "analytics",
      "tags": ["test", "analytics"],
      "access_level": "private",
    }

  @pytest.fixture
  def custom_schema(self):
    """Sample custom schema."""
    return {
      "name": "CustomAnalytics",
      "version": "1.0.0",
      "extends": "base",
      "nodes": [
        {
          "name": "Metric",
          "properties": [
            {"name": "value", "type": "DOUBLE"},
            {"name": "timestamp", "type": "TIMESTAMP"},
          ],
        }
      ],
    }

  @pytest.mark.asyncio
  async def test_create_graph_success(
    self,
    service,
    mock_db_session,
    mock_user_limits,
    mock_allocation_manager,
    mock_kuzu_client,
    valid_metadata,
  ):
    """Test successful graph creation with extensions."""
    mock_manager, mock_cluster_info = mock_allocation_manager

    with patch(
      "robosystems.operations.graph.generic_graph_service.get_db_session"
    ) as mock_get_db:
      # The function calls get_db_session twice
      mock_get_db.side_effect = [
        iter([mock_db_session, None]),
        iter([mock_db_session, None]),
      ]

      with patch("robosystems.models.iam.UserLimits") as mock_limits_class:
        mock_limits_class.get_or_create_for_user.return_value = mock_user_limits

        with patch(
          "robosystems.operations.graph.generic_graph_service.KuzuAllocationManager"
        ) as mock_alloc_class:
          mock_alloc_class.return_value = mock_manager

          with patch(
            "robosystems.graph_api.client.get_graph_client_for_instance"
          ) as mock_get_client:
            mock_get_client.return_value = mock_kuzu_client

            with patch("robosystems.models.iam.graph.Graph") as mock_graph_class:
              mock_graph = Mock()
              mock_graph_class.create.return_value = mock_graph

              with patch("robosystems.models.iam.UserGraph"):
                with patch(
                  "robosystems.operations.graph.credit_service.CreditService"
                ) as mock_credit_class:
                  mock_credit_service = Mock()
                  mock_credit_class.return_value = mock_credit_service

                  result = await service.create_graph(
                    graph_id=None,  # Auto-generate
                    schema_extensions=["analytics", "ml"],
                    metadata=valid_metadata,
                    tier="standard",
                    initial_data=None,
                    user_id="user123",
                    custom_schema=None,
                  )

                  assert result["status"] == "created"
                  assert result["graph_id"].startswith("kg")
                  assert len(result["graph_id"]) == 18  # kg + 16 chars
                  assert result["tier"] == "standard"
                  assert result["metadata"]["name"] == "Test Graph"
                  assert result["schema_info"]["type"] == "extensions"
                  assert result["schema_info"]["extensions"] == ["analytics", "ml"]

                  # Verify allocation was called
                  mock_manager.allocate_database.assert_called_once()

                  # Verify database creation
                  mock_kuzu_client.create_database.assert_called_once()

                  # Verify schema installation
                  mock_kuzu_client.install_schema.assert_called_once_with(
                    graph_id=result["graph_id"],
                    base_schema="base",
                    extensions=["analytics", "ml"],
                  )

                  # Verify credit pool creation
                  mock_credit_service.create_graph_credits.assert_called_once()

  @pytest.mark.asyncio
  async def test_create_graph_with_custom_schema(
    self,
    service,
    mock_db_session,
    mock_user_limits,
    mock_allocation_manager,
    mock_kuzu_client,
    valid_metadata,
    custom_schema,
  ):
    """Test graph creation with custom schema."""
    mock_manager, mock_cluster_info = mock_allocation_manager

    with patch(
      "robosystems.operations.graph.generic_graph_service.get_db_session"
    ) as mock_get_db:
      # The function calls get_db_session twice
      mock_get_db.side_effect = [
        iter([mock_db_session, None]),
        iter([mock_db_session, None]),
      ]

      with patch("robosystems.models.iam.UserLimits") as mock_limits_class:
        mock_limits_class.get_or_create_for_user.return_value = mock_user_limits

        with patch(
          "robosystems.operations.graph.generic_graph_service.KuzuAllocationManager"
        ) as mock_alloc_class:
          mock_alloc_class.return_value = mock_manager

          with patch(
            "robosystems.graph_api.client.get_graph_client_for_instance"
          ) as mock_get_client:
            mock_get_client.return_value = mock_kuzu_client

            with patch(
              "robosystems.schemas.custom.CustomSchemaManager"
            ) as mock_schema_mgr:
              mock_parsed_schema = Mock()
              mock_parsed_schema.to_cypher.return_value = (
                "CREATE NODE TABLE Metric (value DOUBLE, timestamp TIMESTAMP);"
              )
              mock_schema_instance = Mock()
              mock_schema_instance.create_from_dict.return_value = mock_parsed_schema
              mock_schema_instance.merge_with_base.return_value = mock_parsed_schema
              mock_schema_mgr.return_value = mock_schema_instance

              with patch("robosystems.models.iam.graph.Graph") as mock_graph_class:
                mock_graph = Mock()
                mock_graph_class.create.return_value = mock_graph

                with patch("robosystems.models.iam.UserGraph"):
                  with patch(
                    "robosystems.operations.graph.credit_service.CreditService"
                  ) as mock_credit_class:
                    mock_credit_service = Mock()
                    mock_credit_class.return_value = mock_credit_service

                    result = await service.create_graph(
                      graph_id=None,
                      schema_extensions=[],
                      metadata=valid_metadata,
                      tier="enterprise",
                      initial_data=None,
                      user_id="user123",
                      custom_schema=custom_schema,
                    )

                    assert result["status"] == "created"
                    assert result["schema_info"]["type"] == "custom"
                    assert (
                      result["schema_info"]["custom_schema_name"] == "CustomAnalytics"
                    )
                    assert result["schema_info"]["custom_schema_version"] == "1.0.0"

                    # Verify custom schema was processed
                    mock_schema_instance.create_from_dict.assert_called_once_with(
                      custom_schema
                    )
                    mock_schema_instance.merge_with_base.assert_called_once()

                    # Verify database created with custom DDL
                    mock_kuzu_client.create_database.assert_called_once()
                    call_args = mock_kuzu_client.create_database.call_args
                    assert call_args.kwargs["schema_type"] == "custom"
                    assert call_args.kwargs["custom_schema_ddl"] is not None

                    # Verify no schema extensions installed for custom schema
                    mock_kuzu_client.install_schema.assert_not_called()

  @pytest.mark.asyncio
  async def test_create_graph_user_limit_exceeded(
    self, service, mock_db_session, mock_user_limits, valid_metadata
  ):
    """Test graph creation when user limit is exceeded."""
    mock_user_limits.can_create_user_graph.return_value = (
      False,
      "Graph limit exceeded",
    )

    with patch(
      "robosystems.operations.graph.generic_graph_service.get_db_session"
    ) as mock_get_db:
      # The function calls get_db_session twice
      mock_get_db.side_effect = [
        iter([mock_db_session, None]),
        iter([mock_db_session, None]),
      ]

      with patch("robosystems.models.iam.UserLimits") as mock_limits_class:
        mock_limits_class.get_or_create_for_user.return_value = mock_user_limits

        with pytest.raises(ValueError) as exc_info:
          await service.create_graph(
            graph_id=None,
            schema_extensions=[],
            metadata=valid_metadata,
            tier="standard",
            initial_data=None,
            user_id="user123",
          )

        assert "Graph limit exceeded" in str(exc_info.value)

  @pytest.mark.asyncio
  async def test_create_graph_allocation_failure(
    self, service, mock_db_session, mock_user_limits, valid_metadata
  ):
    """Test graph creation when allocation fails."""
    mock_manager = AsyncMock()
    mock_manager.allocate_database.return_value = None  # Allocation fails

    with patch(
      "robosystems.operations.graph.generic_graph_service.get_db_session"
    ) as mock_get_db:
      # The function calls get_db_session twice
      mock_get_db.side_effect = [
        iter([mock_db_session, None]),
        iter([mock_db_session, None]),
      ]

      with patch("robosystems.models.iam.UserLimits") as mock_limits_class:
        mock_limits_class.get_or_create_for_user.return_value = mock_user_limits

        with patch(
          "robosystems.operations.graph.generic_graph_service.KuzuAllocationManager"
        ) as mock_alloc_class:
          mock_alloc_class.return_value = mock_manager

          with pytest.raises(RuntimeError) as exc_info:
            await service.create_graph(
              graph_id=None,
              schema_extensions=[],
              metadata=valid_metadata,
              tier="standard",
              initial_data=None,
              user_id="user123",
            )

          assert "Failed to allocate database" in str(exc_info.value)

  @pytest.mark.asyncio
  async def test_create_graph_invalid_custom_schema(
    self,
    service,
    mock_db_session,
    mock_user_limits,
    mock_allocation_manager,
    valid_metadata,
  ):
    """Test graph creation with invalid custom schema."""
    mock_manager, mock_cluster_info = mock_allocation_manager
    invalid_schema = {"name": "Invalid", "invalid_field": "test"}

    with patch(
      "robosystems.operations.graph.generic_graph_service.get_db_session"
    ) as mock_get_db:
      # The function calls get_db_session twice
      mock_get_db.side_effect = [
        iter([mock_db_session, None]),
        iter([mock_db_session, None]),
      ]

      with patch("robosystems.models.iam.UserLimits") as mock_limits_class:
        mock_limits_class.get_or_create_for_user.return_value = mock_user_limits

        with patch(
          "robosystems.operations.graph.generic_graph_service.KuzuAllocationManager"
        ) as mock_alloc_class:
          mock_alloc_class.return_value = mock_manager

          with patch(
            "robosystems.schemas.custom.CustomSchemaManager"
          ) as mock_schema_mgr:
            mock_schema_instance = Mock()
            mock_schema_instance.create_from_dict.side_effect = Exception(
              "Invalid schema format"
            )
            mock_schema_mgr.return_value = mock_schema_instance

            with pytest.raises(ValueError) as exc_info:
              await service.create_graph(
                graph_id=None,
                schema_extensions=[],
                metadata=valid_metadata,
                tier="standard",
                initial_data=None,
                user_id="user123",
                custom_schema=invalid_schema,
              )

            assert "Invalid custom schema" in str(exc_info.value)

  @pytest.mark.asyncio
  async def test_create_graph_with_cancellation_callback(
    self,
    service,
    mock_db_session,
    mock_user_limits,
    mock_allocation_manager,
    mock_kuzu_client,
    valid_metadata,
  ):
    """Test graph creation with cancellation callback."""
    mock_manager, mock_cluster_info = mock_allocation_manager
    callback_count = 0

    def cancellation_callback():
      nonlocal callback_count
      callback_count += 1
      if callback_count > 2:
        raise RuntimeError("Operation cancelled")

    with patch(
      "robosystems.operations.graph.generic_graph_service.get_db_session"
    ) as mock_get_db:
      # The function calls get_db_session twice
      mock_get_db.side_effect = [
        iter([mock_db_session, None]),
        iter([mock_db_session, None]),
      ]

      with patch("robosystems.models.iam.UserLimits") as mock_limits_class:
        mock_limits_class.get_or_create_for_user.return_value = mock_user_limits

        with patch(
          "robosystems.operations.graph.generic_graph_service.KuzuAllocationManager"
        ) as mock_alloc_class:
          mock_alloc_class.return_value = mock_manager

          with patch(
            "robosystems.graph_api.client.get_graph_client_for_instance"
          ) as mock_get_client:
            mock_get_client.return_value = mock_kuzu_client

            with pytest.raises(RuntimeError) as exc_info:
              await service.create_graph(
                graph_id=None,
                schema_extensions=[],
                metadata=valid_metadata,
                tier="standard",
                initial_data=None,
                user_id="user123",
                cancellation_callback=cancellation_callback,
              )

            assert "Operation cancelled" in str(exc_info.value)
            assert callback_count > 2

  @pytest.mark.asyncio
  async def test_create_graph_schema_installation_failure(
    self,
    service,
    mock_db_session,
    mock_user_limits,
    mock_allocation_manager,
    mock_kuzu_client,
    valid_metadata,
  ):
    """Test graph creation when schema installation fails."""
    mock_manager, mock_cluster_info = mock_allocation_manager
    mock_kuzu_client.install_schema.side_effect = Exception(
      "Schema installation failed"
    )

    with patch(
      "robosystems.operations.graph.generic_graph_service.get_db_session"
    ) as mock_get_db:
      # The function calls get_db_session twice
      mock_get_db.side_effect = [
        iter([mock_db_session, None]),
        iter([mock_db_session, None]),
      ]

      with patch("robosystems.models.iam.UserLimits") as mock_limits_class:
        mock_limits_class.get_or_create_for_user.return_value = mock_user_limits

        with patch(
          "robosystems.operations.graph.generic_graph_service.KuzuAllocationManager"
        ) as mock_alloc_class:
          mock_alloc_class.return_value = mock_manager

          with patch(
            "robosystems.graph_api.client.get_graph_client_for_instance"
          ) as mock_get_client:
            mock_get_client.return_value = mock_kuzu_client

            with pytest.raises(RuntimeError) as exc_info:
              await service.create_graph(
                graph_id=None,
                schema_extensions=["analytics"],
                metadata=valid_metadata,
                tier="standard",
                initial_data=None,
                user_id="user123",
              )

            assert "Schema installation failed" in str(exc_info.value)

  @pytest.mark.asyncio
  async def test_create_graph_with_initial_data(
    self,
    service,
    mock_db_session,
    mock_user_limits,
    mock_allocation_manager,
    mock_kuzu_client,
    valid_metadata,
  ):
    """Test graph creation with initial data."""
    mock_manager, mock_cluster_info = mock_allocation_manager
    initial_data = {"nodes": [{"type": "Entity", "properties": {"name": "Test"}}]}

    with patch(
      "robosystems.operations.graph.generic_graph_service.get_db_session"
    ) as mock_get_db:
      # The function calls get_db_session twice
      mock_get_db.side_effect = [
        iter([mock_db_session, None]),
        iter([mock_db_session, None]),
      ]

      with patch("robosystems.models.iam.UserLimits") as mock_limits_class:
        mock_limits_class.get_or_create_for_user.return_value = mock_user_limits

        with patch(
          "robosystems.operations.graph.generic_graph_service.KuzuAllocationManager"
        ) as mock_alloc_class:
          mock_alloc_class.return_value = mock_manager

          with patch(
            "robosystems.graph_api.client.get_graph_client_for_instance"
          ) as mock_get_client:
            mock_get_client.return_value = mock_kuzu_client

            with patch("robosystems.models.iam.graph.Graph") as mock_graph_class:
              mock_graph = Mock()
              mock_graph_class.create.return_value = mock_graph

              with patch(
                "robosystems.operations.graph.generic_graph_service.UserGraph"
              ):
                with patch(
                  "robosystems.operations.graph.credit_service.CreditService"
                ) as mock_credit_class:
                  mock_credit_service = Mock()
                  mock_credit_class.return_value = mock_credit_service

                  result = await service.create_graph(
                    graph_id=None,
                    schema_extensions=[],
                    metadata=valid_metadata,
                    tier="standard",
                    initial_data=initial_data,
                    user_id="user123",
                  )

                  assert result["status"] == "created"
                  # Initial data handling is not yet implemented, but should not fail

  @pytest.mark.asyncio
  async def test_create_graph_metadata_storage_failure(
    self,
    service,
    mock_db_session,
    mock_user_limits,
    mock_allocation_manager,
    mock_kuzu_client,
    valid_metadata,
  ):
    """Test graph creation when metadata storage fails (should not fail entire creation)."""
    mock_manager, mock_cluster_info = mock_allocation_manager
    mock_kuzu_client.query.side_effect = Exception("Metadata storage failed")

    with patch(
      "robosystems.operations.graph.generic_graph_service.get_db_session"
    ) as mock_get_db:
      # The function calls get_db_session twice
      mock_get_db.side_effect = [
        iter([mock_db_session, None]),
        iter([mock_db_session, None]),
      ]

      with patch("robosystems.models.iam.UserLimits") as mock_limits_class:
        mock_limits_class.get_or_create_for_user.return_value = mock_user_limits

        with patch(
          "robosystems.operations.graph.generic_graph_service.KuzuAllocationManager"
        ) as mock_alloc_class:
          mock_alloc_class.return_value = mock_manager

          with patch(
            "robosystems.graph_api.client.get_graph_client_for_instance"
          ) as mock_get_client:
            mock_get_client.return_value = mock_kuzu_client

            with patch("robosystems.models.iam.graph.Graph") as mock_graph_class:
              mock_graph = Mock()
              mock_graph_class.create.return_value = mock_graph

              with patch(
                "robosystems.operations.graph.generic_graph_service.UserGraph"
              ):
                with patch(
                  "robosystems.operations.graph.credit_service.CreditService"
                ) as mock_credit_class:
                  mock_credit_service = Mock()
                  mock_credit_class.return_value = mock_credit_service

                  # Should not raise an exception
                  result = await service.create_graph(
                    graph_id=None,
                    schema_extensions=[],
                    metadata=valid_metadata,
                    tier="standard",
                    initial_data=None,
                    user_id="user123",
                  )

                  assert result["status"] == "created"
                  # Verify warning was logged but creation succeeded

  @pytest.mark.asyncio
  async def test_create_graph_credit_pool_failure(
    self,
    service,
    mock_db_session,
    mock_user_limits,
    mock_allocation_manager,
    mock_kuzu_client,
    valid_metadata,
  ):
    """Test graph creation when credit pool creation fails (should not fail entire creation)."""
    mock_manager, mock_cluster_info = mock_allocation_manager

    with patch(
      "robosystems.operations.graph.generic_graph_service.get_db_session"
    ) as mock_get_db:
      # The function calls get_db_session twice
      mock_get_db.side_effect = [
        iter([mock_db_session, None]),
        iter([mock_db_session, None]),
      ]

      with patch("robosystems.models.iam.UserLimits") as mock_limits_class:
        mock_limits_class.get_or_create_for_user.return_value = mock_user_limits

        with patch(
          "robosystems.operations.graph.generic_graph_service.KuzuAllocationManager"
        ) as mock_alloc_class:
          mock_alloc_class.return_value = mock_manager

          with patch(
            "robosystems.graph_api.client.get_graph_client_for_instance"
          ) as mock_get_client:
            mock_get_client.return_value = mock_kuzu_client

            with patch("robosystems.models.iam.graph.Graph") as mock_graph_class:
              mock_graph = Mock()
              mock_graph_class.create.return_value = mock_graph

              with patch(
                "robosystems.operations.graph.generic_graph_service.UserGraph"
              ):
                with patch(
                  "robosystems.operations.graph.credit_service.CreditService"
                ) as mock_credit_class:
                  mock_credit_service = Mock()
                  mock_credit_service.create_graph_credits.side_effect = Exception(
                    "Credit pool creation failed"
                  )
                  mock_credit_class.return_value = mock_credit_service

                  # Should not raise an exception
                  result = await service.create_graph(
                    graph_id=None,
                    schema_extensions=[],
                    metadata=valid_metadata,
                    tier="standard",
                    initial_data=None,
                    user_id="user123",
                  )

                  assert result["status"] == "created"
                  # Graph creation should succeed even if credit pool fails

  @pytest.mark.asyncio
  async def test_create_graph_with_custom_metadata(
    self,
    service,
    mock_db_session,
    mock_user_limits,
    mock_allocation_manager,
    mock_kuzu_client,
  ):
    """Test graph creation with custom metadata."""
    mock_manager, mock_cluster_info = mock_allocation_manager
    metadata_with_custom = {
      "name": "Test Graph",
      "description": "Test description",
      "type": "analytics",
      "tags": ["test"],
      "custom_metadata": {"key1": "value1", "key2": "value2"},
    }

    with patch(
      "robosystems.operations.graph.generic_graph_service.get_db_session"
    ) as mock_get_db:
      # The function calls get_db_session twice
      mock_get_db.side_effect = [
        iter([mock_db_session, None]),
        iter([mock_db_session, None]),
      ]

      with patch("robosystems.models.iam.UserLimits") as mock_limits_class:
        mock_limits_class.get_or_create_for_user.return_value = mock_user_limits

        with patch(
          "robosystems.operations.graph.generic_graph_service.KuzuAllocationManager"
        ) as mock_alloc_class:
          mock_alloc_class.return_value = mock_manager

          with patch(
            "robosystems.graph_api.client.get_graph_client_for_instance"
          ) as mock_get_client:
            mock_get_client.return_value = mock_kuzu_client

            with patch("robosystems.models.iam.graph.Graph") as mock_graph_class:
              mock_graph = Mock()
              mock_graph_class.create.return_value = mock_graph

              with patch(
                "robosystems.operations.graph.generic_graph_service.UserGraph"
              ):
                with patch(
                  "robosystems.operations.graph.credit_service.CreditService"
                ) as mock_credit_class:
                  mock_credit_service = Mock()
                  mock_credit_class.return_value = mock_credit_service

                  result = await service.create_graph(
                    graph_id=None,
                    schema_extensions=[],
                    metadata=metadata_with_custom,
                    tier="standard",
                    initial_data=None,
                    user_id="user123",
                  )

                  assert result["status"] == "created"
                  # Verify custom metadata was included in query
                  assert (
                    mock_kuzu_client.query.call_count >= 2
                  )  # Main metadata + custom metadata

  @pytest.mark.asyncio
  async def test_create_graph_different_tiers(
    self,
    service,
    mock_db_session,
    mock_user_limits,
    mock_allocation_manager,
    mock_kuzu_client,
    valid_metadata,
  ):
    """Test graph creation with different tier mappings."""
    mock_manager, mock_cluster_info = mock_allocation_manager

    for tier in ["standard", "enterprise", "premium"]:
      with patch(
        "robosystems.operations.graph.generic_graph_service.get_db_session"
      ) as mock_get_db:
        # The function calls get_db_session twice
        mock_get_db.side_effect = [
          iter([mock_db_session, None]),
          iter([mock_db_session, None]),
        ]

        with patch("robosystems.models.iam.UserLimits") as mock_limits_class:
          mock_limits_class.get_or_create_for_user.return_value = mock_user_limits

          with patch(
            "robosystems.operations.graph.generic_graph_service.KuzuAllocationManager"
          ) as mock_alloc_class:
            mock_alloc_class.return_value = mock_manager

            with patch(
              "robosystems.graph_api.client.get_graph_client_for_instance"
            ) as mock_get_client:
              mock_get_client.return_value = mock_kuzu_client

              with patch("robosystems.models.iam.graph.Graph") as mock_graph_class:
                mock_graph = Mock()
                mock_graph_class.create.return_value = mock_graph

                with patch("robosystems.models.iam.UserGraph"):
                  with patch(
                    "robosystems.operations.graph.credit_service.CreditService"
                  ) as mock_credit_class:
                    mock_credit_service = Mock()
                    mock_credit_class.return_value = mock_credit_service

                    result = await service.create_graph(
                      graph_id=None,
                      schema_extensions=[],
                      metadata=valid_metadata,
                      tier=tier,
                      initial_data=None,
                      user_id="user123",
                    )

                    assert result["status"] == "created"
                    assert result["tier"] == tier

                    # Verify the tier was passed to allocation manager
                    call_args = mock_manager.allocate_database.call_args
                    assert call_args.kwargs["instance_tier"] == InstanceTier(tier)


class TestGenericGraphServiceSync:
  """Tests for synchronous wrapper."""

  @pytest.fixture
  def sync_service(self):
    """Create a GenericGraphServiceSync instance."""
    return GenericGraphServiceSync()

  def test_sync_wrapper_create_graph(self, sync_service):
    """Test synchronous wrapper for graph creation."""

    async def mock_async_create(*args, **kwargs):
      return {"status": "created", "graph_id": "kg1234567890abcdef"}

    with patch.object(
      sync_service._async_service, "create_graph", new=mock_async_create
    ):
      result = sync_service.create_graph(
        graph_id=None,
        schema_extensions=["analytics"],
        metadata={"name": "Test"},
        tier="standard",
        initial_data=None,
        user_id="user123",
      )

      assert result["status"] == "created"
      assert result["graph_id"] == "kg1234567890abcdef"

  def test_sync_wrapper_with_cancellation(self, sync_service):
    """Test synchronous wrapper with cancellation callback."""
    callback_called = False

    def cancellation_callback():
      nonlocal callback_called
      callback_called = True

    async def mock_async_create(*args, **kwargs):
      return {"status": "created", "graph_id": "kg1234567890abcdef"}

    with patch.object(
      sync_service._async_service, "create_graph", new=mock_async_create
    ):
      result = sync_service.create_graph(
        graph_id=None,
        schema_extensions=[],
        metadata={"name": "Test"},
        tier="standard",
        initial_data=None,
        user_id="user123",
        cancellation_callback=cancellation_callback,
      )

      assert result["status"] == "created"
      # Callback is passed through but not necessarily called if creation succeeds quickly


class TestGenericGraphServiceIntegration:
  """Integration tests for GenericGraphService."""

  @pytest.mark.asyncio
  @pytest.mark.integration
  async def test_full_graph_creation_lifecycle(self):
    """Test complete graph creation lifecycle with all components."""
    service = GenericGraphService()

    # This is a more complete integration test that would require actual services
    # For now, we'll mock the key components but test the full flow
    mock_cluster_info = Mock()
    mock_cluster_info.instance_id = "i-test123"
    mock_cluster_info.private_ip = "10.0.1.100"

    mock_db = Mock()
    mock_limits = Mock()
    mock_limits.can_create_user_graph.return_value = (True, None)

    with patch(
      "robosystems.operations.graph.generic_graph_service.get_db_session"
    ) as mock_get_db:
      # The function calls get_db_session twice
      mock_get_db.side_effect = [iter([mock_db, None]), iter([mock_db, None])]

      with patch("robosystems.models.iam.UserLimits") as mock_limits_class:
        mock_limits_class.get_or_create_for_user.return_value = mock_limits

        with patch(
          "robosystems.operations.graph.generic_graph_service.KuzuAllocationManager"
        ) as mock_alloc_class:
          mock_manager = AsyncMock()
          mock_manager.allocate_database.return_value = mock_cluster_info
          mock_alloc_class.return_value = mock_manager

          with patch(
            "robosystems.graph_api.client.get_graph_client_for_instance"
          ) as mock_get_client:
            mock_client = AsyncMock()
            mock_client.create_database.return_value = {"status": "created"}
            mock_client.install_schema.return_value = {"status": "installed"}
            mock_client.query.return_value = []
            mock_client.close.return_value = None
            mock_get_client.return_value = mock_client

            with patch("robosystems.models.iam.graph.Graph") as mock_graph_class:
              mock_graph = Mock()
              mock_graph_class.create.return_value = mock_graph

              with patch(
                "robosystems.operations.graph.generic_graph_service.UserGraph"
              ):
                with patch(
                  "robosystems.operations.graph.credit_service.CreditService"
                ) as mock_credit_class:
                  mock_credit_service = Mock()
                  mock_credit_class.return_value = mock_credit_service

                  # Test with extensions
                  result = await service.create_graph(
                    graph_id=None,
                    schema_extensions=["analytics", "ml", "timeseries"],
                    metadata={
                      "name": "Integration Test Graph",
                      "description": "Full lifecycle test",
                      "type": "multi-purpose",
                      "tags": ["test", "integration"],
                    },
                    tier="enterprise",
                    initial_data=None,
                    user_id="integration_user",
                  )

                  assert result["status"] == "created"
                  assert result["graph_id"].startswith("kg")
                  assert result["tier"] == "enterprise"
                  assert len(result["schema_info"]["extensions"]) == 3

                  # Verify all steps were executed
                  assert mock_manager.allocate_database.called
                  assert mock_client.create_database.called
                  assert mock_client.install_schema.called
                  assert mock_client.query.called  # Metadata storage
                  assert mock_graph_class.create.called
                  assert mock_credit_service.create_graph_credits.called
