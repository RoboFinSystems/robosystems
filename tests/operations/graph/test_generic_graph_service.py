"""
Tests for GenericGraphService.

These tests focus on the core graph creation functionality with proper mocking
of all dependencies including the new GraphSchema and TableService features.
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock

from robosystems.operations.graph.generic_graph_service import (
  GenericGraphService,
  GenericGraphServiceSync,
)
from robosystems.middleware.graph.types import GraphTier


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
    mock_db.query = Mock()
    return mock_db

  @pytest.fixture
  def mock_user_limits(self):
    """Mock UserLimits."""
    mock_limits = Mock()
    mock_limits.can_create_user_graph = Mock(return_value=(True, None))
    return mock_limits

  @pytest.fixture
  def mock_cluster_info(self):
    """Mock cluster info."""
    mock_info = Mock()
    mock_info.instance_id = "i-1234567890abcdef0"
    mock_info.private_ip = "10.0.1.100"
    return mock_info

  @pytest.fixture
  def mock_kuzu_client(self):
    """Mock Kuzu client."""
    client = AsyncMock()
    client.create_database = AsyncMock(return_value={"status": "created"})
    client.install_schema = AsyncMock(return_value={"status": "installed"})
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

  @pytest.mark.asyncio
  async def test_create_graph_with_extensions_success(
    self,
    service,
    mock_db_session,
    mock_user_limits,
    mock_cluster_info,
    mock_kuzu_client,
    valid_metadata,
  ):
    """Test successful graph creation with schema extensions."""
    with patch(
      "robosystems.operations.graph.generic_graph_service.get_db_session"
    ) as mock_get_db:
      # Mock all 4 calls to get_db_session
      mock_get_db.side_effect = [
        iter([mock_db_session, None]),  # User limits check
        iter([mock_db_session, None]),  # Schema persistence
        iter([mock_db_session, None]),  # Auto-table creation
        iter([mock_db_session, None]),  # User-graph relationship
      ]

      with patch("robosystems.models.iam.UserLimits") as mock_limits_class:
        mock_limits_class.get_or_create_for_user.return_value = mock_user_limits

        with patch(
          "robosystems.operations.graph.generic_graph_service.KuzuAllocationManager"
        ) as mock_alloc_class:
          mock_manager = AsyncMock()
          mock_manager.allocate_database = AsyncMock(return_value=mock_cluster_info)
          mock_alloc_class.return_value = mock_manager

          with patch(
            "robosystems.graph_api.client.get_graph_client_for_instance"
          ) as mock_get_client:
            mock_get_client.return_value = mock_kuzu_client

            with patch("robosystems.models.iam.graph.Graph") as mock_graph_class:
              mock_graph = Mock()
              mock_graph_class.create.return_value = mock_graph

              with patch("robosystems.models.iam.UserGraph"):
                with patch("robosystems.models.iam.GraphSchema") as mock_schema_class:
                  mock_schema_class.create.return_value = Mock()

                  with patch(
                    "robosystems.operations.graph.table_service.TableService"
                  ) as mock_table_service_class:
                    mock_table_service = Mock()
                    mock_table_service.create_tables_from_schema.return_value = []
                    mock_table_service_class.return_value = mock_table_service

                    with patch(
                      "robosystems.operations.graph.credit_service.CreditService"
                    ) as mock_credit_class:
                      mock_credit_service = Mock()
                      mock_credit_class.return_value = mock_credit_service

                      result = await service.create_graph(
                        graph_id=None,
                        schema_extensions=["roboledger", "robofo"],
                        metadata=valid_metadata,
                        tier="kuzu-standard",
                        initial_data=None,
                        user_id="user123",
                        custom_schema=None,
                      )

                      assert result["status"] == "created"
                      assert result["graph_id"].startswith("kg")
                      assert len(result["graph_id"]) == 18
                      assert result["tier"] == "kuzu-standard"
                      assert result["metadata"]["name"] == "Test Graph"
                      assert result["schema_info"]["type"] == "extensions"
                      assert result["schema_info"]["extensions"] == [
                        "roboledger",
                        "robofo",
                      ]

                      # Verify allocation was called with correct tier
                      mock_manager.allocate_database.assert_called_once()
                      call_args = mock_manager.allocate_database.call_args
                      assert (
                        call_args.kwargs["instance_tier"] == GraphTier.KUZU_STANDARD
                      )

                      # Verify database creation
                      mock_kuzu_client.create_database.assert_called_once()

                      # Verify schema installation
                      mock_kuzu_client.install_schema.assert_called_once_with(
                        graph_id=result["graph_id"],
                        base_schema="base",
                        extensions=["roboledger", "robofo"],
                      )

                      # Verify GraphSchema was persisted
                      mock_schema_class.create.assert_called_once()

                      # Verify TableService was called for auto-table creation
                      mock_table_service.create_tables_from_schema.assert_called_once()

                      # Verify credit pool creation
                      mock_credit_service.create_graph_credits.assert_called_once()

  @pytest.mark.asyncio
  async def test_create_graph_with_custom_schema_success(
    self,
    service,
    mock_db_session,
    mock_user_limits,
    mock_cluster_info,
    mock_kuzu_client,
    valid_metadata,
  ):
    """Test successful graph creation with custom schema."""
    custom_schema = {
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

    with patch(
      "robosystems.operations.graph.generic_graph_service.get_db_session"
    ) as mock_get_db:
      mock_get_db.side_effect = [
        iter([mock_db_session, None]),  # User limits
        iter([mock_db_session, None]),  # Schema persistence
        iter([mock_db_session, None]),  # User-graph relationship
      ]

      with patch("robosystems.models.iam.UserLimits") as mock_limits_class:
        mock_limits_class.get_or_create_for_user.return_value = mock_user_limits

        with patch(
          "robosystems.operations.graph.generic_graph_service.KuzuAllocationManager"
        ) as mock_alloc_class:
          mock_manager = AsyncMock()
          mock_manager.allocate_database = AsyncMock(return_value=mock_cluster_info)
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
                  with patch("robosystems.models.iam.GraphSchema") as mock_schema_class:
                    mock_schema_class.create.return_value = Mock()

                    with patch(
                      "robosystems.operations.graph.table_service.TableService"
                    ) as mock_table_service_class:
                      mock_table_service = Mock()
                      mock_table_service.create_tables_from_schema.return_value = [
                        "Metric"
                      ]
                      mock_table_service_class.return_value = mock_table_service

                      with patch(
                        "robosystems.operations.graph.credit_service.CreditService"
                      ) as mock_credit_class:
                        mock_credit_service = Mock()
                        mock_credit_class.return_value = mock_credit_service

                        result = await service.create_graph(
                          graph_id=None,
                          schema_extensions=[],
                          metadata=valid_metadata,
                          tier="kuzu-xlarge",
                          initial_data=None,
                          user_id="user123",
                          custom_schema=custom_schema,
                        )

                        assert result["status"] == "created"
                        assert result["schema_info"]["type"] == "custom"
                        assert (
                          result["schema_info"]["custom_schema_name"]
                          == "CustomAnalytics"
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

                        # Verify auto-table creation happened
                        mock_table_service.create_tables_from_schema.assert_called_once()

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
      mock_get_db.return_value = iter([mock_db_session, None])

      with patch("robosystems.models.iam.UserLimits") as mock_limits_class:
        mock_limits_class.get_or_create_for_user.return_value = mock_user_limits

        with pytest.raises(ValueError) as exc_info:
          await service.create_graph(
            graph_id=None,
            schema_extensions=[],
            metadata=valid_metadata,
            tier="kuzu-standard",
            initial_data=None,
            user_id="user123",
          )

        assert "Graph limit exceeded" in str(exc_info.value)

  @pytest.mark.asyncio
  async def test_create_graph_allocation_failure(
    self, service, mock_db_session, mock_user_limits, valid_metadata
  ):
    """Test graph creation when allocation fails."""
    with patch(
      "robosystems.operations.graph.generic_graph_service.get_db_session"
    ) as mock_get_db:
      mock_get_db.return_value = iter([mock_db_session, None])

      with patch("robosystems.models.iam.UserLimits") as mock_limits_class:
        mock_limits_class.get_or_create_for_user.return_value = mock_user_limits

        with patch(
          "robosystems.operations.graph.generic_graph_service.KuzuAllocationManager"
        ) as mock_alloc_class:
          mock_manager = AsyncMock()
          mock_manager.allocate_database.return_value = None
          mock_alloc_class.return_value = mock_manager

          with pytest.raises(RuntimeError) as exc_info:
            await service.create_graph(
              graph_id=None,
              schema_extensions=[],
              metadata=valid_metadata,
              tier="kuzu-standard",
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
    mock_cluster_info,
    valid_metadata,
  ):
    """Test graph creation with invalid custom schema."""
    invalid_schema = {"name": "Invalid", "invalid_field": "test"}

    with patch(
      "robosystems.operations.graph.generic_graph_service.get_db_session"
    ) as mock_get_db:
      mock_get_db.return_value = iter([mock_db_session, None])

      with patch("robosystems.models.iam.UserLimits") as mock_limits_class:
        mock_limits_class.get_or_create_for_user.return_value = mock_user_limits

        with patch(
          "robosystems.operations.graph.generic_graph_service.KuzuAllocationManager"
        ) as mock_alloc_class:
          mock_manager = AsyncMock()
          mock_manager.allocate_database = AsyncMock(return_value=mock_cluster_info)
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
                tier="kuzu-standard",
                initial_data=None,
                user_id="user123",
                custom_schema=invalid_schema,
              )

            assert "Invalid custom schema" in str(exc_info.value)


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
        schema_extensions=["roboledger"],
        metadata={"name": "Test"},
        tier="kuzu-standard",
        initial_data=None,
        user_id="user123",
      )

      assert result["status"] == "created"
      assert result["graph_id"] == "kg1234567890abcdef"
