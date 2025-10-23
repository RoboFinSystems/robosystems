"""
Tests for unified graph creation router.

This test suite covers:
- Graph creation with different configurations
- Entity graph creation
- Schema extension handling
- Rate limiting and authorization
- Error handling and validation
- SSE operation response
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock
from httpx import AsyncClient
from uuid import uuid4

from robosystems.models.iam import UserLimits, GraphTier
from robosystems.routers.graphs.main import (
  CreateGraphRequest,
  InitialEntityData,
  _create_error_response,
  _raise_http_exception,
)
from robosystems.models.api.graph import (
  GraphMetadata,
  CustomSchemaDefinition,
)


@pytest.mark.asyncio
class TestGraphCreationEndpoint:
  """Test unified graph creation endpoint."""

  @pytest.fixture
  def mock_user_limits(self):
    """Create mock user limits."""
    limits = Mock(spec=UserLimits)
    limits.can_create_user_graph.return_value = (True, None)
    return limits

  @pytest.fixture
  def sample_graph_request(self):
    """Create a sample graph creation request."""
    return CreateGraphRequest(
      metadata=GraphMetadata(
        graph_name="Test Graph",
        description="Test graph description",
        schema_extensions=["roboledger"],
      ),
      instance_tier="kuzu-standard",
      custom_schema=None,
      initial_entity=None,
      tags=["test", "production"],
    )

  @pytest.fixture
  def sample_entity_graph_request(self):
    """Create a sample entity graph creation request."""
    return CreateGraphRequest(
      metadata=GraphMetadata(
        graph_name="Entity Test Graph",
        description="Entity graph description",
        schema_extensions=["roboledger"],
      ),
      instance_tier="kuzu-xlarge",
      custom_schema=None,
      initial_entity=InitialEntityData(
        name="Test Corp",
        uri="https://testcorp.com",
        cik="0001234567",
        sic="3711",
        sic_description="Motor Vehicles & Passenger Car Bodies",
        category="Manufacturing",
        state_of_incorporation="DE",
        fiscal_year_end="1231",
        ein="12-3456789",
      ),
      tags=["entity", "test"],
    )

  async def test_create_graph_success(
    self, async_client: AsyncClient, sample_graph_request, mock_user_limits
  ):
    """Test successful graph creation without entity."""
    with patch("robosystems.database.get_db_session") as mock_get_db:
      with patch.object(UserLimits, "get_by_user_id", return_value=mock_user_limits):
        with patch(
          "robosystems.routers.graphs.main.create_operation_response",
          new_callable=AsyncMock,
        ) as mock_create_op:
          with patch(
            "robosystems.tasks.graph_operations.create_graph.create_graph_sse_task"
          ) as mock_task:
            # Setup mocks
            mock_db = Mock()
            mock_get_db.return_value = iter([mock_db])

            operation_id = str(uuid4())
            mock_create_op.return_value = {
              "operation_id": operation_id,
              "status": "pending",
              "operation_type": "graph_creation",
              "_links": {
                "stream": f"/v1/operations/{operation_id}/stream",
                "status": f"/v1/operations/{operation_id}/status",
              },
            }

            mock_task.delay.return_value = Mock(id="task-123")

            # Make request
            response = await async_client.post(
              "/v1/graphs",
              json=sample_graph_request.model_dump(),
            )

            # Assert response
            assert response.status_code == 202
            data = response.json()
            assert data["operation_id"] == operation_id
            assert data["status"] == "pending"
            assert "_links" in data

            # Assert task was queued
            mock_task.delay.assert_called_once()
            task_data = mock_task.delay.call_args[0][0]
            assert task_data["graph_id"] is None
            assert task_data["schema_extensions"] == ["roboledger"]
            assert task_data["tier"] == "kuzu-standard"

  async def test_create_entity_graph_success(
    self, async_client: AsyncClient, sample_entity_graph_request, mock_user_limits
  ):
    """Test successful entity graph creation."""
    with patch("robosystems.database.get_db_session") as mock_get_db:
      with patch.object(UserLimits, "get_by_user_id", return_value=mock_user_limits):
        with patch(
          "robosystems.routers.graphs.main.create_operation_response",
          new_callable=AsyncMock,
        ) as mock_create_op:
          with patch(
            "robosystems.tasks.graph_operations.create_entity_graph.create_entity_with_new_graph_sse_task"
          ) as mock_task:
            # Setup mocks
            mock_db = Mock()
            mock_get_db.return_value = iter([mock_db])

            operation_id = str(uuid4())
            mock_create_op.return_value = {
              "operation_id": operation_id,
              "status": "pending",
              "operation_type": "entity_graph_creation",
              "_links": {
                "stream": f"/v1/operations/{operation_id}/stream",
                "status": f"/v1/operations/{operation_id}/status",
              },
            }

            mock_task.delay.return_value = Mock(id="task-456")

            # Make request
            response = await async_client.post(
              "/v1/graphs",
              json=sample_entity_graph_request.model_dump(),
            )

            # Assert response
            assert response.status_code == 202
            data = response.json()
            assert data["operation_id"] == operation_id
            assert data["operation_type"] == "entity_graph_creation"

            # Assert entity task was queued
            mock_task.delay.assert_called_once()
            entity_data = mock_task.delay.call_args[0][0]
            assert entity_data["name"] == "Test Corp"
            assert entity_data["cik"] == "0001234567"
            assert entity_data["graph_tier"] == GraphTier.KUZU_XLARGE.value

  async def test_create_graph_with_custom_schema(
    self, async_client: AsyncClient, mock_user_limits
  ):
    """Test graph creation with custom schema definition."""
    request = CreateGraphRequest(
      metadata=GraphMetadata(
        graph_name="Custom Schema Graph",
        description="Graph with custom schema",
        schema_extensions=["roboledger"],
      ),
      instance_tier="kuzu-xlarge",
      custom_schema=CustomSchemaDefinition(
        name="custom_schema",
        version="1.0.0",
        description="Test custom schema",
        extends=None,
        nodes=[
          {
            "name": "CustomNode",
            "properties": [
              {"name": "id", "type": "STRING", "is_primary_key": True},
              {"name": "value", "type": "DOUBLE"},
            ],
          }
        ],
        relationships=[
          {
            "name": "CUSTOM_REL",
            "from_node": "CustomNode",
            "to_node": "Entity",
            "properties": [],
          }
        ],
        metadata={},
      ),
      initial_entity=None,
      tags=[],
    )

    with patch("robosystems.database.get_db_session") as mock_get_db:
      with patch.object(UserLimits, "get_by_user_id", return_value=mock_user_limits):
        with patch(
          "robosystems.routers.graphs.main.create_operation_response",
          new_callable=AsyncMock,
        ) as mock_create_op:
          with patch(
            "robosystems.tasks.graph_operations.create_graph.create_graph_sse_task"
          ) as mock_task:
            # Setup mocks
            mock_db = Mock()
            mock_get_db.return_value = iter([mock_db])

            operation_id = str(uuid4())
            mock_create_op.return_value = {
              "operation_id": operation_id,
              "status": "pending",
              "operation_type": "graph_creation",
            }

            mock_task.delay.return_value = Mock(id="task-789")

            # Make request
            response = await async_client.post(
              "/v1/graphs",
              json=request.model_dump(),
            )

            # Assert response
            assert response.status_code == 202

            # Assert custom schema was included
            task_data = mock_task.delay.call_args[0][0]
            assert task_data["custom_schema"] is not None
            assert task_data["custom_schema"]["nodes"][0]["name"] == "CustomNode"
            assert task_data["graph_tier"] == GraphTier.KUZU_XLARGE.value

  async def test_create_graph_user_limits_not_found(
    self, async_client: AsyncClient, sample_graph_request
  ):
    """Test graph creation when user limits not found."""
    with patch("robosystems.database.get_db_session") as mock_get_db:
      with patch.object(UserLimits, "get_by_user_id", return_value=None):
        mock_db = Mock()
        mock_get_db.return_value = iter([mock_db])

        response = await async_client.post(
          "/v1/graphs",
          json=sample_graph_request.model_dump(),
        )

        assert response.status_code == 403
        data = response.json()
        assert data["detail"]["error"]["code"] == "user_limits_not_found"
        assert "User limits not found" in data["detail"]["error"]["message"]

  async def test_create_graph_limit_reached(
    self, async_client: AsyncClient, sample_graph_request
  ):
    """Test graph creation when user has reached their limit."""
    with patch("robosystems.database.get_db_session") as mock_get_db:
      mock_limits = Mock(spec=UserLimits)
      mock_limits.can_create_user_graph.return_value = (
        False,
        "You have reached your maximum graph limit (3/3)",
      )

      with patch.object(UserLimits, "get_by_user_id", return_value=mock_limits):
        mock_db = Mock()
        mock_get_db.return_value = iter([mock_db])

        response = await async_client.post(
          "/v1/graphs",
          json=sample_graph_request.model_dump(),
        )

        assert response.status_code == 403
        data = response.json()
        assert data["detail"]["error"]["code"] == "graph_limit_reached"
        assert "maximum graph limit" in data["detail"]["error"]["message"]

  async def test_create_graph_invalid_tier(self, async_client: AsyncClient):
    """Test graph creation with invalid instance tier."""
    request_data = {
      "metadata": {
        "graph_name": "Test Graph",
        "description": "Test",
        "schema_extensions": ["roboledger"],
      },
      "instance_tier": "invalid_tier",  # Invalid tier
    }

    response = await async_client.post(
      "/v1/graphs",
      json=request_data,
    )

    assert response.status_code == 422
    data = response.json()
    # Check for pattern matching error message
    assert any("pattern" in str(error).lower() for error in data["detail"])

  async def test_create_graph_missing_metadata(self, async_client: AsyncClient):
    """Test graph creation without required metadata."""
    request_data = {
      "instance_tier": "kuzu-standard",
      # Missing metadata
    }

    response = await async_client.post(
      "/v1/graphs",
      json=request_data,
    )

    assert response.status_code == 422
    data = response.json()
    assert any("metadata" in str(error).lower() for error in data["detail"])

  async def test_create_graph_too_many_tags(self, async_client: AsyncClient):
    """Test graph creation with too many tags."""
    request_data = {
      "metadata": {
        "graph_name": "Test Graph",
        "description": "Test",
        "schema_extensions": ["roboledger"],
      },
      "instance_tier": "kuzu-standard",
      "tags": [f"tag_{i}" for i in range(15)],  # More than 10 tags
    }

    response = await async_client.post(
      "/v1/graphs",
      json=request_data,
    )

    assert response.status_code == 422
    data = response.json()
    assert any("tags" in str(error).lower() for error in data["detail"])

  async def test_create_graph_task_failure(
    self, async_client: AsyncClient, sample_graph_request, mock_user_limits
  ):
    """Test handling of task creation failure."""
    with patch("robosystems.database.get_db_session") as mock_get_db:
      with patch.object(UserLimits, "get_by_user_id", return_value=mock_user_limits):
        with patch(
          "robosystems.routers.graphs.main.create_operation_response",
          new_callable=AsyncMock,
        ) as mock_create_op:
          with patch(
            "robosystems.tasks.graph_operations.create_graph.create_graph_sse_task"
          ) as mock_task:
            # Setup mocks
            mock_db = Mock()
            mock_get_db.return_value = iter([mock_db])
            mock_create_op.return_value = {"operation_id": str(uuid4())}

            # Simulate task failure
            mock_task.delay.side_effect = Exception("Celery connection failed")

            # Make request
            response = await async_client.post(
              "/v1/graphs",
              json=sample_graph_request.model_dump(),
            )

            # Should handle the exception
            assert response.status_code == 500
            data = response.json()
            assert "Failed to create graph creation operation" in data["detail"]


@pytest.mark.asyncio
class TestGetAvailableExtensions:
  """Test getting available schema extensions."""

  async def test_get_available_extensions_success(self, async_client: AsyncClient):
    """Test successful retrieval of available extensions."""
    with patch("robosystems.schemas.manager.SchemaManager") as MockManager:
      manager_instance = MockManager.return_value
      manager_instance.list_available_extensions.return_value = [
        {
          "name": "roboledger",
          "description": "Accounting system",
          "available": True,
        },
        {
          "name": "roboinvestor",
          "description": "Investment management",
          "available": True,
        },
        {
          "name": "unavailable_ext",
          "description": "Not available",
          "available": False,
        },
      ]

      with patch(
        "robosystems.schemas.loader.get_contextual_schema_loader"
      ) as mock_context_loader:
        with patch("robosystems.schemas.loader.get_schema_loader") as mock_loader:
          # Setup schema loaders
          mock_roboledger_loader = Mock()
          mock_roboledger_loader.list_node_types.return_value = [
            f"node_{i}" for i in range(22)
          ]  # 14 extension nodes + 8 base
          mock_roboledger_loader.list_relationship_types.return_value = [
            f"rel_{i}" for i in range(25)
          ]  # 13 extension rels + 12 base

          mock_roboinvestor_loader = Mock()
          mock_roboinvestor_loader.list_node_types.return_value = [
            f"node_{i}" for i in range(13)
          ]  # 5 extension nodes + 8 base
          mock_roboinvestor_loader.list_relationship_types.return_value = [
            f"rel_{i}" for i in range(17)
          ]  # 5 extension rels + 12 base

          mock_context_loader.return_value = mock_roboledger_loader
          mock_loader.return_value = mock_roboinvestor_loader

          response = await async_client.get("/v1/graphs/extensions")

          assert response.status_code == 200
          data = response.json()
          assert "extensions" in data
          assert len(data["extensions"]) == 2  # Only available ones

          # Check first extension
          ext1 = data["extensions"][0]
          assert ext1["name"] == "roboledger"
          assert "accounting" in ext1["description"].lower()

          # Check second extension
          ext2 = data["extensions"][1]
          assert ext2["name"] == "roboinvestor"
          assert "investment" in ext2["description"].lower()

  async def test_get_available_extensions_schema_manager_failure(
    self, async_client: AsyncClient
  ):
    """Test fallback response when schema manager fails."""
    with patch("robosystems.schemas.manager.SchemaManager") as MockManager:
      MockManager.return_value.list_available_extensions.side_effect = Exception(
        "Schema loading failed"
      )

      response = await async_client.get("/v1/graphs/extensions")

      assert response.status_code == 200
      data = response.json()
      assert "extensions" in data
      assert len(data["extensions"]) == 2  # Fallback extensions

      # Should have default extensions
      extension_names = [ext["name"] for ext in data["extensions"]]
      assert "roboledger" in extension_names
      assert "roboinvestor" in extension_names

  async def test_get_available_extensions_loader_failure(
    self, async_client: AsyncClient
  ):
    """Test handling of schema loader failures."""
    with patch("robosystems.schemas.manager.SchemaManager") as MockManager:
      manager_instance = MockManager.return_value
      manager_instance.list_available_extensions.return_value = [
        {
          "name": "roboledger",
          "description": "Accounting system",
          "available": True,
        },
      ]

      with patch(
        "robosystems.schemas.loader.get_contextual_schema_loader"
      ) as mock_loader:
        # Simulate loader failure
        mock_loader.side_effect = Exception("Loader failed")

        response = await async_client.get("/v1/graphs/extensions")

        assert response.status_code == 200
        data = response.json()
        assert "extensions" in data
        # Should still return extension but with 0 counts
        ext = data["extensions"][0]
        assert ext["name"] == "roboledger"

  async def test_get_available_extensions_no_auth_required(
    self, async_client: AsyncClient
  ):
    """Test that extensions endpoint doesn't require authentication."""
    with patch("robosystems.schemas.manager.SchemaManager") as MockManager:
      manager_instance = MockManager.return_value
      manager_instance.list_available_extensions.return_value = []

      # No auth headers provided
      response = await async_client.get("/v1/graphs/extensions")

      assert response.status_code == 200


class TestHelperFunctions:
  """Test helper functions."""

  def test_create_error_response_basic(self):
    """Test basic error response creation."""
    response = _create_error_response("test_error", "Test error message")

    assert response["error"]["code"] == "test_error"
    assert response["error"]["message"] == "Test error message"
    assert "field" not in response["error"]
    assert "details" not in response["error"]

  def test_create_error_response_with_field(self):
    """Test error response with field."""
    response = _create_error_response(
      "validation_error", "Invalid value", field="graph_name"
    )

    assert response["error"]["code"] == "validation_error"
    assert response["error"]["message"] == "Invalid value"
    assert response["error"]["field"] == "graph_name"

  def test_create_error_response_with_details(self):
    """Test error response with details."""
    details = {"allowed_values": ["standard", "enterprise", "premium"]}
    response = _create_error_response("invalid_tier", "Invalid tier", details=details)

    assert response["error"]["code"] == "invalid_tier"
    assert response["error"]["message"] == "Invalid tier"
    assert response["error"]["details"] == details

  def test_raise_http_exception(self):
    """Test raising HTTP exception with standard format."""
    with pytest.raises(Exception) as exc_info:
      _raise_http_exception(
        status_code=400,
        error_code="bad_request",
        message="Bad request",
        field="test_field",
      )

    # Check exception details
    exception = exc_info.value
    assert exception.status_code == 400
    assert exception.detail["error"]["code"] == "bad_request"
    assert exception.detail["error"]["message"] == "Bad request"
    assert exception.detail["error"]["field"] == "test_field"


class TestDataModels:
  """Test request/response data models."""

  def test_initial_entity_data_validation(self):
    """Test InitialEntityData model validation."""
    # Valid data
    entity = InitialEntityData(
      name="Test Corp",
      uri="https://test.com",
      cik="0001234567",
      sic=None,
      sic_description=None,
      category=None,
      state_of_incorporation=None,
      fiscal_year_end=None,
      ein=None,
    )
    assert entity.name == "Test Corp"
    assert entity.uri == "https://test.com"
    assert entity.cik == "0001234567"

    # Test optional fields
    entity_full = InitialEntityData(
      name="Test Corp",
      uri="https://test.com",
      cik="0001234567",
      sic="3711",
      sic_description="Manufacturing",
      category="Tech",
      state_of_incorporation="DE",
      fiscal_year_end="1231",
      ein="12-3456789",
    )
    assert entity_full.sic == "3711"
    assert entity_full.ein == "12-3456789"

  def test_initial_entity_data_validation_errors(self):
    """Test InitialEntityData validation errors."""
    from pydantic import ValidationError

    # Empty name
    with pytest.raises(ValidationError) as exc_info:
      InitialEntityData(
        name="",
        uri="https://test.com",
        cik=None,
        sic=None,
        sic_description=None,
        category=None,
        state_of_incorporation=None,
        fiscal_year_end=None,
        ein=None,
      )
    assert "at least 1 character" in str(exc_info.value).lower()

    # Name too long
    with pytest.raises(ValidationError) as exc_info:
      InitialEntityData(
        name="x" * 256,
        uri="https://test.com",
        cik=None,
        sic=None,
        sic_description=None,
        category=None,
        state_of_incorporation=None,
        fiscal_year_end=None,
        ein=None,
      )
    assert "at most 255 character" in str(exc_info.value).lower()

    # Empty URI
    with pytest.raises(ValidationError) as exc_info:
      InitialEntityData(
        name="Test",
        uri="",
        cik=None,
        sic=None,
        sic_description=None,
        category=None,
        state_of_incorporation=None,
        fiscal_year_end=None,
        ein=None,
      )
    assert "at least 1 character" in str(exc_info.value).lower()

  def test_create_graph_request_validation(self):
    """Test CreateGraphRequest model validation."""
    # Valid request with minimal data
    request = CreateGraphRequest(
      metadata=GraphMetadata(
        graph_name="Test",
        description="Test graph",
        schema_extensions=["roboledger"],
      ),
      instance_tier="kuzu-standard",
      custom_schema=None,
      initial_entity=None,
      tags=[],
    )
    assert request.instance_tier == "kuzu-standard"  # Default
    assert request.tags == []  # Default
    assert request.initial_entity is None
    assert request.custom_schema is None

    # Valid request with all fields
    request_full = CreateGraphRequest(
      metadata=GraphMetadata(
        graph_name="Full Test",
        description="Full test graph",
        schema_extensions=["roboledger", "roboinvestor"],
      ),
      instance_tier="kuzu-xlarge",
      custom_schema=CustomSchemaDefinition(
        name="test_schema",
        version="1.0.0",
        description=None,
        extends=None,
        nodes=[],
        relationships=[],
      ),
      initial_entity=InitialEntityData(
        name="Corp",
        uri="https://corp.com",
        cik=None,
        sic=None,
        sic_description=None,
        category=None,
        state_of_incorporation=None,
        fiscal_year_end=None,
        ein=None,
      ),
      tags=["tag1", "tag2"],
    )
    assert request_full.instance_tier == "kuzu-xlarge"
    assert len(request_full.tags) == 2

  def test_create_graph_request_tier_validation(self):
    """Test CreateGraphRequest tier validation."""
    from pydantic import ValidationError

    # Invalid tier
    with pytest.raises(ValidationError) as exc_info:
      CreateGraphRequest(
        metadata=GraphMetadata(
          graph_name="Test",
          description="Test",
          schema_extensions=["roboledger"],
        ),
        instance_tier="ultra",  # Invalid
        custom_schema=None,
        initial_entity=None,
        tags=[],
      )
    assert "pattern" in str(exc_info.value).lower()

  def test_create_graph_request_tags_limit(self):
    """Test CreateGraphRequest tags limit."""
    from pydantic import ValidationError

    # Too many tags
    with pytest.raises(ValidationError) as exc_info:
      CreateGraphRequest(
        metadata=GraphMetadata(
          graph_name="Test",
          description="Test",
          schema_extensions=["roboledger"],
        ),
        instance_tier="kuzu-standard",
        custom_schema=None,
        initial_entity=None,
        tags=[f"tag_{i}" for i in range(11)],  # 11 tags, max is 10
      )
    assert "at most 10 item" in str(exc_info.value).lower()


@pytest.mark.asyncio
class TestTierMapping:
  """Test instance tier to GraphTier enum mapping."""

  @pytest.fixture
  def mock_user_limits(self):
    """Create mock user limits."""
    limits = Mock(spec=UserLimits)
    limits.can_create_user_graph.return_value = (True, None)
    return limits

  async def test_tier_mapping_standard(
    self, async_client: AsyncClient, mock_user_limits
  ):
    """Test standard tier mapping."""
    request = CreateGraphRequest(
      metadata=GraphMetadata(
        graph_name="Standard Graph",
        description="Test",
        schema_extensions=["roboledger"],
      ),
      instance_tier="kuzu-standard",
      custom_schema=None,
      initial_entity=None,
      tags=[],
    )

    with patch("robosystems.database.get_db_session") as mock_get_db:
      with patch.object(UserLimits, "get_by_user_id", return_value=mock_user_limits):
        with patch(
          "robosystems.routers.graphs.main.create_operation_response",
          new_callable=AsyncMock,
        ) as mock_create_op:
          with patch(
            "robosystems.tasks.graph_operations.create_graph.create_graph_sse_task"
          ) as mock_task:
            mock_db = Mock()
            mock_get_db.return_value = iter([mock_db])
            mock_task.delay.return_value = Mock(id="task-123")

            mock_create_op.return_value = {
              "operation_id": "op-123",
              "status": "pending",
              "operation_type": "graph_creation",
              "_links": {
                "stream": "/v1/operations/op-123/stream",
                "status": "/v1/operations/op-123/status",
              },
            }

            await async_client.post(
              "/v1/graphs",
              json=request.model_dump(),
            )

            task_data = mock_task.delay.call_args[0][0]
            assert task_data["graph_tier"] == GraphTier.KUZU_STANDARD.value

  async def test_tier_mapping_enterprise(
    self, async_client: AsyncClient, mock_user_limits
  ):
    """Test enterprise tier mapping."""
    request = CreateGraphRequest(
      metadata=GraphMetadata(
        graph_name="Enterprise Graph",
        description="Test",
        schema_extensions=["roboledger"],
      ),
      instance_tier="kuzu-xlarge",
      custom_schema=None,
      initial_entity=None,
      tags=[],
    )

    with patch("robosystems.database.get_db_session") as mock_get_db:
      with patch.object(UserLimits, "get_by_user_id", return_value=mock_user_limits):
        with patch(
          "robosystems.routers.graphs.main.create_operation_response",
          new_callable=AsyncMock,
        ) as mock_create_op:
          with patch(
            "robosystems.tasks.graph_operations.create_graph.create_graph_sse_task"
          ) as mock_task:
            mock_db = Mock()
            mock_get_db.return_value = iter([mock_db])
            mock_task.delay.return_value = Mock(id="task-123")

            mock_create_op.return_value = {
              "operation_id": "op-124",
              "status": "pending",
              "operation_type": "graph_creation",
              "_links": {
                "stream": "/v1/operations/op-124/stream",
                "status": "/v1/operations/op-124/status",
              },
            }

            await async_client.post(
              "/v1/graphs",
              json=request.model_dump(),
            )

            task_data = mock_task.delay.call_args[0][0]
            assert task_data["graph_tier"] == GraphTier.KUZU_XLARGE.value

  async def test_tier_mapping_premium(
    self, async_client: AsyncClient, mock_user_limits
  ):
    """Test premium tier mapping."""
    request = CreateGraphRequest(
      metadata=GraphMetadata(
        graph_name="Premium Graph",
        description="Test",
        schema_extensions=["roboledger"],
      ),
      instance_tier="kuzu-xlarge",
      custom_schema=None,
      initial_entity=None,
      tags=[],
    )

    with patch("robosystems.database.get_db_session") as mock_get_db:
      with patch.object(UserLimits, "get_by_user_id", return_value=mock_user_limits):
        with patch(
          "robosystems.routers.graphs.main.create_operation_response",
          new_callable=AsyncMock,
        ) as mock_create_op:
          with patch(
            "robosystems.tasks.graph_operations.create_graph.create_graph_sse_task"
          ) as mock_task:
            mock_db = Mock()
            mock_get_db.return_value = iter([mock_db])
            mock_task.delay.return_value = Mock(id="task-123")

            mock_create_op.return_value = {
              "operation_id": "op-125",
              "status": "pending",
              "operation_type": "graph_creation",
              "_links": {
                "stream": "/v1/operations/op-125/stream",
                "status": "/v1/operations/op-125/status",
              },
            }

            await async_client.post(
              "/v1/graphs",
              json=request.model_dump(),
            )

            task_data = mock_task.delay.call_args[0][0]
            assert task_data["graph_tier"] == GraphTier.KUZU_XLARGE.value
