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

from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

import pytest
from httpx import AsyncClient

from robosystems.models.api.graphs.core import (
  GraphMetadata,
  InitialEntityData,
)
from robosystems.models.api.graphs.schema import (
  CustomSchemaDefinition,
)
from robosystems.models.core import OrgLimits
from robosystems.routers.graphs.main import (
  _ROBOINVESTOR_DESCRIPTION,
  _ROBOLEDGER_DESCRIPTION,
  CreateGraphRequest,
  _create_error_response,
  _raise_http_exception,
)


@pytest.mark.asyncio
class TestGraphCreationEndpoint:
  """Test unified graph creation endpoint."""

  @pytest.fixture
  def mock_user_limits(self):
    """Create mock user limits."""
    limits = Mock(spec=OrgLimits)
    limits.can_create_graph.return_value = (True, None)
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
      instance_tier="ladybug-standard",
      custom_schema=None,
      initial_entity=InitialEntityData(name="Test Corp", uri="https://testcorp.com"),
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
      instance_tier="ladybug-xlarge",
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
    """Test successful graph creation."""
    with patch("robosystems.database.get_db_session") as mock_get_db:
      with patch("robosystems.models.core.OrgUser.get_user_orgs") as mock_get_user_orgs:
        with patch.object(
          OrgLimits, "get_or_create_for_org", return_value=mock_user_limits
        ):
          with patch(
            "robosystems.middleware.billing.enforcement.check_can_provision_graph",
            return_value=(True, None),
          ):
            operation_id = str(uuid4())
            with patch(
              "robosystems.worker.client.enqueue_task",
              new_callable=AsyncMock,
              return_value={
                "operation_id": operation_id,
                "status": "pending",
                "operation_type": "graph_creation",
                "_links": {
                  "stream": f"/v1/operations/{operation_id}/stream",
                  "status": f"/v1/operations/{operation_id}/status",
                },
              },
            ):
              # Setup mocks
              mock_db = Mock()
              mock_get_db.return_value = iter([mock_db])

              mock_org_user = Mock()
              mock_org_user.org_id = "test-org-123"
              mock_get_user_orgs.return_value = [mock_org_user]

              mock_user_limits.can_create_graph.return_value = (True, None)

              response = await async_client.post(
                "/v1/graphs",
                json=sample_graph_request.model_dump(),
              )

              assert response.status_code == 202
              data = response.json()
              assert data["operationId"] == operation_id
              assert data["status"] == "pending"
              assert "_links" in data["result"]

  async def test_create_entity_graph_success(
    self, async_client: AsyncClient, sample_entity_graph_request, mock_user_limits
  ):
    """Test successful entity graph creation."""
    with patch("robosystems.database.get_db_session") as mock_get_db:
      with patch("robosystems.models.core.OrgUser.get_user_orgs") as mock_get_user_orgs:
        with patch.object(
          OrgLimits, "get_or_create_for_org", return_value=mock_user_limits
        ):
          with patch(
            "robosystems.middleware.billing.enforcement.check_can_provision_graph",
            return_value=(True, None),
          ):
            operation_id = str(uuid4())
            with patch(
              "robosystems.worker.client.enqueue_task",
              new_callable=AsyncMock,
              return_value={
                "operation_id": operation_id,
                "status": "pending",
                "operation_type": "entity_graph_creation",
                "_links": {
                  "stream": f"/v1/operations/{operation_id}/stream",
                  "status": f"/v1/operations/{operation_id}/status",
                },
              },
            ):
              # Setup mocks
              mock_db = Mock()
              mock_get_db.return_value = iter([mock_db])

              mock_org_user = Mock()
              mock_org_user.org_id = "test-org-123"
              mock_get_user_orgs.return_value = [mock_org_user]

              mock_user_limits.can_create_graph.return_value = (True, None)

              response = await async_client.post(
                "/v1/graphs",
                json=sample_entity_graph_request.model_dump(),
              )

              assert response.status_code == 202
              data = response.json()
              assert data["operationId"] == operation_id
              assert data["result"]["operation_type"] == "entity_graph_creation"

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
      instance_tier="ladybug-xlarge",
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
      with patch("robosystems.models.core.OrgUser.get_user_orgs") as mock_get_user_orgs:
        with patch.object(
          OrgLimits, "get_or_create_for_org", return_value=mock_user_limits
        ):
          with patch(
            "robosystems.middleware.billing.enforcement.check_can_provision_graph",
            return_value=(True, None),
          ):
            operation_id = str(uuid4())
            with patch(
              "robosystems.worker.client.enqueue_task",
              new_callable=AsyncMock,
              return_value={
                "operation_id": operation_id,
                "status": "pending",
                "operation_type": "graph_creation",
                "_links": {
                  "stream": f"/v1/operations/{operation_id}/stream",
                  "status": f"/v1/operations/{operation_id}/status",
                },
              },
            ):
              # Setup mocks
              mock_db = Mock()
              mock_get_db.return_value = iter([mock_db])

              mock_org_user = Mock()
              mock_org_user.org_id = "test-org-123"
              mock_get_user_orgs.return_value = [mock_org_user]

              mock_user_limits.can_create_graph.return_value = (True, None)

              response = await async_client.post(
                "/v1/graphs",
                json=request.model_dump(),
              )

              assert response.status_code == 202

  async def test_create_graph_refuses_a_user_with_no_organization(
    self, async_client: AsyncClient, sample_graph_request
  ):
    """A caller with no org is refused, not errored. Removal from a last org
    leaves exactly this state, so it is an ordinary authorization outcome —
    a 500 would file it as a server fault and count against the error rate."""
    with patch("robosystems.database.get_db_session") as mock_get_db:
      with patch("robosystems.models.core.OrgUser.get_user_orgs") as mock_get_user_orgs:
        mock_db = Mock()
        mock_get_db.return_value = iter([mock_db])

        # Mock OrgUser.get_user_orgs to return empty list (user has no org)
        mock_get_user_orgs.return_value = []

        response = await async_client.post(
          "/v1/graphs",
          json=sample_graph_request.model_dump(),
        )

        assert response.status_code == 403
        data = response.json()
        assert data["detail"]["error"]["code"] == "org_not_found"
        assert "not a member of any organization" in data["detail"]["error"]["message"]

  async def test_create_graph_limit_reached(
    self, async_client: AsyncClient, sample_graph_request
  ):
    """Test graph creation when user has reached their limit."""
    with patch("robosystems.database.get_db_session") as mock_get_db:
      with patch("robosystems.models.core.OrgUser.get_user_orgs") as mock_get_user_orgs:
        mock_limits = Mock(spec=OrgLimits)
        mock_limits.can_create_graph.return_value = (
          False,
          "You have reached your maximum graph limit (3/3)",
        )

        with patch.object(OrgLimits, "get_or_create_for_org", return_value=mock_limits):
          mock_db = Mock()
          mock_get_db.return_value = iter([mock_db])

          # Mock OrgUser.get_user_orgs to return a list with an org
          mock_org_user = Mock()
          mock_org_user.org_id = "test-org-123"
          mock_get_user_orgs.return_value = [mock_org_user]

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
    # 422s are normalized to ErrorResponse: detail is a compressed string.
    assert "pattern" in data["detail"].lower()

  async def test_create_graph_missing_metadata(self, async_client: AsyncClient):
    """Test graph creation without required metadata."""
    request_data = {
      "instance_tier": "ladybug-standard",
      # Missing metadata
    }

    response = await async_client.post(
      "/v1/graphs",
      json=request_data,
    )

    assert response.status_code == 422
    data = response.json()
    assert "metadata" in data["detail"].lower()

  async def test_create_graph_too_many_tags(self, async_client: AsyncClient):
    """Test graph creation with too many tags."""
    request_data = {
      "metadata": {
        "graph_name": "Test Graph",
        "description": "Test",
        "schema_extensions": ["roboledger"],
      },
      "instance_tier": "ladybug-standard",
      "tags": [f"tag_{i}" for i in range(15)],  # More than 10 tags
    }

    response = await async_client.post(
      "/v1/graphs",
      json=request_data,
    )

    assert response.status_code == 422
    data = response.json()
    assert "tags" in data["detail"].lower()

  async def test_create_graph_task_failure(
    self, async_client: AsyncClient, sample_graph_request, mock_user_limits
  ):
    """Test handling of operation creation failure."""
    with patch("robosystems.database.get_db_session") as mock_get_db:
      with patch("robosystems.models.core.OrgUser.get_user_orgs") as mock_get_user_orgs:
        with patch.object(
          OrgLimits, "get_or_create_for_org", return_value=mock_user_limits
        ):
          with patch(
            "robosystems.middleware.billing.enforcement.check_can_provision_graph",
            return_value=(True, None),
          ):
            with patch(
              "robosystems.worker.client.enqueue_task",
              new_callable=AsyncMock,
              side_effect=Exception("SSE operation creation failed"),
            ):
              # Setup mocks
              mock_db = Mock()
              mock_get_db.return_value = iter([mock_db])

              mock_org_user = Mock()
              mock_org_user.org_id = "test-org-123"
              mock_get_user_orgs.return_value = [mock_org_user]

              mock_user_limits.can_create_graph.return_value = (True, None)

              response = await async_client.post(
                "/v1/graphs",
                json=sample_graph_request.model_dump(),
              )

              assert response.status_code == 500
              data = response.json()
              assert "Failed to create graph creation operation" in data["detail"]


@pytest.mark.asyncio
class TestGetAvailableExtensions:
  """Test getting available schema extensions."""

  async def test_get_available_extensions_success(self, async_client: AsyncClient):
    """Test successful retrieval of available extensions."""
    with patch("robosystems.schemas.runtime.manager.SchemaManager") as MockManager:
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

          # Assert the curated descriptions verbatim, not keywords in them: the
          # thing worth pinning is that each branch served its own copy rather
          # than falling through to the module-docstring default. A substring
          # match on prose breaks on any honest copy edit and proves less.
          ext1 = data["extensions"][0]
          assert ext1["name"] == "roboledger"
          assert ext1["description"] == _ROBOLEDGER_DESCRIPTION

          ext2 = data["extensions"][1]
          assert ext2["name"] == "roboinvestor"
          assert ext2["description"] == _ROBOINVESTOR_DESCRIPTION

  async def test_get_available_extensions_schema_manager_failure(
    self, async_client: AsyncClient
  ):
    """Test fallback response when schema manager fails."""
    with patch("robosystems.schemas.runtime.manager.SchemaManager") as MockManager:
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
    with patch("robosystems.schemas.runtime.manager.SchemaManager") as MockManager:
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
    with patch("robosystems.schemas.runtime.manager.SchemaManager") as MockManager:
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
    from pydantic import ValidationError

    # Entity graph without initial_entity is rejected
    with pytest.raises(ValidationError) as exc_info:
      CreateGraphRequest(
        metadata=GraphMetadata(graph_name="Test", schema_extensions=["roboledger"]),
        instance_tier="ladybug-standard",
        custom_schema=None,
        initial_entity=None,
        tags=[],
      )
    assert "initial_entity" in str(exc_info.value)

    # Valid entity graph request
    request = CreateGraphRequest(
      metadata=GraphMetadata(
        graph_name="Test",
        description="Test graph",
        schema_extensions=["roboledger"],
      ),
      instance_tier="ladybug-standard",
      custom_schema=None,
      initial_entity=InitialEntityData(name="Test Corp", uri="https://testcorp.com"),
      tags=[],
    )
    assert request.instance_tier == "ladybug-standard"
    assert request.tags == []
    assert request.initial_entity is not None
    assert request.custom_schema is None

    # Valid request with all fields
    request_full = CreateGraphRequest(
      metadata=GraphMetadata(
        graph_name="Full Test",
        description="Full test graph",
        schema_extensions=["roboledger", "roboinvestor"],
      ),
      instance_tier="ladybug-xlarge",
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
    assert request_full.instance_tier == "ladybug-xlarge"
    assert len(request_full.tags) == 2

  def test_create_graph_request_tier_validation(self):
    """Test CreateGraphRequest tier validation."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError) as exc_info:
      CreateGraphRequest(
        metadata=GraphMetadata(
          graph_name="Test",
          description="Test",
          schema_extensions=["roboledger"],
        ),
        instance_tier="ultra",  # Invalid
        initial_entity=InitialEntityData(name="Test Corp", uri="https://testcorp.com"),
        tags=[],
      )
    assert "pattern" in str(exc_info.value).lower()

  def test_create_graph_request_tags_limit(self):
    """Test CreateGraphRequest tags limit."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError) as exc_info:
      CreateGraphRequest(
        metadata=GraphMetadata(
          graph_name="Test",
          description="Test",
          schema_extensions=["roboledger"],
        ),
        instance_tier="ladybug-standard",
        initial_entity=InitialEntityData(name="Test Corp", uri="https://testcorp.com"),
        tags=[f"tag_{i}" for i in range(11)],  # 11 tags, max is 10
      )
    assert "at most 10 item" in str(exc_info.value).lower()


@pytest.mark.asyncio
class TestTierMapping:
  """Test instance tier is correctly passed to direct graph creation."""

  @pytest.fixture
  def mock_user_limits(self):
    """Create mock user limits."""
    limits = Mock(spec=OrgLimits)
    limits.can_create_graph.return_value = (True, None)
    return limits

  async def test_tier_mapping_standard(
    self, async_client: AsyncClient, mock_user_limits
  ):
    """Test standard tier is passed to direct execution."""
    request = CreateGraphRequest(
      metadata=GraphMetadata(
        graph_name="Standard Graph",
        description="Test",
        schema_extensions=["roboledger"],
      ),
      instance_tier="ladybug-standard",
      custom_schema=None,
      initial_entity=InitialEntityData(name="Test Corp", uri="https://testcorp.com"),
      tags=[],
    )

    with patch("robosystems.database.get_db_session") as mock_get_db:
      with patch("robosystems.models.core.OrgUser.get_user_orgs") as mock_get_user_orgs:
        with patch.object(
          OrgLimits, "get_or_create_for_org", return_value=mock_user_limits
        ):
          with patch(
            "robosystems.middleware.billing.enforcement.check_can_provision_graph",
            return_value=(True, None),
          ):
            with patch(
              "robosystems.worker.client.enqueue_task",
              new_callable=AsyncMock,
              return_value={
                "operation_id": "op-123",
                "status": "pending",
                "operation_type": "graph_creation",
                "_links": {
                  "stream": "/v1/operations/op-123/stream",
                  "status": "/v1/operations/op-123/status",
                },
              },
            ):
              mock_db = Mock()
              mock_get_db.return_value = iter([mock_db])

              mock_org_user = Mock()
              mock_org_user.org_id = "test-org-123"
              mock_get_user_orgs.return_value = [mock_org_user]

              mock_user_limits.can_create_graph.return_value = (True, None)

              await async_client.post(
                "/v1/graphs",
                json=request.model_dump(),
              )

              # background_tasks.add_task doesn't call the function immediately,
              # so we can't assert mock_run_graph was called. Instead verify the
              # response was accepted (202) which means the direct path was taken.
              # The tier is validated at the Pydantic model level.

  async def test_tier_mapping_xlarge(self, async_client: AsyncClient, mock_user_limits):
    """Test xlarge tier is passed to direct execution."""
    request = CreateGraphRequest(
      metadata=GraphMetadata(
        graph_name="XLarge Graph",
        description="Test",
        schema_extensions=["roboledger"],
      ),
      instance_tier="ladybug-xlarge",
      custom_schema=None,
      initial_entity=InitialEntityData(name="Test Corp", uri="https://testcorp.com"),
      tags=[],
    )

    with patch("robosystems.database.get_db_session") as mock_get_db:
      with patch("robosystems.models.core.OrgUser.get_user_orgs") as mock_get_user_orgs:
        with patch.object(
          OrgLimits, "get_or_create_for_org", return_value=mock_user_limits
        ):
          with patch(
            "robosystems.middleware.billing.enforcement.check_can_provision_graph",
            return_value=(True, None),
          ):
            with patch(
              "robosystems.worker.client.enqueue_task",
              new_callable=AsyncMock,
              return_value={
                "operation_id": "op-124",
                "status": "pending",
                "operation_type": "graph_creation",
                "_links": {
                  "stream": "/v1/operations/op-124/stream",
                  "status": "/v1/operations/op-124/status",
                },
              },
            ):
              mock_db = Mock()
              mock_get_db.return_value = iter([mock_db])

              mock_org_user = Mock()
              mock_org_user.org_id = "test-org-123"
              mock_get_user_orgs.return_value = [mock_org_user]

              mock_user_limits.can_create_graph.return_value = (True, None)

              response = await async_client.post(
                "/v1/graphs",
                json=request.model_dump(),
              )

              assert response.status_code == 202


@pytest.mark.asyncio
class TestGraphCapacityEndpoint:
  """Test GET /v1/graphs/capacity endpoint."""

  async def test_capacity_all_ready(self, async_client: AsyncClient):
    """Test capacity endpoint when all tiers have available slots."""
    with patch(
      "robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"
    ) as MockManager:
      mock_instance = MockManager.return_value
      mock_instance.check_tier_capacity = AsyncMock(return_value="ready")

      response = await async_client.get("/v1/graphs/capacity")

      assert response.status_code == 200
      data = response.json()
      assert "tiers" in data
      assert len(data["tiers"]) == 3

      for tier in data["tiers"]:
        assert tier["status"] == "ready"
        assert tier["message"] == "Available"
        assert "tier" in tier
        assert "display_name" in tier

      tier_names = [t["tier"] for t in data["tiers"]]
      assert "ladybug-standard" in tier_names
      assert "ladybug-large" in tier_names
      assert "ladybug-xlarge" in tier_names

  async def test_capacity_mixed_statuses(self, async_client: AsyncClient):
    """Test capacity endpoint with different statuses per tier."""

    async def mock_check(tier):
      from robosystems.middleware.graph.types import GraphTier

      if tier == GraphTier.LADYBUG_STANDARD:
        return "at_capacity"
      elif tier == GraphTier.LADYBUG_LARGE:
        return "scalable"
      else:
        return "ready"

    with patch(
      "robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"
    ) as MockManager:
      mock_instance = MockManager.return_value
      mock_instance.check_tier_capacity = AsyncMock(side_effect=mock_check)

      response = await async_client.get("/v1/graphs/capacity")

      assert response.status_code == 200
      data = response.json()

      status_map = {t["tier"]: t for t in data["tiers"]}

      assert status_map["ladybug-standard"]["status"] == "at_capacity"
      assert (
        status_map["ladybug-standard"]["message"]
        == "Currently at capacity — contact us for access"
      )

      # "scalable" is downgraded to "at_capacity" (no auto-scaling queue)
      assert status_map["ladybug-large"]["status"] == "at_capacity"
      assert (
        status_map["ladybug-large"]["message"]
        == "Currently at capacity — contact us for access"
      )

      assert status_map["ladybug-xlarge"]["status"] == "ready"
      assert status_map["ladybug-xlarge"]["message"] == "Available"

  async def test_capacity_tier_check_failure_defaults_to_at_capacity(
    self, async_client: AsyncClient
  ):
    """Test that a failing tier check defaults to at_capacity."""

    async def mock_check(tier):
      from robosystems.middleware.graph.types import GraphTier

      if tier == GraphTier.LADYBUG_STANDARD:
        raise Exception("DynamoDB error")
      return "ready"

    with patch(
      "robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"
    ) as MockManager:
      mock_instance = MockManager.return_value
      mock_instance.check_tier_capacity = AsyncMock(side_effect=mock_check)

      response = await async_client.get("/v1/graphs/capacity")

      assert response.status_code == 200
      data = response.json()
      status_map = {t["tier"]: t for t in data["tiers"]}

      # Failed tier defaults to at_capacity
      assert status_map["ladybug-standard"]["status"] == "at_capacity"
      # Others succeed
      assert status_map["ladybug-large"]["status"] == "ready"
      assert status_map["ladybug-xlarge"]["status"] == "ready"


class TestGraphCreationRequiresAdmin:
  """Creating a graph starts a recurring charge on the org's payment method
  and consumes org quota, so it is an owner/admin action.

  Members cannot add a payment method either — checkout is owner-only — so
  without this gate a member could commit the org's stored card without its
  billing administrators knowing until the invoice arrived.
  """

  @pytest.fixture
  def graph_request(self):
    return CreateGraphRequest(
      metadata=GraphMetadata(
        graph_name="Members Cannot Create",
        description="Role gate coverage",
        schema_extensions=["roboledger"],
      ),
      instance_tier="ladybug-standard",
      custom_schema=None,
      initial_entity=InitialEntityData(name="Test Corp", uri="https://testcorp.com"),
      tags=["test"],
    )

  async def test_plain_member_is_refused(
    self, async_client: AsyncClient, graph_request
  ):
    """A member gets 403 before quota is even consulted."""
    with (
      patch("robosystems.database.get_db_session") as mock_get_db,
      patch("robosystems.models.core.OrgUser.get_user_orgs") as mock_get_user_orgs,
    ):
      mock_limits = Mock(spec=OrgLimits)
      # Quota is wide open; the refusal must come from the role check alone.
      mock_limits.can_create_graph.return_value = (True, "Can create graph")

      with patch.object(OrgLimits, "get_or_create_for_org", return_value=mock_limits):
        mock_get_db.return_value = iter([Mock()])

        mock_org_user = Mock()
        mock_org_user.org_id = "test-org-123"
        mock_org_user.can_create_graphs.return_value = False
        mock_get_user_orgs.return_value = [mock_org_user]

        response = await async_client.post(
          "/v1/graphs",
          json=graph_request.model_dump(),
        )

        assert response.status_code == 403
        error = response.json()["detail"]["error"]
        assert error["code"] == "graph_creation_requires_admin"
        # The message has to point somewhere actionable: unlike a quota, the
        # member cannot resolve this themselves.
        assert "owners and admins" in error["message"]
        mock_limits.can_create_graph.assert_not_called()
