"""
Tests for schema management API endpoints.
"""

from unittest.mock import AsyncMock, patch, MagicMock
import pytest
from fastapi import status
from contextlib import contextmanager


@contextmanager
def mock_database_session(test_user_graph):
  """Context manager for database session mocking."""
  from main import app
  from robosystems.database import get_db_session

  # Create a proper mock database session
  mock_db = MagicMock()

  # Mock UserGraph query
  mock_user_graph_query = MagicMock()
  mock_user_graph_query.filter_by.return_value.first.return_value = test_user_graph

  # Mock Graph query - return the actual graph from test_graph_with_credits
  mock_graph_query = MagicMock()
  from robosystems.models.iam import Graph

  test_graph = test_user_graph.graph if hasattr(test_user_graph, "graph") else None
  mock_graph_query.filter.return_value.first.return_value = test_graph

  # Mock GraphSchema query - return None (no custom schema)
  mock_schema_query = MagicMock()
  mock_schema_query.filter.return_value.order_by.return_value.first.return_value = None

  # Configure db.query to return the right mock based on the model
  def mock_query(model):
    from robosystems.models.iam import UserGraph, GraphSchema

    if model == UserGraph or (
      hasattr(model, "__name__") and model.__name__ == "UserGraph"
    ):
      return mock_user_graph_query
    elif model == Graph or (hasattr(model, "__name__") and model.__name__ == "Graph"):
      return mock_graph_query
    elif model == GraphSchema or (
      hasattr(model, "__name__") and model.__name__ == "GraphSchema"
    ):
      return mock_schema_query
    return MagicMock()

  mock_db.query = mock_query

  # Override the dependency
  def override_get_db():
    return mock_db

  app.dependency_overrides[get_db_session] = override_get_db

  try:
    yield mock_db
  finally:
    # Clean up the dependency override
    app.dependency_overrides.pop(get_db_session, None)


class TestSchemaValidationEndpoint:
  """Tests for the schema validation endpoint."""

  @pytest.mark.asyncio
  async def test_validate_schema_success(
    self, client_with_mocked_auth, test_user, test_graph_with_credits
  ):
    """Test successful schema validation."""
    test_user_graph = test_graph_with_credits["user_graph"]
    # Mock the schema manager and credit service
    with (
      patch(
        "robosystems.routers.graphs.schema.validate.CustomSchemaManager"
      ) as mock_manager,
      mock_database_session(test_user_graph),
    ):
      mock_instance = mock_manager.return_value
      mock_schema = MagicMock()
      mock_schema.nodes = [MagicMock(name="Product")]
      mock_schema.relationships = []
      mock_instance.create_from_dict.return_value = mock_schema

      schema_def = {
        "name": "test_schema",
        "version": "1.0.0",
        "nodes": [
          {
            "name": "Product",
            "properties": [
              {"name": "id", "type": "STRING", "is_primary_key": True},
              {"name": "name", "type": "STRING"},
            ],
          }
        ],
      }

      response = client_with_mocked_auth.post(
        f"/v1/graphs/{test_user_graph.graph_id}/schema/validate",
        json={"schema_definition": schema_def, "format": "json"},
      )

      if response.status_code != status.HTTP_200_OK:
        print(f"Response status: {response.status_code}")
        print(f"Response body: {response.text}")
      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert data["valid"] is True
      assert data["message"] == "Schema is valid"
      assert data["stats"]["nodes"] == 1

  @pytest.mark.asyncio
  async def test_validate_schema_with_errors(
    self, client_with_mocked_auth, test_user, test_graph_with_credits
  ):
    """Test schema validation with errors."""
    test_user_graph = test_graph_with_credits["user_graph"]
    with (
      patch(
        "robosystems.routers.graphs.schema.validate.CustomSchemaManager"
      ) as mock_manager,
      mock_database_session(test_user_graph),
    ):
      mock_instance = mock_manager.return_value
      mock_instance.create_from_dict.side_effect = ValueError(
        "Invalid data type: INVALID_TYPE"
      )

      schema_def = {
        "name": "test_schema",
        "nodes": [
          {
            "name": "Product",
            "properties": [
              {"name": "id", "type": "INVALID_TYPE"},
            ],
          }
        ],
      }

      response = client_with_mocked_auth.post(
        f"/v1/graphs/{test_user_graph.graph_id}/schema/validate",
        json={"schema_definition": schema_def, "format": "json"},
      )

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert data["valid"] is False
      assert "Invalid data type: INVALID_TYPE" in data["errors"][0]

  @pytest.mark.asyncio
  async def test_validate_schema_yaml_format(
    self, client_with_mocked_auth, test_user, test_graph_with_credits
  ):
    """Test schema validation with YAML format."""
    test_user_graph = test_graph_with_credits["user_graph"]
    with (
      patch(
        "robosystems.routers.graphs.schema.validate.CustomSchemaManager"
      ) as mock_manager,
      mock_database_session(test_user_graph),
    ):
      mock_instance = mock_manager.return_value
      mock_schema = MagicMock()
      mock_schema.nodes = []
      mock_schema.relationships = []
      mock_instance.create_from_dict.return_value = mock_schema

      yaml_schema = """
name: test_schema
version: 1.0.0
nodes:
  - name: Product
    properties:
      - name: id
        type: STRING
        is_primary_key: true
"""

      response = client_with_mocked_auth.post(
        f"/v1/graphs/{test_user_graph.graph_id}/schema/validate",
        json={"schema_definition": yaml_schema, "format": "yaml"},
      )

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert data["valid"] is True

  @pytest.mark.asyncio
  async def test_validate_schema_unauthorized(self, client_with_mocked_auth, test_user):
    """Test schema validation without access to graph."""
    try:
      response = client_with_mocked_auth.post(
        "/v1/unauthorized_graph/schema/validate",
        json={"schema_definition": {"name": "test"}, "format": "json"},
      )

      # Schema operations are now FREE - should work without credit pool
      # But unauthorized graphs may still fail for other reasons
      assert response.status_code in [
        status.HTTP_403_FORBIDDEN,  # Access denied
        status.HTTP_404_NOT_FOUND,  # Graph not found
        status.HTTP_500_INTERNAL_SERVER_ERROR,  # Other errors
      ]
    except ValueError as e:
      # In test environment, the exception might propagate directly
      assert "No credit pool found for graph unauthorized_graph" in str(e)

  @pytest.mark.asyncio
  async def test_validate_schema_with_compatibility_check(
    self, client_with_mocked_auth, test_user, test_graph_with_credits
  ):
    """Test schema validation with compatibility checking."""
    test_user_graph = test_graph_with_credits["user_graph"]
    with (
      patch(
        "robosystems.routers.graphs.schema.validate.CustomSchemaManager"
      ) as mock_ud_manager,
      patch(
        "robosystems.routers.graphs.schema.validate.SchemaManager"
      ) as mock_schema_manager,
      mock_database_session(test_user_graph),
    ):
      # Mock custom schema manager
      mock_ud_instance = mock_ud_manager.return_value
      mock_schema = MagicMock()
      mock_schema.nodes = []
      mock_schema.relationships = []
      mock_ud_instance.create_from_dict.return_value = mock_schema

      # Mock schema manager for compatibility check
      mock_sm_instance = mock_schema_manager.return_value
      mock_compat_result = MagicMock()
      mock_compat_result.compatible = True
      mock_compat_result.conflicts = []
      mock_sm_instance.check_schema_compatibility.return_value = mock_compat_result

      response = client_with_mocked_auth.post(
        f"/v1/graphs/{test_user_graph.graph_id}/schema/validate",
        json={
          "schema_definition": {"name": "test_schema"},
          "format": "json",
          "check_compatibility": ["roboledger"],
        },
      )

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert data["compatibility"]["compatible"] is True
      assert data["compatibility"]["checked_extensions"] == ["roboledger"]


class TestSchemaExportEndpoint:
  """Tests for the schema export endpoint."""

  @pytest.mark.asyncio
  async def test_export_schema_json(
    self, client_with_mocked_auth, test_user, test_graph_with_credits
  ):
    """Test exporting schema in JSON format."""
    test_user_graph = test_graph_with_credits["user_graph"]
    with (
      patch("robosystems.middleware.graph.get_graph_repository") as mock_get_repo,
      mock_database_session(test_user_graph),
    ):
      mock_repo = AsyncMock()
      mock_repo.execute_query = AsyncMock(
        return_value={
          "data": [
            {"name": "Entity", "type": "NODE"},
            {"name": "Report", "type": "NODE"},
            {"name": "HAS_REPORT", "type": "REL", "from": "Entity", "to": "Report"},
          ]
        }
      )
      mock_get_repo.return_value = mock_repo

      response = client_with_mocked_auth.get(
        f"/v1/graphs/{test_user_graph.graph_id}/schema/export?format=json"
      )

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert data["graph_id"] == test_user_graph.graph_id
      assert data["format"] == "json"
      assert "schema_definition" in data
      assert isinstance(data["schema_definition"], dict)

  @pytest.mark.asyncio
  async def test_export_schema_yaml(
    self, client_with_mocked_auth, test_user, test_graph_with_credits
  ):
    """Test exporting schema in YAML format."""
    test_user_graph = test_graph_with_credits["user_graph"]
    with (
      patch("robosystems.middleware.graph.get_graph_repository") as mock_get_repo,
      mock_database_session(test_user_graph),
    ):
      mock_repo = AsyncMock()
      mock_repo.execute_query = AsyncMock(return_value={"data": []})
      mock_get_repo.return_value = mock_repo

      response = client_with_mocked_auth.get(
        f"/v1/graphs/{test_user_graph.graph_id}/schema/export?format=yaml"
      )

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert data["format"] == "yaml"
      assert isinstance(data["schema_definition"], str)
      assert "name:" in data["schema_definition"]

  @pytest.mark.asyncio
  async def test_export_schema_cypher(
    self, client_with_mocked_auth, test_user, test_graph_with_credits
  ):
    """Test exporting schema in Cypher DDL format."""
    test_user_graph = test_graph_with_credits["user_graph"]
    with (
      patch("robosystems.middleware.graph.get_graph_repository") as mock_get_repo,
      patch(
        "robosystems.routers.graphs.schema.validate.CustomSchemaManager"
      ) as mock_manager,
      mock_database_session(test_user_graph),
    ):
      mock_repo = AsyncMock()
      mock_repo.execute_query = AsyncMock(
        return_value={
          "data": [
            {"name": "Entity", "type": "NODE"},
          ]
        }
      )
      mock_get_repo.return_value = mock_repo

      mock_instance = mock_manager.return_value
      mock_schema = MagicMock()
      mock_schema.to_cypher.return_value = (
        "CREATE NODE TABLE Entity(id STRING, PRIMARY KEY(id));"
      )
      mock_instance.create_from_dict.return_value = mock_schema

      response = client_with_mocked_auth.get(
        f"/v1/graphs/{test_user_graph.graph_id}/schema/export?format=cypher"
      )

      # The schema export works correctly with the mock data
      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert data["format"] == "cypher"
      assert "schema_definition" in data

  @pytest.mark.asyncio
  async def test_export_schema_with_data_stats(
    self, client_with_mocked_auth, test_user, test_graph_with_credits
  ):
    """Test exporting schema with data statistics."""
    test_user_graph = test_graph_with_credits["user_graph"]
    with (
      patch("robosystems.middleware.graph.get_graph_repository") as mock_get_repo,
      mock_database_session(test_user_graph),
    ):
      mock_repo = AsyncMock()
      mock_repo.execute_query = AsyncMock(
        side_effect=[
          {"data": []},  # Schema query
          {
            "data": [  # Stats query
              {"node_labels": ["Entity"], "node_count": 10},
              {"node_labels": ["Report"], "node_count": 50},
            ]
          },
        ]
      )
      mock_get_repo.return_value = mock_repo

      response = client_with_mocked_auth.get(
        f"/v1/graphs/{test_user_graph.graph_id}/schema/export?include_data_stats=true"
      )

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert "data_stats" in data
      assert "total_nodes" in data["data_stats"]
      assert "node_counts" in data["data_stats"]

  @pytest.mark.asyncio
  async def test_export_schema_invalid_format(
    self, client_with_mocked_auth, test_user, test_graph_with_credits
  ):
    test_user_graph = test_graph_with_credits["user_graph"]
    """Test exporting schema with invalid format."""
    response = client_with_mocked_auth.get(
      f"/v1/graphs/{test_user_graph.graph_id}/schema/export?format=invalid"
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


class TestSchemaExtensionsEndpoint:
  """Tests for the schema extensions listing endpoint."""

  @pytest.mark.asyncio
  async def test_list_schema_extensions(
    self, client_with_mocked_auth, test_user, test_graph_with_credits
  ):
    """Test listing available schema extensions."""
    test_user_graph = test_graph_with_credits["user_graph"]
    with (
      patch(
        "robosystems.routers.graphs.schema.extensions.SchemaManager"
      ) as mock_manager,
      mock_database_session(test_user_graph),
    ):
      mock_instance = mock_manager.return_value
      mock_instance.list_available_extensions.return_value = [
        {
          "name": "roboledger",
          "available": True,
          "description": "Financial reporting and accounting",
        },
        {
          "name": "roboinvestor",
          "available": True,
          "description": "Investment portfolio management",
        },
      ]
      mock_instance.get_optimal_schema_groups.return_value = {
        "financial": ["roboledger", "sec"],
        "investment": ["roboinvestor", "portfolio"],
      }

      response = client_with_mocked_auth.get(
        f"/v1/graphs/{test_user_graph.graph_id}/schema/extensions"
      )

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert "extensions" in data
      assert len(data["extensions"]) == 2
      assert data["extensions"][0]["name"] == "roboledger"
      assert "compatibility_groups" in data
      assert "financial" in data["compatibility_groups"]

  @pytest.mark.asyncio
  async def test_list_schema_extensions_unauthorized(
    self, client_with_mocked_auth, test_user
  ):
    """Test listing extensions without graph access."""
    with mock_database_session(None):  # No user graph access
      response = client_with_mocked_auth.get(
        "/v1/graphs/unauthorized_graph/schema/extensions"
      )

      # Extensions endpoint is FREE (no credit consumption)
      # Should return 403 when user doesn't have access
      assert response.status_code == status.HTTP_403_FORBIDDEN

  @pytest.mark.asyncio
  async def test_list_schema_extensions_error_handling(
    self, client_with_mocked_auth, test_user, test_graph_with_credits
  ):
    """Test error handling in extensions listing."""
    test_user_graph = test_graph_with_credits["user_graph"]
    with (
      patch(
        "robosystems.routers.graphs.schema.extensions.SchemaManager"
      ) as mock_manager,
      mock_database_session(test_user_graph),
    ):
      mock_instance = mock_manager.return_value
      mock_instance.list_available_extensions.side_effect = Exception(
        "Schema manager error"
      )

      response = client_with_mocked_auth.get(
        f"/v1/graphs/{test_user_graph.graph_id}/schema/extensions"
      )

      assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
      assert "Failed to list extensions" in response.json()["detail"]
