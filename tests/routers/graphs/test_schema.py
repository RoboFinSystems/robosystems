"""
Tests for schema management API endpoints.
"""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from fastapi import status


@contextmanager
def mock_database_session(test_user_graph, schema_record=None):
  """Context manager for database session mocking."""
  from main import app
  from robosystems.database import get_db_session

  # Create a proper mock database session
  mock_db = MagicMock()

  # Mock GraphUser query
  mock_user_graph_query = MagicMock()
  mock_user_graph_query.filter_by.return_value.first.return_value = test_user_graph

  # Mock Graph query - return the actual graph from test_graph_with_credits
  mock_graph_query = MagicMock()
  from robosystems.models.iam import Graph

  test_graph = test_user_graph.graph if hasattr(test_user_graph, "graph") else None
  mock_graph_query.filter.return_value.first.return_value = test_graph

  # Mock GraphSchema query - return provided schema_record or None
  mock_schema_query = MagicMock()
  mock_schema_query.filter.return_value.order_by.return_value.first.return_value = (
    schema_record
  )

  # Configure db.query to return the right mock based on the model
  def mock_query(model):
    from robosystems.models.iam import GraphSchema, GraphUser

    if model == GraphUser or (
      hasattr(model, "__name__") and model.__name__ == "GraphUser"
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

      # Schema operations are included - should work without credit pool
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

    # Create mock GraphSchema record
    mock_schema_record = MagicMock()
    mock_schema_record.custom_schema_name = "Test Schema"
    mock_schema_record.schema_version = 1
    mock_schema_record.schema_type = "extensions"
    mock_schema_record.schema_json = {
      "name": "Test Schema",
      "version": "1",
      "type": "extensions",
      "base": "base",
      "extensions": ["roboledger"],
    }
    mock_schema_record.schema_ddl = (
      "CREATE NODE TABLE Entity(identifier STRING, PRIMARY KEY(identifier));"
    )
    mock_schema_record.created_at = MagicMock()
    mock_schema_record.created_at.isoformat.return_value = "2024-01-01T00:00:00Z"

    with mock_database_session(test_user_graph, schema_record=mock_schema_record):
      response = client_with_mocked_auth.get(
        f"/v1/graphs/{test_user_graph.graph_id}/schema/export?format=json"
      )

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert data["graph_id"] == test_user_graph.graph_id
      assert data["format"] == "json"
      assert "schema_definition" in data
      assert isinstance(data["schema_definition"], dict)
      assert data["schema_definition"]["name"] == "Test Schema"
      assert data["schema_definition"]["extensions"] == ["roboledger"]

  @pytest.mark.asyncio
  async def test_export_schema_yaml(
    self, client_with_mocked_auth, test_user, test_graph_with_credits
  ):
    """Test exporting schema in YAML format."""
    test_user_graph = test_graph_with_credits["user_graph"]

    # Create mock GraphSchema record
    mock_schema_record = MagicMock()
    mock_schema_record.custom_schema_name = "Test Schema"
    mock_schema_record.schema_version = 1
    mock_schema_record.schema_type = "extensions"
    mock_schema_record.schema_json = {
      "name": "Test Schema",
      "version": "1",
      "type": "extensions",
    }
    mock_schema_record.schema_ddl = (
      "CREATE NODE TABLE Entity(identifier STRING, PRIMARY KEY(identifier));"
    )

    with mock_database_session(test_user_graph, schema_record=mock_schema_record):
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

    # Create mock GraphSchema record with DDL
    mock_schema_record = MagicMock()
    mock_schema_record.custom_schema_name = "Test Schema"
    mock_schema_record.schema_version = 1
    mock_schema_record.schema_type = "extensions"
    mock_schema_record.schema_json = {"name": "Test Schema"}
    mock_schema_record.schema_ddl = (
      "CREATE NODE TABLE Entity(identifier STRING, PRIMARY KEY(identifier));"
    )

    with mock_database_session(test_user_graph, schema_record=mock_schema_record):
      response = client_with_mocked_auth.get(
        f"/v1/graphs/{test_user_graph.graph_id}/schema/export?format=cypher"
      )

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert data["format"] == "cypher"
      assert "schema_definition" in data
      assert "CREATE NODE TABLE" in data["schema_definition"]

  @pytest.mark.asyncio
  async def test_export_schema_with_data_stats(
    self, client_with_mocked_auth, test_user, test_graph_with_credits
  ):
    """Test exporting schema with data statistics (gracefully handles unavailable stats)."""
    test_user_graph = test_graph_with_credits["user_graph"]

    # Create mock GraphSchema record
    mock_schema_record = MagicMock()
    mock_schema_record.custom_schema_name = "Test Schema"
    mock_schema_record.schema_version = 1
    mock_schema_record.schema_type = "extensions"
    mock_schema_record.schema_json = {"name": "Test Schema"}
    mock_schema_record.schema_ddl = (
      "CREATE NODE TABLE Entity(identifier STRING, PRIMARY KEY(identifier));"
    )

    with mock_database_session(test_user_graph, schema_record=mock_schema_record):
      response = client_with_mocked_auth.get(
        f"/v1/graphs/{test_user_graph.graph_id}/schema/export?include_data_stats=true"
      )

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert "data_stats" in data
      # Stats may be unavailable in test environment, which is ok
      assert (
        "message" in data["data_stats"] or "node_labels_count" in data["data_stats"]
      )

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
