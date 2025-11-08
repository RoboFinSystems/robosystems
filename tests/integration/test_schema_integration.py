"""
Integration tests for schema management functionality.
"""

import pytest
from unittest.mock import patch, AsyncMock, MagicMock


@pytest.mark.integration
class TestSchemaManagementIntegration:
  """Integration tests for the complete schema management flow."""

  @pytest.mark.asyncio
  async def test_create_graph_with_custom_schema_flow(
    self, client_with_mocked_auth, test_user
  ):
    """Test the complete flow of creating a graph with custom schema."""
    # First, validate the schema
    schema_def = {
      "name": "retail_inventory",
      "version": "1.0.0",
      "description": "Retail inventory management schema",
      "extends": "base",
      "nodes": [
        {
          "name": "Product",
          "properties": [
            {"name": "sku", "type": "STRING", "is_primary_key": True},
            {"name": "name", "type": "STRING", "is_required": True},
            {"name": "description", "type": "STRING"},
            {"name": "price", "type": "DOUBLE"},
            {"name": "quantity", "type": "INT64"},
            {"name": "category", "type": "STRING"},
            {"name": "tags", "type": "LIST"},
          ],
        },
        {
          "name": "Warehouse",
          "properties": [
            {"name": "id", "type": "STRING", "is_primary_key": True},
            {"name": "name", "type": "STRING", "is_required": True},
            {"name": "location", "type": "STRING"},
            {"name": "capacity", "type": "INT64"},
          ],
        },
        {
          "name": "Supplier",
          "properties": [
            {"name": "id", "type": "STRING", "is_primary_key": True},
            {"name": "name", "type": "STRING", "is_required": True},
            {"name": "contact_email", "type": "STRING"},
            {"name": "rating", "type": "DOUBLE"},
          ],
        },
      ],
      "relationships": [
        {
          "name": "STORED_IN",
          "from_node": "Product",
          "to_node": "Warehouse",
          "properties": [
            {"name": "quantity", "type": "INT64"},
            {"name": "location", "type": "STRING"},
          ],
        },
        {
          "name": "SUPPLIED_BY",
          "from_node": "Product",
          "to_node": "Supplier",
          "properties": [
            {"name": "cost", "type": "DOUBLE"},
            {"name": "lead_time_days", "type": "INT32"},
          ],
        },
      ],
    }

    # Mock dependencies
    with (
      patch(
        "robosystems.routers.graphs.schema.validate.CustomSchemaManager"
      ) as mock_ud_manager,
      patch("robosystems.routers.graphs.schema.validate.SchemaManager"),
    ):
      # Setup validation mocks
      from main import app
      from robosystems.database import get_db_session

      # Create a proper mock database session
      mock_db = MagicMock()

      # Mock GraphUser query
      mock_user_graph_instance = MagicMock(graph_id="test_graph", user_id=test_user.id)
      mock_user_graph_query = MagicMock()
      mock_user_graph_query.filter_by.return_value.first.return_value = (
        mock_user_graph_instance
      )

      # Configure db.query to return the right mock based on the model
      def mock_query(model):
        from robosystems.models.iam import GraphUser

        if model == GraphUser or (
          hasattr(model, "__name__") and model.__name__ == "GraphUser"
        ):
          return mock_user_graph_query
        return MagicMock()

      mock_db.query = mock_query

      # Override the dependency
      def override_get_db():
        return mock_db

      app.dependency_overrides[get_db_session] = override_get_db

      try:
        # Mock schema parsing
        mock_manager = mock_ud_manager.return_value
        mock_schema = MagicMock()
        mock_schema.nodes = [
          MagicMock(name=n["name"], properties=[]) for n in schema_def["nodes"]
        ]
        mock_schema.relationships = [MagicMock() for _ in schema_def["relationships"]]
        mock_manager.create_from_dict.return_value = mock_schema

        # Validate the schema
        response = client_with_mocked_auth.post(
          "/v1/graphs/test_graph/schema/validate",
          json={
            "schema_definition": schema_def,
            "format": "json",
            "check_compatibility": ["roboledger"],
          },
        )

        assert response.status_code == 200
        validation_result = response.json()
        assert validation_result["valid"] is True
        assert validation_result["stats"]["nodes"] == 3
        assert validation_result["stats"]["relationships"] == 2

      finally:
        # Clean up the dependency override
        app.dependency_overrides.pop(get_db_session, None)

    # Now create a graph with this schema
    with (
      patch(
        "robosystems.tasks.graph_operations.create_graph.create_graph_sse_task"
      ) as mock_task,
      patch("robosystems.models.iam.OrgLimits.get_by_org_id") as mock_get_limits,
      patch(
        "robosystems.middleware.billing.enforcement.check_can_provision_graph",
        return_value=(True, None),
      ),
    ):
      # Mock org limits to allow graph creation
      mock_limits = MagicMock()
      mock_limits.can_create_graph.return_value = (True, "")
      mock_limits.subscription_tier = "standard"
      mock_get_limits.return_value = mock_limits

      mock_task.delay.return_value = MagicMock(id="task-123")

      create_response = client_with_mocked_auth.post(
        "/v1/graphs",
        json={
          "metadata": {
            "graph_name": "Retail Inventory System",
            "description": "Production inventory tracking",
            "schema_extensions": [],
          },
          "instance_tier": "kuzu-standard",  # Updated to new tier naming
          "custom_schema": schema_def,
          "tags": ["retail", "inventory", "production"],
        },
      )

      # Debug output if test fails
      if create_response.status_code != 202:
        print(f"Response status: {create_response.status_code}")
        print(f"Response body: {create_response.json()}")

      assert create_response.status_code == 202
      create_result = create_response.json()
      assert "operation_id" in create_result
      assert create_result["status"] == "pending"
      assert "_links" in create_result
      assert "stream" in create_result["_links"]
      assert "status" in create_result["_links"]

  @pytest.mark.asyncio
  async def test_export_and_reimport_schema(
    self, client_with_mocked_auth, test_user, test_user_graph
  ):
    """Test exporting a schema and re-importing it."""
    # Mock the export functionality
    with (
      patch("robosystems.middleware.graph.get_graph_repository") as mock_get_repo,
    ):
      # Setup database mock
      from main import app
      from robosystems.database import get_db_session

      # Create a proper mock database session
      mock_db = MagicMock()

      # Mock GraphUser query
      mock_user_graph_query = MagicMock()
      mock_user_graph_query.filter_by.return_value.first.return_value = test_user_graph

      # Mock GraphSchema record
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

      # Mock GraphSchema query
      mock_schema_query = MagicMock()
      mock_schema_query.filter.return_value.order_by.return_value.first.return_value = (
        mock_schema_record
      )

      # Mock Graph query
      mock_graph_query = MagicMock()
      mock_graph_query.filter.return_value.first.return_value = (
        test_user_graph.graph if hasattr(test_user_graph, "graph") else None
      )

      # Configure db.query to return the right mock based on the model
      def mock_query(model):
        from robosystems.models.iam import GraphUser, GraphSchema, Graph

        if model == GraphUser or (
          hasattr(model, "__name__") and model.__name__ == "GraphUser"
        ):
          return mock_user_graph_query
        elif model == GraphSchema or (
          hasattr(model, "__name__") and model.__name__ == "GraphSchema"
        ):
          return mock_schema_query
        elif model == Graph or (
          hasattr(model, "__name__") and model.__name__ == "Graph"
        ):
          return mock_graph_query
        return MagicMock()

      mock_db.query = mock_query

      # Override the dependency
      def override_get_db():
        return mock_db

      app.dependency_overrides[get_db_session] = override_get_db

      try:
        mock_repo = AsyncMock()
        mock_repo.execute_query = AsyncMock(
          return_value={
            "data": [
              {"name": "Product", "type": "NODE"},
              {"name": "Warehouse", "type": "NODE"},
              {
                "name": "STORED_IN",
                "type": "REL",
                "from": "Product",
                "to": "Warehouse",
              },
            ]
          }
        )
        mock_get_repo.return_value = mock_repo

        # Export the schema
        export_response = client_with_mocked_auth.get(
          f"/v1/graphs/{test_user_graph.graph_id}/schema/export?format=json"
        )

        assert export_response.status_code == 200
        exported_schema = export_response.json()
        schema_definition = exported_schema["schema_definition"]

        # Now validate the exported schema
        with patch(
          "robosystems.routers.graphs.schema.validate.CustomSchemaManager"
        ) as mock_manager:
          mock_instance = mock_manager.return_value
          mock_schema = MagicMock()
          mock_schema.nodes = [MagicMock(name="Product"), MagicMock(name="Warehouse")]
          mock_schema.relationships = [MagicMock(name="STORED_IN")]
          mock_instance.create_from_dict.return_value = mock_schema

          validate_response = client_with_mocked_auth.post(
            f"/v1/graphs/{test_user_graph.graph_id}/schema/validate",
            json={"schema_definition": schema_definition, "format": "json"},
          )

          assert validate_response.status_code == 200
          validation = validate_response.json()
          assert validation["valid"] is True

      finally:
        # Clean up the dependency override
        app.dependency_overrides.pop(get_db_session, None)

  @pytest.mark.asyncio
  async def test_schema_compatibility_workflow(
    self, client_with_mocked_auth, test_user, test_user_graph
  ):
    """Test checking schema compatibility before creating a graph."""
    # First, list available extensions (using global endpoint)
    with patch("robosystems.schemas.manager.SchemaManager") as mock_schema_manager:
      mock_manager = mock_schema_manager.return_value
      mock_manager.list_available_extensions.return_value = [
        {
          "name": "roboledger",
          "available": True,
          "description": "Financial reporting",
        },
        {
          "name": "roboinvestor",
          "available": True,
          "description": "Investment tracking",
        },
      ]

      extensions_response = client_with_mocked_auth.get("/v1/graphs/extensions")

      assert extensions_response.status_code == 200
      extensions = extensions_response.json()
      assert len(extensions["extensions"]) == 2

    # Now create a schema that should be compatible with roboledger
    financial_schema = {
      "name": "custom_financial",
      "extends": "base",
      "nodes": [
        {
          "name": "Account",
          "properties": [
            {"name": "id", "type": "STRING", "is_primary_key": True},
            {"name": "balance", "type": "DOUBLE"},
          ],
        }
      ],
    }

    # Validate with compatibility check
    with (
      patch(
        "robosystems.routers.graphs.schema.validate.CustomSchemaManager"
      ) as mock_ud_manager,
    ):
      from main import app
      from robosystems.database import get_db_session

      # Create a proper mock database session
      mock_db = MagicMock()

      # Mock GraphUser query
      mock_user_graph_query = MagicMock()
      mock_user_graph_query.filter_by.return_value.first.return_value = test_user_graph

      # Configure db.query to return the right mock based on the model
      def mock_query(model):
        from robosystems.models.iam import GraphUser

        if model == GraphUser or (
          hasattr(model, "__name__") and model.__name__ == "GraphUser"
        ):
          return mock_user_graph_query
        return MagicMock()

      mock_db.query = mock_query

      # Override the dependency
      def override_get_db():
        return mock_db

      app.dependency_overrides[get_db_session] = override_get_db

      try:
        mock_ud_instance = mock_ud_manager.return_value
        mock_schema = MagicMock()
        mock_schema.nodes = [MagicMock(name="Account")]
        mock_schema.relationships = []
        mock_ud_instance.create_from_dict.return_value = mock_schema

        # Mock compatibility check
        mock_compat_result = MagicMock()
        mock_compat_result.compatible = True
        mock_compat_result.conflicts = []
        mock_schema_manager.return_value.check_schema_compatibility.return_value = (
          mock_compat_result
        )

        compat_response = client_with_mocked_auth.post(
          f"/v1/graphs/{test_user_graph.graph_id}/schema/validate",
          json={
            "schema_definition": financial_schema,
            "format": "json",
            "check_compatibility": ["roboledger"],
          },
        )

        assert compat_response.status_code == 200
        compat_result = compat_response.json()
        assert compat_result["valid"] is True
        assert compat_result["compatibility"]["compatible"] is True
      finally:
        # Clean up the dependency override
        app.dependency_overrides.pop(get_db_session, None)

  @pytest.mark.asyncio
  async def test_yaml_schema_workflow(
    self, client_with_mocked_auth, test_user, test_user_graph
  ):
    """Test working with YAML schema definitions."""
    yaml_schema = """
name: logistics_system
version: 2.0.0
description: Logistics and shipping management
extends: base

nodes:
  - name: Shipment
    properties:
      - name: tracking_id
        type: STRING
        is_primary_key: true
      - name: origin
        type: STRING
        is_required: true
      - name: destination
        type: STRING
        is_required: true
      - name: status
        type: STRING
      - name: estimated_delivery
        type: DATE
      
  - name: Package
    properties:
      - name: id
        type: UUID
        is_primary_key: true
      - name: weight_kg
        type: DOUBLE
      - name: dimensions
        type: STRUCT
      
relationships:
  - name: CONTAINS
    from_node: Shipment
    to_node: Package
    properties:
      - name: position
        type: INT32
"""

    with (
      patch(
        "robosystems.routers.graphs.schema.validate.CustomSchemaManager"
      ) as mock_manager,
    ):
      from main import app
      from robosystems.database import get_db_session

      # Create a proper mock database session
      mock_db = MagicMock()

      # Mock GraphUser query
      mock_user_graph_query = MagicMock()
      mock_user_graph_query.filter_by.return_value.first.return_value = test_user_graph

      # Configure db.query to return the right mock based on the model
      def mock_query(model):
        from robosystems.models.iam import GraphUser

        if model == GraphUser or (
          hasattr(model, "__name__") and model.__name__ == "GraphUser"
        ):
          return mock_user_graph_query
        return MagicMock()

      mock_db.query = mock_query

      # Override the dependency
      def override_get_db():
        return mock_db

      app.dependency_overrides[get_db_session] = override_get_db

      try:
        # Mock YAML parsing
        mock_instance = mock_manager.return_value
        mock_schema = MagicMock()
        mock_schema.nodes = [MagicMock(name="Shipment"), MagicMock(name="Package")]
        mock_schema.relationships = [MagicMock(name="CONTAINS")]
        mock_instance.create_from_dict.return_value = mock_schema

        # Validate YAML schema
        response = client_with_mocked_auth.post(
          f"/v1/graphs/{test_user_graph.graph_id}/schema/validate",
          json={"schema_definition": yaml_schema, "format": "yaml"},
        )

        assert response.status_code == 200
        result = response.json()
        assert result["valid"] is True
        assert result["stats"]["nodes"] == 2
        assert result["stats"]["relationships"] == 1
      finally:
        # Clean up the dependency override
        app.dependency_overrides.pop(get_db_session, None)
