"""Test error handling for disabled providers."""

import pytest
from unittest.mock import patch, MagicMock
from fastapi import status
from fastapi.testclient import TestClient


class TestDisabledProviderHandling:
  """Test that disabled providers return proper client errors."""

  def test_create_connection_disabled_sec_provider(
    self, client: TestClient, auth_headers
  ):
    """Test creating a connection for disabled SEC provider returns 403."""
    with patch("robosystems.operations.providers.registry.env") as mock_env:
      # Configure mock env with SEC disabled
      mock_env.CONNECTION_SEC_ENABLED = False
      mock_env.CONNECTION_QUICKBOOKS_ENABLED = True
      mock_env.CONNECTION_PLAID_ENABLED = True

      # Re-initialize the provider registry with SEC disabled
      from robosystems.operations.providers.registry import ProviderRegistry
      from robosystems.routers.graphs.connections import utils
      import robosystems.routers.graphs.connections.management as management

      new_registry = ProviderRegistry()
      utils.provider_registry = new_registry
      management.provider_registry = new_registry

      request_data = {
        "provider": "sec",
        "entity_id": "entity_123",
        "sec_config": {"cik": "0000320193"},
      }

      response = client.post(
        "/v1/graphs/kg1a2b3c4d5/connections", json=request_data, headers=auth_headers
      )

      # Should return 403 Forbidden, not 500
      assert response.status_code == status.HTTP_403_FORBIDDEN
      response_json = response.json()
      # Check the error message (response_json is the full error object with detail, code, timestamp)
      assert "SEC provider is not enabled" in str(response_json)

  def test_create_connection_disabled_plaid_provider(
    self, client: TestClient, auth_headers
  ):
    """Test creating a connection for disabled Plaid provider returns 403."""
    with patch("robosystems.operations.providers.registry.env") as mock_env:
      # Configure mock env with Plaid disabled
      mock_env.CONNECTION_SEC_ENABLED = True
      mock_env.CONNECTION_QUICKBOOKS_ENABLED = True
      mock_env.CONNECTION_PLAID_ENABLED = False

      # Re-initialize the provider registry with Plaid disabled
      from robosystems.operations.providers.registry import ProviderRegistry
      from robosystems.routers.graphs.connections import utils
      import robosystems.routers.graphs.connections.management as management

      new_registry = ProviderRegistry()
      utils.provider_registry = new_registry
      management.provider_registry = new_registry

      request_data = {
        "provider": "plaid",
        "entity_id": "entity_123",
        "plaid_config": {"link_token": "test_token"},
      }

      response = client.post(
        "/v1/graphs/kg1a2b3c4d5/connections", json=request_data, headers=auth_headers
      )

      # Should return 403 Forbidden, not 500
      assert response.status_code == status.HTTP_403_FORBIDDEN
      response_json = response.json()
      # Check the error message (response_json is the full error object with detail, code, timestamp)
      assert "Plaid provider is not enabled" in str(response_json)

  def test_sync_connection_disabled_provider(self, client: TestClient, auth_headers):
    """Test syncing a connection for disabled provider returns 403."""
    # Mock the connection service to return a connection
    with patch(
      "robosystems.operations.connection_service.ConnectionService.get_connection"
    ) as mock_get:
      mock_get.return_value = {
        "connection_id": "conn_123",
        "provider": "SEC",
        "entity_id": "entity_123",
        "status": "active",
        "created_at": "2024-01-01T00:00:00",
        "metadata": {},
      }

      with patch("robosystems.operations.providers.registry.env") as mock_env:
        # Configure mock env with SEC disabled
        mock_env.CONNECTION_SEC_ENABLED = False
        mock_env.CONNECTION_QUICKBOOKS_ENABLED = True
        mock_env.CONNECTION_PLAID_ENABLED = True

        # Re-initialize the provider registry
        from robosystems.operations.providers.registry import ProviderRegistry
        from robosystems.routers.graphs.connections import utils
        import robosystems.routers.graphs.connections.sync as sync_module

        new_registry = ProviderRegistry()
        utils.provider_registry = new_registry
        sync_module.provider_registry = new_registry

        request_data = {"sync_options": {}}

        response = client.post(
          "/v1/graphs/kg1a2b3c4d5/connections/conn_123/sync",
          json=request_data,
          headers=auth_headers,
        )

        # Should return 403 Forbidden, not 500
        assert response.status_code == status.HTTP_403_FORBIDDEN
        response_json = response.json()
        # The error response has a nested structure
        assert "SEC provider is not enabled" in response_json["detail"]["detail"]

  def test_create_link_token_disabled_plaid(self, client: TestClient, auth_headers):
    """Test creating a link token for disabled Plaid provider returns 403."""

    # Mock the graph repository to return a coroutine
    async def mock_get_repo(*args, **kwargs):
      mock_repo = MagicMock()
      mock_repo.execute_single.return_value = {
        "identifier": "entity_123",
        "name": "Test Entity",
      }
      return mock_repo

    with patch(
      "robosystems.routers.graphs.connections.link_token.get_graph_repository",
      new=mock_get_repo,
    ):
      with patch("robosystems.operations.providers.registry.env") as mock_env:
        # Configure mock env with Plaid disabled
        mock_env.CONNECTION_SEC_ENABLED = True
        mock_env.CONNECTION_QUICKBOOKS_ENABLED = True
        mock_env.CONNECTION_PLAID_ENABLED = False

        # Re-initialize the provider registry
        from robosystems.operations.providers.registry import ProviderRegistry
        from robosystems.routers.graphs.connections import utils
        import robosystems.routers.graphs.connections.link_token as link_token_module

        new_registry = ProviderRegistry()
        utils.provider_registry = new_registry
        link_token_module.provider_registry = new_registry

        request_data = {
          "entity_id": "entity_123",
          "user_id": "user_123",
          "provider": "plaid",
        }

        response = client.post(
          "/v1/graphs/kg1a2b3c4d5/connections/link/token",
          json=request_data,
          headers=auth_headers,
        )

        # Should return 403 Forbidden, not 500
        assert response.status_code == status.HTTP_403_FORBIDDEN
        response_json = response.json()
        # The error response has a nested structure
        assert "Plaid provider is not enabled" in response_json["detail"]["detail"]

  def test_delete_connection_disabled_provider(self, client: TestClient, auth_headers):
    """Test deleting a connection for disabled provider returns 403."""
    # Mock the connection service
    with patch(
      "robosystems.operations.connection_service.ConnectionService.get_connection"
    ) as mock_get:
      mock_get.return_value = {
        "connection_id": "conn_123",
        "provider": "QUICKBOOKS",
        "entity_id": "entity_123",
        "status": "active",
        "created_at": "2024-01-01T00:00:00",
        "metadata": {},
      }

      with patch(
        "robosystems.operations.connection_service.ConnectionService.delete_connection"
      ) as mock_delete:
        mock_delete.return_value = True

        with patch("robosystems.operations.providers.registry.env") as mock_env:
          # Configure mock env with QuickBooks disabled
          mock_env.CONNECTION_SEC_ENABLED = True
          mock_env.CONNECTION_QUICKBOOKS_ENABLED = False
          mock_env.CONNECTION_PLAID_ENABLED = True

          # Re-initialize the provider registry
          from robosystems.operations.providers.registry import ProviderRegistry
          from robosystems.routers.graphs.connections import utils
          import robosystems.routers.graphs.connections.management as management

          new_registry = ProviderRegistry()
          utils.provider_registry = new_registry
          management.provider_registry = new_registry

          response = client.delete(
            "/v1/graphs/kg1a2b3c4d5/connections/conn_123", headers=auth_headers
          )

          # Should return 403 Forbidden, not 500
          assert response.status_code == status.HTTP_403_FORBIDDEN
          response_json = response.json()
          # The error response has a nested structure
          assert (
            "QuickBooks provider is not enabled" in response_json["detail"]["detail"]
          )

  def test_invalid_provider_returns_422(self, client: TestClient, auth_headers):
    """Test that invalid provider values return 422 validation error."""
    request_data = {
      "provider": "unknown_provider",
      "entity_id": "entity_123",
    }

    response = client.post(
      "/v1/graphs/kg1a2b3c4d5/connections", json=request_data, headers=auth_headers
    )

    # Invalid providers should return 422 Unprocessable Entity (Pydantic validation)
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    response_json = response.json()
    # 422 errors have detail as a list of validation errors
    assert isinstance(response_json["detail"], list)
    assert len(response_json["detail"]) > 0
    # Check that it's about the provider field
    assert any("provider" in str(error).lower() for error in response_json["detail"])

  @pytest.fixture
  def auth_headers(self, test_user, test_org, test_db):
    """Create auth headers for test requests."""
    from robosystems.models.iam import UserAPIKey, GraphUser, Graph

    # Create the graph first (only if it doesn't exist)
    existing_graph = (
      test_db.query(Graph).filter(Graph.graph_id == "kg1a2b3c4d5").first()
    )
    if not existing_graph:
      Graph.create(
        graph_id="kg1a2b3c4d5",
        org_id=test_org.id,
        graph_name="Test Graph",
        graph_type="generic",
        session=test_db,
      )

    # Create GraphUser relationship for the test graph (only if it doesn't exist)
    existing_user_graph = (
      test_db.query(GraphUser)
      .filter(GraphUser.user_id == test_user.id, GraphUser.graph_id == "kg1a2b3c4d5")
      .first()
    )
    if not existing_user_graph:
      GraphUser.create(
        user_id=test_user.id, graph_id="kg1a2b3c4d5", role="admin", session=test_db
      )

    # Create an API key for the test user
    _, plain_key = UserAPIKey.create(
      user_id=test_user.id, name="Test API Key", session=test_db
    )

    return {"X-API-Key": plain_key}
