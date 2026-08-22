"""Test error handling for disabled providers."""

from unittest.mock import AsyncMock, patch

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from tests.conftest import VALID_TEST_GRAPH_ID


class TestDisabledProviderHandling:
  """Test that disabled providers return proper client errors."""

  def test_create_connection_disabled_external_provider(
    self, client: TestClient, auth_headers
  ):
    """Test creating a connection for disabled external provider returns 403."""
    with patch("robosystems.operations.providers.registry.env") as mock_env:
      # Configure mock env with external disabled
      mock_env.CONNECTION_EXTERNAL_ENABLED = False
      mock_env.CONNECTION_QUICKBOOKS_ENABLED = True

      # Re-initialize the provider registry with external disabled
      import robosystems.routers.graphs.connections.management as management
      from robosystems.operations.providers.registry import ProviderRegistry
      from robosystems.routers.graphs.connections import utils

      new_registry = ProviderRegistry()
      utils.provider_registry = new_registry
      management.provider_registry = new_registry

      request_data = {
        "provider": "external",
        "entity_id": "entity_123",
        "external_config": {"source_name": "salesforce"},
      }

      response = client.post(
        f"/v1/graphs/{VALID_TEST_GRAPH_ID}/connections",
        json=request_data,
        headers=auth_headers,
      )

      # Should return 403 Forbidden, not 500
      assert response.status_code == status.HTTP_403_FORBIDDEN
      response_json = response.json()
      # Check the error message (response_json is the full error object with detail, code, timestamp)
      assert "not available" in str(response_json).lower()

  def test_sync_connection_disabled_provider(self, client: TestClient, auth_headers):
    """Test syncing a connection for disabled provider returns 403."""
    # Mock the connection service to return a connection
    with patch(
      "robosystems.operations.connection_service.ConnectionService.get_connection"
    ) as mock_get:
      mock_get.return_value = {
        "connection_id": "conn_123",
        "provider": "EXTERNAL",
        "entity_id": "entity_123",
        "status": "active",
        "created_at": "2024-01-01T00:00:00",
        "metadata": {},
      }

      with patch("robosystems.operations.providers.registry.env") as mock_env:
        # Configure mock env with external disabled
        mock_env.CONNECTION_EXTERNAL_ENABLED = False
        mock_env.CONNECTION_QUICKBOOKS_ENABLED = True

        from robosystems.operations.providers.registry import ProviderRegistry

        new_registry = ProviderRegistry()

      # `dispatch_connection_sync` imports the module-level singleton at call
      # time, so that is the reference the patch has to replace — rebinding
      # the routers' names leaves the dispatcher on the real registry.
      with patch(
        "robosystems.operations.providers.registry.provider_registry",
        new_registry,
      ):
        request_data = {"sync_options": {}}

        response = client.post(
          f"/v1/graphs/{VALID_TEST_GRAPH_ID}/connections/conn_123/sync",
          json=request_data,
          headers=auth_headers,
        )

        # Should return 403 Forbidden, not 500
        assert response.status_code == status.HTTP_403_FORBIDDEN
        response_json = response.json()
        # The error response has a nested structure
        assert "not available" in response_json["detail"]["detail"].lower()

  def test_delete_connection_disabled_provider(self, client: TestClient, auth_headers):
    """Test deleting a connection for disabled provider returns 403."""
    # Mock the connection service (async methods need AsyncMock)
    with patch(
      "robosystems.operations.connection_service.ConnectionService.get_connection",
      new_callable=AsyncMock,
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
        "robosystems.operations.connection_service.ConnectionService.delete_connection",
        new_callable=AsyncMock,
      ) as mock_delete:
        mock_delete.return_value = True

        with patch("robosystems.operations.providers.registry.env") as mock_env:
          # Configure mock env with QuickBooks disabled
          mock_env.CONNECTION_EXTERNAL_ENABLED = False
          mock_env.CONNECTION_QUICKBOOKS_ENABLED = False

          # Re-initialize the provider registry
          import robosystems.routers.graphs.connections.management as management
          from robosystems.operations.providers.registry import ProviderRegistry
          from robosystems.routers.graphs.connections import utils

          new_registry = ProviderRegistry()
          utils.provider_registry = new_registry
          management.provider_registry = new_registry

          response = client.delete(
            f"/v1/graphs/{VALID_TEST_GRAPH_ID}/connections/conn_123",
            headers=auth_headers,
          )

          # Disabled provider skips cleanup but still deletes successfully
          assert response.status_code == status.HTTP_200_OK
          mock_delete.assert_called_once()

  def test_invalid_provider_returns_422(self, client: TestClient, auth_headers):
    """Test that invalid provider values return 422 validation error."""
    request_data = {
      "provider": "unknown_provider",
      "entity_id": "entity_123",
    }

    response = client.post(
      f"/v1/graphs/{VALID_TEST_GRAPH_ID}/connections",
      json=request_data,
      headers=auth_headers,
    )

    # Invalid providers should return 422 Unprocessable Entity (Pydantic validation)
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    response_json = response.json()
    # 422s are normalized to ErrorResponse shape: detail is a compressed string,
    # code is "VALIDATION_ERROR" (see request_validation_handler in main.py).
    assert isinstance(response_json["detail"], str)
    assert "provider" in response_json["detail"].lower()
    assert response_json.get("code") == "VALIDATION_ERROR"

  @pytest.fixture
  def auth_headers(self, test_user, test_org, test_db):
    """Create auth headers for test requests."""
    from robosystems.models.core import Graph, GraphUser, UserAPIKey

    # Create the graph first (only if it doesn't exist)
    existing_graph = (
      test_db.query(Graph).filter(Graph.graph_id == VALID_TEST_GRAPH_ID).first()
    )
    if not existing_graph:
      Graph.create(
        graph_id=VALID_TEST_GRAPH_ID,
        org_id=test_org.id,
        graph_name="Test Graph",
        graph_type="generic",
        session=test_db,
      )

    # Create GraphUser relationship for the test graph (only if it doesn't exist)
    existing_user_graph = (
      test_db.query(GraphUser)
      .filter(
        GraphUser.user_id == test_user.id, GraphUser.graph_id == VALID_TEST_GRAPH_ID
      )
      .first()
    )
    if not existing_user_graph:
      GraphUser.create(
        user_id=test_user.id,
        graph_id=VALID_TEST_GRAPH_ID,
        role="admin",
        session=test_db,
      )

    # An active subscription: creating/syncing a connection is a write, and
    # the write gate now enforces the graph's lifecycle/subscription state
    # (billing is on under pytest.ini). Without this the write gate would 403
    # on "no subscription" before the provider-enabled check under test.
    from robosystems.models.core import OrgUser
    from robosystems.models.core.billing import BillingSubscription

    if (
      BillingSubscription.get_by_resource("graph", VALID_TEST_GRAPH_ID, test_db) is None
    ):
      org_users = OrgUser.get_user_orgs(test_user.id, test_db)
      org_id = org_users[0].org_id if org_users else test_org.id
      sub = BillingSubscription.create_subscription(
        org_id=org_id,
        resource_type="graph",
        resource_id=VALID_TEST_GRAPH_ID,
        plan_name="ladybug-standard",
        base_price_cents=9900,
        session=test_db,
      )
      sub.status = "active"
      test_db.commit()

    # Create an API key for the test user
    _, plain_key = UserAPIKey.create(
      user_id=test_user.id, name="Test API Key", session=test_db
    )

    return {"X-API-Key": plain_key}
