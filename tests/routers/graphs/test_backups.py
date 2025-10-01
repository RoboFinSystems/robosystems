"""
Tests for backup router endpoints.

This test suite validates REST API endpoints for backup management
including authentication, request validation, and response formatting.
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock

from main import app


class TestBackupEndpoints:
  """Test suite for backup REST API endpoints."""

  @pytest.fixture
  def client(self):
    """Create test client with rate limiting disabled."""
    from robosystems.middleware.rate_limits import (
      auth_rate_limit_dependency,
      user_management_rate_limit_dependency,
      sync_operations_rate_limit_dependency,
      connection_management_rate_limit_dependency,
      analytics_rate_limit_dependency,
      backup_operations_rate_limit_dependency,
      sensitive_auth_rate_limit_dependency,
      tasks_management_rate_limit_dependency,
      general_api_rate_limit_dependency,
      subscription_aware_rate_limit_dependency,
      auth_status_rate_limit_dependency,
      sso_rate_limit_dependency,
      graph_scoped_rate_limit_dependency,
    )

    # Save original overrides
    original_overrides = app.dependency_overrides.copy()

    # Disable rate limiting during tests
    app.dependency_overrides[auth_rate_limit_dependency] = lambda: None
    app.dependency_overrides[user_management_rate_limit_dependency] = lambda: None
    app.dependency_overrides[sync_operations_rate_limit_dependency] = lambda: None
    app.dependency_overrides[connection_management_rate_limit_dependency] = lambda: None
    app.dependency_overrides[analytics_rate_limit_dependency] = lambda: None
    app.dependency_overrides[backup_operations_rate_limit_dependency] = lambda: None
    app.dependency_overrides[sensitive_auth_rate_limit_dependency] = lambda: None
    app.dependency_overrides[tasks_management_rate_limit_dependency] = lambda: None
    app.dependency_overrides[general_api_rate_limit_dependency] = lambda: None
    app.dependency_overrides[subscription_aware_rate_limit_dependency] = lambda: None
    app.dependency_overrides[auth_status_rate_limit_dependency] = lambda: None
    app.dependency_overrides[sso_rate_limit_dependency] = lambda: None
    app.dependency_overrides[graph_scoped_rate_limit_dependency] = lambda: None

    client = TestClient(app)
    yield client

    # Restore original overrides
    app.dependency_overrides = original_overrides

  @pytest.fixture
  def mock_auth_user(self):
    """Mock authenticated user."""
    user = MagicMock()
    user.id = "test-user-123"
    user.email = "test@example.com"
    return user

  def test_backup_endpoints_require_authentication(self, client):
    """Test that backup endpoints require authentication."""
    graph_id = "test_graph_123"  # Use underscores instead of dashes

    # Test endpoints without authentication
    endpoints = [
      ("POST", f"/v1/graphs/{graph_id}/backups"),
      ("GET", f"/v1/graphs/{graph_id}/backups"),
      ("GET", f"/v1/graphs/{graph_id}/backups/stats"),
      ("DELETE", f"/v1/graphs/{graph_id}/backups/cleanup"),
      ("GET", f"/v1/graphs/{graph_id}/backups/health"),
      ("GET", f"/v1/graphs/{graph_id}/backups/metrics"),
    ]

    for method, endpoint in endpoints:
      if method == "POST":
        response = client.post(endpoint, json={})
      elif method == "GET":
        response = client.get(endpoint)
      elif method == "DELETE":
        response = client.delete(endpoint)
      else:
        raise ValueError(f"Unsupported HTTP method: {method}")

      # Should require authentication, not exist, or be unavailable
      assert response.status_code in [401, 403, 404, 422, 503], (
        f"Expected auth/not found/unavailable error for {method} {endpoint}"
      )

  @patch("robosystems.models.iam.graph_credits.GraphCredits.get_by_graph_id")
  @patch("robosystems.tasks.graph_operations.backup.create_graph_backup_sse")
  @patch("os.path.exists")
  @patch(
    "robosystems.middleware.graph.multitenant_utils.MultiTenantUtils.get_database_path_for_graph"
  )
  @patch(
    "robosystems.middleware.graph.multitenant_utils.MultiTenantUtils.is_shared_repository"
  )
  @patch("robosystems.models.iam.UserGraph.get_by_user_id")
  def test_create_backup_endpoint(
    self,
    mock_get_by_user_id,
    mock_is_shared,
    mock_get_database_path,
    mock_path_exists,
    mock_task,
    mock_get_graph_credits,
    client,
    mock_auth_user,
  ):
    """Test backup creation endpoint."""
    from robosystems.middleware.auth.dependencies import get_current_user
    from robosystems.database import session

    # Create mock session
    mock_session = MagicMock()

    # Override the dependencies
    app.dependency_overrides[get_current_user] = lambda: mock_auth_user
    app.dependency_overrides[session] = lambda: mock_session

    try:
      # Mock GraphCredits for the reservation pattern
      mock_graph_credits = MagicMock()
      mock_graph_credits.available_credits = 1000.0
      mock_graph_credits.consumed_credits = 0.0
      mock_graph_credits.reserve_credits = MagicMock(return_value=True)
      mock_get_graph_credits.return_value = mock_graph_credits

      # Mock authorization checks
      mock_is_shared.return_value = False  # Not a shared repository

      # Mock database path - returns the database path for the graph
      mock_get_database_path.return_value = "/tmp/kuzu/test_graph_123"

      # Mock os.path.exists to return False (database doesn't exist yet, size will be 0)
      mock_path_exists.return_value = False

      # Mock user graph access - create a mock UserGraph with the test graph_id and admin role
      mock_user_graph = MagicMock()
      mock_user_graph.graph_id = "test_graph_123"
      mock_user_graph.role = "admin"  # Admin role required for backup creation
      mock_get_by_user_id.return_value = [
        mock_user_graph
      ]  # User has access to test_graph_123

      # Set mock user ID
      mock_auth_user.id = "test-user-123"

      # Mock task response
      mock_task_result = MagicMock()
      mock_task_result.id = "task-123"
      mock_task.apply_async.return_value = mock_task_result

      graph_id = "test_graph_123"  # Use underscores instead of dashes
      response = client.post(
        f"/v1/graphs/{graph_id}/backups",
        json={
          "backup_format": "full_dump",  # Required for encryption
          "backup_type": "full",
          "retention_days": 90,
          "compression": True,
          "encryption": True,
        },
      )

      if response.status_code != 202:
        print(f"Response status: {response.status_code}")
        print(f"Response content: {response.text}")
      assert response.status_code == 202
      data = response.json()
      assert "operation_id" in data  # Should have an operation ID
      assert data["status"] == "accepted"
      assert "Backup creation started" in data["message"]

      # Note: This endpoint uses background tasks, not the mocked Celery task
      # The actual backup is executed as a background FastAPI task

      # Verify authorization checks were called
      mock_get_by_user_id.assert_called_once()
    finally:
      # Reset only the specific overrides we added
      if get_current_user in app.dependency_overrides:
        del app.dependency_overrides[get_current_user]
      if session in app.dependency_overrides:
        del app.dependency_overrides[session]

  @patch(
    "robosystems.middleware.graph.multitenant_utils.MultiTenantUtils.is_shared_repository"
  )
  @patch("robosystems.models.iam.UserGraph.get_by_user_id")
  def test_list_backups_endpoint(
    self, mock_get_by_user_id, mock_is_shared, client, mock_auth_user
  ):
    """Test backup listing endpoint."""
    from robosystems.middleware.auth.dependencies import get_current_user
    from robosystems.database import session

    # Create mock session
    mock_session = MagicMock()

    # Override the dependencies
    app.dependency_overrides[get_current_user] = lambda: mock_auth_user
    app.dependency_overrides[session] = lambda: mock_session

    try:
      # Mock authorization checks
      mock_is_shared.return_value = False  # Not a shared repository

      # Mock user graph access - create a mock UserGraph with the test graph_id and admin role
      mock_user_graph = MagicMock()
      mock_user_graph.graph_id = "test_graph_123"
      mock_user_graph.role = "admin"  # Admin role required for backup creation
      mock_get_by_user_id.return_value = [
        mock_user_graph
      ]  # User has access to test_graph_123

      # Set mock user ID
      mock_auth_user.id = "test-user-123"

      # Mock GraphBackup query
      mock_backup_query = MagicMock()
      mock_backup_query.filter.return_value.order_by.return_value.all.return_value = []  # Empty backup list
      mock_session.query.return_value = mock_backup_query

      graph_id = "test_graph_123"  # Use underscores instead of dashes
      response = client.get(
        f"/v1/graphs/{graph_id}/backups", headers={"X-API-Key": "test-api-key"}
      )

      assert response.status_code == 200
      data = response.json()
      assert data["graph_id"] == graph_id
      assert "backups" in data
      assert "total_count" in data

      # Verify authorization checks were called
      # Note: list_backups endpoint doesn't call is_shared_repository
      mock_get_by_user_id.assert_called_once()
    finally:
      # Reset only the specific overrides we added
      if get_current_user in app.dependency_overrides:
        del app.dependency_overrides[get_current_user]
      if session in app.dependency_overrides:
        del app.dependency_overrides[session]

  def test_openapi_schema_includes_backup_endpoints(self, client):
    """Test that OpenAPI schema includes backup endpoints."""
    response = client.get("/openapi.json")
    assert response.status_code == 200

    schema = response.json()
    paths = schema.get("paths", {})

    # Check for backup endpoints
    backup_paths = [p for p in paths.keys() if "/backups" in p]
    assert len(backup_paths) > 0, "No backup endpoints found in OpenAPI schema"

    # Verify operation IDs exist
    for path in backup_paths:
      for method, details in paths[path].items():
        assert "operationId" in details, (
          f"Missing operationId for {method.upper()} {path}"
        )
        assert "summary" in details, f"Missing summary for {method.upper()} {path}"
