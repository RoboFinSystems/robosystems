"""Tests for the user router."""

from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

from main import app
from robosystems.middleware.auth.dependencies import get_current_user


@pytest.fixture
def mock_user_with_accounts():
  """Create a mock user with accounts."""
  # Create a mock for accounts
  mock_account = Mock()
  mock_account.providerId = "github"
  mock_account.providerType = "oauth"
  mock_account.providerAccountId = "12345"

  # Create mock user
  mock_user = Mock()
  mock_user.id = "test-user-id"
  mock_user.name = "Test User"
  mock_user.email = "test@example.com"
  mock_user.accounts = [mock_account]

  return mock_user


@pytest.fixture
def mock_user_without_accounts():
  """Create a mock user without accounts."""
  mock_user = Mock()
  mock_user.id = "test-user-id"
  mock_user.name = "Test User"
  mock_user.email = "test@example.com"
  mock_user.accounts = []

  return mock_user


@pytest.fixture
def error_user():
  """Create a mock user that raises an error."""
  mock_user = Mock()
  mock_user.id = "test-user-id"

  # Make accounts property raise an exception
  def accounts_getter():
    raise Exception("Test error")

  type(mock_user).accounts = property(accounts_getter)

  return mock_user


@pytest.mark.unit
def test_get_current_user_info(mock_user_with_accounts):
  """Test getting current user info."""
  from robosystems.middleware.rate_limits import (
    analytics_rate_limit_dependency,
    auth_rate_limit_dependency,
    backup_operations_rate_limit_dependency,
    connection_management_rate_limit_dependency,
    general_api_rate_limit_dependency,
    rate_limit_dependency,
    sensitive_auth_rate_limit_dependency,
    subscription_aware_rate_limit_dependency,
    sync_operations_rate_limit_dependency,
    tasks_management_rate_limit_dependency,
    user_management_rate_limit_dependency,
  )

  # Override the dependency
  app.dependency_overrides[get_current_user] = lambda: mock_user_with_accounts

  # Disable rate limiting during tests
  app.dependency_overrides[auth_rate_limit_dependency] = lambda: None
  app.dependency_overrides[rate_limit_dependency] = lambda: None
  app.dependency_overrides[user_management_rate_limit_dependency] = lambda: None
  app.dependency_overrides[sync_operations_rate_limit_dependency] = lambda: None
  app.dependency_overrides[connection_management_rate_limit_dependency] = lambda: None
  app.dependency_overrides[analytics_rate_limit_dependency] = lambda: None
  app.dependency_overrides[backup_operations_rate_limit_dependency] = lambda: None
  app.dependency_overrides[sensitive_auth_rate_limit_dependency] = lambda: None
  app.dependency_overrides[tasks_management_rate_limit_dependency] = lambda: None
  app.dependency_overrides[general_api_rate_limit_dependency] = lambda: None
  app.dependency_overrides[subscription_aware_rate_limit_dependency] = lambda: None

  # Create a test client
  client = TestClient(app)

  # Make the request
  response = client.get("/v1/user")

  # Reset overrides after test
  app.dependency_overrides = {}

  # Verify the response
  assert response.status_code == 200
  data = response.json()
  assert data["id"] == "test-user-id"
  assert data["name"] == "Test User"
  assert data["email"] == "test@example.com"

  # Verify accounts are included (empty for now since we don't have OAuth integration)
  assert data["accounts"] == []


@pytest.mark.unit
def test_get_current_user_info_no_accounts(mock_user_without_accounts):
  """Test getting current user info without any accounts."""
  from robosystems.middleware.rate_limits import (
    analytics_rate_limit_dependency,
    auth_rate_limit_dependency,
    backup_operations_rate_limit_dependency,
    connection_management_rate_limit_dependency,
    general_api_rate_limit_dependency,
    rate_limit_dependency,
    sensitive_auth_rate_limit_dependency,
    subscription_aware_rate_limit_dependency,
    sync_operations_rate_limit_dependency,
    tasks_management_rate_limit_dependency,
    user_management_rate_limit_dependency,
  )

  # Override the dependency
  app.dependency_overrides[get_current_user] = lambda: mock_user_without_accounts

  # Disable rate limiting during tests
  app.dependency_overrides[auth_rate_limit_dependency] = lambda: None
  app.dependency_overrides[rate_limit_dependency] = lambda: None
  app.dependency_overrides[user_management_rate_limit_dependency] = lambda: None
  app.dependency_overrides[sync_operations_rate_limit_dependency] = lambda: None
  app.dependency_overrides[connection_management_rate_limit_dependency] = lambda: None
  app.dependency_overrides[analytics_rate_limit_dependency] = lambda: None
  app.dependency_overrides[backup_operations_rate_limit_dependency] = lambda: None
  app.dependency_overrides[sensitive_auth_rate_limit_dependency] = lambda: None
  app.dependency_overrides[tasks_management_rate_limit_dependency] = lambda: None
  app.dependency_overrides[general_api_rate_limit_dependency] = lambda: None
  app.dependency_overrides[subscription_aware_rate_limit_dependency] = lambda: None

  # Create a test client
  client = TestClient(app)

  # Make the request
  response = client.get("/v1/user")

  # Reset overrides after test
  app.dependency_overrides = {}

  # Verify the response
  assert response.status_code == 200
  data = response.json()
  assert data["id"] == "test-user-id"
  assert data["name"] == "Test User"
  assert data["email"] == "test@example.com"
  assert data["accounts"] == []


@pytest.mark.unit
def test_get_current_user_info_error_handling(error_user):
  """Test error handling in the user endpoint."""
  from robosystems.middleware.rate_limits import (
    analytics_rate_limit_dependency,
    auth_rate_limit_dependency,
    backup_operations_rate_limit_dependency,
    connection_management_rate_limit_dependency,
    general_api_rate_limit_dependency,
    rate_limit_dependency,
    sensitive_auth_rate_limit_dependency,
    subscription_aware_rate_limit_dependency,
    sync_operations_rate_limit_dependency,
    tasks_management_rate_limit_dependency,
    user_management_rate_limit_dependency,
  )

  # Override the dependency
  app.dependency_overrides[get_current_user] = lambda: error_user

  # Disable rate limiting during tests
  app.dependency_overrides[auth_rate_limit_dependency] = lambda: None
  app.dependency_overrides[rate_limit_dependency] = lambda: None
  app.dependency_overrides[user_management_rate_limit_dependency] = lambda: None
  app.dependency_overrides[sync_operations_rate_limit_dependency] = lambda: None
  app.dependency_overrides[connection_management_rate_limit_dependency] = lambda: None
  app.dependency_overrides[analytics_rate_limit_dependency] = lambda: None
  app.dependency_overrides[backup_operations_rate_limit_dependency] = lambda: None
  app.dependency_overrides[sensitive_auth_rate_limit_dependency] = lambda: None
  app.dependency_overrides[tasks_management_rate_limit_dependency] = lambda: None
  app.dependency_overrides[general_api_rate_limit_dependency] = lambda: None
  app.dependency_overrides[subscription_aware_rate_limit_dependency] = lambda: None

  # Create a test client
  client = TestClient(app)

  # Make the request
  response = client.get("/v1/user")

  # Reset overrides after test
  app.dependency_overrides = {}

  # Verify error response
  assert response.status_code == 500
  error_response = response.json()
  # The error is wrapped in a detail field by FastAPI
  assert error_response["detail"]["detail"] == "Error retrieving user information"
  assert error_response["detail"]["code"] == "INTERNAL_ERROR"


class TestUserAPIUnauthorizedAccess:
  """Test unauthorized access to user API endpoints."""

  def test_unauthorized_access(self):
    """Test access without authentication."""
    from fastapi.testclient import TestClient

    from main import app

    # Save current dependency overrides and clear them
    original_overrides = app.dependency_overrides.copy()
    app.dependency_overrides.clear()

    try:
      # Create a client without any auth overrides
      with TestClient(app) as test_client:
        # Test GET /v1/user endpoint
        response = test_client.get("/v1/user")
        assert response.status_code == 401
    finally:
      # Restore original overrides
      app.dependency_overrides = original_overrides

  def test_invalid_api_key(self):
    """Test access with invalid API key."""
    from fastapi.testclient import TestClient

    from main import app

    # Save current dependency overrides and clear them
    original_overrides = app.dependency_overrides.copy()
    app.dependency_overrides.clear()

    try:
      # Create a client without any auth overrides
      with TestClient(app) as test_client:
        headers = {"Authorization": "Bearer invalid-api-key-12345"}
        response = test_client.get("/v1/user", headers=headers)
        assert response.status_code == 401
    finally:
      # Restore original overrides
      app.dependency_overrides = original_overrides
