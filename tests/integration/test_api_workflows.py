"""
Integration tests for complete API workflows.

Tests complete user journeys including registration, authentication,
entity management, and user profile operations with metrics verification.
"""

import pytest
import os
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
from robosystems.models.iam import GraphUser, UserAPIKey


class TestUserRegistrationWorkflow:
  """Test complete user registration and setup workflow."""

  @patch("robosystems.middleware.otel.metrics.get_endpoint_metrics")
  @patch.object(
    __import__("robosystems.config", fromlist=["env"]).env,
    "USER_REGISTRATION_ENABLED",
    True,
  )
  @patch.dict(os.environ, {"ENVIRONMENT": "dev"})
  def test_complete_user_onboarding_workflow(
    self, mock_get_metrics, client: TestClient, test_db, test_org
  ):
    """Test complete user onboarding from registration to API usage."""
    mock_metrics_instance = MagicMock()
    mock_get_metrics.return_value = mock_metrics_instance

    # Step 1: User registration
    registration_data = {
      "name": "Integration Test User",
      "email": "integration@example.com",
      "password": "S3cur3P@ssw0rd!2024",  # Updated to meet password requirements
    }

    register_response = client.post("/v1/auth/register", json=registration_data)
    if register_response.status_code != 201:
      print(f"Registration failed: {register_response.status_code}")
      print(f"Error details: {register_response.json()}")
    assert register_response.status_code == 201

    user_data = register_response.json()["user"]
    user_id = user_data["id"]

    # Verify registration metrics
    business_calls = mock_metrics_instance.record_business_event.call_args_list
    registration_events = [
      call for call in business_calls if call[1]["event_type"] == "user_registered"
    ]
    assert len(registration_events) >= 1

    # Step 2: Create API key for the user
    api_key, plain_key = UserAPIKey.create(
      user_id=user_id,
      name="Integration Test API Key",
      description="For integration testing",
      session=test_db,
    )

    headers = {"X-API-Key": plain_key}

    # Step 3: Get user profile information
    profile_response = client.get("/v1/user/", headers=headers)
    if profile_response.status_code != 200:
      print(f"Response status: {profile_response.status_code}")
      print(f"Response body: {profile_response.text}")
      print(f"API key (first 10 chars): {plain_key[:10]}...")
    assert profile_response.status_code == 200

    profile_data = profile_response.json()
    assert profile_data["email"] == "integration@example.com"
    assert profile_data["name"] == "Integration Test User"

    # Step 4: Update user profile
    update_data = {
      "name": "Updated Integration User",
      "email": "updated-integration@example.com",
    }

    update_response = client.put("/v1/user/", json=update_data, headers=headers)
    assert update_response.status_code == 200

    updated_data = update_response.json()
    assert updated_data["name"] == "Updated Integration User"
    assert updated_data["email"] == "updated-integration@example.com"

    # Step 5: Create additional API key
    create_key_data = {
      "name": "Secondary API Key",
      "description": "Additional key for testing",
    }

    create_key_response = client.post(
      "/v1/user/api-keys", json=create_key_data, headers=headers
    )
    assert create_key_response.status_code == 201

    key_data = create_key_response.json()
    assert key_data["api_key"]["name"] == "Secondary API Key"
    assert "key" in key_data

    # Step 6: List all API keys
    list_keys_response = client.get("/v1/user/api-keys", headers=headers)
    assert list_keys_response.status_code == 200

    keys_data = list_keys_response.json()
    assert len(keys_data["api_keys"]) == 2  # Original + newly created

    # Verify all workflow steps recorded appropriate metrics
    all_business_calls = mock_metrics_instance.record_business_event.call_args_list
    event_types = [call[1]["event_type"] for call in all_business_calls]

    # Should include registration and user info access events
    assert "user_registered" in event_types
    assert "user_info_accessed" in event_types
    assert "api_key_created" in event_types


class TestEntityManagementWorkflow:
  """Test complete entity management workflow."""

  @pytest.fixture
  def authenticated_user_with_graph(self, client: TestClient, test_db, test_org):
    """Create authenticated user with graph access."""
    # Patch environment to ensure CAPTCHA is disabled
    with patch.dict(os.environ, {"ENVIRONMENT": "dev"}):
      registration_data = {
        "name": "Entity Workflow User",
        "email": "entityworkflow@example.com",
        "password": "S3cur3P@ssw0rd!2024",
      }
      response = client.post("/v1/auth/register", json=registration_data)
      user_data = response.json()["user"]
      user_id = user_data["id"]

    # Create API key
    api_key, plain_key = UserAPIKey.create(
      user_id=user_id, name="Entity Workflow API Key", session=test_db
    )

    # Create graph first
    from robosystems.models.iam import Graph

    Graph.create(
      graph_id="workflow-graph",
      graph_name="Workflow Test Graph",
      graph_type="entity",
      org_id=test_org.id,  # type: ignore[attr-defined]
      schema_extensions=["roboledger"],
      session=test_db,
    )

    # Create user-graph relationship
    GraphUser.create(
      user_id=user_id,
      graph_id="workflow-graph",
      role="admin",
      is_selected=True,
      session=test_db,
    )

    # Create graph credits for the workflow
    from robosystems.models.iam import GraphCredits
    from decimal import Decimal
    from datetime import datetime, timezone

    graph_credits = GraphCredits(
      id="gc_workflow-graph",
      graph_id="workflow-graph",
      user_id=user_id,
      billing_admin_id=user_id,
      current_balance=Decimal("1000.0"),
      monthly_allocation=Decimal("1000.0"),
      last_allocation_date=datetime.now(timezone.utc),
    )
    test_db.add(graph_credits)
    test_db.commit()

    headers = {"X-API-Key": plain_key}
    return headers

  # Entity CRUD workflow has been removed - use Query API instead
  # This test is deprecated as entity endpoints have been replaced by the Query API
  # See MIGRATION_ENTITY_ENDPOINTS.md for migration guide


class TestAuthenticationWorkflow:
  """Test authentication and session management workflows."""

  @patch("robosystems.middleware.otel.metrics.get_endpoint_metrics")
  @patch.object(
    __import__("robosystems.config", fromlist=["env"]).env,
    "USER_REGISTRATION_ENABLED",
    True,
  )
  @patch.dict(os.environ, {"ENVIRONMENT": "dev"})
  def test_login_session_workflow(self, mock_get_metrics, client: TestClient, test_db):
    """Test login and session management workflow."""
    mock_metrics_instance = MagicMock()
    mock_get_metrics.return_value = mock_metrics_instance

    # Step 1: Register user
    registration_data = {
      "name": "Session Test User",
      "email": "session@example.com",
      "password": "S3cur3P@ssw0rd!2024",
    }

    register_response = client.post("/v1/auth/register", json=registration_data)
    assert register_response.status_code == 201

    # Step 2: Login with the same credentials
    login_data = {"email": "session@example.com", "password": "S3cur3P@ssw0rd!2024"}

    login_response = client.post("/v1/auth/login", json=login_data)
    assert login_response.status_code == 200

    login_user_data = login_response.json()["user"]
    assert login_user_data["email"] == "session@example.com"

    # Verify JWT tokens are returned in both responses
    register_data = register_response.json()
    assert "token" in register_data
    assert register_data["token"] is not None

    login_data = login_response.json()
    assert "token" in login_data
    assert login_data["token"] is not None

    # Verify metrics were recorded for both auth events
    business_calls = mock_metrics_instance.record_business_event.call_args_list
    event_types = [call[1]["event_type"] for call in business_calls]

    assert "user_registered" in event_types
    # Login may not have a business event, but auth metrics should be recorded

  @patch("robosystems.middleware.otel.metrics.get_endpoint_metrics")
  @patch.object(
    __import__("robosystems.config", fromlist=["env"]).env,
    "USER_REGISTRATION_ENABLED",
    True,
  )
  @patch.dict(os.environ, {"ENVIRONMENT": "dev"})
  def test_password_change_workflow(
    self, mock_get_metrics, client: TestClient, test_db
  ):
    """Test password change workflow."""
    mock_metrics_instance = MagicMock()
    mock_get_metrics.return_value = mock_metrics_instance

    # Step 1: Register user
    registration_data = {
      "name": "Password Change User",
      "email": "passwordchange@example.com",
      "password": "0r1g1n@lP@ssw0rd!",
    }

    register_response = client.post("/v1/auth/register", json=registration_data)
    assert register_response.status_code == 201

    user_data = register_response.json()["user"]
    user_id = user_data["id"]

    # Step 2: Create API key for authentication
    api_key, plain_key = UserAPIKey.create(
      user_id=user_id, name="Password Change API Key", session=test_db
    )

    headers = {"X-API-Key": plain_key}

    # Step 3: Change password
    password_change_data = {
      "current_password": "0r1g1n@lP@ssw0rd!",
      "new_password": "N3wS3cur3P@ssw0rd!456",
      "confirm_password": "N3wS3cur3P@ssw0rd!456",
    }

    password_response = client.put(
      "/v1/user/password", json=password_change_data, headers=headers
    )
    assert password_response.status_code == 200

    # Step 4: Verify old password no longer works
    old_login_data = {
      "email": "passwordchange@example.com",
      "password": "0r1g1n@lP@ssw0rd!",
    }

    old_login_response = client.post("/v1/auth/login", json=old_login_data)
    assert old_login_response.status_code == 401

    # Step 5: Verify new password works
    new_login_data = {
      "email": "passwordchange@example.com",
      "password": "N3wS3cur3P@ssw0rd!456",
    }

    new_login_response = client.post("/v1/auth/login", json=new_login_data)
    assert new_login_response.status_code == 200

    # Verify complete workflow succeeded
    new_user_data = new_login_response.json()["user"]
    assert new_user_data["email"] == "passwordchange@example.com"


class TestErrorHandlingWorkflow:
  """Test error handling and recovery workflows."""

  @patch("robosystems.middleware.otel.metrics.get_endpoint_metrics")
  def test_authentication_error_handling(
    self, mock_get_metrics, client: TestClient, test_db
  ):
    """Test handling of authentication errors."""
    mock_metrics_instance = MagicMock()
    mock_get_metrics.return_value = mock_metrics_instance

    # Test various authentication failure scenarios

    # 1. No authentication header
    response = client.get("/v1/user/")
    assert response.status_code == 401

    # 2. Invalid API key format
    headers = {"Authorization": "Bearer invalid-key"}
    response = client.get("/v1/user/", headers=headers)
    assert response.status_code == 401

    # 3. Non-existent API key
    headers = {"Authorization": "Bearer sk_test_this_key_does_not_exist_12345"}
    response = client.get("/v1/user/", headers=headers)
    assert response.status_code == 401

    # 4. Wrong password on login
    login_data = {"email": "nonexistent@example.com", "password": "wrongpassword"}
    response = client.post("/v1/auth/login", json=login_data)
    assert response.status_code == 401

    # Verify error metrics were recorded (implementation dependent)
    mock_get_metrics.assert_called()

  # Entity error handling tests have been removed - use Query API instead
  # This test is deprecated as entity endpoints have been replaced by the Query API
  # See MIGRATION_ENTITY_ENDPOINTS.md for migration guide
  def test_query_api_error_handling(self, client: TestClient, test_db):
    """Test handling of query API errors."""
    # This test can be updated to test the query API error handling instead
    # For now, we'll skip the entity-specific error tests
    pass


class TestPerformanceWorkflow:
  """Test performance-related workflows and stress scenarios."""

  @patch("robosystems.middleware.otel.metrics.get_endpoint_metrics")
  @patch.object(
    __import__("robosystems.config", fromlist=["env"]).env,
    "USER_REGISTRATION_ENABLED",
    True,
  )
  @patch.dict(os.environ, {"ENVIRONMENT": "dev"})
  def test_multiple_concurrent_operations(
    self, mock_get_metrics, client: TestClient, test_db
  ):
    """Test handling of multiple concurrent operations."""
    mock_metrics_instance = MagicMock()
    mock_get_metrics.return_value = mock_metrics_instance

    # Create multiple users in sequence
    users = []
    for i in range(5):
      registration_data = {
        "name": f"Concurrent User {i}",
        "email": f"concurrent{i}@example.com",
        "password": "S3cur3P@ssw0rd!2024",
      }

      response = client.post("/v1/auth/register", json=registration_data)
      assert response.status_code == 201

      user_data = response.json()["user"]
      users.append(user_data)

    # Verify all users were created successfully
    assert len(users) == 5

    # Test rapid API key creation for each user
    for user in users:
      api_key, plain_key = UserAPIKey.create(
        user_id=user["id"], name="Concurrent Test Key", session=test_db
      )

      headers = {"X-API-Key": plain_key}

      # Test rapid profile access
      response = client.get("/v1/user/", headers=headers)
      assert response.status_code == 200

    # Verify metrics were recorded for all operations
    assert mock_get_metrics.call_count >= 10  # Multiple calls expected
