"""
Comprehensive tests for user profile and API key management endpoints.

Tests use pure mocked authentication pattern for reliability and consistency.
These tests focus on endpoint behavior rather than full integration testing.
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock, Mock
import bcrypt


class TestUserProfile:
  """Test user profile management endpoints using pure mocked authentication."""

  @pytest.fixture
  def mock_user(self):
    """Create a consistent mock user for testing."""
    mock_user = Mock()
    mock_user.id = "test-user-123"
    mock_user.email = "test@example.com"
    mock_user.name = "Test User"
    mock_user.password_hash = "dummy_hash"
    mock_user.accounts = []
    mock_user.__name__ = "User"  # Add __name__ attribute for logging
    return mock_user

  @pytest.fixture
  def client_with_user(self, mock_user):
    """Create test client with mocked user authentication."""
    from main import app
    from robosystems.middleware.auth.dependencies import get_current_user
    from robosystems.middleware.rate_limits import (
      auth_rate_limit_dependency,
      rate_limit_dependency,
      user_management_rate_limit_dependency,
      sync_operations_rate_limit_dependency,
      connection_management_rate_limit_dependency,
      analytics_rate_limit_dependency,
      backup_operations_rate_limit_dependency,
      sensitive_auth_rate_limit_dependency,
      tasks_management_rate_limit_dependency,
      general_api_rate_limit_dependency,
      subscription_aware_rate_limit_dependency,
    )

    # Mock the authentication dependency
    app.dependency_overrides[get_current_user] = lambda: mock_user

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

    client = TestClient(app)
    client.mock_user = mock_user

    yield client

    # Cleanup
    app.dependency_overrides = {}

  def test_get_current_user_info_success(self, client_with_user: TestClient):
    """Test successful retrieval of current user info."""
    response = client_with_user.get("/v1/user/")

    assert response.status_code == 200
    data = response.json()

    # Check response structure
    assert "id" in data
    assert "name" in data
    assert "email" in data
    assert "accounts" in data

    # Check user data matches mock
    assert data["id"] == "test-user-123"
    assert data["name"] == "Test User"
    assert data["email"] == "test@example.com"
    assert data["accounts"] == []

  def test_get_current_user_info_unauthorized(self):
    """Test user info retrieval without authentication."""
    from main import app
    from fastapi.testclient import TestClient

    # Save current dependency overrides and clear them
    original_overrides = app.dependency_overrides.copy()
    app.dependency_overrides.clear()

    try:
      # Create a client without any auth overrides
      with TestClient(app) as test_client:
        response = test_client.get("/v1/user/")
        assert response.status_code == 401
    finally:
      # Restore original overrides
      app.dependency_overrides = original_overrides

  def test_get_current_user_info_invalid_token(self):
    """Test user info retrieval with invalid token."""
    from main import app
    from fastapi.testclient import TestClient

    # Save current dependency overrides and clear them
    original_overrides = app.dependency_overrides.copy()
    app.dependency_overrides.clear()

    try:
      # Create a client without any auth overrides
      with TestClient(app) as test_client:
        headers = {"Authorization": "Bearer invalid-token-12345"}
        response = test_client.get("/v1/user/", headers=headers)
        assert response.status_code == 401
    finally:
      # Restore original overrides
      app.dependency_overrides = original_overrides

  @pytest.fixture
  def client_with_real_user(self, test_db, test_user):
    """Create test client with real test user authentication."""
    from main import app
    from robosystems.middleware.auth.dependencies import get_current_user
    from robosystems.middleware.rate_limits import (
      user_management_rate_limit_dependency,
      subscription_aware_rate_limit_dependency,
    )
    from robosystems.database import get_db_session

    # Use the real test_user
    mock_user = Mock()
    mock_user.id = test_user.id
    mock_user.email = test_user.email
    mock_user.name = test_user.name
    mock_user.accounts = []

    # Override dependencies
    app.dependency_overrides[get_current_user] = lambda: mock_user
    app.dependency_overrides[user_management_rate_limit_dependency] = lambda: None
    app.dependency_overrides[subscription_aware_rate_limit_dependency] = lambda: None

    # Override the database session dependency
    def override_get_db():
      yield test_db

    app.dependency_overrides[get_db_session] = override_get_db

    client = TestClient(app)
    yield client

    # Reset the dependency overrides
    app.dependency_overrides = {}

  @patch("robosystems.routers.user.main.get_endpoint_metrics")
  @patch("robosystems.security.audit_logger.SecurityAuditLogger.log_security_event")
  def test_update_user_profile_success(
    self,
    mock_log_security_event,
    mock_get_endpoint_metrics,
    client_with_real_user: TestClient,
    db_session,
    test_user,
  ):
    """Test successful user profile update."""
    import uuid

    # Ensure test_user is in the database
    db_session.merge(test_user)
    db_session.commit()

    # Mock the metrics instance
    mock_metrics_instance = Mock()
    mock_metrics_instance.record_business_event = Mock()
    mock_get_endpoint_metrics.return_value = mock_metrics_instance

    # Create unique email for update
    new_email = f"updated-test+{str(uuid.uuid4())[:8]}@example.com"
    update_data = {"name": "Updated Test User", "email": new_email}

    response = client_with_real_user.put("/v1/user/", json=update_data)

    assert response.status_code == 200
    data = response.json()

    # Check updated data
    assert data["name"] == "Updated Test User"
    assert data["email"] == new_email

    # Verify the user was actually updated in the database
    db_session.refresh(test_user)
    assert test_user.name == "Updated Test User"
    assert test_user.email == new_email

  def test_update_user_profile_empty_data(self, client_with_user: TestClient):
    """Test user profile update with no data."""
    response = client_with_user.put("/v1/user/", json={})

    assert response.status_code == 400
    data = response.json()
    # Handle structured error response
    assert "no fields provided" in data["detail"]["detail"].lower()

  @patch("robosystems.models.iam.User.get_by_id")
  @patch("robosystems.models.iam.User.get_by_email")
  def test_update_user_profile_email_conflict(
    self, mock_get_by_email, mock_get_by_id, client_with_user: TestClient
  ):
    """Test user profile update with email already in use."""
    # Mock the user lookup to return our mock user
    mock_get_by_id.return_value = client_with_user.mock_user

    # Mock email conflict - another user has this email
    existing_user = Mock()
    existing_user.id = "different-user-id"
    existing_user.email = "existing@example.com"
    mock_get_by_email.return_value = existing_user

    update_data = {"email": "existing@example.com"}

    response = client_with_user.put("/v1/user/", json=update_data)

    assert response.status_code == 409
    data = response.json()
    # Handle structured error response
    assert "already in use" in data["detail"]["detail"].lower()

  def test_update_user_profile_invalid_data(self, client_with_user: TestClient):
    """Test user profile update with invalid data."""
    # Empty name
    response = client_with_user.put("/v1/user/", json={"name": ""})
    assert response.status_code == 422

    # Invalid email format
    response = client_with_user.put("/v1/user/", json={"email": "not-an-email"})
    assert response.status_code == 422


class TestUserGraphs:
  """Test user graph management endpoints using pure mocked authentication."""

  @pytest.fixture
  def mock_user_with_graphs(self):
    """Create a mock user with graphs."""
    mock_user = Mock()
    mock_user.id = "test-user-456"
    mock_user.email = "graphtest@example.com"
    mock_user.name = "Graph Test User"

    # Mock user graphs with proper datetime objects and Graph relationships
    from datetime import datetime, timezone

    # Create mock Graph objects for each UserGraph
    mock_graph1 = Mock()
    mock_graph1.graph_name = "Test Graph 1"
    mock_graph1.graph_tier = "kuzu-standard"
    mock_graph1.graph_type = "entity"

    mock_graph2 = Mock()
    mock_graph2.graph_name = "Test Graph 2"
    mock_graph2.graph_tier = "kuzu-standard"
    mock_graph2.graph_type = "entity"

    mock_graph3 = Mock()
    mock_graph3.graph_name = "Test Graph 3"
    mock_graph3.graph_tier = "kuzu-standard"
    mock_graph3.graph_type = "entity"

    mock_graphs = [
      Mock(
        user_id="test-user-456",
        graph_id="graph1",
        role="admin",
        is_selected=True,
        created_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        graph=mock_graph1,  # Relationship to Graph object
      ),
      Mock(
        user_id="test-user-456",
        graph_id="graph2",
        role="member",
        is_selected=False,
        created_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        graph=mock_graph2,  # Relationship to Graph object
      ),
      Mock(
        user_id="test-user-456",
        graph_id="graph3",
        role="admin",
        is_selected=False,
        created_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        graph=mock_graph3,  # Relationship to Graph object
      ),
    ]

    mock_user.graphs = mock_graphs
    return mock_user

  @pytest.fixture
  def client_with_graphs(self, mock_user_with_graphs):
    """Create test client with mocked user that has graphs."""
    from main import app
    from robosystems.middleware.auth.dependencies import (
      get_current_user,
      get_current_user_with_graph,
    )
    from robosystems.middleware.rate_limits import (
      auth_rate_limit_dependency,
      rate_limit_dependency,
      user_management_rate_limit_dependency,
      sync_operations_rate_limit_dependency,
      connection_management_rate_limit_dependency,
      analytics_rate_limit_dependency,
      backup_operations_rate_limit_dependency,
      sensitive_auth_rate_limit_dependency,
      tasks_management_rate_limit_dependency,
      general_api_rate_limit_dependency,
      subscription_aware_rate_limit_dependency,
    )

    # Mock the authentication dependency
    app.dependency_overrides[get_current_user] = lambda: mock_user_with_graphs
    app.dependency_overrides[get_current_user_with_graph] = (
      lambda: mock_user_with_graphs
    )

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

    client = TestClient(app)
    client.mock_user = mock_user_with_graphs

    yield client

    # Cleanup
    app.dependency_overrides = {}

  @patch("robosystems.models.iam.GraphUser.get_by_user_id")
  def test_get_user_graphs_success(
    self, mock_get_by_user_id, client_with_graphs: TestClient
  ):
    """Test successful retrieval of user graphs."""
    # Mock the database query to return the user's graphs
    mock_get_by_user_id.return_value = client_with_graphs.mock_user.graphs

    response = client_with_graphs.get("/v1/graphs")

    assert response.status_code == 200
    data = response.json()

    # Check response structure
    assert "graphs" in data
    assert "selectedGraphId" in data

    # Check graph data
    assert len(data["graphs"]) == 3
    assert data["selectedGraphId"] == "graph1"

    # Check individual graph structure
    graph = data["graphs"][0]
    assert "graphId" in graph
    assert "graphName" in graph
    assert "role" in graph
    assert "isSelected" in graph
    assert "createdAt" in graph

  @patch("robosystems.models.iam.GraphUser.set_selected_graph")
  @patch("robosystems.models.iam.GraphUser.get_by_user_id")
  def test_select_user_graph_success(
    self, mock_get_by_user_id, mock_set_selected, client_with_graphs: TestClient
  ):
    """Test successful graph selection."""
    # Mock the user's available graphs (to pass access check)
    mock_get_by_user_id.return_value = client_with_graphs.mock_user.graphs

    # Mock successful graph selection
    mock_set_selected.return_value = True

    response = client_with_graphs.post("/v1/graphs/graph2/select")

    assert response.status_code == 200
    data = response.json()

    # SuccessResponse format
    assert data["success"] is True
    assert data["message"] == "Graph selected successfully"
    assert data["data"]["selectedGraphId"] == "graph2"

    # Verify the method was called with correct parameters
    mock_set_selected.assert_called_once()

  @patch("robosystems.models.iam.GraphUser.get_by_user_id")
  def test_select_user_graph_access_denied(
    self, mock_get_by_user_id, client_with_graphs: TestClient
  ):
    """Test graph selection for graph user doesn't have access to."""
    # Mock the user's available graphs (only has access to graph1, graph2, graph3)
    mock_get_by_user_id.return_value = client_with_graphs.mock_user.graphs

    # Try to select a graph that's not in the user's available graphs
    response = client_with_graphs.post("/v1/graphs/unauthorized-graph/select")

    assert response.status_code == 403
    data = response.json()
    # Handle structured error response
    assert "access denied" in data["detail"]["detail"].lower()

  @patch("robosystems.models.iam.GraphUser.set_selected_graph")
  @patch("robosystems.models.iam.GraphUser.get_by_user_id")
  def test_select_user_graph_not_found(
    self, mock_get_by_user_id, mock_set_selected, client_with_graphs: TestClient
  ):
    """Test graph selection for non-existent graph."""
    # Create a mock graph that includes the requested graph in user's available graphs
    mock_graphs = client_with_graphs.mock_user.graphs.copy()
    mock_graphs.append(Mock(graph_id="nonexistent-graph"))
    mock_get_by_user_id.return_value = mock_graphs

    # Mock graph not found by set_selected_graph (passes access check but fails selection)
    mock_set_selected.return_value = False

    response = client_with_graphs.post("/v1/graphs/nonexistent-graph/select")

    assert response.status_code == 404


class TestUserAPIKeys:
  """Test user API key management endpoints using pure mocked authentication."""

  @pytest.fixture
  def mock_user_with_api_keys(self):
    """Create a mock user with API keys."""
    mock_user = Mock()
    mock_user.id = "test-user-789"
    mock_user.email = "apitest@example.com"
    mock_user.name = "API Test User"

    # Mock API keys with proper datetime objects
    from datetime import datetime, timezone

    mock_key1 = Mock()
    mock_key1.id = "key1"
    mock_key1.name = "Primary API Key"
    mock_key1.description = "For authentication"
    mock_key1.prefix = "rfs123"
    mock_key1.is_active = True
    mock_key1.created_at = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    mock_key1.last_used_at = None
    mock_key1.expires_at = None

    mock_key2 = Mock()
    mock_key2.id = "key2"
    mock_key2.name = "Test API Key 1"
    mock_key2.description = "Test key 1"
    mock_key2.prefix = "rfs456"
    mock_key2.is_active = True
    mock_key2.created_at = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    mock_key2.last_used_at = None
    mock_key2.expires_at = None

    mock_key3 = Mock()
    mock_key3.id = "key3"
    mock_key3.name = "Test API Key 2"
    mock_key3.description = "Test key 2"
    mock_key3.prefix = "rfs789"
    mock_key3.is_active = True
    mock_key3.created_at = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    mock_key3.last_used_at = None
    mock_key3.expires_at = None

    mock_keys = [mock_key1, mock_key2, mock_key3]

    mock_user.api_keys = mock_keys
    return mock_user

  @pytest.fixture
  def client_with_api_keys(self, mock_user_with_api_keys):
    """Create test client with mocked user that has API keys."""
    from main import app
    from robosystems.middleware.auth.dependencies import get_current_user
    from robosystems.middleware.rate_limits import (
      auth_rate_limit_dependency,
      rate_limit_dependency,
      user_management_rate_limit_dependency,
      sync_operations_rate_limit_dependency,
      connection_management_rate_limit_dependency,
      analytics_rate_limit_dependency,
      backup_operations_rate_limit_dependency,
      sensitive_auth_rate_limit_dependency,
      tasks_management_rate_limit_dependency,
      general_api_rate_limit_dependency,
      subscription_aware_rate_limit_dependency,
    )

    # Mock the authentication dependency
    app.dependency_overrides[get_current_user] = lambda: mock_user_with_api_keys

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

    client = TestClient(app)
    client.mock_user = mock_user_with_api_keys

    yield client

    # Cleanup
    app.dependency_overrides = {}

  @patch("robosystems.models.iam.UserAPIKey.get_by_user_id")
  def test_list_api_keys_success(
    self, mock_get_by_user_id, client_with_api_keys: TestClient
  ):
    """Test successful API key listing."""
    # Mock the database query to return the user's API keys
    mock_get_by_user_id.return_value = client_with_api_keys.mock_user.api_keys

    response = client_with_api_keys.get("/v1/user/api-keys")

    assert response.status_code == 200
    data = response.json()

    # Check response structure
    assert "api_keys" in data
    assert len(data["api_keys"]) == 3

    # Check API key structure
    api_key = data["api_keys"][0]
    assert "id" in api_key
    assert "name" in api_key
    assert "description" in api_key
    assert "prefix" in api_key
    assert "is_active" in api_key
    assert "created_at" in api_key
    assert "last_used_at" in api_key

  @patch("robosystems.models.iam.UserAPIKey.create")
  def test_create_api_key_success(self, mock_create, client_with_api_keys: TestClient):
    """Test successful API key creation."""
    # Mock API key creation
    from datetime import datetime, timezone

    mock_api_key = Mock()
    mock_api_key.id = "new-key-id"
    mock_api_key.name = "New Test API Key"
    mock_api_key.description = "Newly created test key"
    mock_api_key.prefix = "rfs999"
    mock_api_key.is_active = True
    mock_api_key.created_at = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    mock_api_key.last_used_at = None
    mock_api_key.expires_at = None

    mock_create.return_value = (mock_api_key, "rfs999_full_api_key_here")

    create_data = {"name": "New Test API Key", "description": "Newly created test key"}

    response = client_with_api_keys.post("/v1/user/api-keys", json=create_data)

    assert response.status_code == 201
    data = response.json()

    # Check response structure
    assert "api_key" in data
    assert "key" in data

    # Check API key data
    api_key_info = data["api_key"]
    assert api_key_info["name"] == "New Test API Key"
    assert api_key_info["description"] == "Newly created test key"
    assert api_key_info["is_active"]

    # Check that actual key is returned
    assert len(data["key"]) > 20  # API keys should be reasonably long

  def test_create_api_key_invalid_data(self, client_with_api_keys: TestClient):
    """Test API key creation with invalid data."""
    # Missing name
    response = client_with_api_keys.post("/v1/user/api-keys", json={})
    assert response.status_code == 422

    # Empty name
    response = client_with_api_keys.post("/v1/user/api-keys", json={"name": ""})
    assert response.status_code == 422

    # Name too long
    response = client_with_api_keys.post("/v1/user/api-keys", json={"name": "x" * 200})
    assert response.status_code == 422

  @patch("robosystems.models.iam.UserAPIKey.get_by_user_id")
  def test_update_api_key_success(
    self, mock_get_by_user_id, client_with_api_keys: TestClient
  ):
    """Test successful API key update."""
    from unittest.mock import MagicMock
    from main import app
    from robosystems.database import get_db_session

    # Create a mock database session
    mock_db = MagicMock()
    mock_db.commit.return_value = None
    mock_db.refresh.return_value = None

    # Override the database dependency for this test
    def mock_get_db():
      yield mock_db

    app.dependency_overrides[get_db_session] = mock_get_db

    # Mock the API key lookup - returns all user API keys
    mock_api_key = client_with_api_keys.mock_user.api_keys[0]
    mock_get_by_user_id.return_value = client_with_api_keys.mock_user.api_keys

    update_data = {"name": "Updated API Key Name", "description": "Updated description"}

    # Update the mock's attributes so they return the right values
    mock_api_key.name = update_data["name"]
    mock_api_key.description = update_data["description"]

    response = client_with_api_keys.put(
      f"/v1/user/api-keys/{mock_api_key.id}", json=update_data
    )

    # Clean up the override
    del app.dependency_overrides[get_db_session]

    assert response.status_code == 200
    data = response.json()

    # Verify the response contains the updated data
    assert data["name"] == update_data["name"]
    assert data["description"] == update_data["description"]

  @patch("robosystems.models.iam.UserAPIKey.get_by_user_id")
  def test_update_api_key_not_found(
    self, mock_get_by_user_id, client_with_api_keys: TestClient
  ):
    """Test API key update for non-existent key."""
    # Mock API key not found - return empty list
    mock_get_by_user_id.return_value = []

    update_data = {"name": "Updated Name"}

    response = client_with_api_keys.put(
      "/v1/user/api-keys/nonexistent-id", json=update_data
    )

    assert response.status_code == 404
    data = response.json()
    # Handle structured error response
    assert "not found" in data["detail"]["detail"].lower()

  @patch("robosystems.models.iam.UserAPIKey.get_by_user_id")
  def test_revoke_api_key_success(
    self, mock_get_by_user_id, client_with_api_keys: TestClient
  ):
    """Test successful API key revocation."""
    # Mock the API key lookup - returns all user API keys
    mock_api_key = client_with_api_keys.mock_user.api_keys[1]  # Use second key
    mock_get_by_user_id.return_value = client_with_api_keys.mock_user.api_keys

    # Mock the database session and deactivate method
    # The session is now dependency-injected, so we don't need to patch it
    with patch.object(mock_api_key, "deactivate") as mock_deactivate:
      response = client_with_api_keys.delete(f"/v1/user/api-keys/{mock_api_key.id}")

      assert response.status_code == 200
      data = response.json()

      # SuccessResponse format
      assert data["success"] is True
      assert "revoked" in data["message"].lower()

      # Verify deactivate method was called
      mock_deactivate.assert_called_once()

  @patch("robosystems.models.iam.UserAPIKey.get_by_user_id")
  def test_revoke_api_key_not_found(
    self, mock_get_by_user_id, client_with_api_keys: TestClient
  ):
    """Test API key revocation for non-existent key."""
    # Mock API key not found - return empty list
    mock_get_by_user_id.return_value = []

    response = client_with_api_keys.delete("/v1/user/api-keys/nonexistent-id")

    assert response.status_code == 404


class TestUserPasswordUpdate:
  """Test user password update endpoint using pure mocked authentication."""

  @pytest.fixture
  def mock_user_with_password(self):
    """Create a mock user with password hash."""
    mock_user = Mock()
    mock_user.id = "test-user-password"
    mock_user.email = "passwordtest@example.com"
    mock_user.name = "Password Test User"

    # Create a real password hash for testing
    salt = bcrypt.gensalt()
    mock_user.password_hash = bcrypt.hashpw(
      "originalPassword123".encode("utf-8"), salt
    ).decode("utf-8")

    return mock_user

  @pytest.fixture
  def client_with_password_user(self, mock_user_with_password):
    """Create test client with mocked user for password tests."""
    from main import app
    from robosystems.middleware.auth.dependencies import get_current_user
    from robosystems.middleware.rate_limits import (
      auth_rate_limit_dependency,
      rate_limit_dependency,
      user_management_rate_limit_dependency,
      sync_operations_rate_limit_dependency,
      connection_management_rate_limit_dependency,
      analytics_rate_limit_dependency,
      backup_operations_rate_limit_dependency,
      sensitive_auth_rate_limit_dependency,
      tasks_management_rate_limit_dependency,
      general_api_rate_limit_dependency,
      subscription_aware_rate_limit_dependency,
    )

    # Mock the authentication dependency
    app.dependency_overrides[get_current_user] = lambda: mock_user_with_password

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

    client = TestClient(app)
    client.mock_user = mock_user_with_password

    yield client

    # Cleanup
    app.dependency_overrides = {}

  @patch("robosystems.models.iam.User.get_by_id")
  def test_update_password_success(
    self, mock_get_by_id, client_with_password_user: TestClient
  ):
    """Test successful password update."""
    from unittest.mock import MagicMock
    from main import app
    from robosystems.database import get_db_session

    # Create a mock database session
    mock_db = MagicMock()
    mock_db.commit.return_value = None
    mock_db.refresh.return_value = None
    mock_db.rollback.return_value = None

    # Override the database dependency for this test
    def mock_get_db():
      yield mock_db

    app.dependency_overrides[get_db_session] = mock_get_db

    # Mock the user lookup
    mock_get_by_id.return_value = client_with_password_user.mock_user

    password_data = {
      "current_password": "originalPassword123",
      "new_password": "NewSecure@Password456!",
      "confirm_password": "NewSecure@Password456!",
    }

    response = client_with_password_user.put("/v1/user/password", json=password_data)

    # Clean up the override
    del app.dependency_overrides[get_db_session]

    assert response.status_code == 200
    data = response.json()

    # SuccessResponse format
    assert data["success"] is True
    assert "updated" in data["message"].lower()

    # Verify session operations were called
    mock_db.commit.assert_called_once()

  @patch("robosystems.models.iam.User.get_by_id")
  def test_update_password_wrong_current(
    self, mock_get_by_id, client_with_password_user: TestClient
  ):
    """Test password update with wrong current password."""
    # Mock the user lookup
    mock_get_by_id.return_value = client_with_password_user.mock_user

    password_data = {
      "current_password": "wrongPassword123",
      "new_password": "NewSecure@Password456!",
      "confirm_password": "NewSecure@Password456!",
    }

    response = client_with_password_user.put("/v1/user/password", json=password_data)

    assert response.status_code == 400
    data = response.json()
    # Handle structured error response
    assert "incorrect" in data["detail"]["detail"].lower()

  def test_update_password_mismatch(self, client_with_password_user: TestClient):
    """Test password update with confirmation mismatch."""
    password_data = {
      "current_password": "originalPassword123",
      "new_password": "NewSecure@Password456!",
      "confirm_password": "DifferentPass@word789!",
    }

    response = client_with_password_user.put("/v1/user/password", json=password_data)

    assert response.status_code == 400
    data = response.json()
    # Handle structured error response
    assert "do not match" in data["detail"]["detail"].lower()

  def test_update_password_weak_password(self, client_with_password_user: TestClient):
    """Test password update with weak new password."""
    password_data = {
      "current_password": "originalPassword123",
      "new_password": "123",  # Too short
      "confirm_password": "123",
    }

    response = client_with_password_user.put("/v1/user/password", json=password_data)

    assert response.status_code == 422

  def test_update_password_missing_fields(self, client_with_password_user: TestClient):
    """Test password update with missing fields."""
    # Missing current_password
    response = client_with_password_user.put(
      "/v1/user/password",
      json={"new_password": "NewPass@word123!", "confirm_password": "NewPass@word123!"},
    )
    assert response.status_code == 422

    # Missing new_password
    response = client_with_password_user.put(
      "/v1/user/password",
      json={
        "current_password": "originalPassword123",
        "confirm_password": "newPassword123",
      },
    )
    assert response.status_code == 422

    # Missing confirm_password
    response = client_with_password_user.put(
      "/v1/user/password",
      json={
        "current_password": "originalPassword123",
        "new_password": "NewPass@word123!",
      },
    )
    assert response.status_code == 422


class TestUserEndpointsMetrics:
  """Test that metrics are properly recorded for user endpoints."""

  @pytest.fixture
  def mock_user_for_metrics(self):
    """Create a mock user for metrics testing."""
    mock_user = Mock()
    mock_user.id = "test-user-metrics"
    mock_user.email = "metrics@example.com"
    mock_user.name = "Metrics Test User"
    mock_user.accounts = []
    return mock_user

  @pytest.fixture
  def client_with_metrics_user(self, mock_user_for_metrics):
    """Create test client with mocked user for metrics tests."""
    from main import app
    from robosystems.middleware.auth.dependencies import get_current_user
    from robosystems.middleware.rate_limits import (
      auth_rate_limit_dependency,
      rate_limit_dependency,
      user_management_rate_limit_dependency,
      sync_operations_rate_limit_dependency,
      connection_management_rate_limit_dependency,
      analytics_rate_limit_dependency,
      backup_operations_rate_limit_dependency,
      sensitive_auth_rate_limit_dependency,
      tasks_management_rate_limit_dependency,
      general_api_rate_limit_dependency,
      subscription_aware_rate_limit_dependency,
    )

    # Mock the authentication dependency
    app.dependency_overrides[get_current_user] = lambda: mock_user_for_metrics

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

    client = TestClient(app)
    client.mock_user = mock_user_for_metrics

    yield client

    # Cleanup
    app.dependency_overrides = {}

  @patch("robosystems.middleware.otel.metrics.get_endpoint_metrics")
  def test_user_info_metrics_recorded(
    self, mock_get_metrics, client_with_metrics_user: TestClient
  ):
    """Test that metrics are recorded for user info endpoint."""
    mock_metrics_instance = MagicMock()
    mock_get_metrics.return_value = mock_metrics_instance

    response = client_with_metrics_user.get("/v1/user/")
    assert response.status_code == 200

    # Verify business event was recorded
    mock_metrics_instance.record_business_event.assert_called()
    call_args = mock_metrics_instance.record_business_event.call_args
    assert call_args[1]["event_type"] == "user_info_accessed"

  @patch("robosystems.middleware.otel.metrics.get_endpoint_metrics")
  @patch("robosystems.models.iam.UserAPIKey.create")
  def test_api_key_creation_metrics_recorded(
    self, mock_create, mock_get_metrics, client_with_metrics_user: TestClient
  ):
    """Test that metrics are recorded for API key creation."""
    mock_metrics_instance = MagicMock()
    mock_get_metrics.return_value = mock_metrics_instance

    # Mock API key creation
    from datetime import datetime, timezone

    mock_api_key = Mock()
    mock_api_key.id = "metrics-key-id"
    mock_api_key.name = "Metrics Test Key"
    mock_api_key.description = "Test key for metrics"
    mock_api_key.prefix = "rfs999"
    mock_api_key.is_active = True
    mock_api_key.created_at = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    mock_api_key.last_used_at = None
    mock_api_key.expires_at = None

    mock_create.return_value = (mock_api_key, "rfs999_full_api_key_here")

    create_data = {"name": "Metrics Test Key", "description": "Test key for metrics"}

    response = client_with_metrics_user.post("/v1/user/api-keys", json=create_data)
    assert response.status_code == 201

    # Verify business event was recorded
    mock_metrics_instance.record_business_event.assert_called()
    call_args = mock_metrics_instance.record_business_event.call_args
    assert call_args[1]["event_type"] == "api_key_created"
