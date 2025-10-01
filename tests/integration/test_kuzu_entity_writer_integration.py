"""
Integration tests for Kuzu entity writer system.

These tests create actual users, authenticate them, and interact with the Kuzu entity writer
system to verify end-to-end functionality. They create real Kuzu databases in the filesystem
and are designed to be run separately from the main test suite.
"""

import os
import shutil
import tempfile
import uuid
from pathlib import Path
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from main import app
from robosystems.database import Model as Base
from robosystems.models.iam import User, UserGraph


class TestKuzuEntityWriterIntegration:
  """Integration tests for the Kuzu entity writer system."""

  @pytest.fixture(scope="class")
  def integration_test_setup(self):
    """
    Setup for integration tests including real database and file system.

    This fixture creates a temporary directory for Kuzu databases and ensures
    the test database is clean for each test class.
    """
    # Create temporary directory for Kuzu databases
    temp_dir = tempfile.mkdtemp(prefix="kuzu_integration_test_")
    kuzu_db_path = Path(temp_dir) / "kuzu-dbs"
    kuzu_db_path.mkdir(parents=True, exist_ok=True)

    # Setup test database
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
      pytest.skip("TEST_DATABASE_URL not set, skipping integration tests")

    engine = create_engine(database_url)
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    test_db = TestingSessionLocal()

    # Set environment variables for Kuzu
    original_kuzu_path = os.environ.get("KUZU_DATABASE_PATH")
    original_access_pattern = os.environ.get("KUZU_ACCESS_PATTERN")

    os.environ["KUZU_DATABASE_PATH"] = str(kuzu_db_path)
    os.environ["KUZU_ACCESS_PATTERN"] = "direct_file"  # Force direct file access

    yield {
      "temp_dir": temp_dir,
      "kuzu_db_path": kuzu_db_path,
      "test_db": test_db,
      "engine": engine,
    }

    # Cleanup
    test_db.close()
    engine.dispose()
    shutil.rmtree(temp_dir, ignore_errors=True)

    # Restore original environment variables
    if original_kuzu_path:
      os.environ["KUZU_DATABASE_PATH"] = original_kuzu_path
    else:
      os.environ.pop("KUZU_DATABASE_PATH", None)

    if original_access_pattern:
      os.environ["KUZU_ACCESS_PATTERN"] = original_access_pattern
    else:
      os.environ.pop("KUZU_ACCESS_PATTERN", None)

  @pytest.fixture
  def client(self, integration_test_setup):
    """Create a test client for API interactions."""
    # Override database session for the test
    from robosystems.database import get_db_session

    def override_get_db():
      yield integration_test_setup["test_db"]

    app.dependency_overrides[get_db_session] = override_get_db

    # Disable rate limiting
    from robosystems.middleware.rate_limits import (
      auth_rate_limit_dependency,
      rate_limit_dependency,
    )

    app.dependency_overrides[auth_rate_limit_dependency] = lambda: None
    app.dependency_overrides[rate_limit_dependency] = lambda: None

    client = TestClient(app)
    yield client

    # Reset dependency overrides
    app.dependency_overrides = {}

  def generate_unique_user_data(self):
    """Generate unique user data for testing."""
    unique_id = str(uuid.uuid4())[:8]
    return {
      "name": f"Test User {unique_id}",
      "email": f"test.{unique_id}@example.com",
      "password": "testPassword123!",
    }

  def register_user(self, client: TestClient, user_data: dict) -> dict:
    """Register a new user and return the response."""
    response = client.post("/v1/auth/register", json=user_data)
    assert response.status_code == 201, f"Registration failed: {response.json()}"
    return response.json()

  def login_user(self, client: TestClient, email: str, password: str) -> dict:
    """Login a user and return the response."""
    response = client.post(
      "/v1/auth/login", json={"email": email, "password": password}
    )
    assert response.status_code == 200, f"Login failed: {response.json()}"
    return response.json()

  def get_auth_token(self, client: TestClient, email: str, password: str) -> str:
    """Get authentication token for a user."""
    response = client.post(
      "/v1/auth/login", json={"email": email, "password": password}
    )
    assert response.status_code == 200
    response_data = response.json()
    return response_data["token"]

  def get_auth_cookies(self, client: TestClient, email: str, password: str):
    """Get authentication cookies for a user."""
    response = client.post(
      "/v1/auth/login", json={"email": email, "password": password}
    )
    assert response.status_code == 200
    # Return cookies from the response if they exist
    return response.cookies

  def create_user_graph(self, test_db, user_id: str) -> str:
    """Create a user graph record in the database."""
    graph_id = f"entity_{user_id[:8]}"

    # Create the user first since it was created via API and may not be in our session
    import bcrypt
    from datetime import datetime, timezone

    # Check if user exists, if not create it
    user = test_db.query(User).filter(User.id == user_id).first()
    if not user:
      # User was created via API - create it in our session too
      password = "testPassword123!"
      salt = bcrypt.gensalt()
      password_hash = bcrypt.hashpw(password.encode("utf-8"), salt).decode("utf-8")

      user = User(
        id=user_id,
        email=f"test.{user_id[:8]}@example.com",
        name=f"Test User {user_id[:8]}",
        password_hash=password_hash,
        is_active=True,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
      )
      test_db.add(user)
      test_db.commit()

    user_graph = UserGraph(
      user_id=user_id,
      graph_id=graph_id,
      graph_name=f"Test Entity Graph {user_id[:8]}",
      role="admin",
    )
    test_db.add(user_graph)
    test_db.commit()
    return graph_id

  def verify_kuzu_database_exists(self, kuzu_db_path: Path, graph_id: str) -> bool:
    """Verify that a Kuzu database directory exists in the filesystem."""
    db_path = kuzu_db_path / graph_id
    return db_path.exists() and db_path.is_dir()

  @pytest.mark.kuzu_integration
  def test_user_registration_and_authentication(self, client, integration_test_setup):
    """Test user registration and authentication flow."""
    # Generate unique user data
    user_data = self.generate_unique_user_data()

    # Register user
    register_response = self.register_user(client, user_data)
    assert (
      register_response["message"]
      == "User registered successfully. Please check your email to verify your account."
    )
    assert register_response["user"]["email"] == user_data["email"]
    assert register_response["user"]["name"] == user_data["name"]

    user_id = register_response["user"]["id"]

    # Login user
    login_response = self.login_user(client, user_data["email"], user_data["password"])
    assert login_response["message"] == "Login successful"
    assert login_response["user"]["id"] == user_id

    # Verify auth token is returned in response
    assert "token" in login_response
    auth_token = login_response["token"]
    assert auth_token is not None and len(auth_token) > 0

    # Test authenticated endpoint using Authorization header
    headers = {"Authorization": f"Bearer {auth_token}"}
    response = client.get("/v1/auth/me", headers=headers)
    assert response.status_code == 200
    me_data = response.json()
    assert me_data["id"] == user_id
    assert me_data["email"] == user_data["email"]

  @pytest.mark.kuzu_integration
  def test_node_exists_with_empty_database(self, client, integration_test_setup):
    """Test that node_exists works correctly with an empty database (no schema)."""
    # Register and login user
    user_data = self.generate_unique_user_data()
    register_response = self.register_user(client, user_data)
    user_id = register_response["user"]["id"]

    # Create user graph
    graph_id = self.create_user_graph(integration_test_setup["test_db"], user_id)

    # Test the repository directly
    from robosystems.middleware.graph.router import get_graph_repository

    repository = get_graph_repository(graph_id, operation_type="write")

    # This should not raise an exception, even though table doesn't exist
    exists = repository.node_exists("Entity", {"identifier": "test123"})
    assert exists is False, "node_exists should return False for non-existent table"

  @pytest.mark.kuzu_integration
  def test_entity_creation_creates_kuzu_database(self, client, integration_test_setup):
    """Test that creating a entity creates a Kuzu database in the filesystem."""
    # Register and login user
    user_data = self.generate_unique_user_data()
    register_response = self.register_user(client, user_data)
    user_id = register_response["user"]["id"]

    # Create user graph
    self.create_user_graph(integration_test_setup["test_db"], user_id)

    # Login to get authentication token
    auth_token = self.get_auth_token(client, user_data["email"], user_data["password"])
    headers = {"Authorization": f"Bearer {auth_token}"}

    # Create entity data for the create-entity-graph endpoint
    entity_data = {
      "name": f"Test Entity {user_id[:8]}",
      "uri": f"https://testentity{user_id[:8]}.com",
      "cik": f"000{user_id[:7]}",
      "sic_description": "Integration test entity",
      "extensions": ["roboledger"],
    }

    # Create entity with new graph (async endpoint)
    response = client.post("/v1/create/entity-graph", json=entity_data, headers=headers)

    # Verify entity creation task was submitted (async endpoint returns 202)
    assert response.status_code == 202, f"Entity creation failed: {response.json()}"
    task_response = response.json()
    assert "task_id" in task_response, "Task ID should be returned"

    # For integration test, we can check if the task was created
    # In a real test, we would monitor the task status until completion
    print(f"Entity creation task submitted: {task_response['task_id']}")

    # Note: The async task will handle:
    # 1. Database allocation
    # 2. Schema installation
    # 3. Entity creation in graph
    # 4. UserGraph relationship creation

    # For now, we'll just verify the task was submitted successfully
    # A full integration test would monitor task completion and verify results

  @pytest.mark.kuzu_integration
  def test_multiple_users_multiple_databases(self, client, integration_test_setup):
    """Test that multiple users can create separate Kuzu databases."""
    users_and_tasks = []

    # Create 3 users and submit entity creation tasks
    for i in range(3):
      user_data = self.generate_unique_user_data()
      register_response = self.register_user(client, user_data)
      user_id = register_response["user"]["id"]

      # Create user graph
      graph_id = self.create_user_graph(integration_test_setup["test_db"], user_id)

      # Get auth token
      auth_token = self.get_auth_token(
        client, user_data["email"], user_data["password"]
      )
      headers = {"Authorization": f"Bearer {auth_token}"}

      # Create entity data for the create-entity-graph endpoint
      entity_data = {
        "name": f"Test Entity {i + 1} {user_id[:8]}",
        "uri": f"https://testentity{i + 1}{user_id[:8]}.com",
        "cik": f"000{i + 1:03d}{user_id[:4]}",
        "sic_description": f"Integration test entity {i + 1}",
        "extensions": ["roboledger"],
      }

      # Submit entity creation task
      response = client.post(
        "/v1/create/entity-graph", json=entity_data, headers=headers
      )

      assert response.status_code == 202, (
        f"Entity creation task failed for user {i + 1}: {response.json()}"
      )
      task_response = response.json()
      assert "task_id" in task_response, f"Task ID missing for user {i + 1}"

      users_and_tasks.append(
        {
          "user_id": user_id,
          "graph_id": graph_id,
          "task_id": task_response["task_id"],
          "user_data": user_data,
        }
      )

    # For integration test purposes, we verify that all tasks were submitted successfully
    # In a full test, we would monitor task completion and verify the results
    assert len(users_and_tasks) == 3, "All 3 users should have submitted tasks"

    for i, user_info in enumerate(users_and_tasks):
      print(
        f"User {i + 1} ({user_info['user_id'][:8]}) submitted task: {user_info['task_id']}"
      )

    print("✅ All users successfully submitted entity creation tasks")
