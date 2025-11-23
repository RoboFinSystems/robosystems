"""Tests for database management router endpoints."""

from unittest.mock import MagicMock, patch
import pytest
from fastapi import status
from fastapi.testclient import TestClient

from robosystems.graph_api.app import create_app
from robosystems.graph_api.models.database import (
  DatabaseCreateResponse,
  DatabaseListResponse,
  DatabaseInfo,
)
from robosystems.middleware.graph.clusters import NodeType


class TestDatabaseManagementRouter:
  """Test cases for database management endpoints."""

  @pytest.fixture
  def client(self):
    """Create a test client."""
    app = create_app()

    # Override the cluster service dependency
    from robosystems.graph_api.core.cluster_manager import get_cluster_service

    mock_service = MagicMock()
    app.dependency_overrides[get_cluster_service] = lambda: mock_service

    return TestClient(app)

  @pytest.fixture
  def mock_database_info(self):
    """Create mock database info."""
    return DatabaseInfo(
      graph_id="kg1a2b3c4d5",
      database_path="/data/lbug-dbs/kg1a2b3c4d5",
      created_at="2024-01-15T10:30:00Z",
      size_bytes=268697600,  # 256.5 MB in bytes
      read_only=False,
      is_healthy=True,
      last_accessed="2024-01-15T12:00:00Z",
    )

  @pytest.fixture
  def mock_database_list_response(self, mock_database_info):
    """Create mock database list response."""
    return DatabaseListResponse(
      databases=[mock_database_info],
      total_databases=1,
      total_size_bytes=268697600,  # 256.5 MB in bytes
      node_capacity={
        "max_databases": 100,
        "current_databases": 1,
        "available_slots": 99,
      },
    )

  def test_list_databases_success(self, client, mock_database_list_response):
    """Test successful database listing."""
    # Configure the mock service that was already injected
    from robosystems.graph_api.core.cluster_manager import get_cluster_service

    mock_service = client.app.dependency_overrides[get_cluster_service]()
    mock_service.db_manager.get_all_databases_info.return_value = (
      mock_database_list_response
    )

    response = client.get("/databases")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["total_databases"] == 1
    assert data["total_size_bytes"] == 268697600
    assert len(data["databases"]) == 1
    assert data["databases"][0]["graph_id"] == "kg1a2b3c4d5"

  def test_list_databases_empty(self, client):
    """Test listing when no databases exist."""
    empty_response = DatabaseListResponse(
      databases=[],
      total_databases=0,
      total_size_bytes=0,
      node_capacity={
        "max_databases": 100,
        "current_databases": 0,
        "available_slots": 100,
      },
    )

    # Configure the mock service that was already injected
    from robosystems.graph_api.core.cluster_manager import get_cluster_service

    mock_service = client.app.dependency_overrides[get_cluster_service]()
    mock_service.db_manager.get_all_databases_info.return_value = empty_response
    response = client.get("/databases")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["total_databases"] == 0
    assert data["databases"] == []

  def test_create_database_entity_schema(self, client):
    """Test creating database with entity schema."""
    request_data = {
      "graph_id": "kg9z8y7x6w5",
      "schema_type": "entity",
    }

    expected_response = DatabaseCreateResponse(
      status="created",
      graph_id="kg9z8y7x6w5",
      database_path="/data/lbug-dbs/kg9z8y7x6w5",
      schema_applied=True,
      execution_time_ms=150.5,
    )

    # Configure the mock service that was already injected
    from robosystems.graph_api.core.cluster_manager import get_cluster_service

    mock_service = client.app.dependency_overrides[get_cluster_service]()
    mock_service.read_only = False
    mock_service.node_type = NodeType.WRITER
    mock_service.db_manager.create_database.return_value = expected_response
    response = client.post("/databases", json=request_data)

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["graph_id"] == "kg9z8y7x6w5"
    assert data["status"] == "created"
    assert data["schema_applied"] is True
    assert data["database_path"] == "/data/lbug-dbs/kg9z8y7x6w5"

  def test_create_database_shared_schema(self, client):
    """Test creating database with shared schema."""
    request_data = {
      "graph_id": "sec",
      "schema_type": "shared",
      "repository_name": "sec",
    }

    expected_response = DatabaseCreateResponse(
      status="created",
      graph_id="sec",
      database_path="/data/lbug-dbs/sec",
      schema_applied=True,
      execution_time_ms=200.0,
    )

    # Configure the mock service that was already injected
    from robosystems.graph_api.core.cluster_manager import get_cluster_service

    mock_service = client.app.dependency_overrides[get_cluster_service]()
    mock_service.read_only = False
    mock_service.node_type = NodeType.SHARED_MASTER
    mock_service.db_manager.create_database.return_value = expected_response
    response = client.post("/databases", json=request_data)

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["graph_id"] == "sec"
    assert data["status"] == "created"
    assert data["schema_applied"] is True

  def test_create_database_shared_without_repository_name(self, client):
    """Test creating shared database without repository name fails."""
    # Configure the mock service that was already injected
    from robosystems.graph_api.core.cluster_manager import get_cluster_service

    mock_service = client.app.dependency_overrides[get_cluster_service]()
    mock_service.read_only = False
    response = client.post(
      "/databases",
      json={"graph_id": "invalid_shared", "schema_type": "shared"},
    )

    assert response.status_code == status.HTTP_400_BAD_REQUEST
    data = response.json()
    assert "repository_name" in data["detail"]

  def test_create_database_on_readonly_node(self, client):
    """Test that database creation fails on read-only nodes."""
    # Configure the mock service that was already injected
    from robosystems.graph_api.core.cluster_manager import get_cluster_service

    mock_service = client.app.dependency_overrides[get_cluster_service]()
    mock_service.read_only = True
    response = client.post(
      "/databases",
      json={"graph_id": "test_db", "schema_type": "entity"},
    )

    assert response.status_code == status.HTTP_403_FORBIDDEN
    data = response.json()
    assert "read-only" in data["detail"]

  def test_get_database_info_success(self, client, mock_database_info):
    """Test retrieving database information."""
    # Configure the mock service that was already injected
    from robosystems.graph_api.core.cluster_manager import get_cluster_service

    mock_service = client.app.dependency_overrides[get_cluster_service]()
    mock_service.db_manager.get_database_info.return_value = mock_database_info
    response = client.get("/databases/kg1a2b3c4d5")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["graph_id"] == "kg1a2b3c4d5"
    assert data["database_path"] == "/data/lbug-dbs/kg1a2b3c4d5"
    assert data["size_bytes"] == 268697600
    assert data["read_only"] is False
    assert data["is_healthy"] is True

  def test_get_database_info_not_found(self, client):
    """Test retrieving info for non-existent database."""
    from fastapi import HTTPException

    # Configure the mock service that was already injected
    from robosystems.graph_api.core.cluster_manager import get_cluster_service

    mock_service = client.app.dependency_overrides[get_cluster_service]()
    mock_service.db_manager.get_database_info.side_effect = HTTPException(
      status_code=404, detail="Database not found"
    )
    response = client.get("/databases/nonexistent")

    assert response.status_code == status.HTTP_404_NOT_FOUND
    data = response.json()
    assert "not found" in data["detail"].lower()

  def test_delete_database_success(self, client):
    """Test successful database deletion."""
    # Configure the mock service that was already injected
    from robosystems.graph_api.core.cluster_manager import get_cluster_service

    mock_service = client.app.dependency_overrides[get_cluster_service]()
    mock_service.read_only = False
    mock_service.node_type = NodeType.WRITER
    mock_service.db_manager.delete_database.return_value = None
    response = client.delete("/databases/kg1a2b3c4d5")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["status"] == "success"
    assert "kg1a2b3c4d5" in data["message"]
    mock_service.db_manager.delete_database.assert_called_once_with("kg1a2b3c4d5")

  def test_delete_database_on_readonly_node(self, client):
    """Test that database deletion fails on read-only nodes."""
    # Configure the mock service that was already injected
    from robosystems.graph_api.core.cluster_manager import get_cluster_service

    mock_service = client.app.dependency_overrides[get_cluster_service]()
    mock_service.read_only = True
    response = client.delete("/databases/kg1a2b3c4d5")

    assert response.status_code == status.HTTP_403_FORBIDDEN
    data = response.json()
    assert "read-only" in data["detail"]

  def test_delete_shared_database_warning(self, client):
    """Test that deleting shared database logs warning."""
    # Configure the mock service that was already injected
    from robosystems.graph_api.core.cluster_manager import get_cluster_service

    mock_service = client.app.dependency_overrides[get_cluster_service]()
    mock_service.read_only = False
    mock_service.node_type = NodeType.SHARED_MASTER
    mock_service.db_manager.delete_database.return_value = None
    with patch(
      "robosystems.graph_api.routers.databases.management.logger"
    ) as mock_logger:
      response = client.delete("/databases/sec")

      assert response.status_code == status.HTTP_200_OK
      mock_logger.warning.assert_called_once()
      assert "shared database" in mock_logger.warning.call_args[0][0]

  def test_create_database_custom_schema(self, client):
    """Test creating database with custom schema."""
    expected_response = DatabaseCreateResponse(
      status="created",
      graph_id="kg5t6y7u8i9",
      database_path="/data/lbug-dbs/kg5t6y7u8i9",
      schema_applied=True,
      execution_time_ms=180.0,
    )

    # Configure the mock service that was already injected
    from robosystems.graph_api.core.cluster_manager import get_cluster_service

    mock_service = client.app.dependency_overrides[get_cluster_service]()
    mock_service.read_only = False
    mock_service.db_manager.create_database.return_value = expected_response
    response = client.post(
      "/databases",
      json={"graph_id": "kg5t6y7u8i9", "schema_type": "custom"},
    )

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["graph_id"] == "kg5t6y7u8i9"
    assert data["status"] == "created"

  def test_list_databases_multiple(self, client):
    """Test listing multiple databases."""
    databases = [
      DatabaseInfo(
        graph_id=f"kg{i}a2b3c4d5",
        database_path=f"/data/lbug-dbs/kg{i}a2b3c4d5",
        created_at="2024-01-15T10:30:00Z",
        size_bytes=100000000 * i,
        read_only=False,
        is_healthy=True,
        last_accessed="2024-01-15T12:00:00Z",
      )
      for i in range(1, 6)
    ]

    list_response = DatabaseListResponse(
      databases=databases,
      total_databases=5,
      total_size_bytes=sum(db.size_bytes for db in databases),
      node_capacity={
        "max_databases": 100,
        "current_databases": 5,
        "available_slots": 95,
      },
    )

    # Configure the mock service that was already injected
    from robosystems.graph_api.core.cluster_manager import get_cluster_service

    mock_service = client.app.dependency_overrides[get_cluster_service]()
    mock_service.db_manager.get_all_databases_info.return_value = list_response
    response = client.get("/databases")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["total_databases"] == 5
    assert len(data["databases"]) == 5
    assert data["total_size_bytes"] == 1500000000  # 100M+200M+300M+400M+500M

  def test_get_database_info_with_unhealthy_status(self, client):
    """Test retrieving database with unhealthy status."""
    unhealthy_db_info = DatabaseInfo(
      graph_id="unhealthy_db",
      database_path="/data/lbug-dbs/unhealthy_db",
      created_at="2024-01-01T00:00:00Z",
      size_bytes=1024000000,
      read_only=False,
      is_healthy=False,  # Unhealthy database
      last_accessed="2024-01-15T15:30:00Z",
    )

    # Configure the mock service that was already injected
    from robosystems.graph_api.core.cluster_manager import get_cluster_service

    mock_service = client.app.dependency_overrides[get_cluster_service]()
    mock_service.db_manager.get_database_info.return_value = unhealthy_db_info
    response = client.get("/databases/unhealthy_db")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["graph_id"] == "unhealthy_db"
    assert data["is_healthy"] is False
    assert data["size_bytes"] == 1024000000
