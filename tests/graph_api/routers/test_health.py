"""Tests for health router endpoints."""

from unittest.mock import MagicMock, patch
import pytest
from fastapi import status
from fastapi.testclient import TestClient

from robosystems.graph_api.app import create_app


class TestHealthRouter:
  """Test cases for health check endpoints."""

  @pytest.fixture
  def client(self):
    """Create a test client."""
    app = create_app()

    # Override the cluster service dependency
    from robosystems.graph_api.core.ladybug import get_ladybug_service

    mock_service = MagicMock()
    app.dependency_overrides[get_ladybug_service] = lambda: mock_service

    return TestClient(app)

  @pytest.fixture
  def mock_cluster_service(self):
    """Create a mock cluster service."""
    service = MagicMock()
    service.get_uptime.return_value = 3600  # 1 hour
    service.db_manager.list_databases.return_value = ["db1", "db2", "db3"]
    return service

  def test_health_check_success(self, client, mock_cluster_service):
    """Test successful health check."""
    # Configure the mock service that was already injected
    from robosystems.graph_api.core.ladybug import get_ladybug_service

    mock_service = client.app.dependency_overrides[get_ladybug_service]()
    mock_service.get_uptime.return_value = 3600
    mock_service.db_manager.list_databases.return_value = ["db1", "db2", "db3"]

    response = client.get("/health")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["status"] == "healthy"
    assert data["uptime_seconds"] == 3600
    assert data["database_count"] == 3

  def test_health_check_with_memory_info(self, client, mock_cluster_service):
    """Test health check with memory information."""
    # Configure the mock service that was already injected
    from robosystems.graph_api.core.ladybug import get_ladybug_service

    mock_service = client.app.dependency_overrides[get_ladybug_service]()
    mock_service.get_uptime.return_value = 3600
    mock_service.db_manager.list_databases.return_value = ["db1", "db2", "db3"]

    # Mock psutil import and usage
    mock_psutil = MagicMock()
    mock_process = MagicMock()
    mock_memory = MagicMock()
    mock_memory.rss = 100 * 1024 * 1024  # 100 MB
    mock_memory.vms = 200 * 1024 * 1024  # 200 MB
    mock_process.memory_info.return_value = mock_memory
    mock_process.memory_percent.return_value = 5.5
    mock_psutil.Process.return_value = mock_process

    with patch.dict("sys.modules", {"psutil": mock_psutil}):
      response = client.get("/health")

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert data["status"] == "healthy"
      assert data["memory_rss_mb"] == 100.0
      assert data["memory_vms_mb"] == 200.0
      assert data["memory_percent"] == 5.5

  def test_health_check_without_psutil(self, client, mock_cluster_service):
    """Test health check when psutil is not available."""
    # Configure the mock service that was already injected
    from robosystems.graph_api.core.ladybug import get_ladybug_service

    mock_service = client.app.dependency_overrides[get_ladybug_service]()
    mock_service.get_uptime.return_value = 3600
    mock_service.db_manager.list_databases.return_value = ["db1", "db2", "db3"]

    # Mock psutil import failure
    with patch.dict("sys.modules", {"psutil": None}):
      response = client.get("/health")

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert data["status"] == "healthy"
      # Memory info should not be present
      assert "memory_rss_mb" not in data
      assert "memory_vms_mb" not in data
      assert "memory_percent" not in data

  def test_health_check_service_error(self, client):
    """Test health check when cluster service has an error."""
    # Configure the mock service that was already injected
    from robosystems.graph_api.core.ladybug import get_ladybug_service

    mock_service = client.app.dependency_overrides[get_ladybug_service]()
    mock_service.get_uptime.side_effect = Exception("Service unavailable")

    response = client.get("/health")

    assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
    data = response.json()
    assert data["status"] == "unhealthy"
    # Security: Generic error message to avoid information disclosure
    assert data["error"] == "Service temporarily unavailable"

  def test_health_check_database_error(self, client):
    """Test health check when database manager has an error."""
    # Configure the mock service that was already injected
    from robosystems.graph_api.core.ladybug import get_ladybug_service

    mock_service = client.app.dependency_overrides[get_ladybug_service]()
    mock_service.get_uptime.return_value = 1000
    mock_service.db_manager.list_databases.side_effect = Exception("Database error")

    response = client.get("/health")

    assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
    data = response.json()
    assert data["status"] == "unhealthy"
    # Security: Generic error message to avoid information disclosure
    assert data["error"] == "Service temporarily unavailable"

  def test_health_check_zero_databases(self, client, mock_cluster_service):
    """Test health check with zero databases."""
    # Configure the mock service that was already injected
    from robosystems.graph_api.core.ladybug import get_ladybug_service

    mock_service = client.app.dependency_overrides[get_ladybug_service]()
    mock_service.get_uptime.return_value = 3600
    mock_service.db_manager.list_databases.return_value = []

    response = client.get("/health")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["status"] == "healthy"
    assert data["database_count"] == 0

  def test_health_check_response_format(self, client, mock_cluster_service):
    """Test that health check response has expected format."""
    # Configure the mock service that was already injected
    from robosystems.graph_api.core.ladybug import get_ladybug_service

    mock_service = client.app.dependency_overrides[get_ladybug_service]()
    mock_service.get_uptime.return_value = 3600
    mock_service.db_manager.list_databases.return_value = ["db1", "db2", "db3"]

    response = client.get("/health")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()

    # Required fields
    assert "status" in data
    assert "uptime_seconds" in data
    assert "database_count" in data

    # Types
    assert isinstance(data["status"], str)
    assert isinstance(data["uptime_seconds"], (int, float))
    assert isinstance(data["database_count"], int)
