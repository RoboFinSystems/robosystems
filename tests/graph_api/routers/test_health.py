"""Tests for health router endpoints."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from robosystems.graph_api.app import create_app
from robosystems.graph_api.core.ladybug import get_ladybug_service
from robosystems.graph_api.routers.health import MEMORY_LATCH_SECONDS


class TestHealthRouter:
  """Test cases for health check endpoints."""

  @pytest.fixture
  def client(self):
    """Create a test client."""
    app = create_app()

    # Override the service dependency
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


class TestReplicaMemoryLatch:
  """A replica that has refused every query for lack of memory headroom
  fails its health check, so the load balancer replaces it."""

  @pytest.fixture
  def client(self):
    app = create_app()
    service = MagicMock()
    service.get_uptime.return_value = 3600
    service.db_manager.list_databases.return_value = ["sec"]
    app.dependency_overrides[get_ladybug_service] = lambda: service
    return TestClient(app)

  @pytest.fixture
  def starved_for(self, monkeypatch):
    def _set(seconds: float, role: str | None = "replica"):
      if role is None:
        monkeypatch.delenv("LBUG_ROLE", raising=False)
      else:
        monkeypatch.setenv("LBUG_ROLE", role)
      monkeypatch.setattr("robosystems.graph_api.routers.health._replica_ready", True)
      controller = MagicMock()
      controller.memory_starved_seconds.return_value = seconds
      monkeypatch.setattr(
        "robosystems.graph_api.routers.health.get_admission_controller",
        lambda: controller,
      )

    return _set

  def test_a_latched_replica_is_unhealthy(self, client, starved_for):
    starved_for(MEMORY_LATCH_SECONDS)

    response = client.get("/health")

    assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
    assert response.json()["status"] == "memory_latched"

  def test_a_short_spike_does_not_cost_a_replica(self, client, starved_for):
    """A replacement takes many minutes to load; a spike is not worth one."""
    starved_for(MEMORY_LATCH_SECONDS - 1)

    response = client.get("/health")

    assert response.status_code == status.HTTP_200_OK
    assert response.json()["status"] == "healthy"

  @pytest.mark.parametrize("role", ["writer", None])
  def test_only_a_replica_is_judged(self, client, starved_for, role):
    """A writer holds tenant data on its volume; failing its health check
    would have the group replace it."""
    starved_for(MEMORY_LATCH_SECONDS * 10, role=role)

    response = client.get("/health")

    assert response.status_code == status.HTTP_200_OK

  def test_a_replica_that_cannot_read_its_memory_is_not_replaced(
    self, client, monkeypatch
  ):
    """Replacing it would not help, and a read that fails across the fleet
    would cycle every replica at once."""
    from robosystems.graph_api.core.admission_control import (
      LadybugAdmissionController,
    )

    clock = {"now": 1_000.0}
    monkeypatch.setattr(
      "robosystems.graph_api.core.admission_control.time.time", lambda: clock["now"]
    )

    def _boom():
      raise RuntimeError("psutil unavailable")

    monkeypatch.setattr(
      "robosystems.graph_api.core.admission_control.psutil.virtual_memory", _boom
    )
    controller = LadybugAdmissionController(min_available_mb=1024.0, check_interval=0.0)
    monkeypatch.setenv("LBUG_ROLE", "replica")
    monkeypatch.setattr("robosystems.graph_api.routers.health._replica_ready", True)
    monkeypatch.setattr(
      "robosystems.graph_api.routers.health.get_admission_controller",
      lambda: controller,
    )

    assert client.get("/health").status_code == status.HTTP_200_OK
    clock["now"] += MEMORY_LATCH_SECONDS * 2
    assert client.get("/health").status_code == status.HTTP_200_OK
