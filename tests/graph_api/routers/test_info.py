"""Tests for info router endpoints."""

from unittest.mock import MagicMock

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from robosystems.graph_api.app import create_app
from robosystems.graph_api.models.cluster import ClusterInfoResponse


class TestInfoRouter:
  """Test cases for cluster info endpoints."""

  @pytest.fixture
  def client(self):
    """Create a test client."""
    app = create_app()

    # Override the service dependency
    from robosystems.graph_api.routers.info import _get_service_for_info

    mock_service = MagicMock()
    app.dependency_overrides[_get_service_for_info] = lambda: mock_service

    return TestClient(app)

  @pytest.fixture
  def mock_cluster_info(self):
    """Create mock cluster info response."""
    return ClusterInfoResponse(
      node_id="test-node-01",
      node_type="writer",
      cluster_version="1.0.0",
      uptime_seconds=3600,
      max_databases=100,
      databases=["db1", "db2", "db3", "db4", "db5"],
      read_only=False,
      base_path="/data/lbug-dbs",
      configuration=None,
    )

  def test_get_cluster_info_success(self, client, mock_cluster_info):
    """Test successful cluster info retrieval."""
    # Configure the mock service that was already injected
    from robosystems.graph_api.routers.info import _get_service_for_info

    mock_service = client.app.dependency_overrides[_get_service_for_info]()
    mock_service.get_cluster_info.return_value = mock_cluster_info

    response = client.get("/info")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["node_id"] == "test-node-01"
    assert data["node_type"] == "writer"
    assert data["cluster_version"] == "1.0.0"
    assert data["uptime_seconds"] == 3600
    assert data["max_databases"] == 100
    assert len(data["databases"]) == 5
    assert data["read_only"] is False

  def test_get_cluster_info_shared_repository(self, client):
    """Test cluster info for shared repository node."""
    mock_info = ClusterInfoResponse(
      node_id="shared-master-01",
      node_type="shared_master",
      cluster_version="1.0.0",
      uptime_seconds=7200,
      max_databases=50,
      databases=["sec", "industry", "economic"],
      read_only=False,
      base_path="/data/shared",
      configuration=None,
    )

    # Configure the mock service that was already injected
    from robosystems.graph_api.routers.info import _get_service_for_info

    mock_service = client.app.dependency_overrides[_get_service_for_info]()
    mock_service.get_cluster_info.return_value = mock_info

    response = client.get("/info")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["node_type"] == "shared_master"
    assert "sec" in data["databases"]

  def test_get_cluster_info_read_only_node(self, client):
    """Test cluster info for read-only node."""
    mock_info = ClusterInfoResponse(
      node_id="reader-01",
      node_type="shared_replica",
      cluster_version="1.0.0",
      uptime_seconds=1800,
      max_databases=50,
      databases=["sec", "industry", "economic"],
      read_only=True,
      base_path="/data/readonly",
      configuration=None,
    )

    # Configure the mock service that was already injected
    from robosystems.graph_api.routers.info import _get_service_for_info

    mock_service = client.app.dependency_overrides[_get_service_for_info]()
    mock_service.get_cluster_info.return_value = mock_info

    response = client.get("/info")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["read_only"] is True
    assert data["node_type"] == "shared_replica"

  def test_get_cluster_info_no_databases(self, client):
    """Test cluster info with no databases."""
    mock_info = ClusterInfoResponse(
      node_id="empty-node",
      node_type="writer",
      cluster_version="1.0.0",
      uptime_seconds=100,
      max_databases=100,
      databases=[],
      read_only=False,
      base_path="/data/lbug-dbs",
      configuration=None,
    )

    # Configure the mock service that was already injected
    from robosystems.graph_api.routers.info import _get_service_for_info

    mock_service = client.app.dependency_overrides[_get_service_for_info]()
    mock_service.get_cluster_info.return_value = mock_info

    response = client.get("/info")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data["databases"]) == 0
    assert data["databases"] == []

  def test_get_cluster_info_at_capacity(self, client):
    """Test cluster info when at maximum capacity."""
    databases = [f"db{i}" for i in range(100)]
    mock_info = ClusterInfoResponse(
      node_id="full-node",
      node_type="writer",
      cluster_version="1.0.0",
      uptime_seconds=10000,
      max_databases=100,
      databases=databases,
      read_only=False,
      base_path="/data/lbug-dbs",
      configuration=None,
    )

    # Configure the mock service that was already injected
    from robosystems.graph_api.routers.info import _get_service_for_info

    mock_service = client.app.dependency_overrides[_get_service_for_info]()
    mock_service.get_cluster_info.return_value = mock_info

    response = client.get("/info")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["max_databases"] == 100
    assert len(data["databases"]) == 100

  def test_get_cluster_info_response_model_validation(self, client):
    """Test that response model validates correctly."""
    mock_info = ClusterInfoResponse(
      node_id="validation-test",
      node_type="writer",
      cluster_version="2.0.0-beta",
      uptime_seconds=0,  # Just started
      max_databases=200,
      databases=["test_db"],
      read_only=False,
      base_path="/var/lib/lbug",
      configuration=None,
    )

    # Configure the mock service that was already injected
    from robosystems.graph_api.routers.info import _get_service_for_info

    mock_service = client.app.dependency_overrides[_get_service_for_info]()
    mock_service.get_cluster_info.return_value = mock_info

    response = client.get("/info")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()

    # Check all required fields are present
    required_fields = [
      "node_id",
      "node_type",
      "cluster_version",
      "uptime_seconds",
      "max_databases",
      "databases",
      "read_only",
      "base_path",
    ]
    for field in required_fields:
      assert field in data
