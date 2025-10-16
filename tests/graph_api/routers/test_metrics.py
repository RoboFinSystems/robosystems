"""Tests for metrics router endpoints."""

from unittest.mock import MagicMock, patch, AsyncMock
import pytest
from fastapi import status
from fastapi.testclient import TestClient
from datetime import datetime

from robosystems.graph_api.app import create_app
from robosystems.middleware.graph.clusters import NodeType


class TestMetricsRouter:
  """Test cases for metrics endpoints."""

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
  def mock_system_metrics(self):
    """Create mock system metrics."""
    return {
      "timestamp": datetime.utcnow().isoformat(),
      "cpu_percent": 45.5,
      "memory_percent": 62.3,
      "memory_used_mb": 1024.0,
      "memory_available_mb": 2048.0,
      "disk_used_gb": 20.5,
      "disk_available_gb": 80.0,
      "disk_percent": 20.4,
    }

  @pytest.fixture
  def mock_database_metrics(self):
    """Create mock database metrics."""
    return {
      "total_databases": 3,
      "total_size_mb": 512.5,
      "databases": [
        {
          "graph_id": "kg1a2b3c4d5",
          "size_mb": 200.0,
          "node_count": 1000,
          "relationship_count": 5000,
          "table_count": 10,
        },
        {
          "graph_id": "kg2b3c4d5e6",
          "size_mb": 150.0,
          "node_count": 750,
          "relationship_count": 3000,
          "table_count": 8,
        },
        {
          "graph_id": "sec",
          "size_mb": 162.5,
          "node_count": 5000,
          "relationship_count": 15000,
          "table_count": 15,
        },
      ],
    }

  @pytest.fixture
  def mock_query_metrics(self):
    """Create mock query metrics."""
    return {
      "total_queries": 10000,
      "queries_per_second": 5.2,
      "average_execution_time_ms": 45.6,
      "slow_queries_count": 15,
      "cache_hit_rate": 0.85,
      "active_connections": 5,
      "max_connections": 100,
    }

  @pytest.fixture
  def mock_ingestion_metrics(self):
    """Create mock ingestion metrics."""
    return {
      "queue_depth": 10,
      "active_tasks": 2,
      "completed_tasks": 500,
      "failed_tasks": 3,
      "average_processing_time_seconds": 30.5,
      "ingestion_rate_per_minute": 20.0,
    }

  @pytest.fixture
  def mock_admission_metrics(self):
    """Create mock admission control metrics."""
    return {
      "requests_accepted": 9500,
      "requests_rejected": 50,
      "current_load": 0.75,
      "max_load": 1.0,
      "queue_size": 5,
      "max_queue_size": 100,
    }

  @pytest.mark.asyncio
  async def test_get_metrics_complete(
    self,
    client,
    mock_system_metrics,
    mock_database_metrics,
    mock_query_metrics,
    mock_ingestion_metrics,
    mock_admission_metrics,
  ):
    """Test getting complete metrics snapshot."""
    # Configure the mock service that was already injected
    from robosystems.graph_api.core.cluster_manager import get_cluster_service

    mock_service = client.app.dependency_overrides[get_cluster_service]()
    mock_service.node_id = "test-node-01"
    mock_service.node_type = NodeType.WRITER
    mock_service.get_uptime.return_value = 3600

    # Setup metrics collector
    mock_collector = MagicMock()
    mock_collector.collect_system_metrics.return_value = mock_system_metrics
    mock_collector.collect_database_metrics.return_value = mock_database_metrics
    mock_collector.get_query_metrics.return_value = mock_query_metrics
    mock_collector.collect_ingestion_metrics = AsyncMock(
      return_value=mock_ingestion_metrics
    )
    mock_service.metrics_collector = mock_collector

    # Setup admission controller
    with patch(
      "robosystems.graph_api.routers.metrics.get_admission_controller"
    ) as mock_get_admission:
      mock_admission = MagicMock()
      mock_admission.get_metrics.return_value = mock_admission_metrics
      mock_get_admission.return_value = mock_admission

      response = client.get("/metrics")

      assert response.status_code == status.HTTP_200_OK
      data = response.json()

      # Check all metric sections are present
      assert "timestamp" in data
      assert "system" in data
      assert "databases" in data
      assert "queries" in data
      assert "ingestion" in data
      assert "admission_control" in data
      assert "cluster" in data

      # Verify system metrics
      assert data["system"]["cpu_percent"] == 45.5
      assert data["system"]["memory_percent"] == 62.3

      # Verify database metrics
      assert data["databases"]["total_databases"] == 3
      assert data["databases"]["total_size_mb"] == 512.5

      # Verify query metrics
      assert data["queries"]["total_queries"] == 10000
      assert data["queries"]["average_execution_time_ms"] == 45.6

      # Verify ingestion metrics
      assert data["ingestion"]["queue_depth"] == 10
      assert data["ingestion"]["active_tasks"] == 2

      # Verify admission control metrics
      assert data["admission_control"]["requests_accepted"] == 9500
      assert data["admission_control"]["current_load"] == 0.75

      # Verify cluster info
      assert data["cluster"]["node_id"] == "test-node-01"
      assert data["cluster"]["node_type"] == "writer"
      assert data["cluster"]["uptime_seconds"] == 3600

  @pytest.mark.asyncio
  async def test_get_metrics_minimal(self, client):
    """Test metrics with minimal data (empty databases)."""
    minimal_system = {
      "timestamp": datetime.utcnow().isoformat(),
      "cpu_percent": 0.0,
      "memory_percent": 0.0,
    }

    minimal_database = {
      "total_databases": 0,
      "total_size_mb": 0.0,
      "databases": [],
    }

    minimal_query = {
      "total_queries": 0,
      "queries_per_second": 0.0,
    }

    minimal_ingestion = {
      "queue_depth": 0,
      "active_tasks": 0,
    }

    minimal_admission = {
      "requests_accepted": 0,
      "requests_rejected": 0,
    }

    # Configure the mock service that was already injected
    from robosystems.graph_api.core.cluster_manager import get_cluster_service

    mock_service = client.app.dependency_overrides[get_cluster_service]()
    mock_service.node_id = "empty-node"
    mock_service.node_type = NodeType.WRITER
    mock_service.get_uptime.return_value = 0

    mock_collector = MagicMock()
    mock_collector.collect_system_metrics.return_value = minimal_system
    mock_collector.collect_database_metrics.return_value = minimal_database
    mock_collector.get_query_metrics.return_value = minimal_query
    mock_collector.collect_ingestion_metrics = AsyncMock(return_value=minimal_ingestion)
    mock_service.metrics_collector = mock_collector

    with patch(
      "robosystems.graph_api.routers.metrics.get_admission_controller"
    ) as mock_get_admission:
      mock_admission = MagicMock()
      mock_admission.get_metrics.return_value = minimal_admission
      mock_get_admission.return_value = mock_admission

      response = client.get("/metrics")

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert data["databases"]["total_databases"] == 0
      assert data["queries"]["total_queries"] == 0
      assert data["cluster"]["uptime_seconds"] == 0

  @pytest.mark.asyncio
  async def test_get_metrics_shared_node(self, client):
    """Test metrics for shared repository node."""
    # Configure the mock service that was already injected
    from robosystems.graph_api.core.cluster_manager import get_cluster_service

    mock_service = client.app.dependency_overrides[get_cluster_service]()
    mock_service.node_id = "shared-master-01"
    mock_service.node_type = NodeType.SHARED_MASTER
    mock_service.get_uptime.return_value = 7200

    # Shared node specific metrics
    shared_db_metrics = {
      "total_databases": 3,
      "total_size_mb": 5120.0,
      "databases": [
        {
          "graph_id": "sec",
          "size_mb": 3072.0,
          "node_count": 100000,
          "relationship_count": 500000,
        },
        {
          "graph_id": "industry",
          "size_mb": 1024.0,
          "node_count": 50000,
          "relationship_count": 200000,
        },
        {
          "graph_id": "economic",
          "size_mb": 1024.0,
          "node_count": 30000,
          "relationship_count": 150000,
        },
      ],
    }

    mock_collector = MagicMock()
    mock_collector.collect_system_metrics.return_value = {
      "timestamp": "2024-01-15T10:00:00Z"
    }
    mock_collector.collect_database_metrics.return_value = shared_db_metrics
    mock_collector.get_query_metrics.return_value = {"total_queries": 50000}
    mock_collector.collect_ingestion_metrics = AsyncMock(
      return_value={"queue_depth": 0}
    )
    mock_service.metrics_collector = mock_collector

    with patch(
      "robosystems.graph_api.routers.metrics.get_admission_controller"
    ) as mock_get_admission:
      mock_admission = MagicMock()
      mock_admission.get_metrics.return_value = {"requests_accepted": 10000}
      mock_get_admission.return_value = mock_admission

      response = client.get("/metrics")

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert data["cluster"]["node_type"] == "shared_master"
      assert data["databases"]["total_size_mb"] == 5120.0
      assert len(data["databases"]["databases"]) == 3

  @pytest.mark.asyncio
  async def test_get_metrics_high_load(self, client):
    """Test metrics under high load conditions."""
    high_load_system = {
      "timestamp": datetime.utcnow().isoformat(),
      "cpu_percent": 95.0,
      "memory_percent": 90.0,
      "memory_used_mb": 14336.0,
      "memory_available_mb": 1024.0,
    }

    high_load_query = {
      "total_queries": 1000000,
      "queries_per_second": 100.0,
      "slow_queries_count": 500,
      "cache_hit_rate": 0.5,
      "active_connections": 95,
      "max_connections": 100,
    }

    high_load_ingestion = {
      "queue_depth": 1000,
      "active_tasks": 50,
      "failed_tasks": 100,
    }

    high_load_admission = {
      "requests_accepted": 8000,
      "requests_rejected": 2000,
      "current_load": 0.95,
      "queue_size": 90,
      "max_queue_size": 100,
    }

    # Configure the mock service that was already injected
    from robosystems.graph_api.core.cluster_manager import get_cluster_service

    mock_service = client.app.dependency_overrides[get_cluster_service]()
    mock_service.node_id = "overloaded-node"
    mock_service.node_type = NodeType.WRITER
    mock_service.get_uptime.return_value = 86400

    mock_collector = MagicMock()
    mock_collector.collect_system_metrics.return_value = high_load_system
    mock_collector.collect_database_metrics.return_value = {"total_databases": 50}
    mock_collector.get_query_metrics.return_value = high_load_query
    mock_collector.collect_ingestion_metrics = AsyncMock(
      return_value=high_load_ingestion
    )
    mock_service.metrics_collector = mock_collector

    with patch(
      "robosystems.graph_api.routers.metrics.get_admission_controller"
    ) as mock_get_admission:
      mock_admission = MagicMock()
      mock_admission.get_metrics.return_value = high_load_admission
      mock_get_admission.return_value = mock_admission

      response = client.get("/metrics")

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert data["system"]["cpu_percent"] == 95.0
      assert data["system"]["memory_percent"] == 90.0
      assert data["queries"]["queries_per_second"] == 100.0
      assert data["admission_control"]["requests_rejected"] == 2000
      assert data["admission_control"]["current_load"] == 0.95

  @pytest.mark.asyncio
  async def test_get_metrics_error_handling(self, client):
    """Test metrics when some collectors fail."""
    # Configure the mock service that was already injected
    from robosystems.graph_api.core.cluster_manager import get_cluster_service

    mock_service = client.app.dependency_overrides[get_cluster_service]()
    mock_service.node_id = "error-node"
    mock_service.node_type = NodeType.WRITER
    mock_service.get_uptime.return_value = 100

    mock_collector = MagicMock()
    # System metrics succeed
    mock_collector.collect_system_metrics.return_value = {
      "timestamp": "2024-01-15T10:00:00Z"
    }
    # Database metrics fail
    mock_collector.collect_database_metrics.side_effect = Exception("Database error")
    # Query metrics succeed
    mock_collector.get_query_metrics.return_value = {"total_queries": 0}
    # Ingestion metrics succeed
    mock_collector.collect_ingestion_metrics = AsyncMock(
      return_value={"queue_depth": 0}
    )
    mock_service.metrics_collector = mock_collector

    with patch(
      "robosystems.graph_api.routers.metrics.get_admission_controller"
    ) as mock_get_admission:
      mock_admission = MagicMock()
      mock_admission.get_metrics.return_value = {"requests_accepted": 0}
      mock_get_admission.return_value = mock_admission

      # The endpoint doesn't handle errors gracefully - it raises them
      # We expect an exception to be raised
      import pytest

      with pytest.raises(Exception, match="Database error"):
        client.get("/metrics")
