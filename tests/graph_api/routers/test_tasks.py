"""Tests for tasks router endpoints."""

from unittest.mock import MagicMock, patch, AsyncMock
import pytest
from fastapi import status
from fastapi.testclient import TestClient
import json

from robosystems.graph_api.app import create_app
from robosystems.graph_api.core.task_sse import TaskType


class TestTasksRouter:
  """Test cases for task management endpoints."""

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
  def mock_ingestion_task(self):
    """Create a mock ingestion task."""
    return {
      "task_id": "ingest_abc123",
      "status": "running",
      "progress": 50,
      "total_records": 1000,
      "processed_records": 500,
      "created_at": "2024-01-15T10:00:00Z",
      "updated_at": "2024-01-15T10:05:00Z",
      "graph_id": "kg1a2b3c4d5",
      "source": "s3://bucket/data.csv",
    }

  @pytest.fixture
  def mock_backup_task(self):
    """Create a mock backup task."""
    return {
      "task_id": "backup_xyz789",
      "status": "completed",
      "progress": 100,
      "created_at": "2024-01-15T09:00:00Z",
      "updated_at": "2024-01-15T09:10:00Z",
      "completed_at": "2024-01-15T09:10:00Z",
      "graph_id": "kg2b3c4d5e6",
      "backup_location": "s3://backup-bucket/backup.tar.gz",
      "size_mb": 512.5,
    }

  @pytest.fixture
  def mock_restore_task(self):
    """Create a mock restore task."""
    return {
      "task_id": "restore_def456",
      "status": "failed",
      "progress": 30,
      "created_at": "2024-01-15T08:00:00Z",
      "updated_at": "2024-01-15T08:05:00Z",
      "error": "Backup file corrupted",
      "graph_id": "kg3c4d5e6f7",
      "source": "s3://backup-bucket/old-backup.tar.gz",
    }

  @pytest.fixture
  def mock_copy_task(self):
    """Create a mock copy task."""
    return {
      "task_id": "copy_ghi012",
      "status": "pending",
      "created_at": "2024-01-15T11:00:00Z",
      "source_graph": "kg1a2b3c4d5",
      "target_graph": "kg9z8y7x6w5",
    }

  @pytest.mark.asyncio
  async def test_list_tasks_all(
    self,
    client,
    mock_ingestion_task,
    mock_backup_task,
    mock_restore_task,
    mock_copy_task,
  ):
    """Test listing all tasks without filters."""
    all_tasks = [
      mock_ingestion_task,
      mock_backup_task,
      mock_restore_task,
      mock_copy_task,
    ]

    with patch(
      "robosystems.graph_api.routers.tasks.unified_task_manager"
    ) as mock_manager:
      mock_manager.list_all_tasks = AsyncMock(return_value=all_tasks)

      response = client.get("/tasks")

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert len(data) == 4
      assert data[0]["task_id"] == "ingest_abc123"
      assert data[1]["task_id"] == "backup_xyz789"
      assert data[2]["task_id"] == "restore_def456"
      assert data[3]["task_id"] == "copy_ghi012"

  @pytest.mark.asyncio
  async def test_list_tasks_with_status_filter(
    self, client, mock_ingestion_task, mock_backup_task
  ):
    """Test listing tasks filtered by status."""
    # Only running tasks
    running_tasks = [mock_ingestion_task]

    with patch(
      "robosystems.graph_api.routers.tasks.unified_task_manager"
    ) as mock_manager:
      mock_manager.list_all_tasks = AsyncMock(return_value=running_tasks)

      response = client.get("/tasks?status=running")

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert len(data) == 1
      assert data[0]["status"] == "running"

  @pytest.mark.asyncio
  async def test_list_tasks_with_type_filter(
    self, client, mock_ingestion_task, mock_backup_task, mock_copy_task
  ):
    """Test listing tasks filtered by type prefix."""
    all_tasks = [mock_ingestion_task, mock_backup_task, mock_copy_task]

    with patch(
      "robosystems.graph_api.routers.tasks.unified_task_manager"
    ) as mock_manager:
      mock_manager.list_all_tasks = AsyncMock(return_value=all_tasks)

      # Filter for ingestion tasks
      response = client.get("/tasks?task_type=ingest")

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert len(data) == 1
      assert data[0]["task_id"].startswith("ingest")

      # Filter for backup tasks
      response = client.get("/tasks?task_type=backup")

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert len(data) == 1
      assert data[0]["task_id"].startswith("backup")

  @pytest.mark.asyncio
  async def test_list_tasks_with_limit(self, client):
    """Test listing tasks with limit."""
    # Create many tasks
    many_tasks = [
      {
        "task_id": f"ingest_{i:03d}",
        "status": "completed",
        "created_at": f"2024-01-15T{10 + i:02d}:00:00Z",
      }
      for i in range(20)
    ]

    with patch(
      "robosystems.graph_api.routers.tasks.unified_task_manager"
    ) as mock_manager:
      mock_manager.list_all_tasks = AsyncMock(return_value=many_tasks)

      response = client.get("/tasks?limit=5")

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert len(data) == 5

  @pytest.mark.asyncio
  async def test_get_task_status_success(self, client, mock_ingestion_task):
    """Test getting task status successfully."""
    with patch(
      "robosystems.graph_api.routers.tasks.unified_task_manager"
    ) as mock_manager:
      mock_manager.get_task = AsyncMock(return_value=mock_ingestion_task)

      response = client.get("/tasks/ingest_abc123/status")

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert data["task_id"] == "ingest_abc123"
      assert data["status"] == "running"
      assert data["progress"] == 50

  @pytest.mark.asyncio
  async def test_get_task_status_not_found(self, client):
    """Test getting status for non-existent task."""
    with patch(
      "robosystems.graph_api.routers.tasks.unified_task_manager"
    ) as mock_manager:
      mock_manager.get_task = AsyncMock(return_value=None)

      response = client.get("/tasks/nonexistent/status")

      assert response.status_code == status.HTTP_404_NOT_FOUND
      data = response.json()
      assert "not found" in data["detail"].lower()

  @pytest.mark.asyncio
  async def test_get_task_statistics(
    self,
    client,
    mock_ingestion_task,
    mock_backup_task,
    mock_restore_task,
    mock_copy_task,
  ):
    """Test getting task statistics."""
    all_tasks = [
      mock_ingestion_task,  # running
      mock_backup_task,  # completed
      mock_restore_task,  # failed
      mock_copy_task,  # pending
    ]

    with patch(
      "robosystems.graph_api.routers.tasks.unified_task_manager"
    ) as mock_manager:
      mock_manager.list_all_tasks = AsyncMock(return_value=all_tasks)

      response = client.get("/tasks/stats")

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert data["total_tasks"] == 4
      assert data["tasks_by_status"]["running"] == 1
      assert data["tasks_by_status"]["completed"] == 1
      assert data["tasks_by_status"]["failed"] == 1
      assert data["tasks_by_status"]["pending"] == 1
      assert data["active_tasks"] == 1
      assert data["pending_tasks"] == 1
      assert data["completed_tasks"] == 1
      assert data["failed_tasks"] == 1

  @pytest.mark.asyncio
  async def test_get_task_statistics_by_type(self, client):
    """Test task statistics grouped by type."""
    tasks = [
      {"task_id": "ingest_001", "status": "completed"},
      {"task_id": "ingest_002", "status": "running"},
      {"task_id": "backup_001", "status": "completed"},
      {"task_id": "backup_002", "status": "completed"},
      {"task_id": "restore_001", "status": "failed"},
      {"task_id": "copy_001", "status": "pending"},
    ]

    with patch(
      "robosystems.graph_api.routers.tasks.unified_task_manager"
    ) as mock_manager:
      mock_manager.list_all_tasks = AsyncMock(return_value=tasks)

      response = client.get("/tasks/stats")

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert data["tasks_by_type"]["ingestion"] == 3  # 2 ingest + 1 copy
      assert data["tasks_by_type"]["backup"] == 2
      assert data["tasks_by_type"]["restore"] == 1

  @pytest.mark.asyncio
  async def test_get_task_statistics_empty(self, client):
    """Test statistics when no tasks exist."""
    with patch(
      "robosystems.graph_api.routers.tasks.unified_task_manager"
    ) as mock_manager:
      mock_manager.list_all_tasks = AsyncMock(return_value=[])

      response = client.get("/tasks/stats")

      assert response.status_code == status.HTTP_200_OK
      data = response.json()
      assert data["total_tasks"] == 0
      assert data["active_tasks"] == 0
      assert data["pending_tasks"] == 0
      assert data["completed_tasks"] == 0
      assert data["failed_tasks"] == 0

  def test_monitor_task_endpoint_exists(self, client):
    """Test that monitor endpoint exists (SSE endpoints need special handling)."""
    # SSE endpoints return EventSourceResponse which test client doesn't handle well
    # Just verify the endpoint exists
    with patch(
      "robosystems.graph_api.routers.tasks.unified_task_manager"
    ) as mock_manager:
      mock_manager.get_task_type.return_value = TaskType.INGESTION

      with patch("robosystems.graph_api.routers.tasks.EventSourceResponse") as mock_sse:
        mock_sse.return_value = MagicMock()

        with patch("robosystems.graph_api.routers.tasks.generate_task_sse_events"):
          # This will fail with SSE but confirms endpoint is registered
          response = client.get("/tasks/test_123/monitor")
          # EventSourceResponse doesn't work with TestClient
          # but we can verify the endpoint exists
          assert response.status_code != 404

  @pytest.mark.asyncio
  async def test_unified_task_manager_get_task_from_redis(self):
    """Test UnifiedTaskManager getting task from Redis."""
    from robosystems.graph_api.routers.tasks import UnifiedTaskManager

    manager = UnifiedTaskManager()
    mock_task = {"task_id": "test_123", "status": "running"}

    with patch.object(manager, "get_redis") as mock_get_redis:
      mock_redis = AsyncMock()
      mock_redis.get = AsyncMock(return_value=json.dumps(mock_task))
      mock_get_redis.return_value = mock_redis

      result = await manager.get_task("test_123")

      assert result == mock_task
      mock_redis.get.assert_called_once_with("lbug:task:test_123")

  @pytest.mark.asyncio
  async def test_unified_task_manager_get_task_fallback(self):
    """Test UnifiedTaskManager fallback to specific managers."""
    from robosystems.graph_api.routers.tasks import UnifiedTaskManager

    manager = UnifiedTaskManager()
    mock_task = {"task_id": "ingest_123", "status": "completed"}

    with patch.object(manager, "get_redis") as mock_get_redis:
      mock_redis = AsyncMock()
      mock_redis.get = AsyncMock(return_value=None)  # Not in Redis
      mock_get_redis.return_value = mock_redis

      # Mock the ingestion task manager
      mock_ingestion_manager = MagicMock()
      mock_ingestion_manager.get_task = AsyncMock(return_value=mock_task)
      manager.managers["ingest"] = mock_ingestion_manager

      result = await manager.get_task("ingest_123")

      assert result == mock_task
      mock_ingestion_manager.get_task.assert_called_once_with("ingest_123")

  @pytest.mark.asyncio
  async def test_unified_task_manager_list_all_tasks(self):
    """Test UnifiedTaskManager listing all tasks."""
    from robosystems.graph_api.routers.tasks import UnifiedTaskManager

    manager = UnifiedTaskManager()
    tasks = [
      {
        "task_id": "ingest_001",
        "status": "running",
        "created_at": "2024-01-15T10:00:00Z",
      },
      {
        "task_id": "backup_001",
        "status": "completed",
        "created_at": "2024-01-15T09:00:00Z",
      },
    ]

    with patch.object(manager, "get_redis") as mock_get_redis:
      mock_redis = AsyncMock()
      mock_redis.keys = AsyncMock(
        return_value=["lbug:task:ingest_001", "lbug:task:backup_001"]
      )
      mock_redis.get = AsyncMock(
        side_effect=[json.dumps(tasks[0]), json.dumps(tasks[1])]
      )
      mock_get_redis.return_value = mock_redis

      result = await manager.list_all_tasks()

      assert len(result) == 2
      # Should be sorted by created_at descending
      assert result[0]["task_id"] == "ingest_001"
      assert result[1]["task_id"] == "backup_001"

  def test_unified_task_manager_get_task_type(self):
    """Test UnifiedTaskManager determining task type from ID."""
    from robosystems.graph_api.routers.tasks import UnifiedTaskManager

    manager = UnifiedTaskManager()

    assert manager.get_task_type("ingest_123") == TaskType.INGESTION
    assert manager.get_task_type("backup_456") == TaskType.BACKUP
    assert manager.get_task_type("restore_789") == TaskType.RESTORE
    assert manager.get_task_type("copy_012") == TaskType.INGESTION
    assert manager.get_task_type("unknown_345") == TaskType.INGESTION  # Default
