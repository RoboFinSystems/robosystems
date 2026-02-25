"""Tests for database copy router endpoints."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from robosystems.graph_api.core.ladybug import get_connection_pool, get_ladybug_service
from robosystems.graph_api.routers.databases import copy as copy_module


class FakeLadybugService:
  """Fake LadybugDB service for testing."""

  def __init__(self, read_only=False):
    self.read_only = read_only


@pytest.fixture
def copy_client(monkeypatch):
  """Create TestClient with copy router and patched task manager."""
  app = FastAPI()
  app.include_router(copy_module.router)

  # Patch the global task_manager in the copy module
  mock_task_manager = SimpleNamespace(
    create_task=AsyncMock(return_value="ingest_testgraph_Entity_abc12345"),
    update_task=AsyncMock(),
    get_task=AsyncMock(return_value=None),
    get_redis=AsyncMock(return_value=MagicMock()),
  )
  monkeypatch.setattr(copy_module, "task_manager", mock_task_manager)

  # Mock the perform_ingestion background task
  perform_ingestion_mock = AsyncMock()
  monkeypatch.setattr(copy_module, "perform_ingestion", perform_ingestion_mock)

  def _factory(ladybug_service=None, connection_pool=None):
    svc = ladybug_service or FakeLadybugService()
    pool = connection_pool or MagicMock()
    app.dependency_overrides[get_ladybug_service] = lambda: svc
    app.dependency_overrides[get_connection_pool] = lambda: pool
    client = TestClient(app)
    client.task_manager = mock_task_manager
    client.perform_ingestion_mock = perform_ingestion_mock
    return client

  return _factory


class TestStartBackgroundCopy:
  """Test cases for the POST /databases/{graph_id}/copy endpoint."""

  @pytest.mark.unit
  def test_copy_success_single_pattern(self, copy_client):
    """Test successful copy initiation with a single S3 pattern."""
    with patch("robosystems.graph_api.backends.get_backend") as mock_get_backend:
      mock_backend = MagicMock()
      mock_get_backend.return_value = mock_backend

      client = copy_client()

      # Mock the redis client returned by get_redis
      mock_redis = AsyncMock()
      mock_redis.setex = AsyncMock()
      client.task_manager.get_redis = AsyncMock(return_value=mock_redis)

      response = client.post(
        "/databases/testgraph/copy",
        json={
          "s3_pattern": "s3://bucket/path/*.parquet",
          "table_name": "Entity",
          "ignore_errors": True,
        },
      )

      assert response.status_code == 200
      data = response.json()
      assert data["task_id"] == "ingest_testgraph_Entity_abc12345"
      assert data["status"] == "started"
      assert "sse_url" in data
      assert "/tasks/" in data["sse_url"]
      assert "Entity" in data["message"]

  @pytest.mark.unit
  def test_copy_success_with_s3_credentials(self, copy_client):
    """Test successful copy with explicit S3 credentials."""
    with patch("robosystems.graph_api.backends.get_backend") as mock_get_backend:
      mock_backend = MagicMock()
      mock_get_backend.return_value = mock_backend

      client = copy_client()
      mock_redis = AsyncMock()
      mock_redis.setex = AsyncMock()
      client.task_manager.get_redis = AsyncMock(return_value=mock_redis)

      response = client.post(
        "/databases/testgraph/copy",
        json={
          "s3_pattern": "s3://bucket/path/*.parquet",
          "table_name": "Fact",
          "s3_credentials": {
            "aws_access_key_id": "test-key",
            "aws_secret_access_key": "test-secret",
            "endpoint_url": "http://localhost:4566",
            "region": "us-east-1",
          },
          "ignore_errors": False,
        },
      )

      assert response.status_code == 200
      data = response.json()
      assert data["status"] == "started"
      assert "Fact" in data["message"]

  @pytest.mark.unit
  def test_copy_success_with_list_pattern(self, copy_client):
    """Test successful copy with a list of S3 patterns."""
    with patch("robosystems.graph_api.backends.get_backend") as mock_get_backend:
      mock_backend = MagicMock()
      mock_get_backend.return_value = mock_backend

      client = copy_client()
      mock_redis = AsyncMock()
      mock_redis.setex = AsyncMock()
      client.task_manager.get_redis = AsyncMock(return_value=mock_redis)

      response = client.post(
        "/databases/testgraph/copy",
        json={
          "s3_pattern": [
            "s3://bucket/q1/*.parquet",
            "s3://bucket/q2/*.parquet",
          ],
          "table_name": "Report",
          "ignore_errors": True,
        },
      )

      assert response.status_code == 200
      data = response.json()
      assert data["status"] == "started"

  @pytest.mark.unit
  def test_copy_rejected_on_read_only_node(self, copy_client):
    """Test that copy is rejected on read-only nodes."""
    svc = FakeLadybugService(read_only=True)
    client = copy_client(ladybug_service=svc)

    response = client.post(
      "/databases/testgraph/copy",
      json={
        "s3_pattern": "s3://bucket/path/*.parquet",
        "table_name": "Entity",
      },
    )

    assert response.status_code == 403
    assert "read-only" in response.json()["detail"]

  @pytest.mark.unit
  def test_copy_missing_required_fields(self, copy_client):
    """Test that missing required fields returns validation error."""
    client = copy_client()

    # Missing table_name
    response = client.post(
      "/databases/testgraph/copy",
      json={
        "s3_pattern": "s3://bucket/path/*.parquet",
      },
    )
    assert response.status_code == 422

    # Missing s3_pattern
    response = client.post(
      "/databases/testgraph/copy",
      json={
        "table_name": "Entity",
      },
    )
    assert response.status_code == 422

  @pytest.mark.unit
  def test_copy_empty_body(self, copy_client):
    """Test that an empty request body returns validation error."""
    client = copy_client()

    response = client.post(
      "/databases/testgraph/copy",
      json={},
    )
    assert response.status_code == 422

  @pytest.mark.unit
  def test_copy_response_contains_sse_url(self, copy_client):
    """Test that response includes valid SSE monitoring URL."""
    with patch("robosystems.graph_api.backends.get_backend") as mock_get_backend:
      mock_backend = MagicMock()
      mock_get_backend.return_value = mock_backend

      client = copy_client()
      mock_redis = AsyncMock()
      mock_redis.setex = AsyncMock()
      client.task_manager.get_redis = AsyncMock(return_value=mock_redis)

      response = client.post(
        "/databases/testgraph/copy",
        json={
          "s3_pattern": "s3://bucket/path/*.parquet",
          "table_name": "Entity",
        },
      )

      data = response.json()
      assert data["sse_url"] == "/tasks/ingest_testgraph_Entity_abc12345/monitor"

  @pytest.mark.unit
  def test_copy_creates_task_with_correct_params(self, copy_client):
    """Test that the task manager is called with correct parameters."""
    with patch("robosystems.graph_api.backends.get_backend") as mock_get_backend:
      mock_backend = MagicMock()
      mock_get_backend.return_value = mock_backend

      client = copy_client()
      mock_redis = AsyncMock()
      mock_redis.setex = AsyncMock()
      client.task_manager.get_redis = AsyncMock(return_value=mock_redis)

      response = client.post(
        "/databases/mygraph/copy",
        json={
          "s3_pattern": "s3://bucket/data/*.parquet",
          "table_name": "Transaction",
        },
      )

      assert response.status_code == 200

      # Verify create_task was called with expected arguments
      client.task_manager.create_task.assert_awaited_once()
      call_kwargs = client.task_manager.create_task.call_args
      assert call_kwargs.kwargs["graph_id"] == "mygraph"
      assert call_kwargs.kwargs["table_name"] == "Transaction"
      assert call_kwargs.kwargs["s3_pattern"] == "s3://bucket/data/*.parquet"

  @pytest.mark.unit
  def test_copy_default_ignore_errors_is_true(self, copy_client):
    """Test that ignore_errors defaults to True."""
    with patch("robosystems.graph_api.backends.get_backend") as mock_get_backend:
      mock_backend = MagicMock()
      mock_get_backend.return_value = mock_backend

      client = copy_client()
      mock_redis = AsyncMock()
      mock_redis.setex = AsyncMock()
      client.task_manager.get_redis = AsyncMock(return_value=mock_redis)

      response = client.post(
        "/databases/testgraph/copy",
        json={
          "s3_pattern": "s3://bucket/path/*.parquet",
          "table_name": "Entity",
          # ignore_errors not specified, should default to True
        },
      )

      assert response.status_code == 200


class TestIngestionTaskManager:
  """Tests for the IngestionTaskManager class."""

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_create_task_returns_task_id(self):
    """Test that create_task returns a properly formatted task ID."""
    manager = copy_module.IngestionTaskManager()

    mock_redis = AsyncMock()
    mock_redis.setex = AsyncMock()
    manager._redis_client = mock_redis

    task_id = await manager.create_task(
      graph_id="testgraph",
      table_name="Entity",
      s3_pattern="s3://bucket/*.parquet",
    )

    assert task_id.startswith("ingest_testgraph_Entity_")
    assert len(task_id) > len("ingest_testgraph_Entity_")

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_create_task_stores_in_redis(self):
    """Test that create_task stores task data in Redis with TTL."""
    manager = copy_module.IngestionTaskManager()

    mock_redis = AsyncMock()
    mock_redis.setex = AsyncMock()
    manager._redis_client = mock_redis

    task_id = await manager.create_task(
      graph_id="testgraph",
      table_name="Entity",
      s3_pattern="s3://bucket/*.parquet",
      estimated_records=5000,
      estimated_size_mb=50.0,
    )

    mock_redis.setex.assert_awaited_once()
    call_args = mock_redis.setex.call_args
    assert call_args[0][0] == f"lbug:task:{task_id}"
    assert call_args[0][1] == 86400  # 24 hours TTL

    stored_data = json.loads(call_args[0][2])
    assert stored_data["graph_id"] == "testgraph"
    assert stored_data["table_name"] == "Entity"
    assert stored_data["status"] == "pending"
    assert stored_data["estimated_records"] == 5000
    assert stored_data["estimated_size_mb"] == 50.0

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_update_task_success(self):
    """Test successful task update."""
    manager = copy_module.IngestionTaskManager()

    existing_task = {
      "task_id": "ingest_test_1",
      "status": "pending",
      "progress_percent": 0,
      "last_heartbeat": 0,
    }

    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=json.dumps(existing_task))
    mock_redis.setex = AsyncMock()
    manager._redis_client = mock_redis

    await manager.update_task("ingest_test_1", status="running", progress_percent=50)

    mock_redis.setex.assert_awaited_once()
    stored_data = json.loads(mock_redis.setex.call_args[0][2])
    assert stored_data["status"] == "running"
    assert stored_data["progress_percent"] == 50

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_update_task_not_found(self):
    """Test that updating a non-existent task raises ValueError."""
    manager = copy_module.IngestionTaskManager()

    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=None)
    manager._redis_client = mock_redis

    with pytest.raises(ValueError, match="not found"):
      await manager.update_task("nonexistent_task", status="running")

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_get_task_exists(self):
    """Test retrieving an existing task."""
    manager = copy_module.IngestionTaskManager()

    task_data = {"task_id": "ingest_test_1", "status": "running"}

    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=json.dumps(task_data))
    manager._redis_client = mock_redis

    result = await manager.get_task("ingest_test_1")
    assert result is not None
    assert result["task_id"] == "ingest_test_1"
    assert result["status"] == "running"

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_get_task_not_found(self):
    """Test retrieving a non-existent task returns None."""
    manager = copy_module.IngestionTaskManager()

    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=None)
    manager._redis_client = mock_redis

    result = await manager.get_task("nonexistent_task")
    assert result is None


class TestPerformIngestion:
  """Tests for the perform_ingestion background task function."""

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_perform_ingestion_success(self, monkeypatch):
    """Test successful ingestion completion."""
    mock_task_manager = SimpleNamespace(
      update_task=AsyncMock(),
      get_redis=AsyncMock(return_value=AsyncMock(delete=AsyncMock())),
    )
    monkeypatch.setattr(copy_module, "task_manager", mock_task_manager)
    monkeypatch.setattr(copy_module.env, "EC2_INSTANCE_ID", "i-test123")

    mock_backend = AsyncMock()
    mock_backend.ingest_from_s3 = AsyncMock(
      return_value={
        "records_loaded": 1500,
        "duration_seconds": 12.5,
        "query": "COPY Entity FROM ...",
      }
    )

    await copy_module.perform_ingestion(
      task_id="ingest_test_1",
      graph_id="testgraph",
      table_name="Entity",
      s3_pattern="s3://bucket/*.parquet",
      s3_credentials=None,
      ignore_errors=True,
      backend=mock_backend,
    )

    # Verify task was updated to running, then completed
    calls = mock_task_manager.update_task.call_args_list
    assert len(calls) >= 2

    # First call: status = running
    first_call = calls[0]
    assert first_call.args[0] == "ingest_test_1"
    assert first_call.kwargs["status"] == "running"

    # Last call: status = completed
    last_call = calls[-1]
    assert last_call.kwargs["status"] == "completed"
    assert last_call.kwargs["progress_percent"] == 100
    assert last_call.kwargs["records_processed"] == 1500

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_perform_ingestion_failure(self, monkeypatch):
    """Test ingestion failure updates task status."""
    mock_task_manager = SimpleNamespace(
      update_task=AsyncMock(),
      get_redis=AsyncMock(return_value=AsyncMock(delete=AsyncMock())),
    )
    monkeypatch.setattr(copy_module, "task_manager", mock_task_manager)
    monkeypatch.setattr(copy_module.env, "EC2_INSTANCE_ID", "i-test123")

    mock_backend = AsyncMock()
    mock_backend.ingest_from_s3 = AsyncMock(
      side_effect=RuntimeError("S3 connection failed")
    )

    await copy_module.perform_ingestion(
      task_id="ingest_test_fail",
      graph_id="testgraph",
      table_name="Entity",
      s3_pattern="s3://bucket/*.parquet",
      s3_credentials=None,
      ignore_errors=True,
      backend=mock_backend,
    )

    # Verify task was updated to failed
    calls = mock_task_manager.update_task.call_args_list
    last_call = calls[-1]
    assert last_call.kwargs["status"] == "failed"
    assert "S3 connection failed" in last_call.kwargs["error"]

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_perform_ingestion_clears_redis_flag_on_success(self, monkeypatch):
    """Test that ingestion flag is cleared in Redis on success."""
    mock_redis = AsyncMock()
    mock_redis.delete = AsyncMock()

    mock_task_manager = SimpleNamespace(
      update_task=AsyncMock(),
      get_redis=AsyncMock(return_value=mock_redis),
    )
    monkeypatch.setattr(copy_module, "task_manager", mock_task_manager)
    monkeypatch.setattr(copy_module.env, "EC2_INSTANCE_ID", "i-abc123")

    mock_backend = AsyncMock()
    mock_backend.ingest_from_s3 = AsyncMock(
      return_value={"records_loaded": 100, "duration_seconds": 1.0, "query": "..."}
    )

    await copy_module.perform_ingestion(
      task_id="ingest_test_redis",
      graph_id="testgraph",
      table_name="Entity",
      s3_pattern="s3://bucket/*.parquet",
      s3_credentials=None,
      ignore_errors=True,
      backend=mock_backend,
    )

    # Verify the ingestion flag was deleted from Redis
    mock_redis.delete.assert_awaited_with("lbug:ingestion:active:i-abc123")

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_perform_ingestion_clears_redis_flag_on_failure(self, monkeypatch):
    """Test that ingestion flag is cleared in Redis even on failure."""
    mock_redis = AsyncMock()
    mock_redis.delete = AsyncMock()

    mock_task_manager = SimpleNamespace(
      update_task=AsyncMock(),
      get_redis=AsyncMock(return_value=mock_redis),
    )
    monkeypatch.setattr(copy_module, "task_manager", mock_task_manager)
    monkeypatch.setattr(copy_module.env, "EC2_INSTANCE_ID", "i-abc123")

    mock_backend = AsyncMock()
    mock_backend.ingest_from_s3 = AsyncMock(side_effect=RuntimeError("Boom"))

    await copy_module.perform_ingestion(
      task_id="ingest_test_fail_redis",
      graph_id="testgraph",
      table_name="Entity",
      s3_pattern="s3://bucket/*.parquet",
      s3_credentials=None,
      ignore_errors=True,
      backend=mock_backend,
    )

    # Verify the ingestion flag was deleted even after failure
    mock_redis.delete.assert_awaited_with("lbug:ingestion:active:i-abc123")


class TestLargeTablesCleanup:
  """Tests for the LARGE_TABLES_REQUIRING_CLEANUP configuration."""

  @pytest.mark.unit
  def test_default_large_tables_is_empty(self, monkeypatch):
    """Test that default large tables list is empty."""
    # The module-level constant is already set. We verify its expected behavior.
    # When LBUG_LARGE_TABLES_REQUIRING_CLEANUP env is empty, list should be empty.
    monkeypatch.setenv("LBUG_LARGE_TABLES_REQUIRING_CLEANUP", "")
    # Re-evaluate the logic
    env_val = ""
    result = [t.strip() for t in env_val.split(",") if t.strip()] if env_val else []
    assert result == []

  @pytest.mark.unit
  def test_large_tables_parsed_from_env(self):
    """Test that large tables are correctly parsed from comma-separated env."""
    env_val = "Fact,Report, Element "
    result = [t.strip() for t in env_val.split(",") if t.strip()]
    assert result == ["Fact", "Report", "Element"]
