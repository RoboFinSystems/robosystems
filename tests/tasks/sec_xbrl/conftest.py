"""
Fixtures for SEC XBRL task tests.

Provides common mocks and test data for testing SEC XBRL processing tasks.
"""

from unittest.mock import AsyncMock, MagicMock, patch
import pytest
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO


@pytest.fixture
def mock_redis_client():
  """Mock Redis client for state management."""
  client = MagicMock()
  client.get = MagicMock(return_value=None)
  client.set = MagicMock(return_value=True)
  client.setex = MagicMock(return_value=True)
  client.hget = MagicMock(return_value=None)
  client.hset = MagicMock(return_value=True)
  client.hgetall = MagicMock(return_value={})
  client.expire = MagicMock(return_value=True)
  client.delete = MagicMock(return_value=1)
  return client


@pytest.fixture
def mock_orchestrator(mock_redis_client):
  """Mock SEC orchestrator with Redis state management."""
  with patch("robosystems.tasks.sec_xbrl.orchestration.redis.from_url") as mock_redis:
    mock_redis.return_value = mock_redis_client

    from robosystems.tasks.sec_xbrl.orchestration import SECOrchestrator

    orchestrator = SECOrchestrator()
    return orchestrator


@pytest.fixture
def mock_sec_pipeline():
  """Mock SECXBRLPipeline for testing."""
  pipeline = MagicMock()

  # Setup pipeline attributes
  pipeline.pipeline_run_id = "test_pipeline_123"
  pipeline.pipeline_type = "sec_staged"
  pipeline.redis_key = "pipeline:sec_staged:test_pipeline_123"

  # Mock methods
  pipeline._discover_companies = MagicMock(
    return_value=[
      {"cik_str": "0001045810", "ticker": "NVDA", "title": "NVIDIA CORP"},
      {"cik_str": "0000789019", "ticker": "MSFT", "title": "MICROSOFT CORP"},
    ]
  )

  pipeline._download_filings = AsyncMock(
    return_value={"downloaded": 10, "skipped": 5, "failed": 0}
  )

  pipeline._process_xbrl_files = AsyncMock(
    return_value={
      "processed": 10,
      "failed": 0,
      "nodes_created": 1000,
      "relationships_created": 5000,
    }
  )

  pipeline.update_pipeline_state = MagicMock()

  return pipeline


@pytest.fixture
def mock_kuzu_client():
  """Mock Kuzu client for database operations."""
  client = AsyncMock()

  # Mock database operations
  client.get_database_info = AsyncMock(
    return_value={
      "name": "sec",
      "status": "active",
      "node_count": 0,
      "relationship_count": 0,
    }
  )

  client.create_database = AsyncMock(
    return_value={"status": "success", "message": "Database created successfully"}
  )

  client.delete_database = AsyncMock(
    return_value={"status": "success", "message": "Database deleted successfully"}
  )

  client.execute_query = AsyncMock(return_value={"results": [], "status": "success"})

  client.copy_from_s3 = AsyncMock(
    return_value={"status": "success", "rows_loaded": 1000}
  )

  return client


@pytest.fixture
def mock_kuzu_factory(mock_kuzu_client):
  """Mock KuzuClientFactory."""
  with patch("robosystems.kuzu_api.client.factory.KuzuClientFactory") as mock_factory:
    mock_factory.create_client = AsyncMock(return_value=mock_kuzu_client)
    yield mock_factory


@pytest.fixture
def mock_s3_client():
  """Mock S3 client for file operations."""
  client = MagicMock()

  # Mock S3 operations
  client.bucket = "test-sec-bucket"

  client.list_objects = MagicMock(
    return_value=[
      "processed/year=2024/nodes/Entity/part-0001.parquet",
      "processed/year=2024/nodes/Report/part-0001.parquet",
      "processed/year=2024/relationships/ENTITY_HAS_REPORT/part-0001.parquet",
    ]
  )

  client.exists = MagicMock(return_value=True)

  client.upload_file = MagicMock(return_value=True)
  client.download_file = MagicMock(return_value=b"mock_file_content")

  client.delete_objects = MagicMock(return_value={"deleted": 10, "errors": []})

  return client


@pytest.fixture
def sample_parquet_data():
  """Create sample parquet data for testing consolidation."""
  # Create sample dataframes for different node types
  entity_df = pd.DataFrame(
    {
      "id": ["entity_1", "entity_2"],
      "cik": ["0001045810", "0000789019"],
      "name": ["NVIDIA CORP", "MICROSOFT CORP"],
      "ticker": ["NVDA", "MSFT"],
    }
  )

  report_df = pd.DataFrame(
    {
      "id": ["report_1", "report_2"],
      "accession_number": ["0001045810-24-000001", "0000789019-24-000001"],
      "form": ["10-K", "10-Q"],
      "filing_date": ["2024-01-01", "2024-02-01"],
    }
  )

  # Convert to parquet bytes
  entity_buffer = BytesIO()
  report_buffer = BytesIO()

  pq.write_table(pa.Table.from_pandas(entity_df), entity_buffer)
  pq.write_table(pa.Table.from_pandas(report_df), report_buffer)

  return {"Entity": entity_buffer.getvalue(), "Report": report_buffer.getvalue()}


@pytest.fixture
def mock_schema_loader():
  """Mock schema loader for SEC schema types."""
  loader = MagicMock()

  # Mock node types
  loader.nodes = {
    "Entity": MagicMock(name="Entity"),
    "Report": MagicMock(name="Report"),
    "Fact": MagicMock(name="Fact"),
    "Element": MagicMock(name="Element"),
    "Unit": MagicMock(name="Unit"),
  }

  # Mock relationship types
  loader.relationships = {
    "ENTITY_HAS_REPORT": MagicMock(name="ENTITY_HAS_REPORT"),
    "REPORT_HAS_FACT": MagicMock(name="REPORT_HAS_FACT"),
    "FACT_HAS_ELEMENT": MagicMock(name="FACT_HAS_ELEMENT"),
  }

  return loader


@pytest.fixture
def sample_orchestration_state():
  """Sample orchestration state for testing."""
  return {
    "phases": {
      "download": {
        "status": "completed",
        "progress": {"total": 10, "completed": 10, "failed": 0},
      },
      "process": {
        "status": "in_progress",
        "progress": {"total": 10, "completed": 5, "failed": 1},
      },
      "ingest": {"status": "pending", "progress": {}},
    },
    "companies": ["0001045810", "0000789019"],
    "years": [2023, 2024],
    "config": {"max_workers": 4, "batch_size": 10},
    "stats": {
      "start_time": "2024-01-01T00:00:00",
      "total_filings": 100,
      "total_companies": 10,
    },
    "last_updated": "2024-01-01T12:00:00",
  }


@pytest.fixture
def mock_celery_task():
  """Mock Celery task context."""
  task = MagicMock()
  task.request = MagicMock()
  task.request.id = "test_task_id"
  task.request.retries = 0
  task.retry = MagicMock()
  task.update_state = MagicMock()
  return task
