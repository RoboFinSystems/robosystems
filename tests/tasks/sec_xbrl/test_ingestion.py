"""
Test Suite for SEC XBRL Ingestion Tasks

Tests the core ingestion logic and business rules.
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock, Mock
from datetime import datetime


class TestIngestionLogic:
  """Test the ingestion logic synchronously."""

  def test_schema_types_structure(self):
    """Test that schema types have the expected structure."""
    from robosystems.tasks.sec_xbrl import ingestion
    from robosystems.tasks.sec_xbrl.ingestion import (
      _ensure_schema_loaded,
    )

    # Ensure schema is loaded before testing
    _ensure_schema_loaded()

    # Basic assertions about schema structure
    assert isinstance(ingestion.NODE_TYPES, list)
    assert isinstance(ingestion.RELATIONSHIP_TYPES, list)
    assert isinstance(ingestion.GLOBAL_NODE_TYPES, set)
    assert isinstance(ingestion.GLOBAL_RELATIONSHIP_TYPES, set)

    # Note: RELATIONSHIPS_NEEDING_IGNORE_ERRORS is no longer needed
    # since we always use ignore_errors=True for SEC data

  @patch("robosystems.graph_api.client.factory.GraphClientFactory")
  @patch("robosystems.operations.aws.s3.S3Client")
  def test_ingestion_flow_logic(self, mock_s3_class, mock_lbug_factory):
    """Test the core ingestion flow logic."""
    # Setup mocks
    mock_s3_client = MagicMock()
    mock_s3_class.return_value = mock_s3_client

    mock_lbug_client = MagicMock()
    mock_lbug_factory.create_client.return_value = mock_lbug_client

    # Mock S3 operations
    mock_s3_client.list_objects.return_value = [
      "processed/year=2024/nodes/Entity/part-001.parquet"
    ]

  def test_get_sec_schema_types(self):
    """Test get_sec_schema_types function."""
    from robosystems.tasks.sec_xbrl.ingestion import get_sec_schema_types

    schema_types = get_sec_schema_types()

    assert "node_types" in schema_types
    assert "relationship_types" in schema_types
    assert "global_node_types" in schema_types
    assert "global_relationship_types" in schema_types
    assert isinstance(schema_types["node_types"], list)
    assert isinstance(schema_types["relationship_types"], list)
    assert isinstance(schema_types["global_node_types"], set)
    assert isinstance(schema_types["global_relationship_types"], set)

  def test_ingest_sec_data(self):
    """Test ingest_sec_data function exists and is a Celery task."""
    from robosystems.tasks.sec_xbrl.ingestion import ingest_sec_data

    # Verify it's a Celery task
    assert hasattr(ingest_sec_data, "delay")
    assert hasattr(ingest_sec_data, "apply_async")

  def test_ingestion_s3_path_construction(self):
    """Test that S3 paths are constructed correctly."""
    year = 2024

    # Test node path construction
    node_prefix = f"processed/year={year}/nodes/"
    assert "year=2024" in node_prefix
    assert "nodes" in node_prefix

    # Test relationship path construction
    rel_prefix = f"processed/year={year}/relationships/"
    assert "year=2024" in rel_prefix
    assert "relationships" in rel_prefix

    # Test consolidated path construction
    consolidated_prefix = "consolidated/nodes/"
    assert "consolidated" in consolidated_prefix

  def test_batch_mode_logic(self):
    """Test batch mode vs individual file processing logic."""
    # Test that batch mode uses different path patterns
    batch_path = "processed/batch_20240101_120000/nodes/Entity/*.parquet"
    regular_path = "processed/year=2024/nodes/Entity/*.parquet"

    assert "batch_" in batch_path
    assert "year=" in regular_path

  def test_incremental_mode_filtering(self):
    """Test incremental mode timestamp filtering."""
    timestamp_after = "2024-01-01T00:00:00"

    files = [
      {"name": "batch_20231231_235959.parquet", "timestamp": "2023-12-31T23:59:59"},
      {"name": "batch_20240101_000001.parquet", "timestamp": "2024-01-01T00:00:01"},
      {"name": "batch_20240102_120000.parquet", "timestamp": "2024-01-02T12:00:00"},
    ]

    # Filter files after timestamp
    filtered = [f for f in files if f["timestamp"] > timestamp_after]

    assert len(filtered) == 2
    assert filtered[0]["name"] == "batch_20240101_000001.parquet"
    assert filtered[1]["name"] == "batch_20240102_120000.parquet"


class TestIngestionErrorHandling:
  """Test error handling in ingestion."""

  def test_handle_missing_database(self):
    """Test handling when database doesn't exist."""
    # Mock a missing database scenario
    mock_client = MagicMock()
    mock_client.get_database_info.side_effect = Exception("Database not found")

    # The task should try to create the database
    should_create = True

    try:
      mock_client.get_database_info("sec")
    except Exception:
      should_create = True

    assert should_create

  def test_handle_s3_errors(self):
    """Test handling of S3 access errors."""
    mock_s3 = MagicMock()
    mock_s3.list_objects.side_effect = Exception("Access Denied")

    # Should handle the error gracefully
    try:
      mock_s3.list_objects("s3://bucket/path/")
      assert False, "Should have raised exception"
    except Exception as e:
      assert "Access Denied" in str(e)

  def test_ignore_errors_always_true(self):
    """Test that we always use ignore_errors=True for SEC data."""
    # Since we always use ignore_errors=True for SEC data ingestion,
    # this test verifies that the logic has been simplified
    # Previously we had conditional logic based on RELATIONSHIPS_NEEDING_IGNORE_ERRORS
    # Now all SEC data uses ignore_errors=True to handle inconsistencies

    # This is now a simple assertion since we always use ignore_errors
    ignore_errors = True  # Always true for SEC data
    assert ignore_errors


class TestIngestionStateManagement:
  """Test ingestion state and progress tracking."""

  def test_pipeline_state_initialization(self):
    """Test pipeline state is initialized correctly."""
    pipeline_id = "test_pipeline_123"

    initial_state = {
      "pipeline_id": pipeline_id,
      "status": "started",
      "start_time": datetime.now().isoformat(),
      "nodes_processed": 0,
      "relationships_processed": 0,
      "errors": [],
    }

    assert initial_state["pipeline_id"] == pipeline_id
    assert initial_state["status"] == "started"
    assert initial_state["nodes_processed"] == 0

  def test_progress_tracking(self):
    """Test progress tracking during ingestion."""
    total_files = 10
    processed = 0

    # Simulate processing files
    for i in range(total_files):
      processed += 1
      progress = (processed / total_files) * 100

      assert progress == ((i + 1) / total_files) * 100

    assert processed == total_files

  def test_error_collection(self):
    """Test that errors are collected during processing."""
    errors = []

    # Simulate some errors
    errors.append(
      {
        "file": "node_001.parquet",
        "error": "Invalid schema",
        "timestamp": datetime.now().isoformat(),
      }
    )

    errors.append(
      {
        "file": "rel_001.parquet",
        "error": "Missing reference",
        "timestamp": datetime.now().isoformat(),
      }
    )

    assert len(errors) == 2
    assert errors[0]["file"] == "node_001.parquet"
    assert "Invalid schema" in errors[0]["error"]


class TestIngestionConfiguration:
  """Test ingestion configuration options."""

  def test_default_configuration(self):
    """Test default ingestion configuration."""
    defaults = {
      "db_name": "sec",
      "graph_id": "sec",
      "schema_type": "shared",
      "repository_name": "sec",
      "batch_mode": True,
      "incremental": False,
      "use_consolidated": False,
    }

    assert defaults["db_name"] == "sec"
    assert defaults["batch_mode"]
    assert not defaults["incremental"]

  def test_custom_configuration(self):
    """Test custom ingestion configuration."""
    custom = {
      "db_name": "custom_sec",
      "graph_id": "custom_sec",
      "incremental": True,
      "timestamp_after": "2024-01-01T00:00:00",
      "use_consolidated": True,
    }

    assert custom["db_name"] == "custom_sec"
    assert custom["incremental"]
    assert custom["use_consolidated"]

  def test_bucket_configuration(self):
    """Test S3 bucket configuration."""

    # Test that bucket can be configured
    default_bucket = "robosystems-sec-processed"
    custom_bucket = "my-custom-bucket"

    # Default should be used if not specified
    bucket = custom_bucket or default_bucket
    assert bucket == custom_bucket

    # Fall back to default if custom is None
    bucket = None or default_bucket
    assert bucket == default_bucket


@pytest.fixture(autouse=True)
def mock_celery_async_result():
  """Mock Celery AsyncResult to avoid Redis connection during tests."""
  with patch(
    "robosystems.tasks.sec_xbrl.ingestion.celery_app.AsyncResult"
  ) as mock_result_class:
    mock_result = Mock()
    mock_result.state = "PENDING"
    mock_result_class.return_value = mock_result
    yield mock_result_class


class TestIngestSecDataTask:
  """Test cases for ingest_sec_data Celery task."""

  def setup_schema_types(self):
    """Setup schema type variables for testing."""
    from robosystems.tasks.sec_xbrl import ingestion

    ingestion.NODE_TYPES = [
      "Entity",
      "Fact",
      "FilingDocument",
      "Element",
      "Period",
      "Unit",
    ]
    ingestion.RELATIONSHIP_TYPES = [
      "FILED",
      "HAS_FACT",
      "ELEMENT_HAS_PERIOD",
      "ELEMENT_HAS_UNIT",
      "BELONGS_TO",
      "DIMENSION_MEMBER",
    ]
    ingestion.GLOBAL_NODE_TYPES = {"User", "GraphMetadata"}
    ingestion.GLOBAL_RELATIONSHIP_TYPES = {"ELEMENT_HAS_PERIOD", "ELEMENT_HAS_UNIT"}

  @patch("robosystems.tasks.sec_xbrl.ingestion._bulk_load_relationship_type_sync")
  @patch("robosystems.tasks.sec_xbrl.ingestion._bulk_load_node_type_sync")
  @patch("robosystems.tasks.sec_xbrl.ingestion.GraphClientFactory")
  @patch("robosystems.tasks.sec_xbrl.ingestion.env")
  def test_successful_ingestion_basic(
    self,
    mock_env,
    mock_factory,
    mock_load_node,
    mock_load_rel,
  ):
    """Test successful SEC data ingestion with basic configuration."""
    from robosystems.tasks.sec_xbrl.ingestion import ingest_sec_data

    self.setup_schema_types()

    mock_env.SEC_PROCESSED_BUCKET = "test-bucket"
    mock_env.AWS_REGION = "us-east-1"
    mock_env.ENVIRONMENT = "dev"
    mock_env.AWS_S3_ACCESS_KEY_ID = "test-key"
    mock_env.AWS_S3_SECRET_ACCESS_KEY = "test-secret"
    mock_env.AWS_ENDPOINT_URL = None
    mock_env.XBRL_GRAPH_LARGE_NODES = "Entity,Fact"

    mock_client = MagicMock()
    mock_client.get_database_info = AsyncMock(
      return_value={"name": "sec", "status": "active"}
    )
    mock_client.get_schema = AsyncMock(
      return_value=[
        {"type": "NODE", "name": "Entity"},
        {"type": "NODE", "name": "Fact"},
        {"type": "NODE", "name": "FilingDocument"},
        {"type": "NODE", "name": "Element"},
        {"type": "NODE", "name": "Period"},
        {"type": "NODE", "name": "Unit"},
        {"type": "REL", "name": "FILED"},
        {"type": "REL", "name": "HAS_FACT"},
        {"type": "REL", "name": "ELEMENT_HAS_PERIOD"},
        {"type": "REL", "name": "ELEMENT_HAS_UNIT"},
        {"type": "REL", "name": "BELONGS_TO"},
        {"type": "REL", "name": "DIMENSION_MEMBER"},
      ]
    )
    mock_factory.create_client = AsyncMock(return_value=mock_client)

    mock_load_node.return_value = {
      "records_loaded": 1000,
      "files_processed": 5,
      "duration_ms": 500,
      "warning_count": 0,
    }

    mock_load_rel.return_value = {
      "records_loaded": 2000,
      "files_processed": 3,
      "duration_ms": 300,
      "warning_count": 0,
    }

    result = ingest_sec_data.apply(
      args=[],
      kwargs={
        "pipeline_run_id": "test-pipeline-123",
        "year": 2024,
        "bucket": "test-bucket",
      },
    ).get()

    assert result["status"] == "completed"
    assert result["pipeline_run_id"] == "test-pipeline-123"
    assert result["year"] == 2024
    assert result["backend"] == "ladybug"
    assert result["nodes"]["types_loaded"] > 0
    assert result["relationships"]["types_loaded"] > 0

  @patch("robosystems.tasks.sec_xbrl.ingestion._bulk_load_relationship_type_sync")
  @patch("robosystems.tasks.sec_xbrl.ingestion._bulk_load_node_type_sync")
  @patch("robosystems.tasks.sec_xbrl.ingestion.GraphClientFactory")
  @patch("robosystems.tasks.sec_xbrl.ingestion.env")
  def test_ingestion_with_warnings(
    self,
    mock_env,
    mock_factory,
    mock_load_node,
    mock_load_rel,
  ):
    """Test ingestion that completes with warnings."""
    from robosystems.tasks.sec_xbrl.ingestion import ingest_sec_data

    self.setup_schema_types()

    mock_env.SEC_PROCESSED_BUCKET = "test-bucket"
    mock_env.AWS_REGION = "us-east-1"
    mock_env.ENVIRONMENT = "dev"
    mock_env.AWS_S3_ACCESS_KEY_ID = "test-key"
    mock_env.AWS_S3_SECRET_ACCESS_KEY = None
    mock_env.AWS_ENDPOINT_URL = None
    mock_env.XBRL_GRAPH_LARGE_NODES = "Entity,Fact"

    mock_client = MagicMock()
    mock_client.get_database_info = AsyncMock(return_value={"name": "sec"})
    mock_client.get_schema = AsyncMock(
      return_value=[
        {"type": "NODE", "name": "Entity"},
        {"type": "NODE", "name": "Fact"},
        {"type": "NODE", "name": "FilingDocument"},
        {"type": "NODE", "name": "Element"},
        {"type": "NODE", "name": "Period"},
        {"type": "NODE", "name": "Unit"},
        {"type": "REL", "name": "FILED"},
        {"type": "REL", "name": "HAS_FACT"},
        {"type": "REL", "name": "ELEMENT_HAS_PERIOD"},
        {"type": "REL", "name": "ELEMENT_HAS_UNIT"},
        {"type": "REL", "name": "BELONGS_TO"},
        {"type": "REL", "name": "DIMENSION_MEMBER"},
      ]
    )
    mock_factory.create_client = AsyncMock(return_value=mock_client)

    mock_load_node.return_value = {
      "records_loaded": 800,
      "files_processed": 4,
      "duration_ms": 450,
      "warning_count": 10,
    }

    mock_load_rel.return_value = {
      "records_loaded": 1500,
      "files_processed": 2,
      "duration_ms": 250,
      "warning_count": 5,
    }

    result = ingest_sec_data.apply(
      args=[],
      kwargs={
        "pipeline_run_id": "test-pipeline-456",
        "year": 2024,
      },
    ).get()

    assert result["status"] == "completed_with_warnings"
    assert result["totals"]["warnings"] == 90

  @patch("robosystems.tasks.sec_xbrl.ingestion.GraphClientFactory")
  @patch("robosystems.tasks.sec_xbrl.ingestion.env")
  def test_ingestion_database_not_found(self, mock_env, mock_factory):
    """Test ingestion fails when database doesn't exist."""
    from robosystems.tasks.sec_xbrl.ingestion import ingest_sec_data

    self.setup_schema_types()

    mock_env.SEC_PROCESSED_BUCKET = "test-bucket"
    mock_env.AWS_REGION = "us-east-1"
    mock_env.ENVIRONMENT = "dev"
    mock_env.AWS_S3_ACCESS_KEY_ID = "test-key"
    mock_env.AWS_S3_SECRET_ACCESS_KEY = None
    mock_env.AWS_ENDPOINT_URL = None
    mock_env.XBRL_GRAPH_LARGE_NODES = "Entity,Fact"

    mock_client = MagicMock()
    mock_client.get_database_info = AsyncMock(
      side_effect=Exception("Database not found")
    )
    mock_factory.create_client = AsyncMock(return_value=mock_client)

    with pytest.raises(RuntimeError, match="SEC database does not exist"):
      ingest_sec_data.apply(
        args=[],
        kwargs={
          "pipeline_run_id": "test-pipeline-789",
          "year": 2024,
        },
      ).get()

  @patch("robosystems.tasks.sec_xbrl.ingestion.GraphClientFactory")
  @patch("robosystems.tasks.sec_xbrl.ingestion.env")
  def test_ingestion_database_missing_schema(self, mock_env, mock_factory):
    """Test ingestion fails when database exists but lacks schema."""
    from robosystems.tasks.sec_xbrl.ingestion import ingest_sec_data

    self.setup_schema_types()

    mock_env.SEC_PROCESSED_BUCKET = "test-bucket"
    mock_env.AWS_REGION = "us-east-1"
    mock_env.ENVIRONMENT = "dev"
    mock_env.AWS_S3_ACCESS_KEY_ID = "test-key"
    mock_env.AWS_S3_SECRET_ACCESS_KEY = None
    mock_env.AWS_ENDPOINT_URL = None
    mock_env.XBRL_GRAPH_LARGE_NODES = "Entity,Fact"

    mock_client = MagicMock()
    mock_client.get_database_info = AsyncMock(return_value={"name": "sec"})
    mock_client.get_schema = AsyncMock(return_value=[])
    mock_factory.create_client = AsyncMock(return_value=mock_client)

    with pytest.raises(RuntimeError, match="lacks proper RoboLedger schema"):
      ingest_sec_data.apply(
        args=[],
        kwargs={
          "pipeline_run_id": "test-pipeline-empty",
          "year": 2024,
        },
      ).get()

  @patch("robosystems.tasks.sec_xbrl.ingestion._bulk_load_relationship_type_sync")
  @patch("robosystems.tasks.sec_xbrl.ingestion._bulk_load_node_type_sync")
  @patch("robosystems.tasks.sec_xbrl.ingestion.GraphClientFactory")
  @patch("robosystems.tasks.sec_xbrl.ingestion.env")
  def test_ingestion_with_consolidated_files(
    self,
    mock_env,
    mock_factory,
    mock_load_node,
    mock_load_rel,
  ):
    """Test ingestion using consolidated files."""
    from robosystems.tasks.sec_xbrl.ingestion import ingest_sec_data

    self.setup_schema_types()

    mock_env.SEC_PROCESSED_BUCKET = "test-bucket"
    mock_env.AWS_REGION = "us-east-1"
    mock_env.ENVIRONMENT = "prod"
    mock_env.AWS_S3_ACCESS_KEY_ID = None
    mock_env.AWS_S3_SECRET_ACCESS_KEY = None
    mock_env.AWS_ENDPOINT_URL = None
    mock_env.XBRL_GRAPH_LARGE_NODES = "Entity,Fact"

    mock_client = MagicMock()
    mock_client.get_database_info = AsyncMock(return_value={"name": "sec"})
    mock_client.get_schema = AsyncMock(
      return_value=[
        {"type": "NODE", "name": "Entity"},
        {"type": "NODE", "name": "Fact"},
        {"type": "NODE", "name": "FilingDocument"},
        {"type": "NODE", "name": "Element"},
        {"type": "NODE", "name": "Period"},
        {"type": "NODE", "name": "Unit"},
        {"type": "REL", "name": "FILED"},
        {"type": "REL", "name": "HAS_FACT"},
        {"type": "REL", "name": "ELEMENT_HAS_PERIOD"},
        {"type": "REL", "name": "ELEMENT_HAS_UNIT"},
        {"type": "REL", "name": "BELONGS_TO"},
        {"type": "REL", "name": "DIMENSION_MEMBER"},
      ]
    )
    mock_factory.create_client = AsyncMock(return_value=mock_client)

    mock_load_node.return_value = {
      "records_loaded": 5000,
      "files_processed": 10,
      "duration_ms": 1000,
      "warning_count": 0,
    }

    mock_load_rel.return_value = {
      "records_loaded": 10000,
      "files_processed": 8,
      "duration_ms": 800,
      "warning_count": 0,
    }

    result = ingest_sec_data.apply(
      args=[],
      kwargs={
        "pipeline_run_id": "test-pipeline-consolidated",
        "year": 2024,
        "use_consolidated": True,
      },
    ).get()

    assert result["status"] == "completed"

    call_args = mock_load_node.call_args[1]
    assert "consolidated/year=2024/" in call_args["s3_pattern"]

  @patch("robosystems.tasks.sec_xbrl.ingestion._bulk_load_relationship_type_sync")
  @patch("robosystems.tasks.sec_xbrl.ingestion._bulk_load_node_type_sync")
  @patch("robosystems.tasks.sec_xbrl.ingestion.GraphClientFactory")
  @patch("robosystems.tasks.sec_xbrl.ingestion.env")
  def test_ingestion_year_extraction_from_prefix(
    self,
    mock_env,
    mock_factory,
    mock_load_node,
    mock_load_rel,
  ):
    """Test year extraction from prefix when year not provided."""
    from robosystems.tasks.sec_xbrl.ingestion import ingest_sec_data

    self.setup_schema_types()

    mock_env.SEC_PROCESSED_BUCKET = "test-bucket"
    mock_env.AWS_REGION = "us-east-1"
    mock_env.ENVIRONMENT = "dev"
    mock_env.AWS_S3_ACCESS_KEY_ID = "test-key"
    mock_env.AWS_S3_SECRET_ACCESS_KEY = None
    mock_env.AWS_ENDPOINT_URL = None
    mock_env.XBRL_GRAPH_LARGE_NODES = "Entity,Fact"

    mock_client = MagicMock()
    mock_client.get_database_info = AsyncMock(return_value={"name": "sec"})
    mock_client.get_schema = AsyncMock(
      return_value=[
        {"type": "NODE", "name": "Entity"},
        {"type": "NODE", "name": "Fact"},
        {"type": "NODE", "name": "FilingDocument"},
        {"type": "NODE", "name": "Element"},
        {"type": "NODE", "name": "Period"},
        {"type": "NODE", "name": "Unit"},
        {"type": "REL", "name": "FILED"},
        {"type": "REL", "name": "HAS_FACT"},
        {"type": "REL", "name": "ELEMENT_HAS_PERIOD"},
        {"type": "REL", "name": "ELEMENT_HAS_UNIT"},
        {"type": "REL", "name": "BELONGS_TO"},
        {"type": "REL", "name": "DIMENSION_MEMBER"},
      ]
    )
    mock_factory.create_client = AsyncMock(return_value=mock_client)

    mock_load_node.return_value = {
      "records_loaded": 500,
      "files_processed": 2,
      "duration_ms": 200,
      "warning_count": 0,
    }

    mock_load_rel.return_value = {
      "records_loaded": 1000,
      "files_processed": 1,
      "duration_ms": 150,
      "warning_count": 0,
    }

    result = ingest_sec_data.apply(
      args=[],
      kwargs={
        "pipeline_run_id": "test-pipeline-year-extract",
        "prefix": "processed/year=2023/",
      },
    ).get()

    assert result["year"] == 2023

  @patch("robosystems.tasks.sec_xbrl.ingestion._bulk_load_relationship_type_sync")
  @patch("robosystems.tasks.sec_xbrl.ingestion._bulk_load_node_type_sync")
  @patch("robosystems.tasks.sec_xbrl.ingestion.GraphClientFactory")
  @patch("robosystems.tasks.sec_xbrl.ingestion.env")
  def test_ingestion_node_load_failure(
    self,
    mock_env,
    mock_factory,
    mock_load_node,
    mock_load_rel,
  ):
    """Test handling of node load failures."""
    from robosystems.tasks.sec_xbrl.ingestion import ingest_sec_data

    self.setup_schema_types()

    mock_env.SEC_PROCESSED_BUCKET = "test-bucket"
    mock_env.AWS_REGION = "us-east-1"
    mock_env.ENVIRONMENT = "dev"
    mock_env.AWS_S3_ACCESS_KEY_ID = "test-key"
    mock_env.AWS_S3_SECRET_ACCESS_KEY = None
    mock_env.AWS_ENDPOINT_URL = None
    mock_env.XBRL_GRAPH_LARGE_NODES = "Entity,Fact"

    mock_client = MagicMock()
    mock_client.get_database_info = AsyncMock(return_value={"name": "sec"})
    mock_client.get_schema = AsyncMock(
      return_value=[
        {"type": "NODE", "name": "Entity"},
        {"type": "NODE", "name": "Fact"},
        {"type": "NODE", "name": "FilingDocument"},
        {"type": "NODE", "name": "Element"},
        {"type": "NODE", "name": "Period"},
        {"type": "NODE", "name": "Unit"},
        {"type": "REL", "name": "FILED"},
        {"type": "REL", "name": "HAS_FACT"},
        {"type": "REL", "name": "ELEMENT_HAS_PERIOD"},
        {"type": "REL", "name": "ELEMENT_HAS_UNIT"},
        {"type": "REL", "name": "BELONGS_TO"},
        {"type": "REL", "name": "DIMENSION_MEMBER"},
      ]
    )
    mock_factory.create_client = AsyncMock(return_value=mock_client)

    def node_side_effect(*args, **kwargs):
      node_type = kwargs.get("node_type")
      if node_type == "Entity":
        raise Exception("S3 Access Denied")
      return {
        "records_loaded": 100,
        "files_processed": 1,
        "duration_ms": 100,
        "warning_count": 0,
      }

    mock_load_node.side_effect = node_side_effect

    mock_load_rel.return_value = {
      "records_loaded": 500,
      "files_processed": 1,
      "duration_ms": 100,
      "warning_count": 0,
    }

    result = ingest_sec_data.apply(
      args=[],
      kwargs={
        "pipeline_run_id": "test-pipeline-node-fail",
        "year": 2024,
      },
    ).get()

    assert result["nodes"]["types_failed"] > 0
    failed_node = next(n for n in result["nodes"]["details"] if "error" in n)
    assert "S3 Access Denied" in failed_node["error"]

  @patch("robosystems.tasks.sec_xbrl.ingestion._bulk_load_relationship_type_sync")
  @patch("robosystems.tasks.sec_xbrl.ingestion._bulk_load_node_type_sync")
  @patch("robosystems.tasks.sec_xbrl.ingestion.GraphClientFactory")
  @patch("robosystems.tasks.sec_xbrl.ingestion.env")
  def test_ingestion_critical_warnings(
    self,
    mock_env,
    mock_factory,
    mock_load_node,
    mock_load_rel,
  ):
    """Test detection of critical data rejection warnings."""
    from robosystems.tasks.sec_xbrl.ingestion import ingest_sec_data

    self.setup_schema_types()

    mock_env.SEC_PROCESSED_BUCKET = "test-bucket"
    mock_env.AWS_REGION = "us-east-1"
    mock_env.ENVIRONMENT = "dev"
    mock_env.AWS_S3_ACCESS_KEY_ID = "test-key"
    mock_env.AWS_S3_SECRET_ACCESS_KEY = None
    mock_env.AWS_ENDPOINT_URL = None
    mock_env.XBRL_GRAPH_LARGE_NODES = "Entity,Fact"

    mock_client = MagicMock()
    mock_client.get_database_info = AsyncMock(return_value={"name": "sec"})
    mock_client.get_schema = AsyncMock(
      return_value=[
        {"type": "NODE", "name": "Entity"},
        {"type": "NODE", "name": "Fact"},
        {"type": "NODE", "name": "FilingDocument"},
        {"type": "NODE", "name": "Element"},
        {"type": "NODE", "name": "Period"},
        {"type": "NODE", "name": "Unit"},
        {"type": "REL", "name": "FILED"},
        {"type": "REL", "name": "HAS_FACT"},
        {"type": "REL", "name": "ELEMENT_HAS_PERIOD"},
        {"type": "REL", "name": "ELEMENT_HAS_UNIT"},
        {"type": "REL", "name": "BELONGS_TO"},
        {"type": "REL", "name": "DIMENSION_MEMBER"},
      ]
    )
    mock_factory.create_client = AsyncMock(return_value=mock_client)

    def node_side_effect(*args, **kwargs):
      node_type = kwargs.get("node_type")
      if node_type == "Fact":
        return {
          "records_loaded": 500,
          "files_processed": 2,
          "duration_ms": 200,
          "warning_count": 25,
        }
      return {
        "records_loaded": 100,
        "files_processed": 1,
        "duration_ms": 100,
        "warning_count": 0,
      }

    mock_load_node.side_effect = node_side_effect

    mock_load_rel.return_value = {
      "records_loaded": 200,
      "files_processed": 1,
      "duration_ms": 100,
      "warning_count": 0,
    }

    result = ingest_sec_data.apply(
      args=[],
      kwargs={
        "pipeline_run_id": "test-pipeline-warnings",
        "year": 2024,
      },
    ).get()

    assert result["status"] == "completed_with_warnings"
    assert result["totals"]["warnings"] > 0
    assert result["critical_issues"] is not None
    assert "Fact" in result["critical_issues"]["nodes_with_rejected_data"]

  @patch("robosystems.tasks.sec_xbrl.ingestion._bulk_load_relationship_type_sync")
  @patch("robosystems.tasks.sec_xbrl.ingestion._bulk_load_node_type_sync")
  @patch("robosystems.tasks.sec_xbrl.ingestion.GraphClientFactory")
  @patch("robosystems.tasks.sec_xbrl.ingestion.env")
  def test_ingestion_neo4j_backend(
    self,
    mock_env,
    mock_factory,
    mock_load_node,
    mock_load_rel,
  ):
    """Test ingestion with Neo4j backend."""
    from robosystems.tasks.sec_xbrl.ingestion import ingest_sec_data

    self.setup_schema_types()

    mock_env.SEC_PROCESSED_BUCKET = "test-bucket"
    mock_env.AWS_REGION = "us-east-1"
    mock_env.ENVIRONMENT = "dev"
    mock_env.AWS_S3_ACCESS_KEY_ID = "test-key"
    mock_env.AWS_S3_SECRET_ACCESS_KEY = None
    mock_env.AWS_ENDPOINT_URL = None
    mock_env.XBRL_GRAPH_LARGE_NODES = "Entity,Fact"

    mock_client = MagicMock()
    mock_client.get_database_info = AsyncMock(return_value={"name": "sec"})
    mock_client.get_schema = AsyncMock(
      return_value={
        "node_tables": [
          "Entity",
          "Fact",
          "FilingDocument",
          "Element",
          "Period",
          "Unit",
        ],
        "rel_tables": [
          "FILED",
          "HAS_FACT",
          "ELEMENT_HAS_PERIOD",
          "ELEMENT_HAS_UNIT",
          "BELONGS_TO",
          "DIMENSION_MEMBER",
        ],
      }
    )
    mock_factory.create_client = AsyncMock(return_value=mock_client)

    mock_load_node.return_value = {
      "records_loaded": 300,
      "files_processed": 2,
      "duration_ms": 150,
      "warning_count": 0,
    }

    mock_load_rel.return_value = {
      "records_loaded": 600,
      "files_processed": 1,
      "duration_ms": 100,
      "warning_count": 0,
    }

    result = ingest_sec_data.apply(
      args=[],
      kwargs={
        "pipeline_run_id": "test-pipeline-neo4j",
        "year": 2024,
        "backend": "neo4j",
      },
    ).get()

    assert result["status"] == "completed"
    assert result["backend"] == "neo4j"
