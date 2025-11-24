"""
Test Suite for SEC XBRL Consolidation Tasks

Tests the parquet file consolidation functionality for optimized LadybugDB ingestion.
"""

from unittest.mock import MagicMock, patch
from datetime import datetime
import pyarrow as pa

from robosystems.tasks.sec_xbrl.consolidation import (
  get_enforced_schema,
  list_s3_files,
  list_s3_files_all_years,
  create_file_batches,
  consolidate_parquet_files,
  orchestrate_consolidation_phase,
  get_consolidation_status,
)


class TestSchemaEnforcement:
  """Test schema enforcement for critical columns."""

  def test_get_enforced_schema_entity(self):
    """Test schema enforcement for Entity table."""
    # Create original schema with wrong types
    original_schema = pa.schema(
      [
        ("id", pa.string()),
        ("ein", pa.int32()),  # Should be string
        ("cik", pa.int64()),  # Should be string
        ("name", pa.string()),
        ("ticker", pa.int32()),  # Should be string
      ]
    )

    enforced_schema = get_enforced_schema("Entity", original_schema)

    # Verify critical fields are enforced as strings
    assert enforced_schema.field("ein").type == pa.string()
    assert enforced_schema.field("cik").type == pa.string()
    assert enforced_schema.field("ticker").type == pa.string()
    assert enforced_schema.field("name").type == pa.string()  # Unchanged

  def test_get_enforced_schema_identifier_patterns(self):
    """Test that identifier patterns are enforced as strings."""
    original_schema = pa.schema(
      [
        ("some_uri", pa.int32()),  # Should be string (ends with _uri)
        ("entity_id", pa.int64()),  # Should be string (ends with _id)
        ("sic_code", pa.int32()),  # Should be string (ends with _code)
        ("regular_field", pa.int32()),  # Should remain unchanged
      ]
    )

    enforced_schema = get_enforced_schema("CustomTable", original_schema)

    assert enforced_schema.field("some_uri").type == pa.string()
    assert enforced_schema.field("entity_id").type == pa.string()
    assert enforced_schema.field("sic_code").type == pa.string()
    assert enforced_schema.field("regular_field").type == pa.int32()  # Unchanged

  def test_get_enforced_schema_preserves_other_types(self):
    """Test that non-critical fields preserve their types."""
    original_schema = pa.schema(
      [
        ("id", pa.string()),
        ("amount", pa.float64()),
        ("count", pa.int32()),
        ("is_active", pa.bool_()),
        ("created_at", pa.timestamp("us")),
      ]
    )

    enforced_schema = get_enforced_schema("Fact", original_schema)

    # All fields should remain unchanged for Fact table
    assert enforced_schema.field("amount").type == pa.float64()
    assert enforced_schema.field("count").type == pa.int32()
    assert enforced_schema.field("is_active").type == pa.bool_()
    assert enforced_schema.field("created_at").type == pa.timestamp("us")


class TestConsolidationHelpers:
  """Test consolidation helper functions."""

  def test_create_file_batches_small_files(self):
    """Test batching small files."""
    files = [
      {
        "Key": f"file{i}.parquet",
        "Size": 10 * 1024 * 1024,
        "LastModified": datetime.now(),
      }
      for i in range(10)
    ]

    batches = create_file_batches(files)

    # All files should fit in one batch (100MB total < MAX_MEMORY_PER_BATCH)
    assert len(batches) >= 1
    total_files = sum(len(batch) for batch in batches)
    assert total_files == 10

  def test_create_file_batches_large_files(self):
    """Test batching when files exceed memory limit."""
    files = [
      {
        "Key": f"file{i}.parquet",
        "Size": 300 * 1024 * 1024,
        "LastModified": datetime.now(),
      }
      for i in range(5)
    ]

    batches = create_file_batches(files)

    # Each file should be in its own batch due to size
    assert len(batches) >= 3  # Depends on MAX_MEMORY_PER_BATCH

  @patch("robosystems.tasks.sec_xbrl.consolidation.S3Client")
  def test_list_s3_files(self, mock_s3_class):
    """Test listing S3 files."""
    mock_s3_client = MagicMock()
    mock_s3_class.return_value = mock_s3_client

    # Mock paginator
    mock_paginator = MagicMock()
    mock_s3_client.s3_client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [
      {
        "Contents": [
          {"Key": "file1.parquet", "Size": 1000, "LastModified": datetime.now()},
          {"Key": "file2.parquet", "Size": 2000, "LastModified": datetime.now()},
          {
            "Key": "file3.txt",
            "Size": 500,
            "LastModified": datetime.now(),
          },  # Should be filtered
        ]
      }
    ]

    files = list_s3_files(mock_s3_client, "test-bucket", "test-prefix")

    assert len(files) == 2  # Only parquet files
    assert all(f["Key"].endswith(".parquet") for f in files)

  @patch("robosystems.tasks.sec_xbrl.consolidation.list_s3_files")
  @patch("robosystems.tasks.sec_xbrl.consolidation.S3Client")
  def test_list_s3_files_all_years(self, mock_s3_class, mock_list_s3):
    """Test listing files across all years."""
    mock_s3_client = MagicMock()
    mock_s3_class.return_value = mock_s3_client

    # Mock year discovery
    mock_paginator = MagicMock()
    mock_s3_client.s3_client.get_paginator.return_value = mock_paginator

    # First call returns year directories
    # Subsequent calls return files for each year
    mock_paginator.paginate.side_effect = [
      # Year folders listing
      [
        {
          "CommonPrefixes": [
            {"Prefix": "processed/year=2023/"},
            {"Prefix": "processed/year=2024/"},
          ]
        }
      ],
    ]

    mock_list_s3.side_effect = [
      [{"Key": "file1.parquet", "Size": 1000}],
      [{"Key": "file2.parquet", "Size": 2000}],
    ]

    files = list_s3_files_all_years(mock_s3_client, "test-bucket", "nodes", "Entity")

    assert len(files) == 2
    assert sum(f["Size"] for f in files) == 3000


# Note: The Celery task itself is tested via integration tests.
# The helper functions above provide adequate coverage of the core logic.


class TestConsolidationExecution:
  """Test actual consolidation execution."""

  def test_consolidate_parquet_files_is_task(self):
    """Test that consolidate_parquet_files is a Celery task."""
    assert hasattr(consolidate_parquet_files, "delay")
    assert hasattr(consolidate_parquet_files, "apply_async")

  def test_orchestrate_consolidation_phase_is_task(self):
    """Test that orchestrate_consolidation_phase is a Celery task."""
    assert hasattr(orchestrate_consolidation_phase, "delay")
    assert hasattr(orchestrate_consolidation_phase, "apply_async")

  def test_get_consolidation_status(self):
    """Test getting consolidation status function exists."""
    result = get_consolidation_status("pipeline-123")
    assert isinstance(result, dict)
    assert "pipeline_id" in result


class TestConsolidateParquetFilesTask:
  """Integration tests for consolidate_parquet_files Celery task."""

  @patch("robosystems.tasks.sec_xbrl.consolidation.stream_consolidate_batch")
  @patch("robosystems.tasks.sec_xbrl.consolidation.create_file_batches")
  @patch("robosystems.tasks.sec_xbrl.consolidation.list_s3_files")
  @patch("robosystems.tasks.sec_xbrl.consolidation.S3Client")
  def test_successful_consolidation(
    self, mock_s3_class, mock_list_files, mock_create_batches, mock_stream_consolidate
  ):
    """Test successful parquet file consolidation."""
    mock_s3 = MagicMock()
    mock_s3_class.return_value = mock_s3

    source_files = [
      {
        "Key": f"file{i}.parquet",
        "Size": 100 * 1024 * 1024,
        "LastModified": datetime.now(),
      }
      for i in range(10)
    ]

    def list_files_side_effect(s3_client, bucket, prefix):
      if "consolidated" in prefix:
        return []
      return source_files

    mock_list_files.side_effect = list_files_side_effect

    batches = [[source_files[0], source_files[1]], [source_files[2]]]
    mock_create_batches.return_value = batches

    mock_stream_consolidate.side_effect = [1000, 500]

    result = consolidate_parquet_files.apply(
      kwargs={
        "table_type": "nodes",
        "table_name": "Entity",
        "year": 2024,
        "bucket": "test-bucket",
        "pipeline_id": "test-pipeline",
      }
    ).get()

    assert result["status"] == "success"
    assert result["table"] == "Entity"
    assert result["year"] == 2024
    assert result["source_files"] == 10
    assert result["consolidated_files"] == 2
    assert result["total_rows"] == 1500
    assert "duration_seconds" in result
    assert "consolidation_ratio" in result

  @patch("robosystems.tasks.sec_xbrl.consolidation.list_s3_files")
  @patch("robosystems.tasks.sec_xbrl.consolidation.S3Client")
  def test_skipped_too_few_files(self, mock_s3_class, mock_list_files):
    """Test consolidation skipped when too few files."""
    mock_s3 = MagicMock()
    mock_s3_class.return_value = mock_s3

    mock_list_files.return_value = []

    result = consolidate_parquet_files.apply(
      kwargs={
        "table_type": "nodes",
        "table_name": "Entity",
        "year": 2024,
        "bucket": "test-bucket",
      }
    ).get()

    assert result["status"] == "skipped"
    assert result["table"] == "Entity"
    assert result["year"] == 2024
    assert "Only 0 files" in result["reason"]

  @patch("robosystems.tasks.sec_xbrl.consolidation.list_s3_files")
  @patch("robosystems.tasks.sec_xbrl.consolidation.S3Client")
  def test_already_consolidated(self, mock_s3_class, mock_list_files):
    """Test consolidation skipped when already consolidated."""
    mock_s3 = MagicMock()
    mock_s3_class.return_value = mock_s3

    source_files = [
      {"Key": f"file{i}.parquet", "Size": 1024, "LastModified": datetime.now()}
      for i in range(100)
    ]
    consolidated_files = [
      {
        "Key": "consolidated.parquet",
        "Size": 100 * 1024,
        "LastModified": datetime.now(),
      }
    ]

    mock_list_files.side_effect = [source_files, consolidated_files]

    result = consolidate_parquet_files.apply(
      kwargs={
        "table_type": "nodes",
        "table_name": "Entity",
        "year": 2024,
        "bucket": "test-bucket",
      }
    ).get()

    assert result["status"] == "already_consolidated"
    assert result["table"] == "Entity"
    assert result["year"] == 2024
    assert result["source_files"] == 100
    assert result["consolidated_files"] == 1

  @patch("robosystems.tasks.sec_xbrl.consolidation.stream_consolidate_batch")
  @patch("robosystems.tasks.sec_xbrl.consolidation.create_file_batches")
  @patch("robosystems.tasks.sec_xbrl.consolidation.list_s3_files")
  @patch("robosystems.tasks.sec_xbrl.consolidation.S3Client")
  def test_partial_batch_failure(
    self, mock_s3_class, mock_list_files, mock_create_batches, mock_stream_consolidate
  ):
    """Test consolidation continues when one batch fails."""
    mock_s3 = MagicMock()
    mock_s3_class.return_value = mock_s3

    source_files = [
      {"Key": f"file{i}.parquet", "Size": 1024, "LastModified": datetime.now()}
      for i in range(6)
    ]
    mock_list_files.side_effect = [source_files, []]

    batches = [[source_files[0]], [source_files[1]], [source_files[2]]]
    mock_create_batches.return_value = batches

    mock_stream_consolidate.side_effect = [1000, Exception("S3 error"), 500]

    result = consolidate_parquet_files.apply(
      kwargs={
        "table_type": "nodes",
        "table_name": "Entity",
        "year": 2024,
        "bucket": "test-bucket",
      }
    ).get()

    assert result["status"] == "success"
    assert result["consolidated_files"] == 2
    assert result["total_rows"] == 1500

  @patch("robosystems.tasks.sec_xbrl.consolidation.list_s3_files")
  @patch("robosystems.tasks.sec_xbrl.consolidation.S3Client")
  def test_s3_listing_failure_after_retries(self, mock_s3_class, mock_list_files):
    """Test consolidation returns failed status after exhausting retries."""
    from celery.exceptions import Retry
    import pytest

    mock_s3 = MagicMock()
    mock_s3_class.return_value = mock_s3

    mock_list_files.side_effect = Exception("S3 connection failed")

    with pytest.raises(Retry):
      consolidate_parquet_files.apply(
        kwargs={
          "table_type": "nodes",
          "table_name": "Entity",
          "year": 2024,
          "bucket": "test-bucket",
        }
      ).get()

  @patch("robosystems.tasks.sec_xbrl.consolidation.stream_consolidate_batch")
  @patch("robosystems.tasks.sec_xbrl.consolidation.create_file_batches")
  @patch("robosystems.tasks.sec_xbrl.consolidation.list_s3_files")
  @patch("robosystems.tasks.sec_xbrl.consolidation.S3Client")
  def test_zero_rows_consolidated(
    self, mock_s3_class, mock_list_files, mock_create_batches, mock_stream_consolidate
  ):
    """Test successful completion with zero rows consolidated."""
    mock_s3 = MagicMock()
    mock_s3_class.return_value = mock_s3

    source_files = [
      {"Key": "file.parquet", "Size": 1024, "LastModified": datetime.now()}
    ]
    mock_list_files.side_effect = [source_files, []]

    batches = [[source_files[0]]]
    mock_create_batches.return_value = batches

    mock_stream_consolidate.return_value = 0

    result = consolidate_parquet_files.apply(
      kwargs={
        "table_type": "nodes",
        "table_name": "Entity",
        "year": 2024,
        "bucket": "test-bucket",
      }
    ).get()

    assert result["status"] == "success"
    assert result["total_rows"] == 0
    assert result["consolidated_files"] == 1


class TestOrchestrateConsolidationPhaseTask:
  """Integration tests for orchestrate_consolidation_phase Celery task."""

  @patch("celery.group")
  @patch("robosystems.tasks.sec_xbrl.consolidation.get_schema_types")
  def test_successful_orchestration(self, mock_get_schema, mock_group):
    """Test successful consolidation orchestration."""
    mock_get_schema.return_value = (
      ["Entity", "Report"],
      ["REPORT_HAS_FACT", "ENTITY_FILED_REPORT"],
    )

    mock_job = MagicMock()
    mock_job.id = "job-12345"
    mock_group_instance = MagicMock()
    mock_group_instance.apply_async.return_value = mock_job
    mock_group.return_value = mock_group_instance

    result = orchestrate_consolidation_phase.apply(
      kwargs={
        "years": [2023, 2024],
        "bucket": "test-bucket",
        "pipeline_id": "test-pipeline",
      }
    ).get()

    assert result["status"] == "started"
    assert result["phase"] == "consolidation"
    assert result["job_id"] == "job-12345"
    assert result["pipeline_id"] == "test-pipeline"
    assert result["years"] == [2023, 2024]
    assert result["total_tasks"] == 8
    assert result["tasks_per_year"] == 4
    assert result["bucket"] == "test-bucket"

  @patch("celery.group")
  @patch("robosystems.tasks.sec_xbrl.consolidation.get_schema_types")
  def test_orchestration_with_default_bucket(self, mock_get_schema, mock_group):
    """Test orchestration uses default bucket when not provided."""
    mock_get_schema.return_value = (["Entity"], ["REPORT_HAS_FACT"])

    mock_job = MagicMock()
    mock_job.id = "job-12345"
    mock_group_instance = MagicMock()
    mock_group_instance.apply_async.return_value = mock_job
    mock_group.return_value = mock_group_instance

    result = orchestrate_consolidation_phase.apply(
      kwargs={
        "years": [2024],
      }
    ).get()

    assert result["status"] == "started"
    assert "bucket" in result
    assert result["total_tasks"] == 2

  @patch("celery.group")
  @patch("robosystems.tasks.sec_xbrl.consolidation.get_schema_types")
  def test_orchestration_single_year(self, mock_get_schema, mock_group):
    """Test orchestration for single year."""
    mock_get_schema.return_value = (["Entity", "Report", "Fact"], ["REPORT_HAS_FACT"])

    mock_job = MagicMock()
    mock_job.id = "job-12345"
    mock_group_instance = MagicMock()
    mock_group_instance.apply_async.return_value = mock_job
    mock_group.return_value = mock_group_instance

    result = orchestrate_consolidation_phase.apply(
      kwargs={
        "years": [2024],
        "bucket": "test-bucket",
      }
    ).get()

    assert result["status"] == "started"
    assert result["years"] == [2024]
    assert result["total_tasks"] == 4
    assert result["tasks_per_year"] == 4

  @patch("celery.group")
  @patch("robosystems.tasks.sec_xbrl.consolidation.get_schema_types")
  def test_orchestration_creates_correct_task_count(self, mock_get_schema, mock_group):
    """Test orchestration creates correct number of tasks."""
    node_types = ["Entity", "Report", "Fact"]
    rel_types = ["REPORT_HAS_FACT", "ENTITY_FILED_REPORT"]
    mock_get_schema.return_value = (node_types, rel_types)

    mock_job = MagicMock()
    mock_job.id = "job-12345"
    mock_group_instance = MagicMock()
    mock_group_instance.apply_async.return_value = mock_job
    mock_group.return_value = mock_group_instance

    years = [2022, 2023, 2024]
    result = orchestrate_consolidation_phase.apply(
      kwargs={
        "years": years,
        "bucket": "test-bucket",
      }
    ).get()

    expected_tasks = len(years) * (len(node_types) + len(rel_types))

    assert result["total_tasks"] == expected_tasks
    assert result["tasks_per_year"] == len(node_types) + len(rel_types)
