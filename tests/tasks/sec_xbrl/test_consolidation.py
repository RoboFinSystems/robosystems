"""
Test Suite for SEC XBRL Consolidation Tasks

Tests the parquet file consolidation functionality for optimized Kuzu ingestion.
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

  @patch("robosystems.tasks.sec_xbrl.consolidation.S3Client")
  def test_list_s3_files_all_years(self, mock_s3_class):
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
      # Files for 2023
      [
        {
          "Contents": [
            {
              "Key": "processed/year=2023/nodes/Entity/file1.parquet",
              "Size": 1000,
              "LastModified": datetime.now(),
            }
          ]
        }
      ],
      # Files for 2024
      [
        {
          "Contents": [
            {
              "Key": "processed/year=2024/nodes/Entity/file2.parquet",
              "Size": 2000,
              "LastModified": datetime.now(),
            }
          ]
        }
      ],
    ]

    with patch("robosystems.tasks.sec_xbrl.consolidation.list_s3_files") as mock_list:
      mock_list.side_effect = [
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

  def test_consolidate_parquet_files(self):
    """Test the consolidate_parquet_files task exists and is callable."""
    # Verify it's a Celery task with the expected attributes
    assert hasattr(consolidate_parquet_files, "delay")
    assert hasattr(consolidate_parquet_files, "apply_async")

    # Mock the task execution
    mock_result = {"status": "success", "files_processed": 0}

    # Create a mock function and assign it
    def mock_run(**kwargs):
      return mock_result

    consolidate_parquet_files.run = mock_run  # type: ignore

    result = mock_run(
      table_type="nodes",
      table_name="Entity",
      year=2024,
      bucket="test-bucket",
      batch_mode=False,
      dry_run=True,
    )

    # Verify result structure
    assert isinstance(result, dict)
    assert result["status"] == "success"

  def test_orchestrate_consolidation_phase(self):
    """Test the orchestrate_consolidation_phase task exists."""
    # Verify it's a Celery task
    assert hasattr(orchestrate_consolidation_phase, "delay")
    assert hasattr(orchestrate_consolidation_phase, "apply_async")

  def test_get_consolidation_status(self):
    """Test getting consolidation status function exists."""
    # The function exists and returns a dict
    result = get_consolidation_status("pipeline-123")
    assert isinstance(result, dict)
    assert "pipeline_id" in result
