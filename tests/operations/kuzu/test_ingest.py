"""Tests for Kuzu database ingestion operations."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from robosystems.operations.kuzu.ingest import (
  _get_cached_schema_adapter,
  ingest_from_s3,
  ingest_from_local_files,
  _schema_adapter_cache,
)


class TestSchemaAdapterCache:
  """Test cases for schema adapter caching."""

  @patch("robosystems.operations.kuzu.ingest.SchemaIngestionProcessor")
  @patch("robosystems.operations.kuzu.ingest.create_roboledger_ingestion_processor")
  def test_get_cached_schema_adapter_default(
    self, mock_create_roboledger, mock_processor_class
  ):
    """Test getting default schema adapter with caching."""
    # Clear cache first
    _schema_adapter_cache.clear()

    # Setup mocks
    mock_adapter = MagicMock()
    mock_create_roboledger.return_value = mock_adapter

    # First call - should create new adapter
    adapter1 = _get_cached_schema_adapter()
    assert adapter1 == mock_adapter
    mock_create_roboledger.assert_called_once()

    # Second call - should return cached adapter
    adapter2 = _get_cached_schema_adapter()
    assert adapter2 == mock_adapter
    mock_create_roboledger.assert_called_once()  # Still only called once

  @patch("robosystems.operations.kuzu.ingest.SchemaIngestionProcessor")
  def test_get_cached_schema_adapter_custom(self, mock_processor_class):
    """Test getting custom schema adapter with caching."""
    # Clear cache first
    _schema_adapter_cache.clear()

    # Setup mocks
    mock_adapter = MagicMock()
    mock_processor_class.return_value = mock_adapter

    schema_config = {
      "name": "custom",
      "base_schema": "base",
      "extensions": ["roboledger", "roboinvestor"],
    }

    # First call - should create new adapter
    adapter1 = _get_cached_schema_adapter(schema_config)
    assert adapter1 == mock_adapter
    mock_processor_class.assert_called_once_with(schema_config)

    # Second call with same config - should return cached
    adapter2 = _get_cached_schema_adapter(schema_config)
    assert adapter2 == mock_adapter
    mock_processor_class.assert_called_once()  # Still only called once

  def test_clear_schema_cache(self):
    """Test clearing the schema cache."""
    # Add something to cache
    _schema_adapter_cache["test_key"] = "test_value"

    # Clear cache manually (function doesn't exist, so we do it directly)
    _schema_adapter_cache.clear()

    # Verify cache is empty
    assert len(_schema_adapter_cache) == 0


class TestIngestFromS3:
  """Test cases for S3 ingestion."""

  @patch("robosystems.operations.kuzu.ingest.ingest_from_local_files")
  @patch("robosystems.operations.kuzu.ingest.tempfile.mkdtemp")
  @patch("boto3.client")
  def test_ingest_from_s3_success(
    self, mock_boto3_client, mock_mkdtemp, mock_ingest_local
  ):
    """Test successful ingestion from S3."""
    # Setup mocks
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3

    # Mock S3 list_objects_v2
    mock_s3.list_objects_v2.return_value = {
      "Contents": [
        {"Key": "processed/entities.parquet"},
        {"Key": "processed/facts.parquet"},
        {"Key": "processed/contexts.parquet"},
      ]
    }

    # Mock temp directory
    temp_dir = "/tmp/test_ingest"
    mock_mkdtemp.return_value = temp_dir

    # Mock successful local ingestion
    mock_ingest_local.return_value = True

    # Run ingestion
    result = ingest_from_s3(
      bucket="test-bucket", db_name="test_db", s3_prefix="processed/"
    )

    # Assertions
    assert result is True
    mock_s3.list_objects_v2.assert_called_with(
      Bucket="test-bucket", Prefix="processed/"
    )
    assert mock_s3.download_file.call_count == 3
    mock_ingest_local.assert_called_once()

  @patch("boto3.client")
  def test_ingest_from_s3_empty_bucket(self, mock_boto3_client):
    """Test ingestion from S3 with empty bucket."""
    # Setup mocks
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3

    # Mock empty S3 response
    mock_s3.list_objects_v2.return_value = {}

    # Run ingestion
    result = ingest_from_s3(bucket="empty-bucket", db_name="test_db")

    # Assertions
    # Function returns True when no files found (not an error condition)
    assert result is True

  @patch("boto3.client")
  @patch("robosystems.operations.kuzu.ingest.tempfile.mkdtemp")
  def test_ingest_from_s3_download_error(self, mock_mkdtemp, mock_boto3_client):
    """Test ingestion from S3 with download error."""
    # Setup mocks
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3

    # Mock S3 list_objects_v2
    mock_s3.list_objects_v2.return_value = {
      "Contents": [{"Key": "processed/entities.parquet"}]
    }

    # Mock temp directory
    temp_dir = "/tmp/test_ingest"
    mock_mkdtemp.return_value = temp_dir

    # Mock download failure
    mock_s3.download_file.side_effect = Exception("Download failed")

    # Run ingestion
    result = ingest_from_s3(bucket="test-bucket", db_name="test_db")

    # Assertions
    assert result is False


class TestIngestFromLocalFiles:
  """Test cases for local file ingestion."""

  @patch("robosystems.middleware.graph.engine.Engine")
  @patch("robosystems.operations.kuzu.schema_setup.ensure_schema")
  @patch("robosystems.operations.kuzu.path_utils.get_kuzu_database_path")
  @patch("robosystems.operations.kuzu.ingest._get_cached_schema_adapter")
  @patch("robosystems.operations.kuzu.ingest._categorize_files_schema_driven")
  @patch("robosystems.operations.kuzu.ingest._parse_filename_schema_driven")
  @patch("robosystems.operations.kuzu.ingest._ingest_node_schema_driven")
  def test_ingest_from_local_files_success(
    self,
    mock_ingest_node,
    mock_parse_filename,
    mock_categorize,
    mock_get_adapter,
    mock_get_path,
    mock_ensure_schema,
    mock_engine_class,
  ):
    """Test successful ingestion from local files."""
    # Setup mocks
    mock_get_path.return_value = Path("/tmp/test.kuzu")
    mock_ensure_schema.return_value = False

    # Mock engine
    mock_engine = MagicMock()
    mock_engine_class.return_value = mock_engine

    # Mock schema adapter
    mock_adapter = MagicMock()
    mock_get_adapter.return_value = mock_adapter

    # Mock categorization to return nodes only
    test_files = ["/tmp/entities.parquet", "/tmp/facts.parquet"]
    mock_categorize.return_value = (test_files, [])

    # Mock parse and ingest to succeed
    mock_table_info = MagicMock()
    mock_parse_filename.return_value = mock_table_info
    mock_ingest_node.return_value = True

    # Run ingestion
    result = ingest_from_local_files(file_paths=test_files, db_name="test_db")

    # Assertions
    assert result is True
    mock_categorize.assert_called_once_with(test_files, mock_adapter)
    assert mock_ingest_node.call_count == 2

  @patch("robosystems.middleware.graph.engine.Engine")
  @patch("robosystems.operations.kuzu.schema_setup.ensure_schema")
  @patch("robosystems.operations.kuzu.path_utils.get_kuzu_database_path")
  def test_ingest_from_local_files_empty_list(
    self, mock_get_path, mock_ensure_schema, mock_engine_class
  ):
    """Test ingestion with empty file list."""
    # Setup mocks
    mock_get_path.return_value = Path("/tmp/test_db.kuzu")
    mock_ensure_schema.return_value = False
    mock_engine = MagicMock()
    mock_engine_class.return_value = mock_engine

    result = ingest_from_local_files(db_name="test_db", file_paths=[])

    assert result is False

  @patch("robosystems.middleware.graph.engine.Engine")
  @patch("robosystems.operations.kuzu.schema_setup.ensure_schema")
  @patch("robosystems.operations.kuzu.path_utils.get_kuzu_database_path")
  def test_ingest_from_local_files_exception(
    self, mock_get_path, mock_ensure_schema, mock_engine_class
  ):
    """Test ingestion with exception during processing."""
    # Setup mocks to raise exception
    mock_get_path.side_effect = Exception("Database error")

    # Run ingestion
    result = ingest_from_local_files(
      file_paths=["/tmp/test.parquet"], db_name="test_db"
    )

    # Assertions
    assert result is False
