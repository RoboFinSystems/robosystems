"""Tests for LadybugDB database ingestion operations."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from robosystems.operations.lbug.ingest import (
  _get_cached_schema_adapter,
  ingest_from_s3,
  ingest_from_local_files,
  _schema_adapter_cache,
  _categorize_files_schema_driven,
  _parse_filename_schema_driven,
  _ingest_node_schema_driven,
  _ingest_relationship_schema_driven,
  _is_valid_identifier,
  _sanitize_parameter_name,
  _is_global_relationship_schema_driven,
  _is_global_entity_schema_driven,
  _map_arrow_to_lbug_type,
)


class TestSchemaAdapterCache:
  """Test cases for schema adapter caching."""

  @patch("robosystems.operations.lbug.ingest.XBRLSchemaConfigGenerator")
  @patch("robosystems.operations.lbug.ingest.create_roboledger_ingestion_processor")
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

  @patch("robosystems.operations.lbug.ingest.XBRLSchemaConfigGenerator")
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

  @patch("robosystems.operations.lbug.ingest.ingest_from_local_files")
  @patch("robosystems.operations.lbug.ingest.tempfile.mkdtemp")
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
  @patch("robosystems.operations.lbug.ingest.tempfile.mkdtemp")
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

  @patch("robosystems.graph_api.core.ladybug.engine.Engine")
  @patch("robosystems.operations.lbug.schema_setup.ensure_schema")
  @patch("robosystems.operations.lbug.path_utils.get_lbug_database_path")
  @patch("robosystems.operations.lbug.ingest._get_cached_schema_adapter")
  @patch("robosystems.operations.lbug.ingest._categorize_files_schema_driven")
  @patch("robosystems.operations.lbug.ingest._parse_filename_schema_driven")
  @patch("robosystems.operations.lbug.ingest._ingest_node_schema_driven")
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
    mock_get_path.return_value = Path("/tmp/test.lbug")
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

  @patch("robosystems.graph_api.core.ladybug.engine.Engine")
  @patch("robosystems.operations.lbug.schema_setup.ensure_schema")
  @patch("robosystems.operations.lbug.path_utils.get_lbug_database_path")
  def test_ingest_from_local_files_empty_list(
    self, mock_get_path, mock_ensure_schema, mock_engine_class
  ):
    """Test ingestion with empty file list."""
    # Setup mocks
    mock_get_path.return_value = Path("/tmp/test_db.lbug")
    mock_ensure_schema.return_value = False
    mock_engine = MagicMock()
    mock_engine_class.return_value = mock_engine

    result = ingest_from_local_files(db_name="test_db", file_paths=[])

    assert result is False

  @patch("robosystems.graph_api.core.ladybug.engine.Engine")
  @patch("robosystems.operations.lbug.schema_setup.ensure_schema")
  @patch("robosystems.operations.lbug.path_utils.get_lbug_database_path")
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


class TestUtilityFunctions:
  """Test utility functions for ingestion."""

  def test_is_valid_identifier(self):
    """Test identifier validation."""
    # Valid identifiers
    assert _is_valid_identifier("valid_name") is True
    assert _is_valid_identifier("name123") is True
    assert _is_valid_identifier("_underscore") is True
    assert _is_valid_identifier("camelCase") is True

    # Invalid identifiers
    assert _is_valid_identifier("") is False
    assert _is_valid_identifier("123starts_with_number") is False
    assert _is_valid_identifier("has spaces") is False
    assert _is_valid_identifier("has-dashes") is False
    assert _is_valid_identifier("has.dots") is False
    assert _is_valid_identifier("has@symbols") is False

  def test_sanitize_parameter_name(self):
    """Test parameter name sanitization."""
    assert _sanitize_parameter_name("valid_name") == "valid_name"
    assert _sanitize_parameter_name("name with spaces") == "name_with_spaces"
    assert _sanitize_parameter_name("name-with-dashes") == "name_with_dashes"
    assert _sanitize_parameter_name("name.with.dots") == "name_with_dots"
    assert _sanitize_parameter_name("123starts_with_number") == "_123starts_with_number"

  def test_is_global_relationship_schema_driven(self):
    """Test global relationship detection."""
    # Global relationships (defined in BASE_RELATIONSHIPS)
    assert _is_global_relationship_schema_driven("ENTITY_EVOLVED_FROM") is True
    assert _is_global_relationship_schema_driven("ENTITY_OWNS_ENTITY") is True
    assert _is_global_relationship_schema_driven("ELEMENT_HAS_LABEL") is True

    # Non-global relationships
    assert _is_global_relationship_schema_driven("EMPLOYS") is False
    assert _is_global_relationship_schema_driven("entity_facts") is False
    assert _is_global_relationship_schema_driven("custom_relationship") is False

  def test_is_global_entity_schema_driven(self):
    """Test global entity detection."""
    # Global entities (defined in BASE_NODES)
    assert _is_global_entity_schema_driven("Entity") is True
    assert _is_global_entity_schema_driven("Period") is True
    assert _is_global_entity_schema_driven("Unit") is True
    assert _is_global_entity_schema_driven("Element") is True

    # Non-global entities
    assert _is_global_entity_schema_driven("Person") is False
    assert _is_global_entity_schema_driven("entity_facts") is False
    assert _is_global_entity_schema_driven("custom_table") is False

  def test_map_arrow_to_lbug_type(self):
    """Test Arrow to LadybugDB type mapping."""
    # Basic types
    assert _map_arrow_to_lbug_type("int64") == "INT64"
    assert _map_arrow_to_lbug_type("float64") == "DOUBLE"
    assert _map_arrow_to_lbug_type("string") == "STRING"
    assert _map_arrow_to_lbug_type("bool") == "BOOLEAN"
    assert _map_arrow_to_lbug_type("date32[day]") == "DATE"
    assert _map_arrow_to_lbug_type("timestamp[ms]") == "TIMESTAMP"

    # Complex types (currently mapped to basic types)
    assert (
      _map_arrow_to_lbug_type("list<int64>") == "INT64"
    )  # Lists not fully supported, maps to base type
    assert (
      _map_arrow_to_lbug_type("struct<name:string,age:int64>") == "STRING"
    )  # Structs default to STRING

    # Unknown type
    assert _map_arrow_to_lbug_type("unknown_type") == "STRING"


class TestSchemaDrivenIngestion:
  """Test schema-driven ingestion functions."""

  @patch("robosystems.operations.lbug.ingest._get_cached_schema_adapter")
  def test_categorize_files_schema_driven(self, mock_get_adapter):
    """Test file categorization."""
    # Setup mock adapter
    mock_adapter = MagicMock()
    # Mock is_relationship_file to return True for EMPLOYS.parquet
    mock_adapter.is_relationship_file.side_effect = (
      lambda path: "EMPLOYS.parquet" in path
    )
    mock_get_adapter.return_value = mock_adapter

    test_files = [
      "/tmp/Entity.parquet",
      "/tmp/Person.parquet",
      "/tmp/EMPLOYS.parquet",
      "/tmp/unknown.parquet",
    ]

    node_files, relationship_files = _categorize_files_schema_driven(
      test_files, mock_adapter
    )

    assert len(node_files) == 3  # Entity, Person, unknown
    assert len(relationship_files) == 1  # EMPLOYS
    assert any("EMPLOYS.parquet" in f for f in relationship_files)

  @patch("robosystems.operations.lbug.ingest._get_cached_schema_adapter")
  def test_parse_filename_schema_driven(self, mock_get_adapter):
    """Test filename parsing."""
    # Setup mock adapter
    mock_adapter = MagicMock()
    mock_adapter.get_table_name_from_file.return_value = "Entity"

    # Mock table info object with attributes
    mock_table_info = MagicMock()
    mock_table_info.is_relationship = False
    mock_table_info.column_types = {"id": "STRING", "name": "STRING"}
    mock_adapter.get_table_info.return_value = mock_table_info

    mock_get_adapter.return_value = mock_adapter

    result = _parse_filename_schema_driven("Entity_2024.parquet", mock_adapter)

    assert result is not None
    assert result["table_name"] == "Entity"
    assert result["is_relationship"] is False
    mock_adapter.get_table_name_from_file.assert_called_once_with("Entity_2024.parquet")
    mock_adapter.get_table_info.assert_called_once_with("Entity")

  @patch("robosystems.operations.lbug.ingest._get_cached_schema_adapter")
  @patch("robosystems.operations.lbug.ingest._copy_node_data_schema_driven")
  def test_ingest_node_schema_driven(self, mock_copy_data, mock_get_adapter):
    """Test node ingestion."""
    # Setup mocks
    mock_engine = MagicMock()
    mock_adapter = MagicMock()
    mock_table_info = {
      "table_name": "Entity",
      "table_info": {
        "column_types": {"id": "STRING", "name": "STRING"},
        "primary_key": "id",
      },
    }

    mock_get_adapter.return_value = mock_adapter
    mock_copy_data.return_value = True

    # Mock table creation
    with patch(
      "robosystems.operations.lbug.ingest._create_table_from_schema", return_value=True
    ) as mock_create_table:
      result = _ingest_node_schema_driven(
        mock_engine, "/tmp/Entity.parquet", mock_table_info, mock_adapter
      )

      assert result is True
      # Verify the functions were called (focus on coverage, not exact parameters)
      mock_create_table.assert_called_once()
      mock_copy_data.assert_called_once()

  @patch("robosystems.operations.lbug.ingest._get_cached_schema_adapter")
  @patch("robosystems.operations.lbug.ingest._copy_relationship_data_schema_driven")
  def test_ingest_relationship_schema_driven(self, mock_copy_data, mock_get_adapter):
    """Test relationship ingestion."""
    # Setup mocks
    mock_engine = MagicMock()
    mock_adapter = MagicMock()
    mock_table_info = {
      "table_name": "EMPLOYS",
      "table_info": {"column_types": {"from_id": "STRING", "to_id": "STRING"}},
    }

    mock_get_adapter.return_value = mock_adapter
    mock_copy_data.return_value = True

    # Mock table creation
    with patch(
      "robosystems.operations.lbug.ingest._create_relationship_table_from_schema",
      return_value=True,
    ) as mock_create_table:
      result = _ingest_relationship_schema_driven(
        mock_engine, "/tmp/EMPLOYS.parquet", mock_table_info, mock_adapter
      )

      assert result is True
      # Verify the functions were called (focus on coverage, not exact parameters)
      mock_create_table.assert_called_once()
      mock_copy_data.assert_called_once()


class TestS3IngestionEdgeCases:
  """Test edge cases for S3 ingestion."""

  @patch("boto3.client")
  def test_ingest_from_s3_authentication_error(self, mock_boto3_client):
    """Test S3 ingestion with authentication error."""
    mock_boto3_client.side_effect = Exception("Authentication failed")

    result = ingest_from_s3(bucket="test-bucket", db_name="test_db")

    assert result is False

  @patch("boto3.client")
  @patch("robosystems.operations.lbug.ingest.tempfile.mkdtemp")
  def test_ingest_from_s3_partial_download_failure(
    self, mock_mkdtemp, mock_boto3_client
  ):
    """Test S3 ingestion with partial download failure."""
    # Setup mocks
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3

    # Mock S3 list with multiple files
    mock_s3.list_objects_v2.return_value = {
      "Contents": [
        {"Key": "processed/file1.parquet"},
        {"Key": "processed/file2.parquet"},
        {"Key": "processed/file3.parquet"},
      ]
    }

    # Mock temp directory
    temp_dir = "/tmp/test_ingest"
    mock_mkdtemp.return_value = temp_dir

    # Mock download to fail on second file
    def download_side_effect(*args, **kwargs):
      if "file2.parquet" in args[1]:
        raise Exception("Download failed for file2")
      return None

    mock_s3.download_file.side_effect = download_side_effect

    result = ingest_from_s3(bucket="test-bucket", db_name="test_db")

    assert result is False


class TestIngestionErrorHandling:
  """Test error handling in ingestion operations."""

  @patch("robosystems.graph_api.core.ladybug.engine.Engine")
  @patch("robosystems.operations.lbug.schema_setup.ensure_schema")
  @patch("robosystems.operations.lbug.path_utils.get_lbug_database_path")
  def test_ingest_from_local_files_schema_error(
    self, mock_get_path, mock_ensure_schema, mock_engine_class
  ):
    """Test ingestion with schema processing error."""
    # Setup mocks
    mock_get_path.return_value = Path("/tmp/test.lbug")
    mock_ensure_schema.return_value = False
    mock_engine = MagicMock()
    mock_engine_class.return_value = mock_engine

    with patch(
      "robosystems.operations.lbug.ingest._get_cached_schema_adapter",
      side_effect=Exception("Schema error"),
    ):
      result = ingest_from_local_files(
        file_paths=["/tmp/test.parquet"], db_name="test_db"
      )

      assert result is False

  @patch("robosystems.graph_api.core.ladybug.engine.Engine")
  @patch("robosystems.operations.lbug.schema_setup.ensure_schema")
  @patch("robosystems.operations.lbug.path_utils.get_lbug_database_path")
  @patch("robosystems.operations.lbug.ingest._get_cached_schema_adapter")
  def test_ingest_from_local_files_table_creation_failure(
    self, mock_get_adapter, mock_get_path, mock_ensure_schema, mock_engine_class
  ):
    """Test ingestion with table creation failure."""
    # Setup mocks
    mock_get_path.return_value = Path("/tmp/test.lbug")
    mock_ensure_schema.return_value = False
    mock_engine = MagicMock()
    mock_engine_class.return_value = mock_engine

    mock_adapter = MagicMock()
    mock_get_adapter.return_value = mock_adapter

    # Mock categorization and parsing
    with (
      patch(
        "robosystems.operations.lbug.ingest._categorize_files_schema_driven",
        return_value=(["/tmp/test.parquet"], []),
      ),
      patch(
        "robosystems.operations.lbug.ingest._parse_filename_schema_driven"
      ) as mock_parse,
      patch(
        "robosystems.operations.lbug.ingest._create_table_from_schema",
        side_effect=Exception("Table creation failed"),
      ),
    ):
      mock_table_info = MagicMock()
      mock_parse.return_value = mock_table_info

      result = ingest_from_local_files(
        file_paths=["/tmp/test.parquet"], db_name="test_db"
      )

      assert result is False
