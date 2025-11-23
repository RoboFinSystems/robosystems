"""
Unit tests for SECXBRLPipeline operations.

Tests the core pipeline functionality without Celery task orchestration.
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from robosystems.operations.pipelines import SECXBRLPipeline


class TestSECXBRLPipelineOperations:
  """Test the SEC XBRL Pipeline operations."""

  @pytest.fixture
  def mock_env(self):
    """Mock environment configuration."""
    with patch("robosystems.operations.pipelines.sec_xbrl_filings.env") as mock:
      mock.VALKEY_URL = "redis://localhost:6379"
      mock.SEC_RAW_BUCKET = "test-raw-bucket"
      mock.SEC_PROCESSED_BUCKET = "test-processed-bucket"
      mock.get_s3_config.return_value = {
        "endpoint_url": "http://localhost:4566",
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
        "region_name": "us-east-1",
      }
      yield mock

  @pytest.fixture
  def mock_redis(self):
    """Mock Redis client."""
    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.redis.from_url"
    ) as mock:
      redis_instance = MagicMock()
      mock.return_value = redis_instance
      yield redis_instance

  @pytest.fixture
  def mock_s3(self):
    """Mock S3 client."""
    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.boto3.client"
    ) as mock:
      s3_instance = MagicMock()
      mock.return_value = s3_instance
      yield mock

  @pytest.fixture
  def mock_sec_client(self):
    """Mock SEC client."""
    with patch("robosystems.operations.pipelines.sec_xbrl_filings.SECClient") as mock:
      yield mock

  @pytest.fixture
  def pipeline(self, mock_env, mock_redis, mock_s3, mock_sec_client):
    """Create a pipeline instance with all mocks."""
    return SECXBRLPipeline("test_pipeline_123")

  def test_initialization_with_localstack(
    self, mock_env, mock_redis, mock_s3, mock_sec_client
  ):
    """Test pipeline initialization with LocalStack configuration."""
    SECXBRLPipeline("test_pipeline_123")

    # Verify S3 client was created with LocalStack endpoint
    mock_s3.assert_called_with(
      "s3",
      endpoint_url="http://localhost:4566",
      aws_access_key_id="test",
      aws_secret_access_key="test",
      region_name="us-east-1",
    )

  def test_initialization_with_aws(
    self, mock_env, mock_redis, mock_s3, mock_sec_client
  ):
    """Test pipeline initialization with AWS configuration."""
    mock_env.get_s3_config.return_value = {
      "endpoint_url": None,
      "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
      "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
      "region_name": "us-east-1",
    }

    SECXBRLPipeline("test_pipeline_456")

    # Verify S3 client was created with AWS credentials
    mock_s3.assert_called_with(
      "s3",
      aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
      aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
      region_name="us-east-1",
    )

  def test_save_parquet_files_to_s3_by_year(self, pipeline):
    """Test saving parquet files to S3."""
    # Create temporary parquet files
    with tempfile.TemporaryDirectory() as temp_dir:
      temp_path = Path(temp_dir)
      file1 = temp_path / "entity.parquet"
      file2 = temp_path / "report.parquet"
      file1.touch()
      file2.touch()

      parquet_files = [file1, file2]
      cik = "123456"
      accession_number = "000123456725000001"
      year = 2025

      result = pipeline._save_parquet_files_to_s3_by_year(
        parquet_files, cik, accession_number, year
      )

      # Verify S3 uploads
      assert len(result) == 2
      assert pipeline.s3_client.upload_file.call_count == 2

      # Check S3 keys have correct structure
      # Files should be in processed/year={year}/nodes/ or relationships/
      expected_year_prefix = f"processed/year={year}"
      assert all(key.startswith(expected_year_prefix) for key in result)

      # Should have both a node file (Entity) and relationship file (Report)
      # but actually both are node types, so both should be in nodes/
      node_files = [key for key in result if "/nodes/" in key]
      assert len(node_files) == 2  # Both entity and report are node types

  def test_save_parquet_files_with_missing_date(self, pipeline):
    """Test saving parquet files when filing date is missing."""
    with tempfile.TemporaryDirectory() as temp_dir:
      temp_path = Path(temp_dir)
      file1 = temp_path / "test.parquet"
      file1.touch()

      # Method now uses year parameter directly
      cik = "123456"
      accession_number = "000123456725000001"
      year = 2025

      result = pipeline._save_parquet_files_to_s3_by_year(
        [file1], cik, accession_number, year
      )

    # Check S3 keys have correct structure
    # Unknown files go to misc directory
    expected_prefix = f"processed/year={year}/misc"
    assert result[0].startswith(expected_prefix)


class TestRawDataCollection:
  """Test raw data collection functionality."""

  @pytest.fixture
  def mock_env(self):
    """Mock environment configuration."""
    with patch("robosystems.operations.pipelines.sec_xbrl_filings.env") as mock:
      mock.VALKEY_URL = "redis://localhost:6379"
      mock.SEC_RAW_BUCKET = "test-raw-bucket"
      mock.SEC_PROCESSED_BUCKET = "test-processed-bucket"
      mock.get_s3_config.return_value = {
        "endpoint_url": "http://localhost:4566",
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
        "region_name": "us-east-1",
      }
      yield mock

  @pytest.fixture
  def mock_redis(self):
    """Mock Redis client."""
    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.redis.from_url"
    ) as mock:
      redis_instance = MagicMock()
      mock.return_value = redis_instance
      yield redis_instance

  @pytest.fixture
  def mock_s3(self):
    """Mock S3 client."""
    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.boto3.client"
    ) as mock:
      s3_instance = MagicMock()
      mock.return_value = s3_instance
      yield mock

  @pytest.fixture
  def mock_sec_client(self):
    """Mock SEC client."""
    with patch("robosystems.operations.pipelines.sec_xbrl_filings.SECClient") as mock:
      yield mock

  @pytest.fixture
  def pipeline(self, mock_env, mock_redis, mock_s3, mock_sec_client):
    """Create a pipeline instance with all mocks."""
    return SECXBRLPipeline("test_pipeline_123")

  def test_collect_raw_data_by_year_success(self, pipeline):
    """Test successful raw data collection for a year."""
    year = 2025

    # Mock the entire collection process since it's complex
    with (
      patch.object(pipeline, "_discover_companies") as mock_discover_companies,
      patch.object(
        pipeline, "_discover_entity_filings_by_year"
      ) as mock_discover_filings,
      patch.object(pipeline, "_collect_raw_filing") as mock_collect,
    ):
      # Mock companies discovery
      mock_discover_companies.return_value = [{"cik": "123456"}]

      # Mock filings discovery returns some filings
      mock_discover_filings.return_value = [
        {"accessionNumber": "000123456725000001"},
      ]

      # Mock collection succeeds and returns a file key
      mock_collect.return_value = "raw/year=2025/123456_000123456725000001.zip"

      # Run collection
      result = pipeline.collect_raw_data_by_year(year)

      assert result["status"] == "completed"
      assert result["files_collected"] == 1
      assert len(result["collected_files"]) == 1

  def test_collect_raw_data_by_year_empty_filings(self, pipeline):
    """Test raw data collection when no companies are found."""
    year = 2025

    with patch.object(pipeline, "_discover_companies") as mock_discover_companies:
      mock_discover_companies.return_value = []

      with pytest.raises(Exception, match="No companies discovered"):
        pipeline.collect_raw_data_by_year(year)

  def test_collect_raw_data_by_year_collection_failure(self, pipeline):
    """Test raw data collection when company discovery fails."""
    year = 2025

    with patch.object(pipeline, "_discover_companies") as mock_discover_companies:
      mock_discover_companies.side_effect = Exception("Company discovery failed")

      with pytest.raises(Exception, match="Company discovery failed"):
        pipeline.collect_raw_data_by_year(year)


class TestEntityDiscovery:
  """Test entity and filing discovery functionality."""

  @pytest.fixture
  def mock_env(self):
    """Mock environment configuration."""
    with patch("robosystems.operations.pipelines.sec_xbrl_filings.env") as mock:
      mock.VALKEY_URL = "redis://localhost:6379"
      mock.SEC_RAW_BUCKET = "test-raw-bucket"
      mock.SEC_PROCESSED_BUCKET = "test-processed-bucket"
      mock.get_s3_config.return_value = {
        "endpoint_url": "http://localhost:4566",
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
        "region_name": "us-east-1",
      }
      yield mock

  @pytest.fixture
  def mock_redis(self):
    """Mock Redis client."""
    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.redis.from_url"
    ) as mock:
      redis_instance = MagicMock()
      mock.return_value = redis_instance
      yield redis_instance

  @pytest.fixture
  def mock_s3(self):
    """Mock S3 client."""
    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.boto3.client"
    ) as mock:
      s3_instance = MagicMock()
      mock.return_value = s3_instance
      yield mock

  @pytest.fixture
  def mock_sec_client(self):
    """Mock SEC client."""
    with patch("robosystems.operations.pipelines.sec_xbrl_filings.SECClient") as mock:
      yield mock

  @pytest.fixture
  def pipeline(self, mock_env, mock_redis, mock_s3, mock_sec_client):
    """Create a pipeline instance with all mocks."""
    return SECXBRLPipeline("test_pipeline_123")

  def test_discover_companies_success(self, pipeline):
    """Test successful company discovery."""
    import pandas as pd

    max_companies = 10

    mock_df = pd.DataFrame(
      [
        {"cik": "123456", "name": "Test Corp 1"},
        {"cik": "789012", "name": "Test Corp 2"},
      ]
    )

    with patch.object(pipeline.sec_client, "get_companies_df") as mock_get_companies_df:
      mock_get_companies_df.return_value = mock_df

      result = pipeline._discover_companies(max_companies)

      assert len(result) == 2
      assert result[0]["cik"] == "123456"
      mock_get_companies_df.assert_called_once()

  def test_discover_companies_empty(self, pipeline):
    """Test company discovery when no companies found."""
    import pandas as pd

    mock_df = pd.DataFrame()

    with patch.object(pipeline.sec_client, "get_companies_df") as mock_get_companies_df:
      mock_get_companies_df.return_value = mock_df

      result = pipeline._discover_companies()

      assert result == []

  def test_discover_entity_filings_by_year_success(self, pipeline):
    """Test successful entity filings discovery."""
    import pandas as pd

    cik = "123456"
    year = 2025

    mock_df = pd.DataFrame(
      [
        {
          "cik": "123456",
          "accessionNumber": "000123456725000001",
          "form": "10-K",
          "primaryDocument": "d123456d10k.htm",
          "filingDate": "2025-03-15",
          "reportDate": "2024-12-31",
          "isXBRL": True,
          "isInlineXBRL": False,
        }
      ]
    )

    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.SECClient"
    ) as mock_sec_client_class:
      mock_client_instance = MagicMock()
      mock_sec_client_class.return_value = mock_client_instance
      mock_client_instance.submissions_df.return_value = mock_df
      mock_client_instance.get_submissions.return_value = {
        "cik": "123456",
        "filings": {
          "recent": {
            "accessionNumber": ["000123456725000001"],
            "form": ["10-K"],
            "primaryDocument": ["d123456d10k.htm"],
            "filingDate": ["2025-03-15"],
            "reportDate": ["2024-12-31"],
          }
        },
      }

      result = pipeline._discover_entity_filings_by_year(cik, year)

      assert len(result) == 1
      assert result[0]["cik"] == "123456"
      assert result[0]["accessionNumber"] == "000123456725000001"


class TestFilingProcessing:
  """Test filing processing functionality."""

  @pytest.fixture
  def mock_env(self):
    """Mock environment configuration."""
    with patch("robosystems.operations.pipelines.sec_xbrl_filings.env") as mock:
      mock.VALKEY_URL = "redis://localhost:6379"
      mock.SEC_RAW_BUCKET = "test-raw-bucket"
      mock.SEC_PROCESSED_BUCKET = "test-processed-bucket"
      mock.get_s3_config.return_value = {
        "endpoint_url": "http://localhost:4566",
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
        "region_name": "us-east-1",
      }
      yield mock

  @pytest.fixture
  def mock_redis(self):
    """Mock Redis client."""
    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.redis.from_url"
    ) as mock:
      redis_instance = MagicMock()
      mock.return_value = redis_instance
      yield redis_instance

  @pytest.fixture
  def mock_s3(self):
    """Mock S3 client."""
    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.boto3.client"
    ) as mock:
      s3_instance = MagicMock()
      mock.return_value = s3_instance
      yield mock

  @pytest.fixture
  def mock_sec_client(self):
    """Mock SEC client."""
    with patch("robosystems.operations.pipelines.sec_xbrl_filings.SECClient") as mock:
      yield mock

  @pytest.fixture
  def pipeline(self, mock_env, mock_redis, mock_s3, mock_sec_client):
    """Create a pipeline instance with all mocks."""
    return SECXBRLPipeline("test_pipeline_123")

  def test_process_year_data_success(self, pipeline):
    """Test successful year data processing."""
    year = 2025

    with (
      patch.object(pipeline, "_list_raw_files_by_year") as mock_list_raw,
      patch.object(pipeline, "_process_single_raw_file") as mock_process_single,
    ):
      # Mock raw files exist
      mock_list_raw.return_value = ["raw/123456_000123456725000001.zip"]
      mock_process_single.return_value = {"status": "completed"}

      result = pipeline.process_year_data(year)

      assert result["status"] == "completed"
      mock_list_raw.assert_called_once_with(year)
      mock_process_single.assert_called_once()

  def test_process_year_data_no_raw_files(self, pipeline):
    """Test processing when no raw files exist."""
    year = 2025

    with patch.object(pipeline, "_list_raw_files_by_year") as mock_list_raw:
      mock_list_raw.return_value = []

      result = pipeline.process_year_data(year)

      assert result["status"] == "completed"  # No files is not an error

  def test_process_raw_filing_success(self, pipeline):
    """Test successful raw filing processing."""
    filing_key = "raw/year=2025/123456/000123456725000001.zip"
    year = 2025

    with patch.object(pipeline, "_process_single_raw_file") as mock_process_single:
      mock_process_single.return_value = {
        "status": "success",
        "processed_files": ["s3://bucket/processed/year=2025/nodes/entity.parquet"],
      }

      result = pipeline._process_raw_filing(filing_key, year)

      assert result is not None
      assert len(result) == 1
      mock_process_single.assert_called_once()


class TestMetadataRetrieval:
  """Test metadata retrieval functionality."""

  @pytest.fixture
  def mock_env(self):
    """Mock environment configuration."""
    with patch("robosystems.operations.pipelines.sec_xbrl_filings.env") as mock:
      mock.VALKEY_URL = "redis://localhost:6379"
      mock.SEC_RAW_BUCKET = "test-raw-bucket"
      mock.SEC_PROCESSED_BUCKET = "test-processed-bucket"
      mock.get_s3_config.return_value = {
        "endpoint_url": "http://localhost:4566",
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
        "region_name": "us-east-1",
      }
      yield mock

  @pytest.fixture
  def mock_redis(self):
    """Mock Redis client."""
    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.redis.from_url"
    ) as mock:
      redis_instance = MagicMock()
      mock.return_value = redis_instance
      yield redis_instance

  @pytest.fixture
  def mock_s3(self):
    """Mock S3 client."""
    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.boto3.client"
    ) as mock:
      s3_instance = MagicMock()
      mock.return_value = s3_instance
      yield mock

  @pytest.fixture
  def mock_sec_client(self):
    """Mock SEC client."""
    with patch("robosystems.operations.pipelines.sec_xbrl_filings.SECClient") as mock:
      yield mock

  @pytest.fixture
  def pipeline(self, mock_env, mock_redis, mock_s3, mock_sec_client):
    """Create a pipeline instance with all mocks."""
    return SECXBRLPipeline("test_pipeline_123")

  def test_get_entity_metadata_success(self, pipeline):
    """Test successful entity metadata retrieval."""
    cik = "123456"

    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.SECClient"
    ) as mock_sec_client_class:
      mock_client_instance = MagicMock()
      mock_sec_client_class.return_value = mock_client_instance
      mock_client_instance.get_submissions.return_value = {
        "cik": "123456",
        "name": "Test Corporation",
        "sic": "7372",
        "stateOfIncorporation": "DE",
      }

      result = pipeline._get_entity_metadata(cik)

      assert result is not None
      assert result["cik"] == "123456"
      assert result["name"] == "Test Corporation"

  def test_get_entity_metadata_not_found(self, pipeline):
    """Test entity metadata retrieval when company not found."""
    cik = "999999"

    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.SECClient"
    ) as mock_sec_client_class:
      mock_client_instance = MagicMock()
      mock_sec_client_class.return_value = mock_client_instance
      mock_client_instance.get_submissions.return_value = None

      result = pipeline._get_entity_metadata(cik)

      assert result is None

  def test_get_filing_metadata_success(self, pipeline):
    """Test successful filing metadata retrieval."""
    cik = "123456"
    accession_number = "000123456725000001"

    with (
      patch(
        "robosystems.operations.pipelines.sec_xbrl_filings.SECClient"
      ) as mock_sec_client_class,
      patch.object(pipeline, "_load_entity_submissions_snapshot") as mock_load_snapshot,
    ):
      mock_client_instance = MagicMock()
      mock_sec_client_class.return_value = mock_client_instance
      mock_load_snapshot.return_value = {
        "filings": {
          "recent": {
            "accessionNumber": ["0001234567-25-000001"],
            "form": ["10-K"],
            "filingDate": ["2025-03-15"],
            "primaryDocument": ["d123456d10k.htm"],
          }
        }
      }

      result = pipeline._get_filing_metadata(cik, accession_number)

      assert result is not None
      assert result["form"] == "10-K"


class TestFileOperations:
  """Test file operation functionality."""

  @pytest.fixture
  def mock_env(self):
    """Mock environment configuration."""
    with patch("robosystems.operations.pipelines.sec_xbrl_filings.env") as mock:
      mock.VALKEY_URL = "redis://localhost:6379"
      mock.SEC_RAW_BUCKET = "test-raw-bucket"
      mock.SEC_PROCESSED_BUCKET = "test-processed-bucket"
      mock.get_s3_config.return_value = {
        "endpoint_url": "http://localhost:4566",
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
        "region_name": "us-east-1",
      }
      yield mock

  @pytest.fixture
  def mock_redis(self):
    """Mock Redis client."""
    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.redis.from_url"
    ) as mock:
      redis_instance = MagicMock()
      mock.return_value = redis_instance
      yield redis_instance

  @pytest.fixture
  def mock_s3(self):
    """Mock S3 client."""
    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.boto3.client"
    ) as mock:
      s3_instance = MagicMock()
      mock.return_value = s3_instance
      yield mock

  @pytest.fixture
  def mock_sec_client(self):
    """Mock SEC client."""
    with patch("robosystems.operations.pipelines.sec_xbrl_filings.SECClient") as mock:
      yield mock

  @pytest.fixture
  def pipeline(self, mock_env, mock_redis, mock_s3, mock_sec_client):
    """Create a pipeline instance with all mocks."""
    return SECXBRLPipeline("test_pipeline_123")

  def test_list_raw_files_by_year_success(self, pipeline):
    """Test listing raw files for a year."""
    year = 2025

    # Mock S3 list_objects_v2
    pipeline.s3_client.list_objects_v2.return_value = {
      "Contents": [
        {"Key": f"raw/year={year}/123456_000123456725000001.zip"},
        {"Key": f"raw/year={year}/789012_000789012725000002.zip"},
      ]
    }

    result = pipeline._list_raw_files_by_year(year)

    assert len(result) == 2
    assert "123456_000123456725000001.zip" in result[0]
    pipeline.s3_client.list_objects_v2.assert_called_once()

  def test_list_raw_files_by_year_empty(self, pipeline):
    """Test listing raw files when none exist."""
    year = 2025

    pipeline.s3_client.list_objects_v2.return_value = {}

    result = pipeline._list_raw_files_by_year(year)

    assert result == []

  def test_list_processed_files_by_year_success(self, pipeline):
    """Test listing processed files for a year."""
    year = 2025

    pipeline.s3_client.list_objects_v2.return_value = {
      "Contents": [
        {"Key": f"processed/year={year}/nodes/entity.parquet"},
        {"Key": f"processed/year={year}/relationships/facts.parquet"},
      ]
    }

    result = pipeline._list_processed_files_by_year(year)

    assert len(result) == 2
    assert "entity.parquet" in result[0]


class TestParquetValidation:
  """Test parquet file validation functionality."""

  @pytest.fixture
  def mock_env(self):
    """Mock environment configuration."""
    with patch("robosystems.operations.pipelines.sec_xbrl_filings.env") as mock:
      mock.VALKEY_URL = "redis://localhost:6379"
      mock.SEC_RAW_BUCKET = "test-raw-bucket"
      mock.SEC_PROCESSED_BUCKET = "test-processed-bucket"
      mock.get_s3_config.return_value = {
        "endpoint_url": "http://localhost:4566",
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
        "region_name": "us-east-1",
      }
      yield mock

  @pytest.fixture
  def mock_redis(self):
    """Mock Redis client."""
    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.redis.from_url"
    ) as mock:
      redis_instance = MagicMock()
      mock.return_value = redis_instance
      yield redis_instance

  @pytest.fixture
  def mock_s3(self):
    """Mock S3 client."""
    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.boto3.client"
    ) as mock:
      s3_instance = MagicMock()
      mock.return_value = s3_instance
      yield mock

  @pytest.fixture
  def mock_sec_client(self):
    """Mock SEC client."""
    with patch("robosystems.operations.pipelines.sec_xbrl_filings.SECClient") as mock:
      yield mock

  @pytest.fixture
  def pipeline(self, mock_env, mock_redis, mock_s3, mock_sec_client):
    """Create a pipeline instance with all mocks."""
    return SECXBRLPipeline("test_pipeline_123")

  def test_check_parquet_files_exist_by_year_all_exist(self, pipeline):
    """Test parquet file validation when all files exist."""
    cik = "123456"
    accession_number = "000123456725000001"
    year = 2025

    def mock_list_objects_v2(Bucket, Prefix, MaxKeys):
      if "nodes" in Prefix:
        return {
          "Contents": [
            {
              "Key": f"processed/year={year}/nodes/Entity/123456_000123456725000001.parquet"
            },
            {
              "Key": f"processed/year={year}/nodes/Facts/123456_000123456725000001.parquet"
            },
          ]
        }
      elif "relationships" in Prefix:
        return {"Contents": []}
      else:
        return {}

    with patch.object(
      pipeline.s3_client, "list_objects_v2", side_effect=mock_list_objects_v2
    ):
      result = pipeline._check_parquet_files_exist_by_year(cik, accession_number, year)

      assert len(result) == 2

  def test_check_parquet_files_exist_by_year_missing(self, pipeline):
    """Test parquet file validation when files are missing."""
    cik = "123456"
    accession_number = "000123456725000001"
    year = 2025

    with patch.object(pipeline.s3_client, "list_objects_v2") as mock_list:
      mock_list.return_value = {}  # No Contents key

      result = pipeline._check_parquet_files_exist_by_year(cik, accession_number, year)

      assert result == []


class TestErrorHandling:
  """Test error handling scenarios."""

  @pytest.fixture
  def mock_env(self):
    """Mock environment configuration."""
    with patch("robosystems.operations.pipelines.sec_xbrl_filings.env") as mock:
      mock.VALKEY_URL = "redis://localhost:6379"
      mock.SEC_RAW_BUCKET = "test-raw-bucket"
      mock.SEC_PROCESSED_BUCKET = "test-processed-bucket"
      mock.get_s3_config.return_value = {
        "endpoint_url": "http://localhost:4566",
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
        "region_name": "us-east-1",
      }
      yield mock

  @pytest.fixture
  def mock_redis(self):
    """Mock Redis client."""
    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.redis.from_url"
    ) as mock:
      redis_instance = MagicMock()
      mock.return_value = redis_instance
      yield redis_instance

  @pytest.fixture
  def mock_s3(self):
    """Mock S3 client."""
    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.boto3.client"
    ) as mock:
      s3_instance = MagicMock()
      mock.return_value = s3_instance
      yield mock

  @pytest.fixture
  def mock_sec_client(self):
    """Mock SEC client."""
    with patch("robosystems.operations.pipelines.sec_xbrl_filings.SECClient") as mock:
      yield mock

  @pytest.fixture
  def pipeline(self, mock_env, mock_redis, mock_s3, mock_sec_client):
    """Create a pipeline instance with all mocks."""
    return SECXBRLPipeline("test_pipeline_123")

  def test_collect_raw_filing_download_failure(self, pipeline):
    """Test raw filing collection with download failure."""
    cik = "123456"
    filing = {"accessionNumber": "000123456725000001"}
    year = 2025

    with patch.object(pipeline, "_download_with_retry") as mock_download:
      mock_download.side_effect = Exception("Download failed")

      result = pipeline._collect_raw_filing(cik, filing, year, "STANDARD")

      assert result is None

  def test_process_raw_filing_processing_failure(self, pipeline):
    """Test raw filing processing with processing failure."""
    filing_key = "raw/year=2025/123456/000123456725000001.zip"
    year = 2025

    with patch.object(pipeline, "_process_single_raw_file") as mock_process:
      mock_process.return_value = {"status": "failed"}

      result = pipeline._process_raw_filing(filing_key, year)

      assert result is None

  def test_get_entity_metadata_error_handling(self, pipeline):
    """Test entity metadata retrieval with error handling."""
    cik = "123456"

    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.SECClient"
    ) as mock_sec_client_class:
      mock_client_instance = MagicMock()
      mock_sec_client_class.return_value = mock_client_instance
      mock_client_instance.get_submissions.side_effect = Exception("API error")

      result = pipeline._get_entity_metadata(cik)

      assert result is None

  def test_discover_entity_filings_error_handling(self, pipeline):
    """Test entity filings discovery with error handling."""
    cik = "123456"
    year = 2025

    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.SECClient"
    ) as mock_sec_client_class:
      mock_client_instance = MagicMock()
      mock_sec_client_class.return_value = mock_client_instance
      mock_client_instance.submissions_df.side_effect = Exception("API error")
      mock_client_instance.get_submissions.side_effect = Exception("API error")

      result = pipeline._discover_entity_filings_by_year(cik, year)

      assert result == []


class TestInitializationEdgeCases:
  """Test initialization edge cases."""

  @pytest.fixture
  def mock_env(self):
    """Mock environment configuration."""
    with patch("robosystems.operations.pipelines.sec_xbrl_filings.env") as mock:
      mock.VALKEY_URL = "redis://localhost:6379"
      mock.SEC_RAW_BUCKET = "test-raw-bucket"
      mock.SEC_PROCESSED_BUCKET = "test-processed-bucket"
      mock.get_s3_config.return_value = {
        "endpoint_url": "http://localhost:4566",
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
        "region_name": "us-east-1",
      }
      yield mock

  @pytest.fixture
  def mock_redis(self):
    """Mock Redis client."""
    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.redis.from_url"
    ) as mock:
      redis_instance = MagicMock()
      mock.return_value = redis_instance
      yield redis_instance

  @pytest.fixture
  def mock_s3(self):
    """Mock S3 client."""
    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.boto3.client"
    ) as mock:
      s3_instance = MagicMock()
      mock.return_value = s3_instance
      yield mock

  @pytest.fixture
  def mock_sec_client(self):
    """Mock SEC client."""
    with patch("robosystems.operations.pipelines.sec_xbrl_filings.SECClient") as mock:
      yield mock

  def test_init_redis_client_retry_on_failure(
    self, mock_env, mock_redis, mock_s3, mock_sec_client
  ):
    """Test Redis client initialization with retry on failure."""
    from redis import ConnectionError

    with patch("robosystems.config.valkey_registry.create_redis_client") as mock_create:
      # First call fails, second succeeds
      mock_client = MagicMock()
      mock_create.side_effect = [ConnectionError("Connection failed"), mock_client]

      pipeline = SECXBRLPipeline("test_pipeline_456")

      # Should have retried and succeeded
      assert pipeline.redis_client == mock_client
      assert mock_create.call_count == 2

  def test_init_s3_client_aws_configuration(
    self, mock_env, mock_redis, mock_s3, mock_sec_client
  ):
    """Test S3 client initialization with AWS configuration."""
    # Set AWS config (no endpoint_url)
    mock_env.get_s3_config.return_value = {
      "aws_access_key_id": "AKIAEXAMPLE",
      "aws_secret_access_key": "secret",
      "region_name": "us-east-1",
    }

    SECXBRLPipeline("test_pipeline_789")

    # Verify AWS S3 client creation
    mock_s3.assert_called_with(
      "s3",
      aws_access_key_id="AKIAEXAMPLE",
      aws_secret_access_key="secret",
      region_name="us-east-1",
    )

  # def test_check_completion_triggers_ingestion(self, pipeline):
  #   """Test that pipeline completion triggers LadybugDB ingestion."""
  #   # Set up pipeline state for completion
  #   pipeline.redis_client.hgetall.return_value = {
  #     "expected_tasks": "5",
  #     "completed_tasks": "5",
  #     "failed_tasks": "0",
  #   }

  #   # Mock S3 response
  #   pipeline.s3_client.list_objects_v2.return_value = {
  #     "Contents": [{"Key": "processed/test.parquet"}]
  #   }

  #   with patch(
  #     "robosystems.operations.pipelines.sec_xbrl_filings.logger"
  #   ) as mock_logger:
  #     with patch.object(pipeline, "_trigger_lbug_ingestion") as mock_trigger:
  #       is_complete = pipeline._check_completion()

  #       assert is_complete is True
  #       mock_trigger.assert_called_once()
  #       mock_logger.info.assert_called_with(
  #         "Pipeline test_pipeline_123 marked as complete"
  #       )

  # def test_check_completion_not_ready(self, pipeline):
  #   """Test that pipeline doesn't complete prematurely."""
  #   # Set up pipeline state - not all tasks complete
  #   pipeline.redis_client.hgetall.return_value = {
  #     "expected_tasks": "10",
  #     "completed_tasks": "5",
  #     "failed_tasks": "2",
  #   }

  #   is_complete = pipeline._check_completion()

  #   assert is_complete is False
  #   # Status should not be updated to completed
  #   pipeline.redis_client.hset.assert_not_called()

  # def test_filing_failed_updates_tracking(self, pipeline):
  #   """Test that filing failures are properly tracked."""
  #   # Initial state
  #   pipeline.redis_client.hgetall.return_value = {
  #     "expected_tasks": "5",
  #     "completed_tasks": "2",
  #     "failed_tasks": "0",
  #   }

  #   pipeline._filing_failed("123456", "000123456725000001", "XBRL parsing error")

  #   # Verify failed count was incremented
  #   pipeline.redis_client.hincrby.assert_called_with(
  #     "pipeline:sec:test_pipeline_123", "failed_tasks", 1
  #   )

  #   # Verify error was stored
  #   assert pipeline.redis_client.hset.call_count >= 1
  #   error_call = [
  #     c for c in pipeline.redis_client.hset.call_args_list if "error:" in str(c)
  #   ][0]
  #   assert "XBRL parsing error" in str(error_call)

  # def test_mark_failed_sets_pipeline_status(self, pipeline):
  #   """Test marking entire pipeline as failed."""
  #   pipeline._mark_failed("Critical error: Redis connection lost")

  #   # Verify status was set to failed
  #   expected_calls = [
  #     (("pipeline:sec:test_pipeline_123", "status", "failed"),),
  #     (
  #       (
  #         "pipeline:sec:test_pipeline_123",
  #         "error",
  #         "Critical error: Redis connection lost",
  #       ),
  #     ),
  #   ]

  #   actual_calls = [(c[0],) for c in pipeline.redis_client.hset.call_args_list[:2]]
  #   assert actual_calls == expected_calls

  # def test_update_timestamp(self, pipeline):
  #   """Test timestamp update functionality."""
  #   with patch("robosystems.operations.pipelines.sec_xbrl_filings.datetime") as mock_dt:
  #     mock_dt.now.return_value = datetime(2025, 7, 27, 12, 30, 45, tzinfo=timezone.utc)

  #     pipeline._update_timestamp()

  #     pipeline.redis_client.hset.assert_called_with(
  #       "pipeline:sec:test_pipeline_123", "last_updated", "2025-07-27T12:30:45+00:00"
  #     )
