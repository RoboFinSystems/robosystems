"""
Test Suite for SEC XBRL Filings Operations

Tests the SEC XBRL pipeline operations class and core functionality.
This complements the task-level tests in test_sec_xbrl_pipeline.py.
"""

from unittest.mock import MagicMock, patch
import pandas as pd
import pytest
import tempfile
from pathlib import Path

from robosystems.operations.pipelines.sec_xbrl_filings import SECXBRLPipeline


class TestSECXBRLPipelineOperations:
  """Test the SEC XBRL pipeline operations class."""

  @pytest.fixture
  def mock_redis(self):
    """Create a mock Redis client."""
    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.redis.from_url"
    ) as mock:
      redis_instance = MagicMock()
      mock.return_value = redis_instance
      yield redis_instance

  @pytest.fixture
  def mock_boto_client(self):
    """Create a mock boto3 S3 client."""
    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.boto3.client"
    ) as mock:
      s3_instance = MagicMock()
      mock.return_value = s3_instance
      yield s3_instance

  @pytest.fixture
  def mock_sec_client(self):
    """Create a mock SEC client."""
    with patch("robosystems.operations.pipelines.sec_xbrl_filings.SECClient") as mock:
      sec_instance = MagicMock()
      # Setup default companies DataFrame
      companies_df = pd.DataFrame(
        {
          "cik": [1045810],
          "cik_str": ["1045810"],
          "ticker": ["NVDA"],
          "title": ["NVIDIA CORP"],
        }
      )
      sec_instance.get_companies_df.return_value = companies_df
      mock.return_value = sec_instance
      yield sec_instance

  @pytest.fixture
  def mock_xbrl_processor(self):
    """Create a mock XBRL processor."""
    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.XBRLGraphProcessor"
    ) as mock:
      processor_instance = MagicMock()
      mock.return_value = processor_instance
      yield processor_instance

  @pytest.fixture
  def pipeline(self, mock_redis, mock_boto_client, mock_sec_client):
    """Create a pipeline instance with mocked dependencies."""
    pipeline = SECXBRLPipeline("test_pipeline_123")
    # Replace clients with mocks
    pipeline.redis_client = mock_redis
    pipeline.s3_client = mock_boto_client
    pipeline.sec_client = mock_sec_client
    return pipeline

  def test_pipeline_initialization(self, pipeline):
    """Test pipeline is properly initialized."""
    assert pipeline.pipeline_run_id == "test_pipeline_123"
    assert pipeline.pipeline_type == "sec_staged"
    assert pipeline.redis_key == "pipeline:sec_staged:test_pipeline_123"
    assert pipeline.ttl == 604800  # 7 days

  def test_discover_companies(self, pipeline):
    """Test company discovery functionality."""
    companies = pipeline._discover_companies(max_companies=5)

    assert len(companies) == 1
    assert companies[0]["cik_str"] == "1045810"
    assert companies[0]["ticker"] == "NVDA"

  def test_discover_companies_with_limit(self, pipeline, mock_sec_client):
    """Test company discovery with limit."""
    # Setup larger companies list
    large_companies_df = pd.DataFrame(
      {
        "cik": [1045810, 789019, 320193],
        "cik_str": ["1045810", "789019", "320193"],
        "ticker": ["NVDA", "MSFT", "INTC"],
        "title": ["NVIDIA CORP", "MICROSOFT CORP", "INTEL CORP"],
      }
    )
    mock_sec_client.get_companies_df.return_value = large_companies_df

    companies = pipeline._discover_companies(max_companies=2)

    assert len(companies) == 2

  def test_discover_entity_filings_by_year(self, pipeline, mock_sec_client):
    """Test filing discovery for a specific entity and year."""
    # Mock the S3 client to simulate no recent snapshots
    pipeline.s3_client.list_objects_v2.return_value = {}

    # Mock submissions data structure (what get_submissions actually returns)
    mock_submissions_data = {
      "filings": {
        "recent": {
          "accessionNumber": ["0001045810-25-000001", "0001045810-25-000002"],
          "filingDate": ["2025-01-15", "2025-02-15"],
          "form": ["10-K", "10-Q"],
          "isXBRL": [True, True],
          "isInlineXBRL": [1, 1],
        }
      }
    }

    # Create a DataFrame that simulates what submissions_df() returns
    mock_submissions_df = pd.DataFrame(
      {
        "accessionNumber": ["0001045810-25-000001", "0001045810-25-000002"],
        "filingDate": ["2025-01-15", "2025-02-15"],
        "form": ["10-K", "10-Q"],
        "isXBRL": [True, True],
        "isInlineXBRL": [True, True],
      }
    )

    # Mock the entity SEC client that gets created in the method
    entity_sec_client_mock = MagicMock()
    entity_sec_client_mock.get_submissions.return_value = mock_submissions_data
    entity_sec_client_mock.submissions_df.return_value = mock_submissions_df

    # Patch the SECClient constructor to return our mock when called with cik
    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.SECClient",
      return_value=entity_sec_client_mock,
    ):
      filings = pipeline._discover_entity_filings_by_year(
        "1045810", 2025, max_filings=10
      )

    assert len(filings) == 2
    assert filings[0]["accessionNumber"] == "0001045810-25-000001"
    assert filings[0]["form"] == "10-K"

  def test_discover_entity_filings_with_limit(self, pipeline, mock_sec_client):
    """Test filing discovery with filing limit."""
    # Mock the S3 client to simulate no recent snapshots
    pipeline.s3_client.list_objects_v2.return_value = {}

    # Mock submissions data structure
    mock_submissions_data = {
      "filings": {
        "recent": {
          "accessionNumber": [f"0001045810-25-{i:06d}" for i in range(1, 11)],
          "filingDate": [f"2025-{i:02d}-15" for i in range(1, 11)],
          "form": ["10-K"] * 10,
          "isXBRL": [True] * 10,
          "isInlineXBRL": [1] * 10,
        }
      }
    }

    # Create a DataFrame that simulates what submissions_df() returns
    mock_submissions_df = pd.DataFrame(
      {
        "accessionNumber": [f"0001045810-25-{i:06d}" for i in range(1, 11)],
        "filingDate": [f"2025-{i:02d}-15" for i in range(1, 11)],
        "form": ["10-K"] * 10,
        "isXBRL": [True] * 10,
        "isInlineXBRL": [True] * 10,
      }
    )

    # Mock the entity SEC client
    entity_sec_client_mock = MagicMock()
    entity_sec_client_mock.get_submissions.return_value = mock_submissions_data
    entity_sec_client_mock.submissions_df.return_value = mock_submissions_df

    with patch(
      "robosystems.operations.pipelines.sec_xbrl_filings.SECClient",
      return_value=entity_sec_client_mock,
    ):
      filings = pipeline._discover_entity_filings_by_year(
        "1045810", 2025, max_filings=3
      )

    assert len(filings) == 3

  def test_process_raw_filing_success(self, pipeline):
    """Test successful raw filing processing."""
    with patch.object(pipeline, "_process_single_raw_file") as mock_process:
      mock_process.return_value = {
        "status": "success",
        "processed_files": [
          "processed/year=2025/nodes/Entity_file.parquet",
          "processed/year=2025/relationships/ENTITY_HAS_REPORT_file.parquet",
        ],
      }

      result = pipeline._process_raw_filing(
        "raw/year=2025/1045810/filing.zip", 2025, refresh=False
      )

      assert result is not None
      assert len(result) == 2

  def test_process_raw_filing_failure(self, pipeline):
    """Test raw filing processing failure."""
    with patch.object(pipeline, "_process_single_raw_file") as mock_process:
      mock_process.return_value = {"status": "failed", "error": "XBRL parsing error"}

      result = pipeline._process_raw_filing(
        "raw/year=2025/1045810/filing.zip", 2025, refresh=False
      )

      assert result is None

  def test_save_parquet_files_to_s3_by_year(self, pipeline):
    """Test saving parquet files to S3 with year partitioning."""
    # Create temporary parquet files to simulate processing output
    with tempfile.TemporaryDirectory() as temp_dir:
      temp_path = Path(temp_dir)

      # Create mock parquet files
      entity_file = temp_path / "Entity.parquet"
      report_file = temp_path / "Report.parquet"
      entity_file.touch()
      report_file.touch()

      parquet_files = [entity_file, report_file]

      result = pipeline._save_parquet_files_to_s3_by_year(
        parquet_files, "1045810", "000104581025000001", 2025
      )

      # Verify uploads were called
      assert len(result) == 2
      assert pipeline.s3_client.upload_file.call_count == 2

      # Verify S3 key structure
      for s3_key in result:
        assert s3_key.startswith("processed/year=2025")
        assert "/nodes/" in s3_key or "/relationships/" in s3_key

  def test_save_parquet_files_unknown_type(self, pipeline):
    """Test saving unknown file types to misc directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
      temp_path = Path(temp_dir)

      # Create unknown file type
      unknown_file = temp_path / "UnknownType.parquet"
      unknown_file.touch()

      result = pipeline._save_parquet_files_to_s3_by_year(
        [unknown_file], "1045810", "000104581025000001", 2025
      )

      # Should be saved to misc directory
      assert len(result) == 1
      assert "/misc/" in result[0]

  def test_collect_raw_filing_success(self, pipeline, mock_sec_client):
    """Test successful raw filing collection."""
    # Mock XBRL ZIP URL
    mock_sec_client.get_xbrlzip_url.return_value = "https://sec.gov/xbrl.zip"

    # Mock head_object to return no existing file
    pipeline.s3_client.head_object.side_effect = Exception("File not found")

    # Mock the entity SEC client creation
    entity_sec_client_mock = MagicMock()
    entity_sec_client_mock.get_xbrlzip_url.return_value = "https://sec.gov/xbrl.zip"

    with (
      patch(
        "robosystems.operations.pipelines.sec_xbrl_filings.SECClient",
        return_value=entity_sec_client_mock,
      ),
      patch(
        "robosystems.operations.pipelines.sec_xbrl_filings.requests.get"
      ) as mock_get,
    ):
      # Mock response with iter_content support
      mock_response = MagicMock()
      mock_response.status_code = 200
      mock_response.iter_content.return_value = [b"mock zip content"]
      mock_get.return_value = mock_response

      filing_data = {
        "accessionNumber": "0001045810-25-000001",
        "filingDate": "2025-01-15",
      }

      result = pipeline._collect_raw_filing("1045810", filing_data, 2025, "STANDARD")

      assert result is not None
      assert result.startswith("raw/year=2025/1045810/")
      pipeline.s3_client.put_object.assert_called_once()

  def test_collect_raw_filing_download_error(self, pipeline, mock_sec_client):
    """Test raw filing collection with download error."""
    mock_sec_client.get_xbrlzip_url.return_value = "https://sec.gov/xbrl.zip"

    with patch("requests.get") as mock_get:
      mock_response = MagicMock()
      mock_response.status_code = 404
      mock_get.return_value = mock_response

      filing_data = {
        "accessionNumber": "0001045810-25-000001",
        "filingDate": "2025-01-15",
      }

      result = pipeline._collect_raw_filing("1045810", filing_data, 2025, "STANDARD")

      assert result is None

  def test_list_processed_files_for_year(self, pipeline):
    """Test listing processed files for a specific year."""
    # Mock S3 response
    pipeline.s3_client.list_objects_v2.return_value = {
      "Contents": [
        {"Key": "processed/year=2025/nodes/Entity_file1.parquet"},
        {"Key": "processed/year=2025/relationships/ENTITY_HAS_REPORT_file1.parquet"},
      ]
    }

    files = pipeline._list_processed_files_by_year(2025)

    assert len(files) == 2
    assert files[0] == "processed/year=2025/nodes/Entity_file1.parquet"

  def test_list_processed_files_empty(self, pipeline):
    """Test listing processed files when none exist."""
    pipeline.s3_client.list_objects_v2.return_value = {}

    files = pipeline._list_processed_files_by_year(2025)

    assert len(files) == 0


class TestSECXBRLPipelineIntegration:
  """Integration tests for SEC XBRL pipeline operations."""

  @pytest.mark.integration
  def test_full_filing_processing_flow(self):
    """Test complete filing processing from raw ZIP to parquet files."""
    # This would require more complex mocking of the XBRL processor
    # and file system operations. For now, we verify the method calls.

    with (
      patch(
        "robosystems.operations.pipelines.sec_xbrl_filings.redis.from_url"
      ) as mock_redis,
      patch(
        "robosystems.operations.pipelines.sec_xbrl_filings.boto3.client"
      ) as mock_boto,
      patch(
        "robosystems.operations.pipelines.sec_xbrl_filings.SECClient"
      ) as mock_sec_class,
    ):
      # Setup mocks
      mock_redis.return_value = MagicMock()
      mock_boto.return_value = MagicMock()

      mock_sec = MagicMock()
      companies_df = pd.DataFrame(
        {"cik_str": ["1045810"], "ticker": ["NVDA"], "title": ["NVIDIA CORP"]}
      )
      mock_sec.get_companies_df.return_value = companies_df
      mock_sec_class.return_value = mock_sec

      pipeline = SECXBRLPipeline("integration_test")

      companies = pipeline._discover_companies(max_companies=1)

      assert len(companies) == 1
      assert companies[0]["cik_str"] == "1045810"
