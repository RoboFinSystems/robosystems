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

  # def test_check_completion_triggers_ingestion(self, pipeline):
  #   """Test that pipeline completion triggers Kuzu ingestion."""
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
  #     with patch.object(pipeline, "_trigger_kuzu_ingestion") as mock_trigger:
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
