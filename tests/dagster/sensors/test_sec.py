"""Tests for SEC processing sensor."""

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from dagster import build_sensor_context

from robosystems.dagster.sensors.sec import (
  _check_processed_exists,
  _parse_raw_s3_key,
  sec_processing_sensor,
)


class TestParseRawS3Key:
  """Tests for _parse_raw_s3_key helper."""

  def test_valid_key(self):
    """Test parsing a valid S3 key."""
    key = "raw/year=2024/320193/0000320193-24-000081.zip"
    result = _parse_raw_s3_key(key)

    assert result is not None
    year, cik, accession = result
    assert year == "2024"
    assert cik == "320193"
    assert accession == "0000320193-24-000081"

  def test_invalid_key_no_zip(self):
    """Test parsing a key without .zip extension."""
    key = "raw/year=2024/320193/0000320193-24-000081.parquet"
    result = _parse_raw_s3_key(key)
    assert result is None

  def test_invalid_key_wrong_format(self):
    """Test parsing a key with wrong format."""
    key = "processed/year=2024/nodes/Entity.parquet"
    result = _parse_raw_s3_key(key)
    assert result is None

  def test_invalid_key_no_year_prefix(self):
    """Test parsing a key without year= prefix."""
    key = "raw/2024/320193/0000320193-24-000081.zip"
    result = _parse_raw_s3_key(key)
    assert result is None


class TestCheckProcessedExists:
  """Tests for _check_processed_exists helper."""

  def test_processed_exists(self):
    """Test when processed file exists."""
    mock_client = MagicMock()
    mock_client.head_object.return_value = {}

    result = _check_processed_exists(
      mock_client, "test-bucket", "2024", "320193", "0000320193-24-000081"
    )

    assert result is True
    mock_client.head_object.assert_called_once()

  def test_processed_not_exists(self):
    """Test when processed file doesn't exist."""
    mock_client = MagicMock()
    mock_client.head_object.side_effect = ClientError(
      {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
    )

    result = _check_processed_exists(
      mock_client, "test-bucket", "2024", "320193", "0000320193-24-000081"
    )

    assert result is False


class TestSecProcessingSensor:
  """Tests for sec_processing_sensor."""

  @patch("robosystems.dagster.sensors.sec.env")
  def test_skips_in_dev_environment(self, mock_env):
    """Test sensor skips in dev environment."""
    mock_env.ENVIRONMENT = "dev"

    context = build_sensor_context()
    result = list(sec_processing_sensor(context))

    assert len(result) == 1
    assert "Skipped in dev environment" in str(result[0])

  @patch("robosystems.dagster.sensors.sec.env")
  def test_skips_without_bucket_config(self, mock_env):
    """Test sensor skips when S3 buckets are not configured."""
    mock_env.ENVIRONMENT = "prod"
    mock_env.SEC_RAW_BUCKET = None
    mock_env.SEC_PROCESSED_BUCKET = "test-processed"

    context = build_sensor_context()
    result = list(sec_processing_sensor(context))

    assert len(result) == 1
    assert "Missing required S3 bucket configuration" in str(result[0])

  @patch("robosystems.dagster.sensors.sec._get_s3_client")
  @patch("robosystems.dagster.sensors.sec.env")
  def test_handles_no_such_bucket_error(self, mock_env, mock_get_client):
    """Test sensor handles NoSuchBucket error gracefully."""
    mock_env.ENVIRONMENT = "prod"
    mock_env.SEC_RAW_BUCKET = "nonexistent-bucket"
    mock_env.SEC_PROCESSED_BUCKET = "test-processed"

    mock_client = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.side_effect = ClientError(
      {"Error": {"Code": "NoSuchBucket", "Message": "Bucket does not exist"}},
      "ListObjectsV2",
    )
    mock_client.get_paginator.return_value = mock_paginator
    mock_get_client.return_value = mock_client

    context = build_sensor_context()

    with pytest.raises(ClientError):
      list(sec_processing_sensor(context))

  @patch("robosystems.dagster.sensors.sec._check_processed_exists")
  @patch("robosystems.dagster.sensors.sec._get_s3_client")
  @patch("robosystems.dagster.sensors.sec.env")
  def test_discovers_unprocessed_filings(
    self, mock_env, mock_get_client, mock_check_processed
  ):
    """Test sensor discovers unprocessed filings and yields run requests."""
    mock_env.ENVIRONMENT = "prod"
    mock_env.SEC_RAW_BUCKET = "test-raw"
    mock_env.SEC_PROCESSED_BUCKET = "test-processed"

    # Mock S3 listing
    mock_client = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
      {
        "Contents": [
          {"Key": "raw/year=2024/320193/0000320193-24-000081.zip"},
          {"Key": "raw/year=2024/320193/0000320193-24-000082.zip"},
        ]
      }
    ]
    mock_client.get_paginator.return_value = mock_paginator
    mock_get_client.return_value = mock_client

    # First filing is unprocessed, second is processed
    mock_check_processed.side_effect = [False, True]

    # Build context with mock instance
    context = build_sensor_context()
    context._instance = MagicMock()

    result = list(sec_processing_sensor(context))

    # Should yield 1 run request (only unprocessed filing)
    assert len(result) == 1
    assert result[0].partition_key == "2024_320193_0000320193-24-000081"

  @patch("robosystems.dagster.sensors.sec._get_s3_client")
  @patch("robosystems.dagster.sensors.sec.env")
  def test_handles_empty_bucket(self, mock_env, mock_get_client):
    """Test sensor handles empty bucket gracefully."""
    mock_env.ENVIRONMENT = "prod"
    mock_env.SEC_RAW_BUCKET = "test-raw"
    mock_env.SEC_PROCESSED_BUCKET = "test-processed"

    mock_client = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [{"Contents": []}]
    mock_client.get_paginator.return_value = mock_paginator
    mock_get_client.return_value = mock_client

    context = build_sensor_context()
    result = list(sec_processing_sensor(context))

    # Should return empty (no run requests, no skip reason)
    assert len(result) == 0

  @patch("robosystems.dagster.sensors.sec._check_processed_exists")
  @patch("robosystems.dagster.sensors.sec._get_s3_client")
  @patch("robosystems.dagster.sensors.sec.env")
  def test_all_filings_processed(self, mock_env, mock_get_client, mock_check_processed):
    """Test sensor handles case where all filings are already processed."""
    mock_env.ENVIRONMENT = "prod"
    mock_env.SEC_RAW_BUCKET = "test-raw"
    mock_env.SEC_PROCESSED_BUCKET = "test-processed"

    mock_client = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
      {"Contents": [{"Key": "raw/year=2024/320193/0000320193-24-000081.zip"}]}
    ]
    mock_client.get_paginator.return_value = mock_paginator
    mock_get_client.return_value = mock_client

    # All filings are processed
    mock_check_processed.return_value = True

    context = build_sensor_context()
    result = list(sec_processing_sensor(context))

    # Should return empty (no run requests)
    assert len(result) == 0
