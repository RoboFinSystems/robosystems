"""Tests for SEC vector index S3 publish asset."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from dagster import MaterializeResult, build_asset_context

from robosystems.adapters.sec.pipeline.vector_publish import sec_vector_s3_published


@pytest.mark.unit
class TestSecVectorS3Published:
  """Tests for sec_vector_s3_published asset."""

  @patch("robosystems.adapters.sec.pipeline.vector_publish.env")
  def test_skips_in_dev_environment(self, mock_env):
    """Asset skips entirely in dev environment."""
    mock_env.ENVIRONMENT = "dev"

    context = build_asset_context()
    result = sec_vector_s3_published(context)

    assert isinstance(result, MaterializeResult)
    assert result.metadata["status"] == "skipped"
    assert result.metadata["reason"] == "dev_environment"

  @patch(
    "robosystems.graph_api.client.factory.get_graph_client_for_sec_ingestion",
    new_callable=AsyncMock,
  )
  @patch(
    "robosystems.adapters.sec.pipeline.vector_publish.get_shared_repo_database_key"
  )
  @patch("robosystems.adapters.sec.pipeline.vector_publish.boto3")
  @patch("robosystems.adapters.sec.pipeline.vector_publish.env")
  def test_skips_on_export_failure(
    self, mock_env, mock_boto3, mock_get_key, mock_get_client
  ):
    """Asset returns skipped result when export fails."""
    mock_env.ENVIRONMENT = "prod"
    mock_env.AWS_REGION = "us-east-1"

    mock_sts = MagicMock()
    mock_sts.get_caller_identity.return_value = {"Account": "000000000000"}
    mock_boto3.client.return_value = mock_sts
    mock_get_key.return_value = "test-key"

    mock_client = AsyncMock()
    mock_client.vector_export.side_effect = RuntimeError("No index found")
    mock_get_client.return_value = mock_client

    context = build_asset_context()
    result = sec_vector_s3_published(context)

    assert isinstance(result, MaterializeResult)
    assert result.metadata["status"] == "skipped"
    assert "export_failed" in result.metadata["reason"]

  @patch(
    "robosystems.graph_api.client.factory.get_graph_client_for_sec_ingestion",
    new_callable=AsyncMock,
  )
  @patch(
    "robosystems.adapters.sec.pipeline.vector_publish.get_shared_repo_database_key"
  )
  @patch("robosystems.adapters.sec.pipeline.vector_publish.boto3")
  @patch("robosystems.adapters.sec.pipeline.vector_publish.env")
  def test_skips_when_no_s3_uri(
    self, mock_env, mock_boto3, mock_get_key, mock_get_client
  ):
    """Asset returns skipped result when export returns no s3_uri."""
    mock_env.ENVIRONMENT = "prod"
    mock_env.AWS_REGION = "us-east-1"

    mock_sts = MagicMock()
    mock_sts.get_caller_identity.return_value = {"Account": "000000000000"}
    mock_boto3.client.return_value = mock_sts
    mock_get_key.return_value = "test-key"

    mock_client = AsyncMock()
    mock_client.vector_export.return_value = {"size_mb": 0}
    mock_get_client.return_value = mock_client

    context = build_asset_context()
    result = sec_vector_s3_published(context)

    assert isinstance(result, MaterializeResult)
    assert result.metadata["status"] == "skipped"
    assert result.metadata["reason"] == "no_s3_uri"

  @patch(
    "robosystems.graph_api.client.factory.get_graph_client_for_sec_ingestion",
    new_callable=AsyncMock,
  )
  @patch(
    "robosystems.adapters.sec.pipeline.vector_publish.get_shared_repo_database_key"
  )
  @patch("robosystems.adapters.sec.pipeline.vector_publish.boto3")
  @patch("robosystems.adapters.sec.pipeline.vector_publish.env")
  def test_successful_export_and_upload(
    self, mock_env, mock_boto3, mock_get_key, mock_get_client
  ):
    """Asset exports with S3 params and returns full metadata."""
    mock_env.ENVIRONMENT = "prod"
    mock_env.AWS_REGION = "us-east-1"

    mock_sts = MagicMock()
    mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}
    mock_boto3.client.return_value = mock_sts
    mock_get_key.return_value = "shared-repositories/databases/sec.Element.lance.tar.gz"

    mock_client = AsyncMock()
    mock_client.vector_export.return_value = {
      "s3_uri": "s3://robosystems-123456789012-user-prod/shared-repositories/databases/sec.Element.lance.tar.gz",
      "size_mb": 42.5,
      "duration_ms": 3500,
    }
    mock_get_client.return_value = mock_client

    context = build_asset_context()
    result = sec_vector_s3_published(context)

    assert isinstance(result, MaterializeResult)
    assert result.metadata["graph_id"] == "sec"
    assert result.metadata["table_name"] == "Element"
    assert result.metadata["s3_bucket"] == "robosystems-123456789012-user-prod"
    assert (
      result.metadata["s3_key"]
      == "shared-repositories/databases/sec.Element.lance.tar.gz"
    )
    assert result.metadata["file_size_mb"] == 42.5
    assert result.metadata["export_duration_ms"] == 3500

  @patch(
    "robosystems.graph_api.client.factory.get_graph_client_for_sec_ingestion",
    new_callable=AsyncMock,
  )
  @patch(
    "robosystems.adapters.sec.pipeline.vector_publish.get_shared_repo_database_key"
  )
  @patch("robosystems.adapters.sec.pipeline.vector_publish.boto3")
  @patch("robosystems.adapters.sec.pipeline.vector_publish.env")
  def test_passes_s3_params_to_export(
    self, mock_env, mock_boto3, mock_get_key, mock_get_client
  ):
    """Asset passes S3 bucket and key to vector_export for server-side upload."""
    mock_env.ENVIRONMENT = "prod"
    mock_env.AWS_REGION = "us-east-1"

    mock_sts = MagicMock()
    mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}
    mock_boto3.client.return_value = mock_sts
    mock_get_key.return_value = "shared-repositories/databases/sec.Element.lance.tar.gz"

    mock_client = AsyncMock()
    mock_client.vector_export.return_value = {
      "s3_uri": "s3://bucket/key",
      "size_mb": 1.0,
      "duration_ms": 100,
    }
    mock_get_client.return_value = mock_client

    context = build_asset_context()
    sec_vector_s3_published(context)

    mock_client.vector_export.assert_called_once_with(
      graph_id="sec",
      table_name="Element",
      s3_bucket="robosystems-123456789012-user-prod",
      s3_key="shared-repositories/databases/sec.Element.lance.tar.gz",
    )

  @patch(
    "robosystems.graph_api.client.factory.get_graph_client_for_sec_ingestion",
    new_callable=AsyncMock,
  )
  @patch(
    "robosystems.adapters.sec.pipeline.vector_publish.get_shared_repo_database_key"
  )
  @patch("robosystems.adapters.sec.pipeline.vector_publish.boto3")
  @patch("robosystems.adapters.sec.pipeline.vector_publish.env")
  def test_closes_client_on_success(
    self, mock_env, mock_boto3, mock_get_key, mock_get_client
  ):
    """Client is closed after successful export."""
    mock_env.ENVIRONMENT = "prod"
    mock_env.AWS_REGION = "us-east-1"

    mock_sts = MagicMock()
    mock_sts.get_caller_identity.return_value = {"Account": "000000000000"}
    mock_boto3.client.return_value = mock_sts
    mock_get_key.return_value = "test-key"

    mock_client = AsyncMock()
    mock_client.vector_export.return_value = {}
    mock_get_client.return_value = mock_client

    context = build_asset_context()
    sec_vector_s3_published(context)

    mock_client.close.assert_called_once()

  @patch(
    "robosystems.graph_api.client.factory.get_graph_client_for_sec_ingestion",
    new_callable=AsyncMock,
  )
  @patch(
    "robosystems.adapters.sec.pipeline.vector_publish.get_shared_repo_database_key"
  )
  @patch("robosystems.adapters.sec.pipeline.vector_publish.boto3")
  @patch("robosystems.adapters.sec.pipeline.vector_publish.env")
  def test_closes_client_on_failure(
    self, mock_env, mock_boto3, mock_get_key, mock_get_client
  ):
    """Client is closed even when export raises."""
    mock_env.ENVIRONMENT = "prod"
    mock_env.AWS_REGION = "us-east-1"

    mock_sts = MagicMock()
    mock_sts.get_caller_identity.return_value = {"Account": "000000000000"}
    mock_boto3.client.return_value = mock_sts
    mock_get_key.return_value = "test-key"

    mock_client = AsyncMock()
    mock_client.vector_export.side_effect = RuntimeError("boom")
    mock_get_client.return_value = mock_client

    context = build_asset_context()
    sec_vector_s3_published(context)

    mock_client.close.assert_called_once()
