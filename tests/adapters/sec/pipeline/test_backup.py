"""Tests for SEC backup asset."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from dagster import MaterializeResult, build_asset_context

from robosystems.adapters.sec.pipeline.backup import sec_backup
from robosystems.adapters.sec.pipeline.configs import SECBackupConfig


def _make_mock_db_resource():
  """Create a mock DatabaseResource."""
  mock_db = MagicMock()
  mock_session = MagicMock()
  mock_db.get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
  mock_db.get_session.return_value.__exit__ = MagicMock(return_value=False)
  return mock_db, mock_session


def _make_mock_s3_resource():
  """Create a mock S3Resource."""
  mock_s3 = MagicMock()
  mock_s3.client = MagicMock()
  return mock_s3


@pytest.mark.unit
class TestSecBackup:
  """Tests for sec_backup asset."""

  @patch("robosystems.config.env")
  @patch("robosystems.middleware.graph.utils.MultiTenantUtils")
  @patch("robosystems.config.storage.graph.get_shared_repo_backup_key")
  @patch("robosystems.operations.aws.s3.S3BackupAdapter")
  @patch("robosystems.models.iam.GraphBackup")
  def test_skips_in_dev_environment(
    self, mock_backup_cls, mock_s3_adapter_cls, mock_get_key, mock_utils, mock_env
  ):
    """Test backup is skipped in dev environment."""
    mock_env.ENVIRONMENT = "dev"
    mock_utils.is_shared_repository.return_value = True
    mock_utils.get_database_name.return_value = "sec_db"
    mock_get_key.return_value = "backups/sec/2025-01-01.tar.gz"

    mock_s3_adapter = MagicMock()
    mock_s3_adapter.bucket_name = "test-bucket"
    mock_s3_adapter_cls.return_value = mock_s3_adapter

    mock_backup_record = MagicMock()
    mock_backup_record.id = "backup-123"
    mock_backup_cls.create.return_value = mock_backup_record

    mock_db, mock_session = _make_mock_db_resource()
    mock_s3 = _make_mock_s3_resource()

    config = SECBackupConfig()
    context = build_asset_context()

    result = sec_backup(context, config, mock_db, mock_s3)

    assert isinstance(result, MaterializeResult)
    assert result.metadata["status"] == "skipped"
    assert result.metadata["reason"] == "dev_environment"

  @patch("robosystems.config.env")
  @patch("robosystems.middleware.graph.utils.MultiTenantUtils")
  @patch("robosystems.config.storage.graph.get_shared_repo_backup_key")
  @patch("robosystems.operations.aws.s3.S3BackupAdapter")
  @patch("robosystems.models.iam.GraphBackup")
  @patch(
    "robosystems.graph_api.client.factory.get_graph_client_for_sec_ingestion",
    new_callable=AsyncMock,
  )
  @patch("boto3.client")
  def test_successful_backup_non_dev(
    self,
    mock_boto3_client,
    mock_get_client,
    mock_backup_cls,
    mock_s3_adapter_cls,
    mock_get_key,
    mock_utils,
    mock_env,
  ):
    """Test successful backup in non-dev environment."""
    mock_env.ENVIRONMENT = "prod"
    mock_env.AWS_REGION = "us-east-1"
    mock_utils.is_shared_repository.return_value = True
    mock_utils.get_database_name.return_value = "sec_db"
    mock_get_key.return_value = "backups/sec/2025-01-01.tar.gz"

    mock_s3_adapter = MagicMock()
    mock_s3_adapter.bucket_name = "prod-bucket"
    mock_s3_adapter_cls.return_value = mock_s3_adapter

    mock_backup_record = MagicMock()
    mock_backup_record.id = "backup-123"
    mock_backup_cls.create.return_value = mock_backup_record
    mock_backup_cls.get_by_id.return_value = mock_backup_record

    mock_sts = MagicMock()
    mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}
    mock_boto3_client.return_value = mock_sts

    # Mock the graph client - async methods for loop.run_until_complete
    mock_client = MagicMock()
    mock_client.backup_with_sse = AsyncMock(
      return_value={
        "status": "completed",
        "result": {
          "original_size_bytes": 1000000,
          "compressed_size_bytes": 500000,
          "compression_ratio": 0.5,
          "checksum": "abc123",
          "duration_seconds": 30,
          "s3_key": "backups/sec/2025-01-01.tar.gz",
        },
      }
    )
    mock_client.close = AsyncMock()
    mock_get_client.return_value = mock_client

    mock_db, mock_session = _make_mock_db_resource()
    mock_s3 = _make_mock_s3_resource()

    config = SECBackupConfig()
    context = build_asset_context()

    result = sec_backup(context, config, mock_db, mock_s3)

    assert isinstance(result, MaterializeResult)
    assert result.metadata["backup_id"] == "backup-123"
    assert result.metadata["graph_id"] == "sec"

  @patch("robosystems.config.env")
  @patch("robosystems.middleware.graph.utils.MultiTenantUtils")
  @patch("robosystems.config.storage.graph.get_shared_repo_backup_key")
  @patch("robosystems.operations.aws.s3.S3BackupAdapter")
  @patch("robosystems.models.iam.GraphBackup")
  @patch(
    "robosystems.graph_api.client.factory.get_graph_client_for_sec_ingestion",
    new_callable=AsyncMock,
  )
  @patch("boto3.client")
  def test_backup_failure_raises(
    self,
    mock_boto3_client,
    mock_get_client,
    mock_backup_cls,
    mock_s3_adapter_cls,
    mock_get_key,
    mock_utils,
    mock_env,
  ):
    """Test backup failure raises RuntimeError."""
    mock_env.ENVIRONMENT = "prod"
    mock_env.AWS_REGION = "us-east-1"
    mock_utils.is_shared_repository.return_value = True
    mock_utils.get_database_name.return_value = "sec_db"
    mock_get_key.return_value = "backups/sec/2025-01-01.tar.gz"

    mock_s3_adapter = MagicMock()
    mock_s3_adapter.bucket_name = "prod-bucket"
    mock_s3_adapter_cls.return_value = mock_s3_adapter

    mock_backup_record = MagicMock()
    mock_backup_record.id = "backup-123"
    mock_backup_cls.create.return_value = mock_backup_record
    mock_backup_cls.get_by_id.return_value = mock_backup_record

    mock_sts = MagicMock()
    mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}
    mock_boto3_client.return_value = mock_sts

    # Mock the graph client returning failure
    mock_client = MagicMock()
    mock_client.backup_with_sse = AsyncMock(
      return_value={
        "status": "failed",
        "error": "Disk space exceeded",
      }
    )
    mock_client.close = AsyncMock()
    mock_get_client.return_value = mock_client

    mock_db, mock_session = _make_mock_db_resource()
    mock_s3 = _make_mock_s3_resource()

    config = SECBackupConfig()
    context = build_asset_context()

    with pytest.raises(RuntimeError, match="SEC backup failed"):
      sec_backup(context, config, mock_db, mock_s3)

  @patch("robosystems.config.env")
  @patch("robosystems.middleware.graph.utils.MultiTenantUtils")
  @patch("robosystems.config.storage.graph.get_shared_repo_backup_key")
  @patch("robosystems.operations.aws.s3.S3BackupAdapter")
  @patch("robosystems.models.iam.GraphBackup")
  def test_backup_warns_on_non_shared_repo(
    self, mock_backup_cls, mock_s3_adapter_cls, mock_get_key, mock_utils, mock_env
  ):
    """Test backup logs warning for non-shared repository."""
    mock_env.ENVIRONMENT = "dev"
    mock_utils.is_shared_repository.return_value = False
    mock_utils.get_database_name.return_value = "custom_db"
    mock_get_key.return_value = "backups/custom/2025-01-01.tar.gz"

    mock_s3_adapter = MagicMock()
    mock_s3_adapter.bucket_name = "test-bucket"
    mock_s3_adapter_cls.return_value = mock_s3_adapter

    mock_backup_record = MagicMock()
    mock_backup_record.id = "backup-456"
    mock_backup_cls.create.return_value = mock_backup_record

    mock_db, _ = _make_mock_db_resource()
    mock_s3 = _make_mock_s3_resource()

    config = SECBackupConfig(graph_id="custom_graph")
    context = build_asset_context()

    # Should complete without error (warning only) - dev skips backup
    result = sec_backup(context, config, mock_db, mock_s3)
    assert isinstance(result, MaterializeResult)
