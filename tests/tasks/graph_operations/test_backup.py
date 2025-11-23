"""Tests for graph backup Celery tasks."""

import pytest
from unittest.mock import MagicMock, patch

from robosystems.tasks.graph_operations.backup import (
  cleanup_expired_backups,
  backup_health_check,
  restore_graph_backup,
  delete_single_backup,
  generate_backup_metrics,
)


class TestCleanupExpiredBackupsTask:
  """Test cases for expired backup cleanup task."""

  @patch("robosystems.tasks.graph_operations.backup.asyncio")
  @patch("robosystems.tasks.graph_operations.backup.GraphBackup")
  @patch("robosystems.tasks.graph_operations.backup.S3BackupAdapter")
  @patch("robosystems.tasks.graph_operations.backup.session")
  def test_successful_cleanup(
    self,
    mock_session,
    mock_s3_adapter,
    mock_backup_model,
    mock_asyncio,
  ):
    """Test successful cleanup of expired backups."""
    mock_expired_backup = MagicMock()
    mock_expired_backup.id = "expired1"
    mock_expired_backup.s3_key = "backups/expired1.db"
    mock_expired_backup.graph_id = "graph1"
    mock_expired_backup.backup_type = "full"
    mock_expired_backup.encrypted_size_bytes = 500
    mock_expired_backup.compression_enabled = False
    mock_backup_model.get_expired_backups.return_value = [mock_expired_backup]

    mock_s3 = MagicMock()
    mock_s3_adapter.return_value = mock_s3
    mock_asyncio.run.return_value = True

    result = cleanup_expired_backups.apply(args=(), kwargs={}).get()  # type: ignore[attr-defined]

    assert "deleted_count" in result
    assert result["deleted_count"] >= 0
    assert "expired_backups_found" in result


class TestBackupHealthCheckTask:
  """Test cases for backup health check task."""

  @patch("robosystems.operations.lbug.backup_manager.BackupManager")
  @patch("robosystems.tasks.graph_operations.backup.GraphBackup")
  @patch("robosystems.tasks.graph_operations.backup.S3BackupAdapter")
  @patch("robosystems.tasks.graph_operations.backup.session")
  def test_health_check_all_graphs(
    self,
    mock_session,
    mock_s3_adapter,
    mock_backup_model,
    mock_backup_manager_class,
  ):
    """Test health check for all graphs."""
    mock_backup_manager = MagicMock()
    mock_backup_manager.health_check.return_value = {
      "s3": {"status": "healthy"},
      "graph": {"status": "healthy"},
    }
    mock_backup_manager_class.return_value = mock_backup_manager

    mock_backup_model.get_pending_backups.return_value = []

    result = backup_health_check.apply(args=(), kwargs={}).get()  # type: ignore[attr-defined]

    assert "overall_status" in result
    assert result["overall_status"] in ["healthy", "unhealthy"]


class TestRestoreGraphBackupTask:
  """Test cases for graph backup restore task."""

  @patch("robosystems.security.encryption.decrypt_data")
  @patch("gzip.decompress")
  @patch("robosystems.tasks.graph_operations.backup.GraphClientFactory")
  @patch("robosystems.tasks.graph_operations.backup.S3BackupAdapter")
  @patch("robosystems.tasks.graph_operations.backup.GraphBackup")
  @patch("robosystems.middleware.graph.multitenant_utils.MultiTenantUtils")
  @patch("robosystems.tasks.graph_operations.backup.asyncio")
  @patch("robosystems.tasks.graph_operations.backup.session")
  def test_successful_restore(
    self,
    mock_session,
    mock_asyncio,
    mock_utils,
    mock_backup_model,
    mock_s3_adapter,
    mock_client_factory,
    mock_gzip_decompress,
    mock_decrypt,
  ):
    """Test successful backup restore."""
    mock_backup_record = MagicMock()
    mock_backup_record.graph_id = "graph1"
    mock_backup_record.encryption_enabled = True
    mock_backup_record.compression_enabled = True
    mock_backup_record.s3_key = "key1"
    mock_backup_record.backup_metadata = {"backup_format": "full_dump"}

    mock_backup_model.get_by_id.return_value = mock_backup_record

    mock_utils.is_shared_repository.return_value = False
    mock_utils.validate_graph_id.return_value = None
    mock_utils.get_database_name.return_value = "graph1_db"

    mock_s3 = MagicMock()
    mock_s3_adapter.return_value = mock_s3

    mock_client = MagicMock()
    mock_asyncio.run.side_effect = [
      b"encrypted_data",
      mock_client,
      None,
      None,
      None,
    ]

    mock_decrypt.return_value = b"compressed_data"
    mock_gzip_decompress.return_value = b"restored_data"

    result = restore_graph_backup.apply(  # type: ignore[attr-defined]
      args=(),
      kwargs={
        "graph_id": "graph1",
        "backup_id": "backup123",
        "verify_after_restore": False,
      },
    ).get()

    assert result["status"] == "completed"

  @patch("robosystems.tasks.graph_operations.backup.GraphBackup")
  @patch("robosystems.tasks.graph_operations.backup.session")
  def test_restore_backup_not_found(
    self,
    mock_session,
    mock_backup_model,
  ):
    """Test restore when backup is not found."""
    mock_backup_model.get_by_id.return_value = None

    with pytest.raises(ValueError) as exc_info:
      restore_graph_backup.apply(  # type: ignore[attr-defined]
        args=(),
        kwargs={"graph_id": "graph1", "backup_id": "nonexistent"},
      ).get()

    assert "not found" in str(exc_info.value).lower()


class TestDeleteSingleBackupTask:
  """Test cases for single backup deletion task."""

  @patch("robosystems.tasks.graph_operations.backup.asyncio")
  @patch("robosystems.tasks.graph_operations.backup.GraphBackup")
  @patch("robosystems.tasks.graph_operations.backup.S3BackupAdapter")
  @patch("robosystems.tasks.graph_operations.backup.session")
  def test_successful_deletion(
    self,
    mock_session,
    mock_s3_adapter,
    mock_backup_model,
    mock_asyncio,
  ):
    """Test successful backup deletion."""
    mock_backup = MagicMock()
    mock_backup.id = "backup1"
    mock_backup.s3_key = "backups/backup1.db"
    mock_backup.graph_id = "graph1"
    mock_backup.backup_type = "full"
    mock_backup.encrypted_size_bytes = 1000
    mock_backup.compression_enabled = True
    mock_backup_model.get_by_id.return_value = mock_backup

    mock_s3 = MagicMock()
    mock_s3_adapter.return_value = mock_s3
    mock_asyncio.run.return_value = True

    result = delete_single_backup.apply(  # type: ignore[attr-defined]
      args=(),
      kwargs={"backup_id": "backup1"},
    ).get()

    assert result["status"] == "deleted"
    assert result["backup_id"] == "backup1"


class TestGenerateBackupMetricsTask:
  """Test cases for backup metrics generation task."""

  @patch("robosystems.tasks.graph_operations.backup.GraphBackup")
  @patch("robosystems.tasks.graph_operations.backup.session")
  def test_metrics_for_all_graphs(
    self,
    mock_session,
    mock_backup_model,
  ):
    """Test metrics generation for all graphs."""
    mock_session.query.return_value.all.return_value = []
    mock_session.query.return_value.scalar.return_value = 0
    mock_session.query.return_value.filter.return_value.scalar.return_value = 0

    result = generate_backup_metrics.apply(args=(), kwargs={}).get()  # type: ignore[attr-defined]

    assert "graphs" in result
    assert "overall" in result
    assert "generated_at" in result
