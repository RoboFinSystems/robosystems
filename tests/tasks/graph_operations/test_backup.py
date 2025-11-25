"""Tests for graph backup Celery tasks."""

import pytest
from unittest.mock import MagicMock, patch

from robosystems.tasks.graph_operations.backup import (
  cleanup_expired_backups,
  backup_retention_management,
  backup_health_check,
  restore_graph_backup,
  delete_single_backup,
  generate_backup_metrics,
  create_graph_backup,
  restore_graph_backup_sse,
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
  @patch("robosystems.middleware.graph.utils.MultiTenantUtils")
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


class TestBackupRetentionManagementTask:
  """Test cases for backup retention management task."""

  @patch("robosystems.operations.lbug.backup_manager.BackupManager")
  @patch("robosystems.tasks.graph_operations.backup.S3BackupAdapter")
  @patch("robosystems.tasks.graph_operations.backup.asyncio")
  def test_retention_specific_graph(
    self,
    mock_asyncio,
    mock_s3_adapter,
    mock_backup_manager_class,
  ):
    """Test retention management for specific graph."""
    mock_backup_manager = MagicMock()
    mock_backup_manager_class.return_value = mock_backup_manager
    mock_asyncio.run.return_value = 5

    result = backup_retention_management.apply(  # type: ignore[attr-defined]
      args=(),
      kwargs={"graph_id": "kg123456", "retention_days": 90, "dry_run": False},
    ).get()

    assert result["graph_id"] == "kg123456"
    assert result["retention_days"] == 90
    assert result["deleted_count"] == 5
    assert result["dry_run"] is False

  @patch("robosystems.operations.lbug.backup_manager.BackupManager")
  @patch("robosystems.tasks.graph_operations.backup.GraphBackup")
  @patch("robosystems.tasks.graph_operations.backup.S3BackupAdapter")
  @patch("robosystems.tasks.graph_operations.backup.asyncio")
  def test_retention_all_graphs(
    self,
    mock_asyncio,
    mock_s3_adapter,
    mock_backup_model,
    mock_backup_manager_class,
  ):
    """Test retention management for all graphs."""
    mock_backup1 = MagicMock()
    mock_backup1.graph_id = "kg111"
    mock_backup2 = MagicMock()
    mock_backup2.graph_id = "kg222"
    mock_backup_model.query.all.return_value = [mock_backup1, mock_backup2]

    mock_backup_manager = MagicMock()
    mock_backup_manager_class.return_value = mock_backup_manager

    mock_asyncio.run.side_effect = [3, 2]

    result = backup_retention_management.apply(  # type: ignore[attr-defined]
      args=(),
      kwargs={"retention_days": 30, "dry_run": False},
    ).get()

    assert result["graph_id"] is None
    assert result["deleted_count"] == 5
    assert result["retention_days"] == 30

  @patch("robosystems.operations.lbug.backup_manager.BackupManager")
  @patch("robosystems.tasks.graph_operations.backup.S3BackupAdapter")
  @patch("robosystems.tasks.graph_operations.backup.asyncio")
  def test_retention_dry_run(
    self,
    mock_asyncio,
    mock_s3_adapter,
    mock_backup_manager_class,
  ):
    """Test retention management in dry run mode."""
    from datetime import datetime, timezone

    mock_backup_manager = MagicMock()
    mock_backup_manager_class.return_value = mock_backup_manager

    mock_backup_info = [
      {"last_modified": datetime(2023, 1, 1, tzinfo=timezone.utc)},
      {"last_modified": datetime(2023, 1, 2, tzinfo=timezone.utc)},
    ]
    mock_asyncio.run.return_value = mock_backup_info

    result = backup_retention_management.apply(  # type: ignore[attr-defined]
      args=(),
      kwargs={"graph_id": "kg123456", "retention_days": 90, "dry_run": True},
    ).get()

    assert result["dry_run"] is True
    assert result["deleted_count"] == 2


class TestCreateGraphBackupTask:
  """Test cases for SSE-enabled graph backup creation task."""

  @patch("robosystems.middleware.sse.task_progress.TaskSSEProgressTracker")
  @patch("robosystems.operations.lbug.backup_manager.create_backup_manager")
  @patch("robosystems.middleware.graph.utils.MultiTenantUtils")
  @patch("robosystems.tasks.graph_operations.backup.GraphBackup")
  @patch("robosystems.tasks.graph_operations.backup.S3BackupAdapter")
  @patch("robosystems.tasks.graph_operations.backup.session")
  @patch("robosystems.tasks.graph_operations.backup.asyncio")
  def test_successful_backup_with_sse(
    self,
    mock_asyncio,
    mock_session,
    mock_s3_adapter,
    mock_backup_model,
    mock_utils,
    mock_create_backup_manager,
    mock_tracker_class,
  ):
    """Test successful backup creation with SSE progress tracking."""
    mock_tracker = MagicMock()
    mock_tracker_class.return_value = mock_tracker

    mock_utils.is_shared_repository.return_value = False
    mock_utils.get_database_name.return_value = "kg123456_db"

    mock_s3 = MagicMock()
    mock_s3.bucket_name = "test-bucket"
    mock_s3_adapter.return_value = mock_s3

    mock_backup_record = MagicMock()
    mock_backup_record.id = "backup123"
    mock_backup_record.s3_key = "old-key"
    mock_backup_model.create.return_value = mock_backup_record

    mock_backup_manager = MagicMock()
    mock_backup_info = MagicMock()
    mock_backup_info.s3_key = (
      "graph-backups/databases/kg123456/full/backup-20240101_120000.lbug.zip.gz.enc"
    )
    mock_backup_info.original_size = 1000
    mock_backup_info.compressed_size = 500
    mock_backup_info.checksum = "abc123"
    mock_backup_info.node_count = 100
    mock_backup_info.relationship_count = 50
    mock_backup_info.backup_duration_seconds = 10.5
    mock_backup_info.backup_format = "full_dump"
    mock_backup_info.compression_ratio = 0.5
    mock_backup_info.is_encrypted = True
    mock_backup_info.encryption_method = "AES-256-GCM"
    mock_asyncio.run.return_value = mock_backup_info
    mock_create_backup_manager.return_value = mock_backup_manager

    result = create_graph_backup.apply(  # type: ignore[attr-defined]
      args=(),
      kwargs={
        "graph_id": "kg123456",
        "backup_type": "full",
        "user_id": "user123",
        "operation_id": "op123",
      },
    ).get()

    assert result["graph_id"] == "kg123456"
    assert result["backup_id"] == "backup123"
    assert result["backup_type"] == "full"
    mock_tracker.emit_progress.assert_called()
    mock_tracker.emit_completion.assert_called_once()

  @patch("robosystems.middleware.sse.task_progress.TaskSSEProgressTracker")
  @patch("robosystems.operations.lbug.backup_manager.create_backup_manager")
  @patch("robosystems.middleware.graph.utils.MultiTenantUtils")
  @patch("robosystems.tasks.graph_operations.backup.GraphBackup")
  @patch("robosystems.tasks.graph_operations.backup.S3BackupAdapter")
  @patch("robosystems.tasks.graph_operations.backup.session")
  @patch("robosystems.tasks.graph_operations.backup.asyncio")
  def test_backup_with_different_format(
    self,
    mock_asyncio,
    mock_session,
    mock_s3_adapter,
    mock_backup_model,
    mock_utils,
    mock_create_backup_manager,
    mock_tracker_class,
  ):
    """Test backup creation with different formats (CSV, JSON, Parquet)."""
    mock_tracker = MagicMock()
    mock_tracker_class.return_value = mock_tracker

    mock_utils.is_shared_repository.return_value = False
    mock_utils.get_database_name.return_value = "kg123456_db"

    mock_s3 = MagicMock()
    mock_s3.bucket_name = "test-bucket"
    mock_s3_adapter.return_value = mock_s3

    mock_backup_record = MagicMock()
    mock_backup_record.id = "backup123"
    mock_backup_record.s3_key = "old-key"
    mock_backup_model.create.return_value = mock_backup_record

    mock_backup_manager = MagicMock()
    mock_backup_info = MagicMock()
    mock_backup_info.s3_key = "graph-backups/databases/kg123456/full/backup.csv.zip"
    mock_backup_info.original_size = 2000
    mock_backup_info.compressed_size = 800
    mock_backup_info.checksum = "def456"
    mock_backup_info.node_count = 200
    mock_backup_info.relationship_count = 100
    mock_backup_info.backup_duration_seconds = 15.0
    mock_backup_info.backup_format = "csv"
    mock_backup_info.compression_ratio = 0.4
    mock_backup_info.is_encrypted = False
    mock_backup_info.encryption_method = None
    mock_asyncio.run.return_value = mock_backup_info
    mock_create_backup_manager.return_value = mock_backup_manager

    result = create_graph_backup.apply(  # type: ignore[attr-defined]
      args=(),
      kwargs={
        "graph_id": "kg123456",
        "backup_type": "full",
        "backup_format": "csv",
        "encryption": False,
        "operation_id": "op456",
      },
    ).get()

    assert result["backup_format"] == "csv"
    assert result["s3_key"] == "graph-backups/databases/kg123456/full/backup.csv.zip"

  @patch("robosystems.middleware.sse.task_progress.TaskSSEProgressTracker")
  @patch("robosystems.middleware.graph.utils.MultiTenantUtils")
  def test_backup_error_handling(
    self,
    mock_utils,
    mock_tracker_class,
  ):
    """Test error handling during backup creation."""
    mock_tracker = MagicMock()
    mock_tracker_class.return_value = mock_tracker

    mock_utils.is_shared_repository.return_value = False
    mock_utils.validate_graph_id.side_effect = ValueError("Invalid graph ID")

    with pytest.raises(ValueError) as exc_info:
      create_graph_backup.apply(  # type: ignore[attr-defined]
        args=(),
        kwargs={
          "graph_id": "invalid",
          "operation_id": "op789",
        },
      ).get()

    assert "Invalid graph ID" in str(exc_info.value)
    mock_tracker.emit_error.assert_called_once()


class TestRestoreGraphBackupSSETask:
  """Test cases for SSE-enabled graph backup restore task."""

  @patch("robosystems.middleware.sse.task_progress.TaskSSEProgressTracker")
  @patch("robosystems.tasks.graph_operations.backup.GraphClientFactory")
  @patch("robosystems.tasks.graph_operations.backup.GraphBackup")
  @patch("robosystems.tasks.graph_operations.backup.session")
  @patch("robosystems.tasks.graph_operations.backup.asyncio")
  def test_successful_restore_with_sse(
    self,
    mock_asyncio,
    mock_session,
    mock_backup_model,
    mock_client_factory,
    mock_tracker_class,
  ):
    """Test successful restore with SSE progress tracking."""
    mock_tracker = MagicMock()
    mock_tracker_class.return_value = mock_tracker

    mock_backup_record = MagicMock()
    mock_backup_record.id = "backup123"
    mock_backup_record.graph_id = "kg123456"
    mock_backup_record.s3_bucket = "test-bucket"
    mock_backup_record.s3_key = "backups/test.lbug"
    mock_backup_record.encryption_enabled = True
    mock_backup_record.compression_enabled = True
    mock_backup_model.get_by_id.return_value = mock_backup_record

    mock_client = MagicMock()
    mock_client.close = MagicMock()
    mock_restore_result = {"status": "completed"}

    call_count = {"count": 0}

    def asyncio_side_effect(coro):
      call_count["count"] += 1
      if call_count["count"] == 1:
        return mock_client
      elif call_count["count"] == 2:
        return mock_restore_result
      elif call_count["count"] == 3:
        return None
      return None

    mock_asyncio.run.side_effect = asyncio_side_effect

    result = restore_graph_backup_sse.apply(  # type: ignore[attr-defined]
      args=(),
      kwargs={
        "graph_id": "kg123456",
        "backup_id": "backup123",
        "user_id": "user123",
        "create_system_backup": False,
        "operation_id": "op123",
      },
    ).get()

    assert result["graph_id"] == "kg123456"
    assert result["backup_id"] == "backup123"
    assert result["status"] == "completed"
    mock_tracker.emit_progress.assert_called()
    mock_tracker.emit_completion.assert_called_once()

  @patch("robosystems.middleware.sse.task_progress.TaskSSEProgressTracker")
  @patch("robosystems.operations.lbug.backup_manager.create_backup_manager")
  @patch("robosystems.tasks.graph_operations.backup.GraphClientFactory")
  @patch("robosystems.tasks.graph_operations.backup.GraphBackup")
  @patch("robosystems.tasks.graph_operations.backup.session")
  @patch("robosystems.tasks.graph_operations.backup.asyncio")
  def test_restore_with_system_backup(
    self,
    mock_asyncio,
    mock_session,
    mock_backup_model,
    mock_client_factory,
    mock_create_backup_manager,
    mock_tracker_class,
  ):
    """Test restore with system backup creation before restore."""
    mock_tracker = MagicMock()
    mock_tracker_class.return_value = mock_tracker

    mock_backup_record = MagicMock()
    mock_backup_record.id = "backup123"
    mock_backup_record.graph_id = "kg123456"
    mock_backup_record.s3_bucket = "test-bucket"
    mock_backup_record.s3_key = "backups/test.lbug"
    mock_backup_record.encryption_enabled = True
    mock_backup_record.compression_enabled = True
    mock_backup_model.get_by_id.return_value = mock_backup_record

    mock_backup_manager = MagicMock()
    mock_system_backup = MagicMock()
    mock_system_backup.s3_key = "system-backup-key"

    mock_client = MagicMock()
    mock_restore_result = {"status": "completed"}

    call_count = {"count": 0}

    def asyncio_side_effect(coro):
      call_count["count"] += 1
      if call_count["count"] == 1:
        return mock_system_backup
      elif call_count["count"] == 2:
        return mock_client
      elif call_count["count"] == 3:
        return mock_restore_result
      elif call_count["count"] == 4:
        return None
      return None

    mock_asyncio.run.side_effect = asyncio_side_effect
    mock_create_backup_manager.return_value = mock_backup_manager

    result = restore_graph_backup_sse.apply(  # type: ignore[attr-defined]
      args=(),
      kwargs={
        "graph_id": "kg123456",
        "backup_id": "backup123",
        "create_system_backup": True,
        "operation_id": "op456",
      },
    ).get()

    assert result["system_backup_created"] is True

  @patch("robosystems.middleware.sse.task_progress.TaskSSEProgressTracker")
  @patch("robosystems.tasks.graph_operations.backup.GraphBackup")
  @patch("robosystems.tasks.graph_operations.backup.session")
  def test_restore_backup_not_found(
    self,
    mock_session,
    mock_backup_model,
    mock_tracker_class,
  ):
    """Test restore when backup is not found."""
    mock_tracker = MagicMock()
    mock_tracker_class.return_value = mock_tracker

    mock_backup_model.get_by_id.return_value = None

    with pytest.raises(ValueError) as exc_info:
      restore_graph_backup_sse.apply(  # type: ignore[attr-defined]
        args=(),
        kwargs={
          "graph_id": "kg123456",
          "backup_id": "nonexistent",
          "operation_id": "op789",
        },
      ).get()

    assert "not found" in str(exc_info.value).lower()
    mock_tracker.emit_error.assert_called_once()

  @patch("robosystems.middleware.sse.task_progress.TaskSSEProgressTracker")
  @patch("robosystems.tasks.graph_operations.backup.GraphClientFactory")
  @patch("robosystems.tasks.graph_operations.backup.GraphBackup")
  @patch("robosystems.tasks.graph_operations.backup.session")
  @patch("robosystems.tasks.graph_operations.backup.asyncio")
  def test_restore_graph_id_mismatch(
    self,
    mock_asyncio,
    mock_session,
    mock_backup_model,
    mock_client_factory,
    mock_tracker_class,
  ):
    """Test restore when backup belongs to different graph."""
    mock_tracker = MagicMock()
    mock_tracker_class.return_value = mock_tracker

    mock_backup_record = MagicMock()
    mock_backup_record.id = "backup123"
    mock_backup_record.graph_id = "kg999999"
    mock_backup_model.get_by_id.return_value = mock_backup_record

    with pytest.raises(ValueError) as exc_info:
      restore_graph_backup_sse.apply(  # type: ignore[attr-defined]
        args=(),
        kwargs={
          "graph_id": "kg123456",
          "backup_id": "backup123",
          "operation_id": "op123",
        },
      ).get()

    assert "does not belong" in str(exc_info.value).lower()
    mock_tracker.emit_error.assert_called_once()
