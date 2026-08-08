"""Unit tests for backup API models."""

import pytest
from pydantic import ValidationError

from robosystems.models.api.graphs.backups import (
  BackupCreateRequest,
  BackupDownloadUrlResponse,
  BackupExportRequest,
  BackupListResponse,
  BackupResponse,
  BackupRestoreRequest,
  BackupStatsResponse,
)


@pytest.mark.unit
class TestBackupCreateRequest:
  def test_defaults(self):
    model = BackupCreateRequest()
    assert model.backup_format == "full_dump"
    assert model.backup_type == "full"
    assert model.retention_days == 30
    assert model.compression is True
    assert model.schedule is None

  def test_invalid_backup_format(self):
    with pytest.raises(ValidationError) as exc_info:
      BackupCreateRequest(backup_format="incremental")
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("backup_format",) for e in errors)

  def test_invalid_backup_type(self):
    with pytest.raises(ValidationError) as exc_info:
      BackupCreateRequest(backup_type="incremental")
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("backup_type",) for e in errors)

  def test_retention_days_minimum(self):
    model = BackupCreateRequest(retention_days=1)
    assert model.retention_days == 1

  def test_retention_days_below_minimum(self):
    with pytest.raises(ValidationError):
      BackupCreateRequest(retention_days=0)

  def test_retention_days_maximum(self):
    """90 is the ceiling: the S3 lifecycle expires backup objects at 90 days,
    so accepting more (this once allowed 2555) produced COMPLETED backup
    records pointing at objects the lifecycle had already deleted."""
    model = BackupCreateRequest(retention_days=90)
    assert model.retention_days == 90

  def test_retention_days_above_maximum(self):
    with pytest.raises(ValidationError):
      BackupCreateRequest(retention_days=91)

  def test_compression_must_be_true(self):
    with pytest.raises(ValidationError) as exc_info:
      BackupCreateRequest(compression=False)
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("compression",) for e in errors)
    assert any("Compression must be enabled" in str(e.get("msg", "")) for e in errors)

  def test_compression_true_accepted(self):
    model = BackupCreateRequest(compression=True)
    assert model.compression is True

  def test_schedule_optional(self):
    model = BackupCreateRequest(schedule="0 2 * * *")
    assert model.schedule == "0 2 * * *"


@pytest.mark.unit
class TestBackupExportRequest:
  def test_valid_request(self):
    model = BackupExportRequest(backup_id="bk_abc123")
    assert model.backup_id == "bk_abc123"
    assert model.export_format == "original"

  def test_invalid_export_format(self):
    with pytest.raises(ValidationError) as exc_info:
      BackupExportRequest(backup_id="bk_abc", export_format="csv")
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("export_format",) for e in errors)

  def test_backup_id_required(self):
    with pytest.raises(ValidationError):
      BackupExportRequest()  # type: ignore[call-arg]


@pytest.mark.unit
class TestBackupRestoreRequest:
  def test_defaults(self):
    model = BackupRestoreRequest()
    assert model.create_system_backup is True
    assert model.verify_after_restore is True

  def test_no_system_backup(self):
    model = BackupRestoreRequest(
      create_system_backup=False,
      verify_after_restore=False,
    )
    assert model.create_system_backup is False
    assert model.verify_after_restore is False


@pytest.mark.unit
class TestBackupResponse:
  def test_valid_response(self):
    model = BackupResponse(
      backup_id="bk_123",
      graph_id="kg1234567890abcdef",
      backup_format="full_dump",
      backup_type="full",
      status="completed",
      original_size_bytes=1048576,
      compressed_size_bytes=524288,
      compression_ratio=0.5,
      node_count=100,
      relationship_count=200,
      backup_duration_seconds=5.5,
      compression_enabled=True,
      created_at="2024-01-01T00:00:00Z",
      completed_at="2024-01-01T00:00:05Z",
      expires_at="2024-02-01T00:00:00Z",
    )
    assert model.compression_ratio == 0.5
    assert model.node_count == 100


@pytest.mark.unit
class TestBackupListResponse:
  def test_empty_list(self):
    model = BackupListResponse(
      backups=[],
      total_count=0,
      graph_id="kg1234567890abcdef",
    )
    assert model.total_count == 0
    assert model.is_shared_repository is False
    assert model.download_quota is None


@pytest.mark.unit
class TestBackupStatsResponse:
  def test_valid_stats(self):
    model = BackupStatsResponse(
      graph_id="kg1234567890abcdef",
      total_backups=10,
      successful_backups=9,
      failed_backups=1,
      success_rate=0.9,
      total_original_size_bytes=10485760,
      total_compressed_size_bytes=5242880,
      storage_saved_bytes=5242880,
      average_compression_ratio=0.5,
      latest_backup_date="2024-01-15T00:00:00Z",
      backup_formats={"full_dump": 10},
    )
    assert model.success_rate == 0.9


@pytest.mark.unit
class TestBackupDownloadUrlResponse:
  def test_valid_response(self):
    model = BackupDownloadUrlResponse(
      download_url="https://s3.amazonaws.com/bucket/backup.tar.gz?signed=true",
      expires_in=3600,
      expires_at=1705315200.0,
      backup_id="bk_123",
      graph_id="kg1234567890abcdef",
    )
    assert model.expires_in == 3600

  def test_expires_in_minimum(self):
    model = BackupDownloadUrlResponse(
      download_url="https://example.com",
      expires_in=300,
      expires_at=1705315200.0,
      backup_id="bk_123",
      graph_id="kg123",
    )
    assert model.expires_in == 300

  def test_expires_in_below_minimum(self):
    with pytest.raises(ValidationError):
      BackupDownloadUrlResponse(
        download_url="https://example.com",
        expires_in=299,
        expires_at=1705315200.0,
        backup_id="bk_123",
        graph_id="kg123",
      )

  def test_expires_in_maximum(self):
    model = BackupDownloadUrlResponse(
      download_url="https://example.com",
      expires_in=86400,
      expires_at=1705315200.0,
      backup_id="bk_123",
      graph_id="kg123",
    )
    assert model.expires_in == 86400

  def test_expires_in_above_maximum(self):
    with pytest.raises(ValidationError):
      BackupDownloadUrlResponse(
        download_url="https://example.com",
        expires_in=86401,
        expires_at=1705315200.0,
        backup_id="bk_123",
        graph_id="kg123",
      )
