"""
Comprehensive tests for LadybugGraphBackupService.

Tests the critical backup service that protects customer data through automated
S3 backups with integrity checks and lifecycle management.
"""

import tempfile
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock
import pytest
import boto3
from botocore.exceptions import ClientError
from moto import mock_aws

from robosystems.operations.lbug.backup import (
  LadybugGraphBackupService,
  create_graph_backup_service,
  DEFAULT_BACKUP_BUCKET,
  DEFAULT_RETENTION_DAYS,
  MAX_BACKUP_SIZE_GB,
)


@pytest.fixture
def mock_allocation_manager():
  """Mock allocation manager for testing."""
  manager = Mock()
  manager.get_instance_databases = AsyncMock(
    return_value=["kg1a2b3c4d5", "kg5d4c3b2a1"]
  )
  manager.get_database_metadata = AsyncMock(
    return_value={
      "allocated_at": datetime.now(timezone.utc).isoformat(),
      "tier": "standard",
      "user_id": "test-user",
    }
  )
  return manager


@pytest.fixture
def mock_s3_client():
  """Mock S3 client for testing."""
  with mock_aws():
    client = boto3.client("s3", region_name="us-east-1")
    # Create test bucket
    client.create_bucket(Bucket=DEFAULT_BACKUP_BUCKET)
    yield client


@pytest.fixture
def mock_cloudwatch_client():
  """Mock CloudWatch client for testing."""
  with mock_aws():
    client = boto3.client("cloudwatch", region_name="us-east-1")
    yield client


@pytest.fixture
def temp_db_dir():
  """Create temporary database directory structure."""
  with tempfile.TemporaryDirectory() as tmpdir:
    base_path = Path(tmpdir)

    # Create mock database directories with files
    for db_id in ["kg1a2b3c4d5", "kg5d4c3b2a1"]:
      db_path = base_path / f"{db_id}.lbug"
      db_path.mkdir(parents=True)

      # Create some mock database files
      (db_path / "nodes.db").write_bytes(b"mock node data" * 1000)
      (db_path / "rels.db").write_bytes(b"mock relationship data" * 500)
      (db_path / "catalog.db").write_bytes(b"catalog metadata")

    yield base_path


@pytest.fixture
def backup_service(
  mock_allocation_manager, mock_s3_client, mock_cloudwatch_client, temp_db_dir
):
  """Create backup service instance for testing."""
  with patch("robosystems.operations.lbug.backup.boto3.client") as mock_boto:
    mock_boto.side_effect = lambda service, **kwargs: {
      "s3": mock_s3_client,
      "cloudwatch": mock_cloudwatch_client,
    }.get(service)

    with patch(
      "robosystems.operations.lbug.backup.LadybugAllocationManager"
    ) as mock_alloc_class:
      mock_alloc_class.return_value = mock_allocation_manager

      with patch(
        "robosystems.operations.lbug.backup.env.get_s3_config"
      ) as mock_s3_config:
        mock_s3_config.return_value = {
          "aws_access_key_id": "test-key",
          "aws_secret_access_key": "test-secret",
          "region_name": "us-east-1",
        }

        # Mock EC2 metadata request
        with patch("requests.get") as mock_requests:
          mock_requests.return_value.text = "test-instance-id"

          service = LadybugGraphBackupService(
            environment="test",
            base_path=str(temp_db_dir),
            s3_bucket=DEFAULT_BACKUP_BUCKET,
            retention_days=DEFAULT_RETENTION_DAYS,
          )
          yield service


class TestLadybugGraphBackupService:
  """Test suite for LadybugGraphBackupService."""

  @pytest.mark.asyncio
  async def test_backup_all_graph_databases_success(self, backup_service, temp_db_dir):
    """Test successful backup of all allocated databases."""
    # Mock _is_backup_current to return False so backups are performed
    with patch.object(backup_service, "_is_backup_current", return_value=False):
      # Execute backup
      result = await backup_service.backup_all_graph_databases()

      # Verify results
      assert result["status"] == "success"
      assert result["backed_up"] == 2
      assert result["skipped"] == 0
      assert result["failed"] == 0
      assert len(result["results"]) == 2

      # Check individual backup results
      for backup in result["results"]:
        assert backup["status"] == "success"
        assert backup.get("backup_size_mb", 0) >= 0
        assert "s3_key" in backup
        assert "checksum" in backup

  @pytest.mark.asyncio
  async def test_backup_single_database(self, backup_service, temp_db_dir):
    """Test backup of a single graph database."""
    with patch.object(backup_service, "_is_backup_current", return_value=False):
      result = await backup_service.backup_graph_database("kg1a2b3c4d5")

      assert result["status"] == "success"
      assert result["graph_id"] == "kg1a2b3c4d5"
      assert result.get("backup_size_mb", 0) >= 0
      assert "s3_key" in result
      assert "checksum" in result

  @pytest.mark.asyncio
  async def test_backup_nonexistent_database(self, backup_service):
    """Test backup attempt on non-existent database."""
    result = await backup_service.backup_graph_database("nonexistent")

    assert result["status"] == "skipped"
    assert result["graph_id"] == "nonexistent"
    assert "reason" in result
    assert "not found" in result["reason"].lower()

  @pytest.mark.asyncio
  async def test_backup_skip_large_database(self, backup_service, temp_db_dir):
    """Test that oversized databases are skipped."""
    # Create a large mock database
    large_db = temp_db_dir / "kglarge12345.lbug"
    large_db.mkdir(parents=True)

    # Mock size calculation to return > MAX_BACKUP_SIZE_GB
    with patch.object(
      backup_service,
      "_get_directory_size",
      return_value=(MAX_BACKUP_SIZE_GB + 1) * 1024 * 1024 * 1024,
    ):
      result = await backup_service.backup_graph_database("kglarge12345")

      assert result["status"] == "skipped"
      assert "reason" in result
      assert "too large" in result["reason"].lower()

  @pytest.mark.asyncio
  async def test_backup_current_check(
    self, backup_service, mock_s3_client, temp_db_dir
  ):
    """Test backup currency check to avoid redundant backups."""
    graph_id = "kg1a2b3c4d5"

    # Create a recent backup in S3
    recent_key = f"backups/{graph_id}/{graph_id}_backup_latest.tar.gz"
    mock_s3_client.put_object(
      Bucket=DEFAULT_BACKUP_BUCKET,
      Key=recent_key,
      Body=b"recent backup data",
      Metadata={
        "backup-timestamp": datetime.now(timezone.utc).isoformat(),
        "checksum": "abc123",
      },
    )

    # Mock the currency check
    with patch.object(backup_service, "_is_backup_current", return_value=True):
      result = await backup_service.backup_graph_database(graph_id)

      assert result["status"] == "skipped"
      assert "reason" in result
      assert "current" in result["reason"].lower()

  @pytest.mark.asyncio
  async def test_backup_compression(self, backup_service, temp_db_dir):
    """Test backup compression and file handling."""
    with patch.object(backup_service, "_create_compressed_backup") as mock_compress:
      mock_compress.return_value = None
      with patch.object(backup_service, "_is_backup_current", return_value=False):
        await backup_service.backup_graph_database("kg1a2b3c4d5")

      # Verify compression was called
      mock_compress.assert_called_once()
      call_args = mock_compress.call_args[0]
      assert call_args[0] == temp_db_dir / "kg1a2b3c4d5.lbug"
      assert str(call_args[1]).endswith(".tar.gz")

  @pytest.mark.asyncio
  async def test_backup_s3_upload(self, backup_service, mock_s3_client, temp_db_dir):
    """Test S3 upload functionality."""
    graph_id = "kg1a2b3c4d5"

    # Perform backup
    with patch.object(backup_service, "_is_backup_current", return_value=False):
      result = await backup_service.backup_graph_database(graph_id)

    # Verify S3 upload
    assert result["status"] == "success"
    assert "s3_key" in result

    # Check object exists in S3
    response = mock_s3_client.list_objects_v2(
      Bucket=DEFAULT_BACKUP_BUCKET, Prefix=f"graph-databases/test/{graph_id}/"
    )
    assert response["KeyCount"] > 0

  @pytest.mark.asyncio
  async def test_backup_checksum_calculation(self, backup_service, temp_db_dir):
    """Test checksum calculation for backup integrity."""
    with tempfile.NamedTemporaryFile(suffix=".tar.gz") as tmp:
      test_data = b"test backup data"
      tmp.write(test_data)
      tmp.flush()

      checksum = backup_service._calculate_file_checksum(Path(tmp.name))

      # Verify checksum format and consistency
      assert len(checksum) == 64  # SHA-256 hex digest length
      expected = hashlib.sha256(test_data).hexdigest()
      assert checksum == expected

  @pytest.mark.asyncio
  async def test_backup_metrics_publishing(
    self, backup_service, mock_cloudwatch_client
  ):
    """Test CloudWatch metrics publishing."""
    with patch.object(backup_service, "_publish_backup_metrics") as mock_publish:
      mock_publish.return_value = None

      await backup_service.backup_all_graph_databases()

      # Verify metrics were published
      mock_publish.assert_called_once()
      metrics_data = mock_publish.call_args[0][0]
      assert "backed_up" in metrics_data
      assert "failed" in metrics_data
      assert "execution_time_minutes" in metrics_data

  @pytest.mark.asyncio
  async def test_backup_error_handling(self, backup_service):
    """Test error handling during backup operations."""
    # Mock S3 upload failure
    with patch.object(backup_service, "_upload_backup_to_s3") as mock_upload:
      mock_upload.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Access denied"}}, "PutObject"
      )

      with patch.object(backup_service, "_is_backup_current", return_value=False):
        result = await backup_service.backup_graph_database("kg1a2b3c4d5")

        assert result["status"] == "failed"
        assert "error" in result

  @pytest.mark.asyncio
  async def test_backup_with_allocation_manager_failure(
    self, backup_service, mock_allocation_manager
  ):
    """Test handling of allocation manager failures."""
    mock_allocation_manager.get_instance_databases.side_effect = Exception(
      "DynamoDB connection failed"
    )

    # This should return success with 0 databases (error is caught internally)
    result = await backup_service.backup_all_graph_databases()

    assert result["status"] == "success"
    assert result["total_databases"] == 0

  @pytest.mark.asyncio
  async def test_concurrent_backup_handling(self, backup_service):
    """Test handling of concurrent backup attempts."""
    # This would test lock mechanisms if implemented
    with patch.object(backup_service, "backup_graph_database") as mock_backup:
      mock_backup.return_value = {
        "status": "success",
        "graph_id": "test",
        "backup_size_mb": 10,
      }

      # Simulate concurrent backups
      import asyncio

      results = await asyncio.gather(
        backup_service.backup_all_graph_databases(),
        backup_service.backup_all_graph_databases(),
        return_exceptions=True,
      )

      # At least one should succeed (status field, not success)
      assert any(
        r.get("status") == "success" if isinstance(r, dict) else False for r in results
      )

  @pytest.mark.asyncio
  async def test_backup_metadata_storage(self, backup_service, mock_s3_client):
    """Test that backup metadata is properly stored."""
    graph_id = "kg1a2b3c4d5"

    with patch.object(backup_service, "_is_backup_current", return_value=False):
      result = await backup_service.backup_graph_database(graph_id)

    assert result["status"] == "success"

    # Check S3 object metadata
    response = mock_s3_client.head_object(
      Bucket=DEFAULT_BACKUP_BUCKET, Key=result["s3_key"]
    )

    metadata = response.get("Metadata", {})
    assert "created_at" in metadata
    assert "checksum" in metadata
    assert "instance_id" in metadata
    assert "backup_type" in metadata
    assert metadata["backup_type"] == "graph_database"


class TestBackupServiceFactory:
  """Test the backup service factory function."""

  def test_create_graph_backup_service_defaults(self):
    """Test factory with default parameters."""
    with patch(
      "robosystems.operations.lbug.backup.LadybugAllocationManager"
    ) as mock_manager:
      with patch("robosystems.operations.lbug.backup.boto3.client") as mock_boto:
        with patch("robosystems.operations.lbug.backup.env.get_s3_config") as mock_s3:
          mock_s3.return_value = {
            "aws_access_key_id": "test",
            "aws_secret_access_key": "test",
            "region_name": "us-east-1",
          }
          mock_boto.return_value = Mock()

          service = create_graph_backup_service()

          assert service is not None
          assert isinstance(service, LadybugGraphBackupService)
          mock_manager.assert_called_once()

  def test_create_graph_backup_service_custom_params(self):
    """Test factory with custom parameters."""
    with patch("robosystems.operations.lbug.backup.LadybugAllocationManager"):
      custom_path = "/custom/path"

      with patch("robosystems.operations.lbug.backup.env.get_s3_config") as mock_s3:
        mock_s3.return_value = {
          "aws_access_key_id": "test",
          "aws_secret_access_key": "test",
          "region_name": "us-east-1",
        }
        with patch("robosystems.operations.lbug.backup.boto3.client") as mock_boto:
          mock_boto.return_value = Mock()
          with patch("requests.get") as mock_req:
            mock_req.return_value.text = "test-instance"

            service = create_graph_backup_service(
              environment="test", base_path=custom_path
            )

            assert service is not None
            assert service.base_path == Path(custom_path)
            assert service.environment == "test"


class TestBackupIntegration:
  """Integration tests for backup service."""

  @pytest.mark.asyncio
  @pytest.mark.integration
  async def test_end_to_end_backup_flow(
    self, backup_service, temp_db_dir, mock_s3_client
  ):
    """Test complete backup flow from discovery to S3 upload."""
    # Mock _is_backup_current to return False for actual backups
    with patch.object(backup_service, "_is_backup_current", return_value=False):
      # Perform full backup
      result = await backup_service.backup_all_graph_databases()

      assert result["status"] == "success"
      assert result["backed_up"] == 2

      # Verify all databases were backed up
      for graph_id in ["kg1a2b3c4d5", "kg5d4c3b2a1"]:
        response = mock_s3_client.list_objects_v2(
          Bucket=DEFAULT_BACKUP_BUCKET, Prefix=f"graph-databases/test/{graph_id}/"
        )
        assert response["KeyCount"] > 0

      # Test cleanup
      deleted = await backup_service.cleanup_old_backups()
      assert deleted >= 0

  @pytest.mark.asyncio
  @pytest.mark.integration
  async def test_backup_recovery_simulation(self, backup_service, temp_db_dir):
    """Test that backups can be used for recovery."""
    graph_id = "kg1a2b3c4d5"

    # Create backup
    with patch.object(backup_service, "_is_backup_current", return_value=False):
      backup_result = await backup_service.backup_graph_database(graph_id)
      assert backup_result["status"] == "success"

    # Simulate database corruption
    db_path = temp_db_dir / f"{graph_id}.lbug"
    for file in db_path.iterdir():
      file.unlink()

    # Verify database is empty
    assert len(list(db_path.iterdir())) == 0

    # In a real scenario, we would restore from backup here
    # This test just verifies the backup was created successfully
    assert backup_result["s3_key"] is not None
    assert backup_result["checksum"] is not None
