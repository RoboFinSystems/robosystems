"""
Tests for S3 backup adapter functionality.

This test suite validates S3 integration, compression,
and backup lifecycle management using LocalStack.
"""

import asyncio
import os

import pytest

from robosystems.operations.aws.s3 import S3BackupAdapter


class TestS3BackupAdapter:
  """Test suite for S3 backup adapter."""

  @pytest.fixture(autouse=True)
  def setup_localstack_env(self):
    """Set up LocalStack environment variables for testing."""
    # Store original values
    original_env = {}
    env_vars = {
      "USER_DATA_BUCKET": "robosystems-local",
      "AWS_S3_ACCESS_KEY_ID": "test",
      "AWS_S3_SECRET_ACCESS_KEY": "test",
      "AWS_REGION": "us-east-1",
      "AWS_ENDPOINT_URL": "http://localhost:4566",
    }

    for key, value in env_vars.items():
      original_env[key] = os.getenv(key)
      os.environ[key] = value

    yield

    # Restore original values
    for key, value in original_env.items():
      if value is None:
        os.environ.pop(key, None)
      else:
        os.environ[key] = value

  @pytest.fixture
  def s3_adapter(self):
    """Create S3 backup adapter instance."""
    import boto3
    from botocore.exceptions import ClientError

    # Create the bucket in LocalStack first
    s3_client = boto3.client(
      "s3",
      endpoint_url="http://localhost:4566",
      aws_access_key_id="test",
      aws_secret_access_key="test",
      region_name="us-east-1",
    )

    bucket_name = "robosystems-local"
    try:
      s3_client.head_bucket(Bucket=bucket_name)
    except ClientError:
      # Bucket doesn't exist, create it
      s3_client.create_bucket(Bucket=bucket_name)

    return S3BackupAdapter()

  @pytest.fixture
  def sample_cypher_data(self):
    """Sample Cypher data for testing."""
    return """
        CREATE (c:Entity {id: 'comp-123', name: 'Test Corp', sector: 'Technology'})
        CREATE (p:Person {id: 'person-456', name: 'John Doe', role: 'CEO'})
        CREATE (c)-[:EMPLOYS]->(p)
        CREATE (r:Report {id: 'report-789', period: '2024-Q1'})
        CREATE (c)-[:FILED]->(r)
        """.strip()

  @pytest.mark.asyncio
  async def test_s3_adapter_health_check(self, s3_adapter):
    """Test S3 adapter health check with LocalStack."""
    health = s3_adapter.health_check()

    # Note: In real test, LocalStack should be running
    # For now, we'll test the structure
    assert "status" in health
    assert "bucket" in health
    assert health["bucket"] == "robosystems-local"

  @pytest.mark.asyncio
  async def test_backup_upload_download_cycle(self, s3_adapter, sample_cypher_data):
    """Test complete backup upload and download cycle."""
    graph_id = "test-graph-123"
    backup_type = "full"

    # Mock metadata
    metadata = {
      "node_count": 4,
      "relationship_count": 2,
      "backup_duration_seconds": 1.5,
      "lbug_version": "0.10.1",
    }

    # Upload backup
    backup_metadata = await s3_adapter.upload_backup(
      graph_id=graph_id,
      backup_data=sample_cypher_data.encode("utf-8"),
      backup_type=backup_type,
      metadata=metadata,
    )

    # Verify metadata
    assert backup_metadata.graph_id == graph_id
    assert backup_metadata.backup_type == backup_type
    assert backup_metadata.original_size > 0
    assert backup_metadata.compression_ratio > 0  # Should be compressed
    assert backup_metadata.node_count == 4
    assert backup_metadata.relationship_count == 2

    # Download backup
    downloaded_data = await s3_adapter.download_backup(
      graph_id=graph_id,
      timestamp=backup_metadata.timestamp,
      backup_type=backup_type,
    )

    # Verify data integrity
    assert downloaded_data.decode("utf-8") == sample_cypher_data

  @pytest.mark.asyncio
  async def test_backup_compression_effectiveness(self, s3_adapter):
    """Test that compression is working effectively."""
    # Create repetitive data that should compress well
    repetitive_data = "CREATE (n:Node {prop: 'value'})\n" * 1000

    backup_metadata = await s3_adapter.upload_backup(
      graph_id="compression-test",
      backup_data=repetitive_data.encode("utf-8"),
      backup_type="full",
      metadata={"node_count": 1000, "relationship_count": 0},
    )

    # Should achieve good compression on repetitive data
    assert backup_metadata.compression_ratio > 0.5  # At least 50% reduction
    print(f"Compression ratio: {backup_metadata.compression_ratio:.1%}")

  @pytest.mark.asyncio
  async def test_backup_integrity(self, s3_adapter, sample_cypher_data):
    """Test backup integrity validation."""
    graph_id = "integrity-test"

    backup_metadata = await s3_adapter.upload_backup(
      graph_id=graph_id,
      backup_data=sample_cypher_data.encode("utf-8"),
      backup_type="full",
      metadata={},
    )

    # Verify backup was stored properly
    assert backup_metadata.compressed_size > 0

    # Verify checksum for integrity
    assert len(backup_metadata.checksum) == 64  # SHA-256 hex string

    # Download and verify data matches original
    downloaded_data = await s3_adapter.download_backup(
      graph_id=graph_id,
      timestamp=backup_metadata.timestamp,
      backup_type="full",
    )

    assert downloaded_data.decode("utf-8") == sample_cypher_data

  @pytest.mark.asyncio
  async def test_list_backups_functionality(self, s3_adapter, sample_cypher_data):
    """Test backup listing functionality."""
    graph_id = "list-test"

    # Clean up any existing backups for this test
    existing_backups = await s3_adapter.list_backups(graph_id)
    for backup in existing_backups:
      # Delete backup and metadata directly by S3 key
      backup_key = backup["key"]
      # Generate metadata key from backup key
      key_parts = backup_key.split("/")
      if len(key_parts) >= 5:
        timestamp_part = key_parts[4].replace("backup-", "")
        # Handle both old (.cypher) and new (.lbug) extensions, with and without encryption
        timestamp_part = (
          timestamp_part.replace(".cypher.gz.enc", "")
          .replace(".lbug.gz.enc", "")
          .replace(".lbug.gz", "")  # New format without encryption
          .replace(".cypher.gz", "")
        )
        try:
          from datetime import datetime

          # Parse timestamp to validate format
          datetime.strptime(timestamp_part, "%Y%m%d_%H%M%S")
          metadata_key = (
            f"graph-backups/metadata/{graph_id}/backup-{timestamp_part}.json"
          )

          # Delete both backup and metadata directly
          try:
            await asyncio.get_event_loop().run_in_executor(
              None,
              lambda: s3_adapter.s3_client.delete_objects(
                Bucket=s3_adapter.bucket_name,
                Delete={"Objects": [{"Key": backup_key}, {"Key": metadata_key}]},
              ),
            )
          except Exception:
            # Ignore deletion errors during cleanup
            pass
        except ValueError:
          # Skip if timestamp format doesn't match
          pass

    # Create multiple backups with slight delay to ensure different timestamps

    backup_metadatas = []
    for i in range(3):
      metadata = await s3_adapter.upload_backup(
        graph_id=graph_id,
        backup_data=f"{sample_cypher_data}\n// Backup {i}".encode(),
        backup_type="full",
        metadata={"backup_number": i},
      )
      backup_metadatas.append(metadata)
      if i < 2:  # Don't sleep after the last one
        await asyncio.sleep(1)  # 1 second delay to ensure different timestamps

    # List backups
    backups = await s3_adapter.list_backups(graph_id)

    # Should have 3 backups
    assert len(backups) == 3

    # Verify backup structure
    for backup in backups:
      assert "key" in backup
      assert "last_modified" in backup
      assert "size" in backup
      assert graph_id in backup["key"]

  @pytest.mark.asyncio
  async def test_backup_deletion(self, s3_adapter, sample_cypher_data):
    """Test backup deletion functionality."""
    graph_id = "deletion-test"

    # Clean up any existing backups for this test
    existing_backups = await s3_adapter.list_backups(graph_id)
    for backup in existing_backups:
      key_parts = backup["key"].split("/")
      if len(key_parts) >= 5:
        timestamp_part = (
          key_parts[4]
          .replace("backup-", "")
          .replace(".cypher.gz.enc", "")
          .replace(".lbug.gz.enc", "")
          .replace(".lbug.gz", "")  # New format without encryption
          .replace(".cypher.gz", "")
        )
        try:
          from datetime import datetime

          timestamp = datetime.strptime(timestamp_part, "%Y%m%d_%H%M%S")
          await s3_adapter.delete_backup(graph_id, timestamp, "full")
        except ValueError:
          pass

    # Create a backup
    backup_metadata = await s3_adapter.upload_backup(
      graph_id=graph_id,
      backup_data=sample_cypher_data.encode("utf-8"),
      backup_type="full",
      metadata={},
    )

    # Verify backup exists
    backups_before = await s3_adapter.list_backups(graph_id)
    assert len(backups_before) == 1

    # Delete backup
    success = await s3_adapter.delete_backup(
      graph_id=graph_id,
      timestamp=backup_metadata.timestamp,
      backup_type="full",
    )

    assert success is True

    # Verify backup is deleted
    backups_after = await s3_adapter.list_backups(graph_id)
    assert len(backups_after) == 0
