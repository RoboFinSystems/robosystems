"""Comprehensive tests for the GraphBackup model."""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

from robosystems.models.iam import GraphBackup, User
from robosystems.models.iam.graph_backup import BackupStatus, BackupType


class TestGraphBackupModel:
  """Test suite for the GraphBackup model."""

  def test_graph_backup_initialization(self):
    """Test GraphBackup model can be instantiated with required fields."""
    backup = GraphBackup(
      graph_id="kg1a2b3c4d5",
      database_name="test_db",
      backup_type=BackupType.FULL.value,
      s3_bucket="backup-bucket",
      s3_key="backups/test.tar.gz",
    )

    assert backup.graph_id == "kg1a2b3c4d5"
    assert backup.database_name == "test_db"
    assert backup.backup_type == BackupType.FULL.value
    assert backup.s3_bucket == "backup-bucket"
    assert backup.s3_key == "backups/test.tar.gz"
    assert backup.status is None or backup.status == BackupStatus.PENDING.value
    assert backup.id is None  # ID is generated on commit

  def test_graph_backup_id_generation(self):
    """Test that GraphBackup ID is generated with proper format."""
    GraphBackup(
      graph_id="kg_test",
      database_name="test_db",
      backup_type=BackupType.FULL.value,
      s3_bucket="bucket",
      s3_key="key",
    )

    # Call the default lambda to generate ID
    generated_id = GraphBackup.id.default.arg(None)
    assert generated_id.startswith("backup_")
    assert len(generated_id) > 7  # backup_ + token

  def test_graph_backup_repr(self):
    """Test GraphBackup string representation."""
    backup = GraphBackup(
      graph_id="kg1a2b3c4d5",
      database_name="test_db",
      backup_type=BackupType.INCREMENTAL.value,
      status=BackupStatus.COMPLETED.value,
      s3_bucket="bucket",
      s3_key="key",
    )
    backup.id = "backup_test123"

    expected = (
      "<GraphBackup backup_test123 graph=kg1a2b3c4d5 type=incremental status=completed>"
    )
    assert repr(backup) == expected

  def test_backup_status_enum(self):
    """Test BackupStatus enum values."""
    assert BackupStatus.PENDING.value == "pending"
    assert BackupStatus.IN_PROGRESS.value == "in_progress"
    assert BackupStatus.COMPLETED.value == "completed"
    assert BackupStatus.FAILED.value == "failed"
    assert BackupStatus.EXPIRED.value == "expired"

  def test_backup_type_enum(self):
    """Test BackupType enum values."""
    assert BackupType.FULL.value == "full"
    assert BackupType.INCREMENTAL.value == "incremental"

  def test_create_graph_backup(self, db_session):
    """Test creating a new graph backup record."""
    # Create a user for tracking
    user = User.create(
      email="backup@example.com",
      name="Backup User",
      password_hash="hashed_password",
      session=db_session,
    )

    # Create backup with all optional parameters
    backup = GraphBackup.create(
      graph_id="kg_backup",
      database_name="backup_db",
      backup_type=BackupType.FULL.value,
      s3_bucket="my-backup-bucket",
      s3_key="backups/2024/01/backup.tar.gz",
      session=db_session,
      s3_metadata_key="backups/2024/01/backup.meta.json",
      original_size_bytes=1000000,
      compressed_size_bytes=250000,
      encrypted_size_bytes=260000,
      compression_ratio=0.75,
      node_count=5000,
      relationship_count=10000,
      database_version="1.0.0",
      backup_duration_seconds=45.5,
      checksum="sha256_checksum_here",
      encryption_enabled=True,
      compression_enabled=True,
      created_by_user_id=user.id,
      backup_metadata={"schema": "roboledger", "version": 2},
    )

    assert backup.id is not None
    assert backup.id.startswith("backup_")
    assert backup.graph_id == "kg_backup"
    assert backup.database_name == "backup_db"
    assert backup.backup_type == BackupType.FULL.value
    assert backup.status == BackupStatus.PENDING.value
    assert backup.s3_bucket == "my-backup-bucket"
    assert backup.s3_key == "backups/2024/01/backup.tar.gz"
    assert backup.s3_metadata_key == "backups/2024/01/backup.meta.json"
    assert backup.original_size_bytes == 1000000
    assert backup.compressed_size_bytes == 250000
    assert backup.encrypted_size_bytes == 260000
    assert backup.compression_ratio == 0.75
    assert backup.node_count == 5000
    assert backup.relationship_count == 10000
    assert backup.created_by_user_id == user.id
    assert backup.created_at is not None

    # Verify in database
    db_backup = db_session.query(GraphBackup).filter_by(id=backup.id).first()
    assert db_backup is not None
    assert db_backup.graph_id == "kg_backup"

  def test_get_by_id(self, db_session):
    """Test getting backup by ID."""
    # Create a backup
    backup = GraphBackup.create(
      graph_id="kg_find",
      database_name="find_db",
      backup_type=BackupType.FULL.value,
      s3_bucket="bucket",
      s3_key="find.tar.gz",
      session=db_session,
    )

    # Find it
    found = GraphBackup.get_by_id(backup.id, db_session)
    assert found is not None
    assert found.id == backup.id
    assert found.graph_id == "kg_find"

    # Not found
    not_found = GraphBackup.get_by_id("backup_nonexistent", db_session)
    assert not_found is None

  def test_get_by_graph_id(self, db_session):
    """Test getting backups for a specific graph."""
    # Create multiple backups for same graph
    for i in range(3):
      GraphBackup.create(
        graph_id="kg_multi",
        database_name="multi_db",
        backup_type=BackupType.FULL.value if i < 2 else BackupType.INCREMENTAL.value,
        s3_bucket="bucket",
        s3_key=f"backup_{i}.tar.gz",
        session=db_session,
        status=BackupStatus.COMPLETED.value if i < 2 else BackupStatus.PENDING.value,
      )

    # Create expired backup
    GraphBackup.create(
      graph_id="kg_multi",
      database_name="multi_db",
      backup_type=BackupType.FULL.value,
      s3_bucket="bucket",
      s3_key="expired.tar.gz",
      session=db_session,
      status=BackupStatus.EXPIRED.value,
    )

    # Get all backups (excluding expired)
    backups = GraphBackup.get_by_graph_id("kg_multi", db_session)
    assert len(backups) == 3  # Should not include expired

    # Get including expired
    all_backups = GraphBackup.get_by_graph_id(
      "kg_multi", db_session, include_expired=True
    )
    assert len(all_backups) == 4

    # Get by type
    full_backups = GraphBackup.get_by_graph_id(
      "kg_multi", db_session, backup_type=BackupType.FULL.value
    )
    assert len(full_backups) == 2

    # Get by status
    completed = GraphBackup.get_by_graph_id(
      "kg_multi", db_session, status=BackupStatus.COMPLETED.value
    )
    assert len(completed) == 2

    # Get with limit
    limited = GraphBackup.get_by_graph_id("kg_multi", db_session, limit=2)
    assert len(limited) == 2

  def test_get_latest_successful(self, db_session):
    """Test getting the latest successful backup."""
    # Create backups with different completion times
    for i in range(3):
      backup = GraphBackup.create(
        graph_id="kg_latest",
        database_name="latest_db",
        backup_type=BackupType.FULL.value,
        s3_bucket="bucket",
        s3_key=f"backup_{i}.tar.gz",
        session=db_session,
      )
      # Mark as completed with different times
      backup.complete_backup(
        session=db_session,
        original_size=1000000,
        compressed_size=250000,
        encrypted_size=260000,
        checksum=f"checksum_{i}",
      )
      # Manually adjust completed_at for testing
      backup.completed_at = datetime.now(UTC) - timedelta(days=3 - i)
      db_session.commit()

    # Create a failed backup (should not be returned)
    failed = GraphBackup.create(
      graph_id="kg_latest",
      database_name="latest_db",
      backup_type=BackupType.FULL.value,
      s3_bucket="bucket",
      s3_key="failed.tar.gz",
      session=db_session,
    )
    failed.fail_backup(db_session, "Test failure")

    # Get latest successful
    latest = GraphBackup.get_latest_successful(
      "kg_latest", BackupType.FULL.value, db_session
    )
    assert latest is not None
    assert latest.s3_key == "backup_2.tar.gz"  # The most recent one

  def test_get_pending_backups(self, db_session):
    """Test getting all pending backups."""
    # Get initial count of pending backups (in case other tests left some)
    initial_pending = GraphBackup.get_pending_backups(db_session)
    initial_count = len(initial_pending)

    # Create mixed status backups with unique identifiers
    import uuid

    unique_id = str(uuid.uuid4())[:8]
    statuses = [
      BackupStatus.PENDING.value,
      BackupStatus.IN_PROGRESS.value,
      BackupStatus.COMPLETED.value,
      BackupStatus.PENDING.value,
    ]

    created_pending_ids = []
    for i, status in enumerate(statuses):
      backup = GraphBackup.create(
        graph_id=f"kg_test_{unique_id}_{i}",
        database_name=f"db_test_{unique_id}_{i}",
        backup_type=BackupType.FULL.value,
        s3_bucket="bucket",
        s3_key=f"backup_{unique_id}_{i}.tar.gz",
        session=db_session,
        status=status,
      )
      if status == BackupStatus.PENDING.value:
        created_pending_ids.append(backup.graph_id)

    # Get all pending backups
    all_pending = GraphBackup.get_pending_backups(db_session)

    # Verify we have at least the ones we created
    assert len(all_pending) >= initial_count + 2

    # Verify our specific pending backups are in the results
    pending_graph_ids = [b.graph_id for b in all_pending]
    for graph_id in created_pending_ids:
      assert graph_id in pending_graph_ids

  def test_get_expired_backups(self, db_session):
    """Test getting all expired backups."""
    # Create backups with different expiry times
    future_expiry = datetime.now(UTC) + timedelta(days=1)
    past_expiry = datetime.now(UTC) - timedelta(days=1)

    # Not expired (future)
    GraphBackup.create(
      graph_id="kg_future",
      database_name="future_db",
      backup_type=BackupType.FULL.value,
      s3_bucket="bucket",
      s3_key="future.tar.gz",
      session=db_session,
      expires_at=future_expiry,
    )

    # Expired (past)
    GraphBackup.create(
      graph_id="kg_past",
      database_name="past_db",
      backup_type=BackupType.FULL.value,
      s3_bucket="bucket",
      s3_key="past.tar.gz",
      session=db_session,
      expires_at=past_expiry,
    )

    # No expiry
    GraphBackup.create(
      graph_id="kg_noexpiry",
      database_name="noexpiry_db",
      backup_type=BackupType.FULL.value,
      s3_bucket="bucket",
      s3_key="noexpiry.tar.gz",
      session=db_session,
    )

    # Get expired backups
    expired = GraphBackup.get_expired_backups(db_session)
    assert len(expired) == 1
    assert expired[0].graph_id == "kg_past"

  def test_get_backup_stats(self, db_session):
    """Test getting backup statistics for a graph."""
    graph_id = "kg_stats"

    # Create successful backups
    for i in range(3):
      backup = GraphBackup.create(
        graph_id=graph_id,
        database_name="stats_db",
        backup_type=BackupType.FULL.value,
        s3_bucket="bucket",
        s3_key=f"success_{i}.tar.gz",
        session=db_session,
      )
      backup.complete_backup(
        session=db_session,
        original_size=1000000 * (i + 1),
        compressed_size=250000 * (i + 1),
        encrypted_size=260000 * (i + 1),
        checksum=f"checksum_{i}",
      )

    # Create failed backup
    failed = GraphBackup.create(
      graph_id=graph_id,
      database_name="stats_db",
      backup_type=BackupType.FULL.value,
      s3_bucket="bucket",
      s3_key="failed.tar.gz",
      session=db_session,
    )
    failed.fail_backup(db_session, "Test failure")

    # Create expired backup (should be excluded)
    GraphBackup.create(
      graph_id=graph_id,
      database_name="stats_db",
      backup_type=BackupType.FULL.value,
      s3_bucket="bucket",
      s3_key="expired.tar.gz",
      session=db_session,
      status=BackupStatus.EXPIRED.value,
    )

    # Get stats
    stats = GraphBackup.get_backup_stats(graph_id, db_session)

    assert stats["graph_id"] == graph_id
    assert stats["total_backups"] == 4  # Excludes expired
    assert stats["successful_backups"] == 3
    assert stats["failed_backups"] == 1
    assert stats["success_rate"] == 0.75
    assert stats["total_original_size_bytes"] == 6000000  # 1M + 2M + 3M
    assert stats["total_compressed_size_bytes"] == 1500000  # 250K + 500K + 750K
    assert stats["storage_saved_bytes"] == 4500000
    assert stats["average_compression_ratio"] == 0.75
    assert stats["latest_backup_date"] is not None

  def test_start_backup(self, db_session):
    """Test marking backup as started."""
    backup = GraphBackup.create(
      graph_id="kg_start",
      database_name="start_db",
      backup_type=BackupType.FULL.value,
      s3_bucket="bucket",
      s3_key="start.tar.gz",
      session=db_session,
    )

    assert backup.status == BackupStatus.PENDING.value
    assert backup.started_at is None

    # Start backup
    backup.start_backup(db_session)

    assert backup.status == BackupStatus.IN_PROGRESS.value
    assert backup.started_at is not None

    # Verify in database
    db_backup = db_session.query(GraphBackup).filter_by(id=backup.id).first()
    assert db_backup.status == BackupStatus.IN_PROGRESS.value

  def test_complete_backup(self, db_session):
    """Test marking backup as completed with metrics."""
    backup = GraphBackup.create(
      graph_id="kg_complete",
      database_name="complete_db",
      backup_type=BackupType.FULL.value,
      s3_bucket="bucket",
      s3_key="complete.tar.gz",
      session=db_session,
    )

    # Start and complete backup
    backup.start_backup(db_session)

    metadata = {"test": "metadata", "version": 2}
    backup.complete_backup(
      session=db_session,
      original_size=5000000,
      compressed_size=1000000,
      encrypted_size=1050000,
      checksum="sha256_test_checksum",
      node_count=10000,
      relationship_count=25000,
      backup_duration=120.5,
      metadata=metadata,
    )

    assert backup.status == BackupStatus.COMPLETED.value
    assert backup.completed_at is not None
    assert backup.original_size_bytes == 5000000
    assert backup.compressed_size_bytes == 1000000
    assert backup.encrypted_size_bytes == 1050000
    assert backup.compression_ratio == 0.8  # (5M - 1M) / 5M
    assert backup.checksum == "sha256_test_checksum"
    assert backup.node_count == 10000
    assert backup.relationship_count == 25000
    assert backup.backup_duration_seconds == 120.5
    assert backup.backup_metadata == metadata

  def test_fail_backup(self, db_session):
    """Test marking backup as failed."""
    backup = GraphBackup.create(
      graph_id="kg_fail",
      database_name="fail_db",
      backup_type=BackupType.INCREMENTAL.value,
      s3_bucket="bucket",
      s3_key="fail.tar.gz",
      session=db_session,
    )

    # Fail backup
    error_msg = "Connection timeout to S3"
    backup.fail_backup(db_session, error_msg)

    assert backup.status == BackupStatus.FAILED.value
    assert backup.error_message == error_msg
    assert backup.retry_count == 1

    # Fail again to test retry count
    backup.fail_backup(db_session, "Another error")
    assert backup.retry_count == 2

  def test_expire_backup(self, db_session):
    """Test marking backup as expired."""
    backup = GraphBackup.create(
      graph_id="kg_expire",
      database_name="expire_db",
      backup_type=BackupType.FULL.value,
      s3_bucket="bucket",
      s3_key="expire.tar.gz",
      session=db_session,
    )

    # Expire backup
    backup.expire_backup(db_session)

    assert backup.status == BackupStatus.EXPIRED.value

  def test_update_metadata(self, db_session):
    """Test updating backup metadata."""

    initial_metadata = {"initial": "data", "version": 1}

    backup = GraphBackup.create(
      graph_id="kg_meta",
      database_name="meta_db",
      backup_type=BackupType.FULL.value,
      s3_bucket="bucket",
      s3_key="meta.tar.gz",
      session=db_session,
      backup_metadata=initial_metadata,
    )

    # Update metadata - this method updates in place but may need explicit flagging for JSON
    new_metadata = {"additional": "info", "version": 2}
    backup.update_metadata(db_session, new_metadata)

    # Refresh from database to ensure we get the persisted value
    db_session.refresh(backup)

    # The update_metadata method updates the existing dict, so both old and new keys should be present
    assert backup.backup_metadata["initial"] == "data"
    assert backup.backup_metadata["additional"] == "info"
    assert backup.backup_metadata["version"] == 2  # Updated

  def test_delete_backup(self, db_session):
    """Test deleting backup record."""
    backup = GraphBackup.create(
      graph_id="kg_delete",
      database_name="delete_db",
      backup_type=BackupType.FULL.value,
      s3_bucket="bucket",
      s3_key="delete.tar.gz",
      session=db_session,
    )

    backup_id = backup.id

    # Delete
    backup.delete(db_session)

    # Verify deletion
    db_backup = db_session.query(GraphBackup).filter_by(id=backup_id).first()
    assert db_backup is None

  def test_to_dict(self, db_session):
    """Test converting backup record to dictionary."""
    # Create user for testing
    user = User.create(
      email="dict@example.com",
      name="Dict User",
      password_hash="hashed_password",
      session=db_session,
    )

    expires_at = datetime.now(UTC) + timedelta(days=30)

    backup = GraphBackup.create(
      graph_id="kg_dict",
      database_name="dict_db",
      backup_type=BackupType.FULL.value,
      s3_bucket="bucket",
      s3_key="dict.tar.gz",
      session=db_session,
      s3_metadata_key="dict.meta.json",
      checksum="a" * 64,  # Long checksum for truncation test
      created_by_user_id=user.id,
      expires_at=expires_at,
      backup_metadata={"test": "dict"},
    )

    # Complete the backup
    backup.start_backup(db_session)
    backup.complete_backup(
      session=db_session,
      original_size=1000000,
      compressed_size=250000,
      encrypted_size=260000,
      checksum="b" * 64,
    )

    # Convert to dict
    backup_dict = backup.to_dict()

    assert backup_dict["id"] == backup.id
    assert backup_dict["graph_id"] == "kg_dict"
    assert backup_dict["database_name"] == "dict_db"
    assert backup_dict["backup_type"] == BackupType.FULL.value
    assert backup_dict["status"] == BackupStatus.COMPLETED.value
    assert backup_dict["checksum"] == "b" * 16 + "..."  # Truncated
    assert backup_dict["created_by_user_id"] == user.id
    assert backup_dict["metadata"] == {"test": "dict"}
    assert backup_dict["started_at"] is not None
    assert backup_dict["completed_at"] is not None
    assert backup_dict["expires_at"] is not None

  def test_is_completed_property(self):
    """Test is_completed property."""
    backup = GraphBackup(
      graph_id="kg_test",
      database_name="test_db",
      backup_type=BackupType.FULL.value,
      s3_bucket="bucket",
      s3_key="test.tar.gz",
    )

    backup.status = BackupStatus.PENDING.value
    assert backup.is_completed is False

    backup.status = BackupStatus.COMPLETED.value
    assert backup.is_completed is True

  def test_is_failed_property(self):
    """Test is_failed property."""
    backup = GraphBackup(
      graph_id="kg_test",
      database_name="test_db",
      backup_type=BackupType.FULL.value,
      s3_bucket="bucket",
      s3_key="test.tar.gz",
    )

    backup.status = BackupStatus.PENDING.value
    assert backup.is_failed is False

    backup.status = BackupStatus.FAILED.value
    assert backup.is_failed is True

  def test_is_expired_property(self):
    """Test is_expired property."""
    backup = GraphBackup(
      graph_id="kg_test",
      database_name="test_db",
      backup_type=BackupType.FULL.value,
      s3_bucket="bucket",
      s3_key="test.tar.gz",
    )

    # No expiry
    assert backup.is_expired is False

    # Future expiry
    backup.expires_at = datetime.now(UTC) + timedelta(days=1)
    assert backup.is_expired is False

    # Past expiry
    backup.expires_at = datetime.now(UTC) - timedelta(days=1)
    assert backup.is_expired is True

  def test_storage_efficiency_property(self):
    """Test storage_efficiency property."""
    backup = GraphBackup(
      graph_id="kg_test",
      database_name="test_db",
      backup_type=BackupType.FULL.value,
      s3_bucket="bucket",
      s3_key="test.tar.gz",
    )

    # No sizes set
    assert backup.storage_efficiency == 0.0

    # With sizes
    backup.original_size_bytes = 1000000
    backup.encrypted_size_bytes = 300000
    assert backup.storage_efficiency == 0.3  # 300K / 1M

    # Zero original size
    backup.original_size_bytes = 0
    assert backup.storage_efficiency == 0.0

  def test_compression_ratio_calculation(self):
    """Test compression ratio calculation in complete_backup."""
    backup = GraphBackup(
      graph_id="kg_ratio",
      database_name="ratio_db",
      backup_type=BackupType.FULL.value,
      s3_bucket="bucket",
      s3_key="ratio.tar.gz",
    )

    # Mock session
    mock_session = MagicMock()

    # Test with normal sizes
    backup.complete_backup(
      session=mock_session,
      original_size=1000000,
      compressed_size=300000,
      encrypted_size=310000,
      checksum="test",
    )
    assert backup.compression_ratio == 0.7  # (1M - 300K) / 1M

    # Test with zero original size
    backup2 = GraphBackup(
      graph_id="kg_zero",
      database_name="zero_db",
      backup_type=BackupType.FULL.value,
      s3_bucket="bucket",
      s3_key="zero.tar.gz",
    )

    backup2.complete_backup(
      session=mock_session,
      original_size=0,
      compressed_size=0,
      encrypted_size=0,
      checksum="test",
    )
    assert backup2.compression_ratio == 0.0

  def test_backup_with_relationship_to_user(self, db_session):
    """Test backup relationship with User model."""
    # Create user
    user = User.create(
      email="relation@example.com",
      name="Relation User",
      password_hash="hashed_password",
      session=db_session,
    )

    # Create backup with user
    backup = GraphBackup.create(
      graph_id="kg_relation",
      database_name="relation_db",
      backup_type=BackupType.FULL.value,
      s3_bucket="bucket",
      s3_key="relation.tar.gz",
      session=db_session,
      created_by_user_id=user.id,
    )

    # Test relationship
    assert backup.created_by_user_id == user.id
    assert backup.created_by_user is not None
    assert backup.created_by_user.email == "relation@example.com"
