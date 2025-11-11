"""
Tests for Kuzu backup manager operations.

This test suite validates backup manager functionality including
job creation, multitenant validation, and Kuzu integration.
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock, AsyncMock

from robosystems.operations.kuzu.backup_manager import (
  BackupManager,
  BackupJob,
  BackupFormat,
  BackupType,
  RestoreJob,
)
from robosystems.middleware.graph.multitenant_utils import MultiTenantUtils
from robosystems.adapters.s3 import S3BackupAdapter


class TestBackupManager:
  """Test suite for backup manager operations."""

  @pytest.fixture
  def backup_manager(self):
    """Create backup manager with mocked S3 adapter."""
    s3_adapter = MagicMock(spec=S3BackupAdapter)
    return BackupManager(s3_adapter=s3_adapter)

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
  async def test_multitenant_graph_validation(self):
    """Test multitenant graph ID validation."""
    # Test valid graph IDs (alphanumeric, underscores, hyphens allowed)
    valid_ids = [
      "kg1a2b3c",
      "user_abc_def",
      "graph_456",
      "test_graph",
      "sec",
      "entity-123",
      "user-abc-def",
    ]
    for graph_id in valid_ids:
      try:
        MultiTenantUtils.validate_graph_id(graph_id)
        print(f"✓ Valid graph ID: {graph_id}")
      except ValueError:
        pytest.fail(f"Expected {graph_id} to be valid")

    # Test invalid graph IDs
    invalid_ids = [
      "",
      "   ",
      "id with spaces",
      "id/with/slashes",
      "id;with;semicolons",
      "id@with@symbols",
      "id.with.dots",
    ]
    for graph_id in invalid_ids:
      with pytest.raises(ValueError):
        MultiTenantUtils.validate_graph_id(graph_id)
        print(f"✓ Invalid graph ID rejected: {graph_id}")

    # Test shared repository detection
    assert MultiTenantUtils.is_shared_repository("sec") is True
    assert MultiTenantUtils.is_shared_repository("economic") is True
    assert MultiTenantUtils.is_shared_repository("industry") is True
    assert MultiTenantUtils.is_shared_repository("kg1a2b3c") is False

  @pytest.mark.asyncio
  async def test_backup_manager_mocked_kuzu(self, backup_manager, sample_cypher_data):
    """Test backup manager with mocked Kuzu operations."""
    graph_id = "test_kg1a2b3c"

    # Mock graph repository operations and file operations
    with (
      patch(
        "robosystems.operations.kuzu.backup_manager.get_universal_repository"
      ) as mock_get_repo,
      patch("builtins.open", create=True) as mock_open,
      patch("os.path.exists", return_value=True),
      patch("os.unlink"),
      patch("shutil.copy2"),
      patch("shutil.copytree"),
      patch("zipfile.ZipFile"),
      patch("tempfile.mkdtemp", return_value="/tmp/test_backup"),
    ):
      # Mock repository methods as async
      async def mock_execute_single(query, params=None):
        if "count(n)" in query:
          return {"count": 150}
        elif "count(r)" in query:
          return {"count": 75}
        elif "dbms.components" in query:
          return {"version": "5.26.3"}
        elif "apoc.export.cypher.all" in query and "RETURN count" in query:
          return {"count": 1}
        elif "apoc.export.cypher.all" in query and "CALL" in query:
          return None  # APOC export doesn't return anything meaningful
        else:
          return None

      async def mock_execute_query(query, params=None):
        return []  # Empty result for most queries

      # Create a mock repository that supports async context manager
      mock_repo = MagicMock()
      mock_repo.execute_single = mock_execute_single
      mock_repo.execute_query = mock_execute_query

      # Make the mock repository work as an async context manager
      mock_repo.__aenter__ = AsyncMock(return_value=mock_repo)
      mock_repo.__aexit__ = AsyncMock(return_value=None)

      # Mock get_graph_repository to return an awaitable that resolves to mock_repo
      async def mock_get_graph_repository(*args, **kwargs):
        return mock_repo

      mock_get_repo.side_effect = mock_get_graph_repository

      # Mock file operations for APOC export
      mock_file = MagicMock()
      mock_file.read.return_value = sample_cypher_data
      mock_open.return_value.__enter__.return_value = mock_file

      # Mock export data
      mock_repo.export_database_to_cypher.return_value = sample_cypher_data

      # Mock S3 upload
      mock_backup_metadata = MagicMock()
      mock_backup_metadata.original_size = len(sample_cypher_data.encode("utf-8"))
      mock_backup_metadata.compressed_size = 400
      mock_backup_metadata.encrypted_size = 420
      mock_backup_metadata.compression_ratio = 0.6
      mock_backup_metadata.node_count = 150
      mock_backup_metadata.relationship_count = 75
      mock_backup_metadata.backup_duration_seconds = 2.5
      mock_backup_metadata.kuzu_version = "0.10.1"

      backup_manager.s3_adapter.upload_backup = AsyncMock(
        return_value=mock_backup_metadata
      )

      # Create backup job
      from robosystems.operations.kuzu.backup_manager import BackupFormat, BackupType

      backup_job = BackupJob(
        graph_id=graph_id,
        backup_type=BackupType.FULL,
        backup_format=BackupFormat.FULL_DUMP,
        timestamp=datetime.now(timezone.utc),
        retention_days=90,
        compression=True,
        encryption=True,
        allow_export=False,  # Required when encryption is True
      )

      # Execute backup
      result = await backup_manager.create_backup(backup_job)

      # Verify results
      assert result == mock_backup_metadata
      assert result.node_count == 150
      assert result.relationship_count == 75
      assert result.compression_ratio == 0.6

      # Verify S3 upload was called
      backup_manager.s3_adapter.upload_backup.assert_called_once()

      print("✓ Backup manager operations tested successfully")

  @pytest.mark.asyncio
  async def test_create_backup_error_scenarios(self, backup_manager):
    """Test error scenarios in backup creation."""
    graph_id = "test_graph"

    # Test database connection failure
    with patch(
      "robosystems.operations.kuzu.backup_manager.get_universal_repository"
    ) as mock_get_repo:
      mock_get_repo.side_effect = Exception("Database connection failed")

      backup_job = BackupJob(
        graph_id=graph_id,
        backup_type=BackupType.FULL,
        backup_format=BackupFormat.FULL_DUMP,
        timestamp=datetime.now(timezone.utc),
        encryption=False,  # Disable encryption for this test
      )

      with pytest.raises(Exception, match="Database connection failed"):
        await backup_manager.create_backup(backup_job)

    # Test S3 upload failure
    with (
      patch(
        "robosystems.operations.kuzu.backup_manager.get_universal_repository"
      ) as mock_get_repo,
      patch("builtins.open", create=True),
      patch.object(
        backup_manager, "_export_database", new_callable=AsyncMock
      ) as mock_export,
    ):
      # Mock successful database operations
      mock_repo = MagicMock()
      mock_repo.execute_single = AsyncMock(return_value={"count": 10})
      mock_repo.execute_query = AsyncMock(return_value=[])
      mock_repo.__aenter__ = AsyncMock(return_value=mock_repo)
      mock_repo.__aexit__ = AsyncMock(return_value=None)

      async def mock_get_graph_repository(*args, **kwargs):
        return mock_repo

      mock_get_repo.side_effect = mock_get_graph_repository

      # Mock export to return backup data
      mock_export.return_value = (b"backup_data", "zip")

      # Mock S3 upload failure
      backup_manager.s3_adapter.upload_backup = AsyncMock(
        side_effect=Exception("S3 upload failed")
      )

      backup_job = BackupJob(
        graph_id=graph_id,
        backup_type=BackupType.FULL,
        backup_format=BackupFormat.FULL_DUMP,
        encryption=False,  # Disable encryption for this test
      )

      with pytest.raises(Exception, match="S3 upload failed"):
        await backup_manager.create_backup(backup_job)

  @pytest.mark.asyncio
  async def test_create_backup_different_formats(
    self, backup_manager, sample_cypher_data
  ):
    """Test backup creation with different formats."""
    graph_id = "test_graph"

    for backup_format in [BackupFormat.CSV, BackupFormat.JSON, BackupFormat.PARQUET]:
      with (
        patch(
          "robosystems.operations.kuzu.backup_manager.get_universal_repository"
        ) as mock_get_repo,
        patch.object(
          backup_manager, "_export_database", new_callable=AsyncMock
        ) as mock_export,
      ):
        # Mock repository for stats
        mock_repo = MagicMock()
        mock_repo.execute_single = AsyncMock(return_value={"count": 50})
        mock_repo.execute_query = AsyncMock(return_value=[])
        mock_repo.__aenter__ = AsyncMock(return_value=mock_repo)
        mock_repo.__aexit__ = AsyncMock(return_value=None)

        async def mock_get_graph_repository(*args, **kwargs):
          return mock_repo

        mock_get_repo.side_effect = mock_get_graph_repository

        # Mock export to return appropriate data and extension based on format
        if backup_format == BackupFormat.CSV:
          mock_export.return_value = (b"col1,col2\nval1,val2\n", "zip")
        elif backup_format == BackupFormat.JSON:
          mock_export.return_value = (b'[{"col1": "val1", "col2": "val2"}]', "zip")
        elif backup_format == BackupFormat.PARQUET:
          mock_export.return_value = (b"parquet_data", "zip")

        # Mock S3 upload
        mock_backup_metadata = MagicMock()
        mock_backup_metadata.original_size = 100
        mock_backup_metadata.compressed_size = 50
        mock_backup_metadata.compression_ratio = 0.5
        mock_backup_metadata.node_count = 50
        mock_backup_metadata.relationship_count = 25
        backup_manager.s3_adapter.upload_backup = AsyncMock(
          return_value=mock_backup_metadata
        )

        backup_job = BackupJob(
          graph_id=graph_id,
          backup_format=backup_format,
          backup_type=BackupType.FULL,
          encryption=False,  # Disable encryption for this test
        )

        result = await backup_manager.create_backup(backup_job)

        assert result.node_count == 50
        assert result.relationship_count == 25
        assert result.compression_ratio == 0.5
        backup_manager.s3_adapter.upload_backup.assert_called_once()

  @pytest.mark.asyncio
  async def test_backup_job_validation(self):
    """Test BackupJob validation logic."""
    # Test valid job
    valid_job = BackupJob(
      graph_id="test_graph",
      backup_format=BackupFormat.FULL_DUMP,
      backup_type=BackupType.FULL,
      encryption=True,
      allow_export=False,  # Required for encryption
    )
    assert valid_job.encryption is True
    assert valid_job.allow_export is False

    # Test invalid encryption with export
    with pytest.raises(
      ValueError, match="Encryption can only be enabled for non-exportable backups"
    ):
      BackupJob(
        graph_id="test_graph",
        encryption=True,
        allow_export=True,  # Invalid combination
      )

    # Test invalid encryption format
    with pytest.raises(
      ValueError, match="Encryption is only supported for full dump backups"
    ):
      BackupJob(
        graph_id="test_graph",
        backup_format=BackupFormat.CSV,
        encryption=True,
        allow_export=False,
      )

    # Test invalid graph ID
    with pytest.raises(ValueError):
      BackupJob(graph_id="invalid id with spaces")

  @pytest.mark.asyncio
  async def test_restore_backup_functionality(self, backup_manager):
    """Test restore backup functionality."""
    graph_id = "test_graph"

    # Mock backup metadata
    backup_metadata = MagicMock()
    backup_metadata.s3_key = "test_backup_key"
    backup_metadata.timestamp = datetime.now(timezone.utc)
    backup_metadata.backup_type = "full"

    restore_job = RestoreJob(
      graph_id=graph_id,
      backup_metadata=backup_metadata,
      backup_format=BackupFormat.FULL_DUMP,
      create_new_database=True,
      drop_existing=True,
      verify_after_restore=True,
    )

    with (
      patch.object(
        backup_manager, "_validate_backup_integrity", new_callable=AsyncMock
      ) as mock_validate,
      patch.object(
        backup_manager, "_drop_database_if_exists", new_callable=AsyncMock
      ) as mock_drop,
      patch.object(
        backup_manager, "_ensure_database_exists", new_callable=AsyncMock
      ) as mock_ensure,
      patch.object(
        backup_manager, "_import_backup_data", new_callable=AsyncMock
      ) as mock_import,
      patch.object(
        backup_manager, "_verify_restore", new_callable=AsyncMock
      ) as mock_verify,
    ):
      # Mock successful operations
      mock_validate.return_value = True
      mock_verify.return_value = True
      backup_manager.s3_adapter.download_backup_by_key = AsyncMock(
        return_value=b"backup_data"
      )

      result = await backup_manager.restore_backup(restore_job)

      assert result is True
      mock_validate.assert_called_once()
      mock_drop.assert_called_once()
      mock_ensure.assert_called_once()
      mock_import.assert_called_once()
      mock_verify.assert_called_once()

  @pytest.mark.asyncio
  async def test_restore_backup_error_scenarios(self, backup_manager):
    """Test error scenarios in restore backup."""
    graph_id = "test_graph"
    backup_metadata = MagicMock()
    backup_metadata.s3_key = "test_backup_key"

    restore_job = RestoreJob(
      graph_id=graph_id,
      backup_metadata=backup_metadata,
      backup_format=BackupFormat.FULL_DUMP,
    )

    # Test download failure
    backup_manager.s3_adapter.download_backup_by_key = AsyncMock(
      side_effect=Exception("Download failed")
    )

    with pytest.raises(Exception, match="Download failed"):
      await backup_manager.restore_backup(restore_job)

    # Test integrity check failure
    with (
      patch.object(
        backup_manager, "_validate_backup_integrity", new_callable=AsyncMock
      ) as mock_validate,
    ):
      backup_manager.s3_adapter.download_backup_by_key = AsyncMock(
        return_value=b"backup_data"
      )
      mock_validate.return_value = False

      with pytest.raises(ValueError, match="Backup integrity check failed"):
        await backup_manager.restore_backup(restore_job)

  @pytest.mark.asyncio
  async def test_list_backups_and_cleanup(self, backup_manager):
    """Test listing backups and cleanup operations."""
    graph_id = "test_graph"

    # Mock backup listing
    from datetime import datetime, timezone

    mock_backups = [
      {
        "id": "backup1",
        "key": f"{graph_id}/backup-20240101_000000-full.zip",
        "graph_id": graph_id,
        "backup_type": "full",
        "last_modified": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "size": 1000,
      },
      {
        "id": "backup2",
        "key": f"{graph_id}/backup-20240102_000000-full.zip",
        "graph_id": graph_id,
        "backup_type": "full",
        "last_modified": datetime(2024, 1, 2, tzinfo=timezone.utc),
        "size": 1500,
      },
    ]
    backup_manager.s3_adapter.list_backups = AsyncMock(return_value=mock_backups)

    # Test list_backups
    backups = await backup_manager.list_backups(graph_id)
    assert len(backups) == 2
    assert backups[0]["id"] == "backup1"
    backup_manager.s3_adapter.list_backups.assert_called_once_with(graph_id)

    # Test delete_old_backups
    backup_manager.s3_adapter.delete_backup = AsyncMock(return_value=True)
    retention_days = 30

    deleted_count = await backup_manager.delete_old_backups(graph_id, retention_days)

    # Should have called delete for old backups (implementation depends on date logic)
    assert isinstance(deleted_count, int)

  @pytest.mark.asyncio
  async def test_download_backup_functionality(self, backup_manager):
    """Test backup download and URL generation."""
    graph_id = "test_graph"
    backup_id = "backup_123"

    # Mock backup metadata
    mock_metadata = {
      "backup_format": "full_dump",
      "encryption_enabled": False,
    }
    backup_manager.s3_adapter.get_backup_metadata = AsyncMock(
      return_value=mock_metadata
    )
    backup_manager.s3_adapter.generate_download_url = AsyncMock(
      return_value="https://presigned-url.com"
    )

    # Test get_backup_download_url
    url = await backup_manager.get_backup_download_url(
      graph_id, backup_id, expires_in=3600
    )
    assert url == "https://presigned-url.com"
    backup_manager.s3_adapter.generate_download_url.assert_called_once_with(
      graph_id, backup_id, 3600
    )

    # Test encrypted backup download (should fail)
    mock_metadata["encryption_enabled"] = True
    url = await backup_manager.get_backup_download_url(graph_id, backup_id)
    assert url is None

    # Test download_backup
    backup_manager.s3_adapter.download_backup = AsyncMock(
      return_value=b"backup_content"
    )
    mock_metadata["encryption_enabled"] = False

    data, content_type, filename = await backup_manager.download_backup(
      graph_id, backup_id
    )

    assert data == b"backup_content"
    assert content_type == "application/zip"
    assert "database.kuzu.zip" in filename

  @pytest.mark.asyncio
  async def test_format_conversion_methods(self, backup_manager):
    """Test backup format conversion methods."""
    # Test _get_content_type_and_filename
    content_type, filename = backup_manager._get_content_type_and_filename(
      "csv", "backup123"
    )
    assert content_type == "text/csv"
    assert filename == "backup123.csv.zip"

    content_type, filename = backup_manager._get_content_type_and_filename(
      "json", "backup123"
    )
    assert content_type == "application/json"
    assert filename == "backup123.json.zip"

    content_type, filename = backup_manager._get_content_type_and_filename(
      "full_dump", "backup123"
    )
    assert content_type == "application/zip"
    assert filename == "backup123_database.kuzu.zip"

    # Test CSV to JSON conversion
    with patch("pandas.read_csv") as mock_read_csv:
      mock_df = MagicMock()
      mock_df.to_json.return_value = '{"test": "data"}'
      mock_read_csv.return_value = mock_df

      with patch("pathlib.Path") as mock_path:
        mock_path.return_value.write_bytes.return_value = None
        result = await backup_manager._convert_csv_to_json(mock_path)

        assert result == b'{"test": "data"}'
        mock_read_csv.assert_called_once()

    # Test JSON to CSV conversion
    with (
      patch("builtins.open", create=True) as mock_open,
      patch("pandas.DataFrame") as mock_df,
      patch("pandas.read_json"),
    ):
      mock_file = MagicMock()
      mock_file.__enter__.return_value = mock_file
      mock_file.read.return_value = '{"test": "data"}'
      mock_open.return_value = mock_file

      mock_df_instance = MagicMock()
      mock_df_instance.to_csv.return_value = "col1,col2\nval1,val2\n"
      mock_df.return_value = mock_df_instance

      with patch("pathlib.Path") as mock_path:
        result = await backup_manager._convert_json_to_csv(mock_path)

        assert result == b"col1,col2\nval1,val2\n"

  @pytest.mark.asyncio
  async def test_backup_integrity_validation(self, backup_manager):
    """Test backup integrity validation."""
    # Mock metadata
    metadata = MagicMock()
    metadata.checksum = "expected_checksum"
    metadata.encryption_key = None

    # Test successful validation
    with patch("hashlib.sha256") as mock_sha:
      mock_hash = MagicMock()
      mock_hash.hexdigest.return_value = "expected_checksum"
      mock_sha.return_value = mock_hash

      result = await backup_manager._validate_backup_integrity(b"test_data", metadata)
      assert result is True

    # Test failed validation
    with patch("hashlib.sha256") as mock_sha:
      mock_hash = MagicMock()
      mock_hash.hexdigest.return_value = "different_checksum"
      mock_sha.return_value = mock_hash

      result = await backup_manager._validate_backup_integrity(b"test_data", metadata)
      assert result is False

  @pytest.mark.asyncio
  async def test_database_management_methods(self, backup_manager):
    """Test database creation and deletion methods."""
    graph_id = "test_graph"

    with (
      patch(
        "robosystems.operations.kuzu.backup_manager.get_universal_repository"
      ) as mock_get_repo,
      patch("os.path.exists", return_value=True),
      patch("os.path.isfile", return_value=True),
      patch("os.remove") as mock_remove,
    ):
      # Mock repository
      mock_repo = MagicMock()
      mock_repo.execute_query = AsyncMock()
      mock_repo.__aenter__ = AsyncMock(return_value=mock_repo)
      mock_repo.__aexit__ = AsyncMock(return_value=None)

      async def mock_get_graph_repository(*args, **kwargs):
        return mock_repo

      mock_get_repo.side_effect = mock_get_graph_repository

      # Test _ensure_database_exists
      await backup_manager._ensure_database_exists(graph_id)
      mock_repo.execute_query.assert_called()

      # Reset mock
      mock_repo.reset_mock()

      # Test _drop_database_if_exists
      await backup_manager._drop_database_if_exists(graph_id)
      mock_remove.assert_called_once()
