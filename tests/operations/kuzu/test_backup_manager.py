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
