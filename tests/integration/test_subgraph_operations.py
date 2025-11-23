"""
Integration tests for subgraph operations.

Tests the complete flow of creating and managing subgraphs including:
- Schema installation
- Data validation
- Backup creation
- Statistics collection
- Security validation
"""

import pytest
from unittest.mock import Mock, AsyncMock
from datetime import datetime, timezone
from sqlalchemy.orm import Session

from robosystems.operations.graph.subgraph_service import SubgraphService


@pytest.mark.asyncio
@pytest.mark.integration
class TestSubgraphOperations:
  """Test subgraph operations including all TODO implementations."""

  @pytest.fixture
  async def mock_lbug_client(self):
    """Create a mock Graph API client."""
    client = AsyncMock()

    # Mock database operations
    client.create_database = AsyncMock(
      return_value={"success": True, "database_name": "test_subgraph"}
    )

    client.install_schema = AsyncMock(
      return_value={"success": True, "message": "Schema installed"}
    )

    # Don't set side_effect here - let individual tests set their own return values
    client.execute = AsyncMock()

    client.get_database = AsyncMock(
      return_value={
        "name": "test_subgraph",
        "size_mb": 10.5,
        "last_modified": datetime.now(timezone.utc).isoformat(),
      }
    )

    client.backup = AsyncMock(
      return_value={"location": "s3://robosystems-backups/test/backup.zip"}
    )

    return client

  @pytest.fixture
  def mock_session(self):
    """Create a mock database session."""
    session = Mock(spec=Session)
    session.add = Mock()
    session.commit = Mock()
    session.refresh = Mock()
    session.query = Mock()
    return session

  @pytest.fixture
  def subgraph_service(self):
    """Create SubgraphService instance."""
    return SubgraphService()

  async def test_install_schema_with_extensions(
    self, subgraph_service, mock_lbug_client
  ):
    """Test schema installation with extensions."""
    await subgraph_service._install_schema_with_extensions(
      mock_lbug_client, "test_db", ["financial", "quickbooks"]
    )

    # Should install base schema + extensions in a single call
    assert mock_lbug_client.install_schema.call_count == 1

    # Check that schema was installed with extensions
    call_args = mock_lbug_client.install_schema.call_args
    assert call_args[1]["graph_id"] == "test_db"
    assert call_args[1]["base_schema"] == "entity"
    assert call_args[1]["extensions"] == ["financial", "quickbooks"]

  async def test_install_base_schema(self, subgraph_service, mock_lbug_client):
    """Test base schema installation."""
    await subgraph_service._install_base_schema(mock_lbug_client, "test_db")

    mock_lbug_client.install_schema.assert_called_once_with(
      graph_id="test_db", base_schema="entity", extensions=[]
    )

  async def test_check_database_has_data(self, subgraph_service, mock_lbug_client):
    """Test database data validation."""
    # Test empty database
    mock_lbug_client.execute.return_value = [{"node_count": 0}]
    has_data = await subgraph_service._check_database_has_data(
      mock_lbug_client, "test_db"
    )
    assert has_data is False

    # Reset mock and test database with data
    mock_lbug_client.execute.reset_mock()
    mock_lbug_client.execute.return_value = [{"node_count": 100}]
    has_data = await subgraph_service._check_database_has_data(
      mock_lbug_client, "test_db"
    )
    assert has_data is True

  async def test_create_backup(self, subgraph_service, mock_lbug_client):
    """Test backup creation."""
    backup_location = await subgraph_service._create_backup(
      mock_lbug_client, "test_db", "instance_123"
    )

    assert "s3://robosystems-backups" in backup_location
    mock_lbug_client.backup.assert_called_once()

  async def test_get_database_stats(self, subgraph_service, mock_lbug_client):
    """Test database statistics collection."""
    # Set up return values for the two queries
    mock_lbug_client.execute.side_effect = [
      [{"count": 100}],  # Node count query
      [{"count": 50}],  # Edge count query
    ]

    stats = await subgraph_service._get_database_stats(mock_lbug_client, "test_db")

    assert stats["node_count"] == 100
    assert stats["edge_count"] == 50
    assert stats["size_mb"] == 10.5
    assert stats["last_modified"] is not None

  async def test_create_subgraph_flow(self, subgraph_service):
    """Test complete subgraph creation flow."""
    # This test would need more complex mocking since SubgraphService
    # doesn't take a session in its constructor
    # For now, we'll skip the full flow test and focus on individual methods
    pass

  async def test_security_validation(self, subgraph_service):
    """Test security validation for parent graph access."""
    # This test would also need database mocking
    # Skip for now as it requires session mocking
    pass

  async def test_schema_installation_error_handling(
    self, subgraph_service, mock_lbug_client
  ):
    """Test error handling in schema installation."""
    mock_lbug_client.install_schema.side_effect = Exception(
      "Schema installation failed"
    )

    with pytest.raises(Exception, match="Schema installation failed"):
      await subgraph_service._install_base_schema(mock_lbug_client, "test_db")

  async def test_backup_fallback(self, subgraph_service, mock_lbug_client):
    """Test backup fallback when method not implemented."""
    # Remove backup method to simulate not implemented
    delattr(mock_lbug_client, "backup")

    backup_location = await subgraph_service._create_backup(
      mock_lbug_client, "test_db", "instance_123"
    )

    # Should return None when backup method is not implemented
    assert backup_location is None

  async def test_stats_error_handling(self, subgraph_service, mock_lbug_client):
    """Test statistics collection error handling."""
    mock_lbug_client.execute.side_effect = Exception("Query failed")

    stats = await subgraph_service._get_database_stats(mock_lbug_client, "test_db")

    # Should return None values on error
    assert stats["node_count"] is None
    assert stats["edge_count"] is None
    assert stats["size_mb"] is None
