"""
Integration tests for subgraph operations.

Tests the complete flow of creating and managing subgraphs including:
- Schema installation
- Data validation
- Backup creation
- Statistics collection
- Security validation
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.orm import Session

from robosystems.operations.graph.subgraph_service import SubgraphService


@pytest.mark.asyncio
class TestSubgraphOperations:
  """Test subgraph operations including all TODO implementations."""

  @pytest.fixture
  async def mock_lbug_client(self):
    """Create a mock Graph API client."""
    from robosystems.graph_api.client import GraphClient

    # Spec'd, so a call to a method the real client lacks fails here too.
    client = AsyncMock(spec=GraphClient)

    # Mock database operations
    client.create_database = AsyncMock(
      return_value={"success": True, "database_name": "test_subgraph"}
    )

    client.install_schema = AsyncMock(
      return_value={"success": True, "message": "Schema installed"}
    )

    client.get_database = AsyncMock(
      return_value={
        "name": "test_subgraph",
        "size_mb": 10.5,
        "last_modified": datetime.now(UTC).isoformat(),
      }
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

  async def test_check_database_has_data(self, subgraph_service, mock_lbug_client):
    """Test database data validation."""
    # Test empty database
    mock_lbug_client.query.return_value = {"data": [{"node_count": 0}]}
    has_data = await subgraph_service._check_database_has_data(
      mock_lbug_client, "test_db"
    )
    assert has_data is False

    # Reset mock and test database with data
    mock_lbug_client.query.return_value = {"data": [{"node_count": 100}]}
    has_data = await subgraph_service._check_database_has_data(
      mock_lbug_client, "test_db"
    )
    assert has_data is True

  async def test_get_database_stats(self, subgraph_service, mock_lbug_client):
    """Test database statistics collection."""
    # Set up return values for the two queries
    mock_lbug_client.query.side_effect = [
      {"data": [{"count": 100}]},  # Node count query
      {"data": [{"count": 50}]},  # Edge count query
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

  async def test_stats_error_handling(self, subgraph_service, mock_lbug_client):
    """Test statistics collection error handling."""
    mock_lbug_client.query.side_effect = Exception("Query failed")

    stats = await subgraph_service._get_database_stats(mock_lbug_client, "test_db")

    # Should return None values on error
    assert stats["node_count"] is None
    assert stats["edge_count"] is None
    assert stats["size_mb"] is None
