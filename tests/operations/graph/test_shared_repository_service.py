"""
Tests for SharedRepositoryService.

Tests the shared repository service that manages shared graph repositories
like SEC, industry, and economic data accessible across multiple users.
"""

import pytest
from unittest.mock import patch, AsyncMock, MagicMock

from robosystems.operations.graph.shared_repository_service import (
  SharedRepositoryService,
  ensure_shared_repository_exists,
)


class TestSharedRepositoryService:
  """Tests for SharedRepositoryService class."""

  @pytest.fixture
  def service(self):
    """Create a SharedRepositoryService instance."""
    return SharedRepositoryService()

  @pytest.fixture
  def mock_lbug_client(self):
    """Create a mock LadybugDB client."""
    client = AsyncMock()

    # Create a proper 404 exception with status_code
    not_found = Exception("Database not found")
    not_found.status_code = 404

    client.get_database = AsyncMock(side_effect=not_found)
    client.create_database = AsyncMock(return_value={"status": "created"})
    client.install_schema = AsyncMock(return_value={"status": "success"})
    client.get_database_info = AsyncMock(
      return_value={
        "is_healthy": True,
        "node_count": 10,
        "relationship_count": 20,
        "size_mb": 1.5,
        "tables": [],
      }
    )
    client.close = AsyncMock()
    return client

  @pytest.fixture
  def mock_db_session(self):
    """Create a mock database session."""
    session = MagicMock()
    session.commit = MagicMock()
    session.close = MagicMock()
    return session

  @pytest.mark.asyncio
  async def test_create_shared_repository_sec(
    self, service, mock_lbug_client, mock_db_session
  ):
    """Test successful creation of SEC shared repository."""
    with patch(
      "robosystems.graph_api.client.factory.GraphClientFactory.create_client"
    ) as mock_factory:
      mock_factory.return_value = mock_lbug_client

      with patch("robosystems.database.get_db_session") as mock_db:
        mock_db.return_value = iter([mock_db_session])

        with patch("robosystems.models.iam.graph.Graph") as mock_graph_cls:
          mock_graph = MagicMock()
          mock_graph.graph_id = "sec"
          mock_graph_cls.find_or_create_repository.return_value = mock_graph

          result = await service.create_shared_repository("sec", "user123")

          assert result["repository_name"] == "sec"
          assert result["graph_id"] == "sec"
          assert result["status"] == "created"
          assert result["created_by"] == "user123"
          assert "created_at" in result
          assert "database_info" in result
          assert result["config"]["name"] == "SEC EDGAR Filings"

          mock_factory.assert_called_once_with(graph_id="sec", operation_type="write")
          mock_lbug_client.close.assert_called_once()

  @pytest.mark.asyncio
  async def test_create_shared_repository_invalid_name(self, service):
    """Test creation with invalid repository name."""
    with pytest.raises(ValueError) as exc_info:
      await service.create_shared_repository("invalid_repo")

    assert "Invalid repository name" in str(exc_info.value)
    assert "Must be one of" in str(exc_info.value)

  @pytest.mark.asyncio
  async def test_create_shared_repository_all_types(
    self, service, mock_lbug_client, mock_db_session
  ):
    """Test creation of all valid repository types."""
    valid_repos = ["sec", "industry", "economic"]

    for repo_name in valid_repos:
      with patch(
        "robosystems.graph_api.client.factory.GraphClientFactory.create_client"
      ) as mock_factory:
        mock_factory.return_value = mock_lbug_client

        with patch("robosystems.database.get_db_session") as mock_db:
          mock_db.return_value = iter([mock_db_session])

          with patch("robosystems.models.iam.graph.Graph") as mock_graph_cls:
            mock_graph = MagicMock()
            mock_graph.graph_id = repo_name
            mock_graph_cls.find_or_create_repository.return_value = mock_graph

            result = await service.create_shared_repository(repo_name)

            assert result["repository_name"] == repo_name
            assert result["graph_id"] == repo_name
            assert result["status"] == "created"


class TestEnsureSharedRepositoryExists:
  """Tests for ensure_shared_repository_exists function."""

  @pytest.fixture
  def mock_client(self):
    """Create a mock LadybugDB client."""
    client = AsyncMock()
    client.get_database_info = AsyncMock()
    client.close = AsyncMock()
    return client

  @pytest.mark.asyncio
  async def test_repository_already_exists(self, mock_client):
    """Test when repository already exists and is healthy."""
    mock_client.get_database_info.return_value = {
      "is_healthy": True,
      "node_count": 100,
      "relationship_count": 500,
    }

    with patch(
      "robosystems.graph_api.client.factory.GraphClientFactory.create_client"
    ) as mock_factory:
      mock_factory.return_value = mock_client

      result = await ensure_shared_repository_exists("sec")

      assert result["status"] == "exists"
      assert result["repository_name"] == "sec"
      assert "database_info" in result

      mock_factory.assert_called_once_with(graph_id="sec", operation_type="read")
      mock_client.close.assert_called_once()

  @pytest.mark.asyncio
  async def test_repository_does_not_exist_creates_it(self, mock_client):
    """Test when repository doesn't exist and needs to be created."""
    mock_client.get_database_info.side_effect = Exception("Database not found")

    with patch(
      "robosystems.graph_api.client.factory.GraphClientFactory.create_client"
    ) as mock_factory:
      mock_factory.return_value = mock_client

      with patch(
        "robosystems.operations.graph.shared_repository_service.SharedRepositoryService.create_shared_repository"
      ) as mock_create:
        mock_create.return_value = {
          "status": "created",
          "repository_name": "economic",
          "graph_id": "economic",
        }

        result = await ensure_shared_repository_exists("economic")

        assert result["status"] == "created"
        assert result["repository_name"] == "economic"

        mock_create.assert_called_once()
        mock_client.close.assert_called_once()

  @pytest.mark.asyncio
  async def test_repository_exists_but_unhealthy(self, mock_client):
    """Test when repository exists but is not healthy."""
    mock_client.get_database_info.return_value = {
      "is_healthy": False,
      "error": "Database corrupted",
    }

    with patch(
      "robosystems.graph_api.client.factory.GraphClientFactory.create_client"
    ) as mock_factory:
      mock_factory.return_value = mock_client

      with patch(
        "robosystems.operations.graph.shared_repository_service.SharedRepositoryService.create_shared_repository"
      ) as mock_create:
        mock_create.return_value = {
          "status": "created",
          "repository_name": "industry",
        }

        result = await ensure_shared_repository_exists("industry")

        assert result["status"] == "created"
        mock_create.assert_called_once()
