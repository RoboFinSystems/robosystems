"""
Comprehensive tests for SharedRepositoryService.

Tests the critical shared repository service that manages shared graph repositories
like SEC, industry, and economic data accessible across multiple companies.
"""

import pytest
from unittest.mock import patch, AsyncMock

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
  def mock_kuzu_client(self):
    """Create a mock Kuzu client."""
    client = AsyncMock()
    client.create_database = AsyncMock(
      return_value={"status": "created", "graph_id": "sec"}
    )
    client.get_database_info = AsyncMock(
      return_value={
        "is_healthy": True,
        "node_count": 0,
        "relationship_count": 0,
        "size_mb": 0.1,
      }
    )
    client.close = AsyncMock()
    return client

  @pytest.mark.asyncio
  async def test_create_shared_repository_sec(self, service, mock_kuzu_client):
    """Test successful creation of SEC shared repository."""
    with patch(
      "robosystems.kuzu_api.client.factory.KuzuClientFactory._get_shared_master_url"
    ) as mock_get_url:
      mock_get_url.return_value = "http://10.0.1.100:8080"

      with patch(
        "robosystems.operations.graph.shared_repository_service.get_kuzu_client_for_instance"
      ) as mock_get_client:
        mock_get_client.return_value = mock_kuzu_client

        result = await service.create_shared_repository("sec", "user123")

        assert result["repository_name"] == "sec"
        assert result["graph_id"] == "sec"
        assert result["status"] == "created"
        assert result["created_by"] == "user123"
        assert "created_at" in result
        assert "database_info" in result

        # Verify Kuzu client was called correctly
        mock_kuzu_client.create_database.assert_called_once_with(
          graph_id="sec",
          schema_type="shared",
          repository_name="sec",
        )
        mock_kuzu_client.get_database_info.assert_called_once_with("sec")
        mock_kuzu_client.close.assert_called_once()

  @pytest.mark.asyncio
  async def test_create_shared_repository_all_types(self, service, mock_kuzu_client):
    """Test creation of all valid repository types."""
    valid_repos = ["sec", "industry", "economic", "regulatory", "market", "esg"]

    for repo_name in valid_repos:
      with patch(
        "robosystems.kuzu_api.client.factory.KuzuClientFactory._get_shared_master_url"
      ) as mock_get_url:
        mock_get_url.return_value = "http://10.0.1.100:8080"

        with patch(
          "robosystems.operations.graph.shared_repository_service.get_kuzu_client_for_instance"
        ) as mock_get_client:
          # Reset mock for each iteration
          mock_kuzu_client.create_database.reset_mock()
          mock_kuzu_client.get_database_info.reset_mock()
          mock_kuzu_client.close.reset_mock()
          mock_get_client.return_value = mock_kuzu_client

          result = await service.create_shared_repository(repo_name)

          assert result["repository_name"] == repo_name
          assert result["graph_id"] == repo_name
          assert result["status"] == "created"

  @pytest.mark.asyncio
  async def test_create_shared_repository_invalid_name(self, service):
    """Test creation with invalid repository name."""
    with pytest.raises(ValueError) as exc_info:
      await service.create_shared_repository("invalid_repo")

    assert "Invalid repository name" in str(exc_info.value)
    assert "Must be one of" in str(exc_info.value)

  @pytest.mark.asyncio
  async def test_create_shared_repository_invalid_url_format(self, service):
    """Test handling of invalid shared master URL format."""
    with patch(
      "robosystems.kuzu_api.client.factory.KuzuClientFactory._get_shared_master_url"
    ) as mock_get_url:
      # Return invalid URL format
      mock_get_url.return_value = "invalid://url:format"

      with pytest.raises(ValueError) as exc_info:
        await service.create_shared_repository("sec")

      assert "Invalid shared master URL format" in str(exc_info.value)

  @pytest.mark.asyncio
  async def test_create_shared_repository_unhealthy_database(
    self, service, mock_kuzu_client
  ):
    """Test handling when created database is not healthy."""
    # Make database unhealthy
    mock_kuzu_client.get_database_info.return_value = {
      "is_healthy": False,
      "error": "Database initialization failed",
    }

    with patch(
      "robosystems.kuzu_api.client.factory.KuzuClientFactory._get_shared_master_url"
    ) as mock_get_url:
      mock_get_url.return_value = "http://10.0.1.100:8080"

      with patch(
        "robosystems.operations.graph.shared_repository_service.get_kuzu_client_for_instance"
      ) as mock_get_client:
        mock_get_client.return_value = mock_kuzu_client

        with pytest.raises(RuntimeError) as exc_info:
          await service.create_shared_repository("sec")

        assert "created but not healthy" in str(exc_info.value)

        # Ensure cleanup was called
        mock_kuzu_client.close.assert_called_once()

  @pytest.mark.asyncio
  async def test_create_shared_repository_client_error(self, service):
    """Test handling of Kuzu client errors."""
    with patch(
      "robosystems.kuzu_api.client.factory.KuzuClientFactory._get_shared_master_url"
    ) as mock_get_url:
      mock_get_url.return_value = "http://10.0.1.100:8080"

      with patch(
        "robosystems.operations.graph.shared_repository_service.get_kuzu_client_for_instance"
      ) as mock_get_client:
        mock_get_client.side_effect = ConnectionError("Failed to connect to Kuzu")

        with pytest.raises(ConnectionError) as exc_info:
          await service.create_shared_repository("sec")

        assert "Failed to connect to Kuzu" in str(exc_info.value)

  @pytest.mark.asyncio
  async def test_create_shared_repository_creation_failure(
    self, service, mock_kuzu_client
  ):
    """Test handling when database creation fails."""
    mock_kuzu_client.create_database.side_effect = RuntimeError(
      "Database already exists"
    )

    with patch(
      "robosystems.kuzu_api.client.factory.KuzuClientFactory._get_shared_master_url"
    ) as mock_get_url:
      mock_get_url.return_value = "http://10.0.1.100:8080"

      with patch(
        "robosystems.operations.graph.shared_repository_service.get_kuzu_client_for_instance"
      ) as mock_get_client:
        mock_get_client.return_value = mock_kuzu_client

        with pytest.raises(RuntimeError) as exc_info:
          await service.create_shared_repository("sec")

        assert "Database already exists" in str(exc_info.value)

        # Ensure cleanup was still called
        mock_kuzu_client.close.assert_called_once()

  @pytest.mark.asyncio
  async def test_create_shared_repository_no_user(self, service, mock_kuzu_client):
    """Test creation without specifying created_by user."""
    with patch(
      "robosystems.kuzu_api.client.factory.KuzuClientFactory._get_shared_master_url"
    ) as mock_get_url:
      mock_get_url.return_value = "http://10.0.1.100:8080"

      with patch(
        "robosystems.operations.graph.shared_repository_service.get_kuzu_client_for_instance"
      ) as mock_get_client:
        mock_get_client.return_value = mock_kuzu_client

        result = await service.create_shared_repository("industry")

        assert result["created_by"] == "system"

  @pytest.mark.asyncio
  async def test_url_parsing(self, service):
    """Test URL parsing logic with various formats."""
    test_cases = [
      ("http://10.0.1.100:8080", "10.0.1.100"),
      ("http://192.168.1.1:9000", "192.168.1.1"),
      ("http://localhost:8080", "localhost"),
      ("http://kuzu-master.internal:8080", "kuzu-master.internal"),
    ]

    for url, expected_ip in test_cases:
      with patch(
        "robosystems.kuzu_api.client.factory.KuzuClientFactory._get_shared_master_url"
      ) as mock_get_url:
        mock_get_url.return_value = url

        with patch(
          "robosystems.operations.graph.shared_repository_service.get_kuzu_client_for_instance"
        ) as mock_get_client:
          mock_kuzu_client = AsyncMock()
          mock_kuzu_client.create_database = AsyncMock(
            return_value={"status": "created"}
          )
          mock_kuzu_client.get_database_info = AsyncMock(
            return_value={"is_healthy": True}
          )
          mock_kuzu_client.close = AsyncMock()
          mock_get_client.return_value = mock_kuzu_client

          await service.create_shared_repository("sec")

          # Verify the IP was extracted correctly
          mock_get_client.assert_called_once_with(expected_ip)


class TestEnsureSharedRepositoryExists:
  """Tests for ensure_shared_repository_exists function."""

  @pytest.fixture
  def mock_client(self):
    """Create a mock Kuzu client."""
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
      "robosystems.kuzu_api.client.factory.KuzuClientFactory.create_client"
    ) as mock_factory:
      mock_factory.return_value = mock_client

      result = await ensure_shared_repository_exists("sec")

      assert result["status"] == "exists"
      assert result["repository_name"] == "sec"
      assert "database_info" in result

      # Verify factory was called with correct parameters
      mock_factory.assert_called_once_with(graph_id="sec", operation_type="read")
      mock_client.close.assert_called_once()

  @pytest.mark.asyncio
  async def test_repository_does_not_exist_creates_it(self, mock_client):
    """Test when repository doesn't exist and needs to be created."""
    # First call fails (repository doesn't exist)
    mock_client.get_database_info.side_effect = Exception("Database not found")

    with patch(
      "robosystems.kuzu_api.client.factory.KuzuClientFactory.create_client"
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

        # Verify creation was attempted
        mock_create.assert_called_once_with("economic")
        mock_client.close.assert_called_once()

  @pytest.mark.asyncio
  async def test_repository_exists_but_unhealthy(self, mock_client):
    """Test when repository exists but is not healthy."""
    mock_client.get_database_info.return_value = {
      "is_healthy": False,
      "error": "Database corrupted",
    }

    with patch(
      "robosystems.kuzu_api.client.factory.KuzuClientFactory.create_client"
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

        # Should try to create since unhealthy
        assert result["status"] == "created"
        mock_create.assert_called_once_with("industry")

  @pytest.mark.asyncio
  async def test_repository_with_custom_url(self, mock_client):
    """Test ensure with custom Kuzu URL."""
    mock_client.get_database_info.return_value = {
      "is_healthy": True,
      "node_count": 50,
    }

    with patch(
      "robosystems.kuzu_api.client.factory.KuzuClientFactory.create_client"
    ) as mock_factory:
      mock_factory.return_value = mock_client

      custom_url = "http://custom-kuzu:8080"
      result = await ensure_shared_repository_exists("market", kuzu_url=custom_url)

      assert result["status"] == "exists"
      assert result["repository_name"] == "market"

  @pytest.mark.asyncio
  async def test_repository_creation_error_handling(self, mock_client):
    """Test error handling during repository creation."""
    mock_client.get_database_info.side_effect = Exception("Not found")

    with patch(
      "robosystems.kuzu_api.client.factory.KuzuClientFactory.create_client"
    ) as mock_factory:
      mock_factory.return_value = mock_client

      with patch(
        "robosystems.operations.graph.shared_repository_service.SharedRepositoryService.create_shared_repository"
      ) as mock_create:
        mock_create.side_effect = RuntimeError("Creation failed")

        with pytest.raises(RuntimeError) as exc_info:
          await ensure_shared_repository_exists("regulatory")

        assert "Creation failed" in str(exc_info.value)
        mock_client.close.assert_called_once()


class TestIntegration:
  """Integration tests for shared repository service."""

  @pytest.mark.asyncio
  @pytest.mark.integration
  async def test_full_repository_lifecycle(self):
    """Test complete repository creation and verification lifecycle."""
    service = SharedRepositoryService()

    with patch(
      "robosystems.kuzu_api.client.factory.KuzuClientFactory._get_shared_master_url"
    ) as mock_get_url:
      mock_get_url.return_value = "http://10.0.1.100:8080"

      with patch(
        "robosystems.operations.graph.shared_repository_service.get_kuzu_client_for_instance"
      ) as mock_get_client:
        # Create comprehensive mock client
        mock_client = AsyncMock()
        mock_client.create_database = AsyncMock(return_value={"status": "created"})
        mock_client.get_database_info = AsyncMock(
          return_value={
            "is_healthy": True,
            "node_count": 0,
            "relationship_count": 0,
            "size_mb": 0.1,
            "schema_version": "1.0.0",
          }
        )
        mock_client.close = AsyncMock()
        mock_get_client.return_value = mock_client

        # Create repository
        create_result = await service.create_shared_repository("esg", "admin_user")

        assert create_result["status"] == "created"
        assert create_result["repository_name"] == "esg"
        assert create_result["created_by"] == "admin_user"

        # Verify it exists
        with patch(
          "robosystems.kuzu_api.client.factory.KuzuClientFactory.create_client"
        ) as mock_factory:
          mock_factory.return_value = mock_client

          exists_result = await ensure_shared_repository_exists("esg")
          assert exists_result["status"] == "exists"

  @pytest.mark.asyncio
  @pytest.mark.integration
  async def test_concurrent_repository_creation(self):
    """Test handling of concurrent repository creation attempts."""
    import asyncio

    service = SharedRepositoryService()

    with patch(
      "robosystems.kuzu_api.client.factory.KuzuClientFactory._get_shared_master_url"
    ) as mock_get_url:
      mock_get_url.return_value = "http://10.0.1.100:8080"

      with patch(
        "robosystems.operations.graph.shared_repository_service.get_kuzu_client_for_instance"
      ) as mock_get_client:
        call_count = 0

        async def create_database_mock(*args, **kwargs):
          nonlocal call_count
          call_count += 1
          if call_count == 1:
            # First call succeeds
            return {"status": "created"}
          else:
            # Subsequent calls fail (already exists)
            raise RuntimeError("Database already exists")

        mock_client = AsyncMock()
        mock_client.create_database = AsyncMock(side_effect=create_database_mock)
        mock_client.get_database_info = AsyncMock(return_value={"is_healthy": True})
        mock_client.close = AsyncMock()
        mock_get_client.return_value = mock_client

        # Try to create the same repository concurrently
        tasks = [
          service.create_shared_repository("market"),
          service.create_shared_repository("market"),
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # One should succeed, one should fail
        success_count = sum(
          1 for r in results if isinstance(r, dict) and r.get("status") == "created"
        )
        error_count = sum(1 for r in results if isinstance(r, Exception))

        assert success_count == 1
        assert error_count == 1
