"""
Tests for repository factory dependencies.

These tests cover the FastAPI dependency functions for creating graph repositories
with proper routing and access control.
"""

from unittest.mock import Mock, patch

import pytest
from fastapi import HTTPException, status

from robosystems.middleware.graph.dependencies.repositories import (
  get_graph_repository_dependency,
  get_main_repository,
  get_shared_repository,
  get_user_graph_repository,
)


class TestGetGraphRepositoryDependency:
  """Test get_graph_repository_dependency function."""

  @pytest.fixture
  def mock_user(self):
    """Create a mock user."""
    user = Mock()
    user.id = "user123"
    user.email = "test@example.com"
    return user

  @pytest.mark.asyncio
  @patch("robosystems.middleware.graph.dependencies.repositories.get_graph_repository")
  @patch("robosystems.middleware.graph.dependencies.repositories.MultiTenantUtils")
  async def test_create_repository_for_user_graph(
    self, mock_utils, mock_get_repo, mock_user
  ):
    """Test creating repository for a user graph."""
    graph_id = "kg1234567890abcdef"
    mock_repository = Mock()

    mock_identity = Mock()
    mock_identity.is_shared_repository = False
    mock_identity.category.value = "user"

    mock_utils.get_graph_routing.return_value = {
      "graph_identity": mock_identity,
    }
    mock_get_repo.return_value = mock_repository

    result = await get_graph_repository_dependency(
      graph_id=graph_id,
      current_user=mock_user,
      operation_type="write",
    )

    assert result == mock_repository
    mock_get_repo.assert_called_once_with(graph_id, operation_type="write")

  @pytest.mark.asyncio
  @patch("robosystems.middleware.graph.dependencies.repositories.get_graph_repository")
  @patch("robosystems.middleware.graph.dependencies.repositories.MultiTenantUtils")
  async def test_create_repository_for_shared_repo_read(
    self, mock_utils, mock_get_repo, mock_user
  ):
    """Test creating read-only repository for shared repository."""
    repo_id = "sec"
    mock_repository = Mock()

    mock_identity = Mock()
    mock_identity.is_shared_repository = True
    mock_identity.category.value = "shared"

    mock_utils.get_graph_routing.return_value = {
      "graph_identity": mock_identity,
    }
    mock_get_repo.return_value = mock_repository

    result = await get_graph_repository_dependency(
      graph_id=repo_id,
      current_user=mock_user,
      operation_type="read",
    )

    assert result == mock_repository

  @pytest.mark.asyncio
  @patch("robosystems.middleware.graph.dependencies.repositories.MultiTenantUtils")
  async def test_write_to_shared_repository_forbidden(self, mock_utils, mock_user):
    """Test write operations are forbidden on shared repositories."""
    repo_id = "sec"

    mock_identity = Mock()
    mock_identity.is_shared_repository = True

    mock_utils.get_graph_routing.return_value = {
      "graph_identity": mock_identity,
    }

    with pytest.raises(HTTPException) as exc_info:
      await get_graph_repository_dependency(
        graph_id=repo_id,
        current_user=mock_user,
        operation_type="write",
      )

    assert exc_info.value.status_code == status.HTTP_403_FORBIDDEN
    assert "Write operations not allowed on shared repository" in str(
      exc_info.value.detail
    )

  @pytest.mark.asyncio
  @patch("robosystems.middleware.graph.dependencies.repositories.get_graph_repository")
  @patch("robosystems.middleware.graph.dependencies.repositories.MultiTenantUtils")
  async def test_repository_creation_error(self, mock_utils, mock_get_repo, mock_user):
    """Test handling of repository creation errors."""
    graph_id = "kg1234567890abcdef"

    mock_identity = Mock()
    mock_identity.is_shared_repository = False
    mock_identity.category.value = "user"

    mock_utils.get_graph_routing.return_value = {
      "graph_identity": mock_identity,
    }
    mock_get_repo.side_effect = RuntimeError("Connection failed")

    with pytest.raises(HTTPException) as exc_info:
      await get_graph_repository_dependency(
        graph_id=graph_id,
        current_user=mock_user,
        operation_type="read",
      )

    assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "Failed to access graph" in str(exc_info.value.detail)


class TestGetUserGraphRepository:
  """Test get_user_graph_repository function."""

  @pytest.fixture
  def mock_user(self):
    """Create a mock user."""
    user = Mock()
    user.id = "user123"
    user.email = "test@example.com"
    return user

  @pytest.fixture
  def mock_db_session(self):
    """Create a mock database session."""
    return Mock()

  @pytest.mark.asyncio
  @patch(
    "robosystems.middleware.graph.dependencies.repositories.get_graph_repository_dependency"
  )
  @patch("robosystems.middleware.graph.dependencies.repositories.MultiTenantUtils")
  async def test_user_graph_read_access(
    self, mock_utils, mock_get_repo_dep, mock_user, mock_db_session
  ):
    """Test read access to user graph."""
    graph_id = "kg1234567890abcdef"
    mock_repository = Mock()

    mock_identity = Mock()
    mock_identity.is_user_graph = True

    mock_utils.get_graph_identity.return_value = mock_identity
    mock_get_repo_dep.return_value = mock_repository

    with patch(
      "robosystems.middleware.graph.dependencies.repositories.GraphUser"
    ) as mock_graph_user:
      mock_graph_user.user_has_access.return_value = True

      result = await get_user_graph_repository(
        graph_id=graph_id,
        current_user=mock_user,
        operation_type="read",
        db=mock_db_session,
      )

      assert result == mock_repository

  @pytest.mark.asyncio
  @patch("robosystems.middleware.graph.dependencies.repositories.MultiTenantUtils")
  async def test_not_user_graph_rejected(self, mock_utils, mock_user, mock_db_session):
    """Test that non-user graphs are rejected."""
    repo_id = "sec"

    mock_identity = Mock()
    mock_identity.is_user_graph = False

    mock_utils.get_graph_identity.return_value = mock_identity

    with pytest.raises(HTTPException) as exc_info:
      await get_user_graph_repository(
        graph_id=repo_id,
        current_user=mock_user,
        operation_type="read",
        db=mock_db_session,
      )

    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
    assert "is not a user graph" in str(exc_info.value.detail)

  @pytest.mark.asyncio
  @patch("robosystems.middleware.graph.dependencies.repositories.MultiTenantUtils")
  @patch("robosystems.middleware.graph.dependencies.repositories.SecurityAuditLogger")
  async def test_user_no_access_to_graph(
    self, mock_audit, mock_utils, mock_user, mock_db_session
  ):
    """Test access denied when user has no access to graph."""
    graph_id = "kg1234567890abcdef"

    mock_identity = Mock()
    mock_identity.is_user_graph = True

    mock_utils.get_graph_identity.return_value = mock_identity

    with patch(
      "robosystems.middleware.graph.dependencies.repositories.GraphUser"
    ) as mock_graph_user:
      mock_graph_user.user_has_access.return_value = False

      with pytest.raises(HTTPException) as exc_info:
        await get_user_graph_repository(
          graph_id=graph_id,
          current_user=mock_user,
          operation_type="read",
          db=mock_db_session,
        )

      assert exc_info.value.status_code == status.HTTP_403_FORBIDDEN
      mock_audit.log_authorization_denied.assert_called_once()

  @pytest.mark.asyncio
  @patch("robosystems.middleware.graph.dependencies.repositories.MultiTenantUtils")
  async def test_write_access_denied_for_non_admin_member(
    self, mock_utils, mock_user, mock_db_session
  ):
    """Test write access denied for non-admin/member roles."""
    graph_id = "kg1234567890abcdef"

    mock_identity = Mock()
    mock_identity.is_user_graph = True

    mock_utils.get_graph_identity.return_value = mock_identity

    with patch(
      "robosystems.middleware.graph.dependencies.repositories.GraphUser"
    ) as mock_graph_user:
      mock_graph_user.user_has_access.return_value = True
      mock_user_graph = Mock()
      mock_user_graph.role = "viewer"  # Not admin or member
      mock_graph_user.get_by_user_and_graph.return_value = mock_user_graph

      with pytest.raises(HTTPException) as exc_info:
        await get_user_graph_repository(
          graph_id=graph_id,
          current_user=mock_user,
          operation_type="write",
          db=mock_db_session,
        )

      assert exc_info.value.status_code == status.HTTP_403_FORBIDDEN
      assert "Write access denied" in str(exc_info.value.detail)


class TestGetSharedRepository:
  """Test get_shared_repository function."""

  @pytest.fixture
  def mock_user(self):
    """Create a mock user."""
    user = Mock()
    user.id = "user123"
    user.email = "test@example.com"
    return user

  @pytest.fixture
  def mock_db_session(self):
    """Create a mock database session."""
    return Mock()

  @pytest.mark.asyncio
  @patch(
    "robosystems.middleware.graph.dependencies.repositories.get_graph_repository_dependency"
  )
  @patch("robosystems.middleware.graph.dependencies.repositories.MultiTenantUtils")
  async def test_shared_repository_access(
    self, mock_utils, mock_get_repo_dep, mock_user, mock_db_session
  ):
    """Test accessing shared repository with subscription."""
    repo_name = "sec"
    mock_repository = Mock()

    mock_utils.is_shared_repository.return_value = True
    mock_get_repo_dep.return_value = mock_repository

    with patch(
      "robosystems.middleware.graph.dependencies.repositories.UserRepository"
    ) as mock_user_repo:
      mock_user_repo.user_has_access.return_value = True

      result = await get_shared_repository(
        repository_name=repo_name,
        current_user=mock_user,
        db=mock_db_session,
      )

      assert result == mock_repository
      # Should be read-only
      mock_get_repo_dep.assert_called_once_with(repo_name, mock_user, "read")

  @pytest.mark.asyncio
  @patch("robosystems.middleware.graph.dependencies.repositories.MultiTenantUtils")
  async def test_invalid_shared_repository_rejected(
    self, mock_utils, mock_user, mock_db_session
  ):
    """Test invalid repository name is rejected."""
    repo_name = "invalid_repo"

    mock_utils.is_shared_repository.return_value = False

    with pytest.raises(HTTPException) as exc_info:
      await get_shared_repository(
        repository_name=repo_name,
        current_user=mock_user,
        db=mock_db_session,
      )

    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
    assert "is not a valid shared repository" in str(exc_info.value.detail)

  @pytest.mark.asyncio
  @patch("robosystems.middleware.graph.dependencies.repositories.MultiTenantUtils")
  @patch("robosystems.middleware.graph.dependencies.repositories.SecurityAuditLogger")
  async def test_shared_repository_no_subscription(
    self, mock_audit, mock_utils, mock_user, mock_db_session
  ):
    """Test access denied without subscription."""
    repo_name = "sec"

    mock_utils.is_shared_repository.return_value = True

    with patch(
      "robosystems.middleware.graph.dependencies.repositories.UserRepository"
    ) as mock_user_repo:
      mock_user_repo.user_has_access.return_value = False

      with pytest.raises(HTTPException) as exc_info:
        await get_shared_repository(
          repository_name=repo_name,
          current_user=mock_user,
          db=mock_db_session,
        )

      assert exc_info.value.status_code == status.HTTP_403_FORBIDDEN
      assert "Please subscribe to access" in str(exc_info.value.detail)
      mock_audit.log_authorization_denied.assert_called_once()


class TestGetMainRepository:
  """Test get_main_repository function."""

  @pytest.fixture
  def mock_user(self):
    """Create a mock user."""
    user = Mock()
    user.id = "user123"
    user.email = "test@example.com"
    return user

  @pytest.mark.asyncio
  @patch("robosystems.middleware.graph.dependencies.repositories.get_graph_repository")
  async def test_main_repository_success(self, mock_get_repo, mock_user):
    """Test successful main repository creation."""
    mock_repository = Mock()
    mock_get_repo.return_value = mock_repository

    result = await get_main_repository(current_user=mock_user)

    assert result == mock_repository
    mock_get_repo.assert_called_once_with("default", operation_type="write")

  @pytest.mark.asyncio
  @patch("robosystems.middleware.graph.dependencies.repositories.get_graph_repository")
  async def test_main_repository_error(self, mock_get_repo, mock_user):
    """Test main repository creation error handling."""
    mock_get_repo.side_effect = RuntimeError("Connection failed")

    with pytest.raises(HTTPException) as exc_info:
      await get_main_repository(current_user=mock_user)

    assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "Failed to access main database" in str(exc_info.value.detail)
