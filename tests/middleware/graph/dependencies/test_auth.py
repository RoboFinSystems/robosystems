"""
Tests for graph access authorization dependencies.

These tests cover the FastAPI dependency functions for validating user access
to graphs, including user graphs and shared repositories.
"""

from unittest.mock import Mock, patch

import pytest
from fastapi import HTTPException, status

from robosystems.middleware.graph.dependencies.auth import (
  get_graph_database,
  get_graph_repository_with_auth,
  get_universal_repository_with_auth,
)
from robosystems.middleware.graph.types import AccessPattern
from robosystems.models.iam.user_repository import RepositoryAccessLevel


class TestGetGraphDatabase:
  """Test get_graph_database dependency function."""

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
  @patch("robosystems.middleware.graph.dependencies.auth.MultiTenantUtils")
  async def test_resolve_user_graph_database(
    self, mock_utils, mock_user, mock_db_session
  ):
    """Test resolving database name for a user graph."""
    graph_id = "kg1234567890abcdef"
    database_name = "user_graph_db"

    mock_identity = Mock()
    mock_identity.is_shared_repository = False
    mock_identity.is_user_graph = True
    mock_identity.category.value = "user"

    mock_utils.get_graph_routing.return_value = {
      "graph_identity": mock_identity,
      "database_name": database_name,
    }
    mock_utils.validate_graph_access.return_value = True

    with patch(
      "robosystems.middleware.graph.dependencies.auth.GraphUser"
    ) as mock_graph_user:
      mock_graph_user.user_has_access.return_value = True

      result = await get_graph_database(
        graph_id=graph_id,
        current_user=mock_user,
        required_access=None,
        db=mock_db_session,
      )

      assert result == database_name
      mock_utils.get_graph_routing.assert_called_once_with(
        graph_id, session=mock_db_session
      )

  @pytest.mark.asyncio
  @patch("robosystems.middleware.graph.dependencies.auth.MultiTenantUtils")
  async def test_user_graph_access_denied(self, mock_utils, mock_user, mock_db_session):
    """Test access denied to user graph."""
    graph_id = "kg1234567890abcdef"

    mock_identity = Mock()
    mock_identity.is_shared_repository = False
    mock_identity.is_user_graph = True
    mock_identity.category.value = "user"

    mock_utils.get_graph_routing.return_value = {
      "graph_identity": mock_identity,
      "database_name": "test_db",
    }

    with (
      patch(
        "robosystems.middleware.graph.dependencies.auth.GraphUser"
      ) as mock_graph_user,
      patch("robosystems.middleware.graph.dependencies.auth.SecurityAuditLogger"),
    ):
      mock_graph_user.user_has_access.return_value = False

      with pytest.raises(HTTPException) as exc_info:
        await get_graph_database(
          graph_id=graph_id,
          current_user=mock_user,
          required_access=None,
          db=mock_db_session,
        )

      # Exception is caught and re-raised as 404
      assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND

  @pytest.mark.asyncio
  @patch("robosystems.middleware.graph.dependencies.auth.MultiTenantUtils")
  async def test_shared_repository_read_access(
    self, mock_utils, mock_user, mock_db_session
  ):
    """Test read access to shared repository."""
    repo_id = "sec"
    database_name = "sec_db"

    mock_identity = Mock()
    mock_identity.is_shared_repository = True
    mock_identity.is_user_graph = False
    mock_identity.category.value = "shared"

    mock_utils.get_graph_routing.return_value = {
      "graph_identity": mock_identity,
      "database_name": database_name,
    }
    mock_utils.validate_graph_access.return_value = True

    with patch(
      "robosystems.middleware.graph.dependencies.auth.UserRepository"
    ) as mock_user_repo:
      mock_user_repo.get_user_access_level.return_value = RepositoryAccessLevel.READ

      result = await get_graph_database(
        graph_id=repo_id,
        current_user=mock_user,
        required_access=AccessPattern.READ_ONLY,
        db=mock_db_session,
      )

      assert result == database_name

  @pytest.mark.asyncio
  @patch("robosystems.middleware.graph.dependencies.auth.MultiTenantUtils")
  async def test_shared_repository_no_access(
    self, mock_utils, mock_user, mock_db_session
  ):
    """Test access denied to shared repository without subscription."""
    repo_id = "sec"

    mock_identity = Mock()
    mock_identity.is_shared_repository = True
    mock_identity.is_user_graph = False
    mock_identity.category.value = "shared"

    mock_utils.get_graph_routing.return_value = {
      "graph_identity": mock_identity,
      "database_name": "sec_db",
    }

    with (
      patch(
        "robosystems.middleware.graph.dependencies.auth.UserRepository"
      ) as mock_user_repo,
      patch(
        "robosystems.middleware.graph.dependencies.auth.SecurityAuditLogger"
      ) as mock_audit,
    ):
      mock_user_repo.get_user_access_level.return_value = RepositoryAccessLevel.NONE

      with pytest.raises(HTTPException) as exc_info:
        await get_graph_database(
          graph_id=repo_id,
          current_user=mock_user,
          required_access=None,
          db=mock_db_session,
        )

      # Exception is caught and re-raised as 404
      assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
      mock_audit.log_authorization_denied.assert_called_once()

  @pytest.mark.asyncio
  @patch("robosystems.middleware.graph.dependencies.auth.MultiTenantUtils")
  async def test_shared_repository_insufficient_access_level(
    self, mock_utils, mock_user, mock_db_session
  ):
    """Test insufficient access level for shared repository write."""
    repo_id = "sec"

    mock_identity = Mock()
    mock_identity.is_shared_repository = True
    mock_identity.is_user_graph = False
    mock_identity.category.value = "shared"

    mock_utils.get_graph_routing.return_value = {
      "graph_identity": mock_identity,
      "database_name": "sec_db",
    }
    mock_utils.validate_graph_access.return_value = True

    with patch(
      "robosystems.middleware.graph.dependencies.auth.UserRepository"
    ) as mock_user_repo:
      # User has READ access but needs WRITE
      mock_user_repo.get_user_access_level.return_value = RepositoryAccessLevel.READ

      with pytest.raises(HTTPException) as exc_info:
        await get_graph_database(
          graph_id=repo_id,
          current_user=mock_user,
          required_access=AccessPattern.READ_WRITE,
          db=mock_db_session,
        )

      # Caught and re-raised as 404
      assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND

  @pytest.mark.asyncio
  @patch("robosystems.middleware.graph.dependencies.auth.MultiTenantUtils")
  async def test_access_pattern_validation_fails(
    self, mock_utils, mock_user, mock_db_session
  ):
    """Test access pattern validation failure."""
    graph_id = "kg1234567890abcdef"

    mock_identity = Mock()
    mock_identity.is_shared_repository = False
    mock_identity.is_user_graph = True
    mock_identity.category.value = "user"

    mock_utils.get_graph_routing.return_value = {
      "graph_identity": mock_identity,
      "database_name": "test_db",
    }
    mock_utils.validate_graph_access.return_value = False

    with pytest.raises(HTTPException) as exc_info:
      await get_graph_database(
        graph_id=graph_id,
        current_user=mock_user,
        required_access=AccessPattern.READ_WRITE,
        db=mock_db_session,
      )

    # Exception caught and re-raised as 404
    assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND

  @pytest.mark.asyncio
  @patch("robosystems.middleware.graph.dependencies.auth.MultiTenantUtils")
  async def test_user_graph_admin_access_required(
    self, mock_utils, mock_user, mock_db_session
  ):
    """Test admin access requirement for user graph write operations."""
    graph_id = "kg1234567890abcdef"

    mock_identity = Mock()
    mock_identity.is_shared_repository = False
    mock_identity.is_user_graph = True
    mock_identity.category.value = "user"

    mock_utils.get_graph_routing.return_value = {
      "graph_identity": mock_identity,
      "database_name": "test_db",
    }
    mock_utils.validate_graph_access.return_value = True

    with patch(
      "robosystems.middleware.graph.dependencies.auth.GraphUser"
    ) as mock_graph_user:
      mock_graph_user.user_has_access.return_value = True
      mock_graph_user.user_has_admin_access.return_value = False

      with pytest.raises(HTTPException) as exc_info:
        await get_graph_database(
          graph_id=graph_id,
          current_user=mock_user,
          required_access=AccessPattern.READ_WRITE,
          db=mock_db_session,
        )

      # Caught and re-raised as 404
      assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND

  @pytest.mark.asyncio
  @patch("robosystems.middleware.graph.dependencies.auth.MultiTenantUtils")
  async def test_graph_routing_error(self, mock_utils, mock_user, mock_db_session):
    """Test handling of routing errors."""
    graph_id = "invalid_graph"

    mock_utils.get_graph_routing.side_effect = ValueError("Graph not found")

    with pytest.raises(HTTPException) as exc_info:
      await get_graph_database(
        graph_id=graph_id,
        current_user=mock_user,
        required_access=None,
        db=mock_db_session,
      )

    assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
    assert "not found or access denied" in str(exc_info.value.detail)


class TestGetGraphRepositoryWithAuth:
  """Test get_graph_repository_with_auth dependency function."""

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
  @patch("robosystems.middleware.graph.dependencies.auth.get_graph_repository")
  @patch("robosystems.middleware.graph.dependencies.auth.MultiTenantUtils")
  @patch("robosystems.middleware.graph.dependencies.auth.SecurityAuditLogger")
  async def test_user_graph_write_access(
    self,
    mock_audit,
    mock_utils,
    mock_get_repo,
    mock_user,
    mock_db_session,
  ):
    """Test write access to user graph."""
    graph_id = "kg1234567890abcdef"
    mock_repository = Mock()

    mock_identity = Mock()
    mock_identity.is_shared_repository = False
    mock_identity.is_user_graph = True
    mock_identity.category.value = "user"

    mock_utils.get_graph_identity.return_value = mock_identity
    mock_get_repo.return_value = mock_repository

    with patch(
      "robosystems.middleware.graph.dependencies.auth.GraphUser"
    ) as mock_graph_user:
      mock_graph_user.user_has_access.return_value = True

      result = await get_graph_repository_with_auth(
        graph_id=graph_id,
        current_user=mock_user,
        operation_type="write",
        db=mock_db_session,
      )

      assert result == mock_repository
      mock_get_repo.assert_called_once_with(graph_id, operation_type="write")
      mock_audit.log_security_event.assert_called_once()

  @pytest.mark.asyncio
  @patch("robosystems.middleware.graph.dependencies.auth.MultiTenantUtils")
  async def test_shared_repository_access_denied(
    self, mock_utils, mock_user, mock_db_session
  ):
    """Test access denied to shared repository without subscription."""
    repo_id = "sec"

    mock_identity = Mock()
    mock_identity.is_shared_repository = True
    mock_identity.is_user_graph = False

    mock_utils.get_graph_identity.return_value = mock_identity

    with patch(
      "robosystems.middleware.graph.dependencies.auth.UserRepository"
    ) as mock_user_repo:
      mock_user_repo.user_has_access.return_value = False

      with pytest.raises(HTTPException) as exc_info:
        await get_graph_repository_with_auth(
          graph_id=repo_id,
          current_user=mock_user,
          operation_type="read",
          db=mock_db_session,
        )

      assert exc_info.value.status_code == status.HTTP_403_FORBIDDEN
      assert "Access denied to shared repository" in str(exc_info.value.detail)

  @pytest.mark.asyncio
  @patch("robosystems.middleware.graph.dependencies.auth.MultiTenantUtils")
  async def test_shared_repository_write_access_denied(
    self, mock_utils, mock_user, mock_db_session
  ):
    """Test write access denied to shared repository."""
    repo_id = "sec"

    mock_identity = Mock()
    mock_identity.is_shared_repository = True
    mock_identity.is_user_graph = False

    mock_utils.get_graph_identity.return_value = mock_identity

    with patch(
      "robosystems.middleware.graph.dependencies.auth.UserRepository"
    ) as mock_user_repo:
      mock_user_repo.user_has_access.return_value = True
      mock_user_repo.get_user_access_level.return_value = RepositoryAccessLevel.READ

      with pytest.raises(HTTPException) as exc_info:
        await get_graph_repository_with_auth(
          graph_id=repo_id,
          current_user=mock_user,
          operation_type="write",
          db=mock_db_session,
        )

      assert exc_info.value.status_code == status.HTTP_403_FORBIDDEN
      assert "Write access denied" in str(exc_info.value.detail)

  @pytest.mark.asyncio
  @patch("robosystems.middleware.graph.dependencies.auth.MultiTenantUtils")
  async def test_user_graph_access_denied(self, mock_utils, mock_user, mock_db_session):
    """Test access denied to user graph."""
    graph_id = "kg1234567890abcdef"

    mock_identity = Mock()
    mock_identity.is_shared_repository = False
    mock_identity.is_user_graph = True

    mock_utils.get_graph_identity.return_value = mock_identity

    with patch(
      "robosystems.middleware.graph.dependencies.auth.GraphUser"
    ) as mock_graph_user:
      mock_graph_user.user_has_access.return_value = False

      with pytest.raises(HTTPException) as exc_info:
        await get_graph_repository_with_auth(
          graph_id=graph_id,
          current_user=mock_user,
          operation_type="read",
          db=mock_db_session,
        )

      assert exc_info.value.status_code == status.HTTP_403_FORBIDDEN
      assert "Access denied to user graph" in str(exc_info.value.detail)

  @pytest.mark.asyncio
  @patch("robosystems.middleware.graph.dependencies.auth.get_graph_repository")
  @patch("robosystems.middleware.graph.dependencies.auth.MultiTenantUtils")
  @patch("robosystems.middleware.graph.dependencies.auth.SecurityAuditLogger")
  async def test_repository_creation_error(
    self,
    mock_audit,
    mock_utils,
    mock_get_repo,
    mock_user,
    mock_db_session,
  ):
    """Test handling of repository creation errors."""
    graph_id = "kg1234567890abcdef"

    mock_identity = Mock()
    mock_identity.is_shared_repository = False
    mock_identity.is_user_graph = True
    mock_identity.category.value = "user"

    mock_utils.get_graph_identity.return_value = mock_identity
    mock_get_repo.side_effect = RuntimeError("Connection failed")

    with patch(
      "robosystems.middleware.graph.dependencies.auth.GraphUser"
    ) as mock_graph_user:
      mock_graph_user.user_has_access.return_value = True

      with pytest.raises(HTTPException) as exc_info:
        await get_graph_repository_with_auth(
          graph_id=graph_id,
          current_user=mock_user,
          operation_type="write",
          db=mock_db_session,
        )

      assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
      mock_audit.log_authorization_denied.assert_called_once()


class TestGetUniversalRepositoryWithAuth:
  """Test get_universal_repository_with_auth dependency function."""

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
    "robosystems.middleware.graph.dependencies.auth.get_graph_repository_with_auth"
  )
  @patch("robosystems.middleware.graph.dependencies.auth.SecurityAuditLogger")
  async def test_universal_repository_success(
    self,
    mock_audit,
    mock_get_repo_with_auth,
    mock_user,
    mock_db_session,
  ):
    """Test successful universal repository creation."""
    graph_id = "kg1234567890abcdef"
    mock_repository = Mock()

    mock_get_repo_with_auth.return_value = mock_repository

    result = await get_universal_repository_with_auth(
      graph_id=graph_id,
      current_user=mock_user,
      operation_type="write",
      db=mock_db_session,
    )

    # Result should be a UniversalRepository wrapping the repository
    assert result is not None
    mock_get_repo_with_auth.assert_called_once_with(
      graph_id=graph_id,
      current_user=mock_user,
      operation_type="write",
      db=mock_db_session,
    )
    mock_audit.log_security_event.assert_called_once()

  @pytest.mark.asyncio
  @patch(
    "robosystems.middleware.graph.dependencies.auth.get_graph_repository_with_auth"
  )
  async def test_universal_repository_auth_failure_propagates(
    self,
    mock_get_repo_with_auth,
    mock_user,
    mock_db_session,
  ):
    """Test that auth failures from underlying call propagate."""
    graph_id = "kg1234567890abcdef"

    mock_get_repo_with_auth.side_effect = HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Access denied",
    )

    with pytest.raises(HTTPException) as exc_info:
      await get_universal_repository_with_auth(
        graph_id=graph_id,
        current_user=mock_user,
        operation_type="read",
        db=mock_db_session,
      )

    assert exc_info.value.status_code == status.HTTP_403_FORBIDDEN

  @pytest.mark.asyncio
  @patch(
    "robosystems.middleware.graph.dependencies.auth.get_graph_repository_with_auth"
  )
  @patch("robosystems.middleware.graph.dependencies.auth.SecurityAuditLogger")
  async def test_universal_repository_unexpected_error(
    self,
    mock_audit,
    mock_get_repo_with_auth,
    mock_user,
    mock_db_session,
  ):
    """Test handling of unexpected errors."""
    graph_id = "kg1234567890abcdef"

    mock_get_repo_with_auth.side_effect = RuntimeError("Unexpected error")

    with pytest.raises(HTTPException) as exc_info:
      await get_universal_repository_with_auth(
        graph_id=graph_id,
        current_user=mock_user,
        operation_type="write",
        db=mock_db_session,
      )

    assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    mock_audit.log_authorization_denied.assert_called_once()


class TestAccessLevelHierarchy:
  """Test repository access level hierarchy enforcement."""

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
  @patch("robosystems.middleware.graph.dependencies.auth.MultiTenantUtils")
  async def test_admin_can_access_write_required(
    self, mock_utils, mock_user, mock_db_session
  ):
    """Test admin can access when write is required."""
    repo_id = "sec"

    mock_identity = Mock()
    mock_identity.is_shared_repository = True
    mock_identity.is_user_graph = False

    mock_utils.get_graph_routing.return_value = {
      "graph_identity": mock_identity,
      "database_name": "sec_db",
    }
    mock_utils.validate_graph_access.return_value = True

    with patch(
      "robosystems.middleware.graph.dependencies.auth.UserRepository"
    ) as mock_user_repo:
      mock_user_repo.get_user_access_level.return_value = RepositoryAccessLevel.ADMIN

      result = await get_graph_database(
        graph_id=repo_id,
        current_user=mock_user,
        required_access=AccessPattern.READ_WRITE,
        db=mock_db_session,
      )

      assert result == "sec_db"

  @pytest.mark.asyncio
  @patch("robosystems.middleware.graph.dependencies.auth.MultiTenantUtils")
  async def test_write_can_access_read_required(
    self, mock_utils, mock_user, mock_db_session
  ):
    """Test write access can access when read is required."""
    repo_id = "sec"

    mock_identity = Mock()
    mock_identity.is_shared_repository = True
    mock_identity.is_user_graph = False

    mock_utils.get_graph_routing.return_value = {
      "graph_identity": mock_identity,
      "database_name": "sec_db",
    }
    mock_utils.validate_graph_access.return_value = True

    with patch(
      "robosystems.middleware.graph.dependencies.auth.UserRepository"
    ) as mock_user_repo:
      mock_user_repo.get_user_access_level.return_value = RepositoryAccessLevel.WRITE

      result = await get_graph_database(
        graph_id=repo_id,
        current_user=mock_user,
        required_access=AccessPattern.READ_ONLY,
        db=mock_db_session,
      )

      assert result == "sec_db"
