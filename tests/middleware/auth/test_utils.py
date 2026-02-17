"""Tests for authentication utility functions."""

import hashlib
from unittest.mock import Mock, patch

from robosystems.middleware.auth.utils import (
  validate_api_key,
  validate_api_key_with_graph,
  validate_repository_access,
)
from robosystems.models.iam import User, UserAPIKey

# Valid format key for tests: "rfs" + 64 hex chars = 67 chars total
_VALID_TEST_KEY = "rfs" + "a" * 64


class TestValidateAPIKey:
  """Test API key validation."""

  @patch("robosystems.middleware.auth.utils.api_key_cache")
  @patch("robosystems.middleware.auth.utils.UserAPIKey")
  def test_validate_api_key_with_cache_hit(self, mock_api_key_class, mock_cache):
    """Test API key validation with cache hit."""
    # Setup cache hit
    api_key = _VALID_TEST_KEY
    hashlib.sha256(api_key.encode()).hexdigest()

    mock_cache.get_cached_api_key_validation.return_value = {
      "is_active": True,
      "user_data": {
        "id": "user123",
        "name": "Test User",
        "email": "test@example.com",
        "is_active": True,
      },
    }

    # Call function
    result = validate_api_key(api_key)

    # Assertions
    assert result is not None
    assert result.id == "user123"
    assert result.name == "Test User"
    assert result.email == "test@example.com"
    mock_cache.get_cached_api_key_validation.assert_called_once()
    mock_api_key_class.get_by_key.assert_not_called()

  @patch("robosystems.database.SessionFactory")
  @patch("robosystems.middleware.auth.utils.api_key_cache")
  @patch("robosystems.middleware.auth.utils.UserAPIKey")
  @patch("robosystems.middleware.auth.utils.SecurityAuditLogger")
  def test_validate_api_key_with_cache_miss(
    self, mock_audit, mock_api_key_class, mock_cache, mock_session_factory
  ):
    """Test API key validation with cache miss."""
    # Setup cache miss
    api_key = _VALID_TEST_KEY
    mock_cache.get_cached_api_key_validation.return_value = None

    # SessionFactory() returns a mock session
    mock_sess = Mock()
    mock_session_factory.return_value = mock_sess

    # Setup database hit
    mock_user = Mock(spec=User)
    mock_user.id = "user123"
    mock_user.name = "Test User"
    mock_user.email = "test@example.com"
    mock_user.is_active = True

    mock_key_record = Mock(spec=UserAPIKey)
    mock_key_record.user = mock_user
    mock_key_record.is_active = True

    mock_api_key_class.get_by_key.return_value = mock_key_record

    # Call function
    result = validate_api_key(api_key)

    # Assertions
    assert result is not None
    assert result.id == "user123"
    mock_cache.get_cached_api_key_validation.assert_called_once()
    mock_api_key_class.get_by_key.assert_called_once_with(api_key, mock_sess)
    mock_sess.close.assert_called_once()
    mock_audit.log_auth_success.assert_called_once()

  @patch("robosystems.middleware.auth.utils.api_key_cache")
  @patch("robosystems.middleware.auth.utils.UserAPIKey")
  def test_validate_api_key_invalid(self, mock_api_key_class, mock_cache):
    """Test validation with invalid API key (bad format never hits cache/DB)."""
    api_key = "invalid_key"

    result = validate_api_key(api_key)

    assert result is None
    # Format validation rejects before cache or DB
    mock_cache.get_cached_api_key_validation.assert_not_called()
    mock_api_key_class.get_by_key.assert_not_called()

  def test_validate_api_key_empty(self):
    """Test validation with empty API key."""
    assert validate_api_key("") is None
    assert validate_api_key(None) is None


class TestValidateAPIKeyWithGraph:
  """Test API key validation with graph access."""

  @patch("robosystems.database.SessionFactory")
  @patch("robosystems.middleware.auth.utils.api_key_cache")
  @patch("robosystems.middleware.auth.utils.UserAPIKey")
  @patch("robosystems.middleware.auth.utils.GraphUser")
  @patch(
    "robosystems.config.shared_repositories.is_shared_repository_or_subgraph",
    return_value=False,
  )
  @patch("robosystems.middleware.auth.utils.SecurityAuditLogger")
  def test_validate_api_key_with_graph_standard_db(
    self,
    mock_audit,
    mock_is_shared,
    mock_user_graph,
    mock_api_key_class,
    mock_cache,
    mock_session_factory,
  ):
    """Test API key validation with standard database access."""
    api_key = _VALID_TEST_KEY
    graph_id = "kg1234567890"

    mock_sess = Mock()
    mock_session_factory.return_value = mock_sess

    # Setup cache miss
    mock_cache.get_cached_api_key_validation.return_value = None
    mock_cache.get_cached_graph_access.return_value = None

    # Setup user and API key
    mock_user = Mock(spec=User)
    mock_user.id = "user123"
    mock_user.name = "Test User"
    mock_user.email = "test@example.com"
    mock_user.is_active = True

    mock_key_record = Mock(spec=UserAPIKey)
    mock_key_record.user = mock_user
    mock_key_record.user_id = "user123"
    mock_key_record.is_active = True
    mock_key_record.update_last_used = Mock()

    mock_api_key_class.get_by_key.return_value = mock_key_record
    mock_user_graph.user_has_access.return_value = True

    # Call function
    result = validate_api_key_with_graph(api_key, graph_id)

    # Assertions — function returns a detached User, not the mock
    assert result is not None
    assert result.id == "user123"
    mock_is_shared.assert_called_once_with(graph_id)
    mock_user_graph.user_has_access.assert_called_once()
    mock_key_record.update_last_used.assert_called_once()
    mock_sess.close.assert_called_once()

  @patch("robosystems.database.SessionFactory")
  @patch("robosystems.middleware.auth.utils.api_key_cache")
  @patch("robosystems.middleware.auth.utils.UserAPIKey")
  @patch(
    "robosystems.middleware.graph.utils.MultiTenantUtils.validate_repository_access"
  )
  @patch(
    "robosystems.config.shared_repositories.is_shared_repository_or_subgraph",
    return_value=True,
  )
  @patch("robosystems.middleware.auth.utils.SecurityAuditLogger")
  def test_validate_api_key_with_graph_shared_repository(
    self,
    mock_audit,
    mock_is_shared,
    mock_validate_repo,
    mock_api_key_class,
    mock_cache,
    mock_session_factory,
  ):
    """Test API key validation with shared repository access."""
    api_key = _VALID_TEST_KEY
    graph_id = "sec"

    mock_sess = Mock()
    mock_session_factory.return_value = mock_sess

    # Setup cache miss
    mock_cache.get_cached_api_key_validation.return_value = None
    mock_cache.get_cached_graph_access.return_value = None

    # Setup repository access validation
    mock_validate_repo.return_value = True

    # Setup user and API key
    mock_user = Mock(spec=User)
    mock_user.id = "user123"
    mock_user.name = "Test User"
    mock_user.email = "test@example.com"
    mock_user.is_active = True

    mock_key_record = Mock(spec=UserAPIKey)
    mock_key_record.user = mock_user
    mock_key_record.user_id = "user123"
    mock_key_record.is_active = True
    mock_key_record.update_last_used = Mock()

    mock_api_key_class.get_by_key.return_value = mock_key_record

    # Call function
    result = validate_api_key_with_graph(api_key, graph_id)

    # Assertions — function returns a detached User, not the mock
    assert result is not None
    assert result.id == "user123"
    mock_is_shared.assert_called_once_with(graph_id)
    mock_validate_repo.assert_called_once_with(graph_id, "user123", "read")
    mock_sess.close.assert_called_once()

  def test_validate_api_key_with_graph_empty_params(self):
    """Test validation with empty parameters."""
    assert validate_api_key_with_graph("", "graph_id") is None
    assert validate_api_key_with_graph("api_key", "") is None
    assert validate_api_key_with_graph(None, "graph_id") is None
    assert validate_api_key_with_graph("api_key", None) is None


class TestValidateRepositoryAccess:
  """Test repository access validation."""

  @patch("robosystems.middleware.graph.utils.MultiTenantUtils")
  def test_validate_repository_access_valid_user(self, mock_utils):
    """Test repository access with valid active user."""
    mock_user = Mock(spec=User)
    mock_user.id = "user123"
    mock_user.is_active = True

    mock_utils.validate_repository_access.return_value = True

    result = validate_repository_access(mock_user, "sec", "read")

    assert result is True
    mock_utils.validate_repository_access.assert_called_once_with(
      "sec", "user123", "read"
    )

  @patch("robosystems.middleware.graph.utils.MultiTenantUtils")
  def test_validate_repository_access_inactive_user(self, mock_utils):
    """Test repository access with inactive user."""
    mock_user = Mock(spec=User)
    mock_user.id = "user123"
    mock_user.is_active = False

    result = validate_repository_access(mock_user, "sec", "read")

    assert result is False
    mock_utils.validate_repository_access.assert_not_called()

  @patch("robosystems.middleware.graph.utils.MultiTenantUtils")
  def test_validate_repository_access_no_user(self, mock_utils):
    """Test repository access with no user."""
    result = validate_repository_access(None, "sec", "read")

    assert result is False
    mock_utils.validate_repository_access.assert_not_called()

  @patch("robosystems.middleware.graph.utils.MultiTenantUtils")
  def test_validate_repository_access_different_operations(self, mock_utils):
    """Test repository access with different operation types."""
    mock_user = Mock(spec=User)
    mock_user.id = "user123"
    mock_user.is_active = True

    mock_utils.validate_repository_access.return_value = True

    # Test read access
    assert validate_repository_access(mock_user, "sec", "read") is True

    # Test write access
    assert validate_repository_access(mock_user, "sec", "write") is True

    # Test admin access
    assert validate_repository_access(mock_user, "sec", "admin") is True

    assert mock_utils.validate_repository_access.call_count == 3
