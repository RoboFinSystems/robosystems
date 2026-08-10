"""Tests for authentication utility functions."""

import hashlib
from unittest.mock import Mock, patch

from robosystems.middleware.auth.utils import (
  validate_api_key,
  validate_api_key_with_graph,
  validate_repository_access,
)
from robosystems.models.core import User, UserAPIKey

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
        "email_verified": True,
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
    mock_key_record.graph_id = None
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

  @patch("robosystems.middleware.auth.utils.api_key_cache")
  @patch("robosystems.middleware.auth.utils.UserAPIKey")
  def test_validate_api_key_cache_hit_inactive_user_rejected(
    self, mock_api_key_class, mock_cache
  ):
    """Cache hit for an active key whose owning user is deactivated is rejected."""
    api_key = _VALID_TEST_KEY
    mock_cache.get_cached_api_key_validation.return_value = {
      "is_active": True,  # the key itself is active
      "user_data": {
        "id": "user123",
        "name": "Test User",
        "email": "test@example.com",
        "email_verified": True,
        "is_active": False,  # but the owning user is deactivated
      },
    }

    result = validate_api_key(api_key)

    assert result is None
    # Must not fall through to the database either
    mock_api_key_class.get_by_key.assert_not_called()

  @patch("robosystems.database.SessionFactory")
  @patch("robosystems.middleware.auth.utils.api_key_cache")
  @patch("robosystems.middleware.auth.utils.UserAPIKey")
  @patch("robosystems.middleware.auth.utils.SecurityAuditLogger")
  def test_validate_api_key_db_inactive_user_rejected(
    self, mock_audit, mock_api_key_class, mock_cache, mock_session_factory
  ):
    """DB path rejects an active key whose owning user is deactivated."""
    api_key = _VALID_TEST_KEY
    mock_cache.get_cached_api_key_validation.return_value = None

    mock_sess = Mock()
    mock_session_factory.return_value = mock_sess

    mock_user = Mock(spec=User)
    mock_user.id = "user123"
    mock_user.name = "Test User"
    mock_user.email = "test@example.com"
    mock_user.is_active = False  # deactivated user

    mock_key_record = Mock(spec=UserAPIKey)
    mock_key_record.graph_id = None
    mock_key_record.user = mock_user
    mock_key_record.is_active = True  # key row still active
    mock_api_key_class.get_by_key.return_value = mock_key_record

    result = validate_api_key(api_key)

    assert result is None
    mock_audit.log_auth_success.assert_not_called()
    mock_sess.close.assert_called_once()


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
    mock_key_record.graph_id = None
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
  @patch("robosystems.middleware.auth.utils.GraphUser")
  @patch(
    "robosystems.config.shared_repositories.is_shared_repository_or_subgraph",
    return_value=False,
  )
  @patch("robosystems.middleware.auth.utils.SecurityAuditLogger")
  def test_validate_api_key_with_graph_db_inactive_user_rejected(
    self,
    mock_audit,
    mock_is_shared,
    mock_user_graph,
    mock_api_key_class,
    mock_cache,
    mock_session_factory,
  ):
    """A deactivated user's key is rejected before the graph-access check runs."""
    api_key = _VALID_TEST_KEY
    graph_id = "kg1234567890"

    mock_sess = Mock()
    mock_session_factory.return_value = mock_sess

    mock_cache.get_cached_api_key_validation.return_value = None
    mock_cache.get_cached_graph_access.return_value = None

    mock_user = Mock(spec=User)
    mock_user.id = "user123"
    mock_user.name = "Test User"
    mock_user.email = "test@example.com"
    mock_user.is_active = False  # deactivated

    mock_key_record = Mock(spec=UserAPIKey)
    mock_key_record.graph_id = None
    mock_key_record.user = mock_user
    mock_key_record.user_id = "user123"
    mock_key_record.is_active = True  # key row still active
    mock_api_key_class.get_by_key.return_value = mock_key_record

    result = validate_api_key_with_graph(api_key, graph_id)

    assert result is None
    # Rejected before the graph-access check ever runs
    mock_user_graph.user_has_access.assert_not_called()
    mock_audit.log_auth_success.assert_not_called()

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
    mock_key_record.graph_id = None
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


class TestGraphScopedKeys:
  """Graph-scoped API keys: format, scope matching, and carriage rules."""

  def test_api_key_format_accepts_both_prefixes(self):
    from robosystems.middleware.auth.utils import _is_valid_api_key_format

    assert _is_valid_api_key_format("rfs" + "a" * 64) is True
    assert _is_valid_api_key_format("rfsc" + "a" * 64) is True
    assert _is_valid_api_key_format("rfscc" + "a" * 64) is False
    # 67 chars starting "rfsc" is just an rfs-key whose first hex char is c —
    # the two prefixes are disambiguated by total length
    assert _is_valid_api_key_format("rfsc" + "a" * 63) is True
    assert _is_valid_api_key_format("rfsc" + "a" * 62) is False
    assert _is_valid_api_key_format("rfsC" + "a" * 64) is False

  def test_key_scope_allows(self):
    from robosystems.middleware.auth.utils import _key_scope_allows

    # Account-wide keys (no scope) are allowed everywhere
    assert _key_scope_allows(None, "kg123") is True
    # Exact graph match
    assert _key_scope_allows("kg123", "kg123") is True
    # Subgraphs of the scoped graph are covered
    assert _key_scope_allows("kg123", "kg123_dev") is True
    # A different graph is not, even when the scope is its prefix
    assert _key_scope_allows("kg12", "kg123") is False
    # Nor the parent when scoped to a subgraph
    assert _key_scope_allows("kg123_dev", "kg123") is False
    assert _key_scope_allows("kg123", "kg999") is False

  @patch("robosystems.middleware.auth.utils.api_key_cache")
  @patch("robosystems.middleware.auth.utils.UserAPIKey")
  def test_scoped_key_rejected_without_graph_context_cache_hit(
    self, mock_api_key_class, mock_cache
  ):
    """A graph-scoped key must not authenticate account-level surfaces (cache path)."""
    mock_cache.get_cached_api_key_validation.return_value = {
      "is_active": True,
      "user_data": {
        "id": "user123",
        "name": "Test User",
        "email": "test@example.com",
        "email_verified": True,
        "is_active": True,
        "key_graph_id": "kg123",
      },
    }

    assert validate_api_key("rfsc" + "a" * 64) is None
    mock_api_key_class.get_by_key.assert_not_called()

  @patch("robosystems.database.SessionFactory")
  @patch("robosystems.middleware.auth.utils.api_key_cache")
  @patch("robosystems.middleware.auth.utils.UserAPIKey")
  def test_scoped_key_rejected_without_graph_context_db(
    self, mock_api_key_class, mock_cache, mock_session_factory
  ):
    """A graph-scoped key must not authenticate account-level surfaces (DB path)."""
    mock_cache.get_cached_api_key_validation.return_value = None
    mock_session_factory.return_value = Mock()

    mock_user = Mock(spec=User)
    mock_user.id = "user123"
    mock_user.name = "Test User"
    mock_user.email = "test@example.com"
    mock_user.is_active = True

    mock_key_record = Mock(spec=UserAPIKey)
    mock_key_record.graph_id = "kg123"
    mock_key_record.user = mock_user
    mock_key_record.is_active = True
    mock_api_key_class.get_by_key.return_value = mock_key_record

    assert validate_api_key("rfsc" + "a" * 64) is None
    # The positive validation (with scope) is still cached for the graph path
    mock_cache.cache_api_key_validation.assert_called_once()
    cached_user_data = mock_cache.cache_api_key_validation.call_args[0][1]
    assert cached_user_data["key_graph_id"] == "kg123"

  def _db_setup(self, mock_api_key_class, mock_session_factory, key_graph_id):
    mock_session_factory.return_value = Mock()
    mock_user = Mock(spec=User)
    mock_user.id = "user123"
    mock_user.name = "Test User"
    mock_user.email = "test@example.com"
    mock_user.is_active = True
    mock_key_record = Mock(spec=UserAPIKey)
    mock_key_record.graph_id = key_graph_id
    mock_key_record.user = mock_user
    mock_key_record.user_id = "user123"
    mock_key_record.is_active = True
    mock_api_key_class.get_by_key.return_value = mock_key_record
    return mock_key_record

  @patch("robosystems.models.core.GraphUser")
  @patch("robosystems.database.SessionFactory")
  @patch("robosystems.middleware.auth.utils.api_key_cache")
  @patch("robosystems.middleware.auth.utils.UserAPIKey")
  def test_scoped_key_valid_on_its_graph_db(
    self, mock_api_key_class, mock_cache, mock_session_factory, mock_graph_user
  ):
    """A scoped key authenticates its own graph through the DB path."""
    mock_cache.get_cached_api_key_validation.return_value = None
    mock_cache.get_cached_graph_access.return_value = None
    self._db_setup(mock_api_key_class, mock_session_factory, "kg123")

    with patch("robosystems.models.core.GraphUser.user_has_access", return_value=True):
      result = validate_api_key_with_graph(
        "rfsc" + "a" * 64, "kg123", require_scoped=True
      )

    assert result is not None
    assert result.id == "user123"

  @patch("robosystems.database.SessionFactory")
  @patch("robosystems.middleware.auth.utils.api_key_cache")
  @patch("robosystems.middleware.auth.utils.UserAPIKey")
  def test_scoped_key_rejected_on_other_graph_db(
    self, mock_api_key_class, mock_cache, mock_session_factory
  ):
    """A scoped key is rejected for a different graph, and the mismatch is cached."""
    mock_cache.get_cached_api_key_validation.return_value = None
    mock_cache.get_cached_graph_access.return_value = None
    self._db_setup(mock_api_key_class, mock_session_factory, "kg123")

    assert validate_api_key_with_graph("rfsc" + "a" * 64, "kg999") is None
    mock_cache.cache_graph_access.assert_called_once()
    assert mock_cache.cache_graph_access.call_args[1]["has_access"] is False

  @patch("robosystems.database.SessionFactory")
  @patch("robosystems.middleware.auth.utils.api_key_cache")
  @patch("robosystems.middleware.auth.utils.UserAPIKey")
  def test_require_scoped_rejects_account_wide_key_without_cache_poison(
    self, mock_api_key_class, mock_cache, mock_session_factory
  ):
    """require_scoped rejects account-wide keys but must not cache a negative
    access decision — the same key stays valid for this graph via headers."""
    mock_cache.get_cached_api_key_validation.return_value = None
    mock_cache.get_cached_graph_access.return_value = None
    self._db_setup(mock_api_key_class, mock_session_factory, None)

    result = validate_api_key_with_graph("rfs" + "a" * 64, "kg123", require_scoped=True)

    assert result is None
    mock_cache.cache_graph_access.assert_not_called()

  @patch("robosystems.middleware.auth.utils.api_key_cache")
  @patch("robosystems.middleware.auth.utils.UserAPIKey")
  def test_require_scoped_rejects_account_wide_key_cache_hit(
    self, mock_api_key_class, mock_cache
  ):
    """require_scoped rejection also holds on the cache-hit path."""
    mock_cache.get_cached_api_key_validation.return_value = {
      "is_active": True,
      "user_data": {
        "id": "user123",
        "name": "Test User",
        "email": "test@example.com",
        "email_verified": True,
        "is_active": True,
        "key_graph_id": None,
      },
    }
    mock_cache.get_cached_graph_access.return_value = True

    result = validate_api_key_with_graph("rfs" + "a" * 64, "kg123", require_scoped=True)

    assert result is None
    mock_api_key_class.get_by_key.assert_not_called()

  @patch("robosystems.middleware.auth.utils.api_key_cache")
  @patch("robosystems.middleware.auth.utils.UserAPIKey")
  def test_scoped_key_subgraph_allowed_cache_hit(self, mock_api_key_class, mock_cache):
    """A scoped key covers subgraphs of its graph on the cache-hit path."""
    mock_cache.get_cached_api_key_validation.return_value = {
      "is_active": True,
      "user_data": {
        "id": "user123",
        "name": "Test User",
        "email": "test@example.com",
        "email_verified": True,
        "is_active": True,
        "key_graph_id": "kg123",
      },
    }
    mock_cache.get_cached_graph_access.return_value = True

    result = validate_api_key_with_graph(
      "rfsc" + "a" * 64, "kg123_dev", require_scoped=True
    )

    assert result is not None
    assert result.id == "user123"

  @patch("robosystems.middleware.auth.utils.api_key_cache")
  @patch("robosystems.middleware.auth.utils.UserAPIKey")
  def test_scoped_key_rejected_on_other_graph_cache_hit(
    self, mock_api_key_class, mock_cache
  ):
    """Scope mismatch rejects even when user-level graph access is cached True."""
    mock_cache.get_cached_api_key_validation.return_value = {
      "is_active": True,
      "user_data": {
        "id": "user123",
        "name": "Test User",
        "email": "test@example.com",
        "email_verified": True,
        "is_active": True,
        "key_graph_id": "kg123",
      },
    }
    mock_cache.get_cached_graph_access.return_value = True

    assert validate_api_key_with_graph("rfsc" + "a" * 64, "kg999") is None
    mock_api_key_class.get_by_key.assert_not_called()
