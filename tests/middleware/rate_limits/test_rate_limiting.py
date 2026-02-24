"""Unit tests for rate limiting module."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.middleware.rate_limits.rate_limiting import (
  create_custom_rate_limit_dependency,
  get_int_env,
  get_rate_limit_config,
  get_user_from_request,
  get_user_identifier,
  rate_limit_dependency,
)

MODULE = "robosystems.middleware.rate_limits.rate_limiting"


def _make_request(headers=None, cookies=None, host="127.0.0.1", path="/test"):
  """Create a mock FastAPI Request with real dicts for headers/cookies."""
  mock_request = MagicMock()
  mock_request.headers = headers or {}
  mock_request.cookies = cookies or {}
  mock_request.client = MagicMock()
  mock_request.client.host = host
  mock_request.url = MagicMock()
  mock_request.url.path = path
  mock_request.state = MagicMock()
  mock_request.state.current_time = None
  return mock_request


@pytest.mark.unit
class TestGetIntEnv:
  """Tests for get_int_env helper."""

  def test_returns_int_from_env(self):
    from robosystems.config import env

    try:
      with patch.object(env, "RATE_LIMIT_API_KEY", "500"):
        result = get_int_env("RATE_LIMIT_API_KEY", "1000")
    except (AttributeError, TypeError):
      with patch.object(env, "RATE_LIMIT_API_KEY", "500", create=True):
        result = get_int_env("RATE_LIMIT_API_KEY", "1000")
    assert result == 500

  def test_strips_inline_comments(self):
    from robosystems.config import env

    try:
      with patch.object(env, "RATE_LIMIT_API_KEY", "500 # some comment"):
        result = get_int_env("RATE_LIMIT_API_KEY", "1000")
    except (AttributeError, TypeError):
      with patch.object(env, "RATE_LIMIT_API_KEY", "500 # some comment", create=True):
        result = get_int_env("RATE_LIMIT_API_KEY", "1000")
    assert result == 500

  def test_uses_default_when_missing(self):
    result = get_int_env("NONEXISTENT_RATE_LIMIT_KEY_XYZ", "100")
    assert result == 100


@pytest.mark.unit
class TestGetUserIdentifier:
  """Tests for get_user_identifier."""

  def test_api_key_returns_hashed(self):
    request = _make_request(headers={"X-API-Key": "test-key"})
    result = get_user_identifier(request)
    assert result.startswith("apikey:")

  def test_jwt_bearer_returns_user_id(self):
    request = _make_request(headers={"Authorization": "Bearer valid_token"})
    with patch(f"{MODULE}._verify_jwt_for_rate_limiting", return_value="usr_123"):
      result = get_user_identifier(request)
    assert result == "jwt:usr_123"

  def test_cookie_auth_returns_user_id(self):
    request = _make_request(cookies={"auth-token": "valid_token"})
    with patch(f"{MODULE}._verify_jwt_for_rate_limiting", return_value="usr_456"):
      result = get_user_identifier(request)
    assert result == "jwt:usr_456"

  def test_fallback_to_ip(self):
    request = _make_request(host="1.2.3.4")
    result = get_user_identifier(request)
    assert result == "ip:1.2.3.4"


@pytest.mark.unit
class TestGetRateLimitConfig:
  """Tests for get_rate_limit_config."""

  def test_api_key_limits(self):
    with patch(f"{MODULE}.get_int_env", return_value=1000):
      limit, window = get_rate_limit_config("apikey:abc123")
    assert limit == 1000
    assert window == 60

  def test_jwt_limits(self):
    with patch(f"{MODULE}.get_int_env", return_value=500):
      limit, window = get_rate_limit_config("jwt:usr_123")
    assert limit == 500
    assert window == 60

  def test_anonymous_limits(self):
    with patch(f"{MODULE}.get_int_env", return_value=10):
      limit, window = get_rate_limit_config("ip:1.2.3.4")
    assert limit == 10
    assert window == 60


@pytest.mark.unit
class TestGetUserFromRequest:
  """Tests for get_user_from_request."""

  def test_jwt_from_bearer(self):
    request = _make_request(headers={"Authorization": "Bearer valid_token"})
    with patch(f"{MODULE}._verify_jwt_for_rate_limiting", return_value="usr_123"):
      result = get_user_from_request(request)
    assert result == "usr_123"

  def test_jwt_from_cookie(self):
    request = _make_request(cookies={"auth-token": "valid_token"})
    with patch(f"{MODULE}._verify_jwt_for_rate_limiting", return_value="usr_456"):
      result = get_user_from_request(request)
    assert result == "usr_456"

  def test_api_key_returns_hashed(self):
    request = _make_request(headers={"X-API-Key": "test-key"})
    result = get_user_from_request(request)
    assert result is not None
    assert result.startswith("apikey_")


@pytest.mark.unit
class TestRateLimitDependency:
  """Tests for rate_limit_dependency."""

  def test_allowed_sets_state(self):
    request = _make_request(headers={"X-API-Key": "test-key"})
    with (
      patch(f"{MODULE}.rate_limit_cache") as mock_cache,
      patch(f"{MODULE}.get_int_env", return_value=1000),
    ):
      mock_cache.check_rate_limit.return_value = (True, 99)
      rate_limit_dependency(request)
    assert request.state.rate_limit_remaining == 99

  def test_exceeded_raises_429(self):
    request = _make_request(headers={"X-API-Key": "test-key"})
    with (
      patch(f"{MODULE}.rate_limit_cache") as mock_cache,
      patch(f"{MODULE}.get_int_env", return_value=1000),
      patch(f"{MODULE}.SecurityAuditLogger"),
    ):
      mock_cache.check_rate_limit.return_value = (False, 0)
      with pytest.raises(HTTPException) as exc_info:
        rate_limit_dependency(request)
      assert exc_info.value.status_code == 429


@pytest.mark.unit
class TestCreateCustomRateLimitDependency:
  """Tests for create_custom_rate_limit_dependency."""

  def test_authenticated_gets_full_limit(self):
    dep = create_custom_rate_limit_dependency(
      limit_per_hour=100, window_seconds=3600, limit_name="custom"
    )
    request = _make_request(headers={"Authorization": "Bearer valid_token"})
    with (
      patch(f"{MODULE}._verify_jwt_for_rate_limiting", return_value="usr_123"),
      patch(f"{MODULE}.rate_limit_cache") as mock_cache,
    ):
      mock_cache.check_rate_limit.return_value = (True, 99)
      dep(request)
      call_args = mock_cache.check_rate_limit.call_args
      # Second positional arg is the limit
      assert call_args[0][1] == 100

  def test_anonymous_gets_reduced_limit(self):
    dep = create_custom_rate_limit_dependency(
      limit_per_hour=100, window_seconds=3600, limit_name="custom"
    )
    request = _make_request(host="10.0.0.1")
    with patch(f"{MODULE}.rate_limit_cache") as mock_cache:
      mock_cache.check_rate_limit.return_value = (True, 9)
      dep(request)
      call_args = mock_cache.check_rate_limit.call_args
      # Anonymous gets 1/10th: 100 // 10 = 10
      assert call_args[0][1] == 10

  def test_exceeded_raises_429(self):
    dep = create_custom_rate_limit_dependency(
      limit_per_hour=100, window_seconds=60, limit_name="test_limit"
    )
    request = _make_request(headers={"X-API-Key": "test-key"})
    with patch(f"{MODULE}.rate_limit_cache") as mock_cache:
      mock_cache.check_rate_limit.return_value = (False, 0)
      with pytest.raises(HTTPException) as exc_info:
        dep(request)
      assert exc_info.value.status_code == 429
      assert "test_limit" in exc_info.value.detail


@pytest.mark.unit
class TestAuthRateLimitDependency:
  """Tests for auth_rate_limit_dependency."""

  def test_login_allowed(self):
    from robosystems.middleware.rate_limits.rate_limiting import (
      auth_rate_limit_dependency,
    )

    request = _make_request(path="/v1/auth/login")
    with (
      patch(f"{MODULE}.rate_limit_cache") as mock_cache,
      patch(f"{MODULE}.get_int_env", return_value=300),
    ):
      mock_cache.check_rate_limit.return_value = (True, 4)
      auth_rate_limit_dependency(request)
      assert request.state.auth_rate_limit_remaining == 4

  def test_login_exceeded_raises_429(self):
    from robosystems.middleware.rate_limits.rate_limiting import (
      auth_rate_limit_dependency,
    )

    request = _make_request(path="/v1/auth/login")
    with (
      patch(f"{MODULE}.rate_limit_cache") as mock_cache,
      patch(f"{MODULE}.get_int_env", return_value=300),
      patch(f"{MODULE}.SecurityAuditLogger"),
    ):
      mock_cache.check_rate_limit.return_value = (False, 0)
      with pytest.raises(HTTPException) as exc_info:
        auth_rate_limit_dependency(request)
      assert exc_info.value.status_code == 429
      assert "authentication" in exc_info.value.detail.lower()

  def test_register_path(self):
    from robosystems.middleware.rate_limits.rate_limiting import (
      auth_rate_limit_dependency,
    )

    request = _make_request(path="/v1/auth/register")
    with (
      patch(f"{MODULE}.rate_limit_cache") as mock_cache,
      patch(f"{MODULE}.get_int_env", return_value=3600),
    ):
      mock_cache.check_rate_limit.return_value = (True, 2)
      auth_rate_limit_dependency(request)

  def test_default_auth_path(self):
    from robosystems.middleware.rate_limits.rate_limiting import (
      auth_rate_limit_dependency,
    )

    request = _make_request(path="/v1/auth/verify-email")
    with (
      patch(f"{MODULE}.rate_limit_cache") as mock_cache,
      patch(f"{MODULE}.get_int_env", return_value=300),
    ):
      mock_cache.check_rate_limit.return_value = (True, 8)
      auth_rate_limit_dependency(request)


@pytest.mark.unit
class TestBillingRateLimitDependency:
  """Tests for billing_rate_limit_dependency."""

  def test_billing_allowed(self):
    from robosystems.middleware.rate_limits.rate_limiting import (
      billing_rate_limit_dependency,
    )

    request = _make_request(
      headers={"X-API-Key": "test-key"}, path="/v1/billing/checkout"
    )
    with (
      patch(f"{MODULE}.rate_limit_cache") as mock_cache,
      patch(f"{MODULE}.get_int_env", return_value=60),
    ):
      mock_cache.check_rate_limit.return_value = (True, 55)
      billing_rate_limit_dependency(request)
      assert request.state.billing_rate_limit_remaining == 55

  def test_billing_exceeded_raises_429(self):
    from robosystems.middleware.rate_limits.rate_limiting import (
      billing_rate_limit_dependency,
    )

    request = _make_request(path="/v1/billing/checkout")
    with (
      patch(f"{MODULE}.rate_limit_cache") as mock_cache,
      patch(f"{MODULE}.get_int_env", return_value=60),
    ):
      mock_cache.check_rate_limit.return_value = (False, 0)
      with pytest.raises(HTTPException) as exc_info:
        billing_rate_limit_dependency(request)
      assert exc_info.value.status_code == 429


@pytest.mark.unit
class TestNamedRateLimitDependencies:
  """Tests for named rate limit dependency functions."""

  @patch(f"{MODULE}.rate_limit_cache")
  @patch(f"{MODULE}.get_int_env", return_value=600)
  def test_user_management(self, mock_env, mock_cache):
    from robosystems.middleware.rate_limits.rate_limiting import (
      user_management_rate_limit_dependency,
    )

    mock_cache.check_rate_limit.return_value = (True, 599)
    request = _make_request(headers={"X-API-Key": "key"})
    user_management_rate_limit_dependency(request)

  @patch(f"{MODULE}.rate_limit_cache")
  @patch(f"{MODULE}.get_int_env", return_value=50)
  def test_sync_operations(self, mock_env, mock_cache):
    from robosystems.middleware.rate_limits.rate_limiting import (
      sync_operations_rate_limit_dependency,
    )

    mock_cache.check_rate_limit.return_value = (True, 49)
    request = _make_request(headers={"X-API-Key": "key"})
    sync_operations_rate_limit_dependency(request)

  @patch(f"{MODULE}.rate_limit_cache")
  @patch(f"{MODULE}.get_int_env", return_value=30)
  def test_connection_management(self, mock_env, mock_cache):
    from robosystems.middleware.rate_limits.rate_limiting import (
      connection_management_rate_limit_dependency,
    )

    mock_cache.check_rate_limit.return_value = (True, 29)
    request = _make_request(headers={"X-API-Key": "key"})
    connection_management_rate_limit_dependency(request)

  @patch(f"{MODULE}.rate_limit_cache")
  @patch(f"{MODULE}.get_int_env", return_value=100)
  def test_analytics(self, mock_env, mock_cache):
    from robosystems.middleware.rate_limits.rate_limiting import (
      analytics_rate_limit_dependency,
    )

    mock_cache.check_rate_limit.return_value = (True, 99)
    request = _make_request(headers={"X-API-Key": "key"})
    analytics_rate_limit_dependency(request)

  @patch(f"{MODULE}.rate_limit_cache")
  @patch(f"{MODULE}.get_int_env", return_value=10)
  def test_backup_operations(self, mock_env, mock_cache):
    from robosystems.middleware.rate_limits.rate_limiting import (
      backup_operations_rate_limit_dependency,
    )

    mock_cache.check_rate_limit.return_value = (True, 9)
    request = _make_request(headers={"X-API-Key": "key"})
    backup_operations_rate_limit_dependency(request)

  @patch(f"{MODULE}.rate_limit_cache")
  @patch(f"{MODULE}.get_int_env", return_value=60)
  def test_sensitive_auth(self, mock_env, mock_cache):
    from robosystems.middleware.rate_limits.rate_limiting import (
      sensitive_auth_rate_limit_dependency,
    )

    mock_cache.check_rate_limit.return_value = (True, 59)
    request = _make_request(headers={"X-API-Key": "key"})
    sensitive_auth_rate_limit_dependency(request)

  @patch(f"{MODULE}.rate_limit_cache")
  @patch(f"{MODULE}.get_int_env", return_value=300)
  def test_logout(self, mock_env, mock_cache):
    from robosystems.middleware.rate_limits.rate_limiting import (
      logout_rate_limit_dependency,
    )

    mock_cache.check_rate_limit.return_value = (True, 299)
    request = _make_request(headers={"X-API-Key": "key"})
    logout_rate_limit_dependency(request)

  @patch(f"{MODULE}.rate_limit_cache")
  @patch(f"{MODULE}.get_int_env", return_value=200)
  def test_tasks_management(self, mock_env, mock_cache):
    from robosystems.middleware.rate_limits.rate_limiting import (
      tasks_management_rate_limit_dependency,
    )

    mock_cache.check_rate_limit.return_value = (True, 199)
    request = _make_request(headers={"X-API-Key": "key"})
    tasks_management_rate_limit_dependency(request)

  @patch(f"{MODULE}.rate_limit_cache")
  @patch(f"{MODULE}.get_int_env", return_value=600)
  def test_auth_status(self, mock_env, mock_cache):
    from robosystems.middleware.rate_limits.rate_limiting import (
      auth_status_rate_limit_dependency,
    )

    mock_cache.check_rate_limit.return_value = (True, 599)
    request = _make_request(headers={"X-API-Key": "key"})
    auth_status_rate_limit_dependency(request)

  @patch(f"{MODULE}.rate_limit_cache")
  @patch(f"{MODULE}.get_int_env", return_value=100)
  def test_sso(self, mock_env, mock_cache):
    from robosystems.middleware.rate_limits.rate_limiting import (
      sso_rate_limit_dependency,
    )

    mock_cache.check_rate_limit.return_value = (True, 99)
    request = _make_request(headers={"X-API-Key": "key"})
    sso_rate_limit_dependency(request)

  @patch(f"{MODULE}.rate_limit_cache")
  @patch(f"{MODULE}.get_int_env", return_value=200)
  def test_general_api(self, mock_env, mock_cache):
    from robosystems.middleware.rate_limits.rate_limiting import (
      general_api_rate_limit_dependency,
    )

    mock_cache.check_rate_limit.return_value = (True, 199)
    request = _make_request(headers={"X-API-Key": "key"})
    general_api_rate_limit_dependency(request)

  @patch(f"{MODULE}.rate_limit_cache")
  @patch(f"{MODULE}.get_int_env", return_value=600)
  def test_public_api(self, mock_env, mock_cache):
    from robosystems.middleware.rate_limits.rate_limiting import (
      public_api_rate_limit_dependency,
    )

    mock_cache.check_rate_limit.return_value = (True, 599)
    request = _make_request(headers={"X-API-Key": "key"})
    public_api_rate_limit_dependency(request)

  @patch(f"{MODULE}.rate_limit_cache")
  def test_jwt_refresh(self, mock_cache):
    from robosystems.middleware.rate_limits.rate_limiting import (
      jwt_refresh_rate_limit_dependency,
    )

    mock_cache.check_rate_limit.return_value = (True, 9)
    request = _make_request(headers={"X-API-Key": "key"})
    jwt_refresh_rate_limit_dependency(request)
