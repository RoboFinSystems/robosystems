"""Test rate limit isolation between different endpoint categories."""

from unittest.mock import MagicMock, Mock

import pytest
from fastapi import HTTPException, Request

from robosystems.middleware.rate_limits.cache import rate_limit_cache
from robosystems.middleware.rate_limits.rate_limiting import (
  create_custom_rate_limit_dependency,
  logout_rate_limit_dependency,
  sensitive_auth_rate_limit_dependency,
)


class TestRateLimitIsolation:
  """Test that different rate limit categories are properly isolated."""

  @pytest.fixture(autouse=True)
  def setup(self, monkeypatch):
    """Setup test environment."""

    # Mock the get_int_env function to return our test values
    def mock_get_int_env(key, default):
      values = {
        "RATE_LIMIT_LOGOUT": "300",
        "RATE_LIMIT_SENSITIVE_AUTH": "60",
      }
      return int(values.get(key, default))

    monkeypatch.setattr(
      "robosystems.middleware.rate_limits.rate_limiting.get_int_env", mock_get_int_env
    )

    # Mock JWT secret key
    monkeypatch.setattr("robosystems.config.env.JWT_SECRET_KEY", "test-secret-key")

    # Clear cache before each test
    if hasattr(rate_limit_cache, "_redis_client"):
      rate_limit_cache._redis_client = None

  def create_mock_request(self, jwt_user_id="test-user-123", ip="192.168.1.1"):
    """Create a mock request with JWT authentication."""
    request = MagicMock(spec=Request)
    request.client = Mock()
    request.client.host = ip
    request.headers = {
      "Authorization": "Bearer fake-jwt-token",
      "user-agent": "test-agent",
    }
    request.cookies = {}
    request.url = Mock()
    request.url.path = "/test/path"
    request.state = Mock()
    request.state.current_time = None  # Ensure current_time is None, not a Mock
    return request

  def test_logout_does_not_affect_sensitive_auth(self, monkeypatch):
    """Test that logout rate limit does not pollute sensitive auth rate limit."""

    # Mock the JWT verification to return a user ID
    def mock_verify_jwt(token):
      return "test-user-123"

    monkeypatch.setattr(
      "robosystems.middleware.rate_limits.rate_limiting._verify_jwt_for_rate_limiting",
      mock_verify_jwt,
    )

    # Mock the rate limit cache to track calls
    check_calls = []

    def mock_check_rate_limit(identifier, limit, window):
      check_calls.append((identifier, limit, window))
      # Always allow for this test
      return (True, limit - 1)

    monkeypatch.setattr(
      "robosystems.middleware.rate_limits.cache.rate_limit_cache.check_rate_limit",
      mock_check_rate_limit,
    )

    # Create request
    request = self.create_mock_request()

    # Call logout rate limit (300/minute limit)
    logout_rate_limit_dependency(request)

    # Verify the cache key includes "logout"
    assert len(check_calls) == 1
    logout_key, logout_limit, _ = check_calls[0]
    assert ":logout" in logout_key
    assert logout_limit == 300  # Full limit for authenticated user

    # Clear calls
    check_calls.clear()

    # Call sensitive auth rate limit (60/minute limit)
    sensitive_auth_rate_limit_dependency(request)

    # Verify the cache key includes "sensitive_auth" and is different from logout
    assert len(check_calls) == 1
    auth_key, auth_limit, _ = check_calls[0]
    assert ":sensitive_auth" in auth_key
    assert auth_limit == 60  # Full limit for authenticated user

    # Verify the keys are different
    assert logout_key != auth_key
    assert "jwt:test-user-123:logout" in logout_key
    assert "jwt:test-user-123:sensitive_auth" in auth_key

  def test_custom_rate_limit_creates_unique_keys(self, monkeypatch):
    """Test that custom rate limit dependencies create unique cache keys."""

    # Mock the JWT verification
    def mock_verify_jwt(token):
      return "test-user-123"

    monkeypatch.setattr(
      "robosystems.middleware.rate_limits.rate_limiting._verify_jwt_for_rate_limiting",
      mock_verify_jwt,
    )

    # Track cache calls
    check_calls = []

    def mock_check_rate_limit(identifier, limit, window):
      check_calls.append((identifier, limit, window))
      return (True, limit - 1)

    monkeypatch.setattr(
      "robosystems.middleware.rate_limits.cache.rate_limit_cache.check_rate_limit",
      mock_check_rate_limit,
    )

    request = self.create_mock_request()

    # Create different custom rate limiters
    limiter_a = create_custom_rate_limit_dependency(100, 60, "endpoint_a")
    limiter_b = create_custom_rate_limit_dependency(200, 60, "endpoint_b")

    # Call both limiters
    limiter_a(request)
    limiter_b(request)

    # Verify different cache keys were used
    assert len(check_calls) == 2
    key_a, limit_a, _ = check_calls[0]
    key_b, limit_b, _ = check_calls[1]

    assert ":endpoint_a" in key_a
    assert ":endpoint_b" in key_b
    assert key_a != key_b
    assert limit_a == 100
    assert limit_b == 200

  def test_rate_limit_exhaustion_is_isolated(self, monkeypatch):
    """Test that exhausting one rate limit doesn't affect others."""

    # Mock JWT verification
    def mock_verify_jwt(token):
      return "test-user-123"

    monkeypatch.setattr(
      "robosystems.middleware.rate_limits.rate_limiting._verify_jwt_for_rate_limiting",
      mock_verify_jwt,
    )

    # Track rate limit states per key
    rate_limit_states = {}

    def mock_check_rate_limit(identifier, limit, window):
      if identifier not in rate_limit_states:
        rate_limit_states[identifier] = {"count": 0, "limit": limit}

      state = rate_limit_states[identifier]
      state["count"] += 1

      # Allow requests until limit is reached
      allowed = state["count"] <= state["limit"]
      remaining = max(0, state["limit"] - state["count"])

      return (allowed, remaining)

    monkeypatch.setattr(
      "robosystems.middleware.rate_limits.cache.rate_limit_cache.check_rate_limit",
      mock_check_rate_limit,
    )

    request = self.create_mock_request()

    # Create limiters with very low limits for testing
    logout_limiter = create_custom_rate_limit_dependency(2, 60, "logout_test")
    auth_limiter = create_custom_rate_limit_dependency(3, 60, "auth_test")

    # Exhaust logout limit (2 requests)
    logout_limiter(request)
    logout_limiter(request)

    # Third logout request should fail
    with pytest.raises(HTTPException) as exc_info:
      logout_limiter(request)
    assert exc_info.value.status_code == 429
    assert "logout_test" in exc_info.value.detail

    # Auth requests should still work (different limit pool)
    auth_limiter(request)
    auth_limiter(request)
    auth_limiter(request)

    # Fourth auth request should fail
    with pytest.raises(HTTPException) as exc_info:
      auth_limiter(request)
    assert exc_info.value.status_code == 429
    assert "auth_test" in exc_info.value.detail

    # Verify the isolation - logout is still blocked
    with pytest.raises(HTTPException) as exc_info:
      logout_limiter(request)
    assert exc_info.value.status_code == 429

  def test_anonymous_users_get_isolated_limits(self, monkeypatch):
    """Test that anonymous users also get isolated rate limits."""

    # No JWT verification needed - testing anonymous users
    def mock_verify_jwt(token):
      return None

    monkeypatch.setattr(
      "robosystems.middleware.rate_limits.rate_limiting._verify_jwt_for_rate_limiting",
      mock_verify_jwt,
    )

    check_calls = []

    def mock_check_rate_limit(identifier, limit, window):
      check_calls.append((identifier, limit, window))
      return (True, limit - 1)

    monkeypatch.setattr(
      "robosystems.middleware.rate_limits.cache.rate_limit_cache.check_rate_limit",
      mock_check_rate_limit,
    )

    # Create anonymous request (no auth headers)
    request = MagicMock(spec=Request)
    request.client = Mock()
    request.client.host = "192.168.1.100"
    request.headers = {"user-agent": "test-agent"}
    request.cookies = {}
    request.url = Mock()
    request.url.path = "/test/path"
    request.state = Mock()
    request.state.current_time = None  # Ensure current_time is None, not a Mock

    # Call different rate limiters
    logout_rate_limit_dependency(request)
    sensitive_auth_rate_limit_dependency(request)

    # Verify isolation for anonymous users
    assert len(check_calls) == 2
    logout_key, logout_limit, _ = check_calls[0]
    auth_key, auth_limit, _ = check_calls[1]

    # Anonymous users get IP-based keys with category suffix
    assert "ip:192.168.1.100:logout" in logout_key
    assert "ip:192.168.1.100:sensitive_auth" in auth_key
    assert logout_key != auth_key

    # Anonymous users get 1/10th the limit
    assert logout_limit == 30  # 300/10
    assert auth_limit == 6  # 60/10

  def test_public_api_does_not_affect_general_api(self, monkeypatch):
    """Test that public API rate limit does not pollute general API rate limit."""
    from robosystems.middleware.rate_limits.rate_limiting import (
      general_api_rate_limit_dependency,
      public_api_rate_limit_dependency,
    )

    # Mock the get_int_env function to return our test values
    def mock_get_int_env(key, default):
      values = {
        "RATE_LIMIT_PUBLIC_API": "600",  # High limit for public endpoints
        "RATE_LIMIT_GENERAL_API": "200",  # Lower limit for general endpoints
      }
      return int(values.get(key, default))

    monkeypatch.setattr(
      "robosystems.middleware.rate_limits.rate_limiting.get_int_env", mock_get_int_env
    )

    # Mock JWT verification for authenticated user
    def mock_verify_jwt(token):
      return "test-user-456"

    monkeypatch.setattr(
      "robosystems.middleware.rate_limits.rate_limiting._verify_jwt_for_rate_limiting",
      mock_verify_jwt,
    )

    # Track rate limit states per key
    rate_limit_states = {}

    def mock_check_rate_limit(identifier, limit, window):
      if identifier not in rate_limit_states:
        rate_limit_states[identifier] = {"count": 0, "limit": limit}

      state = rate_limit_states[identifier]
      state["count"] += 1

      # Allow requests until limit is reached
      allowed = state["count"] <= state["limit"]
      remaining = max(0, state["limit"] - state["count"])

      return (allowed, remaining)

    monkeypatch.setattr(
      "robosystems.middleware.rate_limits.cache.rate_limit_cache.check_rate_limit",
      mock_check_rate_limit,
    )

    request = self.create_mock_request(jwt_user_id="test-user-456")

    # Make 300 public API requests (well within 600/min limit)
    for _ in range(300):
      public_api_rate_limit_dependency(request)

    # General API requests should still work (separate 200/min limit)
    for _ in range(200):
      general_api_rate_limit_dependency(request)

    # 201st general API request should fail (exceeds 200/min)
    with pytest.raises(HTTPException) as exc_info:
      general_api_rate_limit_dependency(request)
    assert exc_info.value.status_code == 429
    assert "general_api" in exc_info.value.detail

    # But public API should still work (under 600/min limit)
    for _ in range(299):  # 300 + 299 = 599, still under 600
      public_api_rate_limit_dependency(request)

    # 600th public API request should succeed
    public_api_rate_limit_dependency(request)

    # 601st public API request should fail
    with pytest.raises(HTTPException) as exc_info:
      public_api_rate_limit_dependency(request)
    assert exc_info.value.status_code == 429
    assert "public_api" in exc_info.value.detail

    # Verify the keys are properly isolated
    assert len(rate_limit_states) == 2
    assert any("public_api" in key for key in rate_limit_states)
    assert any("general_api" in key for key in rate_limit_states)
