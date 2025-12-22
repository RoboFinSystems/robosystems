"""Tests for subscription-based rate limiting."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException, Request
from starlette.datastructures import Headers

from robosystems.middleware.rate_limits import (
  EndpointCategory,
  get_endpoint_category,
  get_subscription_rate_limit,
  should_use_subscription_limits,
  subscription_aware_rate_limit_dependency,
)


class TestSubscriptionRateLimits:
  """Test subscription rate limit configuration."""

  def test_get_endpoint_category(self):
    """Test endpoint categorization."""
    # Auth endpoints
    assert get_endpoint_category("/v1/auth/login") == EndpointCategory.AUTH
    assert get_endpoint_category("/v1/auth/register") == EndpointCategory.AUTH

    # User management endpoints
    assert get_endpoint_category("/v1/user/profile") == EndpointCategory.USER_MANAGEMENT
    assert (
      get_endpoint_category("/v1/user/subscription") == EndpointCategory.USER_MANAGEMENT
    )

    # Graph-scoped endpoints
    assert (
      get_endpoint_category("/v1/graphs/kg1a2b3c/entity/", "GET")
      == EndpointCategory.GRAPH_READ
    )
    assert (
      get_endpoint_category("/v1/graphs/kg1a2b3c/entity/", "POST")
      == EndpointCategory.GRAPH_WRITE
    )
    assert (
      get_endpoint_category("/v1/graphs/kg1a2b3c/mcp/query")
      == EndpointCategory.GRAPH_MCP
    )
    assert (
      get_endpoint_category("/v1/graphs/kg1a2b3c/agent/query")
      == EndpointCategory.GRAPH_AGENT
    )
    assert (
      get_endpoint_category("/v1/graphs/kg1a2b3c/graph/query/cypher")
      == EndpointCategory.GRAPH_QUERY
    )
    assert (
      get_endpoint_category("/v1/graphs/kg1a2b3c/graph/analytics/metrics")
      == EndpointCategory.GRAPH_ANALYTICS
    )
    assert (
      get_endpoint_category("/v1/graphs/kg1a2b3c/graph/backup/create")
      == EndpointCategory.GRAPH_BACKUP
    )
    assert (
      get_endpoint_category("/v1/graphs/kg1a2b3c/sync/quickbooks")
      == EndpointCategory.GRAPH_SYNC
    )

  def test_should_use_subscription_limits(self):
    """Test which endpoints should use subscription limits."""
    # Graph-scoped endpoints should use subscription limits
    assert should_use_subscription_limits("/v1/graphs/kg1a2b3c/entity/")
    assert should_use_subscription_limits("/v1/graphs/kg1a2b3c/mcp/query")
    assert should_use_subscription_limits("/v1/graphs/sec/entity/")

    # Non-graph endpoints that should use subscription limits
    assert should_use_subscription_limits("/v1/user/subscription/status")
    assert should_use_subscription_limits("/v1/operations/123/status")

    # Non-graph endpoints that should NOT use subscription limits
    assert not should_use_subscription_limits("/v1/auth/login")
    assert not should_use_subscription_limits("/v1/health")
    assert not should_use_subscription_limits("/v1/status")

  def test_get_subscription_rate_limit(self):
    """Test rate limit retrieval for different tiers."""
    # Free tier
    limit, window = get_subscription_rate_limit("free", EndpointCategory.GRAPH_READ)
    assert limit == 100
    assert window == 60  # 1 minute

    limit, window = get_subscription_rate_limit("free", EndpointCategory.GRAPH_MCP)
    assert limit == 10
    assert window == 60  # 1 minute

    # LadybugDB Standard tier
    limit, window = get_subscription_rate_limit(
      "ladybug-standard", EndpointCategory.GRAPH_READ
    )
    assert limit == 500
    assert window == 60

    # LadybugDB Standard tier write
    limit, window = get_subscription_rate_limit(
      "ladybug-standard", EndpointCategory.GRAPH_WRITE
    )
    assert limit == 100
    assert window == 60

    # LadybugDB Large tier (enterprise-level)
    limit, window = get_subscription_rate_limit(
      "ladybug-large", EndpointCategory.GRAPH_QUERY
    )
    assert limit == 1000
    assert window == 60

  def test_standard_tier_has_appropriate_limits(self):
    """Test that standard tier has appropriate limits."""
    for category in EndpointCategory:
      standard_limit = get_subscription_rate_limit("ladybug-standard", category)
      free_limit = get_subscription_rate_limit("free", category)
      # Standard should have higher limits than free
      assert standard_limit is not None and free_limit is not None
      assert standard_limit[0] >= free_limit[0]


class TestSubscriptionAwareRateLimiting:
  """Test subscription-aware rate limiting dependency."""

  @pytest.fixture
  def mock_request(self):
    """Create a mock request."""
    request = MagicMock(spec=Request)
    request.url.path = "/v1/graphs/kg1a2b3c/entity/"
    request.method = "GET"
    request.client = MagicMock()
    request.client.host = "192.168.1.1"
    request.headers = Headers({"user-agent": "test-client"})
    request.state = MagicMock()
    request.cookies = {}
    return request

  @patch("robosystems.middleware.rate_limits.rate_limiting.get_user_from_request")
  @patch(
    "robosystems.middleware.rate_limits.rate_limiting.rate_limit_cache.check_rate_limit"
  )
  def test_subscription_rate_limiting_free_tier(
    self, mock_check_rate_limit, mock_get_user, mock_request
  ):
    """Test rate limiting for anonymous user (free tier)."""
    # Setup mocks - anonymous user gets free tier
    mock_get_user.return_value = None  # Anonymous user
    mock_check_rate_limit.return_value = (True, 50)  # Allowed with 50 remaining

    # Call the dependency
    subscription_aware_rate_limit_dependency(mock_request)

    # Verify correct limit was checked (100/minute for free tier graph reads)
    mock_check_rate_limit.assert_called_once_with(
      "anon_sub:192.168.1.1:graph_read", 100, 60
    )

    # Verify request state was updated
    assert mock_request.state.rate_limit_remaining == 50
    assert mock_request.state.rate_limit_limit == 100
    assert mock_request.state.rate_limit_tier == "free"
    assert mock_request.state.rate_limit_category == "graph_read"

  @patch("robosystems.middleware.rate_limits.rate_limiting.get_user_from_request")
  @patch(
    "robosystems.middleware.rate_limits.rate_limiting.rate_limit_cache.check_rate_limit"
  )
  def test_subscription_rate_limiting_standard_tier(
    self, mock_check_rate_limit, mock_get_user, mock_request
  ):
    """Test rate limiting for authenticated user (standard tier)."""
    # Setup mocks - authenticated users get standard tier
    mock_get_user.return_value = "user_456"  # Authenticated user
    mock_check_rate_limit.return_value = (True, 5000)  # Allowed with 5000 remaining

    # Call the dependency
    subscription_aware_rate_limit_dependency(mock_request)

    # Verify correct limit was checked (500/minute for standard tier graph reads)
    mock_check_rate_limit.assert_called_once_with(
      "user_sub:user_456:graph_read", 500, 60
    )

    # Verify request state was updated
    assert mock_request.state.rate_limit_remaining == 5000
    assert mock_request.state.rate_limit_limit == 500
    assert mock_request.state.rate_limit_tier == "ladybug-standard"

  @patch("robosystems.middleware.rate_limits.rate_limiting.get_user_from_request")
  @patch(
    "robosystems.middleware.rate_limits.rate_limiting.rate_limit_cache.check_rate_limit"
  )
  @patch(
    "robosystems.middleware.rate_limits.rate_limiting.SecurityAuditLogger.log_rate_limit_exceeded"
  )
  def test_subscription_rate_limiting_exceeded(
    self,
    mock_log_exceeded,
    mock_check_rate_limit,
    mock_get_user,
    mock_request,
  ):
    """Test rate limiting when limit is exceeded."""
    # Setup mocks - anonymous user gets free tier
    mock_get_user.return_value = None  # Anonymous user
    mock_check_rate_limit.return_value = (False, 0)  # Not allowed, limit exceeded

    # Call the dependency and expect HTTPException
    with pytest.raises(HTTPException) as exc_info:
      subscription_aware_rate_limit_dependency(mock_request)

    # Verify exception details
    assert exc_info.value.status_code == 429
    assert "Rate limit exceeded for graph read operations" in exc_info.value.detail
    assert "Upgrade your subscription for higher limits" in exc_info.value.detail

    # Verify headers
    assert exc_info.value.headers is not None
    assert exc_info.value.headers["X-RateLimit-Tier"] == "free"
    assert exc_info.value.headers["X-RateLimit-Category"] == "graph_read"

    # Verify security logging
    mock_log_exceeded.assert_called_once()

  @patch("robosystems.middleware.rate_limits.rate_limiting.get_user_from_request")
  @patch(
    "robosystems.middleware.rate_limits.rate_limiting.rate_limit_cache.check_rate_limit"
  )
  def test_anonymous_user_gets_free_tier(
    self, mock_check_rate_limit, mock_get_user, mock_request
  ):
    """Test that anonymous users get free tier limits."""
    # Setup mocks
    mock_get_user.return_value = None  # Anonymous user
    mock_check_rate_limit.return_value = (True, 10)

    # Call the dependency
    subscription_aware_rate_limit_dependency(mock_request)

    # Verify anonymous user identifier and free tier limits
    expected_identifier = f"anon_sub:{mock_request.client.host}:graph_read"
    mock_check_rate_limit.assert_called_once_with(expected_identifier, 100, 60)

    assert mock_request.state.rate_limit_tier == "free"

  def test_mcp_endpoint_category(self):
    """Test MCP endpoints get correct category and limits."""
    # MCP endpoints should be categorized correctly
    assert (
      get_endpoint_category("/v1/graphs/kg1a2b3c/mcp/query")
      == EndpointCategory.GRAPH_MCP
    )
    assert (
      get_endpoint_category("/v1/graphs/kg1a2b3c/mcp/benchmark")
      == EndpointCategory.GRAPH_MCP
    )

    # Check MCP limits for different tiers
    limit, window = get_subscription_rate_limit("free", EndpointCategory.GRAPH_MCP)
    assert limit == 10
    assert window == 60  # Minute limit

    limit, window = get_subscription_rate_limit(
      "ladybug-standard", EndpointCategory.GRAPH_MCP
    )
    assert limit == 100
    assert window == 60
