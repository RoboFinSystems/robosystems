"""Tests for subscription-based rate limiting module."""

from unittest.mock import patch

from robosystems.config.rate_limits import EndpointCategory, RateLimitPeriod
from robosystems.middleware.rate_limits.subscription_rate_limits import (
  SUBSCRIPTION_RATE_LIMITS,
  get_endpoint_category,
  get_subscription_rate_limit,
  should_use_subscription_limits,
)


class TestSubscriptionRateLimits:
  """Test subscription-based rate limiting."""

  def test_subscription_rate_limits_imported(self):
    """Test that subscription rate limits are imported correctly."""
    assert SUBSCRIPTION_RATE_LIMITS is not None
    assert isinstance(SUBSCRIPTION_RATE_LIMITS, dict)

  def test_get_subscription_rate_limit(self):
    """Test getting subscription rate limit for tier and category."""
    # Test for various tiers
    tiers = ["free", "starter", "pro", "enterprise"]
    categories = [
      EndpointCategory.GRAPH_READ,
      EndpointCategory.GRAPH_WRITE,
      EndpointCategory.GRAPH_BACKUP,
      EndpointCategory.AUTH,
      EndpointCategory.USER_MANAGEMENT,
    ]

    for tier in tiers:
      for category in categories:
        result = get_subscription_rate_limit(tier, category)
        # Result should be None or a tuple of (limit, window_seconds)
        assert result is None or (
          isinstance(result, tuple)
          and len(result) == 2
          and isinstance(result[0], int)
          and isinstance(result[1], int)
        )

  @patch(
    "robosystems.middleware.rate_limits.subscription_rate_limits.RateLimitConfig.get_rate_limit"
  )
  def test_get_subscription_rate_limit_delegates(self, mock_get_rate_limit):
    """Test that get_subscription_rate_limit delegates to RateLimitConfig."""
    mock_get_rate_limit.return_value = (100, 60)

    result = get_subscription_rate_limit("pro", EndpointCategory.GRAPH_READ)

    assert result == (100, 60)
    mock_get_rate_limit.assert_called_once_with("pro", EndpointCategory.GRAPH_READ)

  def test_get_endpoint_category_query_endpoints(self):
    """Test endpoint categorization for query endpoints."""
    query_paths = [
      "/v1/graphs/kg1234/query",
      "/v1/graphs/sec/query",
      "/v1/abc123/cypher",
    ]

    for path in query_paths:
      category = get_endpoint_category(path, "POST")
      # Should be categorized appropriately
      assert category is None or isinstance(category, EndpointCategory)

  def test_get_endpoint_category_ingestion_endpoints(self):
    """Test endpoint categorization for ingestion endpoints."""
    ingestion_paths = [
      "/v1/kg1234/ingest",
      "/v1/abc123/import",
      "/v1/sec/load",
    ]

    for path in ingestion_paths:
      category = get_endpoint_category(path, "POST")
      # Should be categorized appropriately
      assert category is None or isinstance(category, EndpointCategory)

  @patch(
    "robosystems.middleware.rate_limits.subscription_rate_limits.RateLimitConfig.get_endpoint_category"
  )
  def test_get_endpoint_category_delegates(self, mock_get_category):
    """Test that get_endpoint_category delegates to RateLimitConfig."""
    mock_get_category.return_value = EndpointCategory.GRAPH_READ

    result = get_endpoint_category("/v1/graphs/kg1234/query", "POST")

    assert result == EndpointCategory.GRAPH_READ
    mock_get_category.assert_called_once_with("/v1/graphs/kg1234/query", "POST")

  def test_should_use_subscription_limits_graph_endpoints(self):
    """Test subscription limit detection for graph-scoped endpoints."""
    # Graph-scoped endpoints should use subscription limits
    graph_paths = [
      "/v1/graphs/kg1234567890/query",
      "/v1/sec/info",
      "/v1/abc123/schema",
      "/v1/xyz789/backup",
    ]

    for path in graph_paths:
      assert should_use_subscription_limits(path) is True

  def test_should_use_subscription_limits_non_graph_endpoints(self):
    """Test subscription limit detection for non-graph endpoints."""
    # Auth and status endpoints should not use subscription limits
    non_graph_paths = [
      "/v1/auth/login",
      "/v1/status",
      "/v1/health",
      "/v1/create/graph",
    ]

    for path in non_graph_paths:
      assert should_use_subscription_limits(path) is False

  def test_should_use_subscription_limits_user_endpoints(self):
    """Test subscription limit detection for user endpoints."""
    # Some user endpoints should use subscription limits
    assert should_use_subscription_limits("/v1/user/subscription/status") is True
    assert should_use_subscription_limits("/v1/user/limits") is True
    assert should_use_subscription_limits("/v1/operations/agent") is True

    # But not all user endpoints
    assert should_use_subscription_limits("/v1/user/profile") is False

  def test_should_use_subscription_limits_edge_cases(self):
    """Test edge cases for subscription limit detection."""
    # Test empty path
    assert should_use_subscription_limits("") is False

    # Test root path
    assert should_use_subscription_limits("/") is False

    # Test v2 endpoints
    assert should_use_subscription_limits("/v2/kg123/query") is False

    # Test path without enough segments
    assert should_use_subscription_limits("/v1/") is False
    assert should_use_subscription_limits("/v1/single") is False

  def test_should_use_subscription_limits_special_endpoints(self):
    """Test subscription limits for special operation endpoints."""
    special_paths = [
      "/v1/user/subscription/upgrade",
      "/v1/user/limits/current",
      "/v1/operations/sync",
    ]

    for path in special_paths:
      assert should_use_subscription_limits(path) is True

  def test_path_parsing_correctness(self):
    """Test that path parsing handles various formats correctly."""
    # Test paths with trailing slashes
    assert should_use_subscription_limits("/v1/kg123/query/") is True
    assert should_use_subscription_limits("/v1/auth/login/") is False

    # Test paths with query parameters (should be handled before calling this)
    # In practice, query params should be stripped before checking
    path_with_query = "/v1/kg123/query?limit=10"
    # The function doesn't handle query params, so it treats them as part of path
    result = should_use_subscription_limits(path_with_query)
    assert isinstance(result, bool)

  def test_subscription_rate_limits_structure(self):
    """Test that subscription rate limits have expected structure."""
    if SUBSCRIPTION_RATE_LIMITS:
      for tier, categories in SUBSCRIPTION_RATE_LIMITS.items():
        assert isinstance(tier, str)
        assert isinstance(categories, dict)

        for category, limits in categories.items():
          # Category should be an EndpointCategory or string
          assert isinstance(category, (EndpointCategory, str))
          # Limits should be a tuple of (limit, window)
          assert isinstance(limits, tuple)
          assert len(limits) == 2
          assert isinstance(limits[0], int)  # limit
          assert isinstance(
            limits[1], (int, RateLimitPeriod)
          )  # window in seconds or period

  def test_rate_limit_hierarchy(self):
    """Test that rate limits follow expected hierarchy."""
    # If we have access to actual limits, verify enterprise > pro > starter > free
    if SUBSCRIPTION_RATE_LIMITS and len(SUBSCRIPTION_RATE_LIMITS) > 0:
      # Get limits for a common category if available
      test_category = EndpointCategory.GRAPH_READ

      free_limit = get_subscription_rate_limit("free", test_category)
      starter_limit = get_subscription_rate_limit("starter", test_category)
      pro_limit = get_subscription_rate_limit("pro", test_category)
      enterprise_limit = get_subscription_rate_limit("enterprise", test_category)

      # If all tiers have limits, verify hierarchy
      if all([free_limit, starter_limit, pro_limit, enterprise_limit]):
        # Higher tiers should have higher or equal limits
        assert free_limit is not None and starter_limit is not None
        assert free_limit[0] <= starter_limit[0]
        assert starter_limit is not None and pro_limit is not None
        assert starter_limit[0] <= pro_limit[0]
        assert pro_limit is not None and enterprise_limit is not None
        assert pro_limit[0] <= enterprise_limit[0]

  def test_endpoint_category_enum_values(self):
    """Test that EndpointCategory enum is accessible."""
    # Verify we can access category enum values
    assert hasattr(EndpointCategory, "AUTH")
    assert hasattr(EndpointCategory, "USER_MANAGEMENT")
    assert hasattr(EndpointCategory, "GRAPH_READ")
    assert hasattr(EndpointCategory, "GRAPH_WRITE")
    assert hasattr(EndpointCategory, "GRAPH_BACKUP")

  @patch("robosystems.middleware.rate_limits.subscription_rate_limits.RateLimitConfig")
  def test_functions_use_rate_limit_config(self, mock_config):
    """Test that all functions properly delegate to RateLimitConfig."""
    # Setup mocks
    mock_config.get_rate_limit.return_value = (100, 60)
    mock_config.get_endpoint_category.return_value = EndpointCategory.GRAPH_READ

    # Test get_subscription_rate_limit
    result1 = get_subscription_rate_limit("pro", EndpointCategory.GRAPH_READ)
    assert result1 == (100, 60)

    # Test get_endpoint_category
    result2 = get_endpoint_category("/v1/graphs/kg123/query", "POST")
    assert result2 == EndpointCategory.GRAPH_READ

  def test_module_exports(self):
    """Test that module exports expected functions and constants."""
    from robosystems.middleware.rate_limits import subscription_rate_limits

    assert hasattr(subscription_rate_limits, "get_subscription_rate_limit")
    assert hasattr(subscription_rate_limits, "get_endpoint_category")
    assert hasattr(subscription_rate_limits, "should_use_subscription_limits")
    assert hasattr(subscription_rate_limits, "SUBSCRIPTION_RATE_LIMITS")

  def test_path_validation_in_should_use_subscription_limits(self):
    """Test path validation in should_use_subscription_limits."""
    # Test various invalid paths
    invalid_paths = [
      None,  # None path (would cause error if not handled)
      123,  # Non-string path (would cause error if not handled)
      [],  # List instead of string
    ]

    for invalid_path in invalid_paths:
      try:
        # Should either handle gracefully or raise appropriate error
        result = should_use_subscription_limits(invalid_path)
        # If it doesn't raise, it should return False for invalid input
        assert result is False
      except (TypeError, AttributeError):
        # These are acceptable errors for invalid input
        pass
