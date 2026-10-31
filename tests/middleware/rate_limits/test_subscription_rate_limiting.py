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
      get_endpoint_category("/v1/graphs/kg1a2b3c/operator/query")
      == EndpointCategory.GRAPH_OPERATOR
    )
    # Real route is POST /v1/graphs/{graph_id}/query (not /graph/query/cypher).
    # Must be GRAPH_QUERY — never GRAPH_WRITE — even for read-only shared repos.
    assert (
      get_endpoint_category("/v1/graphs/kg1a2b3c/query", "POST")
      == EndpointCategory.GRAPH_QUERY
    )
    # Data sync lives under connections; only the /sync trigger is GRAPH_SYNC,
    # while connection reads are ordinary GRAPH_READ.
    assert (
      get_endpoint_category("/v1/graphs/kg1a2b3c/connections/c1/sync", "POST")
      == EndpointCategory.GRAPH_SYNC
    )
    assert (
      get_endpoint_category("/v1/graphs/kg1a2b3c/connections", "GET")
      == EndpointCategory.GRAPH_READ
    )
    # Lifecycle operations get their own management bucket
    assert (
      get_endpoint_category("/v1/graphs/kg1a2b3c/operations/create-subgraph", "POST")
      == EndpointCategory.GRAPH_MANAGEMENT
    )

  def test_should_use_subscription_limits(self):
    """Test which endpoints should use subscription limits."""
    # Graph-scoped endpoints should use subscription limits
    assert should_use_subscription_limits("/v1/graphs/kg1a2b3c/entity/")
    assert should_use_subscription_limits("/v1/graphs/kg1a2b3c/mcp/query")
    # The graph-agnostic OAuth transport is tenant-scoped through its grant.
    assert should_use_subscription_limits("/v1/mcp")
    assert should_use_subscription_limits("/v1/mcp/roboledger")
    assert should_use_subscription_limits("/v1/graphs/sec/entity/")

    # Non-graph endpoints that should use subscription limits
    assert should_use_subscription_limits("/v1/user/subscription/status")
    assert should_use_subscription_limits("/v1/operations/123/status")

    # Non-graph endpoints that should NOT use subscription limits
    assert not should_use_subscription_limits("/v1/auth/login")
    assert not should_use_subscription_limits("/v1/health")
    assert not should_use_subscription_limits("/v1/status")

  def test_should_use_subscription_limits_for_extensions(self):
    """The post-cutover extensions surface must use subscription buckets.

    Regression: an earlier version of `should_use_subscription_limits`
    only opted in for `/v1/*` paths, which meant the new graph-scoped
    GraphQL read endpoint and the REST operation endpoints silently
    fell back to the generic API limiter — losing per-tier
    observability and the read/write bucket split.
    """
    # GraphQL reads (graph-scoped at the URL level)
    assert should_use_subscription_limits("/extensions/kg1a2b3c/graphql")
    # RoboLedger operation writes
    assert should_use_subscription_limits(
      "/extensions/roboledger/kg1a2b3c/operations/update-entity"
    )
    # RoboInvestor operation writes
    assert should_use_subscription_limits(
      "/extensions/roboinvestor/kg1a2b3c/operations/create-portfolio"
    )

  def test_get_endpoint_category_for_extensions(self):
    """Extensions paths map to the dedicated EXTENSIONS_* buckets.

    The extensions surface is OLTP on shared RDS, so it has its own
    rate-limit categories independent of the LadybugDB graph categories.
    (Mapping to None would fall through to the generic limiter.)
    """
    # Graph-scoped GraphQL read → EXTENSIONS_GRAPHQL
    assert (
      get_endpoint_category("/extensions/kg1a2b3c/graphql", "POST")
      == EndpointCategory.EXTENSIONS_GRAPHQL
    )
    # Subgraph IDs (with underscore) work too
    assert (
      get_endpoint_category("/extensions/kg1a2b3c_dev/graphql", "POST")
      == EndpointCategory.EXTENSIONS_GRAPHQL
    )
    # RoboLedger operations → EXTENSIONS_WRITE
    assert (
      get_endpoint_category(
        "/extensions/roboledger/kg1a2b3c/operations/update-entity", "POST"
      )
      == EndpointCategory.EXTENSIONS_WRITE
    )
    assert (
      get_endpoint_category(
        "/extensions/roboledger/kg1a2b3c/operations/close-period", "POST"
      )
      == EndpointCategory.EXTENSIONS_WRITE
    )
    # RoboInvestor operations → EXTENSIONS_WRITE
    assert (
      get_endpoint_category(
        "/extensions/roboinvestor/kg1a2b3c/operations/create-portfolio", "POST"
      )
      == EndpointCategory.EXTENSIONS_WRITE
    )

  def test_get_subscription_rate_limit(self):
    """Test rate limit retrieval for different tiers."""
    # Base tier
    limit, window = get_subscription_rate_limit("base", EndpointCategory.GRAPH_READ)
    assert limit == 30
    assert window == 60  # 1 minute

    limit, window = get_subscription_rate_limit("base", EndpointCategory.GRAPH_MCP)
    assert limit == 5
    assert window == 60  # 1 minute

    # LadybugDB Standard tier
    limit, window = get_subscription_rate_limit(
      "ladybug-standard", EndpointCategory.GRAPH_READ
    )
    assert limit == 120
    assert window == 60

    # LadybugDB Standard tier write
    limit, window = get_subscription_rate_limit(
      "ladybug-standard", EndpointCategory.GRAPH_WRITE
    )
    assert limit == 30
    assert window == 60

    # LadybugDB Large tier — 2x Standard on dedicated-resource categories,
    # matching its 2 vCPU against Standard's 1. This previously asserted 60
    # with the note "1.5x multiplier applied separately"; nothing applied it,
    # so the tables were identical and the comment documented a bug.
    limit, window = get_subscription_rate_limit(
      "ladybug-large", EndpointCategory.GRAPH_QUERY
    )
    assert limit == 120
    assert window == 60

  def test_standard_tier_has_appropriate_limits(self):
    """Test that standard tier has appropriate limits."""
    for category in EndpointCategory:
      standard_limit = get_subscription_rate_limit("ladybug-standard", category)
      free_limit = get_subscription_rate_limit("base", category)
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
  def test_subscription_rate_limiting_base_tier(
    self, mock_check_rate_limit, mock_get_user, mock_request
  ):
    """Test rate limiting for anonymous user (base tier)."""
    # Setup mocks - anonymous user gets base tier
    mock_get_user.return_value = None  # Anonymous user
    mock_check_rate_limit.return_value = (True, 50)  # Allowed with 50 remaining

    # Call the dependency
    subscription_aware_rate_limit_dependency(mock_request)

    # Verify correct limit was checked (30/minute for base tier graph reads)
    mock_check_rate_limit.assert_called_once_with(
      "anon_sub:192.168.1.1:graph_read", 30, 60
    )

    # Verify request state was updated
    assert mock_request.state.rate_limit_remaining == 50
    assert mock_request.state.rate_limit_limit == 30
    assert mock_request.state.rate_limit_tier == "base"
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
    # The graph auth dependency authorized this caller on the graph; the graph
    # bucket is a member's budget and is charged only with that evidence.
    mock_request.state.auth_graph_id = "kg1a2b3c"

    # Call the dependency
    subscription_aware_rate_limit_dependency(mock_request)

    # Graph-scoped reads bucket per graph, not per user: graph_read is a
    # dedicated-resource category, so each graph gets its own budget on its own
    # instance. Buckets were keyed "user_sub:{user_id}" until per-graph pricing
    # made that wrong — ten graphs shared one budget.
    mock_check_rate_limit.assert_called_once_with(
      "graph_sub:kg1a2b3c:graph_read", 120, 60
    )

    # Verify request state was updated
    assert mock_request.state.rate_limit_remaining == 5000
    assert mock_request.state.rate_limit_limit == 120
    assert mock_request.state.rate_limit_tier == "ladybug-standard"

  @patch("robosystems.middleware.rate_limits.rate_limiting.get_user_from_request")
  @patch(
    "robosystems.middleware.rate_limits.rate_limiting.rate_limit_cache.check_rate_limit"
  )
  def test_agnostic_mcp_route_draws_from_the_grant_graph_budget(
    self, mock_check_rate_limit, mock_get_user, mock_request
  ):
    """POST /v1/mcp names no graph in its path. The OAuth dependency
    publishes the grant's graph on request.state before the limiter runs,
    and that graph's budget — not the caller's — is what a directory
    connector must draw from."""
    mock_get_user.return_value = "user_456"
    mock_check_rate_limit.return_value = (True, 4)
    mock_request.url.path = "/v1/mcp"
    mock_request.method = "POST"
    mock_request.state.auth_graph_id = "kg1a2b3c"

    subscription_aware_rate_limit_dependency(mock_request)

    limit, window = get_subscription_rate_limit(
      "ladybug-standard", EndpointCategory.GRAPH_MCP
    )
    mock_check_rate_limit.assert_called_once_with(
      "graph_sub:kg1a2b3c:graph_mcp", limit, window
    )
    assert mock_request.state.rate_limit_category == "graph_mcp"
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
    # Setup mocks - anonymous user gets base tier
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
    assert exc_info.value.headers["X-RateLimit-Tier"] == "base"
    assert exc_info.value.headers["X-RateLimit-Category"] == "graph_read"

    # Verify security logging
    mock_log_exceeded.assert_called_once()

  @patch("robosystems.middleware.rate_limits.rate_limiting.get_user_from_request")
  @patch(
    "robosystems.middleware.rate_limits.rate_limiting.rate_limit_cache.check_rate_limit"
  )
  def test_anonymous_user_gets_base_tier(
    self, mock_check_rate_limit, mock_get_user, mock_request
  ):
    """Test that anonymous users get base tier limits."""
    # Setup mocks
    mock_get_user.return_value = None  # Anonymous user
    mock_check_rate_limit.return_value = (True, 10)

    # Call the dependency
    subscription_aware_rate_limit_dependency(mock_request)

    # Verify anonymous user identifier and base tier limits
    expected_identifier = f"anon_sub:{mock_request.client.host}:graph_read"
    mock_check_rate_limit.assert_called_once_with(expected_identifier, 30, 60)

    assert mock_request.state.rate_limit_tier == "base"

  def test_agnostic_mcp_routes_are_the_mcp_category(self):
    """Neither graph-agnostic transport carries a graph in its path; both
    must land in the MCP bucket, not the generic API limiter."""
    assert get_endpoint_category("/v1/mcp", "POST") == EndpointCategory.GRAPH_MCP
    assert (
      get_endpoint_category("/v1/mcp/roboledger", "POST") == EndpointCategory.GRAPH_MCP
    )

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
    # The graph-agnostic route lands in the same bucket as its per-graph sibling.
    assert get_endpoint_category("/v1/mcp", "POST") == EndpointCategory.GRAPH_MCP

    # Check MCP limits for different tiers
    limit, window = get_subscription_rate_limit("base", EndpointCategory.GRAPH_MCP)
    assert limit == 5
    assert window == 60  # Minute limit

    limit, window = get_subscription_rate_limit(
      "ladybug-standard", EndpointCategory.GRAPH_MCP
    )
    assert limit == 30
    assert window == 60


class TestOAuthBearerOnGraphRoutes:
  """On `/v1/graphs/{graph_id}/...` the limiter is a router dependency and
  runs before the auth dependency (#1945), so nothing is published on
  `request.state` yet. A cached live OAuth token still names the caller and
  the one graph its grant covers; that is the evidence the graph bucket is
  charged on. Without it every OAuth MCP client behind one IP drew from the
  anonymous 5/min bucket."""

  MODULE = "robosystems.middleware.rate_limits.rate_limiting"

  @pytest.fixture
  def oauth_request(self):
    request = MagicMock(spec=Request)
    request.url.path = "/v1/graphs/kg1a2b3c/mcp"
    request.method = "POST"
    request.client = MagicMock()
    request.client.host = "192.168.1.1"
    request.headers = Headers(
      {"user-agent": "claude-code", "authorization": "Bearer rfso" + "a" * 48}
    )
    request.state = MagicMock()
    request.state.auth_graph_id = None
    request.cookies = {}
    return request

  def test_cached_oauth_grant_charges_the_grant_graph_bucket(self, oauth_request):
    with (
      patch(f"{self.MODULE}.rate_limit_cache.check_rate_limit") as check,
      patch(
        f"{self.MODULE}._cached_oauth_identity",
        return_value=("user_456", "kg1a2b3c"),
      ),
      patch(
        f"{self.MODULE}._verify_jwt_for_rate_limiting",
        return_value=None,
      ),
      patch(
        "robosystems.middleware.rate_limits.graph_tier_resolver.resolve_graph_tier",
        return_value="ladybug-standard",
      ),
    ):
      check.return_value = (True, 29)
      subscription_aware_rate_limit_dependency(oauth_request)

    limit, window = get_subscription_rate_limit(
      "ladybug-standard", EndpointCategory.GRAPH_MCP
    )
    check.assert_called_once_with("graph_sub:kg1a2b3c:graph_mcp", limit, window)
    assert oauth_request.state.rate_limit_tier == "ladybug-standard"

  def test_a_grant_on_another_graph_is_not_evidence_for_this_one(self, oauth_request):
    """The caller is known, so the budget is theirs — but a grant that names
    a different graph does not let the URL alone charge this graph's bucket."""
    with (
      patch(f"{self.MODULE}.rate_limit_cache.check_rate_limit") as check,
      patch(
        f"{self.MODULE}._cached_oauth_identity",
        return_value=("user_456", "kg9other"),
      ),
      patch(
        f"{self.MODULE}._verify_jwt_for_rate_limiting",
        return_value=None,
      ),
      patch(
        "robosystems.middleware.auth.cache.api_key_cache.get_cached_jwt_graph_access",
        return_value=None,
      ),
    ):
      check.return_value = (True, 29)
      subscription_aware_rate_limit_dependency(oauth_request)

    limit, window = get_subscription_rate_limit(
      "ladybug-standard", EndpointCategory.GRAPH_MCP
    )
    check.assert_called_once_with("user_sub:user_456:graph_mcp", limit, window)

  def test_an_uncached_oauth_bearer_is_one_anonymous_hit(self, oauth_request):
    """A token's first use is validated and cached by the auth dependency
    that runs next; until then it is counted as the IP's."""
    with (
      patch(f"{self.MODULE}.rate_limit_cache.check_rate_limit") as check,
      patch(f"{self.MODULE}._cached_oauth_identity", return_value=None),
      patch(f"{self.MODULE}._verify_jwt_for_rate_limiting", return_value=None),
    ):
      check.return_value = (True, 4)
      subscription_aware_rate_limit_dependency(oauth_request)

    limit, window = get_subscription_rate_limit("base", EndpointCategory.GRAPH_MCP)
    check.assert_called_once_with("anon_sub:192.168.1.1:graph_mcp", limit, window)
    assert oauth_request.state.rate_limit_tier == "base"
