"""Tests for graph-scoped rate limit tier resolution.

The limiter previously pinned every authenticated user to ladybug-standard and
bucketed by user, so the large/xlarge tables were unreachable and a customer
with ten graphs shared one budget. These cover the resolution that replaced it,
weighted toward the failure modes — resolution must never fail *open*.
"""

from unittest.mock import MagicMock, patch

import pytest

from robosystems.config.rate_limits import EndpointCategory, RateLimitConfig
from robosystems.middleware.rate_limits.graph_tier_resolver import (
  FALLBACK_TIER,
  extract_graph_id,
  resolve_graph_tier,
)


class TestExtractGraphId:
  @pytest.mark.parametrize(
    "path,expected",
    [
      ("/v1/graphs/kg1abc/query/cypher", "kg1abc"),
      ("/v1/graphs/kg1abc/mcp/call-tool", "kg1abc"),
      ("/extensions/kg1abc/graphql", "kg1abc"),
      ("/extensions/roboledger/kg1abc/operations/close-period", "kg1abc"),
      ("/extensions/roboinvestor/kg1abc/operations/create-security", "kg1abc"),
      # Not graph-scoped
      ("/v1/user/subscription", None),
      ("/v1/status", None),
      ("/v1/auth/login", None),
      # Carries no graph id despite sitting under /graphs
      ("/v1/graphs/schema/validate", None),
    ],
  )
  def test_extraction(self, path, expected):
    assert extract_graph_id(path) == expected


class TestResolveGraphTier:
  """Every failure path must land on the tightest tier, never the loosest."""

  @patch("robosystems.middleware.rate_limits.graph_tier_resolver._store_tier")
  @patch("robosystems.middleware.rate_limits.graph_tier_resolver._cached_tier")
  def test_returns_the_graphs_tier(self, mock_cached, _mock_store):
    mock_cached.return_value = None
    graph = MagicMock()
    graph.graph_tier = "ladybug-xlarge"

    with patch("robosystems.models.core.graph.Graph.get_by_id", return_value=graph):
      with patch("robosystems.database.session", return_value=MagicMock()):
        assert resolve_graph_tier("kg1abc") == "ladybug-xlarge"

  @patch("robosystems.middleware.rate_limits.graph_tier_resolver._cached_tier")
  def test_cache_hit_skips_the_database(self, mock_cached):
    mock_cached.return_value = "ladybug-large"

    with patch("robosystems.models.core.graph.Graph.get_by_id") as mock_get:
      assert resolve_graph_tier("kg1abc") == "ladybug-large"
      mock_get.assert_not_called()

  @patch("robosystems.middleware.rate_limits.graph_tier_resolver._store_tier")
  @patch("robosystems.middleware.rate_limits.graph_tier_resolver._cached_tier")
  def test_unknown_graph_falls_back(self, mock_cached, _mock_store):
    mock_cached.return_value = None

    with patch("robosystems.models.core.graph.Graph.get_by_id", return_value=None):
      with patch("robosystems.database.session", return_value=MagicMock()):
        assert resolve_graph_tier("kg1missing") == FALLBACK_TIER

  @patch("robosystems.middleware.rate_limits.graph_tier_resolver._cached_tier")
  def test_database_failure_falls_back_rather_than_raising(self, mock_cached):
    """A lookup error must not 500 the request, nor grant more throughput."""
    mock_cached.return_value = None

    with patch("robosystems.database.session", side_effect=RuntimeError("db is down")):
      assert resolve_graph_tier("kg1abc") == FALLBACK_TIER

  def test_fallback_is_the_tightest_customer_tier(self):
    """Degrading must never hand out more than the cheapest tier allows."""
    fallback_limit = RateLimitConfig.get_rate_limit(
      FALLBACK_TIER, EndpointCategory.GRAPH_QUERY
    )
    for tier in ("ladybug-standard", "ladybug-large", "ladybug-xlarge"):
      tier_limit = RateLimitConfig.get_rate_limit(tier, EndpointCategory.GRAPH_QUERY)
      assert fallback_limit and tier_limit
      assert fallback_limit[0] <= tier_limit[0], (
        f"{FALLBACK_TIER} is not the tightest tier — {tier} allows less"
      )


class TestSharedRepositoryIsolation:
  """Shared repositories must stay user-bucketed, never graph-bucketed.

  `sec` is a single graph every tenant queries. Keying its bucket by graph id
  would put every customer in one budget, letting a single heavy user exhaust
  the limit for everyone — the opposite of what rate limiting is for. Per-graph
  bucketing is only safe when the graph belongs to one tenant.
  """

  def test_shared_repositories_and_their_subgraphs_are_recognised(self):
    from robosystems.config.shared_repositories import (
      is_shared_repository_or_subgraph,
    )

    assert is_shared_repository_or_subgraph("sec")
    assert is_shared_repository_or_subgraph("sec_historical")
    assert not is_shared_repository_or_subgraph("kg1a2b3c")

  def test_shared_repository_query_buckets_by_user(self):
    """Two users on `sec` must not share a bucket."""
    from robosystems.middleware.rate_limits.rate_limiting import (
      subscription_aware_rate_limit_dependency,
    )

    seen = []

    def record(identifier, limit, window):
      seen.append(identifier)
      return (True, 100)

    request = MagicMock()
    request.url.path = "/v1/graphs/sec/query/cypher"
    request.method = "POST"
    request.client.host = "1.2.3.4"
    request.state = MagicMock()
    request.headers = {}

    with patch(
      "robosystems.middleware.rate_limits.rate_limiting.rate_limit_cache.check_rate_limit",
      side_effect=record,
    ):
      with patch(
        "robosystems.middleware.rate_limits.rate_limiting.get_user_from_request",
        side_effect=["user_a", "user_b"],
      ):
        subscription_aware_rate_limit_dependency(request)
        subscription_aware_rate_limit_dependency(request)

    assert seen == [
      "user_sub:user_a:graph_query",
      "user_sub:user_b:graph_query",
    ], seen

  def test_tenant_graph_query_buckets_by_graph(self):
    """A tenant's own graphs each get their own budget."""
    from robosystems.middleware.rate_limits.rate_limiting import (
      subscription_aware_rate_limit_dependency,
    )

    seen = []

    def record(identifier, limit, window):
      seen.append(identifier)
      return (True, 100)

    request = MagicMock()
    request.method = "POST"
    request.client.host = "1.2.3.4"
    request.state = MagicMock()
    request.headers = {}

    with patch(
      "robosystems.middleware.rate_limits.rate_limiting.rate_limit_cache.check_rate_limit",
      side_effect=record,
    ):
      with patch(
        "robosystems.middleware.rate_limits.rate_limiting.get_user_from_request",
        return_value="user_a",
      ):
        with patch(
          "robosystems.middleware.rate_limits.graph_tier_resolver.resolve_graph_tier",
          return_value="ladybug-standard",
        ):
          for gid in ("kg1aaa", "kg1bbb"):
            request.url.path = f"/v1/graphs/{gid}/query/cypher"
            subscription_aware_rate_limit_dependency(request)

    assert seen == [
      "graph_sub:kg1aaa:graph_query",
      "graph_sub:kg1bbb:graph_query",
    ], seen


class TestTierDifferentiation:
  def test_dedicated_categories_scale_with_vcpu(self):
    """1x / 2x / 4x, matching m7g.medium -> m7g.large -> r7g.xlarge."""
    for category in RateLimitConfig.DEDICATED_RESOURCE_CATEGORIES:
      standard = RateLimitConfig.get_rate_limit("ladybug-standard", category)
      assert standard, category
      for tier, factor in (("ladybug-large", 2), ("ladybug-xlarge", 4)):
        actual = RateLimitConfig.get_rate_limit(tier, category)
        assert actual and actual[0] == standard[0] * factor, (
          f"{tier}/{category.value} is {actual[0] if actual else None}, "
          f"expected {standard[0] * factor}"
        )

  def test_shared_resource_categories_stay_flat(self):
    """Shared infra must not scale, or one tenant can starve the rest."""
    shared = {
      EndpointCategory.GRAPH_SEARCH,  # OpenSearch t3.medium
      EndpointCategory.GRAPH_IMPORT,  # sequential in LadybugDB regardless
      EndpointCategory.EXTENSIONS_GRAPHQL,  # shared RDS
      EndpointCategory.EXTENSIONS_WRITE,
    }
    assert not (shared & RateLimitConfig.DEDICATED_RESOURCE_CATEGORIES)

    for category in shared:
      limits = {
        RateLimitConfig.get_rate_limit(t, category)[0]  # type: ignore[index]
        for t in ("ladybug-standard", "ladybug-large", "ladybug-xlarge")
      }
      assert len(limits) == 1, f"{category.value} differs across tiers: {limits}"

  def test_reported_multiplier_matches_enforcement(self):
    """The multiplier is derived, so it cannot drift from what is enforced."""
    from robosystems.config.graph_tier import GraphTierConfig

    for tier, expected in (
      ("ladybug-standard", 1.0),
      ("ladybug-large", 2.0),
      ("ladybug-xlarge", 4.0),
    ):
      assert GraphTierConfig.get_api_rate_multiplier(tier) == expected
