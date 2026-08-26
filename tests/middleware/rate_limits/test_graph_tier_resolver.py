"""Tests for graph-scoped rate limit tier resolution.

The limiter previously pinned every authenticated user to ladybug-standard and
bucketed by user, so the large/xlarge tables were unreachable and a customer
with ten graphs shared one budget. These cover the resolution that replaced it,
weighted toward the failure modes — resolution must never fail *open*.
"""

from types import SimpleNamespace
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
      ("/v1/graphs/kg1abc/mcp", "kg1abc"),
      ("/extensions/kg1abc/graphql", "kg1abc"),
      ("/extensions/roboledger/kg1abc/operations/close-period", "kg1abc"),
      ("/extensions/roboinvestor/kg1abc/operations/create-security", "kg1abc"),
      # Not graph-scoped
      ("/v1/user/subscription", None),
      ("/v1/status", None),
      ("/v1/auth/login", None),
      # Carries no graph id despite sitting under /graphs
      ("/v1/graphs/schema/validate", None),
      # Static routes whose second segment is not a graph id. Treating one as
      # a graph id would collapse every caller into a single shared bucket.
      ("/v1/graphs/tiers", None),
      ("/v1/graphs/capacity", None),
      ("/v1/graphs/extensions", None),
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

  @patch("robosystems.middleware.rate_limits.graph_tier_resolver._store_tier")
  @patch("robosystems.middleware.rate_limits.graph_tier_resolver._cached_tier")
  def test_tier_without_a_limits_table_falls_back(self, mock_cached, _mock_store):
    """A stored tier string with no SUBSCRIPTION_RATE_LIMITS entry — a legacy
    value or ladybug-shared — must floor at FALLBACK_TIER, not fall through to
    the anonymous base table."""
    mock_cached.return_value = None
    graph = MagicMock()
    graph.graph_tier = "kuzu-standard"

    with patch("robosystems.models.core.graph.Graph.get_by_id", return_value=graph):
      with patch("robosystems.database.session", return_value=MagicMock()):
        assert resolve_graph_tier("kg1legacy") == FALLBACK_TIER

  @patch("robosystems.middleware.rate_limits.graph_tier_resolver._cached_tier")
  def test_cached_tier_without_a_limits_table_falls_back(self, mock_cached):
    mock_cached.return_value = "ladybug-shared"

    with patch("robosystems.models.core.graph.Graph.get_by_id") as mock_get:
      assert resolve_graph_tier("kg1abc") == FALLBACK_TIER
      mock_get.assert_not_called()

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
            # The graph auth dependency ran and authorized this caller.
            request.state.auth_graph_id = gid
            subscription_aware_rate_limit_dependency(request)

    assert seen == [
      "graph_sub:kg1aaa:graph_query",
      "graph_sub:kg1bbb:graph_query",
    ], seen

  def test_subgraph_queries_draw_from_the_parents_budget(self):
    """kg…_dev lives on its parent's instance and is not separately priced,
    so it must share the parent's bucket — not mint its own."""
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

    parent = "kg0123456789abcdef"

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
        ) as mock_resolve:
          for gid in (parent, f"{parent}_dev"):
            request.url.path = f"/v1/graphs/{gid}/query/cypher"
            request.state.auth_graph_id = gid
            subscription_aware_rate_limit_dependency(request)

    assert seen == [f"graph_sub:{parent}:graph_query"] * 2, seen
    for call in mock_resolve.call_args_list:
      assert call.args[0] == parent


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
      # No limits table of their own — enforced at the standard fallback.
      ("ladybug-shared", 1.0),
      ("kuzu-standard", 1.0),
    ):
      assert GraphTierConfig.get_api_rate_multiplier(tier) == expected


class TestGraphBucketsAreMembersOnly:
  """A graph's budget is charged only by a caller authorized on it. The URL
  alone is not authorization: wherever the limiter runs ahead of the graph
  auth dependency, an anonymous or unrelated caller could otherwise drain a
  tenant's read/write budget with requests that all go on to 401."""

  @staticmethod
  def _request(path: str, headers: dict | None = None):
    request = MagicMock()
    request.method = "POST"
    request.client.host = "1.2.3.4"
    request.state = SimpleNamespace()  # nothing published: auth has not run
    request.headers = headers or {}
    request.url.path = path
    return request

  @staticmethod
  def _limit(request, seen):
    from robosystems.middleware.rate_limits.rate_limiting import (
      subscription_aware_rate_limit_dependency,
    )

    def record(identifier, limit, window):
      seen.append(identifier)
      return (True, 100)

    with patch(
      "robosystems.middleware.rate_limits.rate_limiting.rate_limit_cache.check_rate_limit",
      side_effect=record,
    ):
      with patch(
        "robosystems.middleware.rate_limits.graph_tier_resolver.resolve_graph_tier",
        return_value="ladybug-standard",
      ):
        subscription_aware_rate_limit_dependency(request)

  def test_an_unverified_api_key_is_not_an_identity(self):
    """Any string in X-API-Key used to mint a fresh per-key budget. Now a key
    the auth cache has never validated is anonymous, and anonymous requests
    share the IP bucket."""
    from robosystems.middleware.rate_limits.rate_limiting import get_user_from_request

    request = self._request(
      "/v1/graphs/kg1victim/memory/remember", {"X-API-Key": "junk"}
    )
    with patch(
      "robosystems.middleware.auth.cache.api_key_cache.get_cached_api_key_validation",
      return_value=None,
    ):
      assert get_user_from_request(request) is None
    with patch(
      "robosystems.middleware.auth.cache.api_key_cache.get_cached_api_key_validation",
      return_value={"user_id": "u1"},
    ):
      assert get_user_from_request(request).startswith("apikey_")

  def test_anonymous_requests_never_charge_the_graph_bucket(self):
    seen: list = []
    request = self._request(
      "/v1/graphs/kg1victim/memory/remember", {"X-API-Key": "junk"}
    )
    with patch(
      "robosystems.middleware.auth.cache.api_key_cache.get_cached_api_key_validation",
      return_value=None,
    ):
      self._limit(request, seen)
    assert seen and all(i.startswith("anon_sub:") for i in seen), seen

  def test_an_identified_but_unauthorized_caller_stays_on_their_own_budget(self):
    seen: list = []
    request = self._request("/v1/graphs/kg1victim/documents")
    with (
      patch(
        "robosystems.middleware.rate_limits.rate_limiting.get_user_from_request",
        return_value="user_attacker",
      ),
      patch(
        "robosystems.middleware.auth.cache.api_key_cache.get_cached_jwt_graph_access",
        return_value=None,
      ),
    ):
      self._limit(request, seen)
    assert seen == ["user_sub:user_attacker:graph_write"], seen

  def test_a_cached_positive_decision_is_enough_when_auth_has_not_run_yet(self):
    """Router-level mounting runs the limiter first; a member's earlier
    request left a positive decision in the auth cache, so the graph
    bucket applies without a second authorization round-trip."""
    seen: list = []
    request = self._request("/v1/graphs/kg1mine/documents")
    with (
      patch(
        "robosystems.middleware.rate_limits.rate_limiting.get_user_from_request",
        return_value="user_member",
      ),
      patch(
        "robosystems.middleware.auth.cache.api_key_cache.get_cached_jwt_graph_access",
        return_value=True,
      ),
    ):
      self._limit(request, seen)
    assert seen == ["graph_sub:kg1mine:graph_write"], seen

  def test_a_negative_cached_decision_does_not_unlock_the_graph_bucket(self):
    seen: list = []
    request = self._request("/v1/graphs/kg1victim/documents")
    with (
      patch(
        "robosystems.middleware.rate_limits.rate_limiting.get_user_from_request",
        return_value="apikey_" + "0" * 64,
      ),
      patch(
        "robosystems.middleware.auth.cache.api_key_cache.get_cached_graph_access",
        return_value=False,
      ),
    ):
      self._limit(request, seen)
    assert seen and seen[0].startswith("user_sub:apikey_"), seen
