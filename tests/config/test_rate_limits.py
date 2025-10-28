import pytest

from robosystems.config.rate_limits import (
  EndpointCategory,
  RateLimitConfig,
  RateLimitPeriod,
)


def test_periods_convert_to_expected_seconds():
  assert RateLimitPeriod.MINUTE.to_seconds() == 60
  assert RateLimitPeriod.HOUR.to_seconds() == 3600
  assert RateLimitPeriod.DAY.to_seconds() == 86400


def test_unknown_tier_falls_back_to_free_limits():
  # pick category only present in free config
  limit, window = RateLimitConfig.get_rate_limit("mystery", EndpointCategory.AUTH)

  assert (
    limit == RateLimitConfig.SUBSCRIPTION_RATE_LIMITS["free"][EndpointCategory.AUTH][0]
  )
  assert window == RateLimitPeriod.MINUTE.to_seconds()


def test_rate_limit_with_multiplier_adjusts_value(monkeypatch):
  monkeypatch.setattr(
    "robosystems.config.rate_limits.get_tier_rate_limit_multiplier",
    lambda tier: 2.5,
  )

  adjusted_limit, window = RateLimitConfig.get_rate_limit_with_multiplier(
    "kuzu-standard", EndpointCategory.GRAPH_READ
  )

  base_limit = RateLimitConfig.SUBSCRIPTION_RATE_LIMITS["kuzu-standard"][
    EndpointCategory.GRAPH_READ
  ][0]
  assert adjusted_limit == int(base_limit * 2.5)
  assert window == RateLimitPeriod.MINUTE.to_seconds()


def test_multiplier_can_be_skipped(monkeypatch):
  monkeypatch.setattr(
    "robosystems.config.rate_limits.get_tier_rate_limit_multiplier",
    lambda tier: 99,
  )

  limit_without_multiplier, window = RateLimitConfig.get_rate_limit_with_multiplier(
    "kuzu-standard", EndpointCategory.GRAPH_READ, use_tier_config=False
  )

  base_limit, expected_window = RateLimitConfig.get_rate_limit(
    "kuzu-standard", EndpointCategory.GRAPH_READ
  )
  assert limit_without_multiplier == base_limit
  assert window == expected_window == RateLimitPeriod.MINUTE.to_seconds()


@pytest.mark.parametrize(
  "path,method,expected",
  [
    ("/v1/graphs/abc/tables/query", "POST", EndpointCategory.TABLE_QUERY),
    ("/v1/graphs/abc/tables/ingest", "POST", EndpointCategory.GRAPH_IMPORT),
    ("/v1/graphs/abc/tables/files", "POST", EndpointCategory.TABLE_UPLOAD),
    ("/v1/graphs/abc/tables/files", "GET", EndpointCategory.GRAPH_READ),
    ("/v1/graphs/abc/mcp/execute", "POST", EndpointCategory.GRAPH_MCP),
    ("/v1/graphs/abc/agent/run", "POST", EndpointCategory.GRAPH_AGENT),
    ("/v1/graphs/abc/graph/backup", "POST", EndpointCategory.GRAPH_BACKUP),
    ("/v1/graphs/abc/graph/query", "POST", EndpointCategory.GRAPH_QUERY),
    ("/v1/graphs/abc/graph/analytics", "GET", EndpointCategory.GRAPH_ANALYTICS),
    ("/v1/graphs/abc/sync/start", "POST", EndpointCategory.GRAPH_SYNC),
    ("/v1/graphs/abc/import", "POST", EndpointCategory.GRAPH_IMPORT),
    ("/v1/graphs/abc/custom", "POST", EndpointCategory.GRAPH_WRITE),
    ("/v1/graphs/abc/custom", "GET", EndpointCategory.GRAPH_READ),
    ("/v1/auth/login", "POST", EndpointCategory.AUTH),
    ("/v1/tasks/run", "POST", EndpointCategory.TASKS),
  ],
)
def test_endpoint_category_detection(path, method, expected):
  assert RateLimitConfig.get_endpoint_category(path, method) == expected


def test_endpoint_category_returns_none_when_unmatched():
  assert RateLimitConfig.get_endpoint_category("/v1/unknown/path", "GET") is None
