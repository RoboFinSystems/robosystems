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


def test_unknown_tier_falls_back_to_base_limits():
  # pick category only present in base config
  limit, window = RateLimitConfig.get_rate_limit("mystery", EndpointCategory.AUTH)

  assert (
    limit == RateLimitConfig.SUBSCRIPTION_RATE_LIMITS["base"][EndpointCategory.AUTH][0]
  )
  assert window == RateLimitPeriod.MINUTE.to_seconds()


@pytest.mark.parametrize(
  "path,method,expected",
  [
    # SQL query layer — keeps the DuckDB/columnar bucket (former /tables/query)
    ("/v1/graphs/abc/query/sql", "POST", EndpointCategory.TABLE_QUERY),
    ("/v1/graphs/abc/tables/ingest", "POST", EndpointCategory.GRAPH_IMPORT),
    # New first-class files endpoints
    ("/v1/graphs/abc/files", "POST", EndpointCategory.TABLE_UPLOAD),
    ("/v1/graphs/abc/files", "GET", EndpointCategory.GRAPH_READ),
    ("/v1/graphs/abc/files/f123", "DELETE", EndpointCategory.TABLE_MANAGEMENT),
    ("/v1/graphs/abc/files/f123", "PATCH", EndpointCategory.TABLE_MANAGEMENT),
    ("/v1/graphs/abc/files/f123", "GET", EndpointCategory.GRAPH_READ),
    # Legacy table-nested files endpoints
    ("/v1/graphs/abc/tables/Entity/files", "POST", EndpointCategory.TABLE_UPLOAD),
    ("/v1/graphs/abc/tables/Entity/files", "GET", EndpointCategory.GRAPH_READ),
    ("/v1/graphs/abc/tables/files/f123", "DELETE", EndpointCategory.TABLE_MANAGEMENT),
    ("/v1/graphs/abc/tables/files/f123", "PATCH", EndpointCategory.TABLE_MANAGEMENT),
    # Other graph endpoints
    ("/v1/graphs/abc/mcp/execute", "POST", EndpointCategory.GRAPH_MCP),
    ("/v1/graphs/abc/operator/run", "POST", EndpointCategory.GRAPH_OPERATOR),
    # Search endpoints (OpenSearch)
    ("/v1/graphs/abc/search", "POST", EndpointCategory.GRAPH_SEARCH),
    ("/v1/graphs/abc/search/doc123", "GET", EndpointCategory.GRAPH_SEARCH),
    # Cypher query layer
    ("/v1/graphs/abc/query/cypher", "POST", EndpointCategory.GRAPH_QUERY),
    # Schema surface: info/export are graph-scoped reads; validate is
    # graph-independent (no graph_id) but still read-like.
    ("/v1/graphs/abc/schema", "GET", EndpointCategory.GRAPH_READ),
    ("/v1/graphs/schema/validate", "POST", EndpointCategory.GRAPH_READ),
    # Content metrics + consumption usage — dedicated bucket
    ("/v1/graphs/abc/metrics", "GET", EndpointCategory.GRAPH_ANALYTICS),
    ("/v1/graphs/abc/usage", "GET", EndpointCategory.GRAPH_ANALYTICS),
    # Backup list is a read; download too
    ("/v1/graphs/abc/backups", "GET", EndpointCategory.GRAPH_READ),
    # Graph lifecycle operations get dedicated buckets
    (
      "/v1/graphs/abc/operations/create-backup",
      "POST",
      EndpointCategory.GRAPH_BACKUP,
    ),
    ("/v1/graphs/abc/operations/materialize", "POST", EndpointCategory.GRAPH_IMPORT),
    (
      "/v1/graphs/abc/operations/create-subgraph",
      "POST",
      EndpointCategory.GRAPH_MANAGEMENT,
    ),
    (
      "/v1/graphs/abc/operations/change-tier",
      "POST",
      EndpointCategory.GRAPH_MANAGEMENT,
    ),
    # Connections: reads/writes are ordinary; only the sync trigger is GRAPH_SYNC
    ("/v1/graphs/abc/connections", "GET", EndpointCategory.GRAPH_READ),
    ("/v1/graphs/abc/connections", "POST", EndpointCategory.GRAPH_WRITE),
    ("/v1/graphs/abc/connections/options", "GET", EndpointCategory.GRAPH_READ),
    ("/v1/graphs/abc/connections/c1/sync", "POST", EndpointCategory.GRAPH_SYNC),
    ("/v1/graphs/abc/import", "POST", EndpointCategory.GRAPH_IMPORT),
    ("/v1/graphs/abc/custom", "POST", EndpointCategory.GRAPH_WRITE),
    ("/v1/graphs/abc/custom", "GET", EndpointCategory.GRAPH_READ),
    # Extensions surface (OLTP — distinct from LadybugDB graph categories)
    ("/extensions/kg123/graphql", "POST", EndpointCategory.EXTENSIONS_GRAPHQL),
    (
      "/extensions/roboledger/kg123/operations/create-event-block",
      "POST",
      EndpointCategory.EXTENSIONS_WRITE,
    ),
    (
      "/extensions/roboledger/kg123/operations/build-fact-grid",
      "POST",
      EndpointCategory.EXTENSIONS_WRITE,
    ),
    (
      "/extensions/roboledger/kg123/reports/rep_1/download",
      "GET",
      EndpointCategory.EXTENSIONS_WRITE,
    ),
    # Non-graph endpoints
    ("/v1/auth/login", "POST", EndpointCategory.AUTH),
    ("/v1/tasks/run", "POST", EndpointCategory.TASKS),
  ],
)
def test_endpoint_category_detection(path, method, expected):
  assert RateLimitConfig.get_endpoint_category(path, method) == expected


def test_endpoint_category_returns_none_when_unmatched():
  assert RateLimitConfig.get_endpoint_category("/v1/unknown/path", "GET") is None
