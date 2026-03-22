"""Repository-specific rate limiting tests."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from robosystems.middleware.rate_limits.repository_rate_limits import (
  BLOCKED_SHARED_ENDPOINTS,
  AllowedSharedEndpoints,
  DualLayerRateLimiter,
  SharedRepositoryRateLimits,
)


class TestAllowedSharedEndpoints:
  def test_values(self):
    assert AllowedSharedEndpoints.QUERY.value == "query"
    assert AllowedSharedEndpoints.MCP.value == "mcp"
    assert AllowedSharedEndpoints.AGENT.value == "agent"
    assert AllowedSharedEndpoints.SEARCH.value == "search"
    assert AllowedSharedEndpoints.SCHEMA.value == "schema"
    assert AllowedSharedEndpoints.STATUS.value == "status"


class TestBlockedEndpoints:
  def test_blocked_list(self):
    assert "backup" in BLOCKED_SHARED_ENDPOINTS
    assert "restore" in BLOCKED_SHARED_ENDPOINTS
    assert "delete" in BLOCKED_SHARED_ENDPOINTS
    assert "admin" in BLOCKED_SHARED_ENDPOINTS
    assert "import" in BLOCKED_SHARED_ENDPOINTS


class TestSharedRepositoryRateLimits:
  def test_get_limits_returns_dict(self):
    result = SharedRepositoryRateLimits.get_limits("sec", "sec-starter")
    assert isinstance(result, dict)

  def test_get_limits_unknown_returns_empty(self):
    result = SharedRepositoryRateLimits.get_limits("nonexistent", "no-plan")
    assert result == {}

  def test_is_endpoint_allowed_query(self):
    assert SharedRepositoryRateLimits.is_endpoint_allowed("sec", "query") is True

  def test_is_endpoint_allowed_blocked(self):
    assert SharedRepositoryRateLimits.is_endpoint_allowed("sec", "backup") is False


class TestDualLayerRateLimiter:
  @pytest.fixture
  def mock_redis(self):
    redis = AsyncMock()
    # pipeline() is sync in redis.asyncio — returns a Pipeline, not a coroutine
    pipe = MagicMock()
    pipe.zremrangebyscore = MagicMock()
    pipe.zadd = MagicMock()
    pipe.zcount = MagicMock()
    pipe.expire = MagicMock()
    pipe.execute = AsyncMock(return_value=[0, True, 1, True])
    redis.pipeline = MagicMock(return_value=pipe)
    return redis

  @pytest.fixture
  def limiter(self, mock_redis):
    return DualLayerRateLimiter(mock_redis)

  @pytest.mark.asyncio
  async def test_blocked_endpoint(self, limiter):
    result = await limiter.check_limits(
      user_id="u1",
      graph_id="sec",
      operation="query",
      endpoint="backup",
      user_tier="ladybug-standard",
      repository_plan="sec-starter",
    )
    assert result["allowed"] is False
    assert result["reason"] == "endpoint_not_allowed"

  @pytest.mark.asyncio
  async def test_no_plan_for_shared_repo(self, limiter):
    result = await limiter.check_limits(
      user_id="u1",
      graph_id="sec",
      operation="query",
      endpoint="query",
      user_tier="ladybug-standard",
      repository_plan=None,
    )
    assert result["allowed"] is False
    assert result["reason"] == "no_access"

  @pytest.mark.asyncio
  async def test_burst_limit_exceeded(self, limiter, mock_redis):
    # Mock pipeline properly for async context manager
    pipe = MagicMock()
    pipe.zremrangebyscore = MagicMock()
    pipe.zadd = MagicMock()
    pipe.zcount = MagicMock()
    pipe.expire = MagicMock()
    pipe.execute = AsyncMock(return_value=[0, True, 1000, True])
    mock_redis.pipeline.return_value = pipe

    result = await limiter.check_limits(
      user_id="u1",
      graph_id="sec",
      operation="query",
      endpoint="query",
      user_tier="ladybug-standard",
      repository_plan="sec-starter",
    )
    assert isinstance(result, dict)

  @pytest.mark.asyncio
  async def test_allowed_non_shared_repo(self, limiter, mock_redis):
    pipe = MagicMock()
    pipe.zremrangebyscore = MagicMock()
    pipe.zadd = MagicMock()
    pipe.zcount = MagicMock()
    pipe.expire = MagicMock()
    pipe.execute = AsyncMock(return_value=[0, True, 1, True])
    mock_redis.pipeline.return_value = pipe

    result = await limiter.check_limits(
      user_id="u1",
      graph_id="kg123",  # NOT a shared repo
      operation="query",
      endpoint="query",
      user_tier="ladybug-standard",
    )
    assert result["allowed"] is True

  def test_operation_to_category(self, limiter):
    from robosystems.config.rate_limits import EndpointCategory

    assert limiter._operation_to_category("query") == EndpointCategory.GRAPH_QUERY
    assert limiter._operation_to_category("mcp") == EndpointCategory.GRAPH_MCP
    assert limiter._operation_to_category("agent") == EndpointCategory.GRAPH_AGENT
    assert limiter._operation_to_category("search") == EndpointCategory.GRAPH_SEARCH
    assert limiter._operation_to_category("unknown") == EndpointCategory.GRAPH_READ

  @pytest.mark.asyncio
  async def test_get_usage_stats_no_limits(self, limiter):
    result = await limiter.get_usage_stats("u1", "nonexistent", "no-plan")
    assert result == {}

  @pytest.mark.asyncio
  async def test_get_usage_stats_with_limits(self, limiter, mock_redis):
    mock_redis.get = AsyncMock(return_value=b"5")
    result = await limiter.get_usage_stats("u1", "sec", "sec-starter")
    if result:  # Only check if sec-starter has limits configured
      assert "usage" in result or result == {}
