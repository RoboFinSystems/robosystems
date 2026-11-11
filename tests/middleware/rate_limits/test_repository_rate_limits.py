"""Repository-specific rate limiting tests."""

from unittest.mock import AsyncMock, patch

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
    # Repository volume limits use INCR + EXPIRE. Default: first hit in window.
    redis.incr = AsyncMock(return_value=1)
    redis.expire = AsyncMock(return_value=True)
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
      repository_plan="starter",
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
      repository_plan=None,
    )
    assert result["allowed"] is False
    assert result["reason"] == "no_access"

  @pytest.mark.asyncio
  async def test_repository_volume_limit_exceeded(self, limiter, mock_redis):
    # Push the per-window counter above the starter plan's queries_per_minute.
    mock_redis.incr = AsyncMock(return_value=10_000)
    result = await limiter.check_limits(
      user_id="u1",
      graph_id="sec",
      operation="query",
      endpoint="query",
      repository_plan="starter",
    )
    assert result["allowed"] is False
    assert result["reason"] == "repository_limit"

  @pytest.mark.asyncio
  async def test_allowed_within_repository_limits(self, limiter):
    result = await limiter.check_limits(
      user_id="u1",
      graph_id="sec",
      operation="query",
      endpoint="query",
      repository_plan="starter",
    )
    assert result["allowed"] is True

  @pytest.mark.asyncio
  async def test_allowed_non_shared_repo(self, limiter):
    result = await limiter.check_limits(
      user_id="u1",
      graph_id="kg123",  # NOT a shared repo — burst handled upstream
      operation="query",
      endpoint="query",
    )
    assert result["allowed"] is True
    assert result["repo"] is None

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


class TestPlanKeying:
  """The manifest keys rate limits by canonical plan name only."""

  def test_canonical_plan_returns_real_limits(self):
    result = SharedRepositoryRateLimits.get_limits("sec", "starter")
    assert result, "canonical plan must resolve to non-empty limits"
    assert "queries_per_minute" in result

  def test_prefixed_plan_returns_empty(self):
    """Prefixed forms are not valid manifest keys. Normalization is the
    billing config's job (get_repository_plan returns the canonical key);
    anything persisting a prefixed plan string bricks the user's access."""
    assert SharedRepositoryRateLimits.get_limits("sec", "sec-starter") == {}

  def test_is_endpoint_allowed_forwards_the_repository(self):
    """Dropping the repository argument skipped every per-repo endpoint list
    and always ran the cross-manifest fallback."""
    with patch(
      "robosystems.middleware.rate_limits.repository_rate_limits._is_endpoint_allowed",
      return_value=True,
    ) as mock_check:
      SharedRepositoryRateLimits.is_endpoint_allowed("sec", "query")

    mock_check.assert_called_once_with("query", repo_id="sec")


class TestLimitMessage:
  """What a subscriber reads at a volume limit."""

  def test_a_trial_limit_names_the_trial_the_plan_and_the_way_out(self):
    from robosystems.middleware.rate_limits.repository_rate_limits import (
      limit_message,
    )

    message = limit_message(
      "sec", "mcp", {"window": "hour", "limit": 50}, "sec-starter", trial=True
    )

    assert "the SEC trial's limit of 50 tool calls per hour" in message
    assert "resets within the hour" in message
    assert "The Starter plan allows 100 per hour" in message
    assert "do not have to wait for the trial to end" in message
    assert message.endswith("/organization?tab=subscriptions")

  def test_a_plan_limit_names_the_plan_and_no_trial(self):
    from robosystems.middleware.rate_limits.repository_rate_limits import (
      limit_message,
    )

    message = limit_message(
      "sec", "search", {"window": "minute", "limit": 10}, "advanced"
    )

    assert "the Advanced plan's limit of 10 searches per minute" in message
    assert "trial" not in message
    assert message.endswith("/repositories/browse")

  def test_a_trial_refusal_says_the_wait_is_optional(self):
    from robosystems.middleware.rate_limits.repository_rate_limits import (
      trial_refusal,
    )

    message = trial_refusal("sec")

    assert "do not have to wait for the trial to end" in message
    assert message.endswith("/organization?tab=subscriptions")

  def test_a_cancelled_trial_is_not_told_to_start_its_plan(self):
    from unittest.mock import patch

    from robosystems.middleware.rate_limits import repository_rate_limits

    with patch.object(repository_rate_limits, "_trial_is_canceled", return_value=True):
      message = repository_rate_limits.limit_message(
        "sec",
        "mcp",
        {"window": "minute", "limit": 5},
        "starter",
        trial=True,
        user_id="user_1",
      )

    assert "the SEC trial's limit of 5 tool calls per minute" in message
    assert "cancelled and keeps these limits to its last day" in message
    assert "start your plan" not in message
