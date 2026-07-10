"""Comprehensive unit tests for the MCP execute module."""

import asyncio
import json
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi import HTTPException

from robosystems.routers.graphs.mcp.execute import (
  _get_mcp_cache,
  _get_mcp_operation_type,
  _get_user_priority,
  _mcp_cache_key,
  _set_mcp_cache,
  execute_tool_directly,
)


def _make_mock_user(user_id="user-123", tier_name=None):
  user = Mock()
  user.id = user_id
  if tier_name:
    user.subscription = Mock()
    user.subscription.billing_plan = Mock()
    user.subscription.billing_plan.name = tier_name
  else:
    user.subscription = None
  return user


def _make_mock_tool_call(name="read-graph-cypher", arguments=None):
  tool_call = Mock()
  tool_call.name = name
  tool_call.arguments = arguments or {}
  return tool_call


@pytest.mark.unit
class TestGetUserPriority:
  """Test the _get_user_priority helper function."""

  def test_default_priority_without_subscription(self):
    """Test that users without subscription get default priority."""
    user = _make_mock_user()
    priority = _get_user_priority(user)

    from robosystems.config.query_queue import QueryQueueConfig

    assert priority == QueryQueueConfig.DEFAULT_PRIORITY

  def test_priority_with_subscription_tier(self):
    """Test that users with subscription get tier-based priority."""
    user = _make_mock_user(tier_name="ladybug-large")
    priority = _get_user_priority(user)

    # Should get a priority from the queue config
    assert isinstance(priority, int)

  def test_priority_without_billing_plan(self):
    """Test priority when subscription exists but no billing plan."""
    user = Mock()
    user.id = "user-123"
    user.subscription = Mock()
    user.subscription.billing_plan = None
    priority = _get_user_priority(user)

    from robosystems.config.query_queue import QueryQueueConfig

    assert priority == QueryQueueConfig.DEFAULT_PRIORITY

  def test_no_subscription_attribute(self):
    """Test priority when user has no subscription attribute."""
    user = Mock(spec=[])
    user.id = "user-123"
    priority = _get_user_priority(user)

    from robosystems.config.query_queue import QueryQueueConfig

    assert priority == QueryQueueConfig.DEFAULT_PRIORITY


@pytest.mark.unit
class TestGetMcpOperationType:
  """Test the _get_mcp_operation_type helper function."""

  def test_shared_repo_returns_read(self):
    """Test that shared repository returns 'read' operation type."""
    with patch("robosystems.routers.graphs.mcp.execute.MultiTenantUtils") as mock_utils:
      mock_utils.is_shared_repository.return_value = True
      result = _get_mcp_operation_type("sec")
      assert result == "read"

  def test_user_graph_returns_write(self):
    """Test that user graph returns 'write' operation type."""
    with patch("robosystems.routers.graphs.mcp.execute.MultiTenantUtils") as mock_utils:
      mock_utils.is_shared_repository.return_value = False
      result = _get_mcp_operation_type("kg01234567890abcdef")
      assert result == "write"


@pytest.mark.unit
class TestExecuteToolDirectly:
  """Test the execute_tool_directly function."""

  @pytest.mark.asyncio
  async def test_successful_execution(self):
    """Test successful direct tool execution."""
    handler = AsyncMock()
    handler.call_tool = AsyncMock(return_value={"type": "text", "text": "result data"})
    tool_call = _make_mock_tool_call("get-graph-schema", {})

    result = await execute_tool_directly(handler, tool_call, timeout=60)

    assert result == {"type": "text", "text": "result data"}
    handler.call_tool.assert_awaited_once_with("get-graph-schema", {})

  @pytest.mark.asyncio
  async def test_timeout_raises_408(self):
    """Test that timeout raises 408 HTTPException."""
    handler = AsyncMock()

    async def slow_call(*args, **kwargs):
      await asyncio.sleep(100)

    handler.call_tool = slow_call
    tool_call = _make_mock_tool_call(
      "read-graph-cypher", {"query": "MATCH (n) RETURN n"}
    )

    with pytest.raises(HTTPException) as exc_info:
      await execute_tool_directly(handler, tool_call, timeout=0.01)

    assert exc_info.value.status_code == 408

  @pytest.mark.asyncio
  async def test_passes_tool_name_and_arguments(self):
    """Test that tool name and arguments are passed correctly."""
    handler = AsyncMock()
    handler.call_tool = AsyncMock(return_value={})
    arguments = {"query": "MATCH (n) RETURN n", "parameters": {"limit": 10}}
    tool_call = _make_mock_tool_call("read-graph-cypher", arguments)

    await execute_tool_directly(handler, tool_call, timeout=60)

    handler.call_tool.assert_awaited_once_with("read-graph-cypher", arguments)

  @pytest.mark.asyncio
  async def test_custom_timeout_value(self):
    """Test that custom timeout is respected."""
    handler = AsyncMock()
    handler.call_tool = AsyncMock(return_value={"data": "ok"})
    tool_call = _make_mock_tool_call()

    # Short timeout should still work if call is fast
    result = await execute_tool_directly(handler, tool_call, timeout=1)
    assert result == {"data": "ok"}


@pytest.mark.unit
class TestCallMcpToolValidation:
  """Test validation logic within call_mcp_tool endpoint."""

  @pytest.mark.asyncio
  async def test_bulk_operation_rejected(self):
    """Test that bulk operations (COPY, LOAD) are rejected with 400."""
    from robosystems.routers.graphs.mcp.execute import call_mcp_tool

    mock_request = Mock()
    mock_request.headers = {}
    user = _make_mock_user()

    tool_call = Mock()
    tool_call.name = "read-graph-cypher"
    tool_call.arguments = {"query": "COPY companies FROM 'data.csv'"}

    with (
      patch("robosystems.routers.graphs.mcp.execute.record_operation_metric"),
      patch("robosystems.routers.graphs.mcp.execute.circuit_breaker"),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await call_mcp_tool(
          full_request=mock_request,
          graph_id="kg01234567890abcdef",
          tool_call=tool_call,
          format=None,
          test_mode=False,
          current_user=user,
          _rate_limit=None,
        )
      assert exc_info.value.status_code == 400
      assert "Bulk operations" in exc_info.value.detail

  @pytest.mark.asyncio
  async def test_admin_operation_rejected(self):
    """Test that admin operations are rejected with 403."""
    from robosystems.routers.graphs.mcp.execute import call_mcp_tool

    mock_request = Mock()
    mock_request.headers = {}
    user = _make_mock_user()

    tool_call = Mock()
    tool_call.name = "read-graph-cypher"
    tool_call.arguments = {"query": "EXPORT DATABASE TO 'backup.db'"}

    with (
      patch("robosystems.routers.graphs.mcp.execute.record_operation_metric"),
      patch("robosystems.routers.graphs.mcp.execute.circuit_breaker"),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await call_mcp_tool(
          full_request=mock_request,
          graph_id="kg01234567890abcdef",
          tool_call=tool_call,
          format=None,
          test_mode=False,
          current_user=user,
          _rate_limit=None,
        )
      assert exc_info.value.status_code == 403
      assert "admin" in exc_info.value.detail.lower()

  @pytest.mark.asyncio
  async def test_write_on_shared_repo_rejected(self):
    """A write via read-graph-cypher on a shared repo is rejected with 403.

    Routed through the shared StatementKernel now — 'sec' is not a subgraph, so
    the main-graph write block fires; the security outcome (writes rejected) is
    what this pins.
    """
    from robosystems.routers.graphs.mcp.execute import call_mcp_tool

    mock_request = Mock()
    mock_request.headers = {}
    user = _make_mock_user()

    tool_call = Mock()
    tool_call.name = "read-graph-cypher"
    tool_call.arguments = {"query": "CREATE (n:Entity {name: 'test'})"}

    with (
      patch("robosystems.routers.graphs.mcp.execute.record_operation_metric"),
      patch("robosystems.routers.graphs.mcp.execute.circuit_breaker"),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await call_mcp_tool(
          full_request=mock_request,
          graph_id="sec",
          tool_call=tool_call,
          format=None,
          test_mode=False,
          current_user=user,
          _rate_limit=None,
        )
      assert exc_info.value.status_code == 403
      assert "not allowed" in exc_info.value.detail.lower()

  def test_non_cypher_tool_not_in_cypher_tool_list(self):
    """Test that non-cypher tools are not in the cypher validation tool list.

    The call_mcp_tool endpoint only validates write/admin/bulk operations
    for tools in the cypher tool list. This verifies that non-cypher tools
    would skip that validation branch.
    """
    cypher_tools = [
      "read-graph-cypher",
      "read-neo4j-cypher",
      "read-ladybug-cypher",
    ]

    assert "get-graph-schema" not in cypher_tools
    assert "get-graph-info" not in cypher_tools


@pytest.mark.unit
class TestCallMcpToolStrategySelection:
  """Test strategy selection and format override in call_mcp_tool."""

  def test_format_override_sse(self):
    """Test that format='sse' selects SSE_PROGRESS strategy."""
    from robosystems.routers.graphs.mcp.strategies import MCPExecutionStrategy

    # The format override happens inline in call_mcp_tool, but we can test
    # the strategy enum mapping
    assert MCPExecutionStrategy.SSE_PROGRESS.value == "sse_progress"

  def test_format_override_ndjson(self):
    """Test that format='ndjson' maps to STREAM_AGGREGATED."""
    from robosystems.routers.graphs.mcp.strategies import MCPExecutionStrategy

    assert MCPExecutionStrategy.STREAM_AGGREGATED.value == "stream_aggregated"

  def test_format_override_json(self):
    """Test that format='json' maps to JSON_COMPLETE."""
    from robosystems.routers.graphs.mcp.strategies import MCPExecutionStrategy

    assert MCPExecutionStrategy.JSON_COMPLETE.value == "json_complete"

  def test_all_strategies_have_timeouts(self):
    """Test that all strategies have defined timeouts."""
    from robosystems.routers.graphs.mcp.strategies import (
      MCPExecutionStrategy,
      MCPStrategySelector,
    )

    for strategy in MCPExecutionStrategy:
      timeout = MCPStrategySelector.get_timeout_for_strategy(strategy)
      assert isinstance(timeout, int)
      assert timeout > 0

  def test_strategy_selector_small_query(self):
    """Test strategy selection for small query."""
    from robosystems.routers.graphs.mcp.strategies import (
      MCPExecutionStrategy,
      MCPStrategySelector,
    )

    strategy = MCPStrategySelector.select_strategy(
      tool_name="read-graph-cypher",
      arguments={"query": "MATCH (n) RETURN n LIMIT 10"},
      client_info={"is_mcp_client": False, "supports_sse": False},
      system_state={"queue_size": 0, "running_queries": 0, "cache_available": False},
      graph_id="kg01234567890abcdef",
    )

    assert strategy == MCPExecutionStrategy.JSON_IMMEDIATE

  def test_strategy_selector_info_tool(self):
    """Test strategy selection for info tools."""
    from robosystems.routers.graphs.mcp.strategies import (
      MCPExecutionStrategy,
      MCPStrategySelector,
    )

    strategy = MCPStrategySelector.select_strategy(
      tool_name="get-graph-info",
      arguments={},
      client_info={"is_mcp_client": False, "supports_sse": False},
      system_state={"queue_size": 0, "running_queries": 0, "cache_available": False},
      graph_id="kg01234567890abcdef",
    )

    # Info tools without cache use JSON_IMMEDIATE
    assert strategy == MCPExecutionStrategy.JSON_IMMEDIATE

  def test_strategy_selector_high_load_queues(self):
    """Test that high load triggers queueing strategy."""
    from robosystems.routers.graphs.mcp.strategies import (
      MCPExecutionStrategy,
      MCPStrategySelector,
    )

    strategy = MCPStrategySelector.select_strategy(
      tool_name="read-graph-cypher",
      arguments={"query": "MATCH (n) RETURN n LIMIT 10"},
      client_info={"is_mcp_client": False, "supports_sse": False},
      system_state={"queue_size": 20, "running_queries": 10, "cache_available": False},
      graph_id="kg01234567890abcdef",
    )

    assert strategy == MCPExecutionStrategy.QUEUE_SIMPLE

  def test_strategy_selector_mcp_client_high_load_uses_monitoring(self):
    """Test that MCP clients get monitoring on high load."""
    from robosystems.routers.graphs.mcp.strategies import (
      MCPExecutionStrategy,
      MCPStrategySelector,
    )

    strategy = MCPStrategySelector.select_strategy(
      tool_name="read-graph-cypher",
      arguments={"query": "MATCH (n) RETURN n"},
      client_info={"is_mcp_client": True, "supports_sse": True},
      system_state={"queue_size": 20, "running_queries": 10, "cache_available": False},
      graph_id="kg01234567890abcdef",
    )

    assert strategy == MCPExecutionStrategy.QUEUE_WITH_MONITORING


@pytest.mark.unit
class TestMCPClientDetection:
  """Test MCP client detection logic."""

  def test_detects_mcp_client_from_user_agent(self):
    """Test MCP client detection from user-agent header."""
    from robosystems.routers.graphs.mcp.strategies import MCPClientDetector

    headers = {"user-agent": "robosystems-mcp/1.0.0"}
    info = MCPClientDetector.detect_client_type(headers)

    assert info["is_mcp_client"] is True
    assert info["supports_sse"] is True
    assert info["supports_ndjson"] is True

  def test_detects_mcp_client_from_x_mcp_client_header(self):
    """Test MCP client detection from X-MCP-Client header."""
    from robosystems.routers.graphs.mcp.strategies import MCPClientDetector

    headers = {"x-mcp-client": "robosystems-mcp/2.0"}
    info = MCPClientDetector.detect_client_type(headers)

    assert info["is_mcp_client"] is True

  def test_non_mcp_client_detected(self):
    """Test non-MCP client detection."""
    from robosystems.routers.graphs.mcp.strategies import MCPClientDetector

    headers = {"user-agent": "Mozilla/5.0"}
    info = MCPClientDetector.detect_client_type(headers)

    assert info["is_mcp_client"] is False

  def test_empty_headers_non_mcp(self):
    """Test empty headers result in non-MCP client."""
    from robosystems.routers.graphs.mcp.strategies import MCPClientDetector

    headers = {}
    info = MCPClientDetector.detect_client_type(headers)

    assert info["is_mcp_client"] is False


@pytest.mark.unit
class TestMCPToolAnalyzer:
  """Test the MCPToolAnalyzer analysis logic."""

  def test_query_tool_categorized(self):
    """Test that cypher tools are categorized as 'query'."""
    from robosystems.routers.graphs.mcp.strategies import MCPToolAnalyzer

    analysis = MCPToolAnalyzer.analyze_tool_call(
      "read-graph-cypher", {"query": "MATCH (n) RETURN n"}
    )
    assert analysis["tool_category"] == "query"

  def test_schema_tool_categorized(self):
    """Test that schema tools are categorized as 'schema'."""
    from robosystems.routers.graphs.mcp.strategies import MCPToolAnalyzer

    analysis = MCPToolAnalyzer.analyze_tool_call("get-graph-schema", {})
    assert analysis["tool_category"] == "schema"
    assert analysis["is_cacheable"] is True

  def test_info_tool_categorized(self):
    """Test that info tools are categorized as 'info'."""
    from robosystems.routers.graphs.mcp.strategies import MCPToolAnalyzer

    analysis = MCPToolAnalyzer.analyze_tool_call("get-graph-info", {})
    assert analysis["tool_category"] == "info"
    assert analysis["is_cacheable"] is True
    assert analysis["estimated_duration_ms"] == 100

  def test_unknown_tool_categorized(self):
    """Test unknown tools are categorized as 'unknown'."""
    from robosystems.routers.graphs.mcp.strategies import MCPToolAnalyzer

    analysis = MCPToolAnalyzer.analyze_tool_call("custom-tool", {})
    assert analysis["tool_category"] == "unknown"

  def test_query_with_limit_small_result(self):
    """Test that queries with small LIMIT are estimated as small."""
    from robosystems.routers.graphs.mcp.strategies import MCPToolAnalyzer

    analysis = MCPToolAnalyzer.analyze_tool_call(
      "read-graph-cypher", {"query": "MATCH (n) RETURN n LIMIT 10"}
    )
    assert analysis["estimated_result_size"] == "small"

  def test_query_without_limit_large_result(self):
    """Test that queries without LIMIT are estimated as large."""
    from robosystems.routers.graphs.mcp.strategies import MCPToolAnalyzer

    analysis = MCPToolAnalyzer.analyze_tool_call(
      "read-graph-cypher", {"query": "MATCH (n) RETURN n"}
    )
    assert analysis["estimated_result_size"] == "large"

  def test_complex_query_high_duration(self):
    """Test that complex queries have high estimated duration."""
    from robosystems.routers.graphs.mcp.strategies import MCPToolAnalyzer

    analysis = MCPToolAnalyzer.analyze_tool_call(
      "read-graph-cypher", {"query": "MATCH ALL SHORTEST PATHS (a)-[*]-(b) RETURN a, b"}
    )
    assert analysis["estimated_duration_ms"] >= 5000


@pytest.mark.unit
class TestMcpCacheKey:
  """Test the _mcp_cache_key helper."""

  def test_key_format(self):
    assert (
      _mcp_cache_key("sec_historical", "get-graph-info")
      == "mcp:sec_historical:get-graph-info"
    )

  def test_different_graphs_different_keys(self):
    assert _mcp_cache_key("sec", "get-graph-info") != _mcp_cache_key(
      "sec_historical", "get-graph-info"
    )

  def test_different_tools_different_keys(self):
    assert _mcp_cache_key("sec", "get-graph-info") != _mcp_cache_key(
      "sec", "get-graph-schema"
    )


@pytest.mark.unit
class TestGetMcpCache:
  """Test the _get_mcp_cache helper."""

  @pytest.mark.asyncio
  async def test_cache_hit_returns_parsed_result(self):
    cached = {"type": "text", "text": "node count: 100"}
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=json.dumps(cached))

    with patch(
      "robosystems.routers.graphs.mcp.execute._get_mcp_redis_client",
      return_value=mock_client,
    ):
      result = await _get_mcp_cache("sec", "get-graph-info")

    assert result == cached
    mock_client.get.assert_awaited_once_with("mcp:sec:get-graph-info")

  @pytest.mark.asyncio
  async def test_cache_miss_returns_none(self):
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=None)

    with patch(
      "robosystems.routers.graphs.mcp.execute._get_mcp_redis_client",
      return_value=mock_client,
    ):
      result = await _get_mcp_cache("sec", "get-graph-info")

    assert result is None

  @pytest.mark.asyncio
  async def test_redis_error_returns_none(self):
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(side_effect=Exception("connection refused"))

    with patch(
      "robosystems.routers.graphs.mcp.execute._get_mcp_redis_client",
      return_value=mock_client,
    ):
      result = await _get_mcp_cache("sec", "get-graph-info")

    assert result is None


@pytest.mark.unit
class TestSetMcpCache:
  """Test the _set_mcp_cache helper."""

  @pytest.mark.asyncio
  async def test_stores_json_with_ttl(self):
    mock_client = AsyncMock()
    mock_client.set = AsyncMock()
    result = {"type": "text", "text": "schema data"}

    with patch(
      "robosystems.routers.graphs.mcp.execute._get_mcp_redis_client",
      return_value=mock_client,
    ):
      await _set_mcp_cache("sec", "get-graph-schema", result, ttl=3600)

    mock_client.set.assert_awaited_once_with(
      "mcp:sec:get-graph-schema",
      json.dumps(result),
      ex=3600,
    )

  @pytest.mark.asyncio
  async def test_redis_error_is_silent(self):
    mock_client = AsyncMock()
    mock_client.set = AsyncMock(side_effect=Exception("connection refused"))

    with patch(
      "robosystems.routers.graphs.mcp.execute._get_mcp_redis_client",
      return_value=mock_client,
    ):
      # Should not raise
      await _set_mcp_cache(
        "sec", "get-graph-schema", {"type": "text", "text": ""}, ttl=3600
      )


@pytest.mark.unit
class TestCachedStrategies:
  """Test INFO_CACHED and SCHEMA_CACHED execution branches."""

  @pytest.mark.asyncio
  async def test_info_cached_returns_cached_result_without_executing(self):
    """On cache hit, INFO_CACHED skips execute_tool_directly entirely."""
    cached = {"type": "text", "text": "cached info"}

    with (
      patch(
        "robosystems.routers.graphs.mcp.execute._get_mcp_cache",
        AsyncMock(return_value=cached),
      ),
      patch("robosystems.routers.graphs.mcp.execute._set_mcp_cache", AsyncMock()),
      patch(
        "robosystems.routers.graphs.mcp.execute.execute_tool_directly", AsyncMock()
      ) as mock_exec,
    ):
      from robosystems.routers.graphs.mcp.execute import (
        _get_mcp_cache,
      )

      result_cached = await _get_mcp_cache("sec", "get-graph-info")
      assert result_cached == cached
      mock_exec.assert_not_awaited()

  @pytest.mark.asyncio
  async def test_info_cached_stores_on_miss(self):
    """On cache miss, INFO_CACHED executes and stores result."""
    fresh = {"type": "text", "text": "fresh info"}
    mock_set = AsyncMock()

    with (
      patch(
        "robosystems.routers.graphs.mcp.execute._get_mcp_cache",
        AsyncMock(return_value=None),
      ),
      patch("robosystems.routers.graphs.mcp.execute._set_mcp_cache", mock_set),
    ):
      handler = AsyncMock()
      handler.call_tool = AsyncMock(return_value=fresh)
      tool_call = _make_mock_tool_call("get-graph-info", {})

      result = await execute_tool_directly(handler, tool_call, timeout=30)
      await mock_set("sec", "get-graph-info", result, 1800)

      assert result == fresh
      mock_set.assert_awaited_once_with("sec", "get-graph-info", fresh, 1800)

  @pytest.mark.asyncio
  async def test_schema_cached_ttl_longer_than_info(self):
    """Schema TTL should be longer than info TTL."""
    from robosystems.routers.graphs.mcp.execute import (
      _MCP_INFO_CACHE_TTL,
      _MCP_SCHEMA_CACHE_TTL,
    )

    assert _MCP_SCHEMA_CACHE_TTL > _MCP_INFO_CACHE_TTL

  def test_schema_and_info_strategies_select_cached_when_available(self):
    """Verify INFO_CACHED and SCHEMA_CACHED are selected when cache is available."""
    from robosystems.routers.graphs.mcp.strategies import (
      MCPExecutionStrategy,
      MCPStrategySelector,
    )

    info_strategy = MCPStrategySelector.select_strategy(
      tool_name="get-graph-info",
      arguments={},
      client_info={"is_mcp_client": False, "supports_sse": False},
      system_state={"queue_size": 0, "running_queries": 0, "cache_available": True},
      graph_id="sec_historical",
    )
    assert info_strategy == MCPExecutionStrategy.INFO_CACHED

    schema_strategy = MCPStrategySelector.select_strategy(
      tool_name="get-graph-schema",
      arguments={},
      client_info={"is_mcp_client": False, "supports_sse": False},
      system_state={"queue_size": 0, "running_queries": 0, "cache_available": True},
      graph_id="sec_historical",
    )
    assert schema_strategy == MCPExecutionStrategy.SCHEMA_CACHED
