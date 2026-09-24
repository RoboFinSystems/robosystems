"""MCP execution strategy selection: picks immediate JSON, streaming, queued,
or cached execution from the tool type, request headers, and system load."""

import re
from enum import Enum
from typing import Any

from robosystems.middleware.graph.execution_strategies import (
  BaseAnalyzer,
  BaseStrategySelector,
)


class MCPExecutionStrategy(Enum):
  """MCP tool execution strategies optimized for AI agents."""

  JSON_IMMEDIATE = "json_immediate"
  JSON_COMPLETE = "json_complete"
  NDJSON_STREAMING = "ndjson_streaming"
  SSE_STREAMING = "sse_streaming"
  SSE_PROGRESS = "sse_progress"
  QUEUE_WITH_MONITORING = "queue_monitoring"
  QUEUE_SIMPLE = "queue_simple"
  CACHED = "cached"

  STREAM_AGGREGATED = "stream_aggregated"  # Stream and aggregate for agent
  SCHEMA_CACHED = "schema_cached"  # Use cached schema
  INFO_CACHED = "info_cached"  # Use cached graph info


class MCPToolAnalyzer(BaseAnalyzer):
  """Analyze MCP tool calls to determine optimal execution strategy."""

  QUERY_TOOLS = ["read-graph-cypher", "read-neo4j-cypher", "read-ladybug-cypher"]
  SCHEMA_TOOLS = ["get-graph-schema", "get-neo4j-schema", "get-ladybug-schema"]
  INFO_TOOLS = ["get-graph-info"]

  def analyze(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    """Implementation of the abstract `analyze` method."""
    return self.analyze_tool_call(tool_name, arguments)

  @classmethod
  def analyze_tool_call(
    cls, tool_name: str, arguments: dict[str, Any]
  ) -> dict[str, Any]:
    """Estimate the cost and shape of an MCP tool call."""
    analysis = {
      "tool_category": cls._get_tool_category(tool_name),
      "is_cacheable": cls._is_cacheable(tool_name),
      "estimated_duration_ms": cls._estimate_duration(tool_name, arguments),
      "estimated_result_size": cls._estimate_mcp_result_size(tool_name, arguments),
      "requires_streaming": False,
      "supports_progress": False,
    }

    if tool_name in cls.QUERY_TOOLS:
      query = arguments.get("query", "")
      query_analysis = cls.analyze_cypher_query(query)
      analysis.update(query_analysis)

    elif tool_name in cls.SCHEMA_TOOLS:
      analysis["supports_progress"] = True
      analysis["estimated_result_size"] = "large"  # Schemas can be large

    elif tool_name in cls.INFO_TOOLS:
      analysis["estimated_duration_ms"] = 100
      analysis["estimated_result_size"] = "small"

    return analysis

  @classmethod
  def _get_tool_category(cls, tool_name: str) -> str:
    """Categorize the tool for strategy selection."""
    if tool_name in cls.QUERY_TOOLS:
      return "query"
    elif tool_name in cls.SCHEMA_TOOLS:
      return "schema"
    elif tool_name in cls.INFO_TOOLS:
      return "info"
    else:
      return "unknown"

  @classmethod
  def _is_cacheable(cls, tool_name: str) -> bool:
    """Determine if tool results can be cached."""
    return tool_name in cls.SCHEMA_TOOLS or tool_name in cls.INFO_TOOLS

  @classmethod
  def _estimate_duration(cls, tool_name: str, arguments: dict[str, Any]) -> int:
    """Estimate execution duration in milliseconds."""
    if tool_name in cls.QUERY_TOOLS:
      query = arguments.get("query", "")
      if "SHORTEST" in query.upper() or "ALL" in query.upper():
        return 5000
      elif "MATCH" in query.upper():
        return 1000
      else:
        return 500
    elif tool_name in cls.SCHEMA_TOOLS:
      return 2000  # Schema retrieval is moderate
    else:
      return 100  # Info tools are fast

  @classmethod
  def _estimate_mcp_result_size(cls, tool_name: str, arguments: dict[str, Any]) -> str:
    """Estimate result size category: small, medium, large."""
    if tool_name in cls.QUERY_TOOLS:
      query = arguments.get("query", "")
      limit_match = re.search(r"LIMIT\s+(\d+)", query, re.IGNORECASE)
      if limit_match:
        limit = int(limit_match.group(1))
        if limit <= 100:
          return "small"
        elif limit <= 1000:
          return "medium"
        else:
          return "large"
      return "large"
    elif tool_name in cls.SCHEMA_TOOLS:
      return "large"  # Schemas can be extensive
    else:
      return "small"  # Info is typically compact


class MCPStrategySelector(BaseStrategySelector):
  """Select optimal execution strategy for MCP tools."""

  @classmethod
  def select_strategy(
    cls,
    tool_name: str,
    arguments: dict[str, Any],
    client_info: dict[str, Any],
    system_state: dict[str, Any],
    graph_id: str,
    user_tier: str | None = None,
  ) -> MCPExecutionStrategy:
    """Select the execution strategy for an MCP tool call."""
    analysis = MCPToolAnalyzer.analyze_tool_call(tool_name, arguments)

    is_mcp_client = client_info.get("is_mcp_client", False)

    cached_strategy = cls._select_cached_strategy(analysis, system_state)
    if cached_strategy:
      return cached_strategy

    high_load_strategy = cls._select_high_load_strategy(
      system_state, is_mcp_client, client_info
    )
    if high_load_strategy:
      return high_load_strategy

    if analysis["tool_category"] == "query":
      return cls._select_query_strategy(analysis, is_mcp_client, client_info)
    elif analysis["tool_category"] == "schema":
      return cls._select_schema_strategy()
    elif analysis["tool_category"] == "info":
      return MCPExecutionStrategy.JSON_IMMEDIATE

    return MCPExecutionStrategy.JSON_COMPLETE

  @classmethod
  def _select_cached_strategy(
    cls, analysis: dict[str, Any], system_state: dict[str, Any]
  ) -> MCPExecutionStrategy | None:
    """Select cached strategy if applicable."""
    if not (analysis["is_cacheable"] and system_state.get("cache_available")):
      return None

    if analysis["tool_category"] == "schema":
      return MCPExecutionStrategy.SCHEMA_CACHED
    elif analysis["tool_category"] == "info":
      return MCPExecutionStrategy.INFO_CACHED

    return None

  @classmethod
  def _select_high_load_strategy(
    cls, system_state: dict[str, Any], is_mcp_client: bool, client_info: dict[str, Any]
  ) -> MCPExecutionStrategy | None:
    """Select strategy for high system load."""
    queue_size = system_state.get("queue_size", 0)
    running_count = system_state.get("running_queries", 0)

    if queue_size > 10 or running_count > 5:
      # MCP clients get SSE monitoring for better UX
      if is_mcp_client or client_info.get("supports_sse"):
        return MCPExecutionStrategy.QUEUE_WITH_MONITORING
      else:
        return MCPExecutionStrategy.QUEUE_SIMPLE

    return None

  @classmethod
  def _select_query_strategy(
    cls, analysis: dict[str, Any], is_mcp_client: bool, client_info: dict[str, Any]
  ) -> MCPExecutionStrategy:
    """Select strategy for query tools based on complexity."""
    result_size = analysis["estimated_result_size"]
    requires_streaming = analysis["requires_streaming"]

    if result_size == "small" and not requires_streaming:
      return MCPExecutionStrategy.JSON_IMMEDIATE

    elif result_size == "medium":
      if requires_streaming:
        return MCPExecutionStrategy.STREAM_AGGREGATED
      else:
        return MCPExecutionStrategy.JSON_COMPLETE

    else:
      if is_mcp_client:
        return MCPExecutionStrategy.STREAM_AGGREGATED
      else:
        return MCPExecutionStrategy.SSE_PROGRESS

  @classmethod
  def _select_schema_strategy(cls) -> MCPExecutionStrategy:
    """Always JSON_COMPLETE: schema fetches are fast, and SSE would be chosen
    for MCP clients' Accept header with no benefit."""
    return MCPExecutionStrategy.JSON_COMPLETE

  @classmethod
  def get_timeout_for_strategy(cls, strategy: MCPExecutionStrategy) -> int:
    """Get appropriate timeout in seconds for the strategy."""
    mcp_timeouts = {
      MCPExecutionStrategy.JSON_IMMEDIATE: 30,
      MCPExecutionStrategy.JSON_COMPLETE: 60,
      MCPExecutionStrategy.SSE_STREAMING: 300,
      MCPExecutionStrategy.SSE_PROGRESS: 300,
      MCPExecutionStrategy.NDJSON_STREAMING: 300,
      MCPExecutionStrategy.QUEUE_WITH_MONITORING: 60,
      MCPExecutionStrategy.QUEUE_SIMPLE: 60,
      MCPExecutionStrategy.CACHED: 5,
      MCPExecutionStrategy.STREAM_AGGREGATED: 120,
      MCPExecutionStrategy.SCHEMA_CACHED: 30,
      MCPExecutionStrategy.INFO_CACHED: 30,
    }

    return mcp_timeouts.get(strategy, 60)
