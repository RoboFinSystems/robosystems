"""
MCP-specific strategy selection and execution coordination.

This module provides intelligent strategy selection for MCP tool execution,
optimized for AI agent consumption and shared repository scalability.
"""

from typing import Dict, Any, Optional
import re
from enum import Enum

from robosystems.middleware.graph.execution_strategies import (
  BaseAnalyzer,
  BaseClientDetector,
  BaseStrategySelector,
)
from robosystems.middleware.graph.multitenant_utils import MultiTenantUtils


class MCPExecutionStrategy(Enum):
  """MCP tool execution strategies optimized for AI agents."""

  # Base strategies
  JSON_IMMEDIATE = "json_immediate"
  JSON_COMPLETE = "json_complete"
  NDJSON_STREAMING = "ndjson_streaming"
  SSE_STREAMING = "sse_streaming"
  SSE_PROGRESS = "sse_progress"
  QUEUE_WITH_MONITORING = "queue_monitoring"
  QUEUE_SIMPLE = "queue_simple"
  CACHED = "cached"

  # Additional MCP-specific strategies
  STREAM_AGGREGATED = "stream_aggregated"  # Stream and aggregate for agent
  SCHEMA_CACHED = "schema_cached"  # Use cached schema
  INFO_CACHED = "info_cached"  # Use cached graph info


class MCPToolAnalyzer(BaseAnalyzer):
  """Analyze MCP tool calls to determine optimal execution strategy."""

  # Tool categories
  QUERY_TOOLS = ["read-graph-cypher", "read-neo4j-cypher", "read-ladybug-cypher"]
  SCHEMA_TOOLS = ["get-graph-schema", "get-neo4j-schema", "get-ladybug-schema"]
  INFO_TOOLS = ["get-graph-info", "describe-graph-structure"]

  def analyze(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """
    Implementation of abstract analyze method.

    Args:
        tool_name: Name of the MCP tool
        arguments: Tool arguments

    Returns:
        Dictionary with analysis results
    """
    return self.analyze_tool_call(tool_name, arguments)

  @classmethod
  def analyze_tool_call(
    cls, tool_name: str, arguments: Dict[str, Any]
  ) -> Dict[str, Any]:
    """
    Analyze an MCP tool call to estimate its characteristics.

    Args:
        tool_name: Name of the MCP tool
        arguments: Tool arguments

    Returns:
        Dictionary with analysis results
    """
    analysis = {
      "tool_category": cls._get_tool_category(tool_name),
      "is_cacheable": cls._is_cacheable(tool_name),
      "estimated_duration_ms": cls._estimate_duration(tool_name, arguments),
      "estimated_result_size": cls._estimate_mcp_result_size(tool_name, arguments),
      "requires_streaming": False,
      "supports_progress": False,
    }

    # Query-specific analysis - use base analyzer for Cypher
    if tool_name in cls.QUERY_TOOLS:
      query = arguments.get("query", "")
      query_analysis = cls.analyze_cypher_query(query)
      analysis.update(query_analysis)

    # Schema tools support progress updates
    elif tool_name in cls.SCHEMA_TOOLS:
      analysis["supports_progress"] = True
      analysis["estimated_result_size"] = "large"  # Schemas can be large

    # Info tools are typically fast
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
    # Schema and info are cacheable, queries generally are not
    return tool_name in cls.SCHEMA_TOOLS or tool_name in cls.INFO_TOOLS

  @classmethod
  def _estimate_duration(cls, tool_name: str, arguments: Dict[str, Any]) -> int:
    """Estimate execution duration in milliseconds."""
    if tool_name in cls.QUERY_TOOLS:
      query = arguments.get("query", "")
      # Complex queries take longer
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
  def _estimate_mcp_result_size(cls, tool_name: str, arguments: Dict[str, Any]) -> str:
    """Estimate result size category: small, medium, large."""
    if tool_name in cls.QUERY_TOOLS:
      query = arguments.get("query", "")
      # Check for LIMIT clause
      limit_match = re.search(r"LIMIT\s+(\d+)", query, re.IGNORECASE)
      if limit_match:
        limit = int(limit_match.group(1))
        if limit <= 100:
          return "small"
        elif limit <= 1000:
          return "medium"
        else:
          return "large"
      # No limit means potentially large
      return "large"
    elif tool_name in cls.SCHEMA_TOOLS:
      return "large"  # Schemas can be extensive
    else:
      return "small"  # Info is typically compact

  @classmethod
  def _analyze_cypher_query(cls, query: str) -> Dict[str, Any]:
    """Analyze a Cypher query for MCP-specific optimizations."""
    query_upper = query.upper()

    # Detect patterns that benefit from streaming
    has_match = "MATCH" in query_upper
    has_aggregation = any(
      agg in query_upper for agg in ["COUNT(", "SUM(", "AVG(", "COLLECT("]
    )
    has_order_by = "ORDER BY" in query_upper

    # Determine if streaming would help
    requires_streaming = (
      has_match and "LIMIT" not in query_upper and not has_aggregation
    )

    return {
      "requires_streaming": requires_streaming,
      "supports_progress": has_match and not has_aggregation,
      "has_aggregation": has_aggregation,
      "has_ordering": has_order_by,
    }


class MCPStrategySelector(BaseStrategySelector):
  """Select optimal execution strategy for MCP tools."""

  @classmethod
  def select_strategy(
    cls,
    tool_name: str,
    arguments: Dict[str, Any],
    client_info: Dict[str, Any],
    system_state: Dict[str, Any],
    graph_id: str,
    user_tier: Optional[str] = None,
  ) -> MCPExecutionStrategy:
    """
    Select the optimal execution strategy for an MCP tool call.

    This method considers:
    - Tool type and estimated complexity
    - Client capabilities (though AI agents get transparent handling)
    - System load and queue status
    - Graph type (shared repository vs user graph)
    - User subscription tier

    Args:
        tool_name: Name of the MCP tool
        arguments: Tool arguments
        client_info: Client capabilities (from headers)
        system_state: Current system load
        graph_id: Target graph identifier
        user_tier: User subscription tier

    Returns:
        Selected execution strategy
    """
    # Analyze the tool call
    analysis = MCPToolAnalyzer.analyze_tool_call(tool_name, arguments)

    # Check if this is a shared repository (for future optimizations)
    _is_shared_repo = MultiTenantUtils.is_shared_repository(graph_id)

    # Special handling for AI agents (MCP clients)
    is_mcp_client = client_info.get("is_mcp_client", False)

    # Try strategies in priority order

    # 1. Check for cached strategy
    cached_strategy = cls._select_cached_strategy(analysis, system_state)
    if cached_strategy:
      return cached_strategy

    # 2. Check for high load strategy
    high_load_strategy = cls._select_high_load_strategy(
      system_state, is_mcp_client, client_info
    )
    if high_load_strategy:
      return high_load_strategy

    # 3. Select based on tool category
    if analysis["tool_category"] == "query":
      return cls._select_query_strategy(analysis, is_mcp_client, client_info)
    elif analysis["tool_category"] == "schema":
      return cls._select_schema_strategy(analysis, client_info)
    elif analysis["tool_category"] == "info":
      return MCPExecutionStrategy.JSON_IMMEDIATE

    # Default fallback
    return MCPExecutionStrategy.JSON_COMPLETE

  @classmethod
  def _select_cached_strategy(
    cls, analysis: Dict[str, Any], system_state: Dict[str, Any]
  ) -> Optional[MCPExecutionStrategy]:
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
    cls, system_state: Dict[str, Any], is_mcp_client: bool, client_info: Dict[str, Any]
  ) -> Optional[MCPExecutionStrategy]:
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
    cls, analysis: Dict[str, Any], is_mcp_client: bool, client_info: Dict[str, Any]
  ) -> MCPExecutionStrategy:
    """Select strategy for query tools based on complexity."""
    result_size = analysis["estimated_result_size"]
    requires_streaming = analysis["requires_streaming"]

    # Small queries - direct execution
    if result_size == "small" and not requires_streaming:
      return MCPExecutionStrategy.JSON_IMMEDIATE

    # Medium queries - complete then return
    elif result_size == "medium":
      if requires_streaming:
        return MCPExecutionStrategy.STREAM_AGGREGATED
      else:
        return MCPExecutionStrategy.JSON_COMPLETE

    # Large queries - always stream
    else:
      # For MCP clients, aggregate streaming transparently
      if is_mcp_client:
        return MCPExecutionStrategy.STREAM_AGGREGATED
      else:
        return MCPExecutionStrategy.SSE_PROGRESS

  @classmethod
  def _select_schema_strategy(
    cls, analysis: Dict[str, Any], client_info: Dict[str, Any]
  ) -> MCPExecutionStrategy:
    """Select strategy for schema tools."""
    if analysis["supports_progress"] and client_info.get("supports_sse"):
      return MCPExecutionStrategy.SSE_PROGRESS
    else:
      return MCPExecutionStrategy.JSON_COMPLETE

  @classmethod
  def get_timeout_for_strategy(cls, strategy: MCPExecutionStrategy) -> int:
    """Get appropriate timeout in seconds for the strategy."""
    # MCP-specific timeouts
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
      MCPExecutionStrategy.SCHEMA_CACHED: 5,
      MCPExecutionStrategy.INFO_CACHED: 5,
    }

    # Default to 60 seconds if not specified
    return mcp_timeouts.get(strategy, 60)


class MCPClientDetector(BaseClientDetector):
  """Detect MCP client capabilities and optimize responses."""

  @classmethod
  def detect_client_type(cls, headers: Dict[str, str]) -> Dict[str, Any]:
    """
    Detect MCP client capabilities from request headers.

    MCP clients (Node.js package) are detected by:
    - User-Agent containing 'robosystems-mcp'
    - X-MCP-Client header
    - Accept header preferences

    Args:
        headers: Request headers

    Returns:
        Client capability information
    """
    # Get base client capabilities
    base_info = cls.detect_client_capabilities(headers)

    user_agent = headers.get("user-agent", "").lower()
    mcp_client = headers.get("x-mcp-client", "")

    # Detect MCP client
    is_mcp_client = (
      "robosystems-mcp" in user_agent or mcp_client != "" or "mcp" in user_agent
    )

    # MCP clients get special handling
    if is_mcp_client:
      return {
        "is_mcp_client": True,
        "supports_sse": True,  # Node.js package will handle SSE
        "supports_ndjson": True,  # And NDJSON
        "prefers_streaming": False,  # But aggregate for AI agent
        "client_version": mcp_client or "unknown",
        # Include base info
        "is_testing_tool": base_info["is_testing_tool"],
        "is_browser": base_info["is_browser"],
        "is_interactive": base_info["is_interactive"],
      }

    # For non-MCP clients, use base detection with MCP-specific additions
    return {
      "is_mcp_client": False,
      "supports_sse": base_info["supports_sse"],
      "supports_ndjson": base_info["supports_ndjson"],
      "prefers_streaming": base_info["supports_streaming"],
      "client_version": None,
      # Include base info
      "is_testing_tool": base_info["is_testing_tool"],
      "is_browser": base_info["is_browser"],
      "is_interactive": base_info["is_interactive"],
    }
