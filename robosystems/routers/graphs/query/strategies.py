"""
Query execution strategies and utilities.

This module provides intelligent strategy selection for query execution
based on client capabilities, system load, and query characteristics.
"""

import re
from typing import Dict, Any, Optional, Tuple

from enum import Enum
from robosystems.middleware.graph.execution_strategies import (
  BaseAnalyzer,
  BaseClientDetector,
  BaseStrategySelector,
  ResponseMode as BaseResponseMode,
)
from robosystems.middleware.robustness import TimeoutCoordinator
from robosystems.logger import logger


class ExecutionStrategy(Enum):
  """Query execution strategies including base and query-specific strategies."""

  # Base strategies
  JSON_IMMEDIATE = "json_immediate"
  JSON_COMPLETE = "json_complete"
  NDJSON_STREAMING = "ndjson_streaming"
  SSE_STREAMING = "sse_streaming"
  SSE_PROGRESS = "sse_progress"
  QUEUE_WITH_MONITORING = "queue_monitoring"
  QUEUE_SIMPLE = "queue_simple"
  CACHED = "cached"

  # Additional query-specific strategies
  SSE_QUEUE_STREAM = "sse_queue_stream"  # Queue first, then stream via SSE
  TRADITIONAL_QUEUE = "traditional_queue"  # Return 202 with polling URLs
  SYNC_TESTING = "sync_testing"  # Synchronous for testing tools


# Re-export ResponseMode for backward compatibility
ResponseMode = BaseResponseMode


class QueryAnalyzer(BaseAnalyzer):
  """Analyze Cypher queries to estimate characteristics."""

  @classmethod
  def analyze_query(cls, query: str) -> Dict[str, Any]:
    """
    Analyze a Cypher query to estimate its characteristics.

    Args:
        query: The Cypher query string

    Returns:
        Dictionary with query analysis results
    """
    # Use base Cypher analysis
    return cls.analyze_cypher_query(query)

  def analyze(
    self, query: str, parameters: Optional[Dict[str, Any]] = None
  ) -> Dict[str, Any]:
    """
    Implementation of abstract analyze method.

    Args:
        query: The Cypher query string
        parameters: Optional query parameters (unused in current implementation)

    Returns:
        Dictionary with query analysis results
    """
    # Use the class method for backward compatibility
    return self.analyze_query(query)

  @classmethod
  def _estimate_result_size(cls, query_upper: str, limit_value: Optional[int]) -> int:
    """Estimate the number of rows a query will return."""
    if limit_value:
      return limit_value

    # Parameterized LIMIT (e.g., LIMIT $limit) - assume medium size
    if "LIMIT" in query_upper and limit_value is None:
      return cls.MEDIUM_RESULT

    # Single aggregation without GROUP BY
    if "COUNT(" in query_upper and "GROUP BY" not in query_upper:
      return 1

    # Simple queries without MATCH
    if "MATCH" not in query_upper:
      return 10

    # Queries with WHERE clause (filtered)
    if "WHERE" in query_upper:
      # Very specific filters
      if query_upper.count("AND") >= 2:
        return 50
      return 100

    # Path queries can be very large
    if "PATH" in query_upper:
      return cls.LARGE_RESULT * 10

    # Unfiltered MATCH - assume large
    return cls.LARGE_RESULT


class ClientDetector(BaseClientDetector):
  """Detect client type and capabilities from request headers."""

  @classmethod
  def detect_client_type(cls, headers: Dict[str, str]) -> Dict[str, Any]:
    """
    Detect client type and capabilities from request headers.

    Args:
        headers: Request headers dictionary

    Returns:
        Dictionary with client detection results
    """
    # Get base client capabilities
    base_info = cls.detect_client_capabilities(headers)

    # Extract additional query-specific information
    prefer_header = headers.get("prefer", "")

    # Parse Prefer header
    prefers_wait = "wait" in prefer_header
    prefers_stream = "stream" in prefer_header
    prefers_async = "respond-async" in prefer_header

    # Extract wait time from Prefer header if present
    wait_time = None
    if "wait=" in prefer_header:
      wait_match = re.search(r"wait=(\d+)", prefer_header)
      if wait_match:
        wait_time = int(wait_match.group(1))

    return {
      "is_testing_tool": base_info["is_testing_tool"],
      "is_browser": base_info["is_browser"],
      "is_interactive": base_info["is_interactive"],
      "user_agent": base_info["user_agent"],
      "capabilities": {
        "sse": base_info["supports_sse"],
        "ndjson": base_info["supports_ndjson"],
        "json": True,  # Always support JSON
      },
      "preferences": {
        "wait": prefers_wait,
        "stream": prefers_stream,
        "async": prefers_async,
        "wait_time": wait_time,
      },
    }


class StrategySelector(BaseStrategySelector):
  """Select optimal execution strategy based on context."""

  @classmethod
  def select_strategy(
    cls,
    query_analysis: Dict[str, Any],
    client_info: Dict[str, Any],
    system_state: Dict[str, Any],
    mode_override: Optional[ResponseMode] = None,
    is_write_operation: bool = False,
  ) -> Tuple[ExecutionStrategy, Dict[str, Any]]:
    """
    Select the optimal execution strategy.

    Args:
        query_analysis: Query analysis results
        client_info: Client detection results
        system_state: Current system state (queue size, running queries, etc.)
        mode_override: Optional mode override from request
        is_write_operation: Whether this is a write operation

    Returns:
        Tuple of (selected strategy, metadata for decision)
    """
    metadata = {
      "query_analysis": query_analysis,
      "client_info": client_info,
      "system_state": system_state,
      "mode_override": mode_override,
      "is_write": is_write_operation,
    }

    # Handle mode overrides
    if mode_override == ResponseMode.SYNC:
      # Force synchronous response
      return ExecutionStrategy.SYNC_TESTING, metadata
    elif mode_override == ResponseMode.ASYNC:
      # Force queued response
      return ExecutionStrategy.TRADITIONAL_QUEUE, metadata
    elif mode_override == ResponseMode.STREAM:
      # Force streaming - choose based on capabilities
      if client_info["capabilities"]["sse"]:
        return ExecutionStrategy.SSE_STREAMING, metadata
      elif client_info["capabilities"]["ndjson"]:
        return ExecutionStrategy.NDJSON_STREAMING, metadata
      else:
        # Client doesn't support streaming but requested it
        logger.warning("Client requested streaming but doesn't support SSE or NDJSON")
        return ExecutionStrategy.NDJSON_STREAMING, metadata

    # Auto mode - intelligent selection

    # Testing tools get special treatment
    if client_info["is_interactive"]:
      # Interactive tools (Postman, Swagger) need synchronous responses
      # But respect size limits to avoid timeouts
      if query_analysis["estimated_rows"] <= QueryAnalyzer.MEDIUM_RESULT:
        return ExecutionStrategy.SYNC_TESTING, metadata
      else:
        # Large result for testing tool - warn but try
        logger.info(
          f"Testing tool detected with large query (est. {query_analysis['estimated_rows']} rows)"
        )
        return ExecutionStrategy.SYNC_TESTING, metadata

    # Check system load
    needs_queue = system_state["queue_size"] > 0 or system_state[
      "running_queries"
    ] >= system_state.get("max_concurrent", 5)

    # Write operations can't stream
    if is_write_operation:
      if needs_queue:
        return ExecutionStrategy.TRADITIONAL_QUEUE, metadata
      return ExecutionStrategy.JSON_COMPLETE, metadata

    # System needs queue - decide based on client capabilities
    if needs_queue:
      if client_info["capabilities"]["sse"] and not client_info["preferences"]["async"]:
        # Best option: Queue with SSE then stream results
        return ExecutionStrategy.SSE_QUEUE_STREAM, metadata
      else:
        # Fallback to traditional queuing
        return ExecutionStrategy.TRADITIONAL_QUEUE, metadata

    # System has capacity - decide based on result size
    estimated_rows = query_analysis["estimated_rows"]

    if estimated_rows <= QueryAnalyzer.SMALL_RESULT:
      # Small results - return immediately as JSON
      return ExecutionStrategy.JSON_IMMEDIATE, metadata

    elif estimated_rows <= QueryAnalyzer.MEDIUM_RESULT:
      # Medium results - choose based on client preference
      if client_info["preferences"]["stream"]:
        if client_info["capabilities"]["sse"]:
          return ExecutionStrategy.SSE_STREAMING, metadata
        elif client_info["capabilities"]["ndjson"]:
          return ExecutionStrategy.NDJSON_STREAMING, metadata
      # Default: wait for complete result
      return ExecutionStrategy.JSON_COMPLETE, metadata

    else:  # Large result
      # Large results should stream if possible
      if client_info["capabilities"]["sse"]:
        return ExecutionStrategy.SSE_STREAMING, metadata
      elif client_info["capabilities"]["ndjson"]:
        return ExecutionStrategy.NDJSON_STREAMING, metadata
      elif (
        query_analysis["has_limit"]
        and query_analysis["limit_value"] <= QueryAnalyzer.MEDIUM_RESULT
      ):
        # Has reasonable limit, can return as JSON
        return ExecutionStrategy.JSON_COMPLETE, metadata
      else:
        # Client doesn't support streaming but result is large
        # Try NDJSON anyway (some clients might handle it)
        logger.warning(
          f"Large result ({estimated_rows} rows) without streaming support, using NDJSON"
        )
        return ExecutionStrategy.NDJSON_STREAMING, metadata


class QueryTimeoutCoordinator(TimeoutCoordinator):
  """Coordinate timeout hierarchy for query operations."""

  # Default timeout buffers (in seconds)
  QUEUE_BUFFER = 30  # Queue timeout should be 30s less than endpoint
  EXECUTION_BUFFER = 30  # Execution timeout should be 30s less than queue

  # Maximum timeouts by context
  MAX_TESTING_TIMEOUT = 30  # Max timeout for testing tools
  MAX_STREAMING_TIMEOUT = 300  # Max timeout for streaming
  MAX_QUEUE_TIMEOUT = 600  # Max timeout for queued queries

  @classmethod
  def calculate_timeouts(
    cls, requested_timeout: int, strategy: ExecutionStrategy, is_testing: bool = False
  ) -> Dict[str, int]:
    """
    Calculate coordinated timeouts for query execution layers.

    Args:
        requested_timeout: User-requested timeout
        strategy: Selected execution strategy
        is_testing: Whether this is a testing context

    Returns:
        Dictionary with coordinated timeouts for each layer
    """
    # Apply context-based limits
    if is_testing:
      endpoint_timeout = min(requested_timeout, cls.MAX_TESTING_TIMEOUT)
    elif strategy in [
      ExecutionStrategy.SSE_STREAMING,
      ExecutionStrategy.NDJSON_STREAMING,
    ]:
      endpoint_timeout = min(requested_timeout, cls.MAX_STREAMING_TIMEOUT)
    else:
      endpoint_timeout = min(requested_timeout, cls.MAX_QUEUE_TIMEOUT)

    # Calculate cascade with buffers
    queue_timeout = max(endpoint_timeout - cls.QUEUE_BUFFER, 30)
    execution_timeout = max(queue_timeout - cls.EXECUTION_BUFFER, 30)

    return {
      "endpoint": endpoint_timeout,
      "queue": queue_timeout,
      "execution": execution_timeout,
    }
