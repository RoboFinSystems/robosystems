"""Query execution strategy selection from client capabilities, system load,
and query shape."""

import re
from enum import Enum
from typing import Any

from robosystems.logger import logger
from robosystems.middleware.graph.execution_strategies import (
  BaseAnalyzer,
  BaseClientDetector,
  BaseStrategySelector,
)
from robosystems.middleware.graph.execution_strategies import (
  ResponseMode as BaseResponseMode,
)
from robosystems.middleware.robustness import TimeoutCoordinator


class ExecutionStrategy(Enum):
  """Query execution strategies including base and query-specific strategies."""

  JSON_IMMEDIATE = "json_immediate"
  JSON_COMPLETE = "json_complete"
  NDJSON_STREAMING = "ndjson_streaming"
  SSE_STREAMING = "sse_streaming"
  SSE_PROGRESS = "sse_progress"
  QUEUE_WITH_MONITORING = "queue_monitoring"
  QUEUE_SIMPLE = "queue_simple"
  CACHED = "cached"

  SSE_QUEUE_STREAM = "sse_queue_stream"  # Queue first, then stream via SSE
  TRADITIONAL_QUEUE = "traditional_queue"  # Return 202 with polling URLs
  SYNC_TESTING = "sync_testing"  # Synchronous for testing tools


# Alias for callers that import ResponseMode from this module.
ResponseMode = BaseResponseMode


class QueryAnalyzer(BaseAnalyzer):
  """Analyze Cypher queries to estimate characteristics."""

  @classmethod
  def analyze_query(cls, query: str) -> dict[str, Any]:
    """Estimate the cost and shape of a Cypher query."""
    return cls.analyze_cypher_query(query)

  def analyze(
    self, query: str, parameters: dict[str, Any] | None = None
  ) -> dict[str, Any]:
    """Implementation of the abstract `analyze` method."""
    return self.analyze_query(query)

  @classmethod
  def _estimate_result_size(cls, query_upper: str, limit_value: int | None) -> int:
    """Estimate the number of rows a query will return."""
    if limit_value:
      return limit_value

    if "LIMIT" in query_upper and limit_value is None:
      return cls.MEDIUM_RESULT

    if "COUNT(" in query_upper and "GROUP BY" not in query_upper:
      return 1

    if "MATCH" not in query_upper:
      return 10

    if "WHERE" in query_upper:
      if query_upper.count("AND") >= 2:
        return 50
      return 100

    if "PATH" in query_upper:
      return cls.LARGE_RESULT * 10

    return cls.LARGE_RESULT


class ClientDetector(BaseClientDetector):
  """Detect client type and capabilities from request headers."""

  @classmethod
  def detect_client_type(cls, headers: dict[str, str]) -> dict[str, Any]:
    """Detect client type and capabilities from request headers."""
    base_info = cls.detect_client_capabilities(headers)

    prefer_header = headers.get("prefer", "")

    prefers_wait = "wait" in prefer_header
    prefers_stream = "stream" in prefer_header
    prefers_async = "respond-async" in prefer_header

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
    query_analysis: dict[str, Any],
    client_info: dict[str, Any],
    system_state: dict[str, Any],
    mode_override: ResponseMode | None = None,
    is_write_operation: bool = False,
  ) -> tuple[ExecutionStrategy, dict[str, Any]]:
    """Returns the strategy paired with the metadata explaining the choice."""
    metadata = {
      "query_analysis": query_analysis,
      "client_info": client_info,
      "system_state": system_state,
      "mode_override": mode_override,
      "is_write": is_write_operation,
    }

    if mode_override == ResponseMode.SYNC:
      return ExecutionStrategy.SYNC_TESTING, metadata
    elif mode_override == ResponseMode.ASYNC:
      return ExecutionStrategy.TRADITIONAL_QUEUE, metadata
    elif mode_override == ResponseMode.STREAM:
      if client_info["capabilities"]["sse"]:
        return ExecutionStrategy.SSE_STREAMING, metadata
      elif client_info["capabilities"]["ndjson"]:
        return ExecutionStrategy.NDJSON_STREAMING, metadata
      else:
        logger.warning("Client requested streaming but doesn't support SSE or NDJSON")
        return ExecutionStrategy.NDJSON_STREAMING, metadata

    if client_info["is_interactive"]:
      # Interactive tools (Postman, Swagger) always get a synchronous response;
      # a large estimate is only logged.
      if query_analysis["estimated_rows"] <= QueryAnalyzer.MEDIUM_RESULT:
        return ExecutionStrategy.SYNC_TESTING, metadata
      else:
        logger.info(
          f"Testing tool detected with large query (est. {query_analysis['estimated_rows']} rows)"
        )
        return ExecutionStrategy.SYNC_TESTING, metadata

    needs_queue = system_state["queue_size"] > 0 or system_state[
      "running_queries"
    ] >= system_state.get("max_concurrent", 5)

    # Writes can't stream.
    if is_write_operation:
      if needs_queue:
        return ExecutionStrategy.TRADITIONAL_QUEUE, metadata
      return ExecutionStrategy.JSON_COMPLETE, metadata

    # System needs queue - decide based on client capabilities
    if needs_queue:
      if client_info["capabilities"]["sse"] and not client_info["preferences"]["async"]:
        return ExecutionStrategy.SSE_QUEUE_STREAM, metadata
      else:
        return ExecutionStrategy.TRADITIONAL_QUEUE, metadata

    estimated_rows = query_analysis["estimated_rows"]

    if estimated_rows <= QueryAnalyzer.SMALL_RESULT:
      return ExecutionStrategy.JSON_IMMEDIATE, metadata

    elif estimated_rows <= QueryAnalyzer.MEDIUM_RESULT:
      if client_info["preferences"]["stream"]:
        if client_info["capabilities"]["sse"]:
          return ExecutionStrategy.SSE_STREAMING, metadata
        elif client_info["capabilities"]["ndjson"]:
          return ExecutionStrategy.NDJSON_STREAMING, metadata
      return ExecutionStrategy.JSON_COMPLETE, metadata

    else:  # Large result
      if client_info["capabilities"]["sse"]:
        return ExecutionStrategy.SSE_STREAMING, metadata
      elif client_info["capabilities"]["ndjson"]:
        return ExecutionStrategy.NDJSON_STREAMING, metadata
      else:
        return ExecutionStrategy.JSON_COMPLETE, metadata


class QueryTimeoutCoordinator(TimeoutCoordinator):
  """Coordinate timeout hierarchy for query operations."""

  QUEUE_BUFFER = 30  # Queue timeout should be 30s less than endpoint
  EXECUTION_BUFFER = 30  # Execution timeout should be 30s less than queue

  MAX_TESTING_TIMEOUT = 30  # Max timeout for testing tools
  MAX_STREAMING_TIMEOUT = 300  # Max timeout for streaming
  MAX_QUEUE_TIMEOUT = 600  # Max timeout for queued queries

  @classmethod
  def calculate_timeouts(
    cls, requested_timeout: int, strategy: ExecutionStrategy, is_testing: bool = False
  ) -> dict[str, int]:
    """Per-layer timeouts for one query: each inner layer gets a 30s-smaller
    budget than the one above it, floored at 30s."""
    if is_testing:
      endpoint_timeout = min(requested_timeout, cls.MAX_TESTING_TIMEOUT)
    elif strategy in [
      ExecutionStrategy.SSE_STREAMING,
      ExecutionStrategy.NDJSON_STREAMING,
    ]:
      endpoint_timeout = min(requested_timeout, cls.MAX_STREAMING_TIMEOUT)
    else:
      endpoint_timeout = min(requested_timeout, cls.MAX_QUEUE_TIMEOUT)

    queue_timeout = max(endpoint_timeout - cls.QUEUE_BUFFER, 30)
    execution_timeout = max(queue_timeout - cls.EXECUTION_BUFFER, 30)

    return {
      "endpoint": endpoint_timeout,
      "queue": queue_timeout,
      "execution": execution_timeout,
    }
