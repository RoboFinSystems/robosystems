"""
Shared execution strategies and utilities for query and MCP endpoints.

This module provides base classes and common logic for intelligent strategy
selection based on client capabilities, system load, and operation characteristics.
"""

import re
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any

from robosystems.config.query_queue import QueryQueueConfig


class BaseExecutionStrategy(Enum):
  """Base execution strategies shared by Query and MCP."""

  # Immediate execution strategies
  JSON_IMMEDIATE = "json_immediate"  # Small result, immediate response
  JSON_COMPLETE = "json_complete"  # Medium result, wait for complete

  # Streaming strategies
  NDJSON_STREAMING = "ndjson_streaming"  # Stream as newline-delimited JSON
  SSE_STREAMING = "sse_streaming"  # Stream via Server-Sent Events
  SSE_PROGRESS = "sse_progress"  # SSE with progress updates

  # Queue strategies
  QUEUE_WITH_MONITORING = "queue_monitoring"  # Queue with SSE monitoring
  QUEUE_SIMPLE = "queue_simple"  # Simple queue with polling

  # Specialized strategies
  CACHED = "cached"  # Use cached result


class ResponseMode(Enum):
  """Response modes for execution."""

  AUTO = "auto"  # Automatic selection based on context
  SYNC = "sync"  # Force synchronous response
  ASYNC = "async"  # Force asynchronous/queued response
  STREAM = "stream"  # Force streaming response


class BaseAnalyzer(ABC):
  """Base analyzer for operations."""

  # Result size thresholds
  SMALL_RESULT = 100
  MEDIUM_RESULT = 1000
  LARGE_RESULT = 10000

  @abstractmethod
  def analyze(self, *args, **kwargs) -> dict[str, Any]:
    """Analyze operation to estimate characteristics."""
    pass

  @classmethod
  def analyze_cypher_query(cls, query: str) -> dict[str, Any]:
    """
    Analyze a Cypher query to estimate its characteristics and execution requirements.

    This method performs static analysis on a Cypher query to determine execution
    strategy based on query patterns, complexity, and estimated result size.

    Analysis includes:
    - Result size estimation based on LIMIT clauses
    - Detection of aggregation operations (COUNT, SUM, AVG, etc.)
    - Identification of expensive graph operations (shortest path, all paths)
    - Detection of potential Cartesian products
    - Streaming requirements based on estimated data volume
    - Progress reporting capabilities

    Args:
        query: The Cypher query string to analyze

    Returns:
        Dict[str, Any]: Analysis results containing:
            - has_limit (bool): Whether query has a LIMIT clause
            - limit_value (Optional[int]): Extracted LIMIT value if present
            - estimated_rows (Union[str, int]): Size category ('small', 'medium', 'large')
            - has_aggregation (bool): Contains aggregation functions
            - has_match (bool): Contains MATCH clause
            - has_where (bool): Contains WHERE clause
            - has_order_by (bool): Contains ORDER BY clause
            - has_shortest_path (bool): Uses shortest path algorithm
            - has_all_paths (bool): Searches for all paths
            - potentially_expensive (bool): May require significant resources
            - is_count_only (bool): Returns only count without grouping
            - requires_streaming (bool): Should use streaming for large results
            - supports_progress (bool): Can provide progress updates

    Examples:
        >>> analyze_cypher_query("MATCH (n) RETURN n LIMIT 10")
        {'has_limit': True, 'limit_value': 10, 'estimated_rows': 'small', ...}

        >>> analyze_cypher_query("MATCH (a)-[:KNOWS*]-(b) RETURN a, b")
        {'potentially_expensive': True, 'requires_streaming': True, ...}
    """
    query_upper = query.upper()

    # Check for LIMIT clause
    has_limit = "LIMIT" in query_upper
    limit_value = None
    if has_limit:
      limit_match = re.search(r"LIMIT\s+(\d+)", query_upper)
      if limit_match:
        limit_value = int(limit_match.group(1))

    # Estimate result size
    estimated_rows = cls._estimate_result_size(query_upper, limit_value)

    # Check for aggregations
    has_aggregation = any(
      agg in query_upper
      for agg in ["COUNT(", "SUM(", "AVG(", "MAX(", "MIN(", "COLLECT("]
    )

    # Check for expensive operations
    has_shortest_path = "SHORTEST" in query_upper and "PATH" in query_upper
    has_all_paths = "ALL" in query_upper and "PATH" in query_upper
    has_cartesian = query_upper.count("MATCH") > 1 and "," in query_upper

    return {
      "has_limit": has_limit,
      "limit_value": limit_value,
      "estimated_rows": estimated_rows,
      "has_aggregation": has_aggregation,
      "has_match": "MATCH" in query_upper,
      "has_where": "WHERE" in query_upper,
      "has_order_by": "ORDER BY" in query_upper,
      "has_shortest_path": has_shortest_path,
      "has_all_paths": has_all_paths,
      "potentially_expensive": has_shortest_path or has_all_paths or has_cartesian,
      "is_count_only": has_aggregation
      and "COUNT(" in query_upper
      and "GROUP BY" not in query_upper,
      "requires_streaming": estimated_rows == "large" and not has_aggregation,
      "supports_progress": "MATCH" in query_upper and not has_aggregation,
    }

  @classmethod
  def _estimate_result_size(
    cls, query_upper: str, limit_value: int | None
  ) -> int | str:
    """
    Estimate the result size category based on query patterns and LIMIT clause.

    This method uses heuristics to categorize expected result size for execution
    strategy selection. The categorization affects decisions about streaming,
    batching, and memory allocation.

    Estimation logic:
    1. If LIMIT is present: categorize based on limit value
       - <= 100 rows: 'small' (immediate JSON response)
       - <= 1000 rows: 'medium' (complete JSON response)
       - > 1000 rows: 'large' (requires streaming)
    2. If no LIMIT:
       - Single COUNT without GROUP BY: 'small' (single value)
       - No LIMIT clause at all: 'large' (unbounded results)
       - Other patterns: 'medium' (default assumption)

    Args:
        query_upper: The uppercase version of the Cypher query
        limit_value: Extracted LIMIT value if present, None otherwise

    Returns:
        Union[int, str]: Size category as string ('small', 'medium', 'large')
            Note: Despite the Union type hint, this always returns str in practice

    Examples:
        >>> _estimate_result_size("MATCH (n) RETURN n LIMIT 50", 50)
        'small'
        >>> _estimate_result_size("MATCH (n) RETURN COUNT(n)", None)
        'small'
        >>> _estimate_result_size("MATCH (n) RETURN n", None)
        'large'
    """
    if limit_value is not None:
      if limit_value <= cls.SMALL_RESULT:
        return "small"
      elif limit_value <= cls.MEDIUM_RESULT:
        return "medium"
      else:
        return "large"

    # No limit value but LIMIT clause present (parameterized limit like $limit)
    if "LIMIT" in query_upper and limit_value is None:
      return "medium"  # Assume reasonable size for parameterized limits

    # No limit - check for patterns
    if "COUNT(" in query_upper and "GROUP BY" not in query_upper:
      return "small"  # Single count result
    elif "LIMIT" not in query_upper:
      return "large"  # No limit means potentially large
    else:
      return "medium"  # Default


class BaseClientDetector:
  """Base client capability detection."""

  @classmethod
  def detect_client_capabilities(cls, headers: dict[str, str]) -> dict[str, Any]:
    """
    Detect client capabilities from request headers.

    Args:
        headers: Request headers

    Returns:
        Client capability information
    """
    user_agent = headers.get("user-agent", "").lower()
    accept = headers.get("accept", "").lower()

    # Check for SSE support
    supports_sse = (
      "text/event-stream" in accept or "eventstream" in accept or "sse" in accept
    )

    # Check for NDJSON support
    supports_ndjson = (
      "application/x-ndjson" in accept
      or "ndjson" in accept
      or "application/stream+json" in accept
    )

    # Detect testing tools
    is_testing_tool = any(
      tool in user_agent
      for tool in ["postman", "insomnia", "swagger", "openapi", "curl", "httpie"]
    )

    # Detect browsers
    is_browser = any(
      browser in user_agent
      for browser in ["mozilla", "chrome", "safari", "firefox", "edge"]
    )

    # Detect interactive environment
    is_interactive = is_testing_tool or (
      is_browser and "swagger" in headers.get("referer", "").lower()
    )

    return {
      "supports_sse": supports_sse,
      "supports_ndjson": supports_ndjson,
      "supports_streaming": supports_sse or supports_ndjson,
      "is_testing_tool": is_testing_tool,
      "is_browser": is_browser,
      "is_interactive": is_interactive,
      "user_agent": user_agent,
      "accept_header": accept,
    }


class BaseStrategySelector(ABC):
  """Base strategy selection logic."""

  @abstractmethod
  def select_strategy(self, *args, **kwargs) -> Any:
    """Select optimal execution strategy."""
    pass

  @classmethod
  def should_use_cache(
    cls, is_cacheable: bool, cache_available: bool, cache_ttl: int | None = None
  ) -> bool:
    """
    Determine if cache should be used.

    Args:
        is_cacheable: Whether the operation is cacheable
        cache_available: Whether cache is available
        cache_ttl: Cache time-to-live in seconds

    Returns:
        Whether to use cache
    """
    return is_cacheable and cache_available and (cache_ttl is None or cache_ttl > 0)

  @classmethod
  def should_queue(
    cls,
    queue_size: int,
    running_count: int,
    max_concurrent: int = 5,
    queue_threshold: int = 10,
  ) -> bool:
    """
    Determine if operation should be queued.

    Args:
        queue_size: Current queue size
        running_count: Currently running operations
        max_concurrent: Maximum concurrent operations
        queue_threshold: Queue size threshold

    Returns:
        Whether to queue the operation
    """
    return queue_size > queue_threshold or running_count >= max_concurrent

  @classmethod
  def get_priority_for_user(cls, user_tier: str | None) -> int:
    """
    Get priority based on user subscription tier.

    Args:
        user_tier: User subscription tier name

    Returns:
        Priority value (lower is higher priority)
    """
    return QueryQueueConfig.get_priority_for_user(user_tier)

  @classmethod
  def select_streaming_strategy(
    cls,
    supports_sse: bool,
    supports_ndjson: bool,
    requires_streaming: bool,
    is_interactive: bool,
    estimated_size: str,
  ) -> BaseExecutionStrategy | None:
    """
    Select appropriate streaming strategy.

    Args:
        supports_sse: Client supports SSE
        supports_ndjson: Client supports NDJSON
        requires_streaming: Operation requires streaming
        is_interactive: Client is interactive (testing tool/browser)
        estimated_size: Estimated result size

    Returns:
        Selected streaming strategy or None
    """
    if not (supports_sse or supports_ndjson):
      return None

    # For large results, always prefer streaming
    if estimated_size == "large" or requires_streaming:
      if supports_sse:
        return BaseExecutionStrategy.SSE_STREAMING
      elif supports_ndjson:
        return BaseExecutionStrategy.NDJSON_STREAMING

    # For medium results with progress support
    if estimated_size == "medium" and supports_sse:
      return BaseExecutionStrategy.SSE_PROGRESS

    return None


# NOTE: TimeoutCoordinator is available from robosystems.middleware.robustness
# Use: from robosystems.middleware.robustness import TimeoutCoordinator
