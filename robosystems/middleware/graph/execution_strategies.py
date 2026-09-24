"""Execution-strategy selection shared by the query and MCP endpoints."""

import re
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any

from robosystems.config.query_queue import QueryQueueConfig


class BaseExecutionStrategy(Enum):
  """Base execution strategies shared by Query and MCP."""

  JSON_IMMEDIATE = "json_immediate"  # small result
  JSON_COMPLETE = "json_complete"  # medium result, wait for completion

  NDJSON_STREAMING = "ndjson_streaming"
  SSE_STREAMING = "sse_streaming"
  SSE_PROGRESS = "sse_progress"

  QUEUE_WITH_MONITORING = "queue_monitoring"  # SSE monitoring
  QUEUE_SIMPLE = "queue_simple"  # polling

  CACHED = "cached"


class ResponseMode(Enum):
  """Response modes for execution."""

  AUTO = "auto"
  SYNC = "sync"
  ASYNC = "async"  # queued
  STREAM = "stream"


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
    """Keyword heuristics over the query text for strategy selection."""
    query_upper = query.upper()

    has_limit = "LIMIT" in query_upper
    limit_value = None
    if has_limit:
      limit_match = re.search(r"LIMIT\s+(\d+)", query_upper)
      if limit_match:
        limit_value = int(limit_match.group(1))

    estimated_rows = cls._estimate_result_size(query_upper, limit_value)

    has_aggregation = any(
      agg in query_upper
      for agg in ["COUNT(", "SUM(", "AVG(", "MAX(", "MIN(", "COLLECT("]
    )

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
    """'small', 'medium' or 'large' from the LIMIT value, else from the shape."""
    if limit_value is not None:
      if limit_value <= cls.SMALL_RESULT:
        return "small"
      elif limit_value <= cls.MEDIUM_RESULT:
        return "medium"
      else:
        return "large"

    # A parameterized LIMIT ($limit).
    if "LIMIT" in query_upper and limit_value is None:
      return "medium"

    if "COUNT(" in query_upper and "GROUP BY" not in query_upper:
      return "small"
    return "large"


class BaseClientDetector:
  """Base client capability detection."""

  @classmethod
  def detect_client_capabilities(cls, headers: dict[str, str]) -> dict[str, Any]:
    """Detect client capabilities from request headers."""
    user_agent = headers.get("user-agent", "").lower()
    accept = headers.get("accept", "").lower()

    supports_sse = (
      "text/event-stream" in accept or "eventstream" in accept or "sse" in accept
    )

    supports_ndjson = (
      "application/x-ndjson" in accept
      or "ndjson" in accept
      or "application/stream+json" in accept
    )

    is_testing_tool = any(
      tool in user_agent
      for tool in ["postman", "insomnia", "swagger", "openapi", "curl", "httpie"]
    )

    is_browser = any(
      browser in user_agent
      for browser in ["mozilla", "chrome", "safari", "firefox", "edge"]
    )

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
    """Determine if cache should be used."""
    return is_cacheable and cache_available and (cache_ttl is None or cache_ttl > 0)

  @classmethod
  def should_queue(
    cls,
    queue_size: int,
    running_count: int,
    max_concurrent: int = 5,
    queue_threshold: int = 10,
  ) -> bool:
    """Determine if operation should be queued."""
    return queue_size > queue_threshold or running_count >= max_concurrent

  @classmethod
  def get_priority_for_user(cls, user_tier: str | None) -> int:
    """Get priority based on user subscription tier."""
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
    """Select appropriate streaming strategy."""
    if not (supports_sse or supports_ndjson):
      return None

    if estimated_size == "large" or requires_streaming:
      if supports_sse:
        return BaseExecutionStrategy.SSE_STREAMING
      elif supports_ndjson:
        return BaseExecutionStrategy.NDJSON_STREAMING

    if estimated_size == "medium" and supports_sse:
      return BaseExecutionStrategy.SSE_PROGRESS

    return None
