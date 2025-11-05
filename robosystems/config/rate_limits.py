"""
Centralized rate limiting configuration.

This module contains all rate limit definitions for different subscription tiers
and endpoint categories.
"""

from enum import Enum
from typing import Dict, Tuple, Optional
from .tier_config import get_tier_api_rate_multiplier


class RateLimitPeriod(str, Enum):
  """Time periods for rate limiting."""

  MINUTE = "minute"
  HOUR = "hour"
  DAY = "day"

  def to_seconds(self) -> int:
    """Convert period to seconds."""
    return {
      self.MINUTE: 60,
      self.HOUR: 3600,
      self.DAY: 86400,
    }[self]


class EndpointCategory(str, Enum):
  """Categories of API endpoints for rate limiting."""

  # Non-graph scoped endpoints
  AUTH = "auth"
  USER_MANAGEMENT = "user_management"
  TASKS = "tasks"
  STATUS = "status"
  SSE = "sse"  # Server-Sent Events connections

  # Graph-scoped endpoints
  GRAPH_READ = "graph_read"
  GRAPH_WRITE = "graph_write"
  GRAPH_ANALYTICS = "graph_analytics"
  GRAPH_BACKUP = "graph_backup"
  GRAPH_SYNC = "graph_sync"
  GRAPH_MCP = "graph_mcp"
  GRAPH_AGENT = "graph_agent"

  # High-cost operations
  GRAPH_QUERY = "graph_query"  # Direct Cypher queries
  GRAPH_IMPORT = "graph_import"  # Bulk data imports

  # Table operations (DuckDB staging tables)
  TABLE_QUERY = "table_query"  # SQL queries on staging tables
  TABLE_UPLOAD = "table_upload"  # File uploads to staging tables
  TABLE_MANAGEMENT = "table_management"  # Table creation/deletion


class RateLimitConfig:
  """Centralized rate limiting configuration."""

  # Default rate limit if not specified
  DEFAULT_LIMIT = (100, RateLimitPeriod.HOUR)

  # Rate limit window sizes (for sliding window implementation)
  WINDOW_SIZE_SECONDS = {
    RateLimitPeriod.MINUTE: 60,
    RateLimitPeriod.HOUR: 3600,
    RateLimitPeriod.DAY: 86400,
  }

  # Burst allowance multiplier (allows short bursts above limit)
  BURST_MULTIPLIER = 1.2

  # Rate limit headers to include in responses
  RATE_LIMIT_HEADERS = {
    "limit": "X-RateLimit-Limit",
    "remaining": "X-RateLimit-Remaining",
    "reset": "X-RateLimit-Reset",
    "retry_after": "Retry-After",
  }

  # Subscription tier rate limits
  # BURST-FOCUSED CONFIGURATION: Short windows for burst protection
  # Volume control is handled by the credit system
  # Format: {tier: {category: (limit, period)}}
  SUBSCRIPTION_RATE_LIMITS: Dict[
    str, Dict[EndpointCategory, Tuple[int, RateLimitPeriod]]
  ] = {
    "free": {
      # Non-graph endpoints - keep some restrictions for free tier
      EndpointCategory.AUTH: (10, RateLimitPeriod.MINUTE),
      EndpointCategory.USER_MANAGEMENT: (60, RateLimitPeriod.MINUTE),
      EndpointCategory.TASKS: (60, RateLimitPeriod.MINUTE),
      EndpointCategory.STATUS: (120, RateLimitPeriod.MINUTE),
      EndpointCategory.SSE: (
        5,
        RateLimitPeriod.MINUTE,
      ),  # Limited SSE connections for free
      # Graph-scoped endpoints - burst protection only
      EndpointCategory.GRAPH_READ: (100, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_WRITE: (20, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_ANALYTICS: (10, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_BACKUP: (2, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_SYNC: (5, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_MCP: (10, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_AGENT: (5, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_QUERY: (50, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_IMPORT: (2, RateLimitPeriod.MINUTE),
      # Table operations - free tier
      EndpointCategory.TABLE_QUERY: (30, RateLimitPeriod.MINUTE),
      EndpointCategory.TABLE_UPLOAD: (10, RateLimitPeriod.MINUTE),
      EndpointCategory.TABLE_MANAGEMENT: (10, RateLimitPeriod.MINUTE),
    },
    # Technical tier names (primary)
    "kuzu-standard": {
      # Non-graph endpoints - generous burst limits
      EndpointCategory.AUTH: (20, RateLimitPeriod.MINUTE),
      EndpointCategory.USER_MANAGEMENT: (600, RateLimitPeriod.MINUTE),
      EndpointCategory.TASKS: (200, RateLimitPeriod.MINUTE),
      EndpointCategory.STATUS: (600, RateLimitPeriod.MINUTE),
      EndpointCategory.SSE: (
        10,
        RateLimitPeriod.MINUTE,
      ),  # Standard SSE connection rate
      # Graph-scoped endpoints - HIGH BURST LIMITS
      EndpointCategory.GRAPH_READ: (500, RateLimitPeriod.MINUTE),  # 30k/hour possible
      EndpointCategory.GRAPH_WRITE: (100, RateLimitPeriod.MINUTE),  # 6k/hour possible
      EndpointCategory.GRAPH_ANALYTICS: (
        50,
        RateLimitPeriod.MINUTE,
      ),  # 3k/hour possible
      EndpointCategory.GRAPH_BACKUP: (10, RateLimitPeriod.MINUTE),  # 600/hour possible
      EndpointCategory.GRAPH_SYNC: (100, RateLimitPeriod.MINUTE),  # 6k/hour possible
      EndpointCategory.GRAPH_MCP: (100, RateLimitPeriod.MINUTE),  # 6k/hour possible
      EndpointCategory.GRAPH_AGENT: (50, RateLimitPeriod.MINUTE),  # 3k/hour possible
      EndpointCategory.GRAPH_QUERY: (200, RateLimitPeriod.MINUTE),  # 12k/hour possible
      EndpointCategory.GRAPH_IMPORT: (50, RateLimitPeriod.MINUTE),  # 3k/hour possible
      # Table operations - standard tier (generous burst limits)
      EndpointCategory.TABLE_QUERY: (60, RateLimitPeriod.MINUTE),  # 3.6k/hour possible
      EndpointCategory.TABLE_UPLOAD: (20, RateLimitPeriod.MINUTE),  # 1.2k/hour possible
      EndpointCategory.TABLE_MANAGEMENT: (
        30,
        RateLimitPeriod.MINUTE,
      ),  # 1.8k/hour possible
    },
    "kuzu-large": {
      # Non-graph endpoints - very high burst limits
      EndpointCategory.AUTH: (50, RateLimitPeriod.MINUTE),
      EndpointCategory.USER_MANAGEMENT: (1000, RateLimitPeriod.MINUTE),
      EndpointCategory.TASKS: (1000, RateLimitPeriod.MINUTE),
      EndpointCategory.STATUS: (3000, RateLimitPeriod.MINUTE),
      EndpointCategory.SSE: (
        30,
        RateLimitPeriod.MINUTE,
      ),  # More SSE connections for large tier
      # Graph-scoped endpoints - VERY HIGH BURST LIMITS
      EndpointCategory.GRAPH_READ: (2000, RateLimitPeriod.MINUTE),  # 120k/hour possible
      EndpointCategory.GRAPH_WRITE: (500, RateLimitPeriod.MINUTE),  # 30k/hour possible
      EndpointCategory.GRAPH_ANALYTICS: (
        200,
        RateLimitPeriod.MINUTE,
      ),  # 12k/hour possible
      EndpointCategory.GRAPH_BACKUP: (50, RateLimitPeriod.MINUTE),  # 3k/hour possible
      EndpointCategory.GRAPH_SYNC: (500, RateLimitPeriod.MINUTE),  # 30k/hour possible
      EndpointCategory.GRAPH_MCP: (500, RateLimitPeriod.MINUTE),  # 30k/hour possible
      EndpointCategory.GRAPH_AGENT: (200, RateLimitPeriod.MINUTE),  # 12k/hour possible
      EndpointCategory.GRAPH_QUERY: (1000, RateLimitPeriod.MINUTE),  # 60k/hour possible
      EndpointCategory.GRAPH_IMPORT: (200, RateLimitPeriod.MINUTE),  # 12k/hour possible
      # Table operations - large tier (very high burst limits)
      EndpointCategory.TABLE_QUERY: (300, RateLimitPeriod.MINUTE),  # 18k/hour possible
      EndpointCategory.TABLE_UPLOAD: (100, RateLimitPeriod.MINUTE),  # 6k/hour possible
      EndpointCategory.TABLE_MANAGEMENT: (
        150,
        RateLimitPeriod.MINUTE,
      ),  # 9k/hour possible
    },
    "kuzu-xlarge": {
      # Premium gets extreme burst limits - essentially unlimited
      # Only safety limits to prevent complete system abuse
      EndpointCategory.AUTH: (100, RateLimitPeriod.MINUTE),
      EndpointCategory.USER_MANAGEMENT: (3000, RateLimitPeriod.MINUTE),
      EndpointCategory.TASKS: (5000, RateLimitPeriod.MINUTE),
      EndpointCategory.STATUS: (10000, RateLimitPeriod.MINUTE),
      EndpointCategory.SSE: (
        100,
        RateLimitPeriod.MINUTE,
      ),  # Generous SSE connections for premium
      # Graph-scoped endpoints - EXTREME BURST LIMITS
      EndpointCategory.GRAPH_READ: (
        10000,
        RateLimitPeriod.MINUTE,
      ),  # 600k/hour possible
      EndpointCategory.GRAPH_WRITE: (
        5000,
        RateLimitPeriod.MINUTE,
      ),  # 300k/hour possible
      EndpointCategory.GRAPH_ANALYTICS: (
        2000,
        RateLimitPeriod.MINUTE,
      ),  # 120k/hour possible
      EndpointCategory.GRAPH_BACKUP: (200, RateLimitPeriod.MINUTE),  # 12k/hour possible
      EndpointCategory.GRAPH_SYNC: (2000, RateLimitPeriod.MINUTE),  # 120k/hour possible
      EndpointCategory.GRAPH_MCP: (5000, RateLimitPeriod.MINUTE),  # 300k/hour possible
      EndpointCategory.GRAPH_AGENT: (
        2000,
        RateLimitPeriod.MINUTE,
      ),  # 120k/hour possible
      EndpointCategory.GRAPH_QUERY: (
        10000,
        RateLimitPeriod.MINUTE,
      ),  # 600k/hour possible
      EndpointCategory.GRAPH_IMPORT: (
        1000,
        RateLimitPeriod.MINUTE,
      ),  # 60k/hour possible
      # Table operations - xlarge tier (extreme burst limits)
      EndpointCategory.TABLE_QUERY: (
        1000,
        RateLimitPeriod.MINUTE,
      ),  # 60k/hour possible
      EndpointCategory.TABLE_UPLOAD: (
        500,
        RateLimitPeriod.MINUTE,
      ),  # 30k/hour possible
      EndpointCategory.TABLE_MANAGEMENT: (
        500,
        RateLimitPeriod.MINUTE,
      ),  # 30k/hour possible
    },
  }

  # Add legacy tier name mappings directly in the class after definition
  SUBSCRIPTION_RATE_LIMITS["standard"] = SUBSCRIPTION_RATE_LIMITS["kuzu-standard"]
  SUBSCRIPTION_RATE_LIMITS["enterprise"] = SUBSCRIPTION_RATE_LIMITS["kuzu-large"]
  SUBSCRIPTION_RATE_LIMITS["premium"] = SUBSCRIPTION_RATE_LIMITS["kuzu-xlarge"]

  @classmethod
  def get_rate_limit(
    cls, tier: str, category: EndpointCategory
  ) -> Optional[Tuple[int, int]]:
    """
    Get rate limit for a subscription tier and endpoint category.

    Returns:
        Tuple of (limit, window_seconds) or None if not configured
    """
    tier_limits = cls.SUBSCRIPTION_RATE_LIMITS.get(tier)
    if not tier_limits:
      # Default to free tier if unknown
      tier_limits = cls.SUBSCRIPTION_RATE_LIMITS["free"]

    limit_config = tier_limits.get(category)
    if not limit_config:
      return None

    limit, period = limit_config
    return limit, period.to_seconds()

  @classmethod
  def get_rate_limit_with_multiplier(
    cls, tier: str, category: EndpointCategory, use_tier_config: bool = True
  ) -> Optional[Tuple[int, int]]:
    """
    Get rate limit with optional tier config multiplier applied.

    Args:
        tier: Subscription tier
        category: Endpoint category
        use_tier_config: If True, apply multiplier from tier config

    Returns:
        Tuple of (limit, window_seconds) or None if not configured
    """
    # Get base rate limit
    base_result = cls.get_rate_limit(tier, category)
    if not base_result:
      return None

    base_limit, window_seconds = base_result

    # Apply tier config multiplier if enabled
    if use_tier_config:
      multiplier = get_tier_api_rate_multiplier(tier)
      adjusted_limit = int(base_limit * multiplier)
      return adjusted_limit, window_seconds

    return base_limit, window_seconds

  @classmethod
  def get_endpoint_category(
    cls, path: str, method: str = "GET"
  ) -> Optional[EndpointCategory]:
    """
    Determine the category of an endpoint based on its path and method.

    Args:
        path: The API endpoint path
        method: HTTP method

    Returns:
        The endpoint category or None if not categorized
    """
    # Remove version prefix
    if path.startswith("/v1/"):
      path = path[4:]

    # Non-graph scoped endpoints - check these first
    if path.startswith("auth/"):
      return EndpointCategory.AUTH
    elif path.startswith("user/"):
      return EndpointCategory.USER_MANAGEMENT
    elif path.startswith("tasks/"):
      return EndpointCategory.TASKS
    elif path.startswith("status/") or path == "health":
      return EndpointCategory.STATUS
    elif "operations" in path and "stream" in path:
      return EndpointCategory.SSE

    # Check if it's a graph-scoped endpoint
    path_parts = path.strip("/").split("/")

    # Graph-scoped endpoints (format: /graphs/{graph_id}/...)
    if len(path_parts) >= 2 and path_parts[0] == "graphs":
      # For graph-scoped endpoints, endpoint_type is the part after graph_id
      # path_parts: ['graphs', '{graph_id}', 'endpoint_type', ...]
      endpoint_type = path_parts[2] if len(path_parts) >= 3 else None

      # Table operations (DuckDB staging tables) - check first for specificity
      if endpoint_type == "tables" or "/tables/" in path:
        if "query" in path:
          return EndpointCategory.TABLE_QUERY
        elif "ingest" in path:
          return EndpointCategory.GRAPH_IMPORT  # Table ingestion is bulk import
        elif "/files" in path and method in ["POST", "PUT"]:
          return EndpointCategory.TABLE_UPLOAD
        elif method in ["POST", "PUT", "DELETE", "PATCH"]:
          return EndpointCategory.TABLE_MANAGEMENT
        else:
          return EndpointCategory.GRAPH_READ  # Table listing/info

      # MCP and Agent endpoints
      elif endpoint_type == "mcp":
        return EndpointCategory.GRAPH_MCP
      elif endpoint_type == "agent":
        return EndpointCategory.GRAPH_AGENT

      # Backup operations
      elif endpoint_type == "graph" and "backup" in path:
        return EndpointCategory.GRAPH_BACKUP

      # Direct queries
      elif endpoint_type == "graph" and "query" in path:
        return EndpointCategory.GRAPH_QUERY

      # Analytics
      elif endpoint_type == "graph" and "analytics" in path:
        return EndpointCategory.GRAPH_ANALYTICS

      # Sync operations
      elif endpoint_type in ["sync", "connections"]:
        return EndpointCategory.GRAPH_SYNC

      # Import operations
      elif "import" in path or "ingest" in path:
        return EndpointCategory.GRAPH_IMPORT

      # Write operations (POST, PUT, DELETE)
      elif method in ["POST", "PUT", "DELETE", "PATCH"]:
        return EndpointCategory.GRAPH_WRITE

      # Default to read for other graph operations
      else:
        return EndpointCategory.GRAPH_READ

    return None
