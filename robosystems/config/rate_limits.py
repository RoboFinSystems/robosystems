"""
Centralized rate limiting configuration.

This module contains all rate limit definitions for different subscription tiers
and endpoint categories.
"""

from enum import Enum

from .graph_tier import get_tier_api_rate_multiplier


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
  BILLING = "billing"  # Checkout and payment flows — never block paying customers

  # Graph-scoped endpoints
  GRAPH_READ = "graph_read"
  GRAPH_WRITE = "graph_write"
  GRAPH_ANALYTICS = "graph_analytics"
  GRAPH_BACKUP = "graph_backup"
  GRAPH_SYNC = "graph_sync"
  GRAPH_MCP = "graph_mcp"
  GRAPH_AGENT = "graph_agent"
  GRAPH_SEARCH = "graph_search"  # OpenSearch full-text search (shared resource)

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
  SUBSCRIPTION_RATE_LIMITS: dict[
    str, dict[EndpointCategory, tuple[int, RateLimitPeriod]]
  ] = {
    # -----------------------------------------------------------------------
    # MANAGED SERVICE RATE LIMITS
    # All tiers share managed infrastructure. Limits are conservative to
    # protect shared resources (OpenSearch t3.medium, LadybugDB on m7g/r7g).
    # For self-hosted scale, customers deploy their own infrastructure.
    # Loosen these as infra scales up.
    # -----------------------------------------------------------------------
    "base": {
      # Anonymous / unrecognized tier — tightest limits
      EndpointCategory.AUTH: (10, RateLimitPeriod.MINUTE),
      EndpointCategory.USER_MANAGEMENT: (30, RateLimitPeriod.MINUTE),
      EndpointCategory.TASKS: (30, RateLimitPeriod.MINUTE),
      EndpointCategory.STATUS: (60, RateLimitPeriod.MINUTE),
      EndpointCategory.SSE: (3, RateLimitPeriod.MINUTE),
      EndpointCategory.BILLING: (60, RateLimitPeriod.MINUTE),  # Never block payments
      # Graph-scoped
      EndpointCategory.GRAPH_READ: (30, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_WRITE: (10, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_ANALYTICS: (5, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_BACKUP: (2, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_SYNC: (3, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_MCP: (5, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_AGENT: (3, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_SEARCH: (5, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_QUERY: (20, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_IMPORT: (2, RateLimitPeriod.MINUTE),
      # Table operations
      EndpointCategory.TABLE_QUERY: (15, RateLimitPeriod.MINUTE),
      EndpointCategory.TABLE_UPLOAD: (5, RateLimitPeriod.MINUTE),
      EndpointCategory.TABLE_MANAGEMENT: (5, RateLimitPeriod.MINUTE),
    },
    # ladybug-standard: m7g.large (8GB, 2 vCPU) — anchor tier
    "ladybug-standard": {
      EndpointCategory.AUTH: (20, RateLimitPeriod.MINUTE),
      EndpointCategory.USER_MANAGEMENT: (60, RateLimitPeriod.MINUTE),
      EndpointCategory.TASKS: (60, RateLimitPeriod.MINUTE),
      EndpointCategory.STATUS: (120, RateLimitPeriod.MINUTE),
      EndpointCategory.SSE: (5, RateLimitPeriod.MINUTE),
      EndpointCategory.BILLING: (60, RateLimitPeriod.MINUTE),  # Never block payments
      # Graph-scoped — sized for m7g.large
      EndpointCategory.GRAPH_READ: (120, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_WRITE: (30, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_ANALYTICS: (15, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_BACKUP: (5, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_SYNC: (10, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_MCP: (30, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_AGENT: (15, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_SEARCH: (
        10,
        RateLimitPeriod.MINUTE,
      ),  # Shared OpenSearch t3.medium
      EndpointCategory.GRAPH_QUERY: (60, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_IMPORT: (10, RateLimitPeriod.MINUTE),
      # Table operations
      EndpointCategory.TABLE_QUERY: (30, RateLimitPeriod.MINUTE),
      EndpointCategory.TABLE_UPLOAD: (10, RateLimitPeriod.MINUTE),
      EndpointCategory.TABLE_MANAGEMENT: (15, RateLimitPeriod.MINUTE),
    },
    # ladybug-large: r7g.large (16GB, 2 vCPU)
    # Same base values as standard — graph.yml api_rate_multiplier (1.5x) handles scaling
    "ladybug-large": {
      EndpointCategory.AUTH: (20, RateLimitPeriod.MINUTE),
      EndpointCategory.USER_MANAGEMENT: (60, RateLimitPeriod.MINUTE),
      EndpointCategory.TASKS: (60, RateLimitPeriod.MINUTE),
      EndpointCategory.STATUS: (120, RateLimitPeriod.MINUTE),
      EndpointCategory.SSE: (5, RateLimitPeriod.MINUTE),
      EndpointCategory.BILLING: (60, RateLimitPeriod.MINUTE),  # Never block payments
      # Graph-scoped — same base, multiplied by 1.5x from graph.yml
      EndpointCategory.GRAPH_READ: (120, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_WRITE: (30, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_ANALYTICS: (15, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_BACKUP: (5, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_SYNC: (10, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_MCP: (30, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_AGENT: (15, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_SEARCH: (
        10,
        RateLimitPeriod.MINUTE,
      ),  # Shared OpenSearch t3.medium
      EndpointCategory.GRAPH_QUERY: (60, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_IMPORT: (10, RateLimitPeriod.MINUTE),
      # Table operations
      EndpointCategory.TABLE_QUERY: (30, RateLimitPeriod.MINUTE),
      EndpointCategory.TABLE_UPLOAD: (10, RateLimitPeriod.MINUTE),
      EndpointCategory.TABLE_MANAGEMENT: (15, RateLimitPeriod.MINUTE),
    },
    # ladybug-xlarge: r7g.xlarge (32GB, 4 vCPU)
    # Same base values as standard — graph.yml api_rate_multiplier (2.5x) handles scaling
    "ladybug-xlarge": {
      EndpointCategory.AUTH: (20, RateLimitPeriod.MINUTE),
      EndpointCategory.USER_MANAGEMENT: (60, RateLimitPeriod.MINUTE),
      EndpointCategory.TASKS: (60, RateLimitPeriod.MINUTE),
      EndpointCategory.STATUS: (120, RateLimitPeriod.MINUTE),
      EndpointCategory.SSE: (5, RateLimitPeriod.MINUTE),
      EndpointCategory.BILLING: (60, RateLimitPeriod.MINUTE),  # Never block payments
      # Graph-scoped — same base, multiplied by 2.5x from graph.yml
      EndpointCategory.GRAPH_READ: (120, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_WRITE: (30, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_ANALYTICS: (15, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_BACKUP: (5, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_SYNC: (10, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_MCP: (30, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_AGENT: (15, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_SEARCH: (
        10,
        RateLimitPeriod.MINUTE,
      ),  # Shared OpenSearch t3.medium
      EndpointCategory.GRAPH_QUERY: (60, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_IMPORT: (10, RateLimitPeriod.MINUTE),
      # Table operations
      EndpointCategory.TABLE_QUERY: (30, RateLimitPeriod.MINUTE),
      EndpointCategory.TABLE_UPLOAD: (10, RateLimitPeriod.MINUTE),
      EndpointCategory.TABLE_MANAGEMENT: (15, RateLimitPeriod.MINUTE),
    },
  }

  @classmethod
  def get_rate_limit(
    cls, tier: str, category: EndpointCategory
  ) -> tuple[int, int] | None:
    """
    Get rate limit for a subscription tier and endpoint category.

    Returns:
        Tuple of (limit, window_seconds) or None if not configured
    """
    tier_limits = cls.SUBSCRIPTION_RATE_LIMITS.get(tier)
    if not tier_limits:
      # Default to base tier if unknown
      tier_limits = cls.SUBSCRIPTION_RATE_LIMITS["base"]

    limit_config = tier_limits.get(category)
    if not limit_config:
      return None

    limit, period = limit_config
    return limit, period.to_seconds()

  @classmethod
  def get_rate_limit_with_multiplier(
    cls, tier: str, category: EndpointCategory, use_tier_config: bool = True
  ) -> tuple[int, int] | None:
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
  ) -> EndpointCategory | None:
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
    elif path.startswith("billing/"):
      return EndpointCategory.BILLING
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

      # Files operations (first-class resources or nested under tables)
      if endpoint_type == "files" or "/files" in path:
        if method in ["POST", "PUT"]:
          return EndpointCategory.TABLE_UPLOAD
        elif method in ["DELETE", "PATCH"]:
          return EndpointCategory.TABLE_MANAGEMENT
        else:
          return EndpointCategory.GRAPH_READ  # File listing/info

      # Table operations (DuckDB staging tables)
      if endpoint_type == "tables" or "/tables/" in path:
        if "query" in path:
          return EndpointCategory.TABLE_QUERY
        elif "ingest" in path:
          return EndpointCategory.GRAPH_IMPORT  # Table ingestion is bulk import
        elif method in ["POST", "PUT", "DELETE", "PATCH"]:
          return EndpointCategory.TABLE_MANAGEMENT
        else:
          return EndpointCategory.GRAPH_READ  # Table listing/info

      # MCP and Agent endpoints
      elif endpoint_type == "mcp":
        return EndpointCategory.GRAPH_MCP
      elif endpoint_type == "agent":
        return EndpointCategory.GRAPH_AGENT

      # Search operations (OpenSearch - shared resource)
      elif endpoint_type == "search":
        return EndpointCategory.GRAPH_SEARCH

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
