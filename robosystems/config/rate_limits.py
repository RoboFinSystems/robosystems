"""
Centralized rate limiting configuration.

This module contains all rate limit definitions for different subscription tiers
and endpoint categories.
"""

from enum import Enum


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
  GRAPH_MANAGEMENT = "graph_management"  # Lifecycle ops (subgraph/tier/style)
  GRAPH_SYNC = "graph_sync"
  GRAPH_MCP = "graph_mcp"
  GRAPH_OPERATOR = "graph_operator"
  GRAPH_SEARCH = "graph_search"  # OpenSearch full-text search (shared resource)

  # High-cost operations
  GRAPH_QUERY = "graph_query"  # Direct Cypher queries
  GRAPH_IMPORT = "graph_import"  # Bulk data imports

  # Extensions surface (OLTP on shared RDS — distinct from LadybugDB categories)
  EXTENSIONS_GRAPHQL = (
    "extensions_graphql"  # Typed GraphQL reads: /extensions/{g}/graphql
  )
  EXTENSIONS_WRITE = (
    "extensions_write"  # Command writes + views: /extensions/{d}/{g}/operations/*
  )

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
      EndpointCategory.GRAPH_MANAGEMENT: (3, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_SYNC: (3, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_MCP: (5, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_OPERATOR: (3, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_SEARCH: (5, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_QUERY: (20, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_IMPORT: (2, RateLimitPeriod.MINUTE),
      # Extensions surface (OLTP on shared RDS)
      EndpointCategory.EXTENSIONS_GRAPHQL: (60, RateLimitPeriod.MINUTE),
      EndpointCategory.EXTENSIONS_WRITE: (30, RateLimitPeriod.MINUTE),
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
      EndpointCategory.GRAPH_MANAGEMENT: (10, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_SYNC: (10, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_MCP: (30, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_OPERATOR: (15, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_SEARCH: (
        10,
        RateLimitPeriod.MINUTE,
      ),  # Shared OpenSearch t3.medium
      EndpointCategory.GRAPH_QUERY: (60, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_IMPORT: (10, RateLimitPeriod.MINUTE),
      # Extensions surface (OLTP on shared RDS)
      EndpointCategory.EXTENSIONS_GRAPHQL: (600, RateLimitPeriod.MINUTE),
      EndpointCategory.EXTENSIONS_WRITE: (300, RateLimitPeriod.MINUTE),
      # Table operations
      EndpointCategory.TABLE_QUERY: (30, RateLimitPeriod.MINUTE),
      EndpointCategory.TABLE_UPLOAD: (10, RateLimitPeriod.MINUTE),
      EndpointCategory.TABLE_MANAGEMENT: (15, RateLimitPeriod.MINUTE),
    },
    # ladybug-large: r7g.large (16GB, 2 vCPU)
    # Identical enforcement to standard: the API limiter classifies all
    # authenticated users as ladybug-standard, so these per-tier tables are
    # reference/parity only. Burst protection is deliberately tier-flat;
    # per-tier volume is governed by credits, not by a rate-limit multiplier.
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
      EndpointCategory.GRAPH_MANAGEMENT: (10, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_SYNC: (10, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_MCP: (30, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_OPERATOR: (15, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_SEARCH: (
        10,
        RateLimitPeriod.MINUTE,
      ),  # Shared OpenSearch t3.medium
      EndpointCategory.GRAPH_QUERY: (60, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_IMPORT: (10, RateLimitPeriod.MINUTE),
      # Extensions surface (OLTP on shared RDS)
      EndpointCategory.EXTENSIONS_GRAPHQL: (600, RateLimitPeriod.MINUTE),
      EndpointCategory.EXTENSIONS_WRITE: (300, RateLimitPeriod.MINUTE),
      # Table operations
      EndpointCategory.TABLE_QUERY: (30, RateLimitPeriod.MINUTE),
      EndpointCategory.TABLE_UPLOAD: (10, RateLimitPeriod.MINUTE),
      EndpointCategory.TABLE_MANAGEMENT: (15, RateLimitPeriod.MINUTE),
    },
    # ladybug-xlarge: r7g.xlarge (32GB, 4 vCPU)
    # Identical enforcement to standard (see ladybug-large note): reference/
    # parity only; the limiter treats all authenticated users as standard.
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
      EndpointCategory.GRAPH_MANAGEMENT: (10, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_SYNC: (10, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_MCP: (30, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_OPERATOR: (15, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_SEARCH: (
        10,
        RateLimitPeriod.MINUTE,
      ),  # Shared OpenSearch t3.medium
      EndpointCategory.GRAPH_QUERY: (60, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_IMPORT: (10, RateLimitPeriod.MINUTE),
      # Extensions surface (OLTP on shared RDS)
      EndpointCategory.EXTENSIONS_GRAPHQL: (600, RateLimitPeriod.MINUTE),
      EndpointCategory.EXTENSIONS_WRITE: (300, RateLimitPeriod.MINUTE),
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
    # Extensions surface — OLTP on shared RDS, so it gets its own buckets
    # (EXTENSIONS_GRAPHQL / EXTENSIONS_WRITE) independent of the LadybugDB
    # graph categories, so the two backends can be tuned separately.
    # Checked BEFORE the `/v1/` prefix strip because the extensions
    # surface is mounted at the top level, not under `/v1/`.
    if path.startswith("/extensions/"):
      ext_parts = path[len("/extensions/") :].split("/")
      # /extensions/{graph_id}/graphql → EXTENSIONS_GRAPHQL (typed OLTP reads)
      if len(ext_parts) >= 2 and ext_parts[1] == "graphql":
        return EndpointCategory.EXTENSIONS_GRAPHQL
      # /extensions/{domain}/{graph_id}/operations/{op_name} → EXTENSIONS_WRITE
      # (command writes; analytical view operations ride here too for now).
      if len(ext_parts) >= 4 and ext_parts[2] == "operations":
        return EndpointCategory.EXTENSIONS_WRITE
      # Any other extensions endpoint (e.g. report bundle download) → the
      # extensions write bucket rather than dropping to the unbounded default.
      return EndpointCategory.EXTENSIONS_WRITE

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

      # MCP and Operator endpoints
      elif endpoint_type == "mcp":
        return EndpointCategory.GRAPH_MCP
      elif endpoint_type == "operator":
        return EndpointCategory.GRAPH_OPERATOR

      # Search operations (OpenSearch - shared resource)
      elif endpoint_type == "search":
        return EndpointCategory.GRAPH_SEARCH

      # Schema inspection / export / validation — all read-only
      elif endpoint_type == "schema":
        return EndpointCategory.GRAPH_READ

      # Direct Cypher queries — POST /v1/graphs/{graph_id}/query, so the
      # segment after graph_id (endpoint_type) is "query". This previously
      # checked `endpoint_type == "graph"`, which never matches the real
      # route, so every query (including read-only shared repos like SEC)
      # fell through to the GRAPH_WRITE default below and reported the
      # misleading "graph write operations" limit.
      elif endpoint_type == "query":
        return EndpointCategory.GRAPH_QUERY

      # Usage analytics — aggregation-heavy reads get a dedicated bucket
      # (the endpoint segment is "analytics"; this previously checked
      # `endpoint_type == "graph"` and never matched).
      elif endpoint_type == "analytics":
        return EndpointCategory.GRAPH_ANALYTICS

      # Graph lifecycle operations — POST /operations/{op_name}. Dedicated
      # buckets so expensive rebuild/backup work can't be abused by sharing
      # the generic write limit. Match the op-name segment specifically rather
      # than the whole path, so a graph_id can't accidentally route the bucket.
      elif endpoint_type == "operations":
        op_name = path_parts[3] if len(path_parts) > 3 else ""
        if "backup" in op_name:  # create-backup, restore-backup
          return EndpointCategory.GRAPH_BACKUP
        elif "materialize" in op_name:  # heavy OLAP rebuild
          return EndpointCategory.GRAPH_IMPORT
        else:  # create/delete subgraph, delete-graph, change-tier/style
          return EndpointCategory.GRAPH_MANAGEMENT

      # Connection management (reads/mutations) + the data-sync trigger.
      # Only the actual /sync call belongs in the tight sync bucket; listing
      # connections, options, and OAuth callbacks are ordinary reads/writes.
      elif endpoint_type == "connections":
        if "sync" in path:
          return EndpointCategory.GRAPH_SYNC
        elif method in ["POST", "PUT", "DELETE", "PATCH"]:
          return EndpointCategory.GRAPH_WRITE
        else:
          return EndpointCategory.GRAPH_READ

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
