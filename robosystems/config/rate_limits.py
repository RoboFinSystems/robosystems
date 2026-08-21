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
  # Categories served by the customer's own dedicated LadybugDB instance.
  # These scale by tier and bucket per graph; everything else is shared
  # infrastructure, stays flat, and buckets per user. See the comment on
  # SUBSCRIPTION_RATE_LIMITS below for why the distinction matters.
  #
  # GRAPH_SEARCH is deliberately absent: it hits the shared OpenSearch cluster,
  # not the tenant's instance. GRAPH_IMPORT is absent because LadybugDB ingests
  # sequentially regardless of tier, so extra cores buy nothing there.
  # GRAPH_BACKUP, GRAPH_MANAGEMENT, and GRAPH_SYNC are also deliberately
  # absent: they are low-frequency control-plane operations whose cost lands
  # on shared infrastructure — S3 for backups, the platform database and
  # orchestration for management, external providers via the shared worker
  # pool for sync — not on the tenant's own cores, so tier vCPU is not the
  # scaling axis for any of them.
  DEDICATED_RESOURCE_CATEGORIES: frozenset[EndpointCategory] = frozenset(
    {
      EndpointCategory.GRAPH_QUERY,
      EndpointCategory.GRAPH_READ,
      EndpointCategory.GRAPH_WRITE,
      EndpointCategory.GRAPH_MCP,
      EndpointCategory.GRAPH_OPERATOR,
      EndpointCategory.GRAPH_ANALYTICS,
    }
  )

  SUBSCRIPTION_RATE_LIMITS: dict[
    str, dict[EndpointCategory, tuple[int, RateLimitPeriod]]
  ] = {
    # -----------------------------------------------------------------------
    # MANAGED SERVICE RATE LIMITS
    #
    # Two kinds of limit live in this table, and they differ by what they
    # protect:
    #
    #   Dedicated  — categories in DEDICATED_RESOURCE_CATEGORIES hit the
    #                customer's own LadybugDB instance. Every customer tier is
    #                databases_per_instance: 1, so a heavy tenant harms only
    #                themselves. These scale with the tier's vCPU count
    #                (m7g.medium 1 → m7g.large 2 → r7g.xlarge 4), because read
    #                throughput is what the extra cores actually buy.
    #
    #   Shared     — everything else lands on infrastructure every tenant
    #                shares: OpenSearch (t3.medium), the extensions RDS, the
    #                API tier itself. These stay flat across tiers and bucket
    #                per user, so buying more graphs cannot multiply one
    #                customer's load on a resource others depend on.
    #
    # An earlier version of this comment described LadybugDB as shared, which
    # is what kept every tier on identical limits. It is not shared, and that
    # is the whole reason the dedicated half can differentiate at all.
    #
    # Admission control (85% of instance CPU/memory) remains the real overload
    # backstop; these numbers are a product lever, not the safety mechanism.
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
    # ladybug-standard: m7g.medium (4GB, 1 vCPU) — the anchor the other tiers
    # multiply from. Sized for m7g.large and deliberately retained after the
    # resize: cutting limits on existing customers would be a service
    # regression in exchange for protection admission control already provides.
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
    # ladybug-large: m7g.large (8GB, 2 vCPU) — 2x Standard's vCPU, so the
    # dedicated-resource categories double. Shared categories match Standard.
    "ladybug-large": {
      EndpointCategory.AUTH: (20, RateLimitPeriod.MINUTE),
      EndpointCategory.USER_MANAGEMENT: (60, RateLimitPeriod.MINUTE),
      EndpointCategory.TASKS: (60, RateLimitPeriod.MINUTE),
      EndpointCategory.STATUS: (120, RateLimitPeriod.MINUTE),
      EndpointCategory.SSE: (5, RateLimitPeriod.MINUTE),
      EndpointCategory.BILLING: (60, RateLimitPeriod.MINUTE),  # Never block payments
      # Dedicated instance — 2x Standard (2 vCPU vs 1)
      EndpointCategory.GRAPH_READ: (240, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_WRITE: (60, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_ANALYTICS: (30, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_BACKUP: (5, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_MANAGEMENT: (10, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_SYNC: (10, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_MCP: (60, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_OPERATOR: (30, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_SEARCH: (
        10,
        RateLimitPeriod.MINUTE,
      ),  # Shared OpenSearch t3.medium
      EndpointCategory.GRAPH_QUERY: (120, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_IMPORT: (10, RateLimitPeriod.MINUTE),
      # Extensions surface (OLTP on shared RDS)
      EndpointCategory.EXTENSIONS_GRAPHQL: (600, RateLimitPeriod.MINUTE),
      EndpointCategory.EXTENSIONS_WRITE: (300, RateLimitPeriod.MINUTE),
      # Table operations
      EndpointCategory.TABLE_QUERY: (30, RateLimitPeriod.MINUTE),
      EndpointCategory.TABLE_UPLOAD: (10, RateLimitPeriod.MINUTE),
      EndpointCategory.TABLE_MANAGEMENT: (15, RateLimitPeriod.MINUTE),
    },
    # ladybug-xlarge: r7g.xlarge (32GB, 4 vCPU) — 4x Standard's vCPU, so the
    # dedicated-resource categories quadruple. Shared categories match Standard.
    "ladybug-xlarge": {
      EndpointCategory.AUTH: (20, RateLimitPeriod.MINUTE),
      EndpointCategory.USER_MANAGEMENT: (60, RateLimitPeriod.MINUTE),
      EndpointCategory.TASKS: (60, RateLimitPeriod.MINUTE),
      EndpointCategory.STATUS: (120, RateLimitPeriod.MINUTE),
      EndpointCategory.SSE: (5, RateLimitPeriod.MINUTE),
      EndpointCategory.BILLING: (60, RateLimitPeriod.MINUTE),  # Never block payments
      # Dedicated instance — 4x Standard (4 vCPU vs 1)
      EndpointCategory.GRAPH_READ: (480, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_WRITE: (120, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_ANALYTICS: (60, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_BACKUP: (5, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_MANAGEMENT: (10, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_SYNC: (10, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_MCP: (120, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_OPERATOR: (60, RateLimitPeriod.MINUTE),
      EndpointCategory.GRAPH_SEARCH: (
        10,
        RateLimitPeriod.MINUTE,
      ),  # Shared OpenSearch t3.medium
      EndpointCategory.GRAPH_QUERY: (240, RateLimitPeriod.MINUTE),
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

    # Non-graph-scoped schema validation (/graphs/schema/validate) — validate a
    # candidate schema BEFORE a graph exists, so it carries no graph_id. It is
    # read-like, so use the graph-read bucket rather than the default POST write
    # bucket. Unambiguous: a graph named "schema" would be /graphs/schema/schema/…
    if path_parts[:3] == ["graphs", "schema", "validate"]:
      return EndpointCategory.GRAPH_READ

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

      # Query layer — POST /v1/graphs/{graph_id}/query/{cypher,sql}, so the
      # segment after graph_id (endpoint_type) is "query" and the language is
      # the next segment. Cypher → GRAPH_QUERY; SQL → the DuckDB/columnar
      # bucket (TABLE_QUERY).
      elif endpoint_type == "query":
        if len(path_parts) >= 4 and path_parts[3] == "sql":
          return EndpointCategory.TABLE_QUERY
        return EndpointCategory.GRAPH_QUERY

      # Content metrics + consumption usage — aggregation-heavy reads get a
      # dedicated bucket. Match on the endpoint segment as it actually appears
      # in the path. Keep this in step with routers/graphs/usage.py.
      elif endpoint_type in ("metrics", "usage"):
        return EndpointCategory.GRAPH_ANALYTICS

      # Graph lifecycle operations — POST /operations/{op_name}. Dedicated
      # buckets so expensive rebuild/backup work can't be abused by sharing
      # the generic write limit. Match the op-name segment specifically rather
      # than the whole path, so a graph_id can't accidentally route the bucket.
      elif endpoint_type == "operations":
        op_name = path_parts[3] if len(path_parts) > 3 else ""
        if "backup" in op_name:  # create-backup
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
