"""Rate limits per subscription tier and endpoint category."""

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

  DEFAULT_LIMIT = (100, RateLimitPeriod.HOUR)

  WINDOW_SIZE_SECONDS = {
    RateLimitPeriod.MINUTE: 60,
    RateLimitPeriod.HOUR: 3600,
    RateLimitPeriod.DAY: 86400,
  }

  # Burst allowance multiplier (allows short bursts above limit)
  BURST_MULTIPLIER = 1.2

  RATE_LIMIT_HEADERS = {
    "limit": "X-RateLimit-Limit",
    "remaining": "X-RateLimit-Remaining",
    "reset": "X-RateLimit-Reset",
    "retry_after": "Retry-After",
  }

  # Categories served by the customer's own LadybugDB instance: they scale by
  # tier and bucket per graph (see SUBSCRIPTION_RATE_LIMITS). Deliberately
  # absent: GRAPH_SEARCH (shared OpenSearch), GRAPH_IMPORT (ingestion is
  # sequential whatever the cores), and the control-plane BACKUP/MANAGEMENT/
  # SYNC, whose cost lands on shared infrastructure.
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
    # Burst protection in one-minute windows; volume is governed by credits.
    #
    # Dedicated categories hit the customer's own instance (one database per
    # instance), so they scale with the tier's vCPU count. Shared categories
    # stay flat and bucket per user, so buying more graphs cannot multiply one
    # customer's load on shared infrastructure. Admission control, not these
    # numbers, is the overload backstop.
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
    # ladybug-standard (1 vCPU): the anchor the other tiers multiply from.
    "ladybug-standard": {
      EndpointCategory.AUTH: (20, RateLimitPeriod.MINUTE),
      EndpointCategory.USER_MANAGEMENT: (60, RateLimitPeriod.MINUTE),
      EndpointCategory.TASKS: (60, RateLimitPeriod.MINUTE),
      EndpointCategory.STATUS: (120, RateLimitPeriod.MINUTE),
      EndpointCategory.SSE: (5, RateLimitPeriod.MINUTE),
      EndpointCategory.BILLING: (60, RateLimitPeriod.MINUTE),  # Never block payments
      # Graph-scoped
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
    # ladybug-large (2 vCPU): dedicated categories 2x Standard.
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
    # ladybug-xlarge (4 vCPU): dedicated categories 4x Standard.
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
    """(limit, window_seconds) for the tier (unknown → base), or None."""
    tier_limits = cls.SUBSCRIPTION_RATE_LIMITS.get(tier)
    if not tier_limits:
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
    """Endpoint category for a path and method, or None if uncategorized."""
    # Extensions (OLTP) have their own buckets; mounted outside /v1/.
    if path.startswith("/extensions/"):
      ext_parts = path[len("/extensions/") :].split("/")
      # /extensions/{graph_id}/graphql → EXTENSIONS_GRAPHQL (typed OLTP reads)
      if len(ext_parts) >= 2 and ext_parts[1] == "graphql":
        return EndpointCategory.EXTENSIONS_GRAPHQL
      # /extensions/{domain}/{graph_id}/operations/{op_name} (views too)
      if len(ext_parts) >= 4 and ext_parts[2] == "operations":
        return EndpointCategory.EXTENSIONS_WRITE
      # Anything else here, rather than the unbounded default.
      return EndpointCategory.EXTENSIONS_WRITE

    if path.startswith("/v1/"):
      path = path[4:]

    # Graph-agnostic MCP transports (the OAuth grant names the graph).
    if path in ("mcp", "mcp/roboledger"):
      return EndpointCategory.GRAPH_MCP

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

    path_parts = path.strip("/").split("/")

    # Pre-creation schema validation carries no graph_id; read-like despite POST.
    if path_parts[:3] == ["graphs", "schema", "validate"]:
      return EndpointCategory.GRAPH_READ

    # Graph-scoped endpoints (format: /graphs/{graph_id}/...)
    if len(path_parts) >= 2 and path_parts[0] == "graphs":
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

      # /query/{cypher,sql}: SQL goes to the DuckDB bucket.
      elif endpoint_type == "query":
        if len(path_parts) >= 4 and path_parts[3] == "sql":
          return EndpointCategory.TABLE_QUERY
        return EndpointCategory.GRAPH_QUERY

      # Aggregation-heavy reads; keep in step with routers/graphs/usage.py.
      elif endpoint_type in ("metrics", "usage"):
        return EndpointCategory.GRAPH_ANALYTICS

      # Lifecycle operations. Match the op-name segment, not the whole path, so
      # a graph_id can't route the bucket.
      elif endpoint_type == "operations":
        op_name = path_parts[3] if len(path_parts) > 3 else ""
        if "backup" in op_name:  # create-backup
          return EndpointCategory.GRAPH_BACKUP
        elif "materialize" in op_name:  # heavy OLAP rebuild
          return EndpointCategory.GRAPH_IMPORT
        else:  # create/delete subgraph, delete-graph, change-tier/style
          return EndpointCategory.GRAPH_MANAGEMENT

      # Only the /sync call gets the tight sync bucket.
      elif endpoint_type == "connections":
        if "sync" in path:
          return EndpointCategory.GRAPH_SYNC
        elif method in ["POST", "PUT", "DELETE", "PATCH"]:
          return EndpointCategory.GRAPH_WRITE
        else:
          return EndpointCategory.GRAPH_READ

      elif "import" in path or "ingest" in path:
        return EndpointCategory.GRAPH_IMPORT

      elif method in ["POST", "PUT", "DELETE", "PATCH"]:
        return EndpointCategory.GRAPH_WRITE

      else:
        return EndpointCategory.GRAPH_READ

    return None


# Per-endpoint burst limits, in requests per window. Constants on purpose: a
# limit raisable at runtime is one an attacker benefits from raising.
BURST_LIMITS: dict[str, int] = {
  # Identity-based limits for the general API bucket, per minute.
  "api_key": 1000,  # 60k/hour possible
  "jwt": 500,  # 30k/hour possible
  "anonymous": 10,  # 600/hour possible
  # Authentication: each attempt count is followed by its window in seconds.
  "auth_attempts": 10,
  "auth_window": 300,
  "login_attempts": 5,
  "login_window": 300,
  "register_attempts": 3,
  "register_window": 3600,
  "jwt_refresh": 20,  # per minute; deliberately stricter than sensitive_auth
  "sensitive_auth": 60,
  "auth_status": 600,  # 10/second — polled by the login screen
  "logout": 300,
  "sso": 100,
  "oidc": 120,  # 2 per flow
  # MCP OAuth 2.1 authorization server. Anonymous callers get limit//10.
  "oauth_authorize": 120,  # browser GET; anonymous → 12/min per IP
  "oauth_consent": 120,  # JWT-authenticated consent read + decision
  "oauth_token": 300,  # code exchange + refresh; anonymous → 30/min per IP
  "oauth_register": 50,  # RFC 7591; anonymous → 5/min per IP, plus a daily cap
  "mfa": 120,  # 2 per handshake
  "passkey_management": 60,
  # SCIM provisioning, which bursts during a directory's initial sync.
  "scim": 120,  # per token
  "scim_ip": 300,  # per IP
  # Ordinary API surfaces, per minute.
  "general_api": 200,
  "public_api": 600,  # divided by 10 for anonymous callers
  "user_management": 600,  # 10/second
  "tasks": 200,
  "analytics": 100,
  "connection_mgmt": 30,
  "sync_ops": 50,
  "backup_ops": 10,  # expensive operations
  "billing": 60,  # checkout polls at ~20/min
  "webhook": 1200,  # anonymous → 120/min per IP
  # SSE fallback, used only when the per-tier lookup returns nothing.
  "sse_connections": 10,
  "sse_connections_window": 60,
}
