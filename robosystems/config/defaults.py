"""Defaults for runtime-tunable parameters.

Override priority: env var > SSM Parameter Store > these defaults. Fixed
values live in constants.py, credentials in secrets_manager.py.
"""


class DatabaseDefaults:
  """SQLAlchemy pool defaults.

  (pool_size + max_overflow) x ECS tasks must stay under the RDS instance's
  max_connections (~112 t4g.micro, ~225 small, ~450 medium, ~900 large).
  """

  POOL_SIZE = 5  # Baseline connections held open per task
  MAX_OVERFLOW = 10  # Additional connections above pool_size (burst)
  POOL_TIMEOUT = 30  # Seconds to wait for a connection from the pool
  POOL_RECYCLE = 3600  # Seconds; handles RDS-side drops
  # The platform session is synchronous and called from async handlers, so an
  # unbounded statement holds the event loop for every tenant. 0 disables.
  STATEMENT_TIMEOUT_MS = 30_000

  # Extensions OLTP engine, tuned independently (it carries the hot write path).
  EXTENSIONS_POOL_SIZE = 5
  EXTENSIONS_MAX_OVERFLOW = 10
  # Interactive extensions sessions; bulk paths opt out explicitly.
  EXTENSIONS_STATEMENT_TIMEOUT_MS = 30_000


class CacheDefaults:
  """Cache TTL defaults (seconds)."""

  # General TTL categories
  SHORT = 300  # 5 minutes - frequently changing data
  MEDIUM = 600  # 10 minutes - moderately stable data
  LONG = 3600  # 1 hour - stable configuration data

  # Specific cache TTLs
  BALANCE_TTL = SHORT  # Credit balance freshness
  SUMMARY_TTL = MEDIUM  # Credit summary cache
  OPERATION_COST_TTL = LONG  # Operation costs rarely change

  # Authentication cache TTLs
  JWT_TTL = 1800  # 30 minutes for JWT validation cache
  API_KEY_TTL = SHORT  # 5 minutes for API key validation
  EMAIL_TOKEN_REF_TTL = 900  # 15 minutes for a queued email's parked token

  # Graph/Schema cache TTLs
  SCHEMA_TTL = SHORT  # 5 minutes for schema/config cache


class TimeoutDefaults:
  """Operation timeout defaults (seconds)."""

  HTTP = 30  # Standard HTTP request timeout
  QUERY = 30  # Database query timeout
  CONNECTION = 10  # Connection establishment timeout
  STREAM = 300  # Streaming operations (5 minutes)

  # Graph API timeouts
  GRAPH_HTTP = 30  # Graph API HTTP request timeout
  GRAPH_QUERY = 30  # Graph query execution timeout


class AdmissionDefaults:
  """Admission control thresholds, read differently by two controllers.

  graph_api/core/admission_control.py only reports MEMORY_THRESHOLD and
  rejects on MIN_AVAILABLE_MB: the LadybugDB buffer pool is meant to fill, so
  percent-of-memory would conflate it with the working set.
  middleware/graph/admission_control.py rejects on MEMORY_THRESHOLD and never
  reads MIN_AVAILABLE_MB.
  """

  MEMORY_THRESHOLD = 85.0  # Report memory pressure at 85% usage
  MIN_AVAILABLE_MB = 1024.0  # Reject when free memory falls below 1GB
  CPU_THRESHOLD = 90.0  # Start rejecting at 90% CPU usage
  QUEUE_THRESHOLD = 80.0  # Start rejecting at 80% queue capacity


class QueueDefaults:
  """Query queue defaults."""

  MAX_SIZE = 1000  # Maximum pending queries in queue
  MAX_CONCURRENT = 50  # Maximum concurrent query execution
  MAX_PER_USER = 10  # Maximum pending queries per user
  TIMEOUT = 300  # Query timeout in queue (5 minutes)


class CircuitBreakerDefaults:
  """Circuit breaker defaults."""

  FAILURE_THRESHOLD = 5  # Failures before opening circuit
  TIMEOUT = 60  # Seconds before retry after circuit opens


class LoadSheddingDefaults:
  """Load shedding thresholds (percent pressure)."""

  START_PRESSURE = 80.0  # Start shedding at 80% pressure
  STOP_PRESSURE = 60.0  # Stop shedding when below 60% pressure


class MCPDefaults:
  """MCP result-size defaults, protecting LLM context windows."""

  MAX_RESULT_ROWS = 1000  # Default row limit for queries
  MAX_RESULT_SIZE_MB = 5.0  # Maximum result size in MB
  POOL_IDLE_TIMEOUT = 300  # Connection pool idle timeout (5 minutes)
  POOL_MAX_LIFETIME = 3600  # Connection pool max lifetime (1 hour)


class WorkerDefaults:
  """Worker pool defaults for batch operations."""

  MAX_WORKERS = 10  # Parallel workers for batch operations (e.g., S3 uploads)
  MIN_WORKERS = 1  # Minimum workers
  POOL_TIMEOUT = 30  # Worker pool timeout


class RetryDefaults:
  """Retry defaults for transient failures."""

  MAX_RETRIES = 3  # Maximum retry attempts
  MIN_DELAY = 1  # Minimum delay between retries (seconds)
  MAX_DELAY = 60  # Maximum delay between retries (seconds)
  BACKOFF_FACTOR = 2  # Exponential backoff multiplier


class RateLimitDefaults:
  """Rate limit window defaults (seconds)."""

  WINDOW_SHORT = 60  # 1 minute window for burst limits
  WINDOW_LONG = 300  # 5 minute window for sustained limits


class SSEDefaults:
  """Server-Sent Events defaults."""

  MAX_CONNECTIONS_PER_USER = 5  # Max concurrent SSE connections per user
  QUEUE_SIZE = 100  # Event queue size per connection
  # Seconds of silence before a keepalive. Must stay well below the client read
  # timeout (30s in the Python SDK) or clients drop jobs still running.
  KEEPALIVE_INTERVAL = 10


class LimitsDefaults:
  """Runtime-adjustable resource quotas."""

  # Must stay the conservative floor: an org bootstrapped during an SSM blip
  # snapshots this value into org_limits.max_graphs durably.
  ORG_GRAPHS_DEFAULT = 1  # Default max graphs per organization


# SSM paths (under tuning/) that tuning.py reads overrides from.
SSM_TUNING_PATHS = {
  # Cache TTLs
  "cache/BALANCE_TTL": CacheDefaults.BALANCE_TTL,
  "cache/SUMMARY_TTL": CacheDefaults.SUMMARY_TTL,
  "cache/JWT_TTL": CacheDefaults.JWT_TTL,
  "cache/API_KEY_TTL": CacheDefaults.API_KEY_TTL,
  "cache/SCHEMA_TTL": CacheDefaults.SCHEMA_TTL,
  "cache/OPERATION_COST_TTL": CacheDefaults.OPERATION_COST_TTL,
  # Admission Control
  "admission/MEMORY_THRESHOLD": AdmissionDefaults.MEMORY_THRESHOLD,
  "admission/CPU_THRESHOLD": AdmissionDefaults.CPU_THRESHOLD,
  "admission/QUEUE_THRESHOLD": AdmissionDefaults.QUEUE_THRESHOLD,
  # Queues
  "queues/MAX_SIZE": QueueDefaults.MAX_SIZE,
  "queues/MAX_CONCURRENT": QueueDefaults.MAX_CONCURRENT,
  "queues/MAX_PER_USER": QueueDefaults.MAX_PER_USER,
  "queues/TIMEOUT": QueueDefaults.TIMEOUT,
  # Circuit Breakers
  "circuits/THRESHOLD": CircuitBreakerDefaults.FAILURE_THRESHOLD,
  "circuits/TIMEOUT": CircuitBreakerDefaults.TIMEOUT,
  # Load Shedding
  "load_shedding/START_PRESSURE": LoadSheddingDefaults.START_PRESSURE,
  "load_shedding/STOP_PRESSURE": LoadSheddingDefaults.STOP_PRESSURE,
  # MCP
  "mcp/MAX_RESULT_ROWS": MCPDefaults.MAX_RESULT_ROWS,
  "mcp/MAX_RESULT_SIZE_MB": MCPDefaults.MAX_RESULT_SIZE_MB,
  "mcp/POOL_IDLE_TIMEOUT": MCPDefaults.POOL_IDLE_TIMEOUT,
  "mcp/POOL_MAX_LIFETIME": MCPDefaults.POOL_MAX_LIFETIME,
  # Workers
  "workers/MAX_WORKERS": WorkerDefaults.MAX_WORKERS,
  # Timeouts
  "timeouts/GRAPH_HTTP": TimeoutDefaults.GRAPH_HTTP,
  "timeouts/GRAPH_QUERY": TimeoutDefaults.GRAPH_QUERY,
  # SSE
  "sse/MAX_CONNECTIONS_PER_USER": SSEDefaults.MAX_CONNECTIONS_PER_USER,
  "sse/QUEUE_SIZE": SSEDefaults.QUEUE_SIZE,
  # Limits
  "limits/ORG_GRAPHS_DEFAULT": LimitsDefaults.ORG_GRAPHS_DEFAULT,
  # Database Pool
  "database/POOL_SIZE": DatabaseDefaults.POOL_SIZE,
  "database/MAX_OVERFLOW": DatabaseDefaults.MAX_OVERFLOW,
  "database/POOL_TIMEOUT": DatabaseDefaults.POOL_TIMEOUT,
  "database/POOL_RECYCLE": DatabaseDefaults.POOL_RECYCLE,
  "database/STATEMENT_TIMEOUT_MS": DatabaseDefaults.STATEMENT_TIMEOUT_MS,
  "database/EXTENSIONS_POOL_SIZE": DatabaseDefaults.EXTENSIONS_POOL_SIZE,
  "database/EXTENSIONS_MAX_OVERFLOW": DatabaseDefaults.EXTENSIONS_MAX_OVERFLOW,
  "database/EXTENSIONS_STATEMENT_TIMEOUT_MS": (
    DatabaseDefaults.EXTENSIONS_STATEMENT_TIMEOUT_MS
  ),
}
