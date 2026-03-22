"""
Centralized default values for tunable configuration.

These values serve as defaults when SSM parameters are not set.
Override priority: Environment Variable > SSM Parameter Store > Default

This module provides the "source of truth" for all tunable configuration values,
eliminating scattered magic numbers throughout the codebase.

Categories:
- CONSTANTS (in constants.py): Values that never change (protocol limits, business rules)
- TUNABLES (here + SSM): Operational parameters adjustable at runtime
- SECRETS (in secrets_manager.py): Sensitive credentials and API keys
"""


class DatabaseDefaults:
  """
  Database connection pool defaults.

  These values control SQLAlchemy connection pooling to PostgreSQL (RDS).
  Tune based on instance size and number of ECS tasks:
  - pool_size + max_overflow = max connections per task
  - Total connections = (pool_size + max_overflow) x number_of_tasks
  - Must stay under RDS max_connections for the instance type

  RDS max_connections by instance (PostgreSQL):
  - db.t4g.micro:  ~112
  - db.t4g.small:  ~225
  - db.t4g.medium: ~450
  - db.t4g.large:  ~900
  """

  POOL_SIZE = 5  # Baseline connections held open per task
  MAX_OVERFLOW = 10  # Additional connections above pool_size (burst)
  POOL_TIMEOUT = 30  # Seconds to wait for a connection from the pool
  POOL_RECYCLE = 3600  # Recycle connections after 1 hour (handles RDS drops)


class CacheDefaults:
  """
  Cache TTL defaults (seconds).

  These values balance freshness with performance. Shorter TTLs mean
  more frequent cache misses but fresher data.
  """

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

  # Graph/Schema cache TTLs
  SCHEMA_TTL = SHORT  # 5 minutes for schema/config cache


class TimeoutDefaults:
  """
  Operation timeout defaults (seconds).

  These values balance responsiveness with allowing operations to complete.
  """

  HTTP = 30  # Standard HTTP request timeout
  QUERY = 30  # Database query timeout
  CONNECTION = 10  # Connection establishment timeout
  STREAM = 300  # Streaming operations (5 minutes)

  # Graph API timeouts
  GRAPH_HTTP = 30  # Graph API HTTP request timeout
  GRAPH_QUERY = 30  # Graph query execution timeout


class AdmissionDefaults:
  """
  Admission control thresholds (all values are percentages 0-100).

  These thresholds determine when to start rejecting new requests
  to protect system stability.
  """

  MEMORY_THRESHOLD = 85.0  # Start rejecting at 85% memory usage
  CPU_THRESHOLD = 90.0  # Start rejecting at 90% CPU usage
  QUEUE_THRESHOLD = 80.0  # Start rejecting at 80% queue capacity


class QueueDefaults:
  """
  Queue configuration defaults.

  These values control query queue behavior and capacity limits.
  """

  MAX_SIZE = 1000  # Maximum pending queries in queue
  MAX_CONCURRENT = 50  # Maximum concurrent query execution
  MAX_PER_USER = 10  # Maximum pending queries per user
  TIMEOUT = 300  # Query timeout in queue (5 minutes)


class CircuitBreakerDefaults:
  """
  Circuit breaker defaults.

  Circuit breakers protect downstream services from cascading failures.
  """

  FAILURE_THRESHOLD = 5  # Failures before opening circuit
  TIMEOUT = 60  # Seconds before retry after circuit opens


class LoadSheddingDefaults:
  """
  Load shedding thresholds (all values are percentages 0-100).

  Load shedding is a last-resort protection mechanism that randomly
  rejects requests when system pressure is too high.
  """

  START_PRESSURE = 80.0  # Start shedding at 80% pressure
  STOP_PRESSURE = 60.0  # Stop shedding when below 60% pressure


class MCPDefaults:
  """
  MCP (Model Context Protocol) operation defaults.

  These values protect LLM context windows from being overwhelmed
  by large result sets.
  """

  MAX_RESULT_ROWS = 1000  # Default row limit for queries
  MAX_RESULT_SIZE_MB = 5.0  # Maximum result size in MB
  POOL_IDLE_TIMEOUT = 300  # Connection pool idle timeout (5 minutes)
  POOL_MAX_LIFETIME = 3600  # Connection pool max lifetime (1 hour)


class WorkerDefaults:
  """
  Worker/thread pool defaults.

  These values control parallel processing for batch operations.
  """

  MAX_WORKERS = 10  # Parallel workers for batch operations (e.g., S3 uploads)
  MIN_WORKERS = 1  # Minimum workers
  POOL_TIMEOUT = 30  # Worker pool timeout


class RetryDefaults:
  """
  Retry configuration defaults.

  These values control retry behavior for transient failures.
  """

  MAX_RETRIES = 3  # Maximum retry attempts
  MIN_DELAY = 1  # Minimum delay between retries (seconds)
  MAX_DELAY = 60  # Maximum delay between retries (seconds)
  BACKOFF_FACTOR = 2  # Exponential backoff multiplier


class RateLimitDefaults:
  """
  Rate limiting defaults.

  These values control burst protection windows.
  """

  WINDOW_SHORT = 60  # 1 minute window for burst limits
  WINDOW_LONG = 300  # 5 minute window for sustained limits


class SSEDefaults:
  """
  Server-Sent Events (SSE) defaults.

  These values control SSE connection limits.
  """

  MAX_CONNECTIONS_PER_USER = 5  # Max concurrent SSE connections per user
  QUEUE_SIZE = 100  # Event queue size per connection


class IndexingDefaults:
  """
  Indexing pipeline defaults.

  These values control OpenSearch text indexing behavior for SEC filings.
  """

  ENABLE_EMBEDDINGS = True  # Generate vector embeddings for semantic search


class LimitsDefaults:
  """
  Default limits for various resources.

  These values control quotas and resource limits that can be adjusted at runtime.
  """

  ORG_GRAPHS_DEFAULT = 10  # Default max graphs per organization


# SSM Parameter paths for tunables
# These paths are used by tuning.py to fetch overrides from SSM
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
  # Indexing
  "indexing/ENABLE_EMBEDDINGS": IndexingDefaults.ENABLE_EMBEDDINGS,
  # Limits
  "limits/ORG_GRAPHS_DEFAULT": LimitsDefaults.ORG_GRAPHS_DEFAULT,
  # Database Pool
  "database/POOL_SIZE": DatabaseDefaults.POOL_SIZE,
  "database/MAX_OVERFLOW": DatabaseDefaults.MAX_OVERFLOW,
  "database/POOL_TIMEOUT": DatabaseDefaults.POOL_TIMEOUT,
  "database/POOL_RECYCLE": DatabaseDefaults.POOL_RECYCLE,
}
