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


class AdmissionDefaults:
  """
  Admission control thresholds.

  These thresholds determine when to start rejecting new requests
  to protect system stability.
  """

  MEMORY_THRESHOLD = 85.0  # Start rejecting at 85% memory usage
  CPU_THRESHOLD = 90.0  # Start rejecting at 90% CPU usage
  QUEUE_THRESHOLD = 0.8  # Start rejecting at 80% queue capacity


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
  Load shedding thresholds (decimal values 0.0-1.0).

  Load shedding is a last-resort protection mechanism that randomly
  rejects requests when system pressure is too high.
  """

  START_PRESSURE = 0.8  # Start shedding at 80% pressure
  STOP_PRESSURE = 0.6  # Stop shedding when below 60% pressure


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

  These values control parallel processing capacity.
  """

  MAX_WORKERS = 10  # Default max workers for thread pools
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


# SSM Parameter paths for tunables
# These paths are used by tuning.py to fetch overrides from SSM
SSM_TUNING_PATHS = {
  # Cache TTLs
  "cache/BALANCE_TTL": CacheDefaults.BALANCE_TTL,
  "cache/SUMMARY_TTL": CacheDefaults.SUMMARY_TTL,
  "cache/JWT_TTL": CacheDefaults.JWT_TTL,
  "cache/API_KEY_TTL": CacheDefaults.API_KEY_TTL,
  "cache/SCHEMA_TTL": CacheDefaults.SCHEMA_TTL,
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
  # Workers
  "workers/MAX_WORKERS": WorkerDefaults.MAX_WORKERS,
}
