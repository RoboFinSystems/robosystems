"""
Runtime tuning configuration via SSM Parameter Store.

Provides access to operational parameters that can be adjusted
at runtime without redeployment.

## Three-Tier Model

```
CONSTANTS (constants.py)    | TUNABLES (this module)      | SECRETS (secrets_manager.py)
----------------------------|-----------------------------|--------------------------
Never change                | Runtime adjustable          | Sensitive data
- XBRL URIs                 | - Cache TTLs                | - DATABASE_URL
- SEC_RATE_LIMIT            | - Queue sizes               | - JWT_SECRET_KEY
- Memory limits             | - Thresholds                | - API keys
- Credit day/hour           | - Timeouts                  | - Passwords
```

## Override Priority

Environment Variable > SSM Parameter Store > Default Value

## Usage

```python
from robosystems.config.tuning import TuningConfig

# Get a tuning value (uses SSM override if set, otherwise default)
balance_ttl = TuningConfig.get_cache_balance_ttl()
memory_threshold = TuningConfig.get_admission_memory_threshold()

# Or use the helper for any path
custom_value = TuningConfig.get_int("cache/CUSTOM_TTL", 300)
```

## SSM Parameter Hierarchy

Parameters are stored at: /robosystems/{env}/tuning/{category}/{key}

Categories:
- cache/       - Cache TTL values
- admission/   - Admission control thresholds
- queues/      - Queue configuration
- circuits/    - Circuit breaker settings
- load_shedding/ - Load shedding thresholds
- mcp/         - MCP operation limits
- workers/     - Worker pool settings
"""

import logging
import os
from functools import lru_cache

from .defaults import (
  AdmissionDefaults,
  CacheDefaults,
  CircuitBreakerDefaults,
  LoadSheddingDefaults,
  MCPDefaults,
  QueueDefaults,
  WorkerDefaults,
)

# Use standard logging to avoid circular import with robosystems.logger
logger = logging.getLogger(__name__)


def _get_env_override(env_key: str) -> str | None:
  """Check for environment variable override."""
  return os.getenv(env_key)


def _get_parameter_manager():
  """Get the parameter manager with lazy import to avoid circular dependency."""
  try:
    from .parameter_store import get_parameter_manager

    return get_parameter_manager()
  except ImportError:
    logger.debug("Parameter store not available, using defaults only")
    return None


class TuningConfig:
  """
  Runtime tunable configuration with SSM backend.

  This class provides typed access to tuning parameters with the
  override priority: env var > SSM > default.

  All methods are class methods for easy access without instantiation.
  """

  # =========================================================================
  # GENERIC ACCESSORS
  # =========================================================================

  @classmethod
  def get(cls, path: str, default: str) -> str:
    """
    Get a tuning parameter as string.

    Args:
        path: SSM path under /tuning/ (e.g., "cache/BALANCE_TTL")
        default: Default value if not found

    Returns:
        Parameter value or default
    """
    # Convert path to env var name (e.g., "cache/BALANCE_TTL" -> "TUNING_CACHE_BALANCE_TTL")
    env_key = "TUNING_" + path.upper().replace("/", "_")
    env_value = _get_env_override(env_key)
    if env_value is not None:
      return env_value

    # Try SSM
    manager = _get_parameter_manager()
    if manager:
      return manager.get_tuning_parameter(path, default)

    return default

  @classmethod
  def get_int(cls, path: str, default: int) -> int:
    """
    Get a tuning parameter as integer.

    Args:
        path: SSM path under /tuning/ (e.g., "cache/BALANCE_TTL")
        default: Default value if not found or invalid

    Returns:
        Integer parameter value or default
    """
    # Convert path to env var name
    env_key = "TUNING_" + path.upper().replace("/", "_")
    env_value = _get_env_override(env_key)
    if env_value is not None:
      try:
        return int(env_value)
      except (ValueError, TypeError):
        logger.warning(f"Invalid int env var {env_key}: {env_value}")

    # Try SSM
    manager = _get_parameter_manager()
    if manager:
      return manager.get_tuning_int(path, default)

    return default

  @classmethod
  def get_float(cls, path: str, default: float) -> float:
    """
    Get a tuning parameter as float.

    Args:
        path: SSM path under /tuning/ (e.g., "admission/MEMORY_THRESHOLD")
        default: Default value if not found or invalid

    Returns:
        Float parameter value or default
    """
    # Convert path to env var name
    env_key = "TUNING_" + path.upper().replace("/", "_")
    env_value = _get_env_override(env_key)
    if env_value is not None:
      try:
        return float(env_value)
      except (ValueError, TypeError):
        logger.warning(f"Invalid float env var {env_key}: {env_value}")

    # Try SSM
    manager = _get_parameter_manager()
    if manager:
      return manager.get_tuning_float(path, default)

    return default

  # =========================================================================
  # CACHE TTL ACCESSORS
  # =========================================================================

  @classmethod
  def get_cache_balance_ttl(cls) -> int:
    """Get credit balance cache TTL in seconds."""
    return cls.get_int("cache/BALANCE_TTL", CacheDefaults.BALANCE_TTL)

  @classmethod
  def get_cache_summary_ttl(cls) -> int:
    """Get credit summary cache TTL in seconds."""
    return cls.get_int("cache/SUMMARY_TTL", CacheDefaults.SUMMARY_TTL)

  @classmethod
  def get_cache_jwt_ttl(cls) -> int:
    """Get JWT validation cache TTL in seconds."""
    return cls.get_int("cache/JWT_TTL", CacheDefaults.JWT_TTL)

  @classmethod
  def get_cache_api_key_ttl(cls) -> int:
    """Get API key validation cache TTL in seconds."""
    return cls.get_int("cache/API_KEY_TTL", CacheDefaults.API_KEY_TTL)

  @classmethod
  def get_cache_schema_ttl(cls) -> int:
    """Get schema/config cache TTL in seconds."""
    return cls.get_int("cache/SCHEMA_TTL", CacheDefaults.SCHEMA_TTL)

  # =========================================================================
  # ADMISSION CONTROL ACCESSORS
  # =========================================================================

  @classmethod
  def get_admission_memory_threshold(cls) -> float:
    """Get memory usage threshold for admission control (percent)."""
    return cls.get_float(
      "admission/MEMORY_THRESHOLD", AdmissionDefaults.MEMORY_THRESHOLD
    )

  @classmethod
  def get_admission_cpu_threshold(cls) -> float:
    """Get CPU usage threshold for admission control (percent)."""
    return cls.get_float("admission/CPU_THRESHOLD", AdmissionDefaults.CPU_THRESHOLD)

  @classmethod
  def get_admission_queue_threshold(cls) -> float:
    """Get queue capacity threshold for admission control (decimal 0-1)."""
    return cls.get_float("admission/QUEUE_THRESHOLD", AdmissionDefaults.QUEUE_THRESHOLD)

  # =========================================================================
  # QUEUE CONFIGURATION ACCESSORS
  # =========================================================================

  @classmethod
  def get_queue_max_size(cls) -> int:
    """Get maximum query queue size."""
    return cls.get_int("queues/MAX_SIZE", QueueDefaults.MAX_SIZE)

  @classmethod
  def get_queue_max_concurrent(cls) -> int:
    """Get maximum concurrent queries."""
    return cls.get_int("queues/MAX_CONCURRENT", QueueDefaults.MAX_CONCURRENT)

  @classmethod
  def get_queue_max_per_user(cls) -> int:
    """Get maximum pending queries per user."""
    return cls.get_int("queues/MAX_PER_USER", QueueDefaults.MAX_PER_USER)

  @classmethod
  def get_queue_timeout(cls) -> int:
    """Get query timeout in queue (seconds)."""
    return cls.get_int("queues/TIMEOUT", QueueDefaults.TIMEOUT)

  # =========================================================================
  # CIRCUIT BREAKER ACCESSORS
  # =========================================================================

  @classmethod
  def get_circuit_breaker_threshold(cls) -> int:
    """Get failures before circuit breaker opens."""
    return cls.get_int("circuits/THRESHOLD", CircuitBreakerDefaults.FAILURE_THRESHOLD)

  @classmethod
  def get_circuit_breaker_timeout(cls) -> int:
    """Get seconds before retrying after circuit opens."""
    return cls.get_int("circuits/TIMEOUT", CircuitBreakerDefaults.TIMEOUT)

  # =========================================================================
  # LOAD SHEDDING ACCESSORS
  # =========================================================================

  @classmethod
  def get_load_shedding_start_pressure(cls) -> float:
    """Get pressure threshold to start load shedding (decimal 0-1)."""
    return cls.get_float(
      "load_shedding/START_PRESSURE", LoadSheddingDefaults.START_PRESSURE
    )

  @classmethod
  def get_load_shedding_stop_pressure(cls) -> float:
    """Get pressure threshold to stop load shedding (decimal 0-1)."""
    return cls.get_float(
      "load_shedding/STOP_PRESSURE", LoadSheddingDefaults.STOP_PRESSURE
    )

  # =========================================================================
  # MCP ACCESSORS
  # =========================================================================

  @classmethod
  def get_mcp_max_result_rows(cls) -> int:
    """Get maximum rows in MCP query results."""
    return cls.get_int("mcp/MAX_RESULT_ROWS", MCPDefaults.MAX_RESULT_ROWS)

  @classmethod
  def get_mcp_max_result_size_mb(cls) -> float:
    """Get maximum MCP result size in MB."""
    return cls.get_float("mcp/MAX_RESULT_SIZE_MB", MCPDefaults.MAX_RESULT_SIZE_MB)

  @classmethod
  def get_mcp_pool_idle_timeout(cls) -> int:
    """Get MCP connection pool idle timeout (seconds)."""
    return cls.get_int("mcp/POOL_IDLE_TIMEOUT", MCPDefaults.POOL_IDLE_TIMEOUT)

  @classmethod
  def get_mcp_pool_max_lifetime(cls) -> int:
    """Get MCP connection pool max lifetime (seconds)."""
    return cls.get_int("mcp/POOL_MAX_LIFETIME", MCPDefaults.POOL_MAX_LIFETIME)

  # =========================================================================
  # WORKER ACCESSORS
  # =========================================================================

  @classmethod
  def get_max_workers(cls) -> int:
    """Get maximum workers for thread pools."""
    return cls.get_int("workers/MAX_WORKERS", WorkerDefaults.MAX_WORKERS)

  # =========================================================================
  # UTILITY METHODS
  # =========================================================================

  @classmethod
  @lru_cache(maxsize=1)
  def preload_all(cls) -> dict[str, str]:
    """
    Preload all tuning parameters from SSM into cache.

    Call this at application startup to batch-load all parameters
    in a single API call, rather than fetching them individually.

    Returns:
        Dictionary of loaded tuning parameters
    """
    manager = _get_parameter_manager()
    if manager:
      return manager.get_all_tuning_parameters()
    return {}

  @classmethod
  def refresh(cls):
    """
    Refresh cached tuning parameters.

    Call this to force re-fetching from SSM on next access.
    """
    # Clear the preload cache
    cls.preload_all.cache_clear()

    # Refresh the parameter manager cache
    manager = _get_parameter_manager()
    if manager:
      manager.refresh()
