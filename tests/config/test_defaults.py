"""
Tests for the centralized defaults module.

These are pure unit tests that don't require any external services.
"""

import pytest

from robosystems.config.defaults import (
  SSM_TUNING_PATHS,
  AdmissionDefaults,
  CacheDefaults,
  CircuitBreakerDefaults,
  LoadSheddingDefaults,
  MCPDefaults,
  QueueDefaults,
  RateLimitDefaults,
  RetryDefaults,
  TimeoutDefaults,
  WorkerDefaults,
)

# Mark all tests in this module as unit tests (no database required)
pytestmark = pytest.mark.unit


class TestCacheDefaults:
  """Test CacheDefaults values."""

  def test_short_ttl_is_five_minutes(self):
    """Verify SHORT TTL is 300 seconds (5 minutes)."""
    assert CacheDefaults.SHORT == 300

  def test_medium_ttl_is_ten_minutes(self):
    """Verify MEDIUM TTL is 600 seconds (10 minutes)."""
    assert CacheDefaults.MEDIUM == 600

  def test_long_ttl_is_one_hour(self):
    """Verify LONG TTL is 3600 seconds (1 hour)."""
    assert CacheDefaults.LONG == 3600

  def test_specific_ttls_reference_categories(self):
    """Verify specific TTLs use the right category values."""
    assert CacheDefaults.BALANCE_TTL == CacheDefaults.SHORT
    assert CacheDefaults.SUMMARY_TTL == CacheDefaults.MEDIUM
    assert CacheDefaults.API_KEY_TTL == CacheDefaults.SHORT
    assert CacheDefaults.SCHEMA_TTL == CacheDefaults.SHORT

  def test_jwt_ttl_is_thirty_minutes(self):
    """Verify JWT TTL is 1800 seconds (30 minutes)."""
    assert CacheDefaults.JWT_TTL == 1800

  def test_operation_cost_ttl_is_one_hour(self):
    """Verify operation cost TTL is 1 hour."""
    assert CacheDefaults.OPERATION_COST_TTL == CacheDefaults.LONG


class TestTimeoutDefaults:
  """Test TimeoutDefaults values."""

  def test_http_timeout_is_30_seconds(self):
    """Verify HTTP timeout is 30 seconds."""
    assert TimeoutDefaults.HTTP == 30

  def test_query_timeout_is_30_seconds(self):
    """Verify query timeout is 30 seconds."""
    assert TimeoutDefaults.QUERY == 30

  def test_connection_timeout_is_10_seconds(self):
    """Verify connection timeout is 10 seconds."""
    assert TimeoutDefaults.CONNECTION == 10

  def test_stream_timeout_is_5_minutes(self):
    """Verify stream timeout is 300 seconds (5 minutes)."""
    assert TimeoutDefaults.STREAM == 300


class TestAdmissionDefaults:
  """Test AdmissionDefaults values."""

  def test_memory_threshold_is_reasonable(self):
    """Verify memory threshold is between 50% and 100%."""
    assert 50.0 <= AdmissionDefaults.MEMORY_THRESHOLD <= 100.0

  def test_cpu_threshold_is_reasonable(self):
    """Verify CPU threshold is between 50% and 100%."""
    assert 50.0 <= AdmissionDefaults.CPU_THRESHOLD <= 100.0

  def test_queue_threshold_is_decimal(self):
    """Verify queue threshold is a decimal between 0 and 1."""
    assert 0.0 < AdmissionDefaults.QUEUE_THRESHOLD < 1.0

  def test_default_values(self):
    """Verify specific default values."""
    assert AdmissionDefaults.MEMORY_THRESHOLD == 85.0
    assert AdmissionDefaults.CPU_THRESHOLD == 90.0
    assert AdmissionDefaults.QUEUE_THRESHOLD == 0.8


class TestQueueDefaults:
  """Test QueueDefaults values."""

  def test_max_size_is_positive(self):
    """Verify max size is a positive integer."""
    assert QueueDefaults.MAX_SIZE > 0

  def test_max_concurrent_is_positive(self):
    """Verify max concurrent is a positive integer."""
    assert QueueDefaults.MAX_CONCURRENT > 0

  def test_max_per_user_is_reasonable(self):
    """Verify max per user is less than total concurrent."""
    assert QueueDefaults.MAX_PER_USER < QueueDefaults.MAX_CONCURRENT

  def test_timeout_is_positive(self):
    """Verify timeout is a positive integer."""
    assert QueueDefaults.TIMEOUT > 0

  def test_default_values(self):
    """Verify specific default values."""
    assert QueueDefaults.MAX_SIZE == 1000
    assert QueueDefaults.MAX_CONCURRENT == 50
    assert QueueDefaults.MAX_PER_USER == 10
    assert QueueDefaults.TIMEOUT == 300


class TestCircuitBreakerDefaults:
  """Test CircuitBreakerDefaults values."""

  def test_failure_threshold_is_reasonable(self):
    """Verify failure threshold is a small positive integer."""
    assert 1 <= CircuitBreakerDefaults.FAILURE_THRESHOLD <= 20

  def test_timeout_is_reasonable(self):
    """Verify timeout is between 10 seconds and 5 minutes."""
    assert 10 <= CircuitBreakerDefaults.TIMEOUT <= 300

  def test_default_values(self):
    """Verify specific default values."""
    assert CircuitBreakerDefaults.FAILURE_THRESHOLD == 5
    assert CircuitBreakerDefaults.TIMEOUT == 60


class TestLoadSheddingDefaults:
  """Test LoadSheddingDefaults values."""

  def test_start_pressure_is_decimal(self):
    """Verify start pressure is a decimal between 0 and 1."""
    assert 0.0 < LoadSheddingDefaults.START_PRESSURE < 1.0

  def test_stop_pressure_is_decimal(self):
    """Verify stop pressure is a decimal between 0 and 1."""
    assert 0.0 < LoadSheddingDefaults.STOP_PRESSURE < 1.0

  def test_start_pressure_greater_than_stop(self):
    """Verify start pressure is greater than stop pressure (hysteresis)."""
    assert LoadSheddingDefaults.START_PRESSURE > LoadSheddingDefaults.STOP_PRESSURE

  def test_default_values(self):
    """Verify specific default values."""
    assert LoadSheddingDefaults.START_PRESSURE == 0.8
    assert LoadSheddingDefaults.STOP_PRESSURE == 0.6


class TestMCPDefaults:
  """Test MCPDefaults values."""

  def test_max_result_rows_is_reasonable(self):
    """Verify max result rows is a reasonable limit."""
    assert 100 <= MCPDefaults.MAX_RESULT_ROWS <= 10000

  def test_max_result_size_is_reasonable(self):
    """Verify max result size is between 1MB and 100MB."""
    assert 1.0 <= MCPDefaults.MAX_RESULT_SIZE_MB <= 100.0

  def test_pool_idle_timeout_is_positive(self):
    """Verify pool idle timeout is positive."""
    assert MCPDefaults.POOL_IDLE_TIMEOUT > 0

  def test_pool_max_lifetime_greater_than_idle(self):
    """Verify pool max lifetime is greater than idle timeout."""
    assert MCPDefaults.POOL_MAX_LIFETIME > MCPDefaults.POOL_IDLE_TIMEOUT

  def test_default_values(self):
    """Verify specific default values."""
    assert MCPDefaults.MAX_RESULT_ROWS == 1000
    assert MCPDefaults.MAX_RESULT_SIZE_MB == 5.0
    assert MCPDefaults.POOL_IDLE_TIMEOUT == 300
    assert MCPDefaults.POOL_MAX_LIFETIME == 3600


class TestWorkerDefaults:
  """Test WorkerDefaults values."""

  def test_max_workers_is_reasonable(self):
    """Verify max workers is a reasonable limit."""
    assert 1 <= WorkerDefaults.MAX_WORKERS <= 100

  def test_min_workers_is_positive(self):
    """Verify min workers is at least 1."""
    assert WorkerDefaults.MIN_WORKERS >= 1

  def test_max_greater_than_min(self):
    """Verify max workers is greater than or equal to min workers."""
    assert WorkerDefaults.MAX_WORKERS >= WorkerDefaults.MIN_WORKERS


class TestRetryDefaults:
  """Test RetryDefaults values."""

  def test_max_retries_is_reasonable(self):
    """Verify max retries is a small positive integer."""
    assert 1 <= RetryDefaults.MAX_RETRIES <= 10

  def test_delays_are_positive(self):
    """Verify delays are positive."""
    assert RetryDefaults.MIN_DELAY > 0
    assert RetryDefaults.MAX_DELAY > 0

  def test_max_delay_greater_than_min(self):
    """Verify max delay is greater than min delay."""
    assert RetryDefaults.MAX_DELAY > RetryDefaults.MIN_DELAY

  def test_backoff_factor_is_reasonable(self):
    """Verify backoff factor is a reasonable multiplier."""
    assert 1 < RetryDefaults.BACKOFF_FACTOR <= 5


class TestRateLimitDefaults:
  """Test RateLimitDefaults values."""

  def test_windows_are_positive(self):
    """Verify rate limit windows are positive."""
    assert RateLimitDefaults.WINDOW_SHORT > 0
    assert RateLimitDefaults.WINDOW_LONG > 0

  def test_long_window_greater_than_short(self):
    """Verify long window is greater than short window."""
    assert RateLimitDefaults.WINDOW_LONG > RateLimitDefaults.WINDOW_SHORT


class TestSSMTuningPaths:
  """Test SSM_TUNING_PATHS dictionary."""

  def test_cache_paths_exist(self):
    """Verify cache tuning paths are defined."""
    assert "cache/BALANCE_TTL" in SSM_TUNING_PATHS
    assert "cache/JWT_TTL" in SSM_TUNING_PATHS
    assert "cache/API_KEY_TTL" in SSM_TUNING_PATHS

  def test_admission_paths_exist(self):
    """Verify admission tuning paths are defined."""
    assert "admission/MEMORY_THRESHOLD" in SSM_TUNING_PATHS
    assert "admission/CPU_THRESHOLD" in SSM_TUNING_PATHS

  def test_queue_paths_exist(self):
    """Verify queue tuning paths are defined."""
    assert "queues/MAX_SIZE" in SSM_TUNING_PATHS
    assert "queues/MAX_CONCURRENT" in SSM_TUNING_PATHS

  def test_circuit_paths_exist(self):
    """Verify circuit breaker tuning paths are defined."""
    assert "circuits/THRESHOLD" in SSM_TUNING_PATHS
    assert "circuits/TIMEOUT" in SSM_TUNING_PATHS

  def test_load_shedding_paths_exist(self):
    """Verify load shedding tuning paths are defined."""
    assert "load_shedding/START_PRESSURE" in SSM_TUNING_PATHS
    assert "load_shedding/STOP_PRESSURE" in SSM_TUNING_PATHS

  def test_mcp_paths_exist(self):
    """Verify MCP tuning paths are defined."""
    assert "mcp/MAX_RESULT_ROWS" in SSM_TUNING_PATHS
    assert "mcp/MAX_RESULT_SIZE_MB" in SSM_TUNING_PATHS

  def test_paths_match_defaults(self):
    """Verify SSM paths have matching default values."""
    assert SSM_TUNING_PATHS["cache/BALANCE_TTL"] == CacheDefaults.BALANCE_TTL
    assert (
      SSM_TUNING_PATHS["admission/MEMORY_THRESHOLD"]
      == AdmissionDefaults.MEMORY_THRESHOLD
    )
    assert SSM_TUNING_PATHS["queues/MAX_SIZE"] == QueueDefaults.MAX_SIZE
    assert (
      SSM_TUNING_PATHS["circuits/THRESHOLD"] == CircuitBreakerDefaults.FAILURE_THRESHOLD
    )
    assert (
      SSM_TUNING_PATHS["load_shedding/START_PRESSURE"]
      == LoadSheddingDefaults.START_PRESSURE
    )
    assert SSM_TUNING_PATHS["mcp/MAX_RESULT_ROWS"] == MCPDefaults.MAX_RESULT_ROWS
