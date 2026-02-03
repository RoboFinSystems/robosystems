"""
Tests for the tuning configuration module.

These are pure unit tests that mock AWS SSM and don't require external services.
"""

from unittest.mock import MagicMock, patch

import pytest

from robosystems.config.defaults import (
  AdmissionDefaults,
  CacheDefaults,
  CircuitBreakerDefaults,
  LoadSheddingDefaults,
  MCPDefaults,
  QueueDefaults,
)
from robosystems.config.tuning import TuningConfig

# Mark all tests in this module as unit tests (no database required)
pytestmark = pytest.mark.unit


class TestTuningConfigGenericAccessors:
  """Test generic accessor methods."""

  def test_get_returns_default_when_no_ssm(self):
    """Test that get() returns default when SSM is not available."""
    with patch("robosystems.config.tuning._get_parameter_manager") as mock_pm:
      mock_pm.return_value = None

      result = TuningConfig.get("cache/TEST_KEY", "default_value")
      assert result == "default_value"

  def test_get_int_returns_default_when_no_ssm(self):
    """Test that get_int() returns default when SSM is not available."""
    with patch("robosystems.config.tuning._get_parameter_manager") as mock_pm:
      mock_pm.return_value = None

      result = TuningConfig.get_int("cache/TEST_KEY", 123)
      assert result == 123

  def test_get_float_returns_default_when_no_ssm(self):
    """Test that get_float() returns default when SSM is not available."""
    with patch("robosystems.config.tuning._get_parameter_manager") as mock_pm:
      mock_pm.return_value = None

      result = TuningConfig.get_float("admission/TEST_KEY", 85.5)
      assert result == 85.5

  def test_get_respects_env_override(self):
    """Test that environment variables override SSM and defaults."""
    with (
      patch("robosystems.config.tuning._get_env_override") as mock_env,
      patch("robosystems.config.tuning._get_parameter_manager") as mock_pm,
    ):
      mock_env.return_value = "env_value"
      mock_pm.return_value = MagicMock()

      result = TuningConfig.get("cache/TEST_KEY", "default")
      assert result == "env_value"

  def test_get_int_respects_env_override(self):
    """Test that get_int() respects environment variable override."""
    with (
      patch("robosystems.config.tuning._get_env_override") as mock_env,
      patch("robosystems.config.tuning._get_parameter_manager") as mock_pm,
    ):
      mock_env.return_value = "999"
      mock_pm.return_value = MagicMock()

      result = TuningConfig.get_int("cache/TEST_KEY", 123)
      assert result == 999

  def test_get_float_respects_env_override(self):
    """Test that get_float() respects environment variable override."""
    with (
      patch("robosystems.config.tuning._get_env_override") as mock_env,
      patch("robosystems.config.tuning._get_parameter_manager") as mock_pm,
    ):
      mock_env.return_value = "95.5"
      mock_pm.return_value = MagicMock()

      result = TuningConfig.get_float("admission/TEST_KEY", 85.0)
      assert result == 95.5

  def test_get_int_returns_default_on_invalid_env_value(self):
    """Test that get_int() returns default when env var is not a valid int."""
    with (
      patch("robosystems.config.tuning._get_env_override") as mock_env,
      patch("robosystems.config.tuning._get_parameter_manager") as mock_pm,
    ):
      mock_env.return_value = "not_an_int"
      mock_pm.return_value = None

      result = TuningConfig.get_int("cache/TEST_KEY", 123)
      assert result == 123

  def test_get_float_returns_default_on_invalid_env_value(self):
    """Test that get_float() returns default when env var is not a valid float."""
    with (
      patch("robosystems.config.tuning._get_env_override") as mock_env,
      patch("robosystems.config.tuning._get_parameter_manager") as mock_pm,
    ):
      mock_env.return_value = "not_a_float"
      mock_pm.return_value = None

      result = TuningConfig.get_float("admission/TEST_KEY", 85.0)
      assert result == 85.0


class TestTuningConfigCacheAccessors:
  """Test cache TTL accessor methods."""

  def test_get_cache_balance_ttl_returns_default(self):
    """Test that cache balance TTL returns default value."""
    with patch("robosystems.config.tuning._get_parameter_manager") as mock_pm:
      mock_pm.return_value = None

      result = TuningConfig.get_cache_balance_ttl()
      assert result == CacheDefaults.BALANCE_TTL

  def test_get_cache_summary_ttl_returns_default(self):
    """Test that cache summary TTL returns default value."""
    with patch("robosystems.config.tuning._get_parameter_manager") as mock_pm:
      mock_pm.return_value = None

      result = TuningConfig.get_cache_summary_ttl()
      assert result == CacheDefaults.SUMMARY_TTL

  def test_get_cache_jwt_ttl_returns_default(self):
    """Test that JWT cache TTL returns default value."""
    with patch("robosystems.config.tuning._get_parameter_manager") as mock_pm:
      mock_pm.return_value = None

      result = TuningConfig.get_cache_jwt_ttl()
      assert result == CacheDefaults.JWT_TTL

  def test_get_cache_api_key_ttl_returns_default(self):
    """Test that API key cache TTL returns default value."""
    with patch("robosystems.config.tuning._get_parameter_manager") as mock_pm:
      mock_pm.return_value = None

      result = TuningConfig.get_cache_api_key_ttl()
      assert result == CacheDefaults.API_KEY_TTL


class TestTuningConfigAdmissionAccessors:
  """Test admission control accessor methods."""

  def test_get_admission_memory_threshold_returns_default(self):
    """Test that memory threshold returns default value."""
    with patch("robosystems.config.tuning._get_parameter_manager") as mock_pm:
      mock_pm.return_value = None

      result = TuningConfig.get_admission_memory_threshold()
      assert result == AdmissionDefaults.MEMORY_THRESHOLD

  def test_get_admission_cpu_threshold_returns_default(self):
    """Test that CPU threshold returns default value."""
    with patch("robosystems.config.tuning._get_parameter_manager") as mock_pm:
      mock_pm.return_value = None

      result = TuningConfig.get_admission_cpu_threshold()
      assert result == AdmissionDefaults.CPU_THRESHOLD

  def test_get_admission_queue_threshold_returns_default(self):
    """Test that queue threshold returns default value."""
    with patch("robosystems.config.tuning._get_parameter_manager") as mock_pm:
      mock_pm.return_value = None

      result = TuningConfig.get_admission_queue_threshold()
      assert result == AdmissionDefaults.QUEUE_THRESHOLD


class TestTuningConfigQueueAccessors:
  """Test queue configuration accessor methods."""

  def test_get_queue_max_size_returns_default(self):
    """Test that queue max size returns default value."""
    with patch("robosystems.config.tuning._get_parameter_manager") as mock_pm:
      mock_pm.return_value = None

      result = TuningConfig.get_queue_max_size()
      assert result == QueueDefaults.MAX_SIZE

  def test_get_queue_max_concurrent_returns_default(self):
    """Test that queue max concurrent returns default value."""
    with patch("robosystems.config.tuning._get_parameter_manager") as mock_pm:
      mock_pm.return_value = None

      result = TuningConfig.get_queue_max_concurrent()
      assert result == QueueDefaults.MAX_CONCURRENT

  def test_get_queue_max_per_user_returns_default(self):
    """Test that queue max per user returns default value."""
    with patch("robosystems.config.tuning._get_parameter_manager") as mock_pm:
      mock_pm.return_value = None

      result = TuningConfig.get_queue_max_per_user()
      assert result == QueueDefaults.MAX_PER_USER


class TestTuningConfigCircuitBreakerAccessors:
  """Test circuit breaker accessor methods."""

  def test_get_circuit_breaker_threshold_returns_default(self):
    """Test that circuit breaker threshold returns default value."""
    with patch("robosystems.config.tuning._get_parameter_manager") as mock_pm:
      mock_pm.return_value = None

      result = TuningConfig.get_circuit_breaker_threshold()
      assert result == CircuitBreakerDefaults.FAILURE_THRESHOLD

  def test_get_circuit_breaker_timeout_returns_default(self):
    """Test that circuit breaker timeout returns default value."""
    with patch("robosystems.config.tuning._get_parameter_manager") as mock_pm:
      mock_pm.return_value = None

      result = TuningConfig.get_circuit_breaker_timeout()
      assert result == CircuitBreakerDefaults.TIMEOUT


class TestTuningConfigLoadSheddingAccessors:
  """Test load shedding accessor methods."""

  def test_get_load_shedding_start_pressure_returns_default(self):
    """Test that start pressure returns default value."""
    with patch("robosystems.config.tuning._get_parameter_manager") as mock_pm:
      mock_pm.return_value = None

      result = TuningConfig.get_load_shedding_start_pressure()
      assert result == LoadSheddingDefaults.START_PRESSURE

  def test_get_load_shedding_stop_pressure_returns_default(self):
    """Test that stop pressure returns default value."""
    with patch("robosystems.config.tuning._get_parameter_manager") as mock_pm:
      mock_pm.return_value = None

      result = TuningConfig.get_load_shedding_stop_pressure()
      assert result == LoadSheddingDefaults.STOP_PRESSURE


class TestTuningConfigMCPAccessors:
  """Test MCP accessor methods."""

  def test_get_mcp_max_result_rows_returns_default(self):
    """Test that MCP max result rows returns default value."""
    with patch("robosystems.config.tuning._get_parameter_manager") as mock_pm:
      mock_pm.return_value = None

      result = TuningConfig.get_mcp_max_result_rows()
      assert result == MCPDefaults.MAX_RESULT_ROWS

  def test_get_mcp_max_result_size_mb_returns_default(self):
    """Test that MCP max result size returns default value."""
    with patch("robosystems.config.tuning._get_parameter_manager") as mock_pm:
      mock_pm.return_value = None

      result = TuningConfig.get_mcp_max_result_size_mb()
      assert result == MCPDefaults.MAX_RESULT_SIZE_MB

  def test_get_mcp_pool_idle_timeout_returns_default(self):
    """Test that MCP pool idle timeout returns default value."""
    with patch("robosystems.config.tuning._get_parameter_manager") as mock_pm:
      mock_pm.return_value = None

      result = TuningConfig.get_mcp_pool_idle_timeout()
      assert result == MCPDefaults.POOL_IDLE_TIMEOUT


class TestTuningConfigWithSSM:
  """Test TuningConfig with mocked SSM responses."""

  def test_get_uses_ssm_value_when_available(self):
    """Test that SSM value is used when available."""
    with (
      patch("robosystems.config.tuning._get_env_override") as mock_env,
      patch("robosystems.config.tuning._get_parameter_manager") as mock_pm,
    ):
      mock_env.return_value = None  # No env override
      mock_manager = MagicMock()
      mock_manager.get_tuning_parameter.return_value = "ssm_value"
      mock_pm.return_value = mock_manager

      result = TuningConfig.get("cache/TEST_KEY", "default")
      assert result == "ssm_value"
      mock_manager.get_tuning_parameter.assert_called_once_with(
        "cache/TEST_KEY", "default"
      )

  def test_get_int_uses_ssm_value_when_available(self):
    """Test that SSM int value is used when available."""
    with (
      patch("robosystems.config.tuning._get_env_override") as mock_env,
      patch("robosystems.config.tuning._get_parameter_manager") as mock_pm,
    ):
      mock_env.return_value = None
      mock_manager = MagicMock()
      mock_manager.get_tuning_int.return_value = 600
      mock_pm.return_value = mock_manager

      result = TuningConfig.get_int("cache/BALANCE_TTL", 300)
      assert result == 600
      mock_manager.get_tuning_int.assert_called_once_with("cache/BALANCE_TTL", 300)


class TestTuningConfigUtilityMethods:
  """Test utility methods."""

  def test_preload_all_calls_parameter_manager(self):
    """Test that preload_all calls the parameter manager."""
    # Clear the cache first
    TuningConfig.preload_all.cache_clear()

    with patch("robosystems.config.tuning._get_parameter_manager") as mock_pm:
      mock_manager = MagicMock()
      mock_manager.get_all_tuning_parameters.return_value = {"cache/BALANCE_TTL": "300"}
      mock_pm.return_value = mock_manager

      result = TuningConfig.preload_all()
      assert result == {"cache/BALANCE_TTL": "300"}
      mock_manager.get_all_tuning_parameters.assert_called_once()

  def test_preload_all_returns_empty_when_no_manager(self):
    """Test that preload_all returns empty dict when no manager."""
    # Clear the cache first
    TuningConfig.preload_all.cache_clear()

    with patch("robosystems.config.tuning._get_parameter_manager") as mock_pm:
      mock_pm.return_value = None

      result = TuningConfig.preload_all()
      assert result == {}

  def test_refresh_clears_cache(self):
    """Test that refresh clears the preload cache."""
    # Clear the cache first
    TuningConfig.preload_all.cache_clear()

    with patch("robosystems.config.tuning._get_parameter_manager") as mock_pm:
      mock_manager = MagicMock()
      mock_manager.get_all_tuning_parameters.return_value = {"key": "value"}
      mock_pm.return_value = mock_manager

      # First call
      TuningConfig.preload_all()
      assert mock_manager.get_all_tuning_parameters.call_count == 1

      # Second call should use cache
      TuningConfig.preload_all()
      assert mock_manager.get_all_tuning_parameters.call_count == 1

      # Refresh should clear cache
      TuningConfig.refresh()

      # Third call should hit manager again
      TuningConfig.preload_all()
      assert mock_manager.get_all_tuning_parameters.call_count == 2


class TestEnvVarNaming:
  """Test environment variable naming convention."""

  def test_env_var_name_conversion(self):
    """Test that paths are converted to correct env var names."""
    # The conversion should be: TUNING_ + path.upper().replace("/", "_")
    # cache/BALANCE_TTL -> TUNING_CACHE_BALANCE_TTL
    # admission/MEMORY_THRESHOLD -> TUNING_ADMISSION_MEMORY_THRESHOLD

    with (
      patch("robosystems.config.tuning._get_env_override") as mock_env,
      patch("robosystems.config.tuning._get_parameter_manager") as mock_pm,
    ):
      mock_pm.return_value = None

      # Set up env override to return None by default
      mock_env.return_value = None

      # Call get to trigger env var check
      TuningConfig.get("cache/BALANCE_TTL", "default")

      # Verify the correct env var name was checked
      mock_env.assert_called_with("TUNING_CACHE_BALANCE_TTL")
