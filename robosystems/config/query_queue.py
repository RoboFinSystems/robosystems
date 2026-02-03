"""
Configuration for query queue and admission control.

Uses TuningConfig for runtime tunability via SSM Parameter Store.
Falls back to defaults when SSM is not available.
"""

from robosystems.config import env
from robosystems.config.tuning import TuningConfig


class QueryQueueConfig:
  """Configuration for query queue system with runtime tunability."""

  # Priority configuration (not tunable - business logic)
  DEFAULT_PRIORITY: int = env.QUERY_DEFAULT_PRIORITY
  PRIORITY_BOOST_PREMIUM: int = env.QUERY_PRIORITY_BOOST_PREMIUM

  # Check interval (not tunable - performance sensitive)
  CHECK_INTERVAL: float = env.ADMISSION_CHECK_INTERVAL

  # Load shedding enabled flag (feature flag, not tunable)
  LOAD_SHEDDING_ENABLED: bool = env.LOAD_SHEDDING_ENABLED

  @classmethod
  def get_max_queue_size(cls) -> int:
    """Get maximum queue size (runtime tunable via SSM)."""
    return TuningConfig.get_queue_max_size()

  @classmethod
  def get_max_concurrent_queries(cls) -> int:
    """Get maximum concurrent queries (runtime tunable via SSM)."""
    return TuningConfig.get_queue_max_concurrent()

  @classmethod
  def get_max_queries_per_user(cls) -> int:
    """Get maximum queries per user (runtime tunable via SSM)."""
    return TuningConfig.get_queue_max_per_user()

  @classmethod
  def get_query_timeout(cls) -> int:
    """Get query timeout in seconds (runtime tunable via SSM)."""
    return TuningConfig.get_queue_timeout()

  @classmethod
  def get_memory_threshold(cls) -> float:
    """Get memory threshold for admission control (runtime tunable via SSM)."""
    return TuningConfig.get_admission_memory_threshold()

  @classmethod
  def get_cpu_threshold(cls) -> float:
    """Get CPU threshold for admission control (runtime tunable via SSM)."""
    return TuningConfig.get_admission_cpu_threshold()

  @classmethod
  def get_queue_threshold(cls) -> float:
    """Get queue threshold for admission control (runtime tunable via SSM)."""
    return TuningConfig.get_admission_queue_threshold()

  @classmethod
  def get_shed_start_pressure(cls) -> float:
    """Get load shedding start pressure (runtime tunable via SSM)."""
    return TuningConfig.get_load_shedding_start_pressure()

  @classmethod
  def get_shed_stop_pressure(cls) -> float:
    """Get load shedding stop pressure (runtime tunable via SSM)."""
    return TuningConfig.get_load_shedding_stop_pressure()

  @classmethod
  def get_queue_config(cls) -> dict:
    """Get queue configuration as dict (runtime tunable values)."""
    return {
      "max_queue_size": cls.get_max_queue_size(),
      "max_concurrent_queries": cls.get_max_concurrent_queries(),
      "max_queries_per_user": cls.get_max_queries_per_user(),
      "query_timeout": cls.get_query_timeout(),
    }

  @classmethod
  def get_admission_config(cls) -> dict:
    """Get admission control configuration as dict (runtime tunable values)."""
    return {
      "memory_threshold": cls.get_memory_threshold(),
      "cpu_threshold": cls.get_cpu_threshold(),
      "queue_threshold": cls.get_queue_threshold(),
      "check_interval": cls.CHECK_INTERVAL,
      "load_shedding_enabled": cls.LOAD_SHEDDING_ENABLED,
    }

  @classmethod
  def get_priority_for_user(cls, user_tier: str | None) -> int:
    """Get priority based on user tier."""
    if user_tier in ["ladybug-xlarge", "ladybug-large"]:
      return cls.DEFAULT_PRIORITY + cls.PRIORITY_BOOST_PREMIUM
    return cls.DEFAULT_PRIORITY
