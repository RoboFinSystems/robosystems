"""
Configuration for query queue and admission control.

Routes all configuration through the centralized env.py config.
"""

from typing import Optional
from robosystems.config import env


class QueryQueueConfig:
  """Configuration for query queue system."""

  # Queue configuration
  MAX_QUEUE_SIZE: int = env.QUERY_QUEUE_MAX_SIZE
  MAX_CONCURRENT_QUERIES: int = env.QUERY_QUEUE_MAX_CONCURRENT
  MAX_QUERIES_PER_USER: int = env.QUERY_QUEUE_MAX_PER_USER
  QUERY_TIMEOUT: int = env.QUERY_QUEUE_TIMEOUT

  # Admission control thresholds
  MEMORY_THRESHOLD: float = env.ADMISSION_MEMORY_THRESHOLD
  CPU_THRESHOLD: float = env.ADMISSION_CPU_THRESHOLD
  QUEUE_THRESHOLD: float = env.ADMISSION_QUEUE_THRESHOLD
  CHECK_INTERVAL: float = env.ADMISSION_CHECK_INTERVAL

  # Load shedding configuration
  LOAD_SHEDDING_ENABLED: bool = env.LOAD_SHEDDING_ENABLED
  SHED_START_PRESSURE: float = env.LOAD_SHED_START_PRESSURE
  SHED_STOP_PRESSURE: float = env.LOAD_SHED_STOP_PRESSURE

  # Priority configuration
  DEFAULT_PRIORITY: int = env.QUERY_DEFAULT_PRIORITY
  PRIORITY_BOOST_PREMIUM: int = env.QUERY_PRIORITY_BOOST_PREMIUM

  @classmethod
  def get_queue_config(cls) -> dict:
    """Get queue configuration as dict."""
    return {
      "max_queue_size": cls.MAX_QUEUE_SIZE,
      "max_concurrent_queries": cls.MAX_CONCURRENT_QUERIES,
      "max_queries_per_user": cls.MAX_QUERIES_PER_USER,
      "query_timeout": cls.QUERY_TIMEOUT,
    }

  @classmethod
  def get_admission_config(cls) -> dict:
    """Get admission control configuration as dict."""
    return {
      "memory_threshold": cls.MEMORY_THRESHOLD,
      "cpu_threshold": cls.CPU_THRESHOLD,
      "queue_threshold": cls.QUEUE_THRESHOLD,
      "check_interval": cls.CHECK_INTERVAL,
      "load_shedding_enabled": cls.LOAD_SHEDDING_ENABLED,
    }

  @classmethod
  def get_priority_for_user(cls, user_tier: Optional[str]) -> int:
    """Get priority based on user tier."""
    if user_tier in ["kuzu-xlarge", "kuzu-large"]:
      return cls.DEFAULT_PRIORITY + cls.PRIORITY_BOOST_PREMIUM
    return cls.DEFAULT_PRIORITY
