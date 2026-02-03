from unittest.mock import patch

from robosystems.config.query_queue import QueryQueueConfig
from robosystems.config.tuning import TuningConfig


def test_get_queue_config_returns_expected_values():
  """Test that get_queue_config returns values from TuningConfig methods."""
  with (
    patch.object(TuningConfig, "get_queue_max_size", return_value=100),
    patch.object(TuningConfig, "get_queue_max_concurrent", return_value=10),
    patch.object(TuningConfig, "get_queue_max_per_user", return_value=4),
    patch.object(TuningConfig, "get_queue_timeout", return_value=30),
  ):
    assert QueryQueueConfig.get_queue_config() == {
      "max_queue_size": 100,
      "max_concurrent_queries": 10,
      "max_queries_per_user": 4,
      "query_timeout": 30,
    }


def test_get_admission_config_includes_load_shedding(monkeypatch):
  """Test that get_admission_config returns values from TuningConfig methods."""
  monkeypatch.setattr(QueryQueueConfig, "CHECK_INTERVAL", 5)
  monkeypatch.setattr(QueryQueueConfig, "LOAD_SHEDDING_ENABLED", True)

  with (
    patch.object(TuningConfig, "get_admission_memory_threshold", return_value=0.8),
    patch.object(TuningConfig, "get_admission_cpu_threshold", return_value=0.75),
    patch.object(TuningConfig, "get_admission_queue_threshold", return_value=0.9),
    patch.object(TuningConfig, "get_load_shedding_start_pressure", return_value=0.8),
    patch.object(TuningConfig, "get_load_shedding_stop_pressure", return_value=0.6),
  ):
    assert QueryQueueConfig.get_admission_config() == {
      "memory_threshold": 0.8,
      "cpu_threshold": 0.75,
      "queue_threshold": 0.9,
      "check_interval": 5,
      "load_shedding_enabled": True,
      "shed_start_pressure": 0.8,
      "shed_stop_pressure": 0.6,
    }


def test_get_priority_for_user_applies_premium_boost(monkeypatch):
  """Test that priority boost is applied for premium tiers."""
  monkeypatch.setattr(QueryQueueConfig, "DEFAULT_PRIORITY", 10)
  monkeypatch.setattr(QueryQueueConfig, "PRIORITY_BOOST_PREMIUM", 5)

  assert QueryQueueConfig.get_priority_for_user("ladybug-xlarge") == 15
  assert QueryQueueConfig.get_priority_for_user("ladybug-large") == 15
  assert QueryQueueConfig.get_priority_for_user("ladybug-standard") == 10
  assert QueryQueueConfig.get_priority_for_user(None) == 10
