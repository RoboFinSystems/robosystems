from robosystems.config.query_queue import QueryQueueConfig


def test_get_queue_config_returns_expected_values(monkeypatch):
  monkeypatch.setattr(QueryQueueConfig, "MAX_QUEUE_SIZE", 100)
  monkeypatch.setattr(QueryQueueConfig, "MAX_CONCURRENT_QUERIES", 10)
  monkeypatch.setattr(QueryQueueConfig, "MAX_QUERIES_PER_USER", 4)
  monkeypatch.setattr(QueryQueueConfig, "QUERY_TIMEOUT", 30)

  assert QueryQueueConfig.get_queue_config() == {
    "max_queue_size": 100,
    "max_concurrent_queries": 10,
    "max_queries_per_user": 4,
    "query_timeout": 30,
  }


def test_get_admission_config_includes_load_shedding(monkeypatch):
  monkeypatch.setattr(QueryQueueConfig, "MEMORY_THRESHOLD", 0.8)
  monkeypatch.setattr(QueryQueueConfig, "CPU_THRESHOLD", 0.75)
  monkeypatch.setattr(QueryQueueConfig, "QUEUE_THRESHOLD", 0.9)
  monkeypatch.setattr(QueryQueueConfig, "CHECK_INTERVAL", 5)
  monkeypatch.setattr(QueryQueueConfig, "LOAD_SHEDDING_ENABLED", True)

  assert QueryQueueConfig.get_admission_config() == {
    "memory_threshold": 0.8,
    "cpu_threshold": 0.75,
    "queue_threshold": 0.9,
    "check_interval": 5,
    "load_shedding_enabled": True,
  }


def test_get_priority_for_user_applies_premium_boost(monkeypatch):
  monkeypatch.setattr(QueryQueueConfig, "DEFAULT_PRIORITY", 10)
  monkeypatch.setattr(QueryQueueConfig, "PRIORITY_BOOST_PREMIUM", 5)

  assert QueryQueueConfig.get_priority_for_user("ladybug-xlarge") == 15
  assert QueryQueueConfig.get_priority_for_user("ladybug-large") == 15
  assert QueryQueueConfig.get_priority_for_user("ladybug-standard") == 10
  assert QueryQueueConfig.get_priority_for_user(None) == 10
