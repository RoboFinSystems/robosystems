import pytest

from robosystems.config import env
from robosystems.config.env import (
  EnvConfig,
  get_bool_env,
  get_float_env,
  get_int_env,
  get_list_env,
  get_str_env,
)


def test_get_int_env_returns_default_on_invalid(monkeypatch, capsys):
  monkeypatch.setenv("INVALID_INT", "not-a-number")

  value = get_int_env("INVALID_INT", 7)

  captured = capsys.readouterr()
  assert "Invalid INVALID_INT value" in captured.out
  assert value == 7


def test_get_float_env_returns_default(monkeypatch, capsys):
  monkeypatch.setenv("INVALID_FLOAT", "oops")

  value = get_float_env("INVALID_FLOAT", 3.14)

  captured = capsys.readouterr()
  assert "Invalid INVALID_FLOAT value" in captured.out
  assert value == pytest.approx(3.14)


@pytest.mark.parametrize(
  "raw,expected",
  [
    ("true", True),
    ("1", True),
    ("yes", True),
    ("on", True),
    ("false", False),
    ("0", False),
    ("no", False),
    ("off", False),
  ],
)
def test_get_bool_env_parses_truthy_values(monkeypatch, raw, expected):
  monkeypatch.setenv("BOOL_TEST", raw)

  assert get_bool_env("BOOL_TEST", default=not expected) is expected


def test_get_str_env_uses_default_when_missing(monkeypatch):
  monkeypatch.delenv("MISSING_STR", raising=False)

  assert get_str_env("MISSING_STR", "fallback") == "fallback"


def test_get_list_env_splits_and_strips(monkeypatch):
  monkeypatch.setenv("LIST_ENV", " alpha , beta,gamma ,, ")

  assert get_list_env("LIST_ENV") == ["alpha", "beta", "gamma"]


def test_get_list_env_returns_empty_for_missing(monkeypatch):
  monkeypatch.delenv("EMPTY_LIST", raising=False)

  assert get_list_env("EMPTY_LIST") == []


@pytest.mark.parametrize(
  "value,method,expected",
  [
    ("prod", EnvConfig.is_production, True),
    ("production", EnvConfig.is_production, True),
    ("staging", EnvConfig.is_staging, True),
    ("stage", EnvConfig.is_staging, True),
    ("dev", EnvConfig.is_development, True),
    ("development", EnvConfig.is_development, True),
    ("local", EnvConfig.is_development, True),
    ("test", EnvConfig.is_test, True),
    ("testing", EnvConfig.is_test, True),
  ],
)
def test_environment_checks(monkeypatch, value, method, expected):
  monkeypatch.setattr(EnvConfig, "ENVIRONMENT", value)

  assert method.__get__(EnvConfig, EnvConfig)() is expected


@pytest.mark.parametrize(
  "value,expected",
  [
    ("prod", "production"),
    ("production", "production"),
    ("staging", "staging"),
    ("stage", "staging"),
    ("dev", "development"),
    ("local", "development"),
  ],
)
def test_get_environment_key(monkeypatch, value, expected):
  monkeypatch.setattr(EnvConfig, "ENVIRONMENT", value)

  assert EnvConfig.get_environment_key() == expected


def test_is_using_secrets_manager_true_for_prod(monkeypatch):
  monkeypatch.setattr(env, "SECRETS_MANAGER_AVAILABLE", True, raising=False)
  monkeypatch.setattr(EnvConfig, "SECRETS_MANAGER_AVAILABLE", True, raising=False)
  monkeypatch.setattr(EnvConfig, "ENVIRONMENT", "prod")

  assert EnvConfig.is_using_secrets_manager()


def test_is_using_secrets_manager_false_for_dev(monkeypatch):
  monkeypatch.setattr(env, "SECRETS_MANAGER_AVAILABLE", True, raising=False)
  monkeypatch.setattr(EnvConfig, "SECRETS_MANAGER_AVAILABLE", True, raising=False)
  monkeypatch.setattr(EnvConfig, "ENVIRONMENT", "dev")

  assert not EnvConfig.is_using_secrets_manager()


def test_is_using_secrets_manager_false_when_unavailable(monkeypatch):
  monkeypatch.setattr(env, "SECRETS_MANAGER_AVAILABLE", False, raising=False)
  monkeypatch.setattr(EnvConfig, "SECRETS_MANAGER_AVAILABLE", False, raising=False)
  monkeypatch.setitem(
    EnvConfig.is_using_secrets_manager.__func__.__globals__,
    "SECRETS_MANAGER_AVAILABLE",
    False,
  )
  monkeypatch.setattr(EnvConfig, "ENVIRONMENT", "prod")

  assert not EnvConfig.is_using_secrets_manager()

  monkeypatch.setattr(EnvConfig, "ENVIRONMENT", "dev")
  assert not EnvConfig.is_using_secrets_manager()


def test_get_kuzu_tier_config_uses_tier_config_overrides(monkeypatch):
  def fake_get_instance_config(cls, tier, environment=None):
    assert tier == "kuzu-shared"
    return {
      "max_memory_mb": 4096,
      "memory_per_db_mb": 512,
      "chunk_size": 256,
      "query_timeout": 200,
      "max_query_length": 2048,
      "connection_pool_size": 12,
      "databases_per_instance": 15,
    }

  def fake_get_tier_config(cls, tier, environment=None):
    return {
      "storage_limit_gb": 750,
      "monthly_credits": 2500,
      "api_rate_multiplier": 1.8,
      "max_subgraphs": 6,
    }

  monkeypatch.setattr(EnvConfig, "CLUSTER_TIER", "shared_master", raising=False)
  monkeypatch.setattr(EnvConfig, "KUZU_NODE_TYPE", "", raising=False)
  monkeypatch.setattr(EnvConfig, "ENVIRONMENT", "prod", raising=False)
  monkeypatch.setattr(EnvConfig, "KUZU_MAX_MEMORY_MB", 2048, raising=False)
  monkeypatch.setattr(EnvConfig, "KUZU_MAX_MEMORY_PER_DB_MB", 256, raising=False)
  monkeypatch.setattr(EnvConfig, "GRAPH_QUERY_TIMEOUT", 60, raising=False)
  monkeypatch.setattr(EnvConfig, "GRAPH_MAX_QUERY_LENGTH", 1024, raising=False)
  monkeypatch.setattr(env, "get_int_env", lambda key, default: default, raising=False)
  monkeypatch.setattr(
    "robosystems.config.tier_config.TierConfig.get_instance_config",
    classmethod(fake_get_instance_config),
  )
  monkeypatch.setattr(
    "robosystems.config.tier_config.TierConfig.get_tier_config",
    classmethod(fake_get_tier_config),
  )

  config = EnvConfig.get_kuzu_tier_config()

  assert config["max_memory_mb"] == 4096
  assert config["memory_per_db_mb"] == 512
  assert config["chunk_size"] == 256
  assert config["query_timeout"] == 200
  assert config["max_query_length"] == 2048
  assert config["connection_pool_size"] == 12
  assert config["databases_per_instance"] == 15
  assert config["max_databases"] == 15
  assert config["tier"] == "kuzu-shared"
  assert config["storage_limit_gb"] == 750
  assert config["monthly_credits"] == 2500
  assert config["api_rate_multiplier"] == 1.8
  assert config["max_subgraphs"] == 6


def test_get_kuzu_tier_config_falls_back_on_errors(monkeypatch):
  def raise_error(cls, *args, **kwargs):
    raise RuntimeError("fail")

  def fallback_get_int_env(key, default):
    overrides = {
      "KUZU_CHUNK_SIZE": 321,
      "KUZU_CONNECTION_POOL_SIZE": 17,
      "KUZU_DATABASES_PER_INSTANCE": 13,
    }
    return overrides.get(key, default)

  monkeypatch.setattr(EnvConfig, "ENVIRONMENT", "prod", raising=False)
  monkeypatch.setattr(EnvConfig, "KUZU_MAX_MEMORY_MB", 5120, raising=False)
  monkeypatch.setattr(EnvConfig, "KUZU_MAX_MEMORY_PER_DB_MB", 640, raising=False)
  monkeypatch.setattr(EnvConfig, "GRAPH_QUERY_TIMEOUT", 45, raising=False)
  monkeypatch.setattr(EnvConfig, "GRAPH_MAX_QUERY_LENGTH", 9000, raising=False)
  monkeypatch.setattr(EnvConfig, "KUZU_MAX_DATABASES_PER_NODE", 25, raising=False)
  monkeypatch.setattr(env, "get_int_env", fallback_get_int_env, raising=False)
  monkeypatch.setitem(
    EnvConfig.get_kuzu_tier_config.__func__.__globals__,
    "get_int_env",
    fallback_get_int_env,
  )
  monkeypatch.setattr(
    "robosystems.config.tier_config.TierConfig.get_instance_config",
    classmethod(raise_error),
  )
  monkeypatch.setattr(
    "robosystems.config.tier_config.TierConfig.get_tier_config",
    classmethod(raise_error),
  )

  config = EnvConfig.get_kuzu_tier_config()

  assert config["max_memory_mb"] == 5120
  assert config["memory_per_db_mb"] == 640
  assert config["chunk_size"] == 321
  assert config["connection_pool_size"] == 17
  assert config["databases_per_instance"] == 13
  assert config["max_databases"] == 25
  assert config["tier"] == "kuzu-standard"
  assert config["storage_limit_gb"] == 500
  assert config["monthly_credits"] == 10000
  assert config["api_rate_multiplier"] == 1.0
  assert config["max_subgraphs"] == 0


def test_get_main_cors_origins_respects_environment(monkeypatch):
  monkeypatch.setattr(EnvConfig, "ENVIRONMENT", "prod", raising=False)
  assert EnvConfig.get_main_cors_origins() == [
    "https://roboledger.ai",
    "https://roboinvestor.ai",
    "https://robosystems.ai",
  ]

  monkeypatch.setattr(EnvConfig, "ENVIRONMENT", "staging", raising=False)
  assert EnvConfig.get_main_cors_origins() == [
    "https://staging.roboledger.ai",
    "https://staging.roboinvestor.ai",
    "https://staging.robosystems.ai",
  ]

  monkeypatch.setattr(EnvConfig, "ENVIRONMENT", "dev", raising=False)
  origins = EnvConfig.get_main_cors_origins()
  assert "http://localhost:3000" in origins
  assert "https://roboledger.ai" in origins


def test_get_kuzu_cors_origins(monkeypatch):
  monkeypatch.setattr(EnvConfig, "ENVIRONMENT", "prod", raising=False)
  assert EnvConfig.get_kuzu_cors_origins() == []

  monkeypatch.setattr(EnvConfig, "ENVIRONMENT", "staging", raising=False)
  assert EnvConfig.get_kuzu_cors_origins() == []

  monkeypatch.setattr(EnvConfig, "ENVIRONMENT", "dev", raising=False)
  assert EnvConfig.get_kuzu_cors_origins() == ["*"]


def test_get_celery_config_without_auth_token(monkeypatch):
  monkeypatch.setattr(EnvConfig, "ENVIRONMENT", "dev", raising=False)
  monkeypatch.setattr(EnvConfig, "CELERY_BROKER_URL", "redis://broker", raising=False)
  monkeypatch.setattr(
    EnvConfig, "CELERY_RESULT_BACKEND", "redis://results", raising=False
  )
  monkeypatch.setattr(EnvConfig, "CELERY_TASK_TIME_LIMIT", 120, raising=False)
  monkeypatch.setattr(EnvConfig, "CELERY_TASK_SOFT_TIME_LIMIT", 90, raising=False)
  monkeypatch.setattr(EnvConfig, "CELERY_WORKER_PREFETCH_MULTIPLIER", 4, raising=False)
  monkeypatch.delenv("CELERY_BROKER_URL", raising=False)
  monkeypatch.delenv("CELERY_RESULT_BACKEND", raising=False)

  monkeypatch.setattr(
    "robosystems.config.valkey_registry.ValkeyURLBuilder.get_auth_token",
    staticmethod(lambda: None),
  )

  config = EnvConfig.get_celery_config()
  assert config["broker_url"] == "redis://broker"
  assert config["result_backend"] == "redis://results"
  assert config["task_time_limit"] == 120
  assert config["task_soft_time_limit"] == 90
  assert config["worker_prefetch_multiplier"] == 4


def test_get_celery_config_builds_authenticated_urls(monkeypatch):
  monkeypatch.setattr(EnvConfig, "ENVIRONMENT", "prod", raising=False)
  monkeypatch.setattr(EnvConfig, "CELERY_BROKER_URL", "redis://default", raising=False)
  monkeypatch.setattr(
    EnvConfig, "CELERY_RESULT_BACKEND", "redis://default-results", raising=False
  )
  monkeypatch.setattr(EnvConfig, "CELERY_TASK_TIME_LIMIT", 60, raising=False)
  monkeypatch.setattr(EnvConfig, "CELERY_TASK_SOFT_TIME_LIMIT", 55, raising=False)
  monkeypatch.setattr(EnvConfig, "CELERY_WORKER_PREFETCH_MULTIPLIER", 1, raising=False)
  monkeypatch.delenv("CELERY_BROKER_URL", raising=False)
  monkeypatch.delenv("CELERY_RESULT_BACKEND", raising=False)

  monkeypatch.setattr(
    "robosystems.config.valkey_registry.ValkeyURLBuilder.get_auth_token",
    staticmethod(lambda: "token"),
  )

  def fake_build_authenticated_url(database, base_url=None, include_ssl_params=True):
    return f"auth://{database.name}"

  monkeypatch.setattr(
    "robosystems.config.valkey_registry.ValkeyURLBuilder.build_authenticated_url",
    staticmethod(fake_build_authenticated_url),
  )

  config = EnvConfig.get_celery_config()
  assert config["broker_url"] == "auth://CELERY_BROKER"
  assert config["result_backend"] == "auth://CELERY_RESULTS"
  assert config["task_time_limit"] == 60
  assert config["task_soft_time_limit"] == 55
  assert config["worker_prefetch_multiplier"] == 1


def test_get_valkey_url_with_enum(monkeypatch):
  from robosystems.config.valkey_registry import ValkeyDatabase

  monkeypatch.setattr(EnvConfig, "VALKEY_URL", "redis://base", raising=False)

  monkeypatch.setattr(
    "robosystems.config.valkey_registry.ValkeyURLBuilder.build_url",
    staticmethod(lambda base_url, database, **_: f"{base_url}-{database.name}"),
  )

  result = EnvConfig.get_valkey_url(ValkeyDatabase.CELERY_BROKER)
  assert result == "redis://base-CELERY_BROKER"


def test_get_valkey_url_with_integer(monkeypatch):
  monkeypatch.setattr(EnvConfig, "VALKEY_URL", "redis://base", raising=False)

  result = EnvConfig.get_valkey_url(5)
  assert result == "redis://base/5"
