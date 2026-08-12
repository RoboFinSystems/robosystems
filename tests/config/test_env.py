import pytest

from robosystems.config import env
from robosystems.config.env import (
  EnvConfig,
  get_bool_env,
  get_float_env,
  get_int_env,
  get_list_env,
  get_str_env,
  get_tuning_float,
  get_tuning_int,
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


def test_get_lbug_tier_config_uses_tier_config_overrides(monkeypatch):
  def fake_get_instance_config(cls, tier, environment=None):
    assert tier == "ladybug-shared"
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

  monkeypatch.setattr(EnvConfig, "CLUSTER_TIER", "ladybug-shared", raising=False)
  monkeypatch.setattr(EnvConfig, "LBUG_NODE_TYPE", "", raising=False)
  monkeypatch.setattr(EnvConfig, "ENVIRONMENT", "prod", raising=False)
  monkeypatch.setattr(EnvConfig, "LBUG_MAX_MEMORY_MB", 2048, raising=False)
  monkeypatch.setattr(EnvConfig, "LBUG_MAX_MEMORY_PER_DB_MB", 256, raising=False)
  monkeypatch.setattr(EnvConfig, "GRAPH_QUERY_TIMEOUT", 60, raising=False)
  monkeypatch.setattr(EnvConfig, "GRAPH_MAX_QUERY_LENGTH", 1024, raising=False)
  monkeypatch.setattr(env, "get_int_env", lambda key, default: default, raising=False)
  monkeypatch.setattr(
    "robosystems.config.graph_tier.GraphTierConfig.get_instance_config",
    classmethod(fake_get_instance_config),
  )
  monkeypatch.setattr(
    "robosystems.config.graph_tier.GraphTierConfig.get_tier_config",
    classmethod(fake_get_tier_config),
  )

  config = EnvConfig.get_lbug_tier_config()

  assert config["max_memory_mb"] == 4096
  assert config["memory_per_db_mb"] == 512
  assert config["chunk_size"] == 256
  assert config["query_timeout"] == 200
  assert config["max_query_length"] == 2048
  assert config["connection_pool_size"] == 12
  assert config["databases_per_instance"] == 15
  assert config["max_databases"] == 15
  assert config["tier"] == "ladybug-shared"
  assert config["max_subgraphs"] == 6
  # Storage, credits, and rate multipliers are deliberately absent: those keys
  # never existed in graph.yml, so this dict only ever fabricated defaults.
  # Their sources of truth are GraphTierConfig, BillingConfig, RateLimitConfig.
  for fabricated in ("storage_limit_gb", "monthly_credits", "api_rate_multiplier"):
    assert fabricated not in config


def test_get_lbug_tier_config_falls_back_on_errors(monkeypatch):
  def raise_error(cls, *args, **kwargs):
    raise RuntimeError("fail")

  def fallback_get_int_env(key, default):
    overrides = {
      "LBUG_MAX_MEMORY_MB": 5120,
      "LBUG_MAX_MEMORY_PER_DB_MB": 640,
      "LBUG_CHUNK_SIZE": 321,
      "LBUG_CONNECTION_POOL_SIZE": 17,
      "LBUG_DATABASES_PER_INSTANCE": 13,
    }
    return overrides.get(key, default)

  monkeypatch.setattr(EnvConfig, "ENVIRONMENT", "prod", raising=False)
  monkeypatch.setattr(EnvConfig, "GRAPH_QUERY_TIMEOUT", 45, raising=False)
  monkeypatch.setattr(env, "get_int_env", fallback_get_int_env, raising=False)
  monkeypatch.setitem(
    EnvConfig.get_lbug_tier_config.__func__.__globals__,
    "get_int_env",
    fallback_get_int_env,
  )
  monkeypatch.setattr(
    "robosystems.config.graph_tier.GraphTierConfig.get_instance_config",
    classmethod(raise_error),
  )
  monkeypatch.setattr(
    "robosystems.config.graph_tier.GraphTierConfig.get_tier_config",
    classmethod(raise_error),
  )

  config = EnvConfig.get_lbug_tier_config()

  assert config["max_memory_mb"] == 5120
  assert config["memory_per_db_mb"] == 640
  assert config["chunk_size"] == 321
  assert config["connection_pool_size"] == 17
  assert config["databases_per_instance"] == 13
  # max_databases now uses same source as databases_per_instance (consolidated config)
  assert config["max_databases"] == 13
  assert config["tier"] == "ladybug-standard"
  for fabricated in ("storage_limit_gb", "monthly_credits", "api_rate_multiplier"):
    assert fabricated not in config
  assert config["max_subgraphs"] == 0


def test_get_main_cors_origins_respects_environment(monkeypatch):
  # Prod with the URL defaults — byte-identical to the historical hardcoded
  # list, proving the derivation changes nothing on the managed platform.
  monkeypatch.setattr(EnvConfig, "ENVIRONMENT", "prod", raising=False)
  assert EnvConfig.get_main_cors_origins() == [
    "https://roboledger.ai",
    "https://roboinvestor.ai",
    "https://robosystems.ai",
    "https://holon.robosystems.ai",
  ]

  # Staging derives from the deployment's own URLs — CloudFormation passes
  # the staging.* values on managed staging (workflow fallbacks match).
  monkeypatch.setattr(EnvConfig, "ENVIRONMENT", "staging", raising=False)
  monkeypatch.setattr(
    EnvConfig, "ROBOLEDGER_URL", "https://staging.roboledger.ai", raising=False
  )
  monkeypatch.setattr(
    EnvConfig, "ROBOINVESTOR_URL", "https://staging.roboinvestor.ai", raising=False
  )
  monkeypatch.setattr(
    EnvConfig, "ROBOSYSTEMS_URL", "https://staging.robosystems.ai", raising=False
  )
  monkeypatch.setattr(
    EnvConfig, "HOLON_URL", "https://staging.holon.robosystems.ai", raising=False
  )
  assert EnvConfig.get_main_cors_origins() == [
    "https://staging.roboledger.ai",
    "https://staging.roboinvestor.ai",
    "https://staging.robosystems.ai",
    "https://staging.holon.robosystems.ai",
  ]

  monkeypatch.setattr(EnvConfig, "ENVIRONMENT", "dev", raising=False)
  origins = EnvConfig.get_main_cors_origins()
  assert "http://localhost:3000" in origins
  assert "https://roboledger.ai" in origins


def test_get_main_cors_origins_derives_fork_domain(monkeypatch):
  """A dedicated-tenant fork serving its own domain allows its own frontend
  without code changes: origins derive from the deployment's app URLs, with
  empty URLs skipped, paths stripped, and duplicates collapsed."""
  monkeypatch.setattr(EnvConfig, "ENVIRONMENT", "prod", raising=False)
  monkeypatch.setattr(
    EnvConfig, "ROBOLEDGER_URL", "https://tenant.robosystems.ai", raising=False
  )
  monkeypatch.setattr(EnvConfig, "ROBOINVESTOR_URL", "", raising=False)
  monkeypatch.setattr(
    EnvConfig, "ROBOSYSTEMS_URL", "https://tenant.robosystems.ai/", raising=False
  )
  monkeypatch.setattr(
    EnvConfig, "HOLON_URL", "https://holon.robosystems.ai", raising=False
  )
  assert EnvConfig.get_main_cors_origins() == [
    "https://tenant.robosystems.ai",
    "https://holon.robosystems.ai",
  ]


def test_get_lbug_cors_origins(monkeypatch):
  monkeypatch.setattr(EnvConfig, "ENVIRONMENT", "prod", raising=False)
  assert EnvConfig.get_lbug_cors_origins() == []

  monkeypatch.setattr(EnvConfig, "ENVIRONMENT", "staging", raising=False)
  assert EnvConfig.get_lbug_cors_origins() == []

  monkeypatch.setattr(EnvConfig, "ENVIRONMENT", "dev", raising=False)
  assert EnvConfig.get_lbug_cors_origins() == ["*"]


def test_get_valkey_url_with_enum(monkeypatch):
  from robosystems.config.valkey_registry import ValkeyDatabase

  monkeypatch.setattr(EnvConfig, "VALKEY_URL", "redis://base", raising=False)

  monkeypatch.setattr(
    "robosystems.config.valkey_registry.ValkeyURLBuilder.build_url",
    staticmethod(lambda base_url, database, **_: f"{base_url}-{database.name}"),
  )

  result = EnvConfig.get_valkey_url(ValkeyDatabase.AUTH)
  assert result == "redis://base-AUTH"


def test_get_valkey_url_with_integer(monkeypatch):
  monkeypatch.setattr(EnvConfig, "VALKEY_URL", "redis://base", raising=False)

  result = EnvConfig.get_valkey_url(5)
  assert result == "redis://base/5"


class TestTuningEnvConvention:
  """get_tuning_int/float honor the documented TUNING_{CATEGORY}_{KEY} env var
  (derived from the ssm_path) as well as the raw env_key (backward compat)."""

  def test_env_key_derivation(self):
    from robosystems.config.env import _tuning_env_key

    assert _tuning_env_key("graphql/MAX_DEPTH") == "TUNING_GRAPHQL_MAX_DEPTH"
    assert (
      _tuning_env_key("lbug_admission/MEMORY_THRESHOLD")
      == "TUNING_LBUG_ADMISSION_MEMORY_THRESHOLD"
    )

  def test_int_honors_tuning_prefix(self, monkeypatch):
    monkeypatch.delenv("EXTENSIONS_GRAPHQL_MAX_DEPTH", raising=False)
    monkeypatch.setenv("TUNING_GRAPHQL_MAX_DEPTH", "7")

    assert get_tuning_int("EXTENSIONS_GRAPHQL_MAX_DEPTH", "graphql/MAX_DEPTH", 15) == 7

  def test_int_honors_raw_env_key(self, monkeypatch):
    monkeypatch.delenv("TUNING_GRAPHQL_MAX_DEPTH", raising=False)
    monkeypatch.setenv("EXTENSIONS_GRAPHQL_MAX_DEPTH", "9")

    assert get_tuning_int("EXTENSIONS_GRAPHQL_MAX_DEPTH", "graphql/MAX_DEPTH", 15) == 9

  def test_int_raw_env_key_wins_when_both_set(self, monkeypatch):
    monkeypatch.setenv("EXTENSIONS_GRAPHQL_MAX_DEPTH", "9")
    monkeypatch.setenv("TUNING_GRAPHQL_MAX_DEPTH", "7")

    assert get_tuning_int("EXTENSIONS_GRAPHQL_MAX_DEPTH", "graphql/MAX_DEPTH", 15) == 9

  def test_int_falls_back_to_default(self, monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.delenv("EXTENSIONS_GRAPHQL_MAX_DEPTH", raising=False)
    monkeypatch.delenv("TUNING_GRAPHQL_MAX_DEPTH", raising=False)

    assert get_tuning_int("EXTENSIONS_GRAPHQL_MAX_DEPTH", "graphql/MAX_DEPTH", 15) == 15

  def test_float_honors_tuning_prefix(self, monkeypatch):
    monkeypatch.delenv("LBUG_ADMISSION_MEMORY_THRESHOLD", raising=False)
    monkeypatch.setenv("TUNING_LBUG_ADMISSION_MEMORY_THRESHOLD", "42.5")

    result = get_tuning_float(
      "LBUG_ADMISSION_MEMORY_THRESHOLD", "lbug_admission/MEMORY_THRESHOLD", 85.0
    )
    assert result == pytest.approx(42.5)
