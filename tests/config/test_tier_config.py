import io

import pytest

from robosystems.config import tier_config as tier_config_module
from robosystems.config.tier_config import (
  TierConfig,
  get_tier_backup_limits,
  get_tier_copy_operation_limits,
  get_tier_max_subgraphs,
  get_tier_monthly_credits,
  get_tier_rate_limit_multiplier,
  get_tier_storage_limit,
)


GRAPH_CONFIG_YAML = """
production:
  writers:
    - tier: kuzu-standard
      max_subgraphs: 5
      storage_limit_gb: 250
      monthly_credits: 1500
      rate_limit_multiplier: 1.5
      copy_operations:
        max_file_size_gb: 3
        timeout_seconds: 600
        concurrent_operations: 2
        max_files_per_operation: 50
        daily_copy_operations: 20
      backup_limits:
        max_backup_size_gb: 20
        backup_retention_days: 14
        max_backups_per_day: 4
      instance:
        memory_per_db_mb: 512
        max_memory_mb: 4096
        chunk_size: 256
        query_timeout: 120
    - tier: kuzu-large
      monthly_credits: 5000
      instance:
        memory_per_db_mb: 2048
staging:
  writers:
    - tier: kuzu-standard
      monthly_credits: 900
      instance:
        query_timeout: 90
"""


@pytest.fixture(autouse=True)
def reset_tier_config_caches():
  TierConfig.clear_cache()
  get_tier_max_subgraphs.cache_clear()
  get_tier_storage_limit.cache_clear()
  get_tier_monthly_credits.cache_clear()
  get_tier_rate_limit_multiplier.cache_clear()
  get_tier_copy_operation_limits.cache_clear()
  get_tier_backup_limits.cache_clear()
  yield
  TierConfig.clear_cache()
  get_tier_max_subgraphs.cache_clear()
  get_tier_storage_limit.cache_clear()
  get_tier_monthly_credits.cache_clear()
  get_tier_rate_limit_multiplier.cache_clear()
  get_tier_copy_operation_limits.cache_clear()
  get_tier_backup_limits.cache_clear()


@pytest.fixture
def mock_graph_config(monkeypatch):
  dev_path = tier_config_module.os.path.join(
    tier_config_module.os.path.dirname(
      tier_config_module.os.path.dirname(
        tier_config_module.os.path.dirname(tier_config_module.__file__)
      )
    ),
    ".github",
    "configs",
    "graph.yml",
  )
  open_calls = []

  def fake_exists(path):
    if path == "/app/configs/graph.yml":
      return False
    return path == dev_path

  def fake_open(path, mode="r", *args, **kwargs):
    assert path == dev_path
    open_calls.append(path)
    return io.StringIO(GRAPH_CONFIG_YAML)

  monkeypatch.setattr(
    "robosystems.config.tier_config.os.path.exists", fake_exists, raising=False
  )
  monkeypatch.setattr("builtins.open", fake_open)
  monkeypatch.setattr("robosystems.config.tier_config.env.ENVIRONMENT", "prod")

  return {"open_calls": open_calls, "dev_path": dev_path}


def test_tier_config_loads_once_when_cached(mock_graph_config):
  config = TierConfig.get_tier_config("kuzu-standard")
  assert config["tier"] == "kuzu-standard"
  assert mock_graph_config["open_calls"] == [mock_graph_config["dev_path"]]

  # Cached result should not trigger additional loads
  TierConfig.get_tier_config("kuzu-standard")
  assert len(mock_graph_config["open_calls"]) == 1

  # Clearing cache should force reload
  TierConfig.clear_cache()
  TierConfig.get_tier_config("kuzu-standard")
  assert len(mock_graph_config["open_calls"]) == 2


def test_accessors_return_configured_values(mock_graph_config):
  assert get_tier_max_subgraphs("kuzu-standard") == 5
  assert get_tier_storage_limit("kuzu-standard") == 250
  assert get_tier_monthly_credits("kuzu-standard") == 1500
  assert get_tier_rate_limit_multiplier("kuzu-standard") == 1.5

  copy_limits = get_tier_copy_operation_limits("kuzu-standard")
  assert copy_limits["max_file_size_gb"] == 3
  assert copy_limits["timeout_seconds"] == 600
  assert copy_limits["daily_copy_operations"] == 20

  backup_limits = get_tier_backup_limits("kuzu-standard")
  assert backup_limits["max_backup_size_gb"] == 20
  assert backup_limits["max_backups_per_day"] == 4

  instance_config = TierConfig.get_instance_config("kuzu-standard")
  assert instance_config["memory_per_db_mb"] == 512
  assert instance_config["max_memory_mb"] == 4096
  assert TierConfig.get_query_timeout("kuzu-standard") == 120
  assert TierConfig.get_chunk_size("kuzu-standard") == 256


def test_accessors_fall_back_to_defaults_when_missing(mock_graph_config):
  # kuzu-large is missing storage limit/multiplier/copy settings so defaults apply
  assert TierConfig.get_tier_config("unknown-tier") == {}
  assert get_tier_storage_limit("kuzu-large") == 500
  assert get_tier_rate_limit_multiplier("kuzu-large") == 1.0

  default_copy = get_tier_copy_operation_limits("kuzu-large")
  assert default_copy["max_file_size_gb"] == 1.0
  assert default_copy["concurrent_operations"] == 1

  default_backup = get_tier_backup_limits("kuzu-large")
  assert default_backup["max_backup_size_gb"] == 10
  assert default_backup["max_backups_per_day"] == 2


def test_environment_default_switches_to_staging(monkeypatch, mock_graph_config):
  monkeypatch.setattr("robosystems.config.tier_config.env.ENVIRONMENT", "dev")
  TierConfig.clear_cache()

  staging_config = TierConfig.get_tier_config("kuzu-standard")
  assert staging_config["monthly_credits"] == 900
  assert TierConfig.get_query_timeout("kuzu-standard") == 90
