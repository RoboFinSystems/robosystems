import io

import pytest

from robosystems.config import graph_tier as tier_config_module
from robosystems.config.graph_tier import (
  GraphTierConfig,
  get_tier_api_rate_multiplier,
  get_tier_backup_limits,
  get_tier_copy_operation_limits,
  get_tier_max_subgraphs,
)

GRAPH_CONFIG_YAML = """
production:
  writers:
    - tier: ladybug-standard
      max_subgraphs: 5
      storage_limit_gb: 250
      monthly_credits: 1500
      api_rate_multiplier: 1.5
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
      graph_limits:
        instance_storage_limit_gb: 20
        max_rows_per_copy: 2000000
        max_single_table_rows: 5000000
        chunk_size_rows: 1000000
        warn_at_percentage: 80
      instance:
        memory_per_db_mb: 512
        memory_per_subgraph_mb: 256
        max_memory_mb: 4096
        chunk_size: 256
        query_timeout: 120
    - tier: ladybug-large
      monthly_credits: 5000
      graph_limits:
        instance_storage_limit_gb: 50
        max_rows_per_copy: 10000000
        max_single_table_rows: 25000000
        chunk_size_rows: 2000000
        warn_at_percentage: 80
      instance:
        memory_per_db_mb: 2048
staging:
  writers:
    - tier: ladybug-standard
      monthly_credits: 900
      instance:
        query_timeout: 90
development:
  writers:
    - tier: ladybug-standard
      monthly_credits: 900
      instance:
        query_timeout: 90
"""


@pytest.fixture(autouse=True)
def reset_tier_config_caches():
  GraphTierConfig.clear_cache()
  get_tier_api_rate_multiplier.cache_clear()
  get_tier_copy_operation_limits.cache_clear()
  get_tier_backup_limits.cache_clear()
  yield
  GraphTierConfig.clear_cache()
  get_tier_api_rate_multiplier.cache_clear()
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
    "robosystems.config.graph_tier.os.path.exists", fake_exists, raising=False
  )
  monkeypatch.setattr("builtins.open", fake_open)
  monkeypatch.setattr("robosystems.config.graph_tier.env.ENVIRONMENT", "prod")

  return {"open_calls": open_calls, "dev_path": dev_path}


def test_tier_config_loads_once_when_cached(mock_graph_config):
  config = GraphTierConfig.get_tier_config("ladybug-standard")
  assert config["tier"] == "ladybug-standard"
  assert mock_graph_config["open_calls"] == [mock_graph_config["dev_path"]]

  # Cached result should not trigger additional loads
  GraphTierConfig.get_tier_config("ladybug-standard")
  assert len(mock_graph_config["open_calls"]) == 1

  # Clearing cache should force reload
  GraphTierConfig.clear_cache()
  GraphTierConfig.get_tier_config("ladybug-standard")
  assert len(mock_graph_config["open_calls"]) == 2


def test_accessors_return_configured_values(mock_graph_config):
  assert get_tier_max_subgraphs("ladybug-standard") == 5
  assert get_tier_api_rate_multiplier("ladybug-standard") == 1.5

  copy_limits = get_tier_copy_operation_limits("ladybug-standard")
  assert copy_limits["max_file_size_gb"] == 3
  assert copy_limits["timeout_seconds"] == 600
  assert copy_limits["daily_copy_operations"] == 20

  backup_limits = get_tier_backup_limits("ladybug-standard")
  assert backup_limits["max_backup_size_gb"] == 20
  assert backup_limits["max_backups_per_day"] == 4

  instance_config = GraphTierConfig.get_instance_config("ladybug-standard")
  assert instance_config["memory_per_db_mb"] == 512
  assert instance_config["max_memory_mb"] == 4096
  assert GraphTierConfig.get_query_timeout("ladybug-standard") == 120
  assert GraphTierConfig.get_chunk_size("ladybug-standard") == 256


def test_accessors_fall_back_to_defaults_when_missing(mock_graph_config):
  # ladybug-large is missing multiplier/copy settings so defaults apply
  assert GraphTierConfig.get_tier_config("unknown-tier") == {}
  assert get_tier_api_rate_multiplier("ladybug-large") == 1.0

  default_copy = get_tier_copy_operation_limits("ladybug-large")
  assert default_copy["max_file_size_gb"] == 1.0
  assert default_copy["concurrent_operations"] == 1

  default_backup = get_tier_backup_limits("ladybug-large")
  assert default_backup["max_backup_size_gb"] == 10
  assert default_backup["max_backups_per_day"] == 2


def test_environment_default_switches_to_staging(monkeypatch, mock_graph_config):
  monkeypatch.setattr("robosystems.config.graph_tier.env.ENVIRONMENT", "dev")
  GraphTierConfig.clear_cache()

  staging_config = GraphTierConfig.get_tier_config("ladybug-standard")
  assert staging_config["monthly_credits"] == 900
  assert GraphTierConfig.get_query_timeout("ladybug-standard") == 90


def test_get_graph_limits_returns_configured_values(mock_graph_config):
  limits = GraphTierConfig.get_graph_limits("ladybug-standard")
  assert limits["instance_storage_limit_gb"] == 20
  assert limits["max_rows_per_copy"] == 2_000_000
  assert limits["max_single_table_rows"] == 5_000_000
  assert limits["chunk_size_rows"] == 1_000_000
  assert limits["warn_at_percentage"] == 80


def test_get_graph_limits_returns_large_tier_values(mock_graph_config):
  limits = GraphTierConfig.get_graph_limits("ladybug-large")
  assert limits["instance_storage_limit_gb"] == 50
  assert limits["max_rows_per_copy"] == 10_000_000
  assert limits["chunk_size_rows"] == 2_000_000


def test_get_graph_limits_falls_back_to_defaults(mock_graph_config):
  limits = GraphTierConfig.get_graph_limits("unknown-tier")
  assert limits["instance_storage_limit_gb"] == 20
  assert limits["max_rows_per_copy"] == 1_000_000
  assert limits["chunk_size_rows"] == 250_000


def test_graph_limit_defaults_do_not_exceed_the_smallest_tier():
  """Fallback row caps must never be larger than the smallest tier's.

  The row caps are OOM guardrails sized to instance RAM. A fallback larger
  than the actual box would apply a bigger tier's copy limit to a smaller
  instance — the exact OOM the guardrail exists to prevent. This drifted
  once already: the defaults kept m7g.large's 2M/5M after ladybug-standard
  moved to m7g.medium, so they are pinned against real config here rather
  than against literals in the mocked fixture.
  """
  GraphTierConfig.clear_cache()
  standard = GraphTierConfig.get_graph_limits("ladybug-standard", "production")
  defaults = GraphTierConfig.get_graph_limits("does-not-exist", "production")

  for key in ("max_rows_per_copy", "max_single_table_rows", "chunk_size_rows"):
    assert defaults[key] <= standard[key], (
      f"default {key}={defaults[key]:,} exceeds ladybug-standard's "
      f"{standard[key]:,} — a fallback must never be larger than the smallest tier"
    )


def test_get_instance_storage_limit_gb(mock_graph_config):
  assert GraphTierConfig.get_instance_storage_limit_gb("ladybug-standard") == 20.0
  assert GraphTierConfig.get_instance_storage_limit_gb("ladybug-large") == 50.0


def test_subgraph_memory_is_configured_separately_from_parent(mock_graph_config):
  """Subgraphs get a smaller buffer pool than the parent database.

  get_database_memory_config() reads memory_per_subgraph_mb for names
  containing an underscore, falling back to memory_per_db_mb otherwise.
  """
  instance = GraphTierConfig.get_tier_config("ladybug-standard")["instance"]
  assert instance["memory_per_db_mb"] == 512
  assert instance["memory_per_subgraph_mb"] == 256


def test_get_storage_cap_gb_from_backup_limits(mock_graph_config):
  cap = GraphTierConfig.get_storage_cap_gb("ladybug-standard")
  assert cap == 20  # From backup_limits.max_backup_size_gb


def test_get_storage_cap_gb_falls_back_to_default(mock_graph_config):
  cap = GraphTierConfig.get_storage_cap_gb("unknown-tier")
  assert cap == 10  # Default
