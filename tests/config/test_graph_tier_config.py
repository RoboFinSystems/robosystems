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
  # No longer read from graph.yml: the multiplier is derived from the limits
  # SUBSCRIPTION_RATE_LIMITS actually enforces, so it cannot drift from them.
  # The mocked YAML's api_rate_multiplier is therefore ignored by design.
  assert get_tier_api_rate_multiplier("ladybug-standard") == 1.0

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
  assert get_tier_api_rate_multiplier("ladybug-large") == 2.0  # derived, 2 vCPU

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

  # The chunked-materialization engine carries its own fallback constant; it
  # drifted to 4x Standard's chunk once (kept m7g.large's 1M after the resize).
  from robosystems.operations.graph.engine.chunked_materialization import (
    DEFAULT_CHUNK_SIZE_ROWS,
  )

  assert standard["chunk_size_rows"] >= DEFAULT_CHUNK_SIZE_ROWS, (
    f"DEFAULT_CHUNK_SIZE_ROWS={DEFAULT_CHUNK_SIZE_ROWS:,} exceeds "
    f"ladybug-standard's {standard['chunk_size_rows']:,}"
  )


def test_get_instance_storage_limit_gb(mock_graph_config):
  assert GraphTierConfig.get_instance_storage_limit_gb("ladybug-standard") == 20.0
  assert GraphTierConfig.get_instance_storage_limit_gb("ladybug-large") == 50.0


def test_tiers_defined_in_an_environment_match_production():
  """Where a tier IS defined, its capability and product limits match prod.

  Not every environment defines every tier — development deliberately defines
  only ladybug-standard, because it runs a single graph_api and Large/XLarge
  cannot exist there. That absence is fine and is handled by omitting the tier
  from /v1/offering. What is not fine is a tier that exists with *different*
  numbers, since subgraph count and storage cap are properties of the tier
  rather than of the deployment.

  api_rate_multiplier needs no assertion here: it is derived from
  SUBSCRIPTION_RATE_LIMITS, which does not vary by environment.
  """
  GraphTierConfig.clear_cache()
  tiers = ("ladybug-standard", "ladybug-large", "ladybug-xlarge")

  for env_name in ("staging", "development"):
    for tier in tiers:
      config = GraphTierConfig.get_tier_config(tier, env_name)
      if not config:
        continue  # Not offered in this environment — see docstring.

      prod = GraphTierConfig.get_tier_config(tier, "production")
      assert config.get("max_subgraphs") == prod.get("max_subgraphs"), (
        f"{env_name}/{tier} max_subgraphs={config.get('max_subgraphs')} does not "
        f"match production's {prod.get('max_subgraphs')}"
      )
      assert GraphTierConfig.get_instance_storage_limit_gb(tier, env_name) == (
        GraphTierConfig.get_instance_storage_limit_gb(tier, "production")
      ), f"{env_name}/{tier} instance_storage_limit_gb does not match production"


def test_available_tiers_report_the_derived_rate_multiplier():
  """The multiplier surfaced by /v1/offering and /v1/graphs/tiers is derived.

  graph.yml no longer carries an api_rate_multiplier key, so any surface that
  reads it off the writer dict with .get() silently flattens every tier to the
  1.0 default — telling Large and XLarge customers they get standard limits
  while enforcement gives them 2x/4x. Pin the reported value to the derived
  one for every tier the catalog can list.
  """
  GraphTierConfig.clear_cache()
  expected = {
    "ladybug-standard": 1.0,
    "ladybug-large": 2.0,
    "ladybug-xlarge": 4.0,
  }

  for env_name in ("production", "staging"):
    tiers = GraphTierConfig.get_available_tiers(env_name, include_disabled=True)
    by_name = {t["tier"]: t for t in tiers if t["tier"] in expected}
    assert set(by_name) == set(expected), (
      f"{env_name} does not define all customer tiers: {sorted(by_name)}"
    )
    for tier_name, multiplier in expected.items():
      assert by_name[tier_name]["api_rate_multiplier"] == multiplier, (
        f"{env_name}/{tier_name} reports "
        f"{by_name[tier_name]['api_rate_multiplier']}, expected {multiplier}"
      )
    assert "2x API rate limits" in by_name["ladybug-large"]["features"]
    assert "4x API rate limits" in by_name["ladybug-xlarge"]["features"]


def test_subgraph_memory_is_configured_separately_from_parent(mock_graph_config):
  """Subgraphs get a smaller buffer pool than the parent database.

  get_database_memory_config() reads memory_per_subgraph_mb for names
  containing an underscore, falling back to memory_per_db_mb otherwise.
  """
  instance = GraphTierConfig.get_tier_config("ladybug-standard")["instance"]
  assert instance["memory_per_db_mb"] == 512
  assert instance["memory_per_subgraph_mb"] == 256


def test_backup_hosting_days_never_exceed_the_s3_lifecycle():
  """The S3 lifecycle rule deletes backup objects at 90 days, and the final
  deprovisioning backup creates no GraphBackup row, so no tier can promise
  hosting beyond 90 days. This table once promised 180/365 to Large/XLarge —
  retention the infrastructure silently could not deliver."""
  from robosystems.config.deprovisioning import get_deprovisioning_config

  config = get_deprovisioning_config()
  for tier, days in config.backup_hosting_days.items():
    assert days <= 90, f"{tier} promises {days}-day backup hosting; S3 deletes at 90"
  assert config.get_backup_hosting_days("unknown-tier") <= 90
