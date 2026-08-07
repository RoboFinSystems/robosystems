"""Graph tier definitions and accessors for per-tier configuration.

``.github/configs/graph.yml`` is the authoritative source for every tier
property read here — instance type and RAM, memory budgets, subgraph caps,
copy/backup/graph limits. This module only reads and defaults it; change a
tier by editing that file, not these accessors.
"""

import os
import warnings
from enum import Enum
from functools import lru_cache
from typing import Any

import yaml

from robosystems.config import env


class GraphTier(str, Enum):
  """Graph database tier definitions.

  IMPORTANT: These values must stay in sync with .github/configs/graph.yml.
  Update both when adding or removing tiers.
  """

  LADYBUG_STANDARD = "ladybug-standard"
  LADYBUG_LARGE = "ladybug-large"
  LADYBUG_XLARGE = "ladybug-xlarge"
  LADYBUG_SHARED = "ladybug-shared"


class GraphTierConfig:
  """Reads per-tier properties out of graph.yml.

  Every getter resolves the tier's block for the current environment and falls
  back to a hardcoded default when the key or the tier is missing.
  """

  _config_cache: dict[str, Any] | None = None

  @classmethod
  def _load_config(cls) -> dict[str, Any]:
    """Load the graph.yml configuration file."""
    if cls._config_cache is not None:
      return cls._config_cache

    # Determine config path - try container location first, then development location
    container_path = "/app/configs/graph.yml"
    dev_path = os.path.join(
      os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
      ".github",
      "configs",
      "graph.yml",
    )

    config_path = container_path if os.path.exists(container_path) else dev_path

    if not os.path.exists(config_path):
      warnings.warn(f"Graph config file not found at {config_path}", stacklevel=2)
      cls._config_cache = {}
      return cls._config_cache

    try:
      with open(config_path) as f:
        config = yaml.safe_load(f)
      cls._config_cache = config or {}
      # Use print for debug since logger isn't available due to circular import
      if os.getenv("DEBUG", "").lower() == "true":
        print(f"DEBUG: Loaded graph config from {config_path}")
      assert cls._config_cache is not None
      return cls._config_cache
    except Exception as e:
      warnings.warn(f"Failed to load graph config: {e}", stacklevel=2)
      cls._config_cache = {}
      return cls._config_cache

  @classmethod
  def get_tier_config(cls, tier: str, environment: str | None = None) -> dict[str, Any]:
    """Get configuration for a specific tier.

    For replicas (LBUG_NODE_TYPE=shared_replica), this merges the writer's
    tier config with any instance overrides from the matching replica config.
    This allows replicas to inherit the writer's settings but override
    instance-specific values like memory limits for their smaller hardware.

    ``environment`` defaults to the one derived from ``env.ENVIRONMENT``.
    """
    if environment is None:
      if env.ENVIRONMENT == "prod":
        environment = "production"
      elif env.ENVIRONMENT == "dev":
        environment = "development"
      else:
        environment = "staging"

    config = cls._load_config()

    env_config = config.get(environment, {})
    writers = env_config.get("writers", [])

    writer_config = {}
    for writer in writers:
      if writer.get("tier") == tier:
        writer_config = writer
        break

    if not writer_config:
      return {}

    # For replicas, merge instance overrides from the replica config
    if env.LBUG_NODE_TYPE == "shared_replica":
      replicas = env_config.get("replicas", [])
      for replica in replicas:
        if replica.get("depends_on") == tier:
          replica_instance = replica.get("instance", {})
          if replica_instance:
            merged = dict(writer_config)
            merged["instance"] = {
              **writer_config.get("instance", {}),
              **replica_instance,
            }
            return merged
          break

    return writer_config

  @classmethod
  def get_max_subgraphs(cls, tier: str, environment: str | None = None) -> int | None:
    """Get maximum subgraphs allowed for a tier; None means unlimited."""
    tier_config = cls.get_tier_config(tier, environment)
    return tier_config.get("max_subgraphs")

  @classmethod
  def get_query_timeout(cls, tier: str, environment: str | None = None) -> int:
    """Get query timeout for a tier, in seconds."""
    tier_config = cls.get_tier_config(tier, environment)
    instance_config = tier_config.get("instance", {})
    return instance_config.get("query_timeout", 30)

  @classmethod
  def get_memory_per_db_mb(cls, tier: str, environment: str | None = None) -> int:
    """Get per-database memory allocation for a tier, in MB."""
    tier_config = cls.get_tier_config(tier, environment)
    instance_config = tier_config.get("instance", {})
    return instance_config.get("memory_per_db_mb", 2048)

  @classmethod
  def get_databases_per_instance(cls, tier: str, environment: str | None = None) -> int:
    """Get how many graph databases share one instance for a tier.

    1 means the tier is dedicated (single-tenant per box); >1 means packed.
    This is the packing property of the *product tier*, distinct from the
    dev-only ``LBUG_DATABASES_PER_INSTANCE`` allocation override.
    """
    tier_config = cls.get_tier_config(tier, environment)
    instance_config = tier_config.get("instance", {})
    return instance_config.get("databases_per_instance", 1)

  @classmethod
  def get_max_memory_mb(cls, tier: str, environment: str | None = None) -> int:
    """Get the LadybugDB memory budget for a tier, in MB.

    This is the engine's budget after OS overhead, not the instance's physical
    RAM (``instance_ram_gb``).
    """
    tier_config = cls.get_tier_config(tier, environment)
    instance_config = tier_config.get("instance", {})
    return instance_config.get("max_memory_mb", 2048)

  @classmethod
  def get_duckdb_memory_limit(cls, tier: str, environment: str | None = None) -> str:
    """Get DuckDB memory limit for a tier, as a size string like "8GB"."""
    tier_config = cls.get_tier_config(tier, environment)
    instance_config = tier_config.get("instance", {})
    return instance_config.get("duckdb_memory_limit", "2GB")

  @classmethod
  def get_duckdb_max_threads(cls, tier: str, environment: str | None = None) -> int:
    """Get DuckDB max threads for a tier.

    Thread counts are aligned with instance vCPU counts to prevent oversubscription:
    - m7g.medium (1 vCPU): 2 threads (DuckDB benefits from slight oversubscription)
    - m7g.large (2 vCPU): 2 threads
    - r7g.xlarge (4 vCPU): 4 threads
    """
    tier_config = cls.get_tier_config(tier, environment)
    instance_config = tier_config.get("instance", {})
    return instance_config.get("duckdb_max_threads", 4)

  @classmethod
  def get_duckdb_memory_boost(
    cls, tier: str, environment: str | None = None
  ) -> str | None:
    """Get DuckDB memory boost limit for staging operations.

    During DuckDB staging (creating external tables from parquet), memory needs
    are high. Returns the boosted limit to apply temporarily (a size string
    like "55GB"), or None when the tier does not configure one.
    """
    tier_config = cls.get_tier_config(tier, environment)
    instance_config = tier_config.get("instance", {})
    return instance_config.get("duckdb_memory_boost")

  @classmethod
  def get_ladybug_memory_boost_mb(
    cls, tier: str, environment: str | None = None
  ) -> int | None:
    """Get LadybugDB memory boost limit for materialization operations.

    During LadybugDB materialization (COPY FROM DuckDB), memory needs are high
    for the buffer pool. Returns the boosted limit in MB, or None when the tier
    does not configure one.
    """
    tier_config = cls.get_tier_config(tier, environment)
    instance_config = tier_config.get("instance", {})
    return instance_config.get("ladybug_memory_boost_mb")

  @classmethod
  def get_chunk_size(cls, tier: str, environment: str | None = None) -> int:
    """Get the per-operation chunk size for a tier, in rows."""
    tier_config = cls.get_tier_config(tier, environment)
    instance_config = tier_config.get("instance", {})
    return instance_config.get("chunk_size", 1000)

  @classmethod
  def get_instance_config(
    cls, tier: str, environment: str | None = None
  ) -> dict[str, Any]:
    """Get the whole ``instance`` block for a tier."""
    tier_config = cls.get_tier_config(tier, environment)
    return tier_config.get("instance", {})

  @classmethod
  def get_api_rate_multiplier(cls, tier: str, environment: str | None = None) -> float:
    """Get this tier's rate limit relative to ladybug-standard.

    Computed from SUBSCRIPTION_RATE_LIMITS — the table the limiter actually
    enforces — rather than a standalone config knob, so what /limits and
    /offering advertise cannot drift from what a caller receives. 1.0 means the
    same throughput as standard. ``environment`` is unused, retained for
    call-site compatibility.
    """
    from .rate_limits import EndpointCategory, RateLimitConfig

    # A tier without its own limits table (ladybug-shared, legacy strings) is
    # enforced at the standard-tier fallback, so its multiplier is 1.0 — not
    # the base-table ratio get_rate_limit's fallback would imply.
    if tier not in RateLimitConfig.SUBSCRIPTION_RATE_LIMITS:
      return 1.0

    baseline = RateLimitConfig.get_rate_limit(
      "ladybug-standard", EndpointCategory.GRAPH_QUERY
    )
    actual = RateLimitConfig.get_rate_limit(tier, EndpointCategory.GRAPH_QUERY)
    if not baseline or not actual or not baseline[0]:
      return 1.0
    return actual[0] / baseline[0]

  @classmethod
  def get_copy_operation_limits(
    cls, tier: str, environment: str | None = None
  ) -> dict[str, Any]:
    """Get copy operation limits for a tier."""
    tier_config = cls.get_tier_config(tier, environment)
    default_limits = {
      "max_file_size_gb": 1.0,
      "timeout_seconds": 300,
      "concurrent_operations": 1,
      "max_files_per_operation": 100,
      "daily_copy_operations": 10,
    }
    return tier_config.get("copy_operations", default_limits)

  @classmethod
  def get_backup_limits(
    cls, tier: str, environment: str | None = None
  ) -> dict[str, Any]:
    """Get backup limits for a tier."""
    tier_config = cls.get_tier_config(tier, environment)
    default_limits = {
      "max_backup_size_gb": 10,
      "backup_retention_days": 7,
      "max_backups_per_day": 2,
    }
    return tier_config.get("backup_limits", default_limits)

  @classmethod
  def get_graph_limits(
    cls, tier: str, environment: str | None = None
  ) -> dict[str, Any]:
    """Get graph content limits for a tier.

    Carries ``instance_storage_limit_gb`` plus the per-copy and per-table row
    caps.
    """
    tier_config = cls.get_tier_config(tier, environment)
    # These fire only when a tier resolves without a graph_limits block (unknown
    # tier, malformed config). They MUST track the smallest tier — the row caps
    # are OOM guardrails sized to instance RAM, so falling back to a larger
    # tier's values would apply an 8GB-sized copy limit to a 4GB box and cause
    # the exact OOM the guardrail exists to prevent. Keep in sync with
    # ladybug-standard in .github/configs/graph.yml on every tier resize.
    defaults: dict[str, Any] = {
      "instance_storage_limit_gb": 20,
      "max_rows_per_copy": 1_000_000,
      "max_single_table_rows": 2_500_000,
      "chunk_size_rows": 250_000,
      "warn_at_percentage": 80,
    }
    return tier_config.get("graph_limits", defaults)

  @classmethod
  def get_instance_storage_limit_gb(
    cls, tier: str, environment: str | None = None
  ) -> float:
    """Get the soft instance storage limit in GB for a tier.

    This is the total storage budget for the entire dedicated instance,
    covering the parent graph, all subgraphs, DuckDB staging, and
    future LanceDB vector indexes. It is enforced, not advisory:
    materialization and file upload both reject over it (IngestionLimitChecker,
    ingest_file).
    """
    graph_limits = cls.get_graph_limits(tier, environment)
    return float(graph_limits.get("instance_storage_limit_gb", 20))

  @classmethod
  def _generate_tier_features(cls, tier_config: dict[str, Any]) -> list[str]:
    """Build the human-readable feature bullets advertised for a tier."""
    features = []

    # Add storage limit
    graph_limits = tier_config.get("graph_limits", {})
    storage_limit = graph_limits.get("instance_storage_limit_gb")
    if storage_limit is not None and storage_limit > 0:
      features.append(f"{int(storage_limit)} GB instance storage")

    # Credit allocations live in billing config, not graph.yml.
    feature_tier = tier_config.get("tier") or tier_config.get("name")
    if feature_tier:
      from .billing import BillingConfig

      monthly_credits = BillingConfig.get_monthly_credits(feature_tier)
      if monthly_credits > 0:
        features.append(f"{monthly_credits:,} AI credits per month")

    # Add subgraph support
    max_subgraphs = tier_config.get("max_subgraphs")
    if max_subgraphs is None:
      features.append("No subgraph support")
    elif max_subgraphs == 0:
      features.append("Single database only")
    elif max_subgraphs >= 25:
      features.append("Unlimited subgraphs")
    elif max_subgraphs > 0:
      features.append(f"Up to {max_subgraphs} subgraphs")

    # Add instance type and memory info
    instance = tier_config.get("instance", {})
    databases_per_instance = instance.get("databases_per_instance", 1)
    is_multitenant = databases_per_instance > 1

    if is_multitenant:
      # Multi-tenant: Show shared infrastructure and per-database memory
      features.append("Shared infrastructure")
      memory_per_db_mb = instance.get("memory_per_db_mb", 0)
      if memory_per_db_mb and memory_per_db_mb > 0:
        if memory_per_db_mb >= 1024:
          features.append(f"{memory_per_db_mb / 1024:.1f}GB RAM per graph")
        else:
          features.append(f"{memory_per_db_mb}MB RAM per graph")
    else:
      # Dedicated: Show instance type and total memory
      instance_type = instance.get("type", "").upper()
      if "XLARGE" in instance_type:
        features.append("Dedicated extra-large instance")
      elif "LARGE" in instance_type:
        features.append("Dedicated large instance")
      elif "MEDIUM" in instance_type:
        features.append("Dedicated medium instance")

      # Advertise the instance's physical RAM, matching /v1/offering's
      # infrastructure line. Deliberately not max_memory_mb, which is the
      # LadybugDB budget after OS overhead and would understate the box.
      instance_ram_gb = instance.get("instance_ram_gb", 0)
      if instance_ram_gb and instance_ram_gb > 0:
        features.append(f"{instance_ram_gb:g} GB RAM")

    # Add rate limit multiplier if not standard.
    tier_name = tier_config.get("tier") or tier_config.get("name")
    rate_multiplier = cls.get_api_rate_multiplier(tier_name) if tier_name else 1.0
    if rate_multiplier > 1:
      features.append(f"{rate_multiplier:g}x API rate limits")

    # Add backup retention
    backup_limits = tier_config.get("backup_limits", {})
    retention_days = backup_limits.get("backup_retention_days")
    if retention_days is not None and retention_days > 0:
      features.append(f"{retention_days}-day backup retention")

    return features

  @classmethod
  def get_available_tiers(
    cls, environment: str | None = None, include_disabled: bool = False
  ) -> list[dict[str, Any]]:
    """List the tiers deployed in an environment, with display metadata."""
    if environment is None:
      if env.ENVIRONMENT == "prod":
        environment = "production"
      elif env.ENVIRONMENT == "dev":
        environment = "development"
      else:
        environment = "staging"

    config = cls._load_config()
    env_config = config.get(environment, {})
    writers = env_config.get("writers", [])

    from .billing import BillingConfig

    available_tiers = []
    for writer in writers:
      # Check if tier is enabled
      deployment = writer.get("deployment", {})
      is_enabled = deployment.get("always_enabled", False) or deployment.get(
        "enabled_default", False
      )

      # Skip disabled tiers unless requested
      if not is_enabled and not include_disabled:
        continue

      # Skip optional tiers that are disabled
      if deployment.get("optional", False) and not deployment.get(
        "enabled_default", False
      ):
        if not include_disabled:
          continue

      instance_config = writer.get("instance", {})
      databases_per_instance = instance_config.get("databases_per_instance", 1)
      is_multitenant = databases_per_instance > 1

      graph_memory_mb = (
        instance_config.get("memory_per_db_mb")
        if is_multitenant and instance_config.get("memory_per_db_mb")
        else instance_config.get("max_memory_mb")
      )

      tier_name = writer.get("tier")
      billing_plan = BillingConfig.get_subscription_plan(tier_name)

      monthly_credits = (
        billing_plan.get("monthly_credit_allocation") if billing_plan else 0
      )

      tier_info = {
        "tier": tier_name,
        "name": writer.get("name"),
        "description": writer.get("description"),
        "backend": writer.get("backend"),
        "enabled": is_enabled,
        "max_subgraphs": writer.get("max_subgraphs"),
        "monthly_credits": monthly_credits,
        "api_rate_multiplier": cls.get_api_rate_multiplier(tier_name),
        "features": cls._generate_tier_features(writer),
        "instance": {
          "type": instance_config.get("type"),
          "memory_mb": graph_memory_mb,
          "is_multitenant": is_multitenant,
        },
        "limits": {
          "monthly_credits": monthly_credits,
          "max_subgraphs": writer.get("max_subgraphs"),
          "copy_operations": writer.get("copy_operations", {}),
          "backup": writer.get("backup_limits", {}),
          "graph_limits": writer.get("graph_limits", {}),
        },
      }

      # Add display name based on tier
      display_names = {
        "ladybug-standard": "Standard",
        "ladybug-large": "Large",
        "ladybug-xlarge": "XLarge",
        "ladybug-shared": "Shared Repository",
      }
      tier_info["display_name"] = display_names.get(tier_name, writer.get("name"))

      # Add pricing placeholder (to be filled from billing config if needed)
      tier_info["monthly_price"] = None  # This should come from billing config

      available_tiers.append(tier_info)

    return available_tiers

  @classmethod
  def clear_cache(cls) -> None:
    """Clear the configuration cache (useful for testing)."""
    cls._config_cache = None


def get_tier_max_subgraphs(tier: str, environment: str | None = None) -> int | None:
  """Get max subgraphs for a tier; None means unlimited."""
  return GraphTierConfig.get_max_subgraphs(tier, environment)


@lru_cache(maxsize=32)
def get_tier_api_rate_multiplier(tier: str, environment: str | None = None) -> float:
  """Cached GraphTierConfig.get_api_rate_multiplier."""
  return GraphTierConfig.get_api_rate_multiplier(tier, environment)


@lru_cache(maxsize=32)
def get_tier_copy_operation_limits(
  tier: str, environment: str | None = None
) -> dict[str, Any]:
  """Cached GraphTierConfig.get_copy_operation_limits."""
  return GraphTierConfig.get_copy_operation_limits(tier, environment)


@lru_cache(maxsize=32)
def get_tier_backup_limits(tier: str, environment: str | None = None) -> dict[str, Any]:
  """Cached GraphTierConfig.get_backup_limits."""
  return GraphTierConfig.get_backup_limits(tier, environment)
