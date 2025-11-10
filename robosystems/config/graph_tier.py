"""Graph tier configuration and utilities.

This module defines:
- GraphTier: Enum of all available graph database tiers
- GraphTierConfig: Utility class for accessing tier-specific configuration from graph.yml
"""

import os
import yaml
import warnings
from typing import Dict, Any, Optional, List
from functools import lru_cache
from enum import Enum

from robosystems.config import env


class GraphTier(str, Enum):
  """Graph database tier definitions.

  IMPORTANT: These values must stay in sync with .github/configs/graph.yml.
  Update both when adding or removing tiers.
  """

  KUZU_STANDARD = "kuzu-standard"
  KUZU_LARGE = "kuzu-large"
  KUZU_XLARGE = "kuzu-xlarge"
  KUZU_SHARED = "kuzu-shared"
  NEO4J_COMMUNITY_LARGE = "neo4j-community-large"
  NEO4J_ENTERPRISE_XLARGE = "neo4j-enterprise-xlarge"


class GraphTierConfig:
  """Utility class for accessing graph tier-specific configuration from graph.yml.

  Provides methods to retrieve tier properties like storage limits, monthly credits,
  instance configuration, backup limits, and more from the centralized graph.yml file.
  """

  _config_cache: Optional[Dict[str, Any]] = None

  @classmethod
  def _load_config(cls) -> Dict[str, Any]:
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
      warnings.warn(f"Graph config file not found at {config_path}")
      cls._config_cache = {}
      return cls._config_cache

    try:
      with open(config_path, "r") as f:
        config = yaml.safe_load(f)
      cls._config_cache = config or {}
      # Use print for debug since logger isn't available due to circular import
      if os.getenv("DEBUG", "").lower() == "true":
        print(f"DEBUG: Loaded graph config from {config_path}")
      assert cls._config_cache is not None
      return cls._config_cache
    except Exception as e:
      warnings.warn(f"Failed to load graph config: {e}")
      cls._config_cache = {}
      return cls._config_cache

  @classmethod
  def get_tier_config(
    cls, tier: str, environment: Optional[str] = None
  ) -> Dict[str, Any]:
    """Get configuration for a specific tier.

    Args:
        tier: The tier name (kuzu-standard, kuzu-large, kuzu-xlarge, kuzu-shared, neo4j-community-large, neo4j-enterprise-xlarge)
        environment: Environment (defaults to current env)

    Returns:
        Tier configuration dictionary
    """
    if environment is None:
      environment = "production" if env.ENVIRONMENT == "prod" else "staging"

    config = cls._load_config()

    env_config = config.get(environment, {})
    writers = env_config.get("writers", [])

    for writer in writers:
      if writer.get("tier") == tier:
        return writer

    return {}

  @classmethod
  def get_max_subgraphs(
    cls, tier: str, environment: Optional[str] = None
  ) -> Optional[int]:
    """Get maximum subgraphs allowed for a tier.

    Args:
        tier: The tier name (kuzu-standard, kuzu-large, kuzu-xlarge)
        environment: Environment (defaults to current env)

    Returns:
        Maximum subgraphs allowed, or None for unlimited
    """
    tier_config = cls.get_tier_config(tier, environment)
    return tier_config.get("max_subgraphs")

  @classmethod
  def get_query_timeout(cls, tier: str, environment: Optional[str] = None) -> int:
    """Get query timeout for a tier.

    Args:
        tier: The tier name (kuzu-standard, kuzu-large, kuzu-xlarge)
        environment: Environment (defaults to current env)

    Returns:
        Query timeout in seconds
    """
    tier_config = cls.get_tier_config(tier, environment)
    instance_config = tier_config.get("instance", {})
    return instance_config.get("query_timeout", 30)

  @classmethod
  def get_memory_per_db_mb(cls, tier: str, environment: Optional[str] = None) -> int:
    """Get memory allocation per database for a tier.

    Args:
        tier: The tier name (kuzu-standard, kuzu-large, kuzu-xlarge)
        environment: Environment (defaults to current env)

    Returns:
        Memory per database in MB
    """
    tier_config = cls.get_tier_config(tier, environment)
    instance_config = tier_config.get("instance", {})
    return instance_config.get("memory_per_db_mb", 2048)

  @classmethod
  def get_max_memory_mb(cls, tier: str, environment: Optional[str] = None) -> int:
    """Get total memory allocation for a tier.

    Args:
        tier: The tier name (kuzu-standard, kuzu-large, kuzu-xlarge, kuzu-shared)
        environment: Environment (defaults to current env)

    Returns:
        Total memory in MB
    """
    tier_config = cls.get_tier_config(tier, environment)
    instance_config = tier_config.get("instance", {})
    return instance_config.get("max_memory_mb", 2048)

  @classmethod
  def get_chunk_size(cls, tier: str, environment: Optional[str] = None) -> int:
    """Get chunk size for a tier.

    Args:
        tier: The tier name (kuzu-standard, kuzu-large, kuzu-xlarge, kuzu-shared)
        environment: Environment (defaults to current env)

    Returns:
        Chunk size for operations
    """
    tier_config = cls.get_tier_config(tier, environment)
    instance_config = tier_config.get("instance", {})
    return instance_config.get("chunk_size", 1000)

  @classmethod
  def get_instance_config(
    cls, tier: str, environment: Optional[str] = None
  ) -> Dict[str, Any]:
    """Get complete instance configuration for a tier.

    Args:
        tier: The tier name (kuzu-standard, kuzu-large, kuzu-xlarge, kuzu-shared)
        environment: Environment (defaults to current env)

    Returns:
        Complete instance configuration dictionary
    """
    tier_config = cls.get_tier_config(tier, environment)
    return tier_config.get("instance", {})

  @classmethod
  def get_api_rate_multiplier(
    cls, tier: str, environment: Optional[str] = None
  ) -> float:
    """Get rate limit multiplier for a tier.

    Args:
        tier: The tier name (kuzu-standard, kuzu-large, kuzu-xlarge)
        environment: Environment (defaults to current env)

    Returns:
        Rate limit multiplier (1.0 = base limits)
    """
    tier_config = cls.get_tier_config(tier, environment)
    return tier_config.get("api_rate_multiplier", 1.0)

  @classmethod
  def get_copy_operation_limits(
    cls, tier: str, environment: Optional[str] = None
  ) -> Dict[str, Any]:
    """Get copy operation limits for a tier.

    Args:
        tier: The tier name (kuzu-standard, kuzu-large, kuzu-xlarge)
        environment: Environment (defaults to current env)

    Returns:
        Copy operation limits dictionary
    """
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
    cls, tier: str, environment: Optional[str] = None
  ) -> Dict[str, Any]:
    """Get backup limits for a tier.

    Args:
        tier: The tier name (kuzu-standard, kuzu-large, kuzu-xlarge, etc.)
        environment: Environment (defaults to current env)

    Returns:
        Backup limits dictionary
    """
    tier_config = cls.get_tier_config(tier, environment)
    default_limits = {
      "max_backup_size_gb": 10,
      "backup_retention_days": 7,
      "max_backups_per_day": 2,
    }
    return tier_config.get("backup_limits", default_limits)

  @classmethod
  def _generate_tier_features(cls, tier_config: Dict[str, Any]) -> List[str]:
    """Generate human-readable features list for a tier.

    Args:
        tier_config: Tier configuration dictionary

    Returns:
        List of feature strings
    """
    features = []

    # Add storage limit
    storage_gb = tier_config.get("storage_limit_gb")
    if storage_gb is not None and storage_gb > 0:
      if storage_gb >= 1000:
        features.append(f"{storage_gb / 1000:.0f}TB storage limit")
      else:
        features.append(f"{storage_gb}GB storage limit")

    # Add AI credits allocation
    monthly_credits = tier_config.get("monthly_credits")
    if monthly_credits is not None and monthly_credits > 0:
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

      max_memory_mb = instance.get("max_memory_mb", 0)
      if max_memory_mb and max_memory_mb > 0:
        features.append(f"{max_memory_mb / 1024:.0f}GB RAM")

    # Add rate limit multiplier if not standard
    rate_multiplier = tier_config.get("api_rate_multiplier", 1.0)
    if rate_multiplier is not None and rate_multiplier > 1:
      features.append(f"{rate_multiplier}x API rate limits")

    # Add backup retention
    backup_limits = tier_config.get("backup_limits", {})
    retention_days = backup_limits.get("backup_retention_days")
    if retention_days is not None and retention_days > 0:
      features.append(f"{retention_days}-day backup retention")

    return features

  @classmethod
  def get_available_tiers(
    cls, environment: Optional[str] = None, include_disabled: bool = False
  ) -> List[Dict[str, Any]]:
    """Get all available tiers for the environment.

    Args:
        environment: Environment (defaults to current env)
        include_disabled: Whether to include disabled tiers (default: False)

    Returns:
        List of tier configuration dictionaries with formatted information
    """
    if environment is None:
      environment = "production" if env.ENVIRONMENT == "prod" else "staging"

    config = cls._load_config()
    env_config = config.get(environment, {})
    writers = env_config.get("writers", [])

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

      tier_info = {
        "tier": writer.get("tier"),
        "name": writer.get("name"),
        "description": writer.get("description"),
        "backend": writer.get("backend"),
        "enabled": is_enabled,
        "max_subgraphs": writer.get("max_subgraphs"),
        "storage_limit_gb": writer.get("storage_limit_gb"),
        "monthly_credits": writer.get("monthly_credits"),
        "api_rate_multiplier": writer.get("api_rate_multiplier", 1.0),
        "features": cls._generate_tier_features(writer),
        "instance": {
          "type": instance_config.get("type"),
          "memory_mb": graph_memory_mb,
          "databases_per_instance": instance_config.get("databases_per_instance", 1),
          "is_multitenant": is_multitenant,
        },
        "limits": {
          "storage_gb": writer.get("storage_limit_gb"),
          "monthly_credits": writer.get("monthly_credits"),
          "max_subgraphs": writer.get("max_subgraphs"),
          "copy_operations": writer.get("copy_operations", {}),
          "backup": writer.get("backup_limits", {}),
        },
      }

      # Add display name based on tier
      display_names = {
        "kuzu-standard": "Kuzu Standard",
        "kuzu-large": "Kuzu Professional",
        "kuzu-xlarge": "Kuzu Enterprise",
        "kuzu-shared": "Shared Repository",
        "neo4j-community-large": "Neo4j Community",
        "neo4j-enterprise-xlarge": "Neo4j Enterprise",
      }
      tier_info["display_name"] = display_names.get(
        writer.get("tier"), writer.get("name")
      )

      # Add pricing placeholder (to be filled from billing config if needed)
      tier_info["monthly_price"] = None  # This should come from billing config

      available_tiers.append(tier_info)

    return available_tiers

  @classmethod
  def clear_cache(cls) -> None:
    """Clear the configuration cache (useful for testing)."""
    cls._config_cache = None


@lru_cache(maxsize=32)
def get_tier_max_subgraphs(
  tier: str, environment: Optional[str] = None
) -> Optional[int]:
  """Cached function to get max subgraphs for a tier.

  Args:
      tier: The tier name (standard, enterprise, premium)
      environment: Environment (defaults to current env)

  Returns:
      Maximum subgraphs allowed, or None for unlimited
  """
  return GraphTierConfig.get_max_subgraphs(tier, environment)


@lru_cache(maxsize=32)
def get_tier_api_rate_multiplier(tier: str, environment: Optional[str] = None) -> float:
  """Cached function to get rate limit multiplier for a tier.

  Args:
      tier: The tier name (standard, enterprise, premium)
      environment: Environment (defaults to current env)

  Returns:
      Rate limit multiplier (1.0 = base limits)
  """
  return GraphTierConfig.get_api_rate_multiplier(tier, environment)


@lru_cache(maxsize=32)
def get_tier_copy_operation_limits(
  tier: str, environment: Optional[str] = None
) -> Dict[str, Any]:
  """Cached function to get copy operation limits for a tier.

  Args:
      tier: The tier name (standard, enterprise, premium)
      environment: Environment (defaults to current env)

  Returns:
      Copy operation limits dictionary
  """
  return GraphTierConfig.get_copy_operation_limits(tier, environment)


@lru_cache(maxsize=32)
def get_tier_backup_limits(
  tier: str, environment: Optional[str] = None
) -> Dict[str, Any]:
  """Cached function to get backup limits for a tier.

  Args:
      tier: The tier name (kuzu-standard, kuzu-large, kuzu-xlarge, etc.)
      environment: Environment (defaults to current env)

  Returns:
      Backup limits dictionary
  """
  return GraphTierConfig.get_backup_limits(tier, environment)
