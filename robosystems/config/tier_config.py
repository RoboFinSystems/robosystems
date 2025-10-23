"""Tier configuration utilities.

This module provides utilities for loading and accessing tier-specific
configuration values from the graph.yml configuration file.
"""

import os
import yaml
import warnings
from typing import Dict, Any, Optional
from functools import lru_cache

from robosystems.config import env


class TierConfig:
  """Utility class for accessing tier-specific configuration."""

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
        tier: The tier name (standard, enterprise, premium)
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
        tier: The tier name (standard, enterprise, premium)
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
        tier: The tier name (standard, enterprise, premium)
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
        tier: The tier name (standard, enterprise, premium)
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
        tier: The tier name (standard, enterprise, premium, shared)
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
        tier: The tier name (standard, enterprise, premium, shared)
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
        tier: The tier name (standard, enterprise, premium, shared)
        environment: Environment (defaults to current env)

    Returns:
        Complete instance configuration dictionary
    """
    tier_config = cls.get_tier_config(tier, environment)
    return tier_config.get("instance", {})

  @classmethod
  def get_storage_limit_gb(cls, tier: str, environment: Optional[str] = None) -> int:
    """Get storage limit for a tier.

    Args:
        tier: The tier name (standard, enterprise, premium)
        environment: Environment (defaults to current env)

    Returns:
        Storage limit in GB
    """
    tier_config = cls.get_tier_config(tier, environment)
    return tier_config.get("storage_limit_gb", 500)

  @classmethod
  def get_monthly_credits(cls, tier: str, environment: Optional[str] = None) -> int:
    """Get monthly credit allocation for a tier.

    Args:
        tier: The tier name (standard, enterprise, premium)
        environment: Environment (defaults to current env)

    Returns:
        Monthly credit allocation
    """
    tier_config = cls.get_tier_config(tier, environment)
    return tier_config.get("monthly_credits", 10000)

  @classmethod
  def get_rate_limit_multiplier(
    cls, tier: str, environment: Optional[str] = None
  ) -> float:
    """Get rate limit multiplier for a tier.

    Args:
        tier: The tier name (standard, enterprise, premium)
        environment: Environment (defaults to current env)

    Returns:
        Rate limit multiplier (1.0 = base limits)
    """
    tier_config = cls.get_tier_config(tier, environment)
    return tier_config.get("rate_limit_multiplier", 1.0)

  @classmethod
  def get_copy_operation_limits(
    cls, tier: str, environment: Optional[str] = None
  ) -> Dict[str, Any]:
    """Get copy operation limits for a tier.

    Args:
        tier: The tier name (standard, enterprise, premium)
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
  return TierConfig.get_max_subgraphs(tier, environment)


@lru_cache(maxsize=32)
def get_tier_storage_limit(tier: str, environment: Optional[str] = None) -> int:
  """Cached function to get storage limit for a tier.

  Args:
      tier: The tier name (standard, enterprise, premium)
      environment: Environment (defaults to current env)

  Returns:
      Storage limit in GB
  """
  return TierConfig.get_storage_limit_gb(tier, environment)


@lru_cache(maxsize=32)
def get_tier_monthly_credits(tier: str, environment: Optional[str] = None) -> int:
  """Cached function to get monthly credit allocation for a tier.

  Args:
      tier: The tier name (standard, enterprise, premium)
      environment: Environment (defaults to current env)

  Returns:
      Monthly credit allocation
  """
  return TierConfig.get_monthly_credits(tier, environment)


@lru_cache(maxsize=32)
def get_tier_rate_limit_multiplier(
  tier: str, environment: Optional[str] = None
) -> float:
  """Cached function to get rate limit multiplier for a tier.

  Args:
      tier: The tier name (standard, enterprise, premium)
      environment: Environment (defaults to current env)

  Returns:
      Rate limit multiplier (1.0 = base limits)
  """
  return TierConfig.get_rate_limit_multiplier(tier, environment)


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
  return TierConfig.get_copy_operation_limits(tier, environment)


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
  return TierConfig.get_backup_limits(tier, environment)
