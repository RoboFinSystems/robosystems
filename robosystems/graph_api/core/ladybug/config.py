"""LadybugDB configuration utilities.

This module provides configuration functions that need to be imported
by multiple modules in the ladybug package without circular imports.
"""

from robosystems.config import env
from robosystems.logger import logger

# Memory override for temporary boosts during heavy operations (e.g., materialization)
_memory_override_mb: int | None = None


def set_ladybug_memory_override(memory_mb: int | None) -> int | None:
  """
  Set a temporary memory override for LadybugDB buffer pool.

  This allows temporarily boosting LadybugDB memory during materialization,
  then restoring the default lower limit afterward.

  IMPORTANT: After setting this, you must call pool.recreate_database()
  on any existing databases to apply the new memory setting.

  Args:
      memory_mb: Memory in MB or None to clear override

  Returns:
      Previous override value (for restore purposes)

  Example:
      # Boost memory for materialization
      old_limit = set_ladybug_memory_override(50000)  # 50GB
      try:
          pool.recreate_database("sec")  # Recreate with new limit
          # ... perform materialization ...
      finally:
          set_ladybug_memory_override(old_limit)  # Restore
          pool.recreate_database("sec")  # Recreate with restored limit
  """
  global _memory_override_mb
  old_value = _memory_override_mb
  _memory_override_mb = memory_mb
  if memory_mb:
    logger.info(f"LadybugDB memory override set to: {memory_mb} MB")
  else:
    logger.info("LadybugDB memory override cleared (using tier default)")
  return old_value


def get_ladybug_memory_override() -> int | None:
  """Get current LadybugDB memory override in MB, if any."""
  return _memory_override_mb


def get_database_memory_config() -> int:
  """
  Get memory configuration in MB for LadybugDB database creation.

  This function provides a single source of truth for memory allocation,
  used by both the connection pool and database manager.

  Priority order:
  1. Temporary override (for materialization operations that need high memory)
  2. Per-database memory limit (memory_per_db_mb) - for standard tier with oversubscription
  3. Total memory allocation (lbug_max_memory_mb or max_memory_mb) - for dedicated instances
  4. Environment variable fallback (LBUG_MAX_MEMORY_MB)

  Returns:
      Memory allocation in megabytes
  """
  # Check for temporary override first (used during materialization)
  override = get_ladybug_memory_override()
  if override:
    logger.info(f"Using LadybugDB memory override: {override} MB")
    return override

  tier_config = env.get_lbug_tier_config()
  memory_per_db_mb = tier_config.get("memory_per_db_mb", 0)

  if memory_per_db_mb > 0:
    # Use per-database limit (for standard tier with oversubscription)
    logger.info(f"Using per-database memory limit: {memory_per_db_mb} MB")
    return memory_per_db_mb

  # Fall back to total memory for single-database instances (shared/dedicated)
  max_memory_mb = tier_config.get(
    "lbug_max_memory_mb", tier_config.get("max_memory_mb", env.LBUG_MAX_MEMORY_MB)
  )
  logger.info(
    f"Using total memory allocation: {max_memory_mb} MB (tier: {tier_config.get('tier', 'default')})"
  )
  return max_memory_mb
