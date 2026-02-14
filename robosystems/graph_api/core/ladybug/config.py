"""LadybugDB configuration utilities.

This module provides configuration functions that need to be imported
by multiple modules in the ladybug package without circular imports.
"""

from robosystems.config import env
from robosystems.logger import logger

# Per-database memory overrides for temporary boosts during heavy operations.
# Using per-database tracking (like DuckDB) prevents a boost for one database
# from leaking to other databases on the same instance, which can cause OOM.
_memory_overrides: dict[str, int] = {}


def set_ladybug_memory_override(
  memory_mb: int | None, graph_id: str | None = None
) -> int | None:
  """
  Set a temporary memory override for a specific LadybugDB database.

  This allows temporarily boosting LadybugDB memory during materialization,
  then restoring the default lower limit afterward. The override is scoped
  to a specific database to prevent boost leaking to subgraphs or other
  databases on the same instance.

  IMPORTANT: After setting this, you must call pool.recreate_database()
  on the target database to apply the new memory setting.

  Args:
      memory_mb: Memory in MB or None to clear override
      graph_id: Database name to scope the override to. If None, clears all.

  Returns:
      Previous override value (for restore purposes)

  Example:
      # Boost memory for materialization
      old_limit = set_ladybug_memory_override(50000, graph_id="sec")
      try:
          pool.recreate_database("sec")  # Recreate with new limit
          # ... perform materialization ...
      finally:
          set_ladybug_memory_override(old_limit, graph_id="sec")  # Restore
          pool.recreate_database("sec")  # Recreate with restored limit
  """
  if graph_id:
    old_value = _memory_overrides.get(graph_id)
    if memory_mb:
      _memory_overrides[graph_id] = memory_mb
      logger.info(f"LadybugDB memory override for '{graph_id}' set to: {memory_mb} MB")
    else:
      _memory_overrides.pop(graph_id, None)
      logger.info(f"LadybugDB memory override for '{graph_id}' cleared")
    return old_value
  else:
    if memory_mb is not None:
      raise ValueError(
        "graph_id is required when setting a memory override — "
        "pass graph_id to scope the override to a specific database"
      )
    # No graph_id + None memory_mb: clear all overrides
    old_value = next(iter(_memory_overrides.values()), None)
    _memory_overrides.clear()
    logger.info("LadybugDB memory overrides cleared (all)")
    return old_value


def get_ladybug_memory_override(graph_id: str | None = None) -> int | None:
  """Get current LadybugDB memory override in MB.

  Args:
      graph_id: Database name to check override for. If None, returns
          any active override (for backward compatibility checks).
  """
  if graph_id:
    return _memory_overrides.get(graph_id)
  # Legacy: return any override if exists (for is-anything-boosted checks)
  return next(iter(_memory_overrides.values()), None) if _memory_overrides else None


def get_database_memory_config(database_name: str | None = None) -> int:
  """
  Get memory configuration in MB for LadybugDB database creation.

  This function provides a single source of truth for memory allocation,
  used by both the connection pool and database manager.

  Priority order:
  1. Per-database override (only for the specific database being boosted)
  2. Subgraph memory limit (memory_per_subgraph_mb) - if database is a subgraph
  3. Per-database memory limit (memory_per_db_mb) - for parent databases
  4. Total memory allocation (lbug_max_memory_mb or max_memory_mb) - for dedicated instances
  5. Environment variable fallback (LBUG_MAX_MEMORY_MB)

  Subgraphs are identified by containing an underscore (e.g., sec_historical,
  kg123_dev). They receive a smaller buffer pool than parent databases, and
  are intentionally oversubscribed since they are rarely all active simultaneously.

  Args:
      database_name: Database name to check for per-database override.
          Pass this to ensure only the target database gets boosted memory.

  Returns:
      Memory allocation in megabytes
  """
  # Check for per-database override first (used during materialization)
  override = get_ladybug_memory_override(database_name)
  if override:
    logger.info(
      f"Using LadybugDB memory override: {override} MB"
      + (f" (for '{database_name}')" if database_name else "")
    )
    return override

  tier_config = env.get_lbug_tier_config()

  # Check if this is a subgraph (contains underscore: sec_historical, kg123_dev)
  is_subgraph = database_name is not None and "_" in database_name
  if is_subgraph:
    memory_per_subgraph_mb = tier_config.get("memory_per_subgraph_mb", 0)
    if memory_per_subgraph_mb > 0:
      logger.info(
        f"Using subgraph memory limit: {memory_per_subgraph_mb} MB (for '{database_name}')"
      )
      return memory_per_subgraph_mb

  memory_per_db_mb = tier_config.get("memory_per_db_mb", 0)

  if memory_per_db_mb > 0:
    logger.info(f"Using per-database memory limit: {memory_per_db_mb} MB")
    return memory_per_db_mb

  # Fall back to total memory for single-database instances (shared/dedicated)
  # Default of 2048 is sensible for local dev when tier config is not available
  max_memory_mb = tier_config.get(
    "lbug_max_memory_mb", tier_config.get("max_memory_mb", 2048)
  )
  logger.info(
    f"Using total memory allocation: {max_memory_mb} MB (tier: {tier_config.get('tier', 'default')})"
  )
  return max_memory_mb
