"""Memory management utilities for graph operations.

This module provides functions for temporarily boosting memory allocation
during heavy operations like DuckDB staging and LadybugDB materialization.

The pattern prevents OOM by:
1. Using conservative default memory limits for both DuckDB and LadybugDB
2. Temporarily boosting memory ONLY for the system that needs it
3. Automatically restoring defaults after the operation

For per-table operations (like materialize endpoint called 36 times),
use ensure_ladybug_memory_boosted() which only boosts on first call.

Usage:
    # For batch operations (single call)
    with boost_duckdb_memory("sec"):
        create_all_staging_tables(...)

    # For per-table operations (called many times)
    ensure_ladybug_memory_boosted("sec")  # Only boosts first time
    materialize_table(...)
    # Call restore_ladybug_memory() when batch is complete
"""

from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager

from robosystems.config import env
from robosystems.config.graph_tier import GraphTierConfig
from robosystems.logger import logger

# Track which graphs have active memory boosts to avoid redundant boost/restore cycles
_active_duckdb_boosts: set[str] = set()
_active_ladybug_boosts: set[str] = set()


@contextmanager
def boost_duckdb_memory(graph_id: str) -> Generator[str | None]:
  """
  Context manager to temporarily boost DuckDB memory for staging operations.

  This sets the DuckDB memory override to the tier's boost value, then
  restores the previous value (usually None for default) on exit.

  Args:
      graph_id: The graph being staged (used for logging)

  Yields:
      The boost memory limit that was applied, or None if no boost configured

  Example:
      with boost_duckdb_memory("sec"):
          # DuckDB now has 55GB instead of 10GB
          create_staging_tables(...)
      # Memory restored to default 10GB
  """
  from robosystems.graph_api.core.duckdb.pool import (
    get_duckdb_pool,
    set_duckdb_memory_override,
  )

  tier = env.CLUSTER_TIER
  boost_limit = None

  if tier:
    boost_limit = GraphTierConfig.get_duckdb_memory_boost(tier)

  if boost_limit:
    logger.info(f"Boosting DuckDB memory to {boost_limit} for staging {graph_id}")
    old_override = set_duckdb_memory_override(boost_limit, graph_id)

    # Reconfigure existing connections to use new limit
    try:
      pool = get_duckdb_pool()
      pool.reconfigure_memory_limit(graph_id, boost_limit)
    except Exception as e:
      logger.warning(f"Could not reconfigure existing DuckDB connections: {e}")

    try:
      yield boost_limit
    finally:
      logger.info(f"Restoring DuckDB memory to default after staging {graph_id}")
      set_duckdb_memory_override(old_override, graph_id)

      # Reconfigure connections back to default
      try:
        default_limit = GraphTierConfig.get_duckdb_memory_limit(tier) if tier else "2GB"
        pool = get_duckdb_pool()
        pool.reconfigure_memory_limit(graph_id, default_limit)
      except Exception as e:
        logger.warning(f"Could not restore DuckDB memory config: {e}")
  else:
    # No boost configured, just yield
    yield None


@contextmanager
def boost_ladybug_memory(graph_id: str) -> Generator[int | None]:
  """
  Context manager to temporarily boost LadybugDB memory for materialization.

  This sets the LadybugDB memory override to the tier's boost value, recreates
  the database with the new buffer pool size, then restores on exit.

  Args:
      graph_id: The graph being materialized (database name)

  Yields:
      The boost memory limit in MB that was applied, or None if no boost configured

  Example:
      with boost_ladybug_memory("sec"):
          # LadybugDB now has 50GB buffer pool instead of 10GB
          materialize_tables(...)
      # Memory restored to default 10GB
  """
  from robosystems.graph_api.core.ladybug.config import set_ladybug_memory_override
  from robosystems.graph_api.core.ladybug.pool import get_connection_pool

  tier = env.CLUSTER_TIER
  boost_mb = None

  if tier:
    boost_mb = GraphTierConfig.get_ladybug_memory_boost_mb(tier)

  if boost_mb:
    logger.info(
      f"Boosting LadybugDB memory to {boost_mb}MB for materializing {graph_id}"
    )
    old_override = set_ladybug_memory_override(boost_mb)

    # Recreate database with new buffer pool size
    try:
      pool = get_connection_pool()
      pool.recreate_database(graph_id)
    except Exception as e:
      logger.warning(f"Could not recreate LadybugDB database with boost memory: {e}")

    try:
      yield boost_mb
    finally:
      logger.info(
        f"Restoring LadybugDB memory to default after materializing {graph_id}"
      )
      set_ladybug_memory_override(old_override)

      # Recreate database with restored memory
      try:
        pool = get_connection_pool()
        pool.recreate_database(graph_id)
      except Exception as e:
        logger.warning(f"Could not restore LadybugDB memory config: {e}")
  else:
    # No boost configured, just yield
    yield None


@asynccontextmanager
async def boost_duckdb_memory_async(graph_id: str) -> AsyncGenerator[str | None]:
  """Async version of boost_duckdb_memory."""
  with boost_duckdb_memory(graph_id) as boost:
    yield boost


@asynccontextmanager
async def boost_ladybug_memory_async(graph_id: str) -> AsyncGenerator[int | None]:
  """Async version of boost_ladybug_memory."""
  with boost_ladybug_memory(graph_id) as boost:
    yield boost


def ensure_duckdb_memory_boosted(graph_id: str) -> str | None:
  """
  Ensure DuckDB memory is boosted for a graph, only boosting if not already active.

  This is designed for per-table operations where the endpoint is called many times
  (e.g., staging endpoint called 36 times for SEC). It only applies the boost
  on the first call.

  Call restore_duckdb_memory(graph_id) when the batch is complete.

  Args:
      graph_id: The graph being staged

  Returns:
      The boost memory limit if newly boosted, or None if already boosted or no boost configured
  """
  from robosystems.graph_api.core.duckdb.pool import (
    get_duckdb_memory_override,
    get_duckdb_pool,
    set_duckdb_memory_override,
  )

  # Check if already boosted for this graph
  if graph_id in _active_duckdb_boosts:
    logger.debug(f"DuckDB memory already boosted for {graph_id}")
    return None

  # Check if boost is configured for this tier
  tier = env.CLUSTER_TIER
  if not tier:
    return None

  boost_limit = GraphTierConfig.get_duckdb_memory_boost(tier)
  if not boost_limit:
    return None

  # Check if override is already set for this graph
  current_override = get_duckdb_memory_override(graph_id)
  if current_override:
    logger.debug(
      f"DuckDB memory override already active for {graph_id}: {current_override}"
    )
    _active_duckdb_boosts.add(graph_id)
    return None

  # Apply boost
  logger.info(f"Boosting DuckDB memory to {boost_limit} for staging {graph_id}")
  set_duckdb_memory_override(boost_limit, graph_id)
  _active_duckdb_boosts.add(graph_id)

  # Reconfigure existing connections
  try:
    pool = get_duckdb_pool()
    pool.reconfigure_memory_limit(graph_id, boost_limit)
  except Exception as e:
    logger.warning(f"Could not reconfigure existing DuckDB connections: {e}")

  return boost_limit


def restore_duckdb_memory(graph_id: str) -> bool:
  """
  Restore DuckDB memory to default after staging is complete.

  Args:
      graph_id: The graph that was staged

  Returns:
      True if memory was restored, False if wasn't boosted or error occurred
  """
  from robosystems.graph_api.core.duckdb.pool import (
    get_duckdb_pool,
    set_duckdb_memory_override,
  )

  if graph_id not in _active_duckdb_boosts:
    logger.debug(f"DuckDB memory not boosted for {graph_id}, nothing to restore")
    return False

  _active_duckdb_boosts.discard(graph_id)

  # Clear override for this graph
  logger.info(f"Restoring DuckDB memory to default after {graph_id}")
  set_duckdb_memory_override(None, graph_id)

  # Reconfigure connections back to default
  tier = env.CLUSTER_TIER
  try:
    default_limit = GraphTierConfig.get_duckdb_memory_limit(tier) if tier else "2GB"
    pool = get_duckdb_pool()
    pool.reconfigure_memory_limit(graph_id, default_limit)
    return True
  except Exception as e:
    logger.warning(f"Could not restore DuckDB memory config: {e}")
    return False


def is_duckdb_memory_boosted(graph_id: str) -> bool:
  """Check if DuckDB memory is currently boosted for a graph."""
  return graph_id in _active_duckdb_boosts


def ensure_ladybug_memory_boosted(graph_id: str) -> int | None:
  """
  Ensure LadybugDB memory is boosted for a graph, only boosting if not already active.

  This is designed for per-table operations where the endpoint is called many times
  (e.g., materialize endpoint called 36 times for SEC). It only applies the boost
  on the first call, avoiding expensive database recreation on subsequent calls.

  Call restore_ladybug_memory(graph_id) when the batch is complete.

  Args:
      graph_id: The graph being materialized

  Returns:
      The boost memory in MB if newly boosted, or None if already boosted or no boost configured
  """
  from robosystems.graph_api.core.ladybug.config import (
    get_ladybug_memory_override,
    set_ladybug_memory_override,
  )
  from robosystems.graph_api.core.ladybug.pool import get_connection_pool

  # Check if already boosted for this graph
  if graph_id in _active_ladybug_boosts:
    logger.debug(f"LadybugDB memory already boosted for {graph_id}")
    return None

  # Check if boost is configured for this tier
  tier = env.CLUSTER_TIER
  if not tier:
    return None

  boost_mb = GraphTierConfig.get_ladybug_memory_boost_mb(tier)
  if not boost_mb:
    return None

  # Check if override is already set (e.g., from another graph's boost)
  current_override = get_ladybug_memory_override()
  if current_override:
    logger.debug(f"LadybugDB memory override already active: {current_override}MB")
    _active_ladybug_boosts.add(graph_id)
    return None

  # Apply boost
  logger.info(f"Boosting LadybugDB memory to {boost_mb}MB for {graph_id}")
  set_ladybug_memory_override(boost_mb)
  _active_ladybug_boosts.add(graph_id)

  # Recreate database with new buffer pool size
  try:
    pool = get_connection_pool()
    pool.recreate_database(graph_id)
  except Exception as e:
    logger.warning(f"Could not recreate LadybugDB database with boost memory: {e}")

  return boost_mb


def restore_ladybug_memory(graph_id: str) -> bool:
  """
  Restore LadybugDB memory to default after materialization is complete.

  This should be called when a batch of materialize operations is complete.

  Args:
      graph_id: The graph that was materialized

  Returns:
      True if memory was restored, False if wasn't boosted or error occurred
  """
  from robosystems.graph_api.core.ladybug.config import set_ladybug_memory_override
  from robosystems.graph_api.core.ladybug.pool import get_connection_pool

  if graph_id not in _active_ladybug_boosts:
    logger.debug(f"LadybugDB memory not boosted for {graph_id}, nothing to restore")
    return False

  _active_ladybug_boosts.discard(graph_id)

  # Only clear override if no other graphs are using boost
  if not _active_ladybug_boosts:
    logger.info(f"Restoring LadybugDB memory to default after {graph_id}")
    set_ladybug_memory_override(None)

    # Recreate database with default memory
    try:
      pool = get_connection_pool()
      pool.recreate_database(graph_id)
      return True
    except Exception as e:
      logger.warning(f"Could not restore LadybugDB memory config: {e}")
      return False
  else:
    logger.debug(
      f"Other graphs still using boost: {_active_ladybug_boosts}, not restoring"
    )
    return False


def is_ladybug_memory_boosted(graph_id: str) -> bool:
  """Check if LadybugDB memory is currently boosted for a graph."""
  return graph_id in _active_ladybug_boosts
