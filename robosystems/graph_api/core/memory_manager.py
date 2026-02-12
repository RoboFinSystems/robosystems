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
    # Free memory before boosting (safe: only reachable on shared tier today,
    # which has no concurrent API traffic — see ensure_ladybug_memory_boosted)
    try:
      release_duckdb_memory(graph_id)
    except Exception as e:
      logger.warning(f"Could not release DuckDB memory before boost: {e}")

    try:
      pool = get_connection_pool()
      _evict_idle_subgraph_databases(pool, graph_id)
    except Exception as e:
      logger.warning(f"Could not evict idle databases before boost: {e}")

    logger.info(
      f"Boosting LadybugDB memory to {boost_mb}MB for materializing {graph_id}"
    )
    old_override = set_ladybug_memory_override(boost_mb, graph_id=graph_id)

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
      set_ladybug_memory_override(old_override, graph_id=graph_id)

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


def _evict_idle_subgraph_databases(pool, target_graph_id: str) -> list[str]:
  """
  Evict idle subgraph databases to free their buffer pool memory.

  Subgraphs are identified by containing an underscore (e.g., sec_historical).
  The target graph and any graph with active connections are skipped.

  Args:
      pool: The LadybugDB connection pool
      target_graph_id: The graph being boosted (will not be evicted)

  Returns:
      List of database names that were evicted
  """
  evicted = []
  for db_name in pool.list_databases():
    # Skip the target graph — it will be recreated with the boost
    if db_name == target_graph_id:
      continue
    # Only evict subgraphs (contain underscore: parent_subgraph)
    if "_" not in db_name:
      continue
    # Skip databases with active connections (someone is querying)
    if pool.has_active_connections(db_name):
      continue
    try:
      pool.close_database_connections(db_name)
      if not pool.has_active_connections(db_name):
        evicted.append(db_name)
      else:
        logger.warning(
          f"Database {db_name} still has connections after eviction attempt"
        )
    except Exception as e:
      logger.warning(f"Failed to evict idle database {db_name}: {e}")
  return evicted


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

  # Check if override is already set for this specific graph
  current_override = get_ladybug_memory_override(graph_id)
  if current_override:
    logger.debug(
      f"LadybugDB memory override already active for {graph_id}: {current_override}MB"
    )
    _active_ladybug_boosts.add(graph_id)
    return None

  # NOTE: The operations below (DuckDB release, subgraph eviction) are currently
  # only reachable on the shared tier (the only tier with ladybug_memory_boost_mb
  # configured). The shared master has no API traffic during Dagster builds, so
  # closing connections and evicting databases is safe.
  #
  # If boost configs are added to customer tiers (standard/large/xlarge) in the
  # future, these operations need API-aware guards because those instances serve
  # live query traffic concurrently with ingestion/materialization.

  # Release DuckDB connections for this graph — staging is complete by the time
  # materialization starts, so these connections are idle but still hold buffer memory.
  try:
    result = release_duckdb_memory(graph_id)
    if result.get("connections_closed", 0) > 0:
      logger.info(
        f"Released DuckDB memory before LadybugDB boost: "
        f"{result['connections_closed']} connections closed"
      )
  except Exception as e:
    logger.warning(f"Could not release DuckDB memory before boost: {e}")

  # Evict idle subgraph databases to reclaim their buffer pool memory.
  # Only subgraphs are evicted — the target graph will be recreated with
  # the boost, and there's only one primary graph per instance.
  try:
    pool = get_connection_pool()
    evicted = _evict_idle_subgraph_databases(pool, graph_id)
    if evicted:
      logger.info(
        f"Evicted {len(evicted)} idle subgraph databases before boost: {evicted}"
      )
  except Exception as e:
    logger.warning(f"Could not evict idle databases before boost: {e}")

  # Apply boost (scoped to this specific database)
  logger.info(f"Boosting LadybugDB memory to {boost_mb}MB for {graph_id}")
  set_ladybug_memory_override(boost_mb, graph_id=graph_id)
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

  # Clear override for this specific graph (per-database scoping)
  logger.info(f"Restoring LadybugDB memory to default after {graph_id}")
  set_ladybug_memory_override(None, graph_id=graph_id)

  # Recreate database with default memory
  try:
    pool = get_connection_pool()
    pool.recreate_database(graph_id)
    return True
  except Exception as e:
    logger.warning(f"Could not restore LadybugDB memory config: {e}")
    return False


def is_ladybug_memory_boosted(graph_id: str) -> bool:
  """Check if LadybugDB memory is currently boosted for a graph."""
  return graph_id in _active_ladybug_boosts


def release_duckdb_memory(graph_id: str) -> dict[str, int | bool]:
  """
  Release DuckDB memory by closing all connections for a graph.

  Unlike restore_duckdb_memory (which only reconfigures limits), this function
  actually closes connections to force DuckDB to release its buffer memory
  back to the OS.

  Call this after staging operations complete to free memory.

  Args:
      graph_id: The graph whose connections should be closed

  Returns:
      Dict with connections_closed count and success status
  """
  from robosystems.graph_api.core.duckdb.pool import get_duckdb_pool

  try:
    pool = get_duckdb_pool()

    # Get connection count before closing
    connections_before = 0
    if graph_id in pool._pools:
      connections_before = len(pool._pools[graph_id])

    # Close all connections - this releases DuckDB's buffer memory
    pool.close_database_connections(graph_id)

    logger.info(
      f"Released DuckDB memory for {graph_id}: closed {connections_before} connections"
    )

    return {
      "connections_closed": connections_before,
      "success": True,
    }
  except Exception as e:
    logger.warning(f"Failed to release DuckDB memory for {graph_id}: {e}")
    return {
      "connections_closed": 0,
      "success": False,
      "error": str(e),
    }


def release_ladybug_memory(graph_id: str, aggressive: bool = True) -> dict[str, bool]:
  """
  Release LadybugDB memory by forcing database cleanup.

  This closes connections and optionally performs aggressive cleanup
  (GC, malloc_trim) to return memory to the OS.

  Args:
      graph_id: The graph whose memory should be released
      aggressive: If True, run GC and malloc_trim for maximum memory release

  Returns:
      Dict with success status
  """
  from robosystems.graph_api.core.ladybug.pool import get_connection_pool

  try:
    pool = get_connection_pool()
    pool.force_database_cleanup(graph_id, aggressive=aggressive)

    logger.info(f"Released LadybugDB memory for {graph_id} (aggressive={aggressive})")

    return {"success": True, "aggressive": aggressive}
  except Exception as e:
    logger.warning(f"Failed to release LadybugDB memory for {graph_id}: {e}")
    return {"success": False, "error": str(e)}
