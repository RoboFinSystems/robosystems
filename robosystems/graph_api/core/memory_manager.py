"""Temporarily shift memory between DuckDB and LadybugDB on a graph instance.

Both engines share one box, so both run with conservative defaults and only
the engine doing the heavy work is boosted — DuckDB during staging, LadybugDB
during materialization — with the default restored afterwards.

Two shapes, matching two call patterns:

- ``boost_*_memory`` context managers, for a single batch call.
- ``ensure_*_memory_boosted`` / ``restore_*_memory``, for per-table endpoints
  invoked many times in a row (SEC materializes ~36 tables); the boost is
  applied on the first call only, since recreating the LadybugDB database is
  expensive.

Raising a limit and lowering it back is not the same as freeing memory:
``release_duckdb_memory`` closes connections, which is what actually returns
DuckDB's buffers to the OS.

Usage:
    with boost_duckdb_memory("sec"):
        create_all_staging_tables(...)

    ensure_ladybug_memory_boosted("sec")  # boosts on the first call only
    materialize_table(...)
    restore_ladybug_memory("sec")  # once the batch is done
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
  """Raise DuckDB's memory limit to the tier's boost value for the block,
  reconfiguring open connections, and restore it on exit.

  Yields the applied limit, or None when the tier configures no boost.

  Example:
      with boost_duckdb_memory("sec"):
          create_staging_tables(...)
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

      try:
        default_limit = GraphTierConfig.get_duckdb_memory_limit(tier) if tier else "2GB"
        pool = get_duckdb_pool()
        pool.reconfigure_memory_limit(graph_id, default_limit)
      except Exception as e:
        logger.warning(f"Could not restore DuckDB memory config: {e}")
  else:
    yield None


@contextmanager
def boost_ladybug_memory(graph_id: str) -> Generator[int | None]:
  """Raise LadybugDB's buffer pool to the tier's boost value for the block.

  Buffer pool size is fixed at database open, so this recreates the database
  on the way in and again on the way out. DuckDB connections are closed and
  idle subgraph databases evicted first, to make the memory actually
  available.

  Yields the applied limit in MB, or None when the tier configures no boost.

  Example:
      with boost_ladybug_memory("sec"):
          materialize_tables(...)
  """
  from robosystems.graph_api.core.ladybug.config import set_ladybug_memory_override
  from robosystems.graph_api.core.ladybug.pool import get_connection_pool

  tier = env.CLUSTER_TIER
  boost_mb = None

  if tier:
    boost_mb = GraphTierConfig.get_ladybug_memory_boost_mb(tier)

  if boost_mb:
    # Free memory before boosting. Safe only because a boost is configured on
    # the shared tier alone, which serves no concurrent API traffic — see
    # ensure_ladybug_memory_boosted.
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

      try:
        pool = get_connection_pool()
        pool.recreate_database(graph_id)
      except Exception as e:
        logger.warning(f"Could not restore LadybugDB memory config: {e}")
  else:
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
  """Boost DuckDB memory for a graph unless it is already boosted.

  For per-table endpoints called many times in a batch. Returns the boost
  limit when this call applied it, None when it was already active or the tier
  configures no boost. Pair with ``restore_duckdb_memory`` at end of batch.
  """
  from robosystems.graph_api.core.duckdb.pool import (
    get_duckdb_memory_override,
    get_duckdb_pool,
    set_duckdb_memory_override,
  )

  tier = env.CLUSTER_TIER
  if not tier:
    return None

  boost_limit = GraphTierConfig.get_duckdb_memory_boost(tier)
  if not boost_limit:
    return None

  # Trust the override itself, not the tracking set, as the state of record.
  current_override = get_duckdb_memory_override(graph_id)
  if current_override:
    logger.debug(
      f"DuckDB memory override already active for {graph_id}: {current_override}"
    )
    _active_duckdb_boosts.add(graph_id)
    return None

  # A stale tracking entry means a previous run was cancelled; re-apply.
  if graph_id in _active_duckdb_boosts:
    logger.warning(
      f"DuckDB tracking set had stale entry for {graph_id} "
      f"(override was cleared but tracking set was not). Re-applying boost."
    )

  logger.info(f"Boosting DuckDB memory to {boost_limit} for staging {graph_id}")
  set_duckdb_memory_override(boost_limit, graph_id)
  _active_duckdb_boosts.add(graph_id)

  try:
    pool = get_duckdb_pool()
    pool.reconfigure_memory_limit(graph_id, boost_limit)
  except Exception as e:
    logger.warning(f"Could not reconfigure existing DuckDB connections: {e}")

  return boost_limit


def restore_duckdb_memory(graph_id: str) -> bool:
  """Drop DuckDB back to the tier default after staging.

  Returns False if the graph was not boosted or the restore failed. This
  lowers the cap only; use ``release_duckdb_memory`` to hand buffers back to
  the OS.
  """
  from robosystems.graph_api.core.duckdb.pool import (
    get_duckdb_pool,
    set_duckdb_memory_override,
  )

  if graph_id not in _active_duckdb_boosts:
    logger.debug(f"DuckDB memory not boosted for {graph_id}, nothing to restore")
    return False

  _active_duckdb_boosts.discard(graph_id)

  logger.info(f"Restoring DuckDB memory to default after {graph_id}")
  set_duckdb_memory_override(None, graph_id)

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
  """Close idle subgraph databases so their buffer pools free up, returning the
  names evicted.

  Subgraphs are the ones whose name carries an underscore
  (``parent_subgraph``). The target graph and anything with live connections
  are left alone.
  """
  evicted = []
  for db_name in pool.list_databases():
    # The target is skipped — it gets recreated with the boost.
    if db_name == target_graph_id:
      continue
    if "_" not in db_name:
      continue
    # Someone is querying it.
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
  """Boost LadybugDB memory for a graph unless it is already boosted.

  For per-table endpoints called many times in a batch: boosting on the first
  call only avoids recreating the database (and refilling its buffer pool) on
  every subsequent one. Returns the boost in MB when this call applied it,
  None otherwise. Pair with ``restore_ladybug_memory`` at end of batch.
  """
  from robosystems.graph_api.core.ladybug.config import (
    get_ladybug_memory_override,
    set_ladybug_memory_override,
  )
  from robosystems.graph_api.core.ladybug.pool import get_connection_pool

  tier = env.CLUSTER_TIER
  if not tier:
    return None

  boost_mb = GraphTierConfig.get_ladybug_memory_boost_mb(tier)
  if not boost_mb:
    return None

  # Trust the override itself, not the tracking set, as the state of record.
  current_override = get_ladybug_memory_override(graph_id)
  if current_override:
    logger.debug(
      f"LadybugDB memory override already active for {graph_id}: {current_override}MB"
    )
    _active_ladybug_boosts.add(graph_id)
    return None

  # A stale tracking entry means a previous run was cancelled; re-apply.
  if graph_id in _active_ladybug_boosts:
    logger.warning(
      f"LadybugDB tracking set had stale entry for {graph_id} "
      f"(override was cleared but tracking set was not). Re-applying boost."
    )

  # The DuckDB release and subgraph eviction below are reachable only on the
  # shared tier, the only one with ladybug_memory_boost_mb configured, whose
  # master serves no API traffic during Dagster builds. Adding a boost config
  # to a customer tier (standard/large/xlarge) requires API-aware guards here
  # first: those instances serve live queries during materialization.

  # Staging is finished by the time materialization starts, so these DuckDB
  # connections are idle but still holding buffer memory. Closing them is what
  # hands it back.
  try:
    result = release_duckdb_memory(graph_id)
    if result.get("connections_closed", 0) > 0:
      logger.info(
        f"Released DuckDB memory before LadybugDB boost: "
        f"{result['connections_closed']} connections closed"
      )
  except Exception as e:
    logger.warning(f"Could not release DuckDB memory before boost: {e}")

  # Only subgraphs are evicted: the target graph is recreated with the boost,
  # and there is only one primary graph per instance.
  try:
    pool = get_connection_pool()
    evicted = _evict_idle_subgraph_databases(pool, graph_id)
    if evicted:
      logger.info(
        f"Evicted {len(evicted)} idle subgraph databases before boost: {evicted}"
      )
  except Exception as e:
    logger.warning(f"Could not evict idle databases before boost: {e}")

  logger.info(f"Boosting LadybugDB memory to {boost_mb}MB for {graph_id}")
  set_ladybug_memory_override(boost_mb, graph_id=graph_id)
  _active_ladybug_boosts.add(graph_id)

  # Buffer pool size is fixed at open, so the database must be recreated.
  try:
    pool = get_connection_pool()
    pool.recreate_database(graph_id)
  except Exception as e:
    logger.warning(f"Could not recreate LadybugDB database with boost memory: {e}")

  return boost_mb


def restore_ladybug_memory(graph_id: str) -> bool:
  """Drop LadybugDB back to the tier default once a materialize batch is done,
  recreating the database with the smaller buffer pool.

  Returns False if the graph was not boosted or the restore failed.
  """
  from robosystems.graph_api.core.ladybug.config import set_ladybug_memory_override
  from robosystems.graph_api.core.ladybug.pool import get_connection_pool

  if graph_id not in _active_ladybug_boosts:
    logger.debug(f"LadybugDB memory not boosted for {graph_id}, nothing to restore")
    return False

  _active_ladybug_boosts.discard(graph_id)

  logger.info(f"Restoring LadybugDB memory to default after {graph_id}")
  set_ladybug_memory_override(None, graph_id=graph_id)

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
  """Close every DuckDB connection for a graph, returning the buffers to the OS.

  ``restore_duckdb_memory`` only lowers the configured limit; closing the
  connections is what actually frees memory. Call this once staging is done,
  before LadybugDB needs the room. Returns connections_closed and success.
  """
  from robosystems.graph_api.core.duckdb.pool import get_duckdb_pool

  try:
    pool = get_duckdb_pool()

    connections_before = 0
    if graph_id in pool._pools:
      connections_before = len(pool._pools[graph_id])

    pool.close_database_connections(graph_id)

    # Clear the override too, or new connections opened during materialization
    # pick up the stale boost and compete with LadybugDB for memory.
    from robosystems.graph_api.core.duckdb.pool import set_duckdb_memory_override

    set_duckdb_memory_override(None, graph_id=graph_id)

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
  """Close a graph's LadybugDB database to release its buffer pool.

  With ``aggressive`` (the default), also runs GC and ``malloc_trim`` so glibc
  hands the freed arenas back to the OS rather than retaining them.
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
