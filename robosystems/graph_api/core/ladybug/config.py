"""LadybugDB memory sizing — the single source of truth for buffer pool limits.

Kept separate from the manager and pool so both can import it without a cycle.
"""

from robosystems.config import env
from robosystems.logger import logger

# Overrides are scoped per database. A process-wide boost would raise the
# buffer pool for every database on the instance at once, which OOMs the node.
_memory_overrides: dict[str, int] = {}


def set_ladybug_memory_override(
  memory_mb: int | None, graph_id: str | None = None
) -> int | None:
  """Set a temporary memory override for one database; returns the previous value.

  Recording the override is not enough on its own — buffer pool size is fixed
  when the Database object opens, so call ``pool.recreate_database(graph_id)``
  afterwards to make it take effect.

  ``graph_id`` is required when setting; passing neither argument clears every
  override.
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
    old_value = next(iter(_memory_overrides.values()), None)
    _memory_overrides.clear()
    logger.info("LadybugDB memory overrides cleared (all)")
    return old_value


def get_ladybug_memory_override(graph_id: str | None = None) -> int | None:
  """Get a database's memory override in MB.

  Without ``graph_id``, returns any active override — enough to answer
  "is anything boosted?", but not which database.
  """
  if graph_id:
    return _memory_overrides.get(graph_id)
  return next(iter(_memory_overrides.values()), None) if _memory_overrides else None


def subgraph_share_mb(tier_config: dict, subgraph_count: int) -> int | None:
  """One subgraph's buffer pool when the tier shares a budget among them.

  The budget is divided by the subgraphs that exist, so a lone subgraph gets
  room a full set could not each have. No share exceeds the parent's own pool
  or falls under the tier's minimum; at the minimum the tier is oversubscribed,
  which holds because a pool is a ceiling the engine grows into, not a
  reservation. None when the tier names no budget.
  """
  budget = int(tier_config.get("subgraph_memory_budget_mb", 0) or 0)
  if budget <= 0:
    return None
  share = budget // max(1, subgraph_count)
  ceiling = int(tier_config.get("memory_per_db_mb", 0) or 0)
  if ceiling > 0:
    share = min(share, ceiling)
  return max(share, int(tier_config.get("subgraph_memory_min_mb", 0) or 0))


def count_subgraph_databases(database_name: str) -> int:
  """The subgraph databases on this node, counting ``database_name`` whether or
  not its file exists yet (it is sized as it is created)."""
  from pathlib import Path

  from robosystems.config.storage.graph import split_graph_db_filename

  names = {database_name}
  try:
    for item in Path(env.LBUG_DATABASE_PATH).iterdir():
      if not item.is_file():
        continue
      name = split_graph_db_filename(item.name)
      if name and "_" in name and not name.endswith(("-wip", "-prev")):
        names.add(name)
  except OSError as e:
    logger.warning(f"Could not count subgraph databases: {e}")
  return len(names)


def get_database_memory_config(database_name: str | None = None) -> int:
  """Get the buffer pool size in MB for a database, in priority order.

  1. That database's memory override, if one is set
  2. A share of ``subgraph_memory_budget_mb`` (or the fixed
     ``memory_per_subgraph_mb``), when the name is a subgraph
  3. ``memory_per_db_mb``, for parent databases
  4. ``lbug_max_memory_mb`` / ``max_memory_mb``, for single-database instances

  Subgraphs are those whose name contains an underscore (``sec_historical``,
  ``kg123_dev``). They get a smaller buffer pool than their parent and are
  deliberately oversubscribed: they are rarely all active at once.

  Always pass ``database_name`` — omitting it silently skips the override and
  subgraph rules.
  """
  override = get_ladybug_memory_override(database_name)
  if override:
    logger.info(
      f"Using LadybugDB memory override: {override} MB"
      + (f" (for '{database_name}')" if database_name else "")
    )
    return override

  tier_config = env.get_lbug_tier_config()

  is_subgraph = database_name is not None and "_" in database_name
  if is_subgraph:
    shared_mb = subgraph_share_mb(tier_config, count_subgraph_databases(database_name))
    if shared_mb:
      logger.info(
        f"Using subgraph memory share: {shared_mb} MB (for '{database_name}')"
      )
      return shared_mb

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

  # The 2048 default covers local dev, where no tier config is present.
  max_memory_mb = tier_config.get(
    "lbug_max_memory_mb", tier_config.get("max_memory_mb", 2048)
  )
  logger.info(
    f"Using total memory allocation: {max_memory_mb} MB (tier: {tier_config.get('tier', 'default')})"
  )
  return max_memory_mb
