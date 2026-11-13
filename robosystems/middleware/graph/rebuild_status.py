"""Whether a graph is mid-rebuild, asked before a query is sent to it.

A rebuild deletes and regenerates the database, so a query in that window
would read a missing or half-loaded graph. The flag lives on the platform
``Graph`` row; the Graph API holds no database connection, so its callers
check here. Cached per process to keep the lookup off the query path, and
failing open: a database error must not take queries down with it.
"""

import asyncio
import time

from robosystems.logger import logger

REBUILD_CACHE_TTL_SECONDS = 30

_cache: dict[str, tuple[bool, float]] = {}


def _cached(graph_id: str) -> bool | None:
  entry = _cache.get(graph_id)
  if entry is None:
    return None
  is_rebuilding, cached_at = entry
  if time.monotonic() - cached_at >= REBUILD_CACHE_TTL_SECONDS:
    return None
  return is_rebuilding


def _lookup(graph_id: str) -> bool:
  from robosystems.database import SessionFactory
  from robosystems.models.core import Graph

  try:
    session = SessionFactory()
    try:
      graph = Graph.get_by_id(graph_id, session)
      is_rebuilding = bool(graph and graph.is_rebuilding)
    finally:
      session.close()
  except Exception:
    logger.warning(
      f"Failed to check rebuild status for {graph_id}, assuming not rebuilding"
    )
    return False
  _cache[graph_id] = (is_rebuilding, time.monotonic())
  return is_rebuilding


async def is_graph_rebuilding(graph_id: str) -> bool:
  cached = _cached(graph_id)
  if cached is not None:
    return cached
  return await asyncio.to_thread(_lookup, graph_id)


def clear_rebuild_status_cache() -> None:
  _cache.clear()
