"""The get-graph-schema tool, cached per instance."""

import time
from threading import RLock
from typing import Any

from robosystems.config.defaults import CacheDefaults
from robosystems.logger import logger

from .base_tool import BaseTool


class SchemaTool(BaseTool):
  def __init__(self, client):
    super().__init__(client)
    self._schema_cache = None
    self._schema_cache_time = None
    self._schema_cache_ttl = CacheDefaults.SHORT
    self._cache_lock = RLock()

    self._cache_hits = 0
    self._cache_misses = 0

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-graph-schema",
      "description": """Get the graph schema: each node table with its property names, and each relationship table with its source and target node.

**WHEN TO USE:**
- Start here to understand the data structure
- Before writing queries, to check property names and relationship directions
- When exploring what data is available in the graph

**RETURNS:** A list of tables, nodes first, then relationships:
- **Node**: `label`, `properties` (names only; the core node types list their key properties), and a `description` / `comment` when one exists
- **Relationship**: `label`, `from_node`, `to_node`, and a `description` / `comment` when one exists

Property data types, keys and row counts are not included.

**NOTES:**
- Check relationship direction: ->() vs <-()
- Property names are case-sensitive""",
      "inputSchema": {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> list[dict[str, Any]]:
    self._log_tool_execution("get-graph-schema", arguments)

    current_time = time.time()
    with self._cache_lock:
      if (
        self._schema_cache is not None
        and self._schema_cache_time is not None
        and current_time - self._schema_cache_time < self._schema_cache_ttl
      ):
        self._cache_hits += 1
        logger.debug(
          f"Schema cache hit (hits: {self._cache_hits}, misses: {self._cache_misses})"
        )
        return self._schema_cache

    with self._cache_lock:
      self._cache_misses += 1
      logger.debug(
        f"Schema cache miss (hits: {self._cache_hits}, misses: {self._cache_misses})"
      )

    try:
      schema = await self.client.get_schema()

      with self._cache_lock:
        self._schema_cache = schema
        self._schema_cache_time = current_time

      return schema

    except Exception as e:
      logger.error(f"Failed to retrieve schema: {e}")
      raise

  def clear_schema_cache(self):
    """Clear the schema cache to force refresh on next call."""
    with self._cache_lock:
      self._schema_cache = None
      self._schema_cache_time = None
    logger.debug("Schema cache cleared")

  def get_cache_stats(self) -> dict[str, Any]:
    """Get cache performance statistics."""
    with self._cache_lock:
      total_requests = self._cache_hits + self._cache_misses
      hit_rate = (self._cache_hits / total_requests * 100) if total_requests > 0 else 0
      cache_age = (
        time.time() - self._schema_cache_time if self._schema_cache_time else None
      )
      return {
        "cache_hits": self._cache_hits,
        "cache_misses": self._cache_misses,
        "hit_rate_percent": round(hit_rate, 2),
        "cache_ttl_seconds": self._schema_cache_ttl,
        "is_cached": self._schema_cache is not None,
        "cache_age_seconds": cache_age,
      }
