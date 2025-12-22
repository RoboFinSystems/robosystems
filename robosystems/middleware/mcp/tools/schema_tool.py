"""
Schema Tool - Retrieves the complete database schema.
"""

import time
from threading import RLock
from typing import Any

from robosystems.logger import logger

from .base_tool import BaseTool


class SchemaTool(BaseTool):
  """
  Tool for retrieving database schema information.
  """

  def __init__(self, client):
    super().__init__(client)
    # Schema caching for performance
    self._schema_cache = None
    self._schema_cache_time = None
    self._schema_cache_ttl = 300  # 5 minutes
    self._cache_lock = RLock()

    # Cache statistics
    self._cache_hits = 0
    self._cache_misses = 0

  def get_tool_definition(self) -> dict[str, Any]:
    """Get the tool definition for schema retrieval."""
    return {
      "name": "get-graph-schema",
      "description": """Get the complete database schema showing all node types, properties, and relationships.

**WHEN TO USE:**
- Always start with this tool to understand the data structure
- Before writing queries to verify property names and relationships
- When exploring what data is available in the graph
- To understand custom schema definitions

**RETURNS:** Comprehensive schema information including:
- **Node Types**: Label, properties with data types, primary keys
- **Relationships**: Label, properties, source/target nodes
- **Property Details**: Name, data type (STRING, INT64, DOUBLE, etc.), nullable flags
- **Metadata**: Descriptions and additional context when available

**SCHEMA TYPES:**
The database may use different schema configurations:
1. **Base Schema**: Standard RoboSystems entities (Entity, User, etc.)
2. **Extended Schema**: Base + domain-specific extensions
3. **Custom Schema**: Custom entities and relationships
4. **Hybrid Schema**: Custom schema extending the base

**DATA TYPES:**
Common property types you'll encounter:
- STRING: Text values
- INT64/INT32: Integer numbers
- DOUBLE/FLOAT: Decimal numbers
- BOOLEAN: True/false values
- TIMESTAMP: Date/time values
- JSON: Complex nested data

**USAGE TIPS:**
- Look for node labels ending in specific patterns (Entity, Fact, etc.)
- Check relationship direction: ->() vs <-()
- Property names are case-sensitive
- Use count properties to understand data volume""",
      "inputSchema": {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> list[dict[str, Any]]:
    """Execute the schema tool with caching."""
    self._log_tool_execution("get-graph-schema", arguments)

    # Check cache first
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

    # Cache miss - fetch fresh schema
    with self._cache_lock:
      self._cache_misses += 1
      logger.debug(
        f"Schema cache miss (hits: {self._cache_hits}, misses: {self._cache_misses})"
      )

    try:
      # Fetch schema from client
      schema = await self.client.get_schema()

      # Cache the result
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
