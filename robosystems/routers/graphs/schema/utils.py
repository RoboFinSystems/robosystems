"""Shared utilities for schema endpoints."""

import asyncio
from typing import Any

from robosystems.logger import logger
from robosystems.middleware.robustness import (
  CircuitBreakerManager,
  TimeoutCoordinator,
)

SCHEMA_QUERIES = {
  "tables": "CALL SHOW_TABLES() RETURN *",
}

circuit_breaker = CircuitBreakerManager()
timeout_coordinator = TimeoutCoordinator()


async def get_schema_info(repository: Any) -> dict[str, Any]:
  """Retrieve node labels, relationship types, and node properties from the
  graph database."""
  schema_info = {"node_labels": [], "relationship_types": [], "node_properties": {}}

  try:
    if hasattr(repository, "execute_query") and asyncio.iscoroutinefunction(
      repository.execute_query
    ):
      tables_result = await repository.execute_query(SCHEMA_QUERIES["tables"])
    else:
      tables_result = repository.execute_query(SCHEMA_QUERIES["tables"])

    node_tables = []
    rel_tables = []

    for table in tables_result:
      table_name = table.get("name", "")
      table_type = table.get("type", "")

      if table_type == "NODE":
        node_tables.append(table_name)
      elif table_type == "REL":
        rel_tables.append(table_name)

    schema_info["node_labels"] = node_tables
    schema_info["relationship_types"] = rel_tables

    # LadybugDB exposes properties through per-table catalog lookups; TABLE_INFO
    # is metadata-only, so this stays fast for many tables.
    for node_label in node_tables:
      try:
        table_info_query = f"CALL TABLE_INFO('{node_label}') RETURN *"
        if hasattr(repository, "execute_query") and asyncio.iscoroutinefunction(
          repository.execute_query
        ):
          columns_result = await repository.execute_query(table_info_query)
        else:
          columns_result = repository.execute_query(table_info_query)

        properties = [col.get("name", "") for col in columns_result if col.get("name")]
        if properties:
          schema_info["node_properties"][node_label] = properties
      except Exception as e:
        logger.debug(f"Failed to get properties for table {node_label}: {e}")
        continue

  except Exception as e:
    logger.warning(f"Failed to get complete schema info: {e!s}")
    # Try fallback with simple queries
    try:
      if hasattr(repository, "execute_query") and asyncio.iscoroutinefunction(
        repository.execute_query
      ):
        tables_result = await repository.execute_query("CALL SHOW_TABLES() RETURN *")
      else:
        tables_result = repository.execute_query("CALL SHOW_TABLES() RETURN *")

      for table in tables_result:
        if table.get("type") == "NODE":
          schema_info["node_labels"].append(table.get("name", ""))
        elif table.get("type") == "REL":
          schema_info["relationship_types"].append(table.get("name", ""))
    except Exception:
      pass  # Return partial schema info

  return schema_info
