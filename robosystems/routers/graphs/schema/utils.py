"""Shared utilities for schema endpoints."""

import asyncio
from typing import Any

from robosystems.logger import logger
from robosystems.middleware.robustness import (
  CircuitBreakerManager,
  TimeoutCoordinator,
)

# Schema queries for runtime graph inspection
SCHEMA_QUERIES = {
  "tables": "CALL SHOW_TABLES() RETURN *",
  "node_labels": """CALL SHOW_TABLES()
    WHERE type = 'NODE'
    RETURN name as label""",
  "relationship_types": """CALL SHOW_TABLES()
    WHERE type = 'REL'
    RETURN name as rel_type""",
  "node_properties": """MATCH (n)
WITH labels(n) as labels, keys(n) as props
UNWIND labels as label
UNWIND props as prop
RETURN DISTINCT label, collect(DISTINCT prop) as properties
LIMIT 100""",
}

# Initialize robustness components (shared across schema endpoints)
circuit_breaker = CircuitBreakerManager()
timeout_coordinator = TimeoutCoordinator()


async def get_schema_info(repository: Any) -> dict[str, Any]:
  """
  Retrieve schema information from the graph database.

  Args:
      repository: The graph repository instance

  Returns:
      Dictionary containing schema information
  """
  schema_info = {"node_labels": [], "relationship_types": [], "node_properties": {}}

  try:
    # Execute LadybugDB-specific schema queries
    if hasattr(repository, "execute_query") and asyncio.iscoroutinefunction(
      repository.execute_query
    ):
      # Get all tables first
      tables_result = await repository.execute_query(SCHEMA_QUERIES["tables"])
    else:
      tables_result = repository.execute_query(SCHEMA_QUERIES["tables"])

    # Process tables to separate nodes and relationships
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

    # For node properties, we need to query each table individually
    # This is a limitation of LadybugDB compared to Neo4j
    # CALL TABLE_INFO is a catalog query (metadata only), so it's fast even for many tables
    for node_label in node_tables:
      try:
        # Get table info for each node type
        table_info_query = f"CALL TABLE_INFO('{node_label}') RETURN *"
        if hasattr(repository, "execute_query") and asyncio.iscoroutinefunction(
          repository.execute_query
        ):
          columns_result = await repository.execute_query(table_info_query)
        else:
          columns_result = repository.execute_query(table_info_query)

        # Extract property names
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
      # Fallback to just getting table names
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
