"""
Database schema management endpoints for Graph API.

This module provides endpoints for installing and retrieving
database schemas.
"""

import re
from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Path
from fastapi import status as http_status

from robosystems.graph_api.models.database import (
  SchemaInstallRequest,
  SchemaInstallResponse,
  QueryRequest,
)
from robosystems.graph_api.core.cluster_manager import get_cluster_service
from robosystems.graph_api.core.utils import validate_database_name
from robosystems.logger import logger

router = APIRouter(prefix="/databases", tags=["Graph Schema"])


# DDL Statement validation patterns
ALLOWED_DDL_PATTERNS = {
  # Node table creation (with optional IF NOT EXISTS)
  r"^\s*CREATE\s+NODE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?\w+\s*\(",
  # Relationship table creation (with optional IF NOT EXISTS)
  r"^\s*CREATE\s+REL\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?\w+\s*\(",
  # Index creation
  r"^\s*CREATE\s+INDEX\s+\w+\s+ON\s+\w+",
  # Comments
  r"^\s*COMMENT\s+ON\s+(TABLE|COLUMN)\s+\w+",
}

# Dangerous DDL patterns to explicitly block
DANGEROUS_DDL_PATTERNS = {
  r"DROP\s+(TABLE|DATABASE|SCHEMA|INDEX)",
  r"ALTER\s+(TABLE|DATABASE|SCHEMA)",
  r"TRUNCATE\s+TABLE",
  r"DELETE\s+FROM",
  r"UPDATE\s+\w+\s+SET",
  r"GRANT\s+\w+",
  r"REVOKE\s+\w+",
  r"CREATE\s+(USER|ROLE)",
  r"CALL\s+\w+\.",  # Block system calls
  r"LOAD\s+CSV",  # Block file operations
  r"COPY\s+FROM",  # Block file operations
}


def validate_ddl_statement(statement: str) -> bool:
  """
  Validate DDL statement for security.

  Args:
      statement: DDL statement to validate

  Returns:
      True if statement is safe, False otherwise
  """
  statement_upper = statement.upper().strip()

  # Check for dangerous patterns first
  for pattern in DANGEROUS_DDL_PATTERNS:
    if re.search(pattern, statement_upper):
      logger.warning(f"Blocked dangerous DDL pattern: {pattern}")
      return False

  # Check if statement matches allowed patterns
  for pattern in ALLOWED_DDL_PATTERNS:
    if re.match(pattern, statement_upper):
      return True

  # If no patterns match, reject for safety
  logger.warning(f"DDL statement doesn't match allowed patterns: {statement[:100]}...")
  return False


def escape_identifier(identifier: str) -> str:
  """
  Safely escape an identifier for use in queries.

  Args:
      identifier: The identifier to escape

  Returns:
      Escaped identifier safe for query construction
  """
  # Remove any existing quotes and whitespace
  cleaned = identifier.strip().strip("'\"")

  # Validate identifier contains only safe characters
  if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", cleaned):
    raise ValueError(f"Invalid identifier: {identifier}")

  return cleaned


@router.post("/{graph_id}/schema", response_model=SchemaInstallResponse)
async def install_schema(
  request: SchemaInstallRequest,
  graph_id: str = Path(..., description="Graph database identifier"),
  cluster_service=Depends(get_cluster_service),
) -> SchemaInstallResponse:
  """
  Install custom schema on an existing database.

  Executes DDL commands to install or update the schema of a database.
  This is useful for adding new node/relationship types or properties
  after database creation.
  """
  if cluster_service.read_only:
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Schema installation not allowed on read-only nodes",
    )

  # Validate database exists
  if graph_id not in cluster_service.db_manager.list_databases():
    raise HTTPException(
      status_code=http_status.HTTP_404_NOT_FOUND,
      detail=f"Database {graph_id} not found",
    )

  # Validate request
  if request.type not in ["custom", "ddl"]:
    raise HTTPException(
      status_code=http_status.HTTP_400_BAD_REQUEST,
      detail="Schema type must be 'custom' or 'ddl'",
    )

  if not request.ddl:
    raise HTTPException(
      status_code=http_status.HTTP_400_BAD_REQUEST,
      detail="DDL commands are required",
    )

  try:
    # Use connection with proper resource management and transaction
    with cluster_service.db_manager.get_connection(graph_id, read_only=False) as conn:
      # Split DDL into statements
      statements = [stmt.strip() for stmt in request.ddl.split(";") if stmt.strip()]
      executed_count = 0

      logger.info(
        f"Installing schema on database {graph_id}: {len(statements)} statements"
      )

      # Validate all statements before execution
      for i, statement in enumerate(statements):
        if not validate_ddl_statement(statement):
          error_msg = f"DDL statement {i + 1} contains forbidden operations or doesn't match allowed patterns"
          logger.error(f"Blocked DDL execution: {error_msg}")
          raise HTTPException(
            status_code=http_status.HTTP_400_BAD_REQUEST,
            detail=error_msg,
          )

      # Begin transaction for atomic schema installation
      try:
        # Execute BEGIN TRANSACTION
        conn.execute("BEGIN TRANSACTION")

        # Execute validated statements within transaction
        for i, statement in enumerate(statements):
          try:
            conn.execute(statement)
            executed_count += 1
            logger.debug(
              f"Executed DDL statement {i + 1}/{len(statements)} on {graph_id}"
            )
          except Exception as e:
            # Rollback on any error
            try:
              conn.execute("ROLLBACK")
            except Exception:
              pass  # Rollback might fail if connection is broken

            error_msg = f"Failed to execute DDL statement {i + 1}: {str(e)}"
            logger.error(error_msg)
            return SchemaInstallResponse(
              success=False,
              message=error_msg,
              statements_executed=0,  # 0 because we rolled back
            )

        # Commit transaction if all statements succeeded
        conn.execute("COMMIT")

        # Log metadata if provided
        if request.metadata:
          logger.info(f"Schema metadata for {graph_id}: {request.metadata}")

        return SchemaInstallResponse(
          success=True,
          message=f"Schema installed successfully on database {graph_id}",
          statements_executed=executed_count,
        )

      except Exception:
        # Ensure rollback on any unexpected error
        try:
          conn.execute("ROLLBACK")
        except Exception:
          pass
        raise

  except Exception as e:
    logger.error(f"Schema installation failed for {graph_id}: {e}")
    return SchemaInstallResponse(
      success=False,
      message=f"Schema installation failed: {str(e)}",
      statements_executed=0,
    )


@router.get("/{graph_id}/schema")
async def get_schema(
  graph_id: str = Path(..., description="Graph database identifier"),
  cluster_service=Depends(get_cluster_service),
) -> Dict[str, Any]:
  """
  Get database schema information.

  Retrieves the complete schema of a database including all node tables,
  relationship tables, and their properties.
  """
  # Validate database name
  validated_graph_id = validate_database_name(graph_id)

  # Check if database exists
  if validated_graph_id not in cluster_service.db_manager.list_databases():
    raise HTTPException(
      status_code=http_status.HTTP_404_NOT_FOUND,
      detail=f"Database '{validated_graph_id}' not found",
    )

  try:
    # Get schema information using Kuzu's SHOW_TABLES
    schema_info = {
      "database": validated_graph_id,
      "tables": [],
      "node_tables": [],
      "rel_tables": [],
    }

    # Execute SHOW_TABLES query with explicit column names
    query_request = QueryRequest(
      database=validated_graph_id,
      cypher="CALL SHOW_TABLES() RETURN id, name, type, comment",
    )

    result = cluster_service.execute_query(query_request)

    # Process the results
    for row in result.data:
      table_info = {
        "id": row.get("id"),
        "name": row.get("name"),
        "type": row.get("type"),
        "comment": row.get("comment"),
      }

      schema_info["tables"].append(table_info)

      # Categorize by type
      table_type = table_info.get("type") or ""
      if table_type:
        table_type = table_type.upper()
        if table_type == "NODE":
          schema_info["node_tables"].append(table_info["name"])
        elif table_type == "REL":
          schema_info["rel_tables"].append(table_info["name"])

    # Try to get additional schema details for each table
    for table in schema_info["tables"]:
      table_name = table.get("name", "")
      try:
        # Safely escape table name to prevent SQL injection
        safe_table_name = escape_identifier(table_name)

        # Get table properties using CALL TABLE_INFO with escaped identifier
        # TABLE_INFO returns: index, name, type, default, isPrimaryKey
        info_request = QueryRequest(
          database=validated_graph_id,
          cypher=f"CALL TABLE_INFO('{safe_table_name}') RETURN *",
        )
        info_result = cluster_service.execute_query(info_request)

        properties = []
        for prop_row in info_result.data:
          # The columns are returned as an array in the format:
          # [index, name, type, default, isPrimaryKey]
          if isinstance(prop_row, dict) and len(prop_row) > 0:
            # When using RETURN *, the result is a dict with a single key
            values = list(prop_row.values())[0] if len(prop_row) == 1 else prop_row
            if isinstance(values, list) and len(values) >= 3:
              properties.append(
                {
                  "name": values[1],  # property name
                  "type": values[2],  # property type
                }
              )
          elif isinstance(prop_row, list) and len(prop_row) >= 3:
            properties.append(
              {
                "name": prop_row[1],  # property name
                "type": prop_row[2],  # property type
              }
            )

        table["properties"] = properties

      except ValueError:
        # Invalid table name, skip this table
        logger.warning(f"Skipping table with invalid name: {table_name}")
        table["properties"] = []
      except Exception as e:
        # If TABLE_INFO fails, just continue without properties
        logger.debug(f"Could not get properties for table {table_name}: {e}")
        table["properties"] = []

    return schema_info

  except Exception as e:
    logger.error(f"Failed to get schema for database {validated_graph_id}: {e}")
    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to retrieve schema: {str(e)}",
    )
