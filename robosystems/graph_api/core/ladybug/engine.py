"""
LadybugDB database engine implementation

This module provides LadybugDB database integration for the application.
"""

import re
import time
from typing import List, Dict, Any, Optional
from pathlib import Path
import real_ladybug as lbug
from robosystems.middleware.graph.base import GraphEngineInterface, GraphOperation
from robosystems.logger import logger, log_db_query, log_app_error
from robosystems.config import env


class ConnectionError(Exception):
  """Raised when database connection fails."""

  pass


class QueryError(Exception):
  """Raised when query execution fails."""

  pass


class Engine(GraphEngineInterface):
  """
  LadybugDB database engine implementation.

  Provides a graph database implementation of the GraphEngineInterface.
  """

  def __init__(self, database_path: str, read_only: bool = False):
    """
    Initialize LadybugDB database engine.

    Args:
        database_path: Path to the graph database file
        read_only: Whether to open in read-only mode
    """
    self.database_path = database_path
    self.read_only = read_only
    self._db = None
    self._conn = None

    # Ensure database directory exists
    db_dir = Path(database_path).parent
    db_dir.mkdir(parents=True, exist_ok=True)

    self._connect()

  @property
  def conn(self):
    """Get the LadybugDB connection object."""
    return self._conn

  @property
  def db(self):
    """Get the LadybugDB database object."""
    return self._db

  def _connect(self) -> None:
    """Establish connection to LadybugDB database."""
    try:
      logger.debug(f"Connecting to LadybugDB database: {self.database_path}")

      # Determine checkpoint threshold based on database
      # SEC has huge tables (Fact, Association) that need more frequent checkpoints
      db_name = Path(self.database_path).stem
      if db_name == "sec":
        checkpoint_threshold = 134217728  # 128MB for SEC
      else:
        checkpoint_threshold = 536870912  # 512MB for regular databases

      # Create database with auto_checkpoint enabled
      # This is CRITICAL for ensuring data is visible after ingestion
      self._db = lbug.Database(
        self.database_path,
        read_only=self.read_only,
        auto_checkpoint=True,
        checkpoint_threshold=checkpoint_threshold,
      )
      self._conn = lbug.Connection(self._db)
      logger.info(f"Successfully connected to LadybugDB database: {self.database_path}")
    except Exception as e:
      logger.error(f"Failed to connect to LadybugDB database {self.database_path}: {e}")
      raise ConnectionError(f"Failed to connect to LadybugDB database: {e}")

  def set_query_timeout(self, timeout_ms: int) -> None:
    """
    Set the query timeout for this connection.

    Args:
        timeout_ms: Timeout in milliseconds (e.g., 120000 for 2 minutes)

    Raises:
        ConnectionError: If database connection is not available
    """
    if not self._conn:
      raise ConnectionError("Database connection is not available")

    try:
      self._conn.execute(f"CALL timeout={timeout_ms};")
      logger.debug(f"Set query timeout to {timeout_ms}ms")
    except Exception as e:
      logger.warning(f"Failed to set query timeout: {e}")
      # Don't raise - timeout is non-critical

  def execute_query(
    self, cypher: str, params: Optional[Dict[str, Any]] = None
  ) -> List[Dict[str, Any]]:
    """
    Execute a single Cypher query and return results.

    Args:
        cypher: The Cypher query string
        params: Optional parameters for the query

    Returns:
        List of result records as dictionaries

    Raises:
        QueryError: If query execution fails
        ConnectionError: If database connection is lost
    """
    if params is None:
      params = {}

    # Validate inputs
    if not isinstance(cypher, str) or not cypher.strip():
      raise QueryError("Query string cannot be empty")

    if not isinstance(params, dict):
      raise QueryError("Parameters must be a dictionary")

    # Check connection health
    if not self._conn:
      raise ConnectionError("Database connection is not available")

    # Start timing for performance logging
    start_time = time.time()

    try:
      logger.debug(f"Executing graph query: {cypher[:100]}...")

      # Validate parameters before execution
      self._validate_parameters(params)

      # Use LadybugDB's native parameter binding (much safer than string substitution)
      if params:
        logger.debug(f"Executing query with {len(params)} parameters")
        result = self._conn.execute(cypher, params)
      else:
        result = self._conn.execute(cypher)

      # Convert result to standard format
      result_dict_list = self._convert_result_to_dict_list(result)

      # Log structured query performance data
      duration_ms = (time.time() - start_time) * 1000
      query_type = self._get_query_type(cypher)

      log_db_query(
        database=str(self.database_path),
        query_type=query_type,
        duration_ms=duration_ms,
        row_count=len(result_dict_list),
        # Note: user_id/entity_id would come from request context when implemented
      )

      return result_dict_list

    except Exception as e:
      # Calculate duration for error logging
      duration_ms = (time.time() - start_time) * 1000 if "start_time" in locals() else 0

      # Use structured error logging
      log_app_error(
        error=e,
        component="lbug_engine",
        action="execute_query",
        error_category="database",
        metadata={
          "query": cypher[:200],  # Truncate for log size
          "params": params,
          "duration_ms": duration_ms,
          "database": str(self.database_path),
        },
      )

      # Re-raise with more specific error types
      if "connection" in str(e).lower():
        raise ConnectionError(f"Database connection error: {e}")
      else:
        raise QueryError(f"Graph query failed: {e}")

  def _get_query_type(self, cypher: str) -> str:
    """Determine the type of Cypher query for logging purposes."""
    cypher_upper = cypher.strip().upper()

    if cypher_upper.startswith("MATCH"):
      return "READ"
    elif cypher_upper.startswith(("CREATE", "MERGE")):
      return "WRITE"
    elif cypher_upper.startswith("COPY"):
      return "COPY"
    elif cypher_upper.startswith("DELETE"):
      return "DELETE"
    elif cypher_upper.startswith("SET"):
      return "UPDATE"
    elif cypher_upper.startswith(("CALL", "RETURN")):
      return "PROCEDURE"
    elif cypher_upper.startswith("EXPLAIN"):
      return "EXPLAIN"
    else:
      return "OTHER"

  def _validate_parameters(self, params: Dict[str, Any]) -> None:
    """
    Validate query parameters for graph databases's native parameter binding.

    Args:
        params: Parameters dictionary to validate

    Raises:
        QueryError: If parameters are invalid

    NOTE: With native parameter binding, LadybugDB handles type conversion and escaping,
    but we still validate for reasonable limits and basic security.
    """
    if not params:
      return

    MAX_DEPTH = 3
    MAX_ARRAY_SIZE = 1000
    MAX_OBJECT_KEYS = 100

    def validate_value(key: str, value: Any, depth: int = 0) -> None:
      """Recursively validate parameter values with depth limiting."""
      if depth > MAX_DEPTH:
        raise QueryError(f"Parameter '{key}' nesting too deep (max depth: {MAX_DEPTH})")

      # Check for excessively long string values (DoS protection)
      if isinstance(value, str) and len(value) > 10000:
        raise QueryError(
          f"Parameter '{key}' value too long: {len(value)} characters (max 10000)"
        )

      # Validate arrays
      if isinstance(value, (list, tuple)):
        if len(value) > MAX_ARRAY_SIZE:
          raise QueryError(
            f"Parameter '{key}' array too large: {len(value)} items (max {MAX_ARRAY_SIZE})"
          )
        for i, item in enumerate(value):
          validate_value(f"{key}[{i}]", item, depth + 1)

      # Validate objects/dicts
      elif isinstance(value, dict):
        if len(value) > MAX_OBJECT_KEYS:
          raise QueryError(
            f"Parameter '{key}' object too large: {len(value)} keys (max {MAX_OBJECT_KEYS})"
          )
        for k, v in value.items():
          # Validate dict keys
          if not isinstance(k, str):
            raise QueryError(f"Parameter '{key}' has non-string key: {type(k)}")
          if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", k):
            raise QueryError(f"Invalid key in parameter '{key}': {k}")
          validate_value(f"{key}.{k}", v, depth + 1)

      # Basic type validation
      else:
        allowed_types = (str, int, float, bool, type(None))
        if not isinstance(value, allowed_types):
          raise QueryError(
            f"Parameter '{key}' has unsupported type: {type(value).__name__}"
          )

    for key, value in params.items():
      # Validate parameter name (alphanumeric + underscore only)
      if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", key):
        raise QueryError(
          f"Invalid parameter name: {key}. Must be alphanumeric with underscores only."
        )

      # Recursively validate the value
      validate_value(key, value)

  def execute_single(
    self, cypher: str, params: Optional[Dict[str, Any]] = None
  ) -> Optional[Dict[str, Any]]:
    """
    Execute a query expecting a single result.

    Args:
        cypher: The Cypher query string
        params: Optional parameters for the query

    Returns:
        Single result record or None
    """
    results = self.execute_query(cypher, params)
    if not results:
      return None
    if len(results) > 1:
      logger.warning(f"Query returned {len(results)} results, expected 1")
    return results[0]

  def execute_transaction(
    self, operations: List[GraphOperation]
  ) -> List[List[Dict[str, Any]]]:
    """
    Execute multiple operations in a single transaction.

    Note: LadybugDB doesn't have explicit transactions like traditional databases, but we can
    simulate this by executing operations sequentially and rolling back
    on failure.

    Args:
        operations: List of GraphOperation objects

    Returns:
        List of result lists for each operation
    """
    logger.debug(f"Starting graph transaction with {len(operations)} operations")

    results = []
    try:
      for i, op in enumerate(operations):
        logger.debug(
          f"Executing operation {i + 1}/{len(operations)}: {op.description or 'unnamed'}"
        )
        result = self.execute_query(op.cypher, op.params)
        results.append(result)

      return results

    except Exception as e:
      logger.error(f"Graph transaction failed: {e}")
      logger.error(f"Operations: {[op.description for op in operations]}")
      # Note: LadybugDB doesn't support rollback like traditional databases
      raise

  def health_check(self) -> Dict[str, Any]:
    """
    Perform a health check on the database connection.

    Returns:
        Health status information
    """
    try:
      result = self.execute_single("RETURN 1 as test")
      return {
        "database": self.database_path,
        "engine": "ladybug",
        "status": "healthy",
        "test_result": result["test"] if result else None,
        "read_only": self.read_only,
      }
    except Exception as e:
      return {
        "database": self.database_path,
        "engine": "ladybug",
        "status": "unhealthy",
        "error": str(e),
      }

  def close(self) -> None:
    """Close the database connection."""
    try:
      if self._conn:
        logger.debug(f"Closing graph connection: {self.database_path}")
        self._conn.close()
        self._conn = None
      if self._db:
        self._db.close()
        self._db = None
    except Exception as e:
      logger.warning(f"Error closing graph connection: {e}")

  # NOTE: _format_query_with_params method removed - we now use LadybugDB's native parameter binding
  # which is much safer than manual string substitution and prevents injection attacks

  def _convert_result_to_dict_list(self, result) -> List[Dict[str, Any]]:
    """
    Convert graph query result to list of dictionaries.

    This method converts the result format to match the format
    expected by the application.
    """
    if not result:
      return []

    try:
      # Get column names
      columns = result.get_column_names()

      # Get all rows
      rows = []
      while result.has_next():
        row_data = result.get_next()
        row_dict = {}

        for i, col_name in enumerate(columns):
          if i < len(row_data):
            row_dict[col_name] = row_data[i]
          else:
            row_dict[col_name] = None

        rows.append(row_dict)

      return rows

    except Exception as e:
      logger.error(f"Failed to convert graph result: {e}")
      return []

  def __enter__(self):
    """Context manager entry."""
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    """Context manager exit."""
    self.close()


class Repository:
  """
  Graph repository that provides the GraphRepository interface.

  This class provides graph database operations.
  """

  def __init__(
    self,
    database_path: str,
    read_only: bool = False,
    validate_schema: Optional[bool] = None,
  ):
    """
    Initialize graph repository.

    Args:
        database_path: Path to the graph database
        read_only: Whether to open in read-only mode
        validate_schema: Whether to validate schema
    """
    self.database_path = database_path
    self.read_only = read_only

    # Schema validation configuration
    if validate_schema is None:
      self.validate_schema = env.ENVIRONMENT.lower() in ("dev", "test", "staging")
    else:
      self.validate_schema = validate_schema

    # Track if we've already logged the schema validation warning
    self._schema_warning_logged = False

    self.engine = Engine(database_path, read_only)

  def set_query_timeout(self, timeout_ms: int) -> None:
    """
    Set the query timeout for this connection.

    Args:
        timeout_ms: Timeout in milliseconds (e.g., 120000 for 2 minutes)
    """
    self.engine.set_query_timeout(timeout_ms)

  def execute_query(
    self, cypher: str, params: Optional[Dict[str, Any]] = None
  ) -> List[Dict[str, Any]]:
    """Execute a single Cypher query and return results."""
    if (
      self.validate_schema
      and self._is_write_operation(cypher)
      and not self._schema_warning_logged
    ):
      logger.warning("Schema validation not yet implemented for graph database")
      self._schema_warning_logged = True

    return self.engine.execute_query(cypher, params)

  def execute_single(
    self, cypher: str, params: Optional[Dict[str, Any]] = None
  ) -> Optional[Dict[str, Any]]:
    """Execute a query expecting a single result."""
    return self.engine.execute_single(cypher, params)

  def execute_transaction(
    self, operations: List[GraphOperation]
  ) -> List[List[Dict[str, Any]]]:
    """Execute multiple operations in a single transaction."""
    return self.engine.execute_transaction(operations)

  def count_nodes(self, label: str, filters: Optional[Dict[str, Any]] = None) -> int:
    """Count nodes with optional filters."""
    where_clause = ""
    params = {}

    if filters:
      conditions = []
      for key, value in filters.items():
        param_key = f"filter_{key}"
        conditions.append(f"n.{key} = ${param_key}")
        params[param_key] = value
      where_clause = " WHERE " + " AND ".join(conditions)

    cypher = f"MATCH (n:{label}){where_clause} RETURN count(n) as count"
    result = self.execute_single(cypher, params)
    return result["count"] if result else 0

  def node_exists(self, label: str, filters: Dict[str, Any]) -> bool:
    """Check if a node exists with given filters."""
    try:
      return self.count_nodes(label, filters) > 0
    except QueryError as e:
      # Handle case where table doesn't exist yet (empty database)
      if "does not exist" in str(e):
        logger.info(
          f"Table {label} does not exist, returning False (expected for new database)"
        )
        return False
      # Re-raise other query errors
      raise

  def execute(
    self, cypher: str, params: Optional[Dict[str, Any]] = None
  ) -> List[Dict[str, Any]]:
    """Alias for execute_query for backward compatibility."""
    return self.execute_query(cypher, params)

  def health_check(self) -> Dict[str, Any]:
    """Perform a health check on the database connection."""
    return self.engine.health_check()

  def close(self) -> None:
    """Close the database connection."""
    self.engine.close()

  def _is_write_operation(self, cypher: str) -> bool:
    """Check if a Cypher query is a write operation using secure detection."""
    from robosystems.security.cypher_analyzer import is_write_operation

    return is_write_operation(cypher)

  def __enter__(self):
    """Context manager entry."""
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    """Context manager exit."""
    self.close()
