"""
Kuzu cluster management service.

This module provides the core cluster service that manages multiple Kuzu databases
on a single node, handling database operations, health monitoring, and metrics.
"""

import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from datetime import datetime
from typing import List, Optional

import psutil
from fastapi import HTTPException, status

from robosystems.config import env
from .database_manager import KuzuDatabaseManager
from .metrics_collector import KuzuMetricsCollector
from robosystems.graph_api.models.database import QueryRequest, QueryResponse
from robosystems.graph_api.models.cluster import (
  ClusterHealthResponse,
  ClusterInfoResponse,
)
from robosystems.graph_api.core.utils import (
  validate_database_name,
  validate_query_parameters,
)
from robosystems.middleware.graph.clusters import NodeType, RepositoryType
from robosystems.models.api.graph import translate_neo4j_to_kuzu
from robosystems.logger import logger
from robosystems.exceptions import (
  ConfigurationError,
)

# OpenTelemetry imports - conditional based on OTEL_ENABLED


# Create a no-op tracer if OTEL is disabled
class NoOpSpan:
  """No-op span for when tracing is disabled."""

  def __enter__(self):
    return self

  def __exit__(self, *args):
    pass

  def set_attribute(self, key, value):
    pass


class NoOpTracer:
  """No-op tracer for when tracing is disabled."""

  def start_as_current_span(self, name, **kwargs):
    return NoOpSpan()


# Only import real tracer if OTEL is enabled

if env.OTEL_ENABLED:
  try:
    from robosystems.middleware.otel import get_tracer

    tracer = get_tracer(__name__)
  except ImportError:
    tracer = NoOpTracer()
else:
  tracer = NoOpTracer()

try:
  from importlib.metadata import version

  __version__ = version("robosystems-service")
except Exception:
  __version__ = "1.0.0"


def _extract_column_aliases_from_cypher(cypher_query: str) -> List[str]:
  """
  Extract column aliases from RETURN clause in Cypher query.

  This function parses the RETURN clause to preserve custom column aliases
  instead of relying on Kuzu's internal generic column names (col0, col1, etc.).

  Args:
      cypher_query: The Cypher query to parse

  Returns:
      List of column aliases/names from the RETURN clause

  Examples:
      "RETURN c as entity" -> ["entity"]
      "RETURN c.name, c.age as user_age" -> ["c.name", "user_age"]
      "RETURN count(c) as total" -> ["total"]
  """
  try:
    # Remove line breaks and normalize whitespace
    query = re.sub(r"\s+", " ", cypher_query.strip())

    # Match RETURN clause (case insensitive)
    # Look for RETURN followed by expressions, stopping at clause keywords or end
    return_match = re.search(
      r"\bRETURN\s+(.+?)(?:\s+(?:WHERE|ORDER|LIMIT|SKIP|WITH|UNION)|$)",
      query,
      re.IGNORECASE,
    )
    if not return_match:
      return []

    return_clause = return_match.group(1).strip()

    # Split by comma, handling simple cases for now
    # Note: This doesn't handle complex nested expressions with commas,
    # but covers the majority of use cases
    column_expressions = [expr.strip() for expr in return_clause.split(",")]

    aliases = []
    for expr in column_expressions:
      # Check for explicit AS clause (case insensitive)
      as_match = re.search(r"^(.+?)\s+(?:as|AS)\s+([a-zA-Z_][a-zA-Z0-9_]*)$", expr)
      if as_match:
        # Has explicit alias
        aliases.append(as_match.group(2))
      else:
        # No alias, use the expression itself (cleaned up)
        # Remove extra whitespace and use as column name
        clean_expr = re.sub(r"\s+", " ", expr.strip())
        aliases.append(clean_expr)

    return aliases
  except Exception as e:
    # If parsing fails, return empty list to fall back to generic names
    logger.debug(f"Failed to parse RETURN clause from query: {e}")
    return []


def validate_cypher_query(cypher: str) -> None:
  """
  Validate Cypher query for basic safety checks.

  Args:
      cypher: Cypher query to validate

  Raises:
      HTTPException: If query contains forbidden keywords or is invalid
  """
  # Basic injection prevention
  forbidden_keywords = [
    "CREATE DATABASE",
    "DROP DATABASE",
    "CREATE USER",
    "DROP USER",
    "ALTER USER",
    "SET PASSWORD",
    "GRANT",
    "REVOKE",
    "CALL DBMS",
    "CALL APOC",
    "LOAD CSV",
  ]

  # Check for empty query
  if not cypher or cypher.strip() == "":
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Query cannot be empty",
    )

  query_upper = cypher.upper()
  for keyword in forbidden_keywords:
    if keyword in query_upper:
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"Query contains forbidden keyword: {keyword}",
      )

  # Check query length
  from robosystems.config import env

  max_query_length = env.KUZU_MAX_QUERY_LENGTH
  if len(cypher) > max_query_length:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"Query too long (max {max_query_length} characters)",
    )


class KuzuClusterService:
  """Kuzu cluster service with multi-database support."""

  def __init__(
    self,
    base_path: str,
    max_databases: int = 200,
    read_only: bool = False,
    node_type: NodeType = NodeType.WRITER,
    repository_type: RepositoryType = RepositoryType.ENTITY,
  ):
    self.base_path = base_path
    self.max_databases = max_databases
    self.read_only = read_only
    self.node_type = node_type
    self.repository_type = repository_type
    self.start_time = time.time()
    self.last_activity: Optional[datetime] = None

    # Initialize database manager
    self.db_manager = KuzuDatabaseManager(base_path, max_databases, read_only=read_only)

    # Initialize metrics collector
    self.metrics_collector = KuzuMetricsCollector(
      base_path=base_path, node_type=node_type.value
    )

    # Validate configuration for node type
    self._validate_node_configuration()

    logger.info(
      f"Kuzu Cluster Service initialized: {base_path} (max: {max_databases}, read_only: {read_only})"
    )
    logger.info(f"Node type: {node_type}, Repository type: {repository_type}")

  def _validate_node_configuration(self):
    """Validate node configuration based on type."""
    if self.node_type == NodeType.WRITER:
      if self.read_only:
        raise ConfigurationError(
          config_key="node_type", reason="Writer nodes cannot be read-only"
        )
      # Writers can now handle both entity and shared repositories

    elif self.node_type == NodeType.SHARED_MASTER:
      if self.read_only:
        raise ConfigurationError(
          config_key="node_type", reason="Shared writer nodes cannot be read-only"
        )
      if self.repository_type != RepositoryType.SHARED:
        raise ValueError("Shared writer nodes must use shared repository type")
      if (
        self.max_databases > 5
      ):  # SEC and other shared repos are typically single databases
        logger.warning(
          f"Shared writer with {self.max_databases} max databases - consider reducing"
        )

  @property
  def node_id(self) -> str:
    """Generate meaningful node ID based on type."""
    return f"kuzu-{self.node_type.value}-{os.getpid()}"

  def get_uptime(self) -> float:
    """Get node uptime in seconds."""
    return time.time() - self.start_time

  def execute_query_streaming(self, request: QueryRequest, chunk_size: int = 1000):
    """
    Execute a query and yield results in chunks for streaming.

    Args:
        request: Query request
        chunk_size: Number of rows per chunk

    Yields:
        Dict containing chunk data
    """
    with tracer.start_as_current_span(
      "kuzu.execute_query_streaming",
      attributes={
        "database.name": request.database,
        "query.length": len(request.cypher),
        "query.has_parameters": bool(request.parameters),
        "chunk_size": chunk_size,
      },
    ) as span:
      start_time = time.time()

      try:
        # Validate inputs
        validated_graph_id = validate_database_name(request.database)
        validate_cypher_query(request.cypher)

        # Check database exists
        if validated_graph_id not in self.db_manager.list_databases():
          raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Database '{validated_graph_id}' not found",
          )

        # Translate Neo4j-style queries to Kuzu equivalents
        translated_cypher = translate_neo4j_to_kuzu(request.cypher)

        logger.debug(
          f"Streaming query on {validated_graph_id}: {translated_cypher[:100]}..."
        )

        # Use connection with proper resource management
        with self.db_manager.get_connection(
          validated_graph_id, read_only=self.read_only
        ) as conn:
          # Execute query with comprehensive error handling
          try:
            if request.parameters:
              query_result = conn.execute(translated_cypher, request.parameters)
            else:
              query_result = conn.execute(translated_cypher)

            # Handle case where execute returns a list of QueryResults
            if isinstance(query_result, list):
              if len(query_result) == 0:
                raise RuntimeError("Query returned no results")
              query_result = query_result[0]  # Use first result

          except RuntimeError as e:
            error_msg = str(e)
            # Handle specific Kuzu errors gracefully
            if "Binder exception" in error_msg:
              # Extract the specific binding error for cleaner logging
              logger.warning(
                f"Query binding error for {validated_graph_id}: {error_msg}"
              )
              # Yield error chunk without stack trace
              yield {
                "error": error_msg,
                "error_type": "BinderException",
                "chunk_index": 0,
                "is_last_chunk": True,
                "row_count": 0,
                "total_rows_sent": 0,
                "execution_time_ms": (time.time() - start_time) * 1000,
              }
              return
            elif "Parser exception" in error_msg:
              logger.warning(
                f"Query parsing error for {validated_graph_id}: {error_msg}"
              )
              yield {
                "error": error_msg,
                "error_type": "ParserException",
                "chunk_index": 0,
                "is_last_chunk": True,
                "row_count": 0,
                "total_rows_sent": 0,
                "execution_time_ms": (time.time() - start_time) * 1000,
              }
              return
            elif "Catalog exception" in error_msg:
              logger.warning(f"Catalog error for {validated_graph_id}: {error_msg}")
              yield {
                "error": error_msg,
                "error_type": "CatalogException",
                "chunk_index": 0,
                "is_last_chunk": True,
                "row_count": 0,
                "total_rows_sent": 0,
                "execution_time_ms": (time.time() - start_time) * 1000,
              }
              return
            else:
              # Other runtime errors
              logger.error(
                f"Query execution error for {validated_graph_id}: {error_msg}"
              )
              yield {
                "error": error_msg,
                "error_type": "RuntimeError",
                "chunk_index": 0,
                "is_last_chunk": True,
                "row_count": 0,
                "total_rows_sent": 0,
                "execution_time_ms": (time.time() - start_time) * 1000,
              }
              return
          except Exception as e:
            # Catch-all for unexpected errors
            logger.error(f"Unexpected query error for {validated_graph_id}: {str(e)}")
            yield {
              "error": str(e),
              "error_type": type(e).__name__,
              "chunk_index": 0,
              "is_last_chunk": True,
              "row_count": 0,
              "total_rows_sent": 0,
              "execution_time_ms": (time.time() - start_time) * 1000,
            }
            return

        # Get columns - try to extract aliases from query first
        columns = []
        extracted_aliases = _extract_column_aliases_from_cypher(translated_cypher)

        if hasattr(query_result, "get_schema"):
          schema = query_result.get_schema()
          schema_columns = [col for col in schema.keys()]

          # Use extracted aliases if available and count matches schema columns
          if extracted_aliases and len(extracted_aliases) == len(schema_columns):
            columns = extracted_aliases
          else:
            columns = schema_columns
        elif extracted_aliases:
          columns = extracted_aliases

        # Stream results in chunks
        chunk_index = 0
        total_rows_sent = 0
        rows_buffer = []

        while query_result.has_next():
          row = query_result.get_next()

          # Convert row to dict
          if columns:
            row_list = list(row)  # Ensure row is treated as a list
            row_dict = {
              columns[i]: row_list[i] if i < len(row_list) else None
              for i in range(len(columns))
            }
          else:
            row_dict = {f"col{i}": val for i, val in enumerate(row)}
            if not columns:  # Set columns on first row
              columns = list(row_dict.keys())

          rows_buffer.append(row_dict)

          # Yield chunk when buffer is full
          if len(rows_buffer) >= chunk_size:
            chunk_data = {
              "chunk_index": chunk_index,
              "data": rows_buffer,
              "columns": columns if chunk_index == 0 else [],
              "is_last_chunk": False,
              "row_count": len(rows_buffer),
              "total_rows_sent": total_rows_sent + len(rows_buffer),
            }
            yield chunk_data

            chunk_index += 1
            total_rows_sent += len(rows_buffer)
            rows_buffer = []

        # Yield final chunk with remaining rows
        if rows_buffer or chunk_index == 0:  # Always send at least one chunk
          execution_time = (time.time() - start_time) * 1000
          chunk_data = {
            "chunk_index": chunk_index,
            "data": rows_buffer,
            "columns": columns if chunk_index == 0 else [],
            "is_last_chunk": True,
            "row_count": len(rows_buffer),
            "total_rows_sent": total_rows_sent + len(rows_buffer),
            "execution_time_ms": execution_time,
            "database": validated_graph_id,
          }
          yield chunk_data

        self.last_activity = datetime.now()

        # Update metrics
        self.metrics_collector.record_query(
          database=validated_graph_id,
          duration_ms=(time.time() - start_time) * 1000,
          success=True,
        )

      except HTTPException:
        raise
      except Exception as e:
        logger.error(f"Streaming query failed on {request.database}: {e}")
        span.set_attribute("error", True)
        span.set_attribute("error.type", type(e).__name__)
        span.set_attribute("error.message", str(e))

        # Update metrics
        self.metrics_collector.record_query(
          database=request.database,
          duration_ms=(time.time() - start_time) * 1000,
          success=False,
        )
        raise

  def execute_query(self, request: QueryRequest) -> QueryResponse:
    """Execute a query against a specific database."""
    with tracer.start_as_current_span(
      "kuzu.execute_query",
      attributes={
        "database.name": request.database,
        "query.length": len(request.cypher),
        "query.has_parameters": bool(request.parameters),
      },
    ) as span:
      start_time = time.time()

      try:
        # Validate inputs for security
        validated_graph_id = validate_database_name(request.database)
        validate_cypher_query(request.cypher)
        validate_query_parameters(request.parameters)

        # Check if database exists
        if validated_graph_id not in self.db_manager.list_databases():
          span.set_attribute("error", True)
          span.set_attribute("error.type", "DatabaseNotFound")
          raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Database '{validated_graph_id}' not found",
          )

        # Translate Neo4j-style queries to Kuzu equivalents
        translated_cypher = translate_neo4j_to_kuzu(request.cypher)

        if translated_cypher != request.cypher:
          logger.info(
            f"Translated Neo4j query to Kuzu: {request.cypher[:100]}... -> {translated_cypher[:100]}..."
          )

        logger.debug(
          f"Executing query on {validated_graph_id}: {translated_cypher[:100]}..."
        )

        # Use connection with proper resource management
        with self.db_manager.get_connection(
          validated_graph_id, read_only=self.read_only
        ) as conn:
          # Execute query with proper thread-based timeout
          from robosystems.config import env

          query_timeout = env.KUZU_QUERY_TIMEOUT

          # Use ThreadPoolExecutor for proper timeout handling
          # This works across all platforms and doesn't interfere with signals
          def execute_query_with_params():
            """Execute the query in a separate thread for timeout control."""
            if request.parameters:
              return conn.execute(translated_cypher, request.parameters)
            else:
              return conn.execute(translated_cypher)

          try:
            # Execute the query with timeout using thread pool
            with ThreadPoolExecutor(max_workers=1) as executor:
              future = executor.submit(execute_query_with_params)
              try:
                query_result = future.result(timeout=query_timeout)

                # Handle case where execute returns a list of QueryResults
                if isinstance(query_result, list):
                  if len(query_result) == 0:
                    raise RuntimeError("Query returned no results")
                  query_result = query_result[0]  # Use first result

              except TimeoutError:
                # Query exceeded timeout
                span.set_attribute("error", True)
                span.set_attribute("error.type", "QueryTimeout")
                logger.warning(
                  f"Query timeout for {validated_graph_id} after {query_timeout} seconds"
                )
                # Try to cancel the future (though Kuzu query may continue)
                future.cancel()
                raise HTTPException(
                  status_code=status.HTTP_408_REQUEST_TIMEOUT,
                  detail=f"Query execution timeout ({query_timeout} seconds)",
                )

          except RuntimeError as e:
            error_msg = str(e)
            # Handle specific Kuzu errors gracefully
            if "Binder exception" in error_msg:
              # Extract the specific binding error for cleaner logging
              logger.warning(
                f"Query binding error for {validated_graph_id}: {error_msg}"
              )
              span.set_attribute("error", True)
              span.set_attribute("error.type", "BinderException")
              raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Query binding error: {error_msg}",
              )
            elif "Parser exception" in error_msg:
              logger.warning(
                f"Query parsing error for {validated_graph_id}: {error_msg}"
              )
              span.set_attribute("error", True)
              span.set_attribute("error.type", "ParserException")
              raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Query parsing error: {error_msg}",
              )
            elif "Catalog exception" in error_msg:
              logger.warning(f"Catalog error for {validated_graph_id}: {error_msg}")
              span.set_attribute("error", True)
              span.set_attribute("error.type", "CatalogException")
              raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Catalog error: {error_msg}",
              )
            else:
              # Other runtime errors
              logger.error(
                f"Query execution error for {validated_graph_id}: {error_msg}"
              )
              span.set_attribute("error", True)
              span.set_attribute("error.type", "RuntimeError")
              raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Query execution error: {error_msg}",
              )
          except Exception as e:
            # Catch-all for unexpected errors
            logger.error(f"Unexpected query error for {validated_graph_id}: {str(e)}")
            span.set_attribute("error", True)
            span.set_attribute("error.type", type(e).__name__)
            raise HTTPException(
              status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
              detail=f"Unexpected error: {str(e)}",
            )

          # Parse results based on Kuzu's output format
          rows = []
          columns = []

          # First, try to extract column aliases from the Cypher query itself
          # This preserves custom aliases like "RETURN c as entity"
          extracted_aliases = _extract_column_aliases_from_cypher(translated_cypher)

          if hasattr(query_result, "get_schema"):
            # Get column names from schema (these are generic: col0, col1, etc.)
            schema = query_result.get_schema()
            schema_columns = [col for col in schema.keys()]

            # Use extracted aliases if available and count matches schema columns
            if extracted_aliases and len(extracted_aliases) == len(schema_columns):
              columns = extracted_aliases
              logger.debug(f"Using extracted column aliases: {columns}")
            else:
              # Fall back to schema column names
              columns = schema_columns
              if extracted_aliases:
                logger.debug(
                  f"Column count mismatch - extracted: {len(extracted_aliases)}, schema: {len(schema_columns)}"
                )
          else:
            # If no schema available, try to use extracted aliases
            if extracted_aliases:
              columns = extracted_aliases

          # Iterate through all results with row limit protection
          MAX_ROWS = 10000
          while query_result.has_next() and len(rows) < MAX_ROWS:
            row = query_result.get_next()
            # Convert row to dictionary based on columns
            if columns:
              row_list = list(row)  # Ensure row is treated as a list
              row_dict = {}
              for i, col in enumerate(columns):
                row_dict[col] = row_list[i] if i < len(row_list) else None
              rows.append(row_dict)
            else:
              # Fallback: use generic column names
              row_dict = {f"col{i}": val for i, val in enumerate(row)}
              rows.append(row_dict)
              if not columns:  # Set columns on first row
                columns = list(row_dict.keys())

          # If we hit the limit, close the result to free resources
          if len(rows) >= MAX_ROWS and hasattr(query_result, "close"):
            query_result.close()

          execution_time = (time.time() - start_time) * 1000
          self.last_activity = datetime.now()

          # Update metrics
          self.metrics_collector.record_query(
            database=validated_graph_id,
            duration_ms=execution_time,
            success=True,
          )

          logger.info(
            f"Query executed successfully on {validated_graph_id}: "
            f"{len(rows)} rows in {execution_time:.2f}ms"
          )

          span.set_attribute("query.row_count", len(rows))
          span.set_attribute("query.execution_time_ms", execution_time)

          return QueryResponse(
            data=rows,
            columns=columns,
            execution_time_ms=execution_time,
            row_count=len(rows),
            database=validated_graph_id,
          )

      except HTTPException:
        raise
      except Exception as e:
        execution_time = (time.time() - start_time) * 1000
        logger.error(
          f"Query execution failed on {request.database}: {e} "
          f"(after {execution_time:.2f}ms)"
        )

        # Update metrics
        self.metrics_collector.record_query(
          database=request.database,
          duration_ms=execution_time,
          success=False,
        )

        span.set_attribute("error", True)
        span.set_attribute("error.message", str(e))

        raise HTTPException(
          status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
          detail=f"Query execution failed: {str(e)}",
        )

  def get_cluster_health(self) -> ClusterHealthResponse:
    """Get cluster health status."""
    databases = self.db_manager.list_databases()
    current_databases = len(databases)
    capacity_remaining = self.max_databases - current_databases
    uptime = time.time() - self.start_time

    # Determine overall status
    if capacity_remaining <= 0:
      status_str = "full"
    elif capacity_remaining < 10:
      status_str = "warning"
    else:
      status_str = "healthy"

    # Check system resource usage
    try:
      # Get actual CPU and memory usage
      cpu_usage = psutil.cpu_percent(interval=0.1)
      memory_usage = psutil.virtual_memory().percent
    except Exception:
      # Fallback to safe defaults if psutil fails
      cpu_usage = 0
      memory_usage = 0

    if cpu_usage > 90 or memory_usage > 90:
      status_str = "critical"
    elif cpu_usage > 75 or memory_usage > 75:
      status_str = "warning"

    return ClusterHealthResponse(
      status=status_str,
      uptime_seconds=uptime,
      node_type=self.node_type.value,
      base_path=self.base_path,
      max_databases=self.max_databases,
      current_databases=current_databases,
      capacity_remaining=capacity_remaining,
      read_only=self.read_only,
      last_activity=(self.last_activity.isoformat() if self.last_activity else None),
    )

  def get_cluster_info(self) -> ClusterInfoResponse:
    """Get detailed cluster information including all configuration parameters."""
    from robosystems.config import env
    from robosystems.graph_api.models.cluster import (
      MemoryConfiguration,
      QueryConfiguration,
      AdmissionControlConfig,
      NodeConfiguration,
    )

    databases = self.db_manager.list_databases()
    uptime = self.get_uptime()

    # Build comprehensive configuration
    memory_config = MemoryConfiguration(
      instance_max_mb=env.KUZU_MAX_MEMORY_MB,
      per_database_max_mb=env.KUZU_MAX_MEMORY_PER_DB_MB,
      admission_threshold_percent=env.KUZU_ADMISSION_MEMORY_THRESHOLD,
    )

    query_config = QueryConfiguration(
      timeout_seconds=env.KUZU_QUERY_TIMEOUT,
      max_connections_per_db=env.KUZU_MAX_CONNECTIONS_PER_DB,
      connection_ttl_minutes=env.KUZU_CONNECTION_TTL_MINUTES,
      health_check_interval_minutes=env.KUZU_HEALTH_CHECK_INTERVAL_MINUTES,
    )

    admission_config = AdmissionControlConfig(
      memory_threshold=env.KUZU_ADMISSION_MEMORY_THRESHOLD,
      cpu_threshold=env.KUZU_ADMISSION_CPU_THRESHOLD,
      queue_threshold=env.ADMISSION_QUEUE_THRESHOLD,
      check_interval=env.ADMISSION_CHECK_INTERVAL,
    )

    node_config = NodeConfiguration(
      base_path=self.base_path,
      read_only=self.read_only,
      max_databases=self.max_databases,
      memory_limits=memory_config,
      query_limits=query_config,
      admission_control=admission_config,
    )

    return ClusterInfoResponse(
      node_id=self.node_id,
      node_type=self.node_type.value,
      cluster_version=__version__,
      base_path=self.base_path,
      max_databases=self.max_databases,
      databases=databases,
      uptime_seconds=uptime,
      read_only=self.read_only,
      configuration=node_config,
    )

  async def create_database_backup_task(
    self,
    task_id: str,
    graph_id: str,
    backup_format: str = "full_dump",
    compression: bool = True,
    encryption: bool = False,
  ) -> None:
    """
    Background task to create a database backup.

    Creates a complete backup of the Kuzu database file.
    """
    try:
      logger.info(f"Starting backup task {task_id} for database {graph_id}")

      # For now, since we're just creating the backup locally on the Kuzu instance,
      # we'll create a simple file-based backup that can be retrieved
      import os
      import tempfile
      import shutil
      import zipfile
      from pathlib import Path
      from robosystems.middleware.graph.multitenant_utils import MultiTenantUtils

      db_path = MultiTenantUtils.get_database_path_for_graph(graph_id)

      if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database not found: {graph_id}")

      # Create backup in temporary directory
      backup_dir = Path(self.base_path) / "backups"
      backup_dir.mkdir(exist_ok=True)

      backup_file = backup_dir / f"{graph_id}_{task_id}.zip"

      with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Copy database files
        if os.path.isfile(db_path):
          # Single file database
          shutil.copy2(db_path, temp_path / f"{graph_id}.kuzu")
        else:
          # Directory-based database
          shutil.copytree(db_path, temp_path / graph_id)

        # Create ZIP archive
        with zipfile.ZipFile(backup_file, "w", zipfile.ZIP_DEFLATED) as zf:
          if os.path.isfile(db_path):
            zf.write(temp_path / f"{graph_id}.kuzu", f"{graph_id}.kuzu")
          else:
            for root, dirs, files in os.walk(temp_path / graph_id):
              for file in files:
                file_path = Path(root) / file
                arc_path = file_path.relative_to(temp_path)
                zf.write(file_path, arc_path)

      logger.info(
        f"Backup task {task_id} completed successfully. Backup saved to: {backup_file}"
      )

    except Exception as e:
      logger.error(f"Backup task {task_id} failed: {e}")
      raise

  async def restore_database_task(
    self,
    task_id: str,
    graph_id: str,
    backup_data: bytes,
    create_system_backup: bool = True,
  ) -> None:
    """
    Background task to restore a database from backup.

    Restores a Kuzu database from the provided backup data.
    """
    try:
      logger.info(f"Starting restore task {task_id} for database {graph_id}")

      import os
      import tempfile
      import shutil
      import zipfile
      from pathlib import Path
      from datetime import datetime, timezone
      from robosystems.middleware.graph.multitenant_utils import MultiTenantUtils

      db_path = MultiTenantUtils.get_database_path_for_graph(graph_id)
      db_dir = os.path.dirname(db_path)

      # Ensure directory exists
      os.makedirs(db_dir, exist_ok=True)

      # Create system backup if database exists
      if create_system_backup and os.path.exists(db_path):
        logger.info(f"Creating system backup before restore for {graph_id}")

        backup_dir = Path(self.base_path) / "system_backups"
        backup_dir.mkdir(exist_ok=True)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        system_backup = backup_dir / f"{graph_id}_system_{timestamp}.zip"

        with tempfile.TemporaryDirectory() as temp_dir:
          temp_path = Path(temp_dir)

          # Copy existing database
          if os.path.isfile(db_path):
            shutil.copy2(db_path, temp_path / f"{graph_id}.kuzu")
          else:
            shutil.copytree(db_path, temp_path / graph_id)

          # Create system backup ZIP
          with zipfile.ZipFile(system_backup, "w", zipfile.ZIP_DEFLATED) as zf:
            if os.path.isfile(db_path):
              zf.write(temp_path / f"{graph_id}.kuzu", f"{graph_id}.kuzu")
            else:
              for root, dirs, files in os.walk(temp_path / graph_id):
                for file in files:
                  file_path = Path(root) / file
                  arc_path = file_path.relative_to(temp_path)
                  zf.write(file_path, arc_path)

        logger.info(f"System backup created at: {system_backup}")

      # Extract and restore the backup
      with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Write backup data to temp file
        zip_file = temp_path / "restore.zip"
        with open(zip_file, "wb") as f:
          f.write(backup_data)

        # Extract backup
        with zipfile.ZipFile(zip_file, "r") as zf:
          zf.extractall(temp_path)

        # Remove existing database if present
        if os.path.exists(db_path):
          if os.path.isfile(db_path):
            os.remove(db_path)
          else:
            shutil.rmtree(db_path)

        # Restore database files
        if (temp_path / f"{graph_id}.kuzu").exists():
          # Single file database
          shutil.copy2(temp_path / f"{graph_id}.kuzu", db_path)
        elif (temp_path / graph_id).exists():
          # Directory-based database
          shutil.copytree(temp_path / graph_id, db_path)
        else:
          raise ValueError("Invalid backup format - no database files found")

      # Database files have been restored to the expected location
      # The database manager will detect them automatically on next access

      logger.info(
        f"Restore task {task_id} completed successfully for database {graph_id}"
      )

    except Exception as e:
      logger.error(f"Restore task {task_id} failed: {e}")
      raise


# Global service instance with thread-safe initialization
_cluster_service: Optional[KuzuClusterService] = None
_cluster_service_lock = threading.Lock()


def get_cluster_service() -> KuzuClusterService:
  """Get the global cluster service instance."""
  if _cluster_service is None:
    raise RuntimeError("Cluster service not initialized")
  return _cluster_service


def init_cluster_service(
  base_path: str,
  max_databases: int = 200,
  read_only: bool = False,
  node_type: NodeType = NodeType.WRITER,
  repository_type: RepositoryType = RepositoryType.ENTITY,
) -> KuzuClusterService:
  """Initialize the global cluster service instance with thread safety."""
  global _cluster_service

  with _cluster_service_lock:
    if _cluster_service is not None:
      logger.warning("Cluster service already initialized, returning existing instance")
      return _cluster_service

    _cluster_service = KuzuClusterService(
      base_path=base_path,
      max_databases=max_databases,
      read_only=read_only,
      node_type=node_type,
      repository_type=repository_type,
    )
    logger.info("Cluster service initialized successfully")
    return _cluster_service
