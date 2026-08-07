"""
LadybugDB service — query execution, health and metrics for one node.

Wraps :class:`~.manager.LadybugDatabaseManager` with the query path (including
validation, timeouts and streaming), node-type configuration, and the health
and metrics surfaces the routers expose.
"""

import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from datetime import UTC, datetime

import psutil
from fastapi import HTTPException, status

from robosystems.config import env
from robosystems.config.constants import (
  ADMISSION_CHECK_INTERVAL,
  GRAPH_HEALTH_CHECK_INTERVAL_MINUTES,
  LBUG_CONNECTION_TTL_MINUTES,
  LBUG_MAX_CONNECTIONS_PER_DB,
  MAX_QUERY_LENGTH,
)
from robosystems.config.tuning import TuningConfig
from robosystems.exceptions import (
  ConfigurationError,
)
from robosystems.graph_api.core.metrics_collector import LadybugMetricsCollector
from robosystems.graph_api.core.utils import (
  validate_database_name,
  validate_query_parameters,
)
from robosystems.graph_api.models.cluster import (
  ClusterHealthResponse,
  ClusterInfoResponse,
)
from robosystems.graph_api.models.database import QueryRequest, QueryResponse
from robosystems.logger import logger
from robosystems.middleware.graph.types import NodeType, RepositoryType
from robosystems.models.api.graphs.query import translate_neo4j_to_lbug

from .manager import LadybugDatabaseManager

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

  __version__ = version("robosystems")
except Exception:
  __version__ = "1.0.0"


def _raise_database_not_found(graph_id: str) -> None:
  """Raise 503 during replica warmup, otherwise 404."""
  from robosystems.graph_api.routers.health import is_warming_up

  if is_warming_up():
    raise HTTPException(
      status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
      detail={
        "error": "Database warming up",
        "reason": "Replica database is still loading",
        "status": "warming",
        "retry_after": 30,
      },
    )
  raise HTTPException(
    status_code=status.HTTP_404_NOT_FOUND,
    detail=f"Database '{graph_id}' not found",
  )


def _extract_column_aliases_from_cypher(cypher_query: str) -> list[str]:
  """Extract column names from a query's RETURN clause.

  Preserves the caller's aliases; without this the response carries
  LadybugDB's generic ``col0``/``col1`` names. Returns an empty list on any
  parse failure, which makes the caller fall back to those generic names.

  Nested expressions containing commas are not handled.

  Examples:
      "RETURN c as entity" -> ["entity"]
      "RETURN c.name, c.age as user_age" -> ["c.name", "user_age"]
      "RETURN count(c) as total" -> ["total"]
  """
  try:
    query = re.sub(r"\s+", " ", cypher_query.strip())

    return_match = re.search(
      r"\bRETURN\s+(.+?)(?:\s+(?:WHERE|ORDER|LIMIT|SKIP|WITH|UNION)|$)",
      query,
      re.IGNORECASE,
    )
    if not return_match:
      return []

    return_clause = return_match.group(1).strip()

    column_expressions = [expr.strip() for expr in return_clause.split(",")]

    aliases = []
    for expr in column_expressions:
      as_match = re.search(r"^(.+?)\s+(?:as|AS)\s+([a-zA-Z_][a-zA-Z0-9_]*)$", expr)
      if as_match:
        aliases.append(as_match.group(2))
      else:
        clean_expr = re.sub(r"\s+", " ", expr.strip())
        aliases.append(clean_expr)

    return aliases
  except Exception as e:
    logger.debug(f"Failed to parse RETURN clause from query: {e}")
    return []


# Hard backstop on rows pulled from a single streaming query. Streaming is the
# large-export path (100x the non-streaming 10k cap), but an unbounded
# cartesian or variable-length query would otherwise pin an instance pulling
# rows forever. On hit, the stream stops and the final chunk is flagged
# `truncated` so the client can paginate rather than silently lose data.
MAX_STREAMING_ROWS = 1_000_000


def validate_cypher_query(cypher: str) -> None:
  """Validate a Cypher query for the ad-hoc query endpoint.

  Raises ``HTTPException`` when the query is empty, over ``MAX_QUERY_LENGTH``,
  or contains an operation this endpoint does not permit.
  """
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

  # Defense in depth behind the keyword list: this process holds the embedded
  # engine and its disk access, so engine-level operations are refused here
  # regardless of how the request arrived. The tokenizer-based predicates from
  # the shared analyzer are the same ones the MCP write-query guard uses.
  from robosystems.security.cypher_analyzer import (
    is_admin_operation,
    is_bulk_operation,
    is_non_read_call,
    is_schema_ddl,
  )

  # `is_write_operation` is deliberately not among these: writer instances run
  # ordinary graph writes (CREATE/SET/MERGE/DELETE) through this path. Bulk
  # load, schema DDL and index creation have dedicated endpoints that do not
  # reach this validator.
  if (
    is_bulk_operation(cypher)
    or is_admin_operation(cypher)
    or is_schema_ddl(cypher)
    or is_non_read_call(cypher)
  ):
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=(
        "Query contains an engine-level operation (bulk load, attach/install, "
        "schema DDL, or a restricted CALL) that is not permitted on the "
        "ad-hoc query endpoint."
      ),
    )

  max_query_length = MAX_QUERY_LENGTH
  if len(cypher) > max_query_length:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"Query too long (max {max_query_length} characters)",
    )


class LadybugService:
  """LadybugDB service with multi-database support."""

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
    self.last_activity: datetime | None = None

    self.db_manager = LadybugDatabaseManager(
      base_path, max_databases, read_only=read_only
    )

    self.metrics_collector = LadybugMetricsCollector(
      base_path=base_path, node_type=node_type.value
    )

    self._validate_node_configuration()

    logger.info(
      f"Graph API Service initialized: {base_path} (max: {max_databases}, read_only: {read_only})"
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
    return f"lbug-{self.node_type.value}-{os.getpid()}"

  def get_uptime(self) -> float:
    """Get node uptime in seconds."""
    return time.time() - self.start_time

  def execute_query_streaming(self, request: QueryRequest, chunk_size: int = 1000):
    """Execute a query and yield ``chunk_size`` rows at a time.

    Errors are yielded as a terminal chunk carrying ``error``/``error_type``
    rather than raised — the response has already started streaming by the
    time most of them surface. Stops at ``MAX_STREAMING_ROWS`` and flags the
    final chunk ``truncated``.
    """
    with tracer.start_as_current_span(
      "lbug.execute_query_streaming",
      attributes={
        "database.name": request.database,
        "query.length": len(request.cypher),
        "query.has_parameters": bool(request.parameters),
        "chunk_size": chunk_size,
      },
    ) as span:
      start_time = time.time()

      try:
        validated_graph_id = validate_database_name(request.database)
        validate_cypher_query(request.cypher)

        if validated_graph_id not in self.db_manager.list_databases():
          _raise_database_not_found(validated_graph_id)

        translated_cypher = translate_neo4j_to_lbug(request.cypher)

        logger.debug(
          f"Streaming query on {validated_graph_id}: {translated_cypher[:100]}..."
        )

        with self.db_manager.get_connection(
          validated_graph_id, read_only=self.read_only
        ) as conn:
          try:
            # Planning and execution run on a worker thread so they can be
            # bounded by a timeout. Cancelling the HTTP coroutine does not
            # stop the engine, so an expensive query would otherwise pin the
            # instance for as long as it takes.
            query_timeout = TuningConfig.get_graph_query_timeout()

            def _execute_streaming():
              if request.parameters:
                return conn.execute(translated_cypher, request.parameters)
              return conn.execute(translated_cypher)

            with ThreadPoolExecutor(max_workers=1) as executor:
              future = executor.submit(_execute_streaming)
              try:
                query_result = future.result(timeout=query_timeout)
              except TimeoutError:
                future.cancel()
                logger.warning(
                  f"Streaming query timeout for {validated_graph_id} "
                  f"after {query_timeout} seconds"
                )
                yield {
                  "error": f"Query execution timeout ({query_timeout} seconds)",
                  "error_type": "QueryTimeout",
                  "chunk_index": 0,
                  "is_last_chunk": True,
                  "row_count": 0,
                  "total_rows_sent": 0,
                  "execution_time_ms": (time.time() - start_time) * 1000,
                }
                return

            # A multi-statement query returns a list; only the first result
            # is streamed.
            if isinstance(query_result, list):
              if len(query_result) == 0:
                raise RuntimeError("Query returned no results")
              query_result = query_result[0]

          except RuntimeError as e:
            error_msg = str(e)
            # Query-authoring errors are reported without a stack trace: they
            # are the caller's, not the server's.
            if "Binder exception" in error_msg:
              logger.warning(
                f"Query binding error for {validated_graph_id}: {error_msg}"
              )
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
            logger.error(f"Unexpected query error for {validated_graph_id}: {e!s}")
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

        # Caller aliases win over the engine's schema names, but only when the
        # counts agree — a mismatch means the parse was wrong.
        columns = []
        extracted_aliases = _extract_column_aliases_from_cypher(translated_cypher)

        if hasattr(query_result, "get_schema"):
          schema = query_result.get_schema()
          schema_columns = list(schema.keys())

          if extracted_aliases and len(extracted_aliases) == len(schema_columns):
            columns = extracted_aliases
          else:
            columns = schema_columns
        elif extracted_aliases:
          columns = extracted_aliases

        chunk_index = 0
        total_rows_sent = 0
        rows_buffer = []
        truncated = False

        while query_result.has_next():
          if total_rows_sent + len(rows_buffer) >= MAX_STREAMING_ROWS:
            truncated = True
            if hasattr(query_result, "close"):
              query_result.close()
            break
          row = query_result.get_next()

          if columns:
            row_list = list(row)
            row_dict = {
              columns[i]: row_list[i] if i < len(row_list) else None
              for i in range(len(columns))
            }
          else:
            row_dict = {f"col{i}": val for i, val in enumerate(row)}
            if not columns:
              columns = list(row_dict.keys())

          rows_buffer.append(row_dict)

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

        # Always send at least one chunk, and always send a closing chunk when
        # truncated — otherwise a cap that lands exactly on a chunk boundary
        # leaves an empty buffer and the client never sees the flag.
        if rows_buffer or chunk_index == 0 or truncated:
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
            "truncated": truncated,
          }
          yield chunk_data

        self.last_activity = datetime.now()

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

        self.metrics_collector.record_query(
          database=request.database,
          duration_ms=(time.time() - start_time) * 1000,
          success=False,
        )
        raise

  def execute_query(self, request: QueryRequest) -> QueryResponse:
    """Execute a query and return the full result set.

    Buffers every row in memory; use :meth:`execute_query_streaming` for large
    exports.
    """
    with tracer.start_as_current_span(
      "lbug.execute_query",
      attributes={
        "database.name": request.database,
        "query.length": len(request.cypher),
        "query.has_parameters": bool(request.parameters),
      },
    ) as span:
      start_time = time.time()

      try:
        validated_graph_id = validate_database_name(request.database)
        validate_cypher_query(request.cypher)
        validate_query_parameters(request.parameters)

        if validated_graph_id not in self.db_manager.list_databases():
          _raise_database_not_found(validated_graph_id)

        translated_cypher = translate_neo4j_to_lbug(request.cypher)

        if translated_cypher != request.cypher:
          logger.info(
            f"Translated Neo4j query to LadybugDB: {request.cypher[:100]}... -> {translated_cypher[:100]}..."
          )

        logger.debug(
          f"Executing query on {validated_graph_id}: {translated_cypher[:100]}..."
        )

        with self.db_manager.get_connection(
          validated_graph_id, read_only=self.read_only
        ) as conn:
          # Tunable at runtime via SSM.
          query_timeout = TuningConfig.get_graph_query_timeout()

          # A worker thread, not a signal-based alarm: signals only fire on the
          # main thread and would not survive a worker process.
          def execute_query_with_params():
            """Run the query on a worker thread so it can be timed out."""
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

                # A multi-statement query returns a list; only the first
                # result is returned to the caller.
                if isinstance(query_result, list):
                  if len(query_result) == 0:
                    raise RuntimeError("Query returned no results")
                  query_result = query_result[0]

              except TimeoutError:
                span.set_attribute("error", True)
                span.set_attribute("error.type", "QueryTimeout")
                logger.warning(
                  f"Query timeout for {validated_graph_id} after {query_timeout} seconds"
                )
                # Frees the caller; the engine keeps running the query.
                future.cancel()
                raise HTTPException(
                  status_code=status.HTTP_408_REQUEST_TIMEOUT,
                  detail=f"Query execution timeout ({query_timeout} seconds)",
                )

          except RuntimeError as e:
            error_msg = str(e)
            # Query-authoring errors map to 400s; anything else is a 500.
            if "Binder exception" in error_msg:
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
            logger.error(f"Unexpected query error for {validated_graph_id}: {e!s}")
            span.set_attribute("error", True)
            span.set_attribute("error.type", type(e).__name__)
            raise HTTPException(
              status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
              detail=f"Unexpected error: {e!s}",
            )

          rows = []
          columns = []

          # Caller aliases ("RETURN c as entity") win over the engine's
          # generic col0/col1 names, but only when the counts agree — a
          # mismatch means the parse was wrong.
          extracted_aliases = _extract_column_aliases_from_cypher(translated_cypher)

          if hasattr(query_result, "get_schema"):
            schema = query_result.get_schema()
            schema_columns = list(schema.keys())

            if extracted_aliases and len(extracted_aliases) == len(schema_columns):
              columns = extracted_aliases
              logger.debug(f"Using extracted column aliases: {columns}")
            else:
              columns = schema_columns
              if extracted_aliases:
                logger.debug(
                  f"Column count mismatch - extracted: {len(extracted_aliases)}, schema: {len(schema_columns)}"
                )
          else:
            if extracted_aliases:
              columns = extracted_aliases

          # Non-streaming results are fully buffered, so the row cap is what
          # keeps a broad query from exhausting the instance's memory.
          MAX_ROWS = 10000
          while query_result.has_next() and len(rows) < MAX_ROWS:
            row = query_result.get_next()
            if columns:
              row_list = list(row)
              row_dict = {}
              for i, col in enumerate(columns):
                row_dict[col] = row_list[i] if i < len(row_list) else None
              rows.append(row_dict)
            else:
              row_dict = {f"col{i}": val for i, val in enumerate(row)}
              rows.append(row_dict)
              if not columns:
                columns = list(row_dict.keys())

          # Abandoning a partially consumed result keeps its Arrow buffers
          # alive; closing releases them.
          if len(rows) >= MAX_ROWS and hasattr(query_result, "close"):
            query_result.close()

          execution_time = (time.time() - start_time) * 1000
          self.last_activity = datetime.now()

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

        self.metrics_collector.record_query(
          database=request.database,
          duration_ms=execution_time,
          success=False,
        )

        span.set_attribute("error", True)
        span.set_attribute("error.message", str(e))

        raise HTTPException(
          status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
          detail=f"Query execution failed: {e!s}",
        )

  def get_cluster_health(self) -> ClusterHealthResponse:
    """Get node health, derived from remaining database capacity and CPU/memory.

    Resource pressure overrides the capacity verdict, so a node with free
    slots still reports critical when it is out of headroom.
    """
    databases = self.db_manager.list_databases()
    current_databases = len(databases)
    capacity_remaining = self.max_databases - current_databases
    uptime = time.time() - self.start_time

    if capacity_remaining <= 0:
      status_str = "full"
    elif capacity_remaining < 10:
      status_str = "warning"
    else:
      status_str = "healthy"

    try:
      cpu_usage = psutil.cpu_percent(interval=0.1)
      memory_usage = psutil.virtual_memory().percent
    except Exception:
      # Unreadable metrics must not make the node look unhealthy.
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
    """Get node identity, its databases, and the full effective configuration."""
    from robosystems.config import env
    from robosystems.graph_api.models.cluster import (
      AdmissionControlConfig,
      MemoryConfiguration,
      NodeConfiguration,
      QueryConfiguration,
    )

    databases = self.db_manager.list_databases()
    uptime = self.get_uptime()

    # Memory limits come from the tier config in .github/configs/graph.yml.
    tier_config = env.get_lbug_tier_config()
    memory_config = MemoryConfiguration(
      instance_max_mb=tier_config.get("max_memory_mb", 2048),
      per_database_max_mb=tier_config.get("memory_per_db_mb", 0),
      admission_threshold_percent=env.LBUG_ADMISSION_MEMORY_THRESHOLD,
    )

    query_config = QueryConfiguration(
      timeout_seconds=TuningConfig.get_graph_query_timeout(),
      max_connections_per_db=LBUG_MAX_CONNECTIONS_PER_DB,
      connection_ttl_minutes=LBUG_CONNECTION_TTL_MINUTES,
      health_check_interval_minutes=GRAPH_HEALTH_CHECK_INTERVAL_MINUTES,
    )

    admission_config = AdmissionControlConfig(
      memory_threshold=env.LBUG_ADMISSION_MEMORY_THRESHOLD,
      cpu_threshold=env.LBUG_ADMISSION_CPU_THRESHOLD,
      queue_threshold=TuningConfig.get_admission_queue_threshold(),
      check_interval=ADMISSION_CHECK_INTERVAL,
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
    """Back up a database to a local ZIP under ``{base_path}/backups``.

    The archive stays on the instance; uploading it is the caller's job.
    Progress is reported through the task manager, not returned.
    """
    try:
      logger.info(f"Starting backup task {task_id} for database {graph_id}")

      import os
      import shutil
      import tempfile
      import zipfile
      from pathlib import Path

      from robosystems.middleware.graph.utils import MultiTenantUtils

      db_path = MultiTenantUtils.get_database_path_for_graph(graph_id)

      if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database not found: {graph_id}")

      backup_dir = Path(self.base_path) / "backups"
      backup_dir.mkdir(exist_ok=True)

      backup_file = backup_dir / f"{graph_id}_{task_id}.zip"

      with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Older engine versions stored a database as a directory.
        if os.path.isfile(db_path):
          shutil.copy2(db_path, temp_path / f"{graph_id}.lbug")
        else:
          shutil.copytree(db_path, temp_path / graph_id)

        with zipfile.ZipFile(backup_file, "w", zipfile.ZIP_DEFLATED) as zf:
          if os.path.isfile(db_path):
            zf.write(temp_path / f"{graph_id}.lbug", f"{graph_id}.lbug")
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
    """Restore a database from ZIP backup bytes, replacing what is on disk.

    ``create_system_backup`` snapshots the current database to
    ``{base_path}/system_backups`` first — it is the only recovery path, since
    the restore deletes the existing files before extracting.
    """
    try:
      logger.info(f"Starting restore task {task_id} for database {graph_id}")

      import os
      import shutil
      import tempfile
      import zipfile
      from datetime import datetime
      from pathlib import Path

      from robosystems.middleware.graph.utils import MultiTenantUtils

      db_path = MultiTenantUtils.get_database_path_for_graph(graph_id)
      db_dir = os.path.dirname(db_path)

      os.makedirs(db_dir, exist_ok=True)

      if create_system_backup and os.path.exists(db_path):
        logger.info(f"Creating system backup before restore for {graph_id}")

        backup_dir = Path(self.base_path) / "system_backups"
        backup_dir.mkdir(exist_ok=True)

        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        system_backup = backup_dir / f"{graph_id}_system_{timestamp}.zip"

        with tempfile.TemporaryDirectory() as temp_dir:
          temp_path = Path(temp_dir)

          if os.path.isfile(db_path):
            shutil.copy2(db_path, temp_path / f"{graph_id}.lbug")
          else:
            shutil.copytree(db_path, temp_path / graph_id)

          with zipfile.ZipFile(system_backup, "w", zipfile.ZIP_DEFLATED) as zf:
            if os.path.isfile(db_path):
              zf.write(temp_path / f"{graph_id}.lbug", f"{graph_id}.lbug")
            else:
              for root, dirs, files in os.walk(temp_path / graph_id):
                for file in files:
                  file_path = Path(root) / file
                  arc_path = file_path.relative_to(temp_path)
                  zf.write(file_path, arc_path)

        logger.info(f"System backup created at: {system_backup}")

      with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        zip_file = temp_path / "restore.zip"
        with open(zip_file, "wb") as f:
          f.write(backup_data)

        with zipfile.ZipFile(zip_file, "r") as zf:
          zf.extractall(temp_path)

        if os.path.exists(db_path):
          if os.path.isfile(db_path):
            os.remove(db_path)
          else:
            shutil.rmtree(db_path)

        # Older engine versions stored a database as a directory.
        if (temp_path / f"{graph_id}.lbug").exists():
          shutil.copy2(temp_path / f"{graph_id}.lbug", db_path)
        elif (temp_path / graph_id).exists():
          shutil.copytree(temp_path / graph_id, db_path)
        else:
          raise ValueError("Invalid backup format - no database files found")

      # The manager picks the restored files up on next access; nothing needs
      # to be re-registered.

      logger.info(
        f"Restore task {task_id} completed successfully for database {graph_id}"
      )

    except Exception as e:
      logger.error(f"Restore task {task_id} failed: {e}")
      raise


_ladybug_service: LadybugService | None = None
_ladybug_service_lock = threading.Lock()


def get_ladybug_service() -> LadybugService:
  """Get the process-wide service, raising if it has not been initialized."""
  if _ladybug_service is None:
    raise RuntimeError("LadybugDB service not initialized")
  return _ladybug_service


def init_ladybug_service(
  base_path: str,
  max_databases: int = 200,
  read_only: bool = False,
  node_type: NodeType = NodeType.WRITER,
  repository_type: RepositoryType = RepositoryType.ENTITY,
) -> LadybugService:
  """Create the process-wide service. Idempotent: a second call is a no-op."""
  global _ladybug_service

  with _ladybug_service_lock:
    if _ladybug_service is not None:
      logger.warning(
        "LadybugDB service already initialized, returning existing instance"
      )
      return _ladybug_service

    _ladybug_service = LadybugService(
      base_path=base_path,
      max_databases=max_databases,
      read_only=read_only,
      node_type=node_type,
      repository_type=repository_type,
    )
    logger.info("LadybugDB service initialized successfully")
    return _ladybug_service
