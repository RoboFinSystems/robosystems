import re
import threading
from contextlib import contextmanager
from functools import wraps
from typing import Any

from fastapi import HTTPException, status

from robosystems.graph_api.core.duckdb.pool import get_duckdb_pool
from robosystems.graph_api.models.tables import (
  TableCreateRequest,
  TableCreateResponse,
  TableInfo,
  TableQueryRequest,
  TableQueryResponse,
)
from robosystems.logger import logger


def validate_table_name(table_name: str) -> None:
  """
  Validate table name to prevent SQL injection.

  Only allows alphanumeric characters, underscores, and hyphens.
  This prevents SQL injection attacks through table name parameters.

  Args:
      table_name: Table name to validate

  Raises:
      HTTPException: If table name contains invalid characters
  """
  if not table_name or not re.match(r"^[a-zA-Z0-9_-]+$", table_name):
    logger.warning(f"Invalid table name rejected: {table_name[:50]}")
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Invalid table name. Only alphanumeric, underscore, and hyphen allowed.",
    )


def validate_table_name_decorator(func):
  """
  Decorator to automatically validate table_name parameters.

  Ensures all table operations validate table names to prevent SQL injection.
  Works with both positional and keyword arguments.
  """

  @wraps(func)
  def wrapper(self, *args, **kwargs):
    # Extract table_name from kwargs
    if "table_name" in kwargs:
      validate_table_name(kwargs["table_name"])
    # Extract from request object if present
    elif "request" in kwargs and hasattr(kwargs["request"], "table_name"):
      validate_table_name(kwargs["request"].table_name)
    # Extract from positional args if method signature has table_name
    # (This is a fallback for positional arguments)
    elif len(args) > 0:
      # Check if first arg after self looks like a request object with table_name
      if hasattr(args[0], "table_name"):
        validate_table_name(args[0].table_name)

    return func(self, *args, **kwargs)

  return wrapper


# Tenant read-query safety limits — the contract the endpoint advertises,
# now actually enforced (alongside the hardened read-only sandbox connection).
MAX_QUERY_ROWS = 10_000
QUERY_TIMEOUT_SECONDS = 30


def _strip_sql_noise(sql: str) -> str:
  """Blank out comments and string/identifier literals so structural checks
  (statement count, leading keyword) can't be fooled by their contents.

  Implemented as a single linear character scan (NO regex) — the tenant SQL path
  must not be turnable into a ReDoS. Comments collapse to a space; string /
  identifier bodies collapse to their empty delimiters.
  """
  out: list[str] = []
  i, n = 0, len(sql)
  while i < n:
    c = sql[i]
    nxt = sql[i + 1] if i + 1 < n else ""
    # line comment: -- ... EOL
    if c == "-" and nxt == "-":
      i += 2
      while i < n and sql[i] != "\n":
        i += 1
      out.append(" ")
      continue
    # block comment: /* ... */
    if c == "/" and nxt == "*":
      i += 2
      while i < n and not (sql[i] == "*" and sql[i + 1 : i + 2] == "/"):
        i += 1
      i += 2  # skip closing */ (runs past end if unterminated — fine)
      out.append(" ")
      continue
    # quoted string / backtick identifier
    if c in ("'", '"', "`"):
      quote = c
      i += 1
      while i < n:
        ch = sql[i]
        if ch == "\\" and quote != "`" and i + 1 < n:
          i += 2  # escaped char
          continue
        if ch == quote:
          if quote != "`" and sql[i + 1 : i + 2] == quote:
            i += 2  # doubled-quote SQL escape stays inside
            continue
          i += 1
          break
        i += 1
      out.append(quote + quote)  # empty delimited body
      continue
    out.append(c)
    i += 1
  return "".join(out)


def validate_read_only_sql(sql: str) -> None:
  """Enforce the advertised SELECT-only, single-statement contract.

  The hardened read-only + external-access-off connection already blocks writes
  and exfiltration at the engine level; this rejects non-read statements and
  statement chaining up front (defense-in-depth + a clear error message).

  Raises:
      HTTPException: 400 if the SQL is empty, chains statements, or does not
          begin with SELECT/WITH.
  """
  if not sql or not sql.strip():
    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Empty query")

  stripped = _strip_sql_noise(sql).strip().rstrip(";").strip()

  # Any remaining ';' sits between statements — reject chaining.
  if ";" in stripped:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Only a single statement is allowed.",
    )

  match = re.match(r"\s*([a-zA-Z]+)", stripped)
  first_keyword = match.group(1).lower() if match else ""
  if first_keyword not in ("select", "with"):
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Only read-only SELECT/WITH queries are allowed on this surface.",
    )


@contextmanager
def _interrupt_after(conn, timeout: float, timed_out: dict):
  """Interrupt `conn` if the wrapped work runs past `timeout` seconds."""

  def _interrupt():
    timed_out["v"] = True
    try:
      conn.interrupt()
    except Exception:
      pass

  timer = threading.Timer(timeout, _interrupt)
  timer.start()
  try:
    yield
  finally:
    timer.cancel()


class DuckDBTableManager:
  """
  DuckDB table manager for staging tables.

  All tables are external views over S3 - zero local storage, used only
  for transformation/staging before ingestion into LadybugDB graph database.
  """

  def __init__(self):
    """
    Initialize DuckDB Table Manager.

    The connection pool manages all database paths automatically.
    """
    logger.info(
      "Initialized DuckDB Table Manager (staging layer for LadybugDB ingestion)"
    )

  def _build_table_sql(
    self,
    quoted_table: str,
    has_identifier: bool,
    has_from_to: bool,
    use_list: bool,
    column_names: list[str] | None = None,
    null_columns: set[str] | None = None,
  ) -> str:
    """
    Build CREATE TABLE SQL with deduplication using GROUP BY + FIRST().

    Uses hash aggregation instead of window functions for deduplication.
    This approach has better memory efficiency and proper spill-to-disk support
    via DuckDB's external aggregation, unlike ROW_NUMBER which can OOM on large datasets.
    See: https://duckdb.org/2024/03/29/external-aggregation

    NOTE: Deduplication is required here because materialization uses a plain
    COPY (no ignore_errors — LadybugDB 0.18's ignore_errors path silently drops
    valid rows in proportion to batch size). LadybugDB enforces unique node
    primary keys and unique relationship (from,to) pairs, so any duplicate that
    reached it would hard-fail the COPY. Dedup in staging is what keeps the
    clean COPY both safe and complete.

    Args:
        quoted_table: Quoted table name (e.g., '"table_name"')
        has_identifier: Whether table has 'identifier' column (node table)
        has_from_to: Whether table has 'from' and 'to' columns (relationship table)
        use_list: Whether using list (placeholder) or pattern (parameter binding)
        column_names: List of column names from schema probe (required for dedup)

    Returns:
        SQL statement with proper deduplication
    """
    # Use placeholder for list (will be replaced) or ? for parameter binding
    read_pattern = "__FILES_PLACEHOLDER__" if use_list else "?"

    if has_identifier and column_names:
      # Node table: deduplicate on identifier using GROUP BY + FIRST()
      # FIRST() is an aggregate that returns an arbitrary value from the group
      # union_by_name=true handles schema variations between files
      null_cols = null_columns or set()
      dedupe_cols = ["identifier"]
      other_cols = [c for c in column_names if c not in dedupe_cols]
      select_parts = [f'"{c}"' for c in dedupe_cols] + [
        f'NULL AS "{c}"' if c in null_cols else f'FIRST("{c}") AS "{c}"'
        for c in other_cols
      ]
      select_clause = ", ".join(select_parts)
      group_by_clause = ", ".join(f'"{c}"' for c in dedupe_cols)

      return f"""
        CREATE OR REPLACE TABLE {quoted_table} AS
        SELECT {select_clause}
        FROM read_parquet({read_pattern}, union_by_name=true, hive_partitioning=false)
        GROUP BY {group_by_clause}
      """
    elif has_from_to and column_names:
      # Relationship table: deduplicate on (from, to) and rename to src/dst
      # IMPORTANT: LadybugDB expects columns in order: src, dst, then properties
      null_cols = null_columns or set()
      dedupe_cols = ["from", "to"]
      other_cols = [c for c in column_names if c not in dedupe_cols]
      # Build select: src, dst first, then FIRST() for other columns
      select_parts = ['"from" AS src', '"to" AS dst'] + [
        f'NULL AS "{c}"' if c in null_cols else f'FIRST("{c}") AS "{c}"'
        for c in other_cols
      ]
      select_clause = ", ".join(select_parts)

      return f"""
        CREATE OR REPLACE TABLE {quoted_table} AS
        SELECT {select_clause}
        FROM read_parquet({read_pattern}, union_by_name=true, hive_partitioning=false)
        GROUP BY "from", "to"
      """
    else:
      # Unknown table type or no column names: just read without deduplication
      # union_by_name=true handles schema variations between files
      return f"""
        CREATE OR REPLACE TABLE {quoted_table} AS
        SELECT *
        FROM read_parquet({read_pattern}, union_by_name=true, hive_partitioning=false)
      """

  def _build_table_sql_with_file_id(
    self,
    quoted_table: str,
    has_identifier: bool,
    has_from_to: bool,
    s3_files: list[str],
    file_id_map: dict[str, str],
    column_names: list[str] | None = None,
    null_columns: set[str] | None = None,
  ) -> str:
    """
    Build CREATE TABLE SQL with file_id column injection (v2 incremental ingestion).

    Uses UNION ALL to combine files, injecting file_id for each source file.
    This enables per-file deletion and provenance tracking.

    Uses GROUP BY + FIRST() for deduplication instead of ROW_NUMBER for better
    memory efficiency and spill-to-disk support.

    Args:
        quoted_table: Quoted table name
        has_identifier: Whether table has 'identifier' column
        has_from_to: Whether table has 'from'/'to' columns
        s3_files: List of S3 file paths
        file_id_map: Map of s3_key -> file_id
        column_names: List of column names from schema probe (required for dedup)

    Returns:
        SQL with UNION ALL and file_id injection
    """
    # Build individual SELECT queries for each file
    selects = []

    for s3_key in s3_files:
      file_id = file_id_map.get(s3_key, "unknown")

      # Escape single quotes to prevent SQL injection via S3 keys or file IDs
      safe_s3_key = s3_key.replace("'", "''")
      safe_file_id = file_id.replace("'", "''")

      if has_identifier:
        # Node table: keep identifier, add file_id
        # union_by_name=true handles schema variations between files
        select = f"""
          SELECT *, '{safe_file_id}' as file_id
          FROM read_parquet('{safe_s3_key}', union_by_name=true, hive_partitioning=false)
        """
      elif has_from_to:
        # Relationship table: rename from/to to src/dst, add file_id
        # union_by_name=true handles schema variations between files
        select = f"""
          SELECT
            "from" as src,
            "to" as dst,
            * EXCLUDE ("from", "to"),
            '{safe_file_id}' as file_id
          FROM read_parquet('{safe_s3_key}', union_by_name=true, hive_partitioning=false)
        """
      else:
        # Unknown table: just add file_id
        # union_by_name=true handles schema variations between files
        select = f"""
          SELECT *, '{safe_file_id}' as file_id
          FROM read_parquet('{safe_s3_key}', union_by_name=true, hive_partitioning=false)
        """

      selects.append(select)

    # Combine with UNION ALL
    union_query = "\n UNION ALL\n".join(selects)

    # Wrap in deduplication using GROUP BY + FIRST() for better memory efficiency
    if has_identifier and column_names:
      # Node table: dedupe on identifier, keep first file_id
      null_cols = null_columns or set()
      dedupe_cols = ["identifier"]
      other_cols = [c for c in column_names if c not in dedupe_cols]
      # Include file_id in the aggregation
      select_parts = (
        [f'"{c}"' for c in dedupe_cols]
        + [
          f'NULL AS "{c}"' if c in null_cols else f'FIRST("{c}") AS "{c}"'
          for c in other_cols
        ]
        + ["FIRST(file_id) AS file_id"]
      )
      select_clause = ", ".join(select_parts)
      group_by_clause = ", ".join(f'"{c}"' for c in dedupe_cols)

      return f"""
        CREATE OR REPLACE TABLE {quoted_table} AS
        SELECT {select_clause}
        FROM ({union_query})
        GROUP BY {group_by_clause}
      """
    elif has_from_to and column_names:
      # Relationship table: dedupe on src/dst (already renamed in union_query)
      # Note: column_names has original names (from, to), but union_query renamed them
      null_cols = null_columns or set()
      other_cols = [c for c in column_names if c not in ["from", "to"]]
      select_parts = (
        ["src", "dst"]
        + [
          f'NULL AS "{c}"' if c in null_cols else f'FIRST("{c}") AS "{c}"'
          for c in other_cols
        ]
        + ["FIRST(file_id) AS file_id"]
      )
      select_clause = ", ".join(select_parts)

      return f"""
        CREATE OR REPLACE TABLE {quoted_table} AS
        SELECT {select_clause}
        FROM ({union_query})
        GROUP BY src, dst
      """
    else:
      # No deduplication for unknown tables
      return f"""
        CREATE OR REPLACE TABLE {quoted_table} AS
        {union_query}
      """

  @validate_table_name_decorator
  def create_table(self, request: TableCreateRequest) -> TableCreateResponse:
    """
    Create an external table (materialized from S3 files).

    DuckDB tables are materialized (not views) to enable LadybugDB ingestion.

    CRITICAL: We use CREATE TABLE (not CREATE VIEW) because:
    - S3 credentials are session-level in DuckDB, not persisted in .duckdb file
    - When LadybugDB's DuckDB extension attaches the database, it creates a new session
    - New session = no S3 config = views fail with "Unsupported duckdb type: NULL"
    - Materialized tables contain the actual data, so no S3 access needed during ingestion

    This is a staging layer for LadybugDB ingestion - tables are dropped after use.

    Supports both:
    - s3_pattern as string: wildcard pattern (e.g., "s3://bucket/path/*.parquet")
    - s3_pattern as list: explicit file paths (uses DuckDB list syntax)
    - file_id_map: Optional map to inject file_id for incremental ingestion tracking
    """
    import time

    start_time = time.time()

    # Explicit validation (decorator provides safety net)
    validate_table_name(request.table_name)

    is_list = isinstance(request.s3_pattern, list)
    file_count = len(request.s3_pattern) if is_list else "pattern"
    has_file_id_map = bool(request.file_id_map)

    logger.info(
      f"Creating external table {request.table_name} for graph {request.graph_id} "
      f"from {file_count} {'files' if is_list else ''} "
      f"(file_id tracking: {has_file_id_map})"
    )

    pool = get_duckdb_pool()

    try:
      with pool.get_connection(request.graph_id) as conn:
        quoted_table = f'"{request.table_name}"'

        # Determine if this is a node table (has identifier) or relationship table (has from/to)
        # Peek at the first file to check schema
        sample_file = request.s3_pattern[0] if is_list else request.s3_pattern

        # Check if 'identifier' column exists (node table) or 'from' column exists (relationship table)
        # Use hive_partitioning=false to prevent DuckDB from auto-adding partition columns
        probe_result = conn.execute(
          "SELECT * FROM read_parquet(?, hive_partitioning=false) LIMIT 0",
          [sample_file],
        ).description
        column_names = [col[0] for col in probe_result]
        has_identifier = "identifier" in column_names
        has_from_to = "from" in column_names and "to" in column_names

        # Columns to NULL out (keep in schema, skip data)
        null_cols = set(request.null_columns) if request.null_columns else None

        # v2 Incremental Ingestion: Build UNION ALL with file_id injection
        if has_file_id_map and is_list:
          sql = self._build_table_sql_with_file_id(
            quoted_table,
            has_identifier,
            has_from_to,
            request.s3_pattern,
            request.file_id_map,
            column_names,
            null_columns=null_cols,
          )
          conn.execute(sql)
        else:
          # Legacy path: without file_id tracking
          sql = self._build_table_sql(
            quoted_table,
            has_identifier,
            has_from_to,
            is_list,
            column_names,
            null_columns=null_cols,
          )

          if is_list:
            # Replace placeholder with DuckDB list syntax: ['file1', 'file2', ...]
            # Escape single quotes to prevent SQL injection via S3 paths
            files_list = (
              "["
              + ", ".join(
                f"'{path.replace(chr(39), chr(39) * 2)}'" for path in request.s3_pattern
              )
              + "]"
            )
            sql = sql.replace("__FILES_PLACEHOLDER__", files_list)
            conn.execute(sql)
          else:
            # Use parameter binding for pattern (prevents SQL injection)
            conn.execute(sql, [request.s3_pattern])

        # Count rows in the created table
        count_result = conn.execute(f"SELECT COUNT(*) FROM {quoted_table}").fetchone()
        row_count = count_result[0] if count_result else 0

        # Checkpoint to flush WAL and clear state after large table creation.
        # This is important for chunked ingestion where CREATE is followed by INSERTs.
        try:
          conn.execute("CHECKPOINT")
          logger.debug(f"Checkpointed DuckDB after CREATE {request.table_name}")
        except Exception as cp_err:
          # Non-fatal - log but continue
          logger.warning(f"Could not checkpoint after CREATE: {cp_err}")

        execution_time_ms = (time.time() - start_time) * 1000

        logger.info(
          f"Created external table {request.table_name} for graph {request.graph_id} "
          f"in {execution_time_ms:.2f}ms ({file_count} {'files' if is_list else ''}, {row_count:,} rows)"
        )

        return TableCreateResponse(
          status="success",
          graph_id=request.graph_id,
          table_name=request.table_name,
          execution_time_ms=execution_time_ms,
          row_count=row_count,
        )

    except Exception as e:
      logger.error(f"Failed to create table {request.table_name}: {e}")
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Failed to create table: {e!s}",
      )

  @validate_table_name_decorator
  def insert_into_table(self, request: TableCreateRequest) -> TableCreateResponse:
    """
    Insert data into an existing table from S3 files.

    Supports two deduplication strategies controlled by request.deduplicate:

    deduplicate=False (default):
      Simple INSERT INTO append. No dedup overhead — callers are responsible
      for ensuring no duplicate rows.

    deduplicate=True:
      Uses INSERT INTO ... SELECT ... WHERE NOT EXISTS to skip rows whose
      dedup key already exists in the target table. The NOT EXISTS hash join
      only holds dedup key strings in memory, which DuckDB can spill to disk.

      This is safe for tables with wide columns like FLOAT[384] embeddings.
      The previous GROUP BY + FIRST() approach could not spill aggregate state
      for fixed-size list columns, causing OOM at 10M+ unique groups.

    Dedup keys:
    - Node tables (has "identifier"): dedup on identifier
    - Relationship tables (has src/dst or from/to): dedup on src+dst or from+to
    - Unknown schema: no dedup (plain append)

    Prerequisites:
    - Table must already exist (created via create_table)
    - Schema must be compatible with the new files

    Args:
        request: TableCreateRequest with graph_id, table_name, s3_pattern, deduplicate

    Returns:
        TableCreateResponse with status, timing info, and row_count (net rows added)

    Raises:
        HTTPException: If table doesn't exist or insert fails
    """
    import time

    start_time = time.time()

    validate_table_name(request.table_name)

    is_list = isinstance(request.s3_pattern, list)
    file_count = len(request.s3_pattern) if is_list else "pattern"

    logger.info(
      f"Inserting into table {request.table_name} for graph {request.graph_id} "
      f"from {file_count} {'files' if is_list else ''} "
      f"(deduplicate={request.deduplicate})"
    )

    pool = get_duckdb_pool()

    try:
      with pool.get_connection(request.graph_id) as conn:
        quoted_table = f'"{request.table_name}"'

        # Count rows before insert
        count_before = conn.execute(f"SELECT COUNT(*) FROM {quoted_table}").fetchone()
        rows_before = count_before[0] if count_before else 0

        # Build parquet read expression (escape single quotes to prevent SQL injection)
        if is_list:
          files_list = (
            "["
            + ", ".join(
              f"'{path.replace(chr(39), chr(39) * 2)}'" for path in request.s3_pattern
            )
            + "]"
          )
          parquet_read = (
            f"read_parquet({files_list}, union_by_name=true, hive_partitioning=false)"
          )
        else:
          safe_pattern = request.s3_pattern.replace("'", "''")
          parquet_read = f"read_parquet('{safe_pattern}', union_by_name=true, hive_partitioning=false)"

        null_cols = set(request.null_columns) if request.null_columns else None

        # Resolve file_id for provenance tracking.
        # When file_id_map is provided, inject file_id into the SELECT so the
        # INSERT matches the table schema (which includes file_id from CREATE).
        file_id_value: str | None = None
        if request.file_id_map and is_list:
          # Single-file INSERT: grab the file_id for the one file
          for s3_key, fid in request.file_id_map.items():
            file_id_value = fid.replace("'", "''")
            break

        if request.deduplicate:
          # Detect table type from existing schema for dedup key
          probe_result = conn.execute(
            f"SELECT * FROM {quoted_table} LIMIT 0"
          ).description
          column_names = [col[0] for col in probe_result]
          has_identifier = "identifier" in column_names
          has_src_dst = "src" in column_names and "dst" in column_names
          has_from_to = "from" in column_names and "to" in column_names

          # Also probe the parquet schema to detect column name mismatches.
          # Initial CREATE TABLE renames from/to → src/dst for LadybugDB compat,
          # but parquet files still use from/to.
          parquet_probe = conn.execute(
            f"SELECT * FROM {parquet_read} LIMIT 0"
          ).description
          parquet_columns = [col[0] for col in parquet_probe]
          parquet_has_from_to = "from" in parquet_columns and "to" in parquet_columns

          # Schema evolution: add any new parquet columns missing from the table.
          # This handles cases where a new property was added to the schema after
          # the table was initially created by a full rebuild.
          table_col_set = set(column_names)
          # Build a map of parquet column name → DuckDB type from the probe
          parquet_type_map = {col[0]: col[1] for col in parquet_probe}
          # Account for from/to → src/dst rename done during initial CREATE TABLE
          rename_map = {"from": "src", "to": "dst"}
          new_cols = [
            c
            for c in parquet_columns
            if c not in table_col_set and rename_map.get(c, c) not in table_col_set
          ]
          if new_cols:
            for col_name in new_cols:
              col_type = parquet_type_map.get(col_name, "VARCHAR")
              conn.execute(
                f'ALTER TABLE {quoted_table} ADD COLUMN "{col_name}" {col_type}'
              )
              logger.info(
                f"Schema evolution: added column {col_name} ({col_type}) "
                f"to {request.table_name}"
              )

          # Build dedup: first deduplicate within the incoming batch (intra-batch),
          # then filter out rows already in the target table (inter-batch).
          # Intra-batch dedup is needed when multiple parquet files (e.g., from
          # different quarters) contain the same row (e.g., Taxonomy URIs).

          # Compute the actual post-evolution column order for the table:
          # pre-evolution columns (column_names) followed by newly added columns (new_cols).
          # Using an explicit INSERT column list avoids positional mismatches when
          # file_id sits between original and schema-evolved columns (Bug: DOUBLE[]→DOUBLE).
          post_cols = list(column_names) + new_cols

          def _explicit_col_expr(table_col: str) -> str:
            # Maps table column → SELECT expr; handles file_id literal, src/dst rename, null_cols.
            if table_col == "file_id":
              if file_id_value:
                return f"'{file_id_value}' AS file_id"
              return 'NULL AS "file_id"'
            if table_col == "src" and parquet_has_from_to:
              if null_cols and "from" in null_cols:
                return 'NULL AS "src"'
              return '"from" AS src'
            if table_col == "dst" and parquet_has_from_to:
              if null_cols and "to" in null_cols:
                return 'NULL AS "dst"'
              return '"to" AS dst'
            if null_cols and table_col in null_cols:
              return f'NULL AS "{table_col}"'
            # Target has a column the source parquet lacks — e.g. a property
            # removed from the schema after this table was first created (an
            # older full-rebuild left the column behind; schema evolution only
            # adds source columns, never drops stale target-only ones). NULL it
            # rather than referencing a nonexistent source column, which would
            # raise "Values list 't' does not have a column named ...".
            if table_col not in parquet_columns:
              return f'NULL AS "{table_col}"'
            return f't."{table_col}"'

          select_expr = ", ".join(_explicit_col_expr(c) for c in post_cols)
          insert_cols_clause = ", ".join(f'"{c}"' for c in post_cols)

          if has_identifier:
            intra_dedup = 'QUALIFY ROW_NUMBER() OVER (PARTITION BY "identifier") = 1'
            inter_dedup = (
              f"WHERE NOT EXISTS (SELECT 1 FROM {quoted_table} a "
              f'WHERE a."identifier" = t."identifier")'
            )
          elif has_src_dst:
            if parquet_has_from_to:
              intra_dedup = 'QUALIFY ROW_NUMBER() OVER (PARTITION BY "from", "to") = 1'
              inter_dedup = (
                f"WHERE NOT EXISTS (SELECT 1 FROM {quoted_table} a "
                f'WHERE a."src" = t."from" AND a."dst" = t."to")'
              )
            else:
              intra_dedup = 'QUALIFY ROW_NUMBER() OVER (PARTITION BY "src", "dst") = 1'
              inter_dedup = (
                f"WHERE NOT EXISTS (SELECT 1 FROM {quoted_table} a "
                f'WHERE a."src" = t."src" AND a."dst" = t."dst")'
              )
          elif has_from_to:
            intra_dedup = 'QUALIFY ROW_NUMBER() OVER (PARTITION BY "from", "to") = 1'
            inter_dedup = (
              f"WHERE NOT EXISTS (SELECT 1 FROM {quoted_table} a "
              f'WHERE a."from" = t."from" AND a."to" = t."to")'
            )
          else:
            # Unknown schema — no dedup key, plain append
            intra_dedup = ""
            inter_dedup = ""

          # CTE deduplicates within incoming batch, outer query filters against existing rows
          if intra_dedup:
            sql = (
              f"INSERT INTO {quoted_table} ({insert_cols_clause}) "
              f"WITH new_data AS (SELECT * FROM {parquet_read} {intra_dedup}) "
              f"SELECT {select_expr} FROM new_data t {inter_dedup}"
            )
          else:
            sql = (
              f"INSERT INTO {quoted_table} ({insert_cols_clause}) "
              f"SELECT {select_expr} FROM {parquet_read} t {inter_dedup}"
            )
        else:
          # Simple append — no dedup
          # Schema evolution: add any new parquet columns missing from the table
          append_parquet_probe = conn.execute(
            f"SELECT * FROM {parquet_read} LIMIT 0"
          ).description
          append_columns = [col[0] for col in append_parquet_probe]
          table_probe = conn.execute(
            f"SELECT * FROM {quoted_table} LIMIT 0"
          ).description
          table_col_set = {col[0] for col in table_probe}
          append_type_map = {col[0]: col[1] for col in append_parquet_probe}
          new_append_cols = [c for c in append_columns if c not in table_col_set]
          if new_append_cols:
            for col_name in new_append_cols:
              col_type = append_type_map.get(col_name, "VARCHAR")
              conn.execute(
                f'ALTER TABLE {quoted_table} ADD COLUMN "{col_name}" {col_type}'
              )
              logger.info(
                f"Schema evolution: added column {col_name} ({col_type}) "
                f"to {request.table_name}"
              )

          # Build select expression with optional file_id injection
          file_id_suffix = f", '{file_id_value}' AS file_id" if file_id_value else ""

          if null_cols:
            append_expr = ", ".join(
              f'NULL AS "{c}"' if c in null_cols else f'"{c}"' for c in append_columns
            )
            sql = f"INSERT INTO {quoted_table} SELECT {append_expr}{file_id_suffix} FROM {parquet_read}"
          else:
            sql = (
              f"INSERT INTO {quoted_table} SELECT *{file_id_suffix} FROM {parquet_read}"
            )

        conn.execute(sql)

        # Count rows after insert
        count_after = conn.execute(f"SELECT COUNT(*) FROM {quoted_table}").fetchone()
        rows_after = count_after[0] if count_after else 0
        rows_added = rows_after - rows_before

        # Checkpoint to flush WAL and clear accumulated state.
        # Without this, chunked ingestion (multiple INSERTs in sequence) causes
        # WAL growth and connection state accumulation that eventually stalls.
        try:
          conn.execute("CHECKPOINT")
          logger.debug(f"Checkpointed DuckDB after INSERT into {request.table_name}")
        except Exception as cp_err:
          # Non-fatal - log but continue
          logger.warning(f"Could not checkpoint after INSERT: {cp_err}")

        execution_time_ms = (time.time() - start_time) * 1000

        logger.info(
          f"Inserted into table {request.table_name} for graph {request.graph_id} "
          f"in {execution_time_ms:.2f}ms ({file_count} {'files' if is_list else ''}, "
          f"{rows_added:,} net rows added, {rows_after:,} total)"
        )

        return TableCreateResponse(
          status="success",
          graph_id=request.graph_id,
          table_name=request.table_name,
          execution_time_ms=execution_time_ms,
          row_count=rows_added,
        )

    except Exception as e:
      logger.error(f"Failed to insert into table {request.table_name}: {e}")
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Failed to insert into table: {e!s}",
      )

  def _fetch_readonly(self, request: TableQueryRequest) -> tuple[list[str], list]:
    """Validate + run a tenant read query on the hardened read-only connection,
    buffering up to MAX_QUERY_ROWS rows. Returns (columns, rows).

    The per-graph lock (held inside ``get_readonly_connection``) is acquired AND
    released here, on a single thread. Callers MUST fully consume this before
    yielding to a streaming client: a ``threading`` lock released on a different
    thread than it was acquired raises and would wedge the lock permanently, and
    the streaming response driver resumes generators on arbitrary worker threads.
    Bounded buffering is safe because results are capped at MAX_QUERY_ROWS.
    """
    validate_read_only_sql(request.sql)
    pool = get_duckdb_pool()
    timed_out = {"v": False}
    try:
      # Hardened, sandboxed read-only connection: external access disabled,
      # config locked, no httpfs/postgres_scanner — reads local staging only.
      with pool.get_readonly_connection(request.graph_id) as conn:
        with _interrupt_after(conn, QUERY_TIMEOUT_SECONDS, timed_out):
          if request.parameters:
            cursor = conn.execute(request.sql, request.parameters)
          else:
            cursor = conn.execute(request.sql)
          # Fetch one past the cap so we can detect + flag truncation.
          result = cursor.fetchmany(MAX_QUERY_ROWS + 1)
        description = cursor.description

      columns = [desc[0] for desc in description] if description else []
      if len(result) > MAX_QUERY_ROWS:
        result = result[:MAX_QUERY_ROWS]
        logger.warning(
          f"Query for {request.graph_id} truncated to {MAX_QUERY_ROWS} rows"
        )
      return columns, result

    except HTTPException:
      raise
    except FileNotFoundError as e:
      raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
      if timed_out["v"]:
        raise HTTPException(
          status_code=status.HTTP_408_REQUEST_TIMEOUT,
          detail=f"Query exceeded {QUERY_TIMEOUT_SECONDS}s time limit.",
        )
      logger.error(f"Query failed for graph {request.graph_id}: {e}")
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Query failed: {e!s}",
      )

  def query_table(self, request: TableQueryRequest) -> TableQueryResponse:
    import time

    start_time = time.time()

    logger.info(f"Executing query for graph {request.graph_id}: {request.sql[:100]}...")

    columns, result = self._fetch_readonly(request)
    rows = [list(row) for row in result]
    execution_time_ms = (time.time() - start_time) * 1000

    logger.info(f"Query returned {len(rows)} rows in {execution_time_ms:.2f}ms")

    return TableQueryResponse(
      columns=columns,
      rows=rows,
      row_count=len(rows),
      execution_time_ms=execution_time_ms,
    )

  def execute_write(self, request: TableQueryRequest) -> TableQueryResponse:
    """Execute a write/DDL statement on the read-write staging connection.

    Internal-only companion to ``query_table``. Where ``query_table`` runs
    tenant SQL on the hardened, sandboxed read-only connection (SELECT/WITH
    only, external access off, no httpfs/postgres_scanner), this runs on the
    read-write connection with httpfs + postgres_scanner available — the
    surface the extensions materializer and connector loaders need for
    ``CREATE TABLE AS SELECT ... postgres_scan(...)`` staging and
    INSERT/DELETE upserts.

    NOT reachable from the tenant ``/query/sql`` path: only the internal graph
    client (materialization, ingestion) calls the ``/tables/execute`` endpoint
    this backs. It mirrors the existing internal write endpoints
    (``create_table``, ``insert_into_table``) that the read-only hardening left
    in place; it adds no surface beyond them.
    """
    import time

    start_time = time.time()

    logger.info(f"Executing write for graph {request.graph_id}: {request.sql[:100]}...")

    pool = get_duckdb_pool()

    try:
      with pool.get_connection(request.graph_id) as conn:
        if request.parameters:
          result = conn.execute(request.sql, request.parameters).fetchall()
        else:
          result = conn.execute(request.sql).fetchall()
        description = conn.description

      columns = [desc[0] for desc in description] if description else []
      rows = [list(row) for row in result]
      execution_time_ms = (time.time() - start_time) * 1000

      logger.info(f"Write returned {len(rows)} rows in {execution_time_ms:.2f}ms")

      return TableQueryResponse(
        columns=columns,
        rows=rows,
        row_count=len(rows),
        execution_time_ms=execution_time_ms,
      )
    except HTTPException:
      raise
    except Exception as e:
      logger.error(f"Write failed for graph {request.graph_id}: {e}")
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Query failed: {e!s}",
      )

  def query_table_streaming(self, request: TableQueryRequest, chunk_size: int = 1000):
    """
    Execute SQL query and yield results in chunks for TRUE streaming.

    IMPORTANT: Uses fetchmany() to avoid loading entire result set in memory.
    This enables efficient streaming of large result sets without memory exhaustion.

    Args:
        request: Query request
        chunk_size: Number of rows per chunk

    Yields:
        Dict containing chunk data with columns, rows, and metadata
    """
    import time

    start_time = time.time()

    logger.info(
      f"Executing streaming query for graph {request.graph_id}: {request.sql[:100]}..."
    )

    # Fetch everything (bounded by MAX_QUERY_ROWS) into memory FIRST — this holds
    # the per-graph read lock only for the duration of the fetch, on ONE thread —
    # then stream the in-memory buffer. We must NOT hold the read-only connection
    # or its lock across `yield`: the streaming response driver
    # (Starlette iterate_in_threadpool) resumes this generator on arbitrary worker
    # threads, and a threading lock released on a different thread than it was
    # acquired raises and wedges the lock permanently. Buffering is safe because
    # results are capped at MAX_QUERY_ROWS.
    try:
      columns, result = self._fetch_readonly(request)
    except HTTPException as e:
      yield {
        "error": e.detail,
        "error_type": "QueryError",
        "chunk_index": 0,
        "is_last_chunk": True,
        "row_count": 0,
        "total_rows_sent": 0,
        "execution_time_ms": (time.time() - start_time) * 1000,
      }
      return

    total_rows = len(result)
    total_rows_sent = 0
    chunk_index = 0

    for offset in range(0, total_rows, chunk_size):
      chunk_rows = [list(row) for row in result[offset : offset + chunk_size]]
      total_rows_sent += len(chunk_rows)
      is_last_chunk = total_rows_sent >= total_rows

      if chunk_index % 10 == 0 or is_last_chunk:
        logger.debug(
          f"Yielding chunk {chunk_index} with {len(chunk_rows)} rows "
          f"(total: {total_rows_sent})"
        )

      yield {
        "columns": columns,
        "rows": chunk_rows,
        "chunk_index": chunk_index,
        "is_last_chunk": is_last_chunk,
        "row_count": len(chunk_rows),
        "total_rows_sent": total_rows_sent,
        "execution_time_ms": (time.time() - start_time) * 1000,
      }
      chunk_index += 1

    logger.info(
      f"Streaming query completed: {total_rows_sent} rows in "
      f"{(time.time() - start_time) * 1000:.2f}ms"
    )

  def list_tables(self, graph_id: str) -> list[TableInfo]:
    logger.info(f"Listing tables for graph {graph_id}")

    pool = get_duckdb_pool()

    # Check if database exists by looking for connections or trying to connect
    try:
      with pool.get_connection(graph_id) as conn:
        result = conn.execute(
          "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()

        tables = []
        for (table_name,) in result:
          try:
            quoted_table = f'"{table_name}"'
            count_result = conn.execute(
              f"SELECT COUNT(*) FROM {quoted_table}"
            ).fetchone()
            row_count = count_result[0] if count_result else 0

            tables.append(
              TableInfo(
                graph_id=graph_id,
                table_name=table_name,
                row_count=row_count,
                size_bytes=0,
                s3_location=None,
              )
            )
          except Exception as e:
            logger.warning(f"Could not get info for table {table_name}: {e}")

        return tables

    except Exception as e:
      logger.error(f"Failed to list tables for graph {graph_id}: {e}")
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Failed to list tables: {e!s}",
      )

  @validate_table_name_decorator
  def refresh_table(self, graph_id: str, table_name: str) -> dict[str, Any]:
    """
    Refresh an external table from current PostgreSQL file registry.

    Rebuilds the table using the current list of files in GraphFile table.
    Use this after file additions, deletions, or replacements in S3.

    Args:
        graph_id: Graph database identifier
        table_name: Table name to refresh

    Returns:
        Dict with refresh details

    Raises:
        HTTPException: If table doesn't exist
    """
    import time

    from robosystems.database import SessionFactory
    from robosystems.models.core.graph.graph_file import GraphFile
    from robosystems.models.core.graph.graph_table import GraphTable

    validate_table_name(table_name)

    logger.info(f"Refreshing external table {table_name} for graph {graph_id}")

    pool = get_duckdb_pool()
    db = SessionFactory()

    try:
      graph_table = GraphTable.get_by_name(graph_id, table_name, db)
      if not graph_table:
        raise HTTPException(
          status_code=status.HTTP_404_NOT_FOUND,
          detail=f"Table {table_name} not found in registry",
        )

      files = GraphFile.get_all_for_table(graph_table.id, db)
      if not files:
        raise HTTPException(
          status_code=status.HTTP_400_BAD_REQUEST,
          detail=f"No files found for table {table_name}",
        )

      s3_keys = [f.s3_key for f in files if f.upload_status == "completed"]
      if not s3_keys:
        raise HTTPException(
          status_code=status.HTTP_400_BAD_REQUEST,
          detail=f"No completed files found for table {table_name}",
        )

      logger.info(
        f"Refreshing {table_name} with {len(s3_keys)} files from PostgreSQL registry"
      )

      with pool.get_connection(graph_id) as conn:
        quoted_table = f'"{table_name}"'
        start_time = time.time()

        conn.execute(f"DROP VIEW IF EXISTS {quoted_table}")
        conn.execute(f"DROP TABLE IF EXISTS {quoted_table}")

        s3_pattern_list = ", ".join(
          [f"'{key.replace(chr(39), chr(39) * 2)}'" for key in s3_keys]
        )
        # union_by_name=true handles schema variations between files
        create_view_sql = f"CREATE VIEW {quoted_table} AS SELECT * FROM read_parquet([{s3_pattern_list}], union_by_name=true)"
        conn.execute(create_view_sql)
        logger.info(f"Recreated external table {table_name} with {len(s3_keys)} files")

        execution_time_ms = (time.time() - start_time) * 1000

        count_result = conn.execute(f"SELECT COUNT(*) FROM {quoted_table}").fetchone()
        row_count = count_result[0] if count_result else 0

        return {
          "status": "success",
          "graph_id": graph_id,
          "table_name": table_name,
          "file_count": len(s3_keys),
          "row_count": row_count,
          "execution_time_ms": execution_time_ms,
          "message": f"Refreshed external table {table_name} from {len(s3_keys)} files",
        }

    except HTTPException:
      raise
    except Exception as e:
      logger.error(f"Failed to refresh table {table_name}: {e}")
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Failed to refresh table: {e!s}",
      )
    finally:
      db.close()

  @validate_table_name_decorator
  def delete_table(self, graph_id: str, table_name: str) -> dict[str, str]:
    # Explicit validation (decorator provides safety net)
    validate_table_name(table_name)

    logger.info(f"Deleting table {table_name} from graph {graph_id}")

    pool = get_duckdb_pool()

    try:
      with pool.get_connection(graph_id) as conn:
        quoted_table = f'"{table_name}"'
        conn.execute(f"DROP TABLE IF EXISTS {quoted_table}")
        conn.execute(f"DROP VIEW IF EXISTS {quoted_table}")

        logger.info(f"Deleted table {table_name} from graph {graph_id}")

        return {"status": "success", "message": f"Table {table_name} deleted"}

    except Exception as e:
      logger.error(f"Failed to delete table {table_name}: {e}")
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Failed to delete table: {e!s}",
      )

  def delete_file_data(
    self, graph_id: str, table_name: str, file_id: str
  ) -> dict[str, Any]:
    """
    Delete all rows for a specific file from DuckDB staging table.

    This is the core of incremental deletion - remove data from specific
    files without rebuilding the entire table.

    Args:
        graph_id: Graph database identifier
        table_name: Table name
        file_id: File identifier to delete

    Returns:
        Dict with status and rows_deleted count
    """
    validate_table_name(table_name)

    logger.info(
      f"Deleting data for file_id={file_id} from table {table_name} in graph {graph_id}"
    )

    pool = get_duckdb_pool()

    try:
      with pool.get_connection(graph_id) as conn:
        quoted_table = f'"{table_name}"'

        # Check if table exists
        table_check = conn.execute(
          "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
          [table_name],
        ).fetchone()

        if not table_check or table_check[0] == 0:
          logger.warning(
            f"Table {table_name} does not exist in graph {graph_id} - nothing to delete"
          )
          return {
            "status": "success",
            "rows_deleted": 0,
            "message": f"Table {table_name} does not exist",
          }

        # Check if file_id column exists
        columns_result = conn.execute(
          "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
          [table_name],
        ).fetchall()
        column_names = [row[0] for row in columns_result]

        if "file_id" not in column_names:
          logger.warning(
            f"Table {table_name} does not have file_id column - cannot delete by file"
          )
          raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Table {table_name} does not support file-level deletion (no file_id column)",
          )

        # Count rows before deletion
        count_result = conn.execute(
          f"SELECT COUNT(*) FROM {quoted_table} WHERE file_id = ?", [file_id]
        ).fetchone()
        count_before = count_result[0] if count_result else 0

        if count_before == 0:
          logger.info(
            f"No rows found for file_id={file_id} in table {table_name} - nothing to delete"
          )
          return {
            "status": "success",
            "rows_deleted": 0,
            "message": f"No rows found for file_id={file_id}",
          }

        # Delete rows by file_id
        conn.execute(f"DELETE FROM {quoted_table} WHERE file_id = ?", [file_id])

        # Verify deletion
        count_result_after = conn.execute(
          f"SELECT COUNT(*) FROM {quoted_table} WHERE file_id = ?", [file_id]
        ).fetchone()
        count_after = count_result_after[0] if count_result_after else 0

        rows_deleted = count_before - count_after

        logger.info(
          f"Deleted {rows_deleted} rows for file_id={file_id} from table {table_name}"
        )

        return {
          "status": "success",
          "rows_deleted": rows_deleted,
          "message": f"Deleted {rows_deleted} rows from {table_name}",
        }

    except HTTPException:
      raise
    except Exception as e:
      logger.error(f"Failed to delete file data from {table_name}: {e}")
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Failed to delete file data: {e!s}",
      )
