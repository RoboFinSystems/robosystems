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
  """Reject any table name outside ``[A-Za-z0-9_-]+`` with a 400.

  Table names are interpolated into SQL, so this is the injection barrier.
  """
  if not table_name or not re.match(r"^[a-zA-Z0-9_-]+$", table_name):
    logger.warning(f"Invalid table name rejected: {table_name[:50]}")
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Invalid table name. Only alphanumeric, underscore, and hyphen allowed.",
    )


_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_IDENTIFIER_MAX_LENGTH = 128


def validate_column_names(column_names: list[str], *, source: str) -> None:
  """Refuse any probed column name outside ``[A-Za-z_][A-Za-z0-9_]*`` with a 400.

  Column names read from an uploaded file are tenant-controlled and are
  interpolated into staging DDL as identifiers. Quoting alone is not the
  barrier — rejection is, because it fails loudly, is testable, and does not
  depend on every interpolation site getting the escape right. The class
  matches what a graph property name can be, so nothing a legitimate file
  needs is excluded. ``source`` names the file or table in the error.
  """
  for name in column_names:
    if (
      not isinstance(name, str)
      or len(name) > _IDENTIFIER_MAX_LENGTH
      or not _IDENTIFIER_PATTERN.match(name)
    ):
      shown = repr(name)[:80]
      logger.warning(f"Invalid column name rejected in {source}: {shown}")
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=(
          f"Invalid column name {shown} in {source}. Column names must start "
          "with a letter or underscore and contain only letters, digits and "
          f"underscores (max {_IDENTIFIER_MAX_LENGTH} characters)."
        ),
      )


def quote_identifier(name: str) -> str:
  """Double-quote a SQL identifier, doubling any embedded quote.

  Every identifier interpolated into staging SQL goes through here, so a
  name that slipped past ``validate_column_names`` still cannot terminate the
  identifier it is quoted into. Defense in depth behind the validator, never
  a substitute for it.
  """
  return '"' + name.replace('"', '""') + '"'


def validate_table_name_decorator(func):
  """Validate a method's ``table_name``, whether passed by keyword, on a
  request object, or as the first positional argument."""

  @wraps(func)
  def wrapper(self, *args, **kwargs):
    if "table_name" in kwargs:
      validate_table_name(kwargs["table_name"])
    elif "request" in kwargs and hasattr(kwargs["request"], "table_name"):
      validate_table_name(kwargs["request"].table_name)
    elif len(args) > 0:
      if hasattr(args[0], "table_name"):
        validate_table_name(args[0].table_name)

    return func(self, *args, **kwargs)

  return wrapper


# Tenant read-query safety limits, enforced alongside the hardened read-only
# sandbox connection.
MAX_QUERY_ROWS = 10_000
QUERY_TIMEOUT_SECONDS = 30


def _strip_sql_noise(sql: str) -> str:
  """Blank out comments and string/identifier literals so structural checks
  (statement count, leading keyword) can't be fooled by their contents.

  A single linear character scan, deliberately regex-free: the tenant SQL path
  must not be turnable into a ReDoS. Comments collapse to a space; string and
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
  """Enforce the SELECT-only, single-statement contract with a 400.

  The hardened read-only + external-access-off connection already blocks writes
  and exfiltration at the engine level; this rejects non-read statements and
  statement chaining up front, for defense in depth and a clear error message.
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


def _dedup_col_expr(column: str, null_cols: set[str]) -> str:
  """``FIRST("c") AS "c"`` for a carried column, ``NULL AS "c"`` for one the
  caller asked to keep in the schema but skip the data of."""
  quoted = quote_identifier(column)
  return f"NULL AS {quoted}" if column in null_cols else f"FIRST({quoted}) AS {quoted}"


class DuckDBTableManager:
  """Create, load, query and drop the DuckDB staging tables that feed
  LadybugDB ingestion.

  Tables are built from parquet on S3 and dropped after materialization.
  Every method borrows a connection from ``get_duckdb_pool()``; tenant reads
  go through the pool's hardened read-only connection, everything else
  through the read-write one.
  """

  def __init__(self):
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
    """Build CREATE TABLE SQL that deduplicates via GROUP BY + FIRST().

    Deduplication is mandatory: materialization uses a plain COPY (LadybugDB
    0.18's ignore_errors path silently drops valid rows in proportion to batch
    size), and LadybugDB enforces unique node primary keys and unique
    relationship (from,to) pairs — any duplicate reaching it hard-fails the
    COPY. Dedup in staging is what keeps the clean COPY both safe and complete.

    Hash aggregation, not ROW_NUMBER: DuckDB can spill aggregate state to disk
    (https://duckdb.org/2024/03/29/external-aggregation) where the window
    function OOMs on large datasets.

    ``use_list`` selects a ``__FILES_PLACEHOLDER__`` the caller substitutes
    with a DuckDB list literal, versus a ``?`` bound as a glob pattern.
    """
    read_pattern = "__FILES_PLACEHOLDER__" if use_list else "?"

    if has_identifier and column_names:
      # Node table: dedupe on identifier. FIRST() picks an arbitrary row from
      # each group; union_by_name=true absorbs schema drift between files.
      null_cols = null_columns or set()
      dedupe_cols = ["identifier"]
      other_cols = [c for c in column_names if c not in dedupe_cols]
      select_parts = [quote_identifier(c) for c in dedupe_cols] + [
        _dedup_col_expr(c, null_cols) for c in other_cols
      ]
      select_clause = ", ".join(select_parts)
      group_by_clause = ", ".join(quote_identifier(c) for c in dedupe_cols)

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
        _dedup_col_expr(c, null_cols) for c in other_cols
      ]
      select_clause = ", ".join(select_parts)

      return f"""
        CREATE OR REPLACE TABLE {quoted_table} AS
        SELECT {select_clause}
        FROM read_parquet({read_pattern}, union_by_name=true, hive_partitioning=false)
        GROUP BY "from", "to"
      """
    else:
      # Unknown table type or no column names: read without deduplication.
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
    """Build CREATE TABLE SQL that UNION ALLs the files, stamping each row with
    its source ``file_id`` so ``delete_file_data`` can remove one file's rows
    without a full rebuild.

    Dedup is the same GROUP BY + FIRST() as ``_build_table_sql``, applied over
    the union; ``file_id_map`` maps s3_key to file_id.
    """
    selects = []

    for s3_key in s3_files:
      file_id = file_id_map.get(s3_key, "unknown")

      # Escape single quotes to prevent SQL injection via S3 keys or file IDs
      safe_s3_key = s3_key.replace("'", "''")
      safe_file_id = file_id.replace("'", "''")

      if has_identifier:
        # Node table: keep identifier, add file_id.
        select = f"""
          SELECT *, '{safe_file_id}' as file_id
          FROM read_parquet('{safe_s3_key}', union_by_name=true, hive_partitioning=false)
        """
      elif has_from_to:
        # Relationship table: rename from/to to src/dst, add file_id.
        select = f"""
          SELECT
            "from" as src,
            "to" as dst,
            * EXCLUDE ("from", "to"),
            '{safe_file_id}' as file_id
          FROM read_parquet('{safe_s3_key}', union_by_name=true, hive_partitioning=false)
        """
      else:
        # Unknown table: just add file_id.
        select = f"""
          SELECT *, '{safe_file_id}' as file_id
          FROM read_parquet('{safe_s3_key}', union_by_name=true, hive_partitioning=false)
        """

      selects.append(select)

    union_query = "\n UNION ALL\n".join(selects)

    if has_identifier and column_names:
      # Node table: dedupe on identifier, keep first file_id.
      null_cols = null_columns or set()
      dedupe_cols = ["identifier"]
      other_cols = [c for c in column_names if c not in dedupe_cols]
      select_parts = (
        [quote_identifier(c) for c in dedupe_cols]
        + [_dedup_col_expr(c, null_cols) for c in other_cols]
        + ["FIRST(file_id) AS file_id"]
      )
      select_clause = ", ".join(select_parts)
      group_by_clause = ", ".join(quote_identifier(c) for c in dedupe_cols)

      return f"""
        CREATE OR REPLACE TABLE {quoted_table} AS
        SELECT {select_clause}
        FROM ({union_query})
        GROUP BY {group_by_clause}
      """
    elif has_from_to and column_names:
      # Relationship table: dedupe on src/dst. column_names still carries the
      # original from/to names, but union_query has already renamed them.
      null_cols = null_columns or set()
      other_cols = [c for c in column_names if c not in ["from", "to"]]
      select_parts = (
        ["src", "dst"]
        + [_dedup_col_expr(c, null_cols) for c in other_cols]
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
      # No deduplication for unknown tables.
      return f"""
        CREATE OR REPLACE TABLE {quoted_table} AS
        {union_query}
      """

  @validate_table_name_decorator
  def create_table(self, request: TableCreateRequest) -> TableCreateResponse:
    """Materialize a staging table from parquet on S3.

    CREATE TABLE, never CREATE VIEW: S3 credentials are session-level in
    DuckDB and are not persisted in the .duckdb file, so when LadybugDB's
    DuckDB extension attaches the database it opens a new session with no S3
    config and views fail with "Unsupported duckdb type: NULL". A materialized
    table holds the data, so ingestion needs no S3 access.

    ``request.s3_pattern`` is either a wildcard string
    ("s3://bucket/path/*.parquet") or an explicit list of file paths; an
    optional ``file_id_map`` stamps each row with its source file for
    incremental deletion.
    """
    import time

    start_time = time.time()

    # Explicit validation; the decorator is the safety net.
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

        # Peek at the first file: an "identifier" column means a node table,
        # "from"/"to" means a relationship table. hive_partitioning=false stops
        # DuckDB auto-adding partition columns.
        sample_file = request.s3_pattern[0] if is_list else request.s3_pattern

        probe_result = conn.execute(
          "SELECT * FROM read_parquet(?, hive_partitioning=false) LIMIT 0",
          [sample_file],
        ).description
        column_names = [col[0] for col in probe_result]
        # Names read from an uploaded file are tenant-controlled; refuse
        # anything outside the identifier class before any SQL is built.
        validate_column_names(
          column_names, source=f"file {sample_file.rsplit('/', 1)[-1]}"
        )
        has_identifier = "identifier" in column_names
        has_from_to = "from" in column_names and "to" in column_names

        # Columns to NULL out (keep in schema, skip data)
        null_cols = set(request.null_columns) if request.null_columns else None

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
          # No file_id tracking.
          sql = self._build_table_sql(
            quoted_table,
            has_identifier,
            has_from_to,
            is_list,
            column_names,
            null_columns=null_cols,
          )

          if is_list:
            # DuckDB list syntax ['file1', 'file2', ...]; single quotes are
            # doubled so an S3 path cannot break out of the literal.
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
            # Parameter binding for the glob pattern.
            conn.execute(sql, [request.s3_pattern])

        count_result = conn.execute(f"SELECT COUNT(*) FROM {quoted_table}").fetchone()
        row_count = count_result[0] if count_result else 0

        # Flush the WAL and clear state: chunked ingestion follows CREATE with
        # a run of INSERTs, and the accumulated state eventually stalls.
        try:
          conn.execute("CHECKPOINT")
          logger.debug(f"Checkpointed DuckDB after CREATE {request.table_name}")
        except Exception as cp_err:
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

    except HTTPException:
      raise
    except Exception as e:
      logger.error(f"Failed to create table {request.table_name}: {e}")
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Failed to create table: {e!s}",
      )

  @validate_table_name_decorator
  def insert_into_table(self, request: TableCreateRequest) -> TableCreateResponse:
    """Append parquet files into a table that ``create_table`` already made.

    ``request.deduplicate=False`` (default) is a plain INSERT INTO append; the
    caller owns uniqueness. ``deduplicate=True`` adds
    ``... SELECT ... WHERE NOT EXISTS`` so rows whose dedup key is already in
    the target are skipped. The NOT EXISTS hash join holds only dedup-key
    strings, which DuckDB can spill to disk — that matters for tables with
    wide columns like FLOAT[384] embeddings, where aggregate state for
    fixed-size list columns cannot spill and OOMs past ~10M unique groups.

    Dedup key: "identifier" for node tables, src+dst (or from+to) for
    relationship tables, none for an unrecognized schema.

    Columns present in the parquet but missing from the table are added by
    ALTER TABLE (schema evolution); columns present only in the table are
    filled with NULL. Returns net rows added in ``row_count``.
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

        count_before = conn.execute(f"SELECT COUNT(*) FROM {quoted_table}").fetchone()
        rows_before = count_before[0] if count_before else 0

        # Single quotes are doubled so an S3 path cannot break out of the
        # literal.
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

        # Inject file_id into the SELECT so the INSERT matches the table
        # schema, which carries file_id from CREATE.
        file_id_value: str | None = None
        if request.file_id_map and is_list:
          # Single-file INSERT: take the file_id for the one file.
          for s3_key, fid in request.file_id_map.items():
            file_id_value = fid.replace("'", "''")
            break

        if request.deduplicate:
          # Detect table type from the existing schema to pick a dedup key.
          probe_result = conn.execute(
            f"SELECT * FROM {quoted_table} LIMIT 0"
          ).description
          column_names = [col[0] for col in probe_result]
          has_identifier = "identifier" in column_names
          has_src_dst = "src" in column_names and "dst" in column_names
          has_from_to = "from" in column_names and "to" in column_names

          # Probe the parquet schema too: CREATE TABLE renamed from/to →
          # src/dst for LadybugDB, but the parquet files still say from/to.
          parquet_probe = conn.execute(
            f"SELECT * FROM {parquet_read} LIMIT 0"
          ).description
          parquet_columns = [col[0] for col in parquet_probe]
          validate_column_names(parquet_columns, source="the uploaded file")
          parquet_has_from_to = "from" in parquet_columns and "to" in parquet_columns

          # Schema evolution: add parquet columns the table lacks, which
          # happens when a property was added to the schema after the last
          # full rebuild created this table.
          table_col_set = set(column_names)
          parquet_type_map = {col[0]: col[1] for col in parquet_probe}
          # Account for the from/to → src/dst rename done at CREATE TABLE.
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
                f"ALTER TABLE {quoted_table} ADD COLUMN "
                f"{quote_identifier(col_name)} {col_type}"
              )
              logger.info(
                f"Schema evolution: added column {col_name} ({col_type}) "
                f"to {request.table_name}"
              )

          # Dedupe twice: within the incoming batch (needed when two parquet
          # files, e.g. different quarters, carry the same row such as a
          # Taxonomy URI), then against rows already in the target.
          #
          # post_cols is the post-evolution column order: original columns
          # followed by newly added ones. The INSERT names its columns
          # explicitly — positional INSERT mismatches types when file_id sits
          # between original and schema-evolved columns.
          post_cols = list(column_names) + new_cols

          def _explicit_col_expr(table_col: str) -> str:
            # Table column → SELECT expr: file_id literal, src/dst rename, NULLs.
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
            quoted_col = quote_identifier(table_col)
            if null_cols and table_col in null_cols:
              return f"NULL AS {quoted_col}"
            # Target has a column the source parquet lacks — schema evolution
            # only adds source columns, never drops stale target-only ones.
            # NULL it rather than referencing a nonexistent source column,
            # which raises "Values list 't' does not have a column named ...".
            if table_col not in parquet_columns:
              return f"NULL AS {quoted_col}"
            return f"t.{quoted_col}"

          select_expr = ", ".join(_explicit_col_expr(c) for c in post_cols)
          insert_cols_clause = ", ".join(quote_identifier(c) for c in post_cols)

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
            # Unknown schema — no dedup key, plain append.
            intra_dedup = ""
            inter_dedup = ""

          # CTE dedupes within the batch; the outer query filters against
          # rows already in the table.
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
          # Plain append, with the same schema-evolution step.
          append_parquet_probe = conn.execute(
            f"SELECT * FROM {parquet_read} LIMIT 0"
          ).description
          append_columns = [col[0] for col in append_parquet_probe]
          validate_column_names(append_columns, source="the uploaded file")
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
                f"ALTER TABLE {quoted_table} ADD COLUMN "
                f"{quote_identifier(col_name)} {col_type}"
              )
              logger.info(
                f"Schema evolution: added column {col_name} ({col_type}) "
                f"to {request.table_name}"
              )

          file_id_suffix = f", '{file_id_value}' AS file_id" if file_id_value else ""

          if null_cols:
            append_expr = ", ".join(
              f"NULL AS {quote_identifier(c)}"
              if c in null_cols
              else quote_identifier(c)
              for c in append_columns
            )
            sql = f"INSERT INTO {quoted_table} SELECT {append_expr}{file_id_suffix} FROM {parquet_read}"
          else:
            sql = (
              f"INSERT INTO {quoted_table} SELECT *{file_id_suffix} FROM {parquet_read}"
            )

        conn.execute(sql)

        count_after = conn.execute(f"SELECT COUNT(*) FROM {quoted_table}").fetchone()
        rows_after = count_after[0] if count_after else 0
        rows_added = rows_after - rows_before

        # Without this checkpoint, chunked ingestion (many sequential INSERTs)
        # grows the WAL and accumulates connection state until it stalls.
        try:
          conn.execute("CHECKPOINT")
          logger.debug(f"Checkpointed DuckDB after INSERT into {request.table_name}")
        except Exception as cp_err:
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

    except HTTPException:
      raise
    except Exception as e:
      logger.error(f"Failed to insert into table {request.table_name}: {e}")
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Failed to insert into table: {e!s}",
      )

  def _fetch_readonly(self, request: TableQueryRequest) -> tuple[list[str], list]:
    """Validate and run a tenant read query on the hardened read-only
    connection, buffering up to MAX_QUERY_ROWS rows. Returns (columns, rows).

    The per-graph lock inside ``get_readonly_connection`` is acquired AND
    released here, on one thread. Results are fully buffered before returning:
    a ``threading`` lock released on a different thread than acquired it raises
    and wedges the lock permanently, and the streaming response driver resumes
    generators on arbitrary worker threads. Buffering is bounded by
    MAX_QUERY_ROWS, so it is safe.
    """
    validate_read_only_sql(request.sql)
    pool = get_duckdb_pool()
    timed_out = {"v": False}
    try:
      with pool.get_readonly_connection(request.graph_id) as conn:
        with _interrupt_after(conn, QUERY_TIMEOUT_SECONDS, timed_out):
          if request.parameters:
            cursor = conn.execute(request.sql, request.parameters)
          else:
            cursor = conn.execute(request.sql)
          # One past the cap, so truncation is detectable.
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
    tenant SQL on the hardened read-only connection (SELECT/WITH only,
    external access off, no httpfs/postgres_scanner), this runs on the
    read-write connection with httpfs + postgres_scanner loaded — what the
    extensions materializer and connector loaders need for
    ``CREATE TABLE AS SELECT ... postgres_scan(...)`` staging and INSERT/DELETE
    upserts.

    Not reachable from the tenant ``/query/sql`` path: only the internal graph
    client calls the ``/tables/execute`` endpoint this backs.
    """
    import time

    from robosystems.security.error_handling import redact_connection_secrets

    start_time = time.time()

    # postgres_scan() statements carry the extensions DSN in their leading
    # characters; redact before the preview rather than relying on the
    # truncation to happen to cut ahead of the password.
    logger.info(
      f"Executing write for graph {request.graph_id}: "
      f"{redact_connection_secrets(request.sql)[:100]}..."
    )

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
      from robosystems.security.error_handling import redact_connection_secrets

      # postgres_scan() statements carry the extensions DSN; a failure can echo
      # the statement, so scrub the credential before it goes to the log or the
      # 400 detail returned to the caller.
      safe = redact_connection_secrets(str(e))
      logger.error(f"Write failed for graph {request.graph_id}: {safe}")
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Query failed: {safe}",
      )

  def query_table_streaming(self, request: TableQueryRequest, chunk_size: int = 1000):
    """Run a tenant read query and yield `chunk_size`-row dicts with columns,
    rows and progress metadata.

    Chunking is over an already-buffered result, not the live cursor — see
    ``_fetch_readonly`` for why the connection cannot be held across a yield.
    """
    import time

    start_time = time.time()

    logger.info(
      f"Executing streaming query for graph {request.graph_id}: {request.sql[:100]}..."
    )

    # Fetch everything (bounded by MAX_QUERY_ROWS) first, holding the per-graph
    # read lock only for the fetch and only on this thread, then stream the
    # buffer. The connection and its lock must NOT be held across a `yield`:
    # Starlette's iterate_in_threadpool resumes this generator on arbitrary
    # worker threads, and a threading lock released on a different thread than
    # acquired it raises and wedges the lock permanently.
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
    """Rebuild a staging table from the completed files currently registered in
    the platform ``GraphFile`` table.

    Use after files are added, deleted, or replaced in S3. Unlike
    ``create_table`` this recreates the object as a VIEW over the parquet, so
    it is a read surface, not an ingestion source.
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
        # union_by_name=true absorbs schema drift between files.
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
    # Explicit validation; the decorator is the safety net.
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
    """Delete one source file's rows from a staging table.

    This is what makes deletion incremental: no table rebuild. Requires the
    table to carry a ``file_id`` column, which only tables created with a
    ``file_id_map`` have; otherwise this raises 400.
    """
    validate_table_name(table_name)

    logger.info(
      f"Deleting data for file_id={file_id} from table {table_name} in graph {graph_id}"
    )

    pool = get_duckdb_pool()

    try:
      with pool.get_connection(graph_id) as conn:
        quoted_table = f'"{table_name}"'

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

        conn.execute(f"DELETE FROM {quoted_table} WHERE file_id = ?", [file_id])

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
