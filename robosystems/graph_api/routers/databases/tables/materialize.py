"""Materialize DuckDB staging tables into LadybugDB, and fork a parent's staging
into a subgraph.

Both paths stream DuckDB → Arrow record batches → LadybugDB COPY with no
intermediate file. Materialization supports hash batching, per-file filters, and
an incremental mode that anti-joins against a keyset snapshot of the target
graph so only new rows are copied.
"""

import re
from pathlib import Path as FilePath

import pyarrow as pa
from fastapi import APIRouter, Body, Depends, HTTPException, Path
from fastapi import status as http_status

from robosystems.config import env
from robosystems.graph_api.core.duckdb import quote_identifier
from robosystems.graph_api.core.ladybug import get_ladybug_service
from robosystems.graph_api.models.fork import (
  ForkFromParentRequest,
  ForkFromParentResponse,
)
from robosystems.graph_api.models.tables import (
  TableMaterializationRequest,
  TableMaterializationResponse,
)
from robosystems.logger import logger
from robosystems.middleware.graph.instance_busy import (
  OP_KIND_MATERIALIZATION,
  instance_busy,
)

# Max rows per Arrow record batch streamed from DuckDB into a single LadybugDB
# COPY. Sized at/above the outer batchers (SEC hash-batching at
# MATERIALIZATION_BATCH_SIZE = 20M/call; tenant/direct chunked_materialization at
# the tier chunk_size_rows) so each already-bounded call materializes in ONE COPY.
# Per-table cost is dominated by COPY COUNT, not row throughput — each COPY is its
# own transaction + WAL commit + checkpoint check, roughly 8s regardless of size.
# Safe at this size only because the materialize connections SET
# arrow_large_buffer_size=true (in the get_connection blocks below): DuckDB then
# emits 64-bit LargeString offsets, lifting Arrow's 2 GiB (2^31) regular-string-
# buffer cap that a wide column like Fact.uri (~128 B/row XBRL concept URIs, not
# externalizable) otherwise overflows at ~19M rows, ABORTING the process.
# LadybugDB's COPY reader accepts Arrow LargeString.
ARROW_STREAM_BATCH_ROWS = 25_000_000


def _copy_result_rows(result, fallback: int) -> int:
  """Ingested-row count from a LadybugDB COPY result, or ``fallback`` if the
  result message can't be parsed."""
  try:
    if result is not None and hasattr(result, "get_as_arrow"):
      arrow_table = result.get_as_arrow()
      if arrow_table.num_rows > 0 and arrow_table.num_columns > 0:
        msg = str(arrow_table.column(0)[0].as_py())
        match = re.search(r"(\d+)\s+tuples?", msg)
        if match:
          return int(match.group(1))
  except Exception:
    return fallback
  return fallback


# graph_id and table_name reach the path builder from URL path params; both are
# registry/schema identifiers (alphanumeric + _ / -). Validate before using them
# to construct a filesystem path so a crafted value cannot escape the staging dir.
_SAFE_PATH_COMPONENT = re.compile(r"[A-Za-z0-9_-]+")


def _incr_keyset_path(graph_id: str, table_name: str) -> FilePath:
  """Local path for a table's incremental keyset snapshot.

  Under DUCKDB_STAGING_PATH (instance-local EBS, colocated with staging) but NOT
  a ``.duckdb``/``.lbug`` file, so it is never published to S3. Transient —
  deleted after the table's incremental run.
  """
  if not _SAFE_PATH_COMPONENT.fullmatch(graph_id):
    raise ValueError(f"Unsafe graph_id for keyset snapshot path: {graph_id!r}")
  if not _SAFE_PATH_COMPONENT.fullmatch(table_name):
    raise ValueError(f"Unsafe table_name for keyset snapshot path: {table_name!r}")
  return (
    FilePath(env.DUCKDB_STAGING_PATH) / "incr_keys" / graph_id / f"{table_name}.parquet"
  )


def _export_incremental_keyset(
  ladybug_service,
  graph_id: str,
  table_name: str,
  is_rel: bool,
  snapshot_path: FilePath,
) -> None:
  """Stream the target graph's existing keys for ``table_name`` into a parquet
  snapshot the DuckDB export SELECT can anti-join against, so only new rows are
  COPYed.

  Node keyset = ``identifier``; relationship keyset = ``(src, dst)`` via a
  traversal. Uses LadybugDB's server-side ``COPY (query) TO parquet`` so the
  whole keyset never materializes in Python — a 200M-edge keyset would be ~15 GB.
  An empty graph writes a 0-row parquet, so the anti-join passes every staged row
  and the first run degrades cleanly to a full load.
  """
  snapshot_path.parent.mkdir(parents=True, exist_ok=True)
  esc_path = str(snapshot_path).replace("'", "''")
  if is_rel:
    query = (
      f"MATCH (a)-[:{table_name}]->(b) RETURN a.identifier AS src, b.identifier AS dst"
    )
  else:
    query = f"MATCH (n:{table_name}) RETURN n.identifier AS identifier"
  with ladybug_service.db_manager.connection_pool.get_connection(graph_id) as conn:
    conn.execute("CALL timeout=3600000")  # 60 min: large rel traversals
    try:
      conn.execute(f"COPY ({query}) TO '{esc_path}'")
    finally:
      conn.execute("CALL timeout=120000")  # reset to 2 minutes


# Type mapping from LadybugDB types to DuckDB-compatible cast types
_LBUG_TO_DUCK_TYPE = {
  "STRING": "VARCHAR",
  "INT64": "BIGINT",
  "INT32": "INT",
  "DOUBLE": "DOUBLE",
  "BOOL": "BOOLEAN",
  "DATE": "DATE",
  "TIMESTAMP": "TIMESTAMP",
}


def _lbug_type_to_duck(lbug_type: str) -> str:
  """Map a LadybugDB column type to a DuckDB-compatible type for casting."""
  mapped = _LBUG_TO_DUCK_TYPE.get(lbug_type.upper())
  if mapped:
    return mapped
  # Pass through parameterized types like FLOAT[384] — DuckDB supports them natively
  if "[" in lbug_type:
    return lbug_type
  return "VARCHAR"


def _get_target_columns(
  ladybug_service, graph_id: str, table_name: str
) -> list[tuple[str, str]] | None:
  """Get column names and types from the LadybugDB target table.

  Returns list of (name, lbug_type) tuples, or None if unavailable.
  """
  try:
    with ladybug_service.db_manager.connection_pool.get_connection(graph_id) as conn:
      result = conn.execute(f"CALL TABLE_INFO('{table_name}') RETURN *")
      rows = result.get_as_list() if hasattr(result, "get_as_list") else list(result)
      columns = []
      for row in rows:
        # TABLE_INFO returns: [index, name, type, default, isPrimaryKey]
        if isinstance(row, (list, tuple)) and len(row) >= 3:
          columns.append((row[1], row[2]))
        elif isinstance(row, dict):
          values = next(iter(row.values())) if len(row) == 1 else row
          if isinstance(values, (list, tuple)) and len(values) >= 3:
            columns.append((values[1], values[2]))
      return columns if columns else None
  except Exception as exc:
    logger.debug(f"Could not get target columns for {table_name}: {exc}")
    return None


def _build_type_safe_select(
  source_columns: list[tuple[str, str]],
  exclude_cols: set[str] | None = None,
  null_cols: set[str] | None = None,
) -> str:
  """Build the export SELECT when the target LadybugDB schema is unknown.

  Preserves source column order and casts DECIMAL to DOUBLE — LadybugDB has no
  DECIMAL type (its numeric target columns are DOUBLE), and postgres_scan
  stages Postgres NUMERIC as DuckDB DECIMAL, so casting at the source keeps the
  Arrow types unambiguous. Columns in null_cols become typed NULLs (preserving
  column count for positional COPY).
  """
  exclude = exclude_cols or set()
  nullify = null_cols or set()
  parts = []
  for col_name, duck_type in source_columns:
    if col_name in exclude:
      continue
    quoted = quote_identifier(col_name)
    if col_name in nullify:
      parts.append(f"NULL::{duck_type} AS {quoted}")
    elif duck_type.upper().startswith("DECIMAL"):
      parts.append(f"CAST({quoted} AS DOUBLE) AS {quoted}")
    else:
      parts.append(quoted)
  return ", ".join(parts)


def _build_reconciled_select(
  target_columns: list[tuple[str, str]],
  source_column_names: list[str],
  source_table: str,
  exclude_cols: set[str] | None = None,
  null_cols: set[str] | None = None,
) -> str:
  """Build a SELECT expression that reconciles source columns to target schema.

  Adds missing columns as NULL with correct type. Casts existing columns
  to the target type (handles DuckDB inferring NULL columns as INT32).
  Columns in null_cols are included as NULL (preserving column count for COPY).

  For relationship tables, `from` and `to` are implicit in LadybugDB's
  TABLE_INFO but must be included in the DuckDB source. They are passed
  through first if present.
  """
  exclude = exclude_cols or set()
  nullify = null_cols or set()
  parts = []
  source_set = set(source_column_names)
  target_name_set = {c[0] for c in target_columns}

  # Pass through relationship key columns first — TABLE_INFO omits them for
  # rel tables but COPY requires them. DuckDB uses src/dst (from/to are reserved).
  for implicit_col in ("from", "to", "src", "dst"):
    if implicit_col in source_set and implicit_col not in target_name_set:
      parts.append(quote_identifier(implicit_col))

  for col_name, lbug_type in target_columns:
    if col_name in exclude:
      continue
    duck_type = _lbug_type_to_duck(lbug_type)
    quoted = quote_identifier(col_name)
    if col_name in nullify:
      parts.append(f"NULL::{duck_type} AS {quoted}")
    elif col_name in source_set:
      parts.append(f"TRY_CAST({quoted} AS {duck_type}) AS {quoted}")
    else:
      parts.append(f"NULL::{duck_type} AS {quoted}")
  return ", ".join(parts)


# Constants for checkpoint retry logic
CHECKPOINT_MAX_RETRIES = 3
CHECKPOINT_RETRY_DELAY_SECONDS = 1


def checkpoint_with_retry(conn, graph_id: str, context: str = "DuckDB") -> None:
  """Checkpoint a DuckDB database, retrying briefly before raising HTTP 500.

  ``context`` labels the connection in logs ("DuckDB", "parent DuckDB").
  """
  import time

  for attempt in range(CHECKPOINT_MAX_RETRIES):
    try:
      conn.execute("CHECKPOINT")
      logger.debug(f"[OK] {context} checkpointed successfully for {graph_id}")
      return
    except Exception as e:
      if attempt == CHECKPOINT_MAX_RETRIES - 1:
        logger.error(
          f"Failed to checkpoint {context} after {CHECKPOINT_MAX_RETRIES} attempts: {e}"
        )
        raise HTTPException(
          status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
          detail=f"Failed to checkpoint {context} after {CHECKPOINT_MAX_RETRIES} attempts: {e!s}",
        )
      logger.warning(f"Checkpoint attempt {attempt + 1} failed, retrying... Error: {e}")
      time.sleep(CHECKPOINT_RETRY_DELAY_SECONDS)


router = APIRouter(prefix="/databases/{graph_id}/tables")


@router.post("/{table_name}/materialize", response_model=TableMaterializationResponse)
async def materialize_table(
  graph_id: str = Path(..., description="Graph database identifier"),
  table_name: str = Path(..., description="Table name to materialize from DuckDB"),
  request: TableMaterializationRequest = Body(...),
  ladybug_service=Depends(get_ladybug_service),
) -> TableMaterializationResponse:
  import time

  start_time = time.time()

  logger.info(
    f"Materializing table {table_name} from DuckDB to LadybugDB graph {graph_id}"
  )

  if ladybug_service.read_only:
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Materialization not allowed on read-only nodes",
    )

  # Mark this instance busy so GHA pre-refresh workflows don't cycle the
  # container mid-materialization. Wraps the full body so the counter
  # decrements on exception too.
  async with instance_busy(env.INSTANCE_ID, OP_KIND_MATERIALIZATION):
    return await _materialize_table_impl(
      graph_id=graph_id,
      table_name=table_name,
      request=request,
      ladybug_service=ladybug_service,
      start_time=start_time,
    )


async def _materialize_table_impl(
  graph_id: str,
  table_name: str,
  request: TableMaterializationRequest,
  ladybug_service,
  start_time: float,
) -> TableMaterializationResponse:
  import time

  # Incremental keyset snapshot (assigned only in incremental mode); referenced
  # again in the cleanup section, so bind it up front.
  incremental = False
  snapshot_path: FilePath | None = None

  try:
    # Blue-green: read DuckDB from source graph if specified, otherwise from target
    duckdb_graph_id = request.source_graph_id or graph_id

    from robosystems.graph_api.core.duckdb import get_duckdb_pool

    duckdb_pool = get_duckdb_pool()

    # Target schema drives column reconciliation, which absorbs schema
    # evolution: staging tables missing new columns, or mistyped NULL columns
    # (DuckDB infers an all-NULL column as INT32 where the target wants
    # FLOAT[384]).
    target_columns = _get_target_columns(ladybug_service, graph_id, table_name)

    try:
      with duckdb_pool.get_connection(duckdb_graph_id) as duck_conn:
        # 64-bit Arrow string offsets: wide columns (e.g. Fact.uri ~128 B/row)
        # otherwise overflow Arrow's 2 GiB regular-string-buffer cap when a large
        # batch streams as one record batch, aborting the process.
        duck_conn.execute("SET arrow_large_buffer_size=true")
        # Flush WAL so the parquet export sees all committed staging data
        checkpoint_with_retry(duck_conn, duckdb_graph_id, context="DuckDB")

        result = duck_conn.execute(
          "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
          [table_name],
        ).fetchone()

        if not result or result[0] == 0:
          logger.info(
            f"Table {table_name} does not exist in DuckDB - skipping materialization (no data uploaded yet)"
          )
          return TableMaterializationResponse(
            status="skipped",
            graph_id=graph_id,
            table_name=table_name,
            rows_ingested=0,
            execution_time_ms=0.0,
          )

        source_columns = duck_conn.execute(
          "SELECT column_name, data_type FROM information_schema.columns "
          "WHERE table_name = ? ORDER BY ordinal_position",
          [table_name],
        ).fetchall()
        column_names = [col[0] for col in source_columns]
        has_file_id = "file_id" in column_names
        is_node = "identifier" in column_names
        is_rel = "src" in column_names and "dst" in column_names
        incremental = request.incremental and (is_node or is_rel)

        # Columns to NULL out (keep column for schema match, but skip data).
        # Embeddings stay in DuckDB staging for LanceDB vector search; materializing
        # them to LadybugDB is optional. Pass materialize_embeddings=true to include them.
        null_cols: set[str] = set()
        if "embedding" in column_names and not request.materialize_embeddings:
          null_cols.add("embedding")
          logger.info(
            f"Nulling embedding column for {table_name} materialization "
            f"(pass materialize_embeddings=true to include)"
          )

        batch_clause = ""
        has_hash_batching = (
          request.num_batches is not None and request.batch_num is not None
        )
        if has_hash_batching:
          assert request.batch_num is not None and request.num_batches is not None

          if is_node:
            hash_col = "identifier"
          elif is_rel:
            hash_col = "src || '|' || dst"
          else:
            hash_col = column_names[0] if column_names else "rowid"

          batch_clause = f" WHERE abs(hash({hash_col}::VARCHAR)) % {request.num_batches} = {request.batch_num}"
          logger.info(
            f"Using hash-based batching for {table_name}: batch {request.batch_num + 1}/{request.num_batches}"
          )

        file_filter = ""
        query_params: list[str] = []
        if request.file_ids and has_file_id:
          file_ids_placeholders = ", ".join(["?" for _ in request.file_ids])
          file_filter = f"file_id IN ({file_ids_placeholders})"
          query_params.extend(request.file_ids)

        # Incremental: export the target graph's existing keys to a parquet
        # snapshot (once per table, reused across hash batches) and anti-join the
        # export SELECT against it so ONLY new rows are COPYed. Mandatory for
        # correctness against a POPULATED graph — a duplicate node PK COPY
        # hard-fails, and a duplicate (src,dst) edge COPY silently duplicates.
        # An empty graph yields an empty snapshot, so the anti-join passes
        # everything and the first run is a full load.
        #
        # Mutable-attribute tables ride the same anti-join: NEW rows are added,
        # but a CHANGED attribute on an ALREADY-materialized node is NOT
        # refreshed here, and a node-level DELETE+re-COPY cannot fix it because
        # DETACH DELETE drops the node's edges. Attribute refresh belongs to the
        # periodic full reconciliation rebuild.
        incr_clause = ""
        if incremental:
          snapshot_path = _incr_keyset_path(graph_id, table_name)
          # Export once per table: on batch 0 / single-pass, or self-heal if a
          # later batch finds the snapshot missing (crash between batches).
          need_export = (
            not has_hash_batching
            or request.batch_num == 0
            or not snapshot_path.exists()
          )
          if need_export:
            _export_incremental_keyset(
              ladybug_service, graph_id, table_name, is_rel, snapshot_path
            )
          esc_snapshot = str(snapshot_path).replace("'", "''")
          if is_rel:
            incr_clause = (
              f"NOT EXISTS (SELECT 1 FROM read_parquet('{esc_snapshot}') k "
              "WHERE k.src = t.src AND k.dst = t.dst)"
            )
          else:
            incr_clause = (
              f"NOT EXISTS (SELECT 1 FROM read_parquet('{esc_snapshot}') k "
              "WHERE k.identifier = t.identifier)"
            )

        # Combine file-filter, hash-batch, and incremental anti-join predicates.
        where_fragments: list[str] = []
        if file_filter:
          where_fragments.append(file_filter)
        if batch_clause:
          where_fragments.append(batch_clause.replace(" WHERE ", "", 1))
        if incr_clause:
          where_fragments.append(incr_clause)
        where = (
          " WHERE " + " AND ".join(f"({frag})" for frag in where_fragments)
          if where_fragments
          else ""
        )

        exclude_cols: set[str] = set()
        if has_file_id:
          exclude_cols.add("file_id")

        # Build the export SELECT. When the LadybugDB target schema is known,
        # reconcile to it — target column order, NULLs for missing columns, and
        # casts to the target types (which also normalize postgres_scan-staged
        # NUMERIC to DOUBLE, since LadybugDB has no DECIMAL type).
        if target_columns:
          select_expr = _build_reconciled_select(
            target_columns,
            column_names,
            table_name,
            exclude_cols=exclude_cols,
            null_cols=null_cols,
          )
        else:
          select_expr = _build_type_safe_select(
            [(col[0], col[1]) for col in source_columns],
            exclude_cols=exclude_cols,
            null_cols=null_cols,
          )

        # Stream DuckDB → Arrow record batches → LadybugDB COPY, no intermediate
        # file. DuckDB hands its result vectors to Arrow (zero-copy for most
        # types) and LadybugDB reads those buffers directly, avoiding a parquet
        # serialization + disk round-trip; the write into LadybugDB's CSR storage
        # is still a copy, unavoidable for a persistent traversable graph.
        # Batching bounds peak memory.
        # The source is aliased `t` so the incremental anti-join subquery can
        # qualify outer columns (t.identifier / t.src / t.dst); single-table FROM,
        # so unqualified columns elsewhere still resolve to `t`.
        select_sql = f"SELECT {select_expr} FROM {table_name} AS t{where}"
        if query_params:
          arrow_reader = duck_conn.execute(select_sql, query_params).fetch_record_batch(
            ARROW_STREAM_BATCH_ROWS
          )
        else:
          arrow_reader = duck_conn.execute(select_sql).fetch_record_batch(
            ARROW_STREAM_BATCH_ROWS
          )

        if request.file_ids:
          logger.info(
            f"COPY {table_name} → {graph_id} ({len(request.file_ids)} file(s))"
          )
        elif has_hash_batching:
          assert request.batch_num is not None and request.num_batches is not None
          logger.info(
            f"COPY {table_name} → {graph_id} "
            f"(batch {request.batch_num + 1}/{request.num_batches})"
          )
        else:
          logger.info(f"COPY {table_name} → {graph_id}")

        rows_ingested = 0
        with ladybug_service.db_manager.connection_pool.get_connection(
          graph_id
        ) as conn:
          # Extended timeout: large tables (Fact) can take minutes per batch.
          try:
            conn.execute("CALL timeout=3600000")  # 60 minutes
            for arrow_batch in arrow_reader:
              # `copy_batch` is resolved BY NAME from this frame — LadybugDB
              # scans the Arrow object via a Python replacement scan, so the
              # local's name MUST match the identifier in the COPY statement.
              # The F841 suppression is load-bearing: the engine reads the
              # binding, the linter cannot see that.
              copy_batch = pa.Table.from_batches([arrow_batch])  # noqa: F841
              # No ignore_errors: LadybugDB's COPY (ignore_errors=true) silently
              # drops VALID rows in proportion to batch size (~37% at 20M).
              # Staging dedupes and all nodes load before any relationship, so a
              # plain COPY is both correct and complete. Do NOT re-add it.
              result = conn.execute(f"COPY {table_name} FROM copy_batch")
              rows_ingested += _copy_result_rows(result, arrow_batch.num_rows)
          finally:
            conn.execute("CALL timeout=120000")  # reset to 2 minutes

    except Exception as err:
      logger.error(f"Could not materialize DuckDB table {table_name}: {err}")
      raise

    execution_time_ms = (time.time() - start_time) * 1000
    logger.info(
      f"Materialized {rows_ingested} rows from {table_name} in {execution_time_ms / 1000:.1f}s"
    )

    # Checkpoint and release LadybugDB memory (prevents accumulation across a
    # multi-table materialization run).
    try:
      with ladybug_service.db_manager.connection_pool.get_connection(graph_id) as conn:
        conn.execute("CHECKPOINT")
        logger.debug(f"Checkpointed LadybugDB after {table_name} materialization")

        # Build vector index when embeddings are materialized (single-pass only).
        # For batched requests, the caller rebuilds the index after all batches
        # complete — creating it on batch 1 would only index partial data.
        is_batched = request.batch_num is not None
        if request.materialize_embeddings and not is_batched:
          ladybug_service.db_manager.create_vector_index(conn, table_name)
          conn.execute("CHECKPOINT")

      ladybug_service.db_manager.connection_pool.force_database_cleanup(
        graph_id, aggressive=True
      )
      logger.debug(f"Released LadybugDB memory after {table_name} materialization")
    except Exception as cleanup_err:
      # Log but don't fail — materialization already succeeded.
      logger.warning(f"Could not release LadybugDB memory: {cleanup_err}")

    # Release DuckDB staging connections.
    try:
      duckdb_pool.close_database_connections(duckdb_graph_id)
      logger.debug(f"Released DuckDB connections for {duckdb_graph_id}")
    except Exception as release_err:
      logger.warning(f"Failed to release DuckDB connections: {release_err}")

    # Incremental: drop the keyset snapshot after the last (or only) batch. On a
    # mid-run failure it lingers and is self-healed by the next batch-0 overwrite.
    if incremental and snapshot_path is not None:
      is_last_batch = (
        request.num_batches is None or request.batch_num == request.num_batches - 1
      )
      if is_last_batch:
        try:
          snapshot_path.unlink(missing_ok=True)
          logger.debug(f"Removed incremental keyset snapshot for {table_name}")
        except Exception as snap_err:
          logger.warning(f"Could not remove incremental keyset snapshot: {snap_err}")

    return TableMaterializationResponse(
      status="success",
      graph_id=graph_id,
      table_name=table_name,
      rows_ingested=rows_ingested,
      execution_time_ms=execution_time_ms,
    )

  except Exception as outer_err:
    logger.error(f"Failed during table materialization: {outer_err}")
    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to materialize table: {outer_err!s}",
    )


@router.post(
  "/{subgraph_id}/fork-from/{parent_graph_id}", response_model=ForkFromParentResponse
)
async def fork_from_parent_duckdb(
  graph_id: str = Path(
    ..., description="Graph database identifier (must match subgraph_id)"
  ),
  parent_graph_id: str = Path(..., description="Parent graph database identifier"),
  subgraph_id: str = Path(..., description="Subgraph database identifier"),
  request: ForkFromParentRequest = Body(...),
  ladybug_service=Depends(get_ladybug_service),
) -> ForkFromParentResponse:
  """
  Fork data from parent graph's DuckDB directly into subgraph's LadybugDB.

  This endpoint:
  1. Attaches parent graph's DuckDB staging database
  2. Copies specified tables (or all tables) from parent DuckDB to subgraph LadybugDB
  3. Runs on the same EC2 instance where both DuckDB and LadybugDB databases live

  Args:
      graph_id: Graph database identifier from router prefix (must equal subgraph_id)
      parent_graph_id: Parent graph to copy data from
      subgraph_id: Subgraph to copy data to
      request: Fork options (tables to copy, error handling)

  Returns:
      ForkFromParentResponse with tables copied and row counts
  """
  import time

  start_time = time.time()

  if graph_id != subgraph_id:
    raise HTTPException(
      status_code=http_status.HTTP_400_BAD_REQUEST,
      detail=f"graph_id ({graph_id}) must match subgraph_id ({subgraph_id})",
    )

  logger.info(f"Forking data from {parent_graph_id} DuckDB to {subgraph_id} LadybugDB")

  if ladybug_service.read_only:
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Fork not allowed on read-only nodes",
    )

  # Mark this instance busy so GHA pre-refresh workflows don't cycle the
  # container mid-fork: a fork is a multi-table DuckDB → LadybugDB COPY, as
  # destructive to interrupt as a materialization.
  async with instance_busy(env.INSTANCE_ID, OP_KIND_MATERIALIZATION):
    return await _fork_from_parent_duckdb_impl(
      parent_graph_id=parent_graph_id,
      subgraph_id=subgraph_id,
      request=request,
      ladybug_service=ladybug_service,
      start_time=start_time,
    )


async def _fork_from_parent_duckdb_impl(
  parent_graph_id: str,
  subgraph_id: str,
  request: ForkFromParentRequest,
  ladybug_service,
  start_time: float,
) -> ForkFromParentResponse:
  import time

  try:
    parent_duck_path = f"{env.DUCKDB_STAGING_PATH}/{parent_graph_id}.duckdb"

    # Checkpoint parent DuckDB to flush WAL and create views
    logger.info(f"Checkpointing parent DuckDB before fork: {parent_duck_path}")
    from robosystems.graph_api.core.duckdb import get_duckdb_pool

    duckdb_pool = get_duckdb_pool()

    with duckdb_pool.get_connection(parent_graph_id) as duck_conn:
      checkpoint_with_retry(duck_conn, parent_graph_id, context="parent DuckDB")

      result = duck_conn.execute("SHOW TABLES").fetchall()
      available_tables = [row[0] for row in result]

    if request.tables:
      tables_to_copy = [t for t in available_tables if t in request.tables]
    else:
      tables_to_copy = available_tables

    # Nodes must land before the relationships that reference them. Relationship
    # tables are all-uppercase (ENTITY_HAS_TRANSACTION); node tables are
    # PascalCase (Entity, Element, LineItem).
    node_tables = [t for t in tables_to_copy if not t.isupper()]
    rel_tables = [t for t in tables_to_copy if t.isupper()]
    tables_to_copy = node_tables + rel_tables

    logger.info(f"Found {len(tables_to_copy)} tables to fork: {tables_to_copy}")
    logger.info(f"  Node tables ({len(node_tables)}): {node_tables}")
    logger.info(f"  Relationship tables ({len(rel_tables)}): {rel_tables}")

    if not tables_to_copy:
      raise HTTPException(
        status_code=http_status.HTTP_400_BAD_REQUEST,
        detail="No tables to copy",
      )

    # Stream each parent table (excluding file_id) DuckDB → Arrow → subgraph
    # LadybugDB, no intermediate file. Do not use DuckDB ATTACH here: LadybugDB
    # writes persistent shadow catalog entries on ATTACH that collide with the
    # installed schema.
    total_rows = 0
    tables_copied: list[str] = []
    try:
      with (
        duckdb_pool.get_connection(parent_graph_id) as duck_conn,
        ladybug_service.db_manager.connection_pool.get_connection(subgraph_id) as conn,
      ):
        # See main path: 64-bit Arrow string offsets to avoid the 2 GiB cap.
        duck_conn.execute("SET arrow_large_buffer_size=true")
        for table_name in tables_to_copy:
          source_columns = duck_conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = ? ORDER BY ordinal_position",
            [table_name],
          ).fetchall()
          exclude_cols = (
            {"file_id"} if any(col[0] == "file_id" for col in source_columns) else set()
          )
          select_expr = _build_type_safe_select(
            [(col[0], col[1]) for col in source_columns],
            exclude_cols=exclude_cols,
          )
          arrow_reader = duck_conn.execute(
            f"SELECT {select_expr} FROM {table_name}"
          ).fetch_record_batch(ARROW_STREAM_BATCH_ROWS)

          table_rows = 0
          logger.info(f"Copying {table_name} from parent to subgraph")
          try:
            conn.execute("CALL timeout=3600000")  # 60 minutes
            for arrow_batch in arrow_reader:
              # `copy_batch` is resolved by name from this frame (replacement scan).
              copy_batch = pa.Table.from_batches([arrow_batch])  # noqa: F841
              # Plain COPY — see the note in materialize_table: LadybugDB's
              # ignore_errors path silently drops valid rows.
              result = conn.execute(f"COPY {table_name} FROM copy_batch")
              table_rows += _copy_result_rows(result, arrow_batch.num_rows)
          except Exception as table_err:
            # Fail fast: a copy error on clean, ordered staging is a real problem,
            # not something to swallow (that would silently lose a whole table).
            logger.error(f"Failed to copy {table_name}: {table_err}")
            raise
          finally:
            conn.execute("CALL timeout=120000")  # reset to 2 minutes

          total_rows += table_rows
          tables_copied.append(table_name)
          logger.info(f"[OK] Copied {table_name}: {table_rows} rows")

      execution_time_ms = (time.time() - start_time) * 1000
      logger.info(
        f"Fork completed: {len(tables_copied)} tables, {total_rows:,} rows in {execution_time_ms / 1000:.1f}s"
      )

      return ForkFromParentResponse(
        status="success",
        parent_graph_id=parent_graph_id,
        subgraph_id=subgraph_id,
        tables_copied=tables_copied,
        total_rows=total_rows,
        execution_time_ms=execution_time_ms,
      )

    except HTTPException:
      raise
    except Exception as e:
      logger.error(f"Failed to fork from {parent_graph_id} to {subgraph_id}: {e}")
      raise HTTPException(
        status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Failed to fork data: {e!s}",
      )

  except Exception as outer_err:
    logger.error(f"Failed during fork preparation: {outer_err}")
    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to fork data: {outer_err!s}",
    )
