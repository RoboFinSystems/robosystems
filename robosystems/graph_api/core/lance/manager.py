"""Batch LanceDB IVF-PQ vector index builder for graph instances.

DORMANT — NOT DEAD CODE. Only ``delete`` is on a live path (called when a
database is dropped, to tear down the graph's whole lance directory including
memory). ``build``/``search``/``export`` have no caller: the live vector path
is LadybugDB-native in-graph HNSW (``CALL QUERY_VECTOR_INDEX``). This is kept
as the IVF-PQ foundation for the planned ``lance`` vector-store subgraph,
which pairs this batch side with the incremental-CRUD side of
``LanceMemoryStore``. Do not remove without retiring that plan.

Per-graph, per-table indexes with a lifecycle mirroring DuckDB staging:

  build:  DuckDB staging query → IVF-PQ lance index (atomic directory swap)
  search: query the index by embedding similarity
  export: package as tar.gz for S3 publish / replica download
  delete: remove the index when a graph is deleted or rebuilt

On disk:
  {LANCE_INDEX_PATH}/{graph_id}/{table_name}/{table_name}.lance/

Embedding column convention: one column named "embedding", type FLOAT[] with
the dimension auto-detected; rows with NULL embeddings are excluded.
"""

from __future__ import annotations

import gc
import logging
import shutil
import tarfile
import time
from pathlib import Path

from robosystems.utils.path_validation import (
  get_lance_index_path,
  validate_table_name,
)

logger = logging.getLogger(__name__)

# Minimum rows to build IVF-PQ index; below this brute-force is fine
_MIN_ROWS_FOR_INDEX = 1000


class LanceManager:
  """Build, search, export and delete LanceDB vector indexes.

  Each graph + table pair gets its own lance directory, built from a DuckDB
  staging table carrying an embedding column.
  """

  def __init__(self, base_path: str | None = None) -> None:
    if base_path is None:
      from robosystems.config import env

      base_path = env.LANCE_INDEX_PATH
    self.base_path = Path(base_path)
    self.base_path.mkdir(parents=True, exist_ok=True)

  # ---------------------------------------------------------------------------
  # Path helpers
  # ---------------------------------------------------------------------------

  def _graph_dir(self, graph_id: str) -> Path:
    """Get the lance directory for a graph (path-validated)."""
    return get_lance_index_path(graph_id, base_path=str(self.base_path))

  def _table_dir(self, graph_id: str, table_name: str) -> Path:
    """Get the lance DB directory for a specific table's index (path-validated).

    LanceDB creates {table_name}.lance/ inside this directory, so the full
    on-disk path is: {base_path}/{graph_id}/{table_name}/{table_name}.lance/
    """
    return get_lance_index_path(graph_id, table_name, base_path=str(self.base_path))

  def _build_dir(self, graph_id: str, table_name: str) -> Path:
    """Get a temporary build directory (atomic swap on success)."""
    return self._graph_dir(graph_id) / f"{validate_table_name(table_name)}.building"

  def index_exists(self, graph_id: str, table_name: str) -> bool:
    """Check if a lance index exists for this graph + table."""
    return self._table_dir(graph_id, table_name).is_dir()

  def get_index_info(self, graph_id: str, table_name: str) -> dict | None:
    """Get metadata about an existing index, or None if it doesn't exist."""
    table_dir = self._table_dir(graph_id, table_name)
    if not table_dir.is_dir():
      return None

    try:
      import lancedb

      db = lancedb.connect(str(table_dir))
      table = db.open_table(table_name)
      row_count = table.count_rows()
      size_bytes = sum(f.stat().st_size for f in table_dir.rglob("*") if f.is_file())
      return {
        "graph_id": graph_id,
        "table_name": table_name,
        "row_count": row_count,
        "size_bytes": size_bytes,
        "size_mb": round(size_bytes / (1024 * 1024), 1),
        "path": str(table_dir),
      }
    except Exception as e:
      logger.warning(f"Could not read index info for {graph_id}/{table_name}: {e}")
      return None

  # ---------------------------------------------------------------------------
  # Build: DuckDB staging → LanceDB index
  # ---------------------------------------------------------------------------

  def build(
    self,
    graph_id: str,
    table_name: str,
    query: str,
    duckdb_path: str | Path | None = None,
    memory_limit: str = "8GB",
  ) -> dict:
    """Build a LanceDB IVF-PQ index from a DuckDB staging query.

    ``query`` must return a "vector" column of type FLOAT[N] (e.g.
    ``embedding::FLOAT[384] AS vector``); the remaining columns are stored as
    metadata beside the vectors. Domain-specific filtering, dedup and column
    selection live in the query, which keeps this endpoint generic.

    Builds into a temp directory and renames on success, so an existing index
    stays queryable throughout and a failed build leaves it untouched.

    ``table_name`` names both the LanceDB table and its directory;
    ``duckdb_path`` defaults to the graph's staging database. Returns
    row_count, num_partitions, index_size_bytes and duration_ms. Raises
    ValueError on an empty result or missing "vector" column, RuntimeError if
    the index build itself fails.
    """
    import duckdb
    import lancedb
    from lancedb.index import IvfPq

    if duckdb_path is None:
      from robosystems.config.storage.shared import get_staging_duckdb_path

      duckdb_path = get_staging_duckdb_path(graph_id)
    duckdb_path = Path(duckdb_path)

    if not duckdb_path.exists():
      raise ValueError(
        f"DuckDB staging database not found: {duckdb_path}. "
        f"Run staging before building vector index."
      )

    start_time = time.time()
    graph_dir = self._graph_dir(graph_id)
    graph_dir.mkdir(parents=True, exist_ok=True)

    build_dir = self._build_dir(graph_id, table_name)
    final_dir = self._table_dir(graph_id, table_name)

    if build_dir.exists():
      shutil.rmtree(build_dir)

    logger.info(f"Building lance index for {graph_id}/{table_name} from {duckdb_path}")

    # Borrow the pool's connection rather than opening our own: DuckDB refuses
    # a second connection with a different config (read_only vs read_write, a
    # different memory_limit) on the same file, and the pool already holds the
    # one staging used.
    try:
      from robosystems.graph_api.core.duckdb import get_duckdb_pool

      pool = get_duckdb_pool()
      with pool.get_connection(graph_id) as con:
        logger.info("Executing vector extraction query against DuckDB (via pool)...")
        arrow_table = con.execute(query).fetch_arrow_table()
    except Exception:
      # No pool (tests, or the pool has not opened this database yet).
      logger.info("Pool unavailable, opening standalone DuckDB connection...")
      con = duckdb.connect(str(duckdb_path), read_only=True)
      try:
        con.execute(f"SET memory_limit = '{memory_limit}'")
        arrow_table = con.execute(query).fetch_arrow_table()
      finally:
        con.close()

    row_count = arrow_table.num_rows

    col_names = [field.name for field in arrow_table.schema]
    if "vector" not in col_names:
      raise ValueError(
        'Query must return a "vector" column (e.g., embedding::FLOAT[384] AS vector)'
      )

    logger.info(f"Extracted {row_count:,} rows for lance index")

    if row_count == 0:
      raise ValueError("Query returned no rows — cannot build vector index.")

    build_dir.mkdir(parents=True, exist_ok=True)
    try:
      db = lancedb.connect(str(build_dir))
      table = db.create_table(table_name, data=arrow_table, mode="overwrite")

      # Drop the Arrow buffers before the index build, which is the memory
      # peak — LanceDB has copied the data into the table by now.
      del arrow_table
      gc.collect()

      num_partitions = 0
      if row_count > _MIN_ROWS_FOR_INDEX:
        num_partitions = min(256, row_count // 10)
        logger.info(
          f"Building IVF-PQ index (cosine, {num_partitions} partitions, "
          f"48 sub-vectors)..."
        )
        table.create_index(
          "vector",
          config=IvfPq(
            distance_type="cosine",
            num_partitions=num_partitions,
            num_sub_vectors=48,
          ),
        )
        logger.info("IVF-PQ index build complete")
      else:
        logger.info(f"Skipping IVF-PQ index ({row_count} rows, brute-force is fine)")

    except Exception as e:
      shutil.rmtree(build_dir, ignore_errors=True)
      raise RuntimeError(f"Lance index build failed: {e}") from e

    # Atomic swap: the old index stays in place until the new one is complete.
    if final_dir.exists():
      backup_dir = graph_dir / f"{table_name}.old"
      if backup_dir.exists():
        shutil.rmtree(backup_dir)
      final_dir.rename(backup_dir)
      try:
        build_dir.rename(final_dir)
        shutil.rmtree(backup_dir, ignore_errors=True)
      except Exception:
        # Put the old index back if the rename fails.
        backup_dir.rename(final_dir)
        shutil.rmtree(build_dir, ignore_errors=True)
        raise
    else:
      build_dir.rename(final_dir)

    index_size = sum(f.stat().st_size for f in final_dir.rglob("*") if f.is_file())
    duration_ms = (time.time() - start_time) * 1000

    logger.info(
      f"Lance index built for {graph_id}/{table_name}: "
      f"{row_count:,} rows, {index_size / (1024**2):.1f} MB, "
      f"{duration_ms / 1000:.1f}s"
    )

    return {
      "graph_id": graph_id,
      "table_name": table_name,
      "row_count": row_count,
      "num_partitions": num_partitions,
      "index_size_bytes": index_size,
      "index_size_mb": round(index_size / (1024**2), 1),
      "duration_ms": round(duration_ms, 2),
      "path": str(final_dir),
    }

  # ---------------------------------------------------------------------------
  # Search: query the lance index
  # ---------------------------------------------------------------------------

  def search(
    self,
    graph_id: str,
    table_name: str,
    embedding: list[float],
    limit: int = 20,
    select_columns: list[str] | None = None,
  ) -> dict:
    """Search a lance index by embedding similarity.

    ``select_columns`` defaults to every non-vector column. Returns results,
    total and execution_time_ms; raises ValueError when the index is absent.
    """
    import lancedb

    table_dir = self._table_dir(graph_id, table_name)

    if not table_dir.is_dir():
      raise ValueError(
        f"No vector index for {graph_id}/{table_name}. Call vector/build first."
      )

    start = time.perf_counter()

    db = lancedb.connect(str(table_dir))
    table = db.open_table(table_name)

    query = table.search(embedding).limit(limit)

    if select_columns:
      # _distance is the score, so it is always fetched.
      cols = list(select_columns)
      if "_distance" not in cols:
        cols.append("_distance")
      query = query.select(cols)

    results = query.with_row_id(False).to_list()

    elapsed_ms = (time.perf_counter() - start) * 1000

    normalized = []
    for r in results:
      row = {k: v for k, v in r.items() if k != "_distance" and k != "vector"}
      row["distance"] = r.get("_distance", 0.0)
      normalized.append(row)

    return {
      "results": normalized,
      "total": len(normalized),
      "execution_time_ms": round(elapsed_ms, 2),
    }

  # ---------------------------------------------------------------------------
  # Export: package index as tar.gz for S3 publish
  # ---------------------------------------------------------------------------

  def export(
    self,
    graph_id: str,
    table_name: str,
    s3_bucket: str | None = None,
    s3_key: str | None = None,
    output_path: str | Path | None = None,
  ) -> dict:
    """Package a lance index as tar.gz and optionally upload it to S3.

    Given ``s3_bucket`` and ``s3_key``, the upload runs on this instance and
    the local tar.gz is deleted afterwards — the Dagster worker calling this
    endpoint cannot reach this instance's filesystem. ``output_path`` defaults
    to a temp path under {LANCE_INDEX_PATH}/{graph_id}/.

    Returns size_bytes, size_mb, duration_ms, plus s3_uri when uploaded.
    Raises ValueError when the index is absent.
    """
    table_dir = self._table_dir(graph_id, table_name)
    if not table_dir.is_dir():
      raise ValueError(
        f"No vector index for {graph_id}/{table_name}. Call vector/build first."
      )

    if output_path is None:
      output_path = self._graph_dir(graph_id) / f"{table_name}.lance.tar.gz"
    output_path = Path(output_path)

    start_time = time.time()
    logger.info(f"Exporting lance index {graph_id}/{table_name} to {output_path}")

    with tarfile.open(str(output_path), "w:gz") as tar:
      # arcname so extraction lands at {graph_id}/{table_name}/
      tar.add(str(table_dir), arcname=f"{graph_id}/{table_name}")

    tar_size = output_path.stat().st_size
    duration_ms = (time.time() - start_time) * 1000

    logger.info(
      f"Lance index exported: {tar_size / (1024**2):.1f} MB in "
      f"{duration_ms / 1000:.1f}s"
    )

    result = {
      "graph_id": graph_id,
      "table_name": table_name,
      "size_bytes": tar_size,
      "size_mb": round(tar_size / (1024**2), 1),
      "duration_ms": round(duration_ms, 2),
    }

    if s3_bucket and s3_key:
      import boto3

      from robosystems.config import env as _env

      s3 = boto3.client("s3", region_name=_env.AWS_REGION)
      logger.info(f"Uploading lance tar.gz to s3://{s3_bucket}/{s3_key}")
      s3.upload_file(str(output_path), s3_bucket, s3_key)

      head = s3.head_object(Bucket=s3_bucket, Key=s3_key)
      result["s3_uri"] = f"s3://{s3_bucket}/{s3_key}"
      result["s3_size_bytes"] = head["ContentLength"]
      logger.info(
        f"Lance index uploaded to S3: s3://{s3_bucket}/{s3_key} "
        f"({head['ContentLength'] / (1024**2):.1f} MB)"
      )

      output_path.unlink(missing_ok=True)

    return result

  # ---------------------------------------------------------------------------
  # Delete: clean up lance index
  # ---------------------------------------------------------------------------

  def delete(self, graph_id: str, table_name: str | None = None) -> dict:
    """Delete one table's lance index, or the graph's whole lance directory
    when ``table_name`` is omitted.

    The directory-wide form is what database drop calls, so it also removes
    the semantic-memory table (see ``memory_store.py``). Returns the paths
    deleted.
    """
    deleted = []

    if table_name:
      table_dir = self._table_dir(graph_id, table_name)
      if table_dir.exists():
        shutil.rmtree(table_dir)
        deleted.append(str(table_dir))
        logger.info(f"Deleted lance index: {graph_id}/{table_name}")
    else:
      graph_dir = self._graph_dir(graph_id)
      if graph_dir.exists():
        shutil.rmtree(graph_dir)
        deleted.append(str(graph_dir))
        logger.info(f"Deleted all lance indexes for graph: {graph_id}")

    return {"graph_id": graph_id, "deleted": deleted}
