"""
LanceDB Vector Index Manager for Graph API.

Manages per-graph, per-table LanceDB IVF-PQ vector indexes on graph instances.
Follows the same lifecycle pattern as DuckDB staging:

  build:  Read embedding column from DuckDB staging → build IVF-PQ lance index
  search: Query the lance index by embedding similarity
  export: Package the lance index as tar.gz for S3 publish / replica download
  delete: Clean up lance index when graph is deleted or rebuilt

Directory structure on disk:
  {LANCE_INDEX_PATH}/
    {graph_id}/
      {table_name}.lance/    ← LanceDB directory (one per indexed table)

The embedding column convention:
  - Column must be named "embedding"
  - Type must be FLOAT[] (dimension auto-detected from data)
  - One embedding column per table (first detected is used)
  - Rows with NULL embeddings are excluded from the index
"""

from __future__ import annotations

import gc
import logging
import shutil
import tarfile
import time
from pathlib import Path

logger = logging.getLogger(__name__)

# Minimum rows to build IVF-PQ index; below this brute-force is fine
_MIN_ROWS_FOR_INDEX = 1000


class LanceManager:
  """Manages LanceDB vector indexes for graph databases.

  Each graph + table combination gets its own lance directory on disk.
  Indexes are built from DuckDB staging tables that contain embedding columns.
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
    """Get the lance directory for a graph."""
    return self.base_path / graph_id

  def _table_dir(self, graph_id: str, table_name: str) -> Path:
    """Get the lance directory for a specific table's index."""
    return self._graph_dir(graph_id) / f"{table_name}.lance"

  def _build_dir(self, graph_id: str, table_name: str) -> Path:
    """Get a temporary build directory (atomic swap on success)."""
    return self._graph_dir(graph_id) / f"{table_name}.lance.building"

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
    """Build a LanceDB IVF-PQ index from a DuckDB query.

    Runs the provided query against the DuckDB staging database and builds
    an IVF-PQ vector index from the results. The query must return a column
    named "vector" of type FLOAT[N] — all other columns are stored as
    metadata alongside the vectors.

    This keeps the Graph API generic — domain-specific filtering, dedup,
    and column selection are the caller's responsibility via the query.

    Uses atomic swap: builds to a temp directory, then renames on success.
    If an existing index is present, it remains queryable during the build.

    Args:
        graph_id: Graph database identifier.
        table_name: Name for the lance index (e.g., "Element"). Used as the
            LanceDB table name and directory name on disk.
        query: DuckDB SQL query that selects the rows to index. Must include
            a "vector" column (e.g., ``embedding::FLOAT[384] AS vector``).
        duckdb_path: Path to the DuckDB staging database. If None, uses
            the default staging path for this graph_id.
        memory_limit: DuckDB memory limit for the query.

    Returns:
        Dict with build results: row_count, index_size_bytes, duration_ms,
        num_partitions.

    Raises:
        ValueError: If the query returns no rows or no "vector" column.
        RuntimeError: If the build fails.
    """
    import duckdb
    import lancedb

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

    # Clean up any prior failed build
    if build_dir.exists():
      shutil.rmtree(build_dir)

    logger.info(f"Building lance index for {graph_id}/{table_name} from {duckdb_path}")

    # Step 1: Execute the caller's query against DuckDB staging
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
      con.execute(f"SET memory_limit = '{memory_limit}'")

      logger.info("Executing vector extraction query against DuckDB...")
      arrow_table = con.execute(query).fetch_arrow_table()
      row_count = arrow_table.num_rows

      # Validate the query returned a "vector" column
      col_names = [field.name for field in arrow_table.schema]
      if "vector" not in col_names:
        raise ValueError(
          'Query must return a "vector" column (e.g., embedding::FLOAT[384] AS vector)'
        )

      logger.info(f"Extracted {row_count:,} rows for lance index")

    finally:
      con.close()

    if row_count == 0:
      raise ValueError("Query returned no rows — cannot build vector index.")

    # Step 2: Build LanceDB table + IVF-PQ index
    build_dir.mkdir(parents=True, exist_ok=True)
    try:
      db = lancedb.connect(str(build_dir))
      table = db.create_table(table_name, data=arrow_table, mode="overwrite")

      # Free arrow table before building index
      del arrow_table
      gc.collect()

      # Build IVF-PQ index for large datasets
      num_partitions = 0
      if row_count > _MIN_ROWS_FOR_INDEX:
        num_partitions = min(256, row_count // 10)
        logger.info(
          f"Building IVF-PQ index (cosine, {num_partitions} partitions, "
          f"48 sub-vectors)..."
        )
        table.create_index(
          metric="cosine",
          num_partitions=num_partitions,
          num_sub_vectors=48,
        )
        logger.info("IVF-PQ index build complete")
      else:
        logger.info(f"Skipping IVF-PQ index ({row_count} rows, brute-force is fine)")

    except Exception as e:
      # Clean up failed build
      shutil.rmtree(build_dir, ignore_errors=True)
      raise RuntimeError(f"Lance index build failed: {e}") from e

    # Step 3: Atomic swap — replace old index with new one
    if final_dir.exists():
      backup_dir = graph_dir / f"{table_name}.lance.old"
      if backup_dir.exists():
        shutil.rmtree(backup_dir)
      final_dir.rename(backup_dir)
      try:
        build_dir.rename(final_dir)
        shutil.rmtree(backup_dir, ignore_errors=True)
      except Exception:
        # Restore old index on rename failure
        backup_dir.rename(final_dir)
        shutil.rmtree(build_dir, ignore_errors=True)
        raise
    else:
      build_dir.rename(final_dir)

    # Calculate index size
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

    Args:
        graph_id: Graph database identifier.
        table_name: Table name whose index to search.
        embedding: Query embedding vector.
        limit: Maximum results to return.
        select_columns: Columns to include in results. If None, returns all
            non-vector columns.

    Returns:
        Dict with results list, total count, and execution_time_ms.

    Raises:
        ValueError: If no index exists for this graph + table.
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
      # Always include _distance for scoring
      cols = list(select_columns)
      if "_distance" not in cols:
        cols.append("_distance")
      query = query.select(cols)

    results = query.with_row_id(False).to_list()

    elapsed_ms = (time.perf_counter() - start) * 1000

    # Normalize _distance to "distance" in output
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
    output_path: str | Path | None = None,
  ) -> dict:
    """Package a lance index as tar.gz for S3 upload / replica download.

    Args:
        graph_id: Graph database identifier.
        table_name: Table name whose index to export.
        output_path: Path for the output tar.gz. If None, writes to
            {LANCE_INDEX_PATH}/{graph_id}/{table_name}.lance.tar.gz

    Returns:
        Dict with tar_path, size_bytes, row_count.

    Raises:
        ValueError: If no index exists for this graph + table.
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
      # Archive with arcname so extraction gives {graph_id}/{table_name}.lance/
      tar.add(str(table_dir), arcname=f"{graph_id}/{table_name}.lance")

    tar_size = output_path.stat().st_size
    duration_ms = (time.time() - start_time) * 1000

    logger.info(
      f"Lance index exported: {tar_size / (1024**2):.1f} MB in "
      f"{duration_ms / 1000:.1f}s"
    )

    return {
      "graph_id": graph_id,
      "table_name": table_name,
      "tar_path": str(output_path),
      "size_bytes": tar_size,
      "size_mb": round(tar_size / (1024**2), 1),
      "duration_ms": round(duration_ms, 2),
    }

  # ---------------------------------------------------------------------------
  # Delete: clean up lance index
  # ---------------------------------------------------------------------------

  def delete(self, graph_id: str, table_name: str | None = None) -> dict:
    """Delete lance index for a graph (or a specific table).

    Args:
        graph_id: Graph database identifier.
        table_name: If provided, delete only this table's index.
            If None, delete all indexes for the graph.

    Returns:
        Dict with deleted paths.
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
