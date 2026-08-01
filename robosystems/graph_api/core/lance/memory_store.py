"""LanceDB Semantic Memory Store for Graph API.

Incremental, per-graph CRUD over a single "memory" LanceDB table — the semantic
modality's first workload (AI memory). Distinct from ``LanceManager``, which is a
batch/overwrite/export index builder: this store is mutated row-by-row, never
overwritten, and is NOT part of the tar.gz replica-sync pipeline (all memory ops
route to the writer/master instance).

Directory structure on disk:
  {LANCE_INDEX_PATH}/{graph_id}/memory/memory.lance/

Timestamps are stored as int64 epoch-milliseconds (``*_ms``) rather than Arrow
timestamps, because rows cross a JSON boundary (main API → graph_api) where
datetimes would arrive as strings; epoch-ms round-trips cleanly and range-filters
trivially. The kernel derives ISO datetimes for API responses.

Predicate safety: memory ids are server-generated and matched against a strict
regex before interpolation; callers never pass raw WHERE strings for id lookups.
Recall/list predicates are built by the kernel from allowlisted typed fields only.
"""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path

from robosystems.utils.path_validation import get_lance_index_path

logger = logging.getLogger(__name__)

MEMORY_TABLE = "memory"
VECTOR_DIM = 384

_MEMORY_ID_RE = re.compile(r"^mem_[0-9a-f]{32}$")

# Full column set (schema order). Rows are completed against this before writes
# so a caller omitting an optional column can't produce an Arrow schema mismatch,
# and unknown keys are dropped.
_ALL_COLUMNS = (
  "id",
  "vector",
  "text",
  "created_at_ms",
  "updated_at_ms",
  "created_by",
  "source",
  "memory_type",
  "tags",
  "source_ref",
  "provenance",
)

# Columns returned to callers (never the raw vector).
_PUBLIC_COLUMNS = [c for c in _ALL_COLUMNS if c != "vector"]


class LanceMemoryStore:
  """Per-graph incremental CRUD over a single LanceDB "memory" table.

  One table per graph at ``{base}/{graph_id}/memory/``. Read methods tolerate an
  absent or empty table (return empty). Write methods create the table on first
  use with a fixed schema.
  """

  MEMORY_TABLE = MEMORY_TABLE
  VECTOR_DIM = VECTOR_DIM

  def __init__(self, base_path: str | None = None) -> None:
    if base_path is None:
      from robosystems.config import env

      base_path = env.LANCE_INDEX_PATH
    self.base_path = Path(base_path)
    self.base_path.mkdir(parents=True, exist_ok=True)

  # ---------------------------------------------------------------------------
  # Paths & schema
  # ---------------------------------------------------------------------------

  def _table_dir(self, graph_id: str) -> Path:
    """Path-validated lance directory for this graph's memory table."""
    return get_lance_index_path(
      graph_id, self.MEMORY_TABLE, base_path=str(self.base_path)
    )

  @staticmethod
  def _schema():
    import pyarrow as pa

    return pa.schema(
      [
        pa.field("id", pa.string(), nullable=False),
        pa.field("vector", pa.list_(pa.float32(), VECTOR_DIM), nullable=False),
        pa.field("text", pa.string(), nullable=False),
        pa.field("created_at_ms", pa.int64()),
        pa.field("updated_at_ms", pa.int64()),
        pa.field("created_by", pa.string()),
        pa.field("source", pa.string()),
        pa.field("memory_type", pa.string()),
        pa.field("tags", pa.list_(pa.string())),
        pa.field("source_ref", pa.string()),
        pa.field("provenance", pa.string()),
      ]
    )

  # ---------------------------------------------------------------------------
  # Predicate / input safety
  # ---------------------------------------------------------------------------

  @staticmethod
  def _validate_id(memory_id: str) -> str:
    # fullmatch (not match) so a trailing newline can't sneak through Python's
    # `$`, which matches before a final "\n".
    if not isinstance(memory_id, str) or not _MEMORY_ID_RE.fullmatch(memory_id):
      raise ValueError(f"Invalid memory id: {memory_id!r}")
    return memory_id

  def _complete_row(self, row: dict) -> dict:
    """Project a row onto the exact schema columns (missing → None)."""
    return {col: row.get(col) for col in _ALL_COLUMNS}

  def _check_vector(self, vec) -> None:
    if not isinstance(vec, list) or len(vec) != VECTOR_DIM:
      raise ValueError(f"vector must be a list of {VECTOR_DIM} floats")

  # ---------------------------------------------------------------------------
  # Connection
  # ---------------------------------------------------------------------------

  def _connect(self, graph_id: str, *, create: bool):
    """Return (db, table). When create=False and the table is absent, table=None."""
    import lancedb

    table_dir = self._table_dir(graph_id)
    if not create and not table_dir.is_dir():
      return None, None

    if create:
      table_dir.mkdir(parents=True, exist_ok=True)

    db = lancedb.connect(str(table_dir))
    # list_tables() returns a ListTablesResponse; membership lives on .tables
    if self.MEMORY_TABLE not in db.list_tables().tables:
      if not create:
        return db, None
      table = db.create_table(self.MEMORY_TABLE, schema=self._schema())
    else:
      table = db.open_table(self.MEMORY_TABLE)
    return db, table

  def exists(self, graph_id: str) -> bool:
    _, table = self._connect(graph_id, create=False)
    return table is not None

  def count(self, graph_id: str) -> int:
    _, table = self._connect(graph_id, create=False)
    return table.count_rows() if table is not None else 0

  # ---------------------------------------------------------------------------
  # Writes
  # ---------------------------------------------------------------------------

  def add_rows(self, graph_id: str, rows: list[dict]) -> dict:
    if not rows:
      return {"added": 0, "total": self.count(graph_id)}
    prepared = []
    for row in rows:
      self._validate_id(row.get("id", ""))
      self._check_vector(row.get("vector"))
      prepared.append(self._complete_row(row))
    _, table = self._connect(graph_id, create=True)
    table.add(prepared)
    return {"added": len(prepared), "total": table.count_rows()}

  def update(self, graph_id: str, memory_id: str, row: dict) -> dict:
    """Update an existing memory by id (never inserts).

    Uses ``when_not_matched_do_nothing`` so a concurrently-deleted memory is NOT
    resurrected (the kernel checks existence via a separate read first, so this
    is a strict update; the do-nothing branch closes that read→write TOCTOU).
    """
    self._validate_id(memory_id)
    merged = self._complete_row({**row, "id": memory_id})
    self._check_vector(merged.get("vector"))
    _, table = self._connect(graph_id, create=False)
    if table is None:
      return {"id": memory_id, "updated": False}
    # No when_not_matched clause → unmatched source rows are NOT inserted, so a
    # concurrently-deleted memory is not resurrected (update-only semantics).
    table.merge_insert("id").when_matched_update_all().execute([merged])
    updated = bool(table.search().where(f"id = '{memory_id}'").limit(1).to_list())
    return {"id": memory_id, "updated": updated}

  def delete(self, graph_id: str, memory_id: str) -> dict:
    self._validate_id(memory_id)
    _, table = self._connect(graph_id, create=False)
    if table is None or table.count_rows() == 0:
      return {"deleted": False}
    existed = bool(table.search().where(f"id = '{memory_id}'").limit(1).to_list())
    table.delete(f"id = '{memory_id}'")
    return {"deleted": existed}

  def delete_where(self, graph_id: str, where: str) -> dict:
    _, table = self._connect(graph_id, create=False)
    if table is None:
      return {"deleted": 0}
    before = table.count_rows()
    table.delete(where)
    return {"deleted": before - table.count_rows()}

  # ---------------------------------------------------------------------------
  # Reads
  # ---------------------------------------------------------------------------

  def search(
    self,
    graph_id: str,
    embedding: list[float],
    limit: int = 10,
    where: str | None = None,
    select_columns: list[str] | None = None,
  ) -> dict:
    self._check_vector(embedding)
    start = time.perf_counter()
    _, table = self._connect(graph_id, create=False)
    if table is None or table.count_rows() == 0:
      return {"results": [], "total": 0, "execution_time_ms": 0.0}

    query = table.search(embedding).metric("cosine")
    if where:
      query = query.where(where, prefilter=True)
    query = query.limit(limit)

    cols = list(select_columns) if select_columns else list(_PUBLIC_COLUMNS)
    if "_distance" not in cols:
      cols.append("_distance")
    query = query.select(cols)

    results = self._normalize(query.to_list())
    return {
      "results": results,
      "total": len(results),
      "execution_time_ms": round((time.perf_counter() - start) * 1000, 2),
    }

  def get(self, graph_id: str, memory_id: str) -> dict | None:
    self._validate_id(memory_id)
    _, table = self._connect(graph_id, create=False)
    if table is None or table.count_rows() == 0:
      return None
    rows = table.search().where(f"id = '{memory_id}'").limit(1).to_list()
    normalized = self._normalize(rows)
    return normalized[0] if normalized else None

  def list(
    self,
    graph_id: str,
    where: str | None = None,
    limit: int = 100,
    offset: int = 0,
  ) -> dict:
    _, table = self._connect(graph_id, create=False)
    if table is None or table.count_rows() == 0:
      return {"results": [], "total": 0}
    # total must reflect the FILTER, not the whole table, or filtered pagination
    # is wrong.
    total = table.count_rows(where) if where else table.count_rows()
    query = table.search()
    if where:
      query = query.where(where)
    query = query.limit(limit).offset(offset)
    return {"results": self._normalize(query.to_list()), "total": total}

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  @staticmethod
  def _normalize(rows: list[dict]) -> list[dict]:
    """Strip the raw vector; surface cosine distance as 'distance' when present."""
    out = []
    for r in rows:
      row = {k: v for k, v in r.items() if k not in ("vector", "_distance")}
      if "_distance" in r:
        row["distance"] = r["_distance"]
      out.append(row)
    return out
