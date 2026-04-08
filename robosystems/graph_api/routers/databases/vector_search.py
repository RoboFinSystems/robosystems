"""
Vector index management endpoints for Graph API.

Supports two backends via explicit `backend` field:

  - **lance** (default): LanceDB IVF-PQ indexes built from DuckDB staging queries.
    Used for shared repositories (SEC) where replicas need exported indexes.

  - **hnsw**: LadybugDB native HNSW indexes on materialized graph data.
    Used for user/custom graphs where vector search runs via Cypher
    (CALL QUERY_VECTOR_INDEX).

Endpoints:

  POST /databases/{graph_id}/tables/{table_name}/vector/build
    Build vector index (lance: from DuckDB query, hnsw: on materialized table)

  POST /databases/{graph_id}/tables/{table_name}/vector/search
    Query by embedding similarity

  POST /databases/{graph_id}/tables/{table_name}/vector/export
    Package lance index as tar.gz for S3 publish (lance only)

  DELETE /databases/{graph_id}/tables/{table_name}/vector
    Delete the vector index

  GET /databases/{graph_id}/tables/{table_name}/vector
    Get index metadata
"""

import asyncio
import os
import re
import time
from typing import Literal

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel, Field, field_validator, model_validator

from robosystems.logger import logger

router = APIRouter(prefix="/databases", tags=["Vector Index"])


# ---------------------------------------------------------------------------
# Request/response models
# ---------------------------------------------------------------------------


class VectorBuildRequest(BaseModel):
  """Request to build a vector index."""

  backend: Literal["lance", "hnsw"] = Field(
    default="lance",
    description="Vector backend. 'lance' builds IVF-PQ from a DuckDB query. "
    "'hnsw' builds a LadybugDB HNSW index on the materialized table.",
  )
  query: str | None = Field(
    default=None,
    description='DuckDB SQL query that selects rows to index (lance only). Must return a "vector" '
    "column (e.g., embedding::FLOAT[384] AS vector). All other columns are "
    "stored as searchable metadata.",
    max_length=10000,
  )
  column: str = Field(
    default="embedding",
    description="Embedding column name (hnsw only).",
  )
  memory_limit: str = Field(
    default="8GB",
    description="DuckDB memory limit for the extraction query (lance only).",
  )

  @field_validator("memory_limit")
  @classmethod
  def validate_memory_limit(cls, v: str) -> str:
    if not re.match(r"^\d+(?:\.\d+)?\s*(?:B|KB|MB|GB|TB)$", v, re.IGNORECASE):
      raise ValueError(
        f"Invalid memory_limit format: '{v}'. Expected format: '8GB', '2048MB', etc."
      )
    return v

  @model_validator(mode="after")
  def validate_backend_fields(self):
    if self.backend == "lance" and not self.query:
      raise ValueError("'query' is required when backend='lance'")
    return self

  class Config:
    extra = "forbid"


class VectorBuildResponse(BaseModel):
  """Response from vector index build."""

  graph_id: str
  table_name: str
  row_count: int
  num_partitions: int = 0
  index_size_mb: float = 0.0
  duration_ms: float
  backend: str = "lance"


class VectorSearchRequest(BaseModel):
  """Request to search the vector index."""

  backend: Literal["lance", "hnsw"] = Field(
    default="lance",
    description="Vector backend to search.",
  )
  embedding: list[float] = Field(
    ...,
    description="Query embedding vector",
    min_length=1,
  )
  limit: int = Field(
    default=20,
    description="Maximum number of results",
    ge=1,
    le=100,
  )
  select: list[str] | None = Field(
    default=None,
    description="Columns to include in results. If omitted, returns all non-vector columns.",
  )
  column: str = Field(
    default="embedding",
    description="Embedding column name (hnsw only).",
  )

  class Config:
    extra = "forbid"


class VectorSearchResponse(BaseModel):
  """Response from vector search."""

  results: list[dict]
  total: int
  execution_time_ms: float


class VectorExportRequest(BaseModel):
  """Request to export a vector index, optionally uploading to S3."""

  s3_bucket: str | None = Field(
    default=None,
    description="S3 bucket to upload the tar.gz to. If provided with s3_key, "
    "the upload runs on this instance (required for Dagster workers that "
    "cannot access this instance's filesystem).",
  )
  s3_key: str | None = Field(
    default=None,
    description="S3 object key for the upload.",
  )

  @model_validator(mode="after")
  def validate_s3_params(self):
    if bool(self.s3_bucket) != bool(self.s3_key):
      raise ValueError("s3_bucket and s3_key must both be provided or both omitted")
    return self

  class Config:
    extra = "forbid"


class VectorExportResponse(BaseModel):
  """Response from vector index export."""

  graph_id: str
  table_name: str
  size_mb: float
  duration_ms: float
  s3_uri: str | None = None


class VectorIndexInfo(BaseModel):
  """Metadata about an existing vector index."""

  graph_id: str
  table_name: str
  row_count: int
  size_mb: float
  path: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_lance_manager = None


def _get_lance_manager():
  """Lazy-load the LanceManager singleton."""
  global _lance_manager
  if _lance_manager is None:
    from robosystems.graph_api.core.lance import LanceManager

    _lance_manager = LanceManager()
    logger.info(f"LanceManager initialized (base_path={_lance_manager.base_path})")
  return _lance_manager


def _get_ladybug_service():
  """Lazy-load the LadybugDB service."""
  from robosystems.graph_api.core.ladybug import get_ladybug_service

  return get_ladybug_service()


def _require_writer():
  """Raise 501 if called on a read-only replica."""
  if os.getenv("LBUG_ROLE") == "replica":
    raise HTTPException(
      status_code=status.HTTP_501_NOT_IMPLEMENTED,
      detail="Vector build/export not available on read-only replicas",
    )


# ---------------------------------------------------------------------------
# HNSW backend operations
# ---------------------------------------------------------------------------


def _build_hnsw_index(
  graph_id: str, table_name: str, column: str = "embedding"
) -> dict:
  """Build/rebuild HNSW vector index on a materialized LadybugDB table."""
  ladybug_service = _get_ladybug_service()
  start = time.time()

  with ladybug_service.db_manager.connection_pool.get_connection(graph_id) as conn:
    success = ladybug_service.db_manager.rebuild_vector_index(conn, table_name, column)
    if not success:
      raise RuntimeError(
        f"Failed to build HNSW index on {table_name}.{column} — "
        f"table may not exist or have no embedding column"
      )

    # Get row count for the response
    try:
      result = conn.execute(f"MATCH (n:{table_name}) RETURN COUNT(n)")
      rows = result.get_as_list() if hasattr(result, "get_as_list") else list(result)
      first = list(rows[0]) if rows else []
      row_count = int(first[0]) if first else 0
    except Exception:
      row_count = 0

    conn.execute("CHECKPOINT")

  duration_ms = (time.time() - start) * 1000
  return {
    "graph_id": graph_id,
    "table_name": table_name,
    "row_count": row_count,
    "duration_ms": duration_ms,
  }


_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_column_names(columns: list[str]) -> None:
  """Validate column names are safe identifiers (no Cypher injection)."""
  for col in columns:
    if not _SAFE_IDENTIFIER.match(col):
      raise ValueError(f"Invalid column name: {col!r}")


def _search_hnsw_index(
  graph_id: str,
  table_name: str,
  embedding: list[float],
  limit: int,
  column: str = "embedding",
  select_columns: list[str] | None = None,
) -> dict:
  """Search HNSW vector index via LadybugDB QUERY_VECTOR_INDEX."""
  if select_columns:
    _validate_column_names(select_columns)

  ladybug_service = _get_ladybug_service()
  index_name = f"{table_name.lower()}_vec_index"
  start = time.time()

  vec_str = str(embedding)

  # Build RETURN clause from select columns
  if select_columns:
    return_parts = [f"node.{col} AS {col}" for col in select_columns]
  else:
    return_parts = ["node"]
  return_parts.append("distance")
  return_clause = ", ".join(return_parts)

  # Over-fetch then limit (HNSW returns approximate results)
  fetch_limit = min(limit * 2, 200)

  query = (
    f"CALL QUERY_VECTOR_INDEX('{table_name}', '{index_name}', {vec_str}, {fetch_limit}) "
    f"WITH node, distance "
    f"RETURN {return_clause} "
    f"ORDER BY distance LIMIT {limit}"
  )

  with ladybug_service.db_manager.connection_pool.get_connection(graph_id) as conn:
    try:
      conn.execute("LOAD EXTENSION vector")
    except Exception:
      conn.execute("INSTALL vector")
      conn.execute("LOAD EXTENSION vector")

    result = conn.execute(query)

    # Parse results
    results = []
    if hasattr(result, "get_as_list"):
      rows = result.get_as_list()
      if select_columns:
        col_names = [*select_columns, "distance"]
        for row in rows:
          results.append(dict(zip(col_names, row, strict=False)))
      else:
        for row in rows:
          if isinstance(row[0], dict):
            entry = {**row[0], "distance": row[1]}
          else:
            entry = {"node": row[0], "distance": row[1]}
          results.append(entry)

  execution_time_ms = (time.time() - start) * 1000
  return {
    "results": results,
    "total": len(results),
    "execution_time_ms": execution_time_ms,
  }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get(
  "/{graph_id}/tables/{table_name}/vector",
  response_model=VectorIndexInfo | None,
  summary="Get vector index metadata",
)
async def vector_info(
  graph_id: str,
  table_name: str,
  backend: Literal["lance", "hnsw"] = Query(
    default="lance", description="Vector backend"
  ),
) -> VectorIndexInfo | None:
  """Get metadata about an existing vector index (row count, size, etc.)."""
  if backend == "hnsw":
    # HNSW indexes don't have standalone metadata — check if the index exists
    try:
      ladybug_service = _get_ladybug_service()
      index_name = f"{table_name.lower()}_vec_index"
      with ladybug_service.db_manager.connection_pool.get_connection(graph_id) as conn:
        try:
          conn.execute("LOAD EXTENSION vector")
        except Exception:
          conn.execute("INSTALL vector")
          conn.execute("LOAD EXTENSION vector")

        # Verify the HNSW index actually exists by probing it
        # A zero-length probe is cheaper than counting all nodes
        try:
          probe_vec = [0.0] * 384  # standard embedding dimension
          conn.execute(
            f"CALL QUERY_VECTOR_INDEX('{table_name}', '{index_name}', {probe_vec}, 1) "
            f"RETURN node, distance"
          )
        except Exception as probe_err:
          if (
            "doesn't have an index" in str(probe_err)
            or "not found" in str(probe_err).lower()
          ):
            return None
          raise

        result = conn.execute(f"MATCH (n:{table_name}) RETURN COUNT(n)")
        rows = result.get_as_list() if hasattr(result, "get_as_list") else list(result)
        first = list(rows[0]) if rows else []
        row_count = int(first[0]) if first else 0

      return VectorIndexInfo(
        graph_id=graph_id,
        table_name=table_name,
        row_count=row_count,
        size_mb=0.0,
        path=f"hnsw://{graph_id}/{index_name}",
      )
    except Exception:
      return None

  manager = _get_lance_manager()
  info = manager.get_index_info(graph_id, table_name)
  if info is None:
    return None

  return VectorIndexInfo(
    graph_id=info["graph_id"],
    table_name=info["table_name"],
    row_count=info["row_count"],
    size_mb=info["size_mb"],
    path=info["path"],
  )


@router.delete(
  "/{graph_id}/tables/{table_name}/vector",
  summary="Delete vector index",
)
async def vector_delete(
  graph_id: str,
  table_name: str,
  backend: Literal["lance", "hnsw"] = Query(
    default="lance", description="Vector backend"
  ),
) -> dict:
  """Delete the vector index for a specific table."""
  _require_writer()

  if backend == "hnsw":
    ladybug_service = _get_ladybug_service()
    index_name = f"{table_name.lower()}_vec_index"
    try:
      with ladybug_service.db_manager.connection_pool.get_connection(graph_id) as conn:
        try:
          conn.execute("LOAD EXTENSION vector")
        except Exception:
          conn.execute("INSTALL vector")
          conn.execute("LOAD EXTENSION vector")
        conn.execute(f"CALL DROP_VECTOR_INDEX('{table_name}', '{index_name}')")
        conn.execute("CHECKPOINT")
      return {"status": "deleted", "graph_id": graph_id, "table_name": table_name}
    except Exception as e:
      if "doesn't have an index" in str(e):
        return {"status": "not_found", "graph_id": graph_id, "table_name": table_name}
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Failed to delete HNSW index: {e}",
      ) from e

  manager = _get_lance_manager()
  result = manager.delete(graph_id=graph_id, table_name=table_name)
  return result


@router.post(
  "/{graph_id}/tables/{table_name}/vector/build",
  response_model=VectorBuildResponse,
  summary="Build vector index",
)
async def vector_build(
  graph_id: str,
  table_name: str,
  request: VectorBuildRequest,
) -> VectorBuildResponse:
  """Build a vector index.

  With backend='lance': runs a DuckDB SQL query and builds a LanceDB IVF-PQ index.
  With backend='hnsw': builds/rebuilds a LadybugDB HNSW index on the materialized table.

  Only available on writer/master instances.
  """
  _require_writer()

  if request.backend == "hnsw":
    try:
      result = await asyncio.to_thread(
        _build_hnsw_index,
        graph_id=graph_id,
        table_name=table_name,
        column=request.column,
      )
    except RuntimeError as e:
      raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e

    return VectorBuildResponse(
      graph_id=result["graph_id"],
      table_name=result["table_name"],
      row_count=result["row_count"],
      duration_ms=result["duration_ms"],
      backend="hnsw",
    )

  # Lance backend
  assert request.query is not None  # validated by model
  manager = _get_lance_manager()

  try:
    result = await asyncio.to_thread(
      manager.build,
      graph_id=graph_id,
      table_name=table_name,
      query=request.query,
      memory_limit=request.memory_limit,
    )
  except ValueError as e:
    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
  except RuntimeError as e:
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
    ) from e

  return VectorBuildResponse(
    graph_id=result["graph_id"],
    table_name=result["table_name"],
    row_count=result["row_count"],
    num_partitions=result["num_partitions"],
    index_size_mb=result["index_size_mb"],
    duration_ms=result["duration_ms"],
    backend="lance",
  )


@router.post(
  "/{graph_id}/tables/{table_name}/vector/search",
  response_model=VectorSearchResponse,
  summary="Search vector index by embedding similarity",
)
async def vector_search(
  graph_id: str,
  table_name: str,
  request: VectorSearchRequest,
) -> VectorSearchResponse:
  """Search for similar rows by embedding vector.

  With backend='lance': queries LanceDB IVF-PQ index (~5ms latency).
  With backend='hnsw': queries LadybugDB HNSW index via QUERY_VECTOR_INDEX.
  """
  if request.backend == "hnsw":
    try:
      result = await asyncio.to_thread(
        _search_hnsw_index,
        graph_id=graph_id,
        table_name=table_name,
        embedding=request.embedding,
        limit=request.limit,
        column=request.column,
        select_columns=request.select,
      )
    except Exception as e:
      if "doesn't have an index" in str(e) or "not found" in str(e).lower():
        raise HTTPException(
          status_code=status.HTTP_404_NOT_FOUND,
          detail=f"No HNSW index found for {table_name}",
        ) from e
      logger.error(f"HNSW vector search failed: {e}", exc_info=True)
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Vector search failed: {e}",
      ) from e

    return VectorSearchResponse(
      results=result["results"],
      total=result["total"],
      execution_time_ms=result["execution_time_ms"],
    )

  # Lance backend
  manager = _get_lance_manager()

  try:
    result = manager.search(
      graph_id=graph_id,
      table_name=table_name,
      embedding=request.embedding,
      limit=request.limit,
      select_columns=request.select,
    )
  except ValueError as e:
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e)) from e
  except Exception as e:
    logger.error(f"Vector search failed: {e}", exc_info=True)
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Vector search failed: {e}",
    ) from e

  return VectorSearchResponse(
    results=result["results"],
    total=result["total"],
    execution_time_ms=result["execution_time_ms"],
  )


@router.post(
  "/{graph_id}/tables/{table_name}/vector/export",
  response_model=VectorExportResponse,
  summary="Export vector index as tar.gz",
)
async def vector_export(
  graph_id: str,
  table_name: str,
  request: VectorExportRequest | None = None,
) -> VectorExportResponse:
  """Package the lance index as tar.gz and optionally upload to S3.

  Only available for lance backend. HNSW indexes live inside LadybugDB
  and don't need separate export.

  Only available on writer/master instances.
  """
  _require_writer()

  manager = _get_lance_manager()
  s3_bucket = request.s3_bucket if request else None
  s3_key = request.s3_key if request else None

  try:
    result = await asyncio.to_thread(
      manager.export,
      graph_id=graph_id,
      table_name=table_name,
      s3_bucket=s3_bucket,
      s3_key=s3_key,
    )
  except ValueError as e:
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e)) from e
  except Exception as e:
    logger.error(f"Vector export failed: {e}", exc_info=True)
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Vector export failed: {e}",
    ) from e

  return VectorExportResponse(
    graph_id=result["graph_id"],
    table_name=result["table_name"],
    size_mb=result["size_mb"],
    duration_ms=result["duration_ms"],
    s3_uri=result.get("s3_uri"),
  )
