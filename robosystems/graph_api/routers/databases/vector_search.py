"""
Vector index management endpoints for Graph API.

Provides per-graph, per-table vector index operations backed by LanceDB:

  POST /databases/{graph_id}/tables/{table_name}/vector/build
    Build IVF-PQ index from DuckDB staging table (master only)

  POST /databases/{graph_id}/tables/{table_name}/vector/search
    Query the lance index by embedding similarity (master + replicas)

  POST /databases/{graph_id}/tables/{table_name}/vector/export
    Package lance index as tar.gz for S3 publish (master only)

  DELETE /databases/{graph_id}/tables/{table_name}/vector
    Delete the lance index for a table

  GET /databases/{graph_id}/tables/{table_name}/vector
    Get index metadata (row count, size, etc.)

Build and export require DuckDB staging access (master/writer instances).
Search works on any instance that has the lance index on disk — masters
build it locally, replicas download it from S3 at boot.
"""

import asyncio
import os
import re

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field, field_validator, model_validator

from robosystems.logger import logger

router = APIRouter(prefix="/databases", tags=["Vector Index"])


# ---------------------------------------------------------------------------
# Request/response models
# ---------------------------------------------------------------------------


class VectorBuildRequest(BaseModel):
  """Request to build a vector index from DuckDB staging."""

  query: str = Field(
    ...,
    description='DuckDB SQL query that selects rows to index. Must return a "vector" '
    "column (e.g., embedding::FLOAT[384] AS vector). All other columns are "
    "stored as searchable metadata. Domain-specific filtering, dedup, and "
    "column selection are the caller's responsibility.",
    max_length=10000,
  )
  memory_limit: str = Field(
    default="8GB",
    description="DuckDB memory limit for the extraction query.",
  )

  @field_validator("memory_limit")
  @classmethod
  def validate_memory_limit(cls, v: str) -> str:
    if not re.match(r"^\d+(?:\.\d+)?\s*(?:B|KB|MB|GB|TB)$", v, re.IGNORECASE):
      raise ValueError(
        f"Invalid memory_limit format: '{v}'. Expected format: '8GB', '2048MB', etc."
      )
    return v

  class Config:
    extra = "forbid"


class VectorBuildResponse(BaseModel):
  """Response from vector index build."""

  graph_id: str
  table_name: str
  row_count: int
  num_partitions: int
  index_size_mb: float
  duration_ms: float


class VectorSearchRequest(BaseModel):
  """Request to search the vector index."""

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


def _require_writer():
  """Raise 501 if called on a read-only replica."""
  if os.getenv("LBUG_ROLE") == "replica":
    raise HTTPException(
      status_code=status.HTTP_501_NOT_IMPLEMENTED,
      detail="Vector build/export not available on read-only replicas",
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post(
  "/{graph_id}/tables/{table_name}/vector/build",
  response_model=VectorBuildResponse,
  summary="Build vector index from DuckDB staging table",
)
async def vector_build(
  graph_id: str,
  table_name: str,
  request: VectorBuildRequest,
) -> VectorBuildResponse:
  """Build an IVF-PQ vector index from a DuckDB query.

  Runs the provided SQL query against the local DuckDB staging database and
  builds a LanceDB IVF-PQ index from the results. The query must return a
  "vector" column — all other columns become searchable metadata.

  Domain-specific filtering (e.g., only numeric elements with facts) and
  deduplication are the caller's responsibility via the query.

  Only available on writer/master instances (requires DuckDB staging access).
  """
  _require_writer()

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

  Queries the LanceDB IVF-PQ index for the specified table using approximate
  nearest neighbor search (~5ms latency). Returns matching rows with cosine
  distance scores.

  Available on both master and replica instances — masters build the index
  locally, replicas download it from S3 at boot.
  """
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

  When s3_bucket and s3_key are provided in the request body, the tar.gz
  is uploaded directly from this instance to S3. This is required because
  the Dagster worker calling this endpoint runs on Fargate and cannot
  access this instance's filesystem.

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


@router.get(
  "/{graph_id}/tables/{table_name}/vector",
  response_model=VectorIndexInfo | None,
  summary="Get vector index metadata",
)
async def vector_info(
  graph_id: str,
  table_name: str,
) -> VectorIndexInfo | None:
  """Get metadata about an existing vector index (row count, size, etc.)."""
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
) -> dict:
  """Delete the lance index for a specific table."""
  _require_writer()

  manager = _get_lance_manager()
  result = manager.delete(graph_id=graph_id, table_name=table_name)
  return result
