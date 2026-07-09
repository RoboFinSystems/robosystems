"""
Semantic memory endpoints for Graph API.

Per-graph incremental CRUD over a single LanceDB "memory" table — the storage
half of the AI-memory feature. This is an INTERNAL surface: only the platform
API (via GraphClient) calls it; the main-API MemoryService is the trust boundary
that builds recall/list predicates from allowlisted fields. All memory ops route
to the writer/master instance (the memory table is not part of replica sync).

Distinct from ``databases/memory.py`` (RAM boost) and the subgraph-Cypher
"memory" MCP tools — this is the LanceDB semantic-vector store.

Endpoints (prefix /databases):

  POST   /{graph_id}/semantic-memory/rows          Add memory rows (pre-embedded)
  POST   /{graph_id}/semantic-memory/search        Vector recall (cosine top-k)
  POST   /{graph_id}/semantic-memory/list          List/filter (metadata scan)
  GET    /{graph_id}/semantic-memory/rows/{id}      Get one by id
  PATCH  /{graph_id}/semantic-memory/rows/{id}      Full-row upsert on id
  DELETE /{graph_id}/semantic-memory/rows/{id}      Delete by id
"""

import asyncio
import os
from typing import Any

from fastapi import APIRouter, HTTPException, Path, status
from pydantic import BaseModel, ConfigDict, Field

from robosystems.graph_api.core.lance.memory_store import VECTOR_DIM
from robosystems.logger import logger

router = APIRouter(prefix="/databases", tags=["Semantic Memory"])

_memory_store = None

# Per-graph write serialization (LanceDB uses optimistic concurrency; concurrent
# add/merge_insert on the same table can raise commit conflicts).
_locks: dict[str, asyncio.Lock] = {}


def _get_memory_store():
  """Lazy-load the LanceMemoryStore singleton."""
  global _memory_store
  if _memory_store is None:
    from robosystems.graph_api.core.lance import LanceMemoryStore

    _memory_store = LanceMemoryStore()
    logger.info(f"LanceMemoryStore initialized (base_path={_memory_store.base_path})")
  return _memory_store


def _lock_for(graph_id: str) -> asyncio.Lock:
  lock = _locks.get(graph_id)
  if lock is None:
    lock = asyncio.Lock()
    _locks[graph_id] = lock
  return lock


def _require_writer() -> None:
  """Raise 501 if called on a read-only replica."""
  if os.getenv("LBUG_ROLE") == "replica":
    raise HTTPException(
      status_code=status.HTTP_501_NOT_IMPLEMENTED,
      detail="Semantic memory writes are not available on read-only replicas",
    )


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class MemoryRowIn(BaseModel):
  """A pre-embedded memory row (vectors computed by the main API)."""

  id: str
  vector: list[float] = Field(min_length=VECTOR_DIM, max_length=VECTOR_DIM)
  text: str
  created_at_ms: int | None = None
  updated_at_ms: int | None = None
  created_by: str | None = None
  source: str | None = None
  memory_type: str | None = None
  tags: list[str] | None = None
  source_ref: str | None = None
  provenance: str | None = None

  model_config = ConfigDict(extra="forbid")


class MemoryUpdateRow(BaseModel):
  """Full row for an upsert (id comes from the path)."""

  vector: list[float] = Field(min_length=VECTOR_DIM, max_length=VECTOR_DIM)
  text: str
  created_at_ms: int | None = None
  updated_at_ms: int | None = None
  created_by: str | None = None
  source: str | None = None
  memory_type: str | None = None
  tags: list[str] | None = None
  source_ref: str | None = None
  provenance: str | None = None

  model_config = ConfigDict(extra="forbid")


class MemoryAddRequest(BaseModel):
  records: list[MemoryRowIn] = Field(min_length=1)


class MemorySearchRequest(BaseModel):
  embedding: list[float] = Field(min_length=VECTOR_DIM, max_length=VECTOR_DIM)
  limit: int = Field(default=10, ge=1, le=100)
  where: str | None = None
  select_columns: list[str] | None = None


class MemoryListRequest(BaseModel):
  where: str | None = None
  limit: int = Field(default=100, ge=1, le=500)
  offset: int = Field(default=0, ge=0)


class MemoryAddResponse(BaseModel):
  added: int
  total: int


class MemorySearchResponse(BaseModel):
  results: list[dict[str, Any]]
  total: int
  execution_time_ms: float


class MemoryListResponse(BaseModel):
  results: list[dict[str, Any]]
  total: int


class MemoryDeleteResponse(BaseModel):
  deleted: bool


class MemoryUpdateResponse(BaseModel):
  id: str
  updated: bool


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/{graph_id}/semantic-memory/rows", response_model=MemoryAddResponse)
async def add_memory_rows(
  request: MemoryAddRequest,
  graph_id: str = Path(...),
) -> MemoryAddResponse:
  _require_writer()
  store = _get_memory_store()
  rows = [r.model_dump() for r in request.records]
  try:
    async with _lock_for(graph_id):
      result = await asyncio.to_thread(store.add_rows, graph_id, rows)
  except HTTPException:
    raise
  except ValueError as e:
    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
  return MemoryAddResponse(**result)


@router.post("/{graph_id}/semantic-memory/search", response_model=MemorySearchResponse)
async def search_memory(
  request: MemorySearchRequest,
  graph_id: str = Path(...),
) -> MemorySearchResponse:
  store = _get_memory_store()
  try:
    result = await asyncio.to_thread(
      store.search,
      graph_id,
      request.embedding,
      request.limit,
      request.where,
      request.select_columns,
    )
  except HTTPException:
    raise
  except ValueError as e:
    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
  return MemorySearchResponse(**result)


@router.post("/{graph_id}/semantic-memory/list", response_model=MemoryListResponse)
async def list_memory(
  request: MemoryListRequest,
  graph_id: str = Path(...),
) -> MemoryListResponse:
  store = _get_memory_store()
  try:
    result = await asyncio.to_thread(
      store.list, graph_id, request.where, request.limit, request.offset
    )
  except HTTPException:
    raise
  except ValueError as e:
    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
  return MemoryListResponse(**result)


@router.get("/{graph_id}/semantic-memory/rows/{memory_id}")
async def get_memory_row(
  graph_id: str = Path(...),
  memory_id: str = Path(...),
) -> dict[str, Any]:
  store = _get_memory_store()
  try:
    row = await asyncio.to_thread(store.get, graph_id, memory_id)
  except HTTPException:
    raise
  except ValueError as e:
    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
  if row is None:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND, detail="Memory not found"
    )
  return row


@router.patch(
  "/{graph_id}/semantic-memory/rows/{memory_id}", response_model=MemoryUpdateResponse
)
async def update_memory_row(
  request: MemoryUpdateRow,
  graph_id: str = Path(...),
  memory_id: str = Path(...),
) -> MemoryUpdateResponse:
  _require_writer()
  store = _get_memory_store()
  try:
    async with _lock_for(graph_id):
      result = await asyncio.to_thread(
        store.update, graph_id, memory_id, request.model_dump()
      )
  except HTTPException:
    raise
  except ValueError as e:
    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
  return MemoryUpdateResponse(**result)


@router.delete(
  "/{graph_id}/semantic-memory/rows/{memory_id}", response_model=MemoryDeleteResponse
)
async def delete_memory_row(
  graph_id: str = Path(...),
  memory_id: str = Path(...),
) -> MemoryDeleteResponse:
  _require_writer()
  store = _get_memory_store()
  try:
    async with _lock_for(graph_id):
      result = await asyncio.to_thread(store.delete, graph_id, memory_id)
  except HTTPException:
    raise
  except ValueError as e:
    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
  return MemoryDeleteResponse(**result)
