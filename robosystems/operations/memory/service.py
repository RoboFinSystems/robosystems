"""MemoryService — the transport-independent semantic-memory kernel.

Session-in style adapted for LanceDB: methods take a graph_id + a Pydantic
request, return Pydantic models (or None), and raise ValueError as the domain
error. Storage lives on the graph_api instance; this kernel computes embeddings
(local fastembed) and delegates persistence to a writer-routed GraphClient.

Recall predicates are built HERE from allowlisted typed fields only (memory_type,
source) with single-quote escaping — callers never pass raw WHERE strings.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from robosystems.config import env
from robosystems.logger import logger
from robosystems.models.api.memory import (
  MemoryCreateRequest,
  MemoryDeleteResponse,
  MemoryListResponse,
  MemoryRecallRequest,
  MemoryRecord,
  MemoryUpdateRequest,
)
from robosystems.models.api.search import SearchHit, SearchResponse

if TYPE_CHECKING:
  from robosystems.graph_api.client.client import GraphClient
  from robosystems.operations.search.embeddings import EmbeddingService


class MemoryService:
  """Per-graph semantic memory: remember / recall / forget + governance reads."""

  def __init__(self, client: GraphClient) -> None:
    self.client = client
    self._embedding_service: EmbeddingService | None = None

  @property
  def embedding_service(self) -> EmbeddingService:
    """Lazy-load the platform embedding service (shared with search)."""
    if self._embedding_service is None:
      from robosystems.operations.search.embeddings import get_embedding_service

      self._embedding_service = get_embedding_service()
      logger.info("Loaded EmbeddingService for memory")
    return self._embedding_service

  # ---------------------------------------------------------------------------
  # Writes
  # ---------------------------------------------------------------------------

  async def remember(
    self, graph_id: str, request: MemoryCreateRequest, *, created_by: str | None = None
  ) -> MemoryRecord:
    vector = await self._embed(request.text)
    now = _now_ms()
    row: dict[str, Any] = {
      "id": f"mem_{uuid4().hex}",
      "vector": vector,
      "text": request.text,
      "created_at_ms": now,
      "updated_at_ms": now,
      "created_by": created_by,
      "source": request.source,
      "memory_type": request.memory_type,
      "tags": request.tags,
      "source_ref": request.source_ref,
      "provenance": _dump_provenance(request.provenance),
    }
    await self.client.memory_add(graph_id, records=[row])
    return _to_record(row)

  async def update_memory(
    self, graph_id: str, memory_id: str, request: MemoryUpdateRequest
  ) -> MemoryRecord | None:
    """Read-modify-write. Always re-embeds (cheap, local) so the vector tracks
    the current text without needing the stored vector back."""
    existing = await self.client.memory_get(graph_id, memory_id)
    if existing is None:
      return None

    fields = request.model_fields_set
    text = request.text if "text" in fields else existing.get("text") or ""
    memory_type = (
      request.memory_type if "memory_type" in fields else existing.get("memory_type")
    )
    tags = request.tags if "tags" in fields else existing.get("tags")
    source_ref = (
      request.source_ref if "source_ref" in fields else existing.get("source_ref")
    )
    provenance = (
      _dump_provenance(request.provenance)
      if "provenance" in fields
      else existing.get("provenance")
    )

    vector = await self._embed(text)
    row: dict[str, Any] = {
      "vector": vector,
      "text": text,
      "created_at_ms": existing.get("created_at_ms"),
      "updated_at_ms": _now_ms(),
      "created_by": existing.get("created_by"),
      "source": existing.get("source"),
      "memory_type": memory_type,
      "tags": tags,
      "source_ref": source_ref,
      "provenance": provenance,
    }
    await self.client.memory_update(graph_id, memory_id, row)
    return _to_record({**row, "id": memory_id})

  async def forget(self, graph_id: str, memory_id: str) -> MemoryDeleteResponse:
    result = await self.client.memory_delete(graph_id, memory_id)
    return MemoryDeleteResponse(
      id=memory_id, deleted=bool(result.get("deleted", False))
    )

  # ---------------------------------------------------------------------------
  # Reads
  # ---------------------------------------------------------------------------

  async def recall(self, graph_id: str, request: MemoryRecallRequest) -> SearchResponse:
    vector = await self._embed(request.query)
    where = _build_predicate(memory_type=request.memory_type, source=request.source)
    rows = await self.client.memory_search(
      graph_id, embedding=vector, limit=request.k, where=where
    )
    hits = [_to_hit(r) for r in rows]
    return SearchResponse(
      total=len(hits), hits=hits, query=request.query, graph_id=graph_id
    )

  async def get_memory(self, graph_id: str, memory_id: str) -> MemoryRecord | None:
    row = await self.client.memory_get(graph_id, memory_id)
    return _to_record(row) if row else None

  async def list_memories(
    self,
    graph_id: str,
    *,
    memory_type: str | None = None,
    source: str | None = None,
    limit: int = 100,
    offset: int = 0,
  ) -> MemoryListResponse:
    where = _build_predicate(memory_type=memory_type, source=source)
    result = await self.client.memory_list(
      graph_id, where=where, limit=limit, offset=offset
    )
    records = [_to_record(r) for r in result.get("results", [])]
    return MemoryListResponse(
      total=result.get("total", len(records)), memories=records, graph_id=graph_id
    )

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  async def _embed(self, text: str) -> list[float]:
    # fastembed is synchronous CPU work — keep it off the event loop.
    return await asyncio.to_thread(self.embedding_service.embed_single, text)


def get_memory_service(client: GraphClient) -> MemoryService | None:
  """Return a MemoryService, or None when the feature is disabled."""
  if not env.SEMANTIC_MEMORY_ENABLED:
    return None
  return MemoryService(client)


# ---------------------------------------------------------------------------
# Module-level pure helpers
# ---------------------------------------------------------------------------


def _now_ms() -> int:
  return int(datetime.now(UTC).timestamp() * 1000)


def _ms_to_dt(ms: int | None) -> datetime | None:
  if ms is None:
    return None
  return datetime.fromtimestamp(ms / 1000, tz=UTC)


def _esc(value: str) -> str:
  """Escape a SQL string literal for a LanceDB predicate."""
  return value.replace("'", "''")


def _build_predicate(
  *, memory_type: str | None = None, source: str | None = None
) -> str | None:
  """Build a filter predicate from allowlisted typed fields only."""
  clauses: list[str] = []
  if memory_type:
    clauses.append(f"memory_type = '{_esc(memory_type)}'")
  if source:
    clauses.append(f"source = '{_esc(source)}'")
  return " AND ".join(clauses) if clauses else None


def _dump_provenance(provenance: dict[str, Any] | None) -> str | None:
  return json.dumps(provenance) if provenance is not None else None


def _load_provenance(value: Any) -> dict[str, Any] | None:
  if not value:
    return None
  if isinstance(value, dict):
    return value
  if isinstance(value, str):
    try:
      parsed = json.loads(value)
      return parsed if isinstance(parsed, dict) else None
    except (ValueError, TypeError):
      return None
  return None


def _to_record(row: dict[str, Any]) -> MemoryRecord:
  return MemoryRecord(
    id=row["id"],
    text=row.get("text") or "",
    source=row.get("source"),
    memory_type=row.get("memory_type"),
    tags=row.get("tags"),
    source_ref=row.get("source_ref"),
    provenance=_load_provenance(row.get("provenance")),
    created_by=row.get("created_by"),
    created_at=_ms_to_dt(row.get("created_at_ms")),
    updated_at=_ms_to_dt(row.get("updated_at_ms")),
  )


def _to_hit(row: dict[str, Any]) -> SearchHit:
  # cosine distance → similarity score
  distance = row.get("distance")
  score = 1.0 - float(distance) if distance is not None else 0.0
  return SearchHit(
    document_id=row["id"],
    score=score,
    source_type="memory",
    snippet=row.get("text") or "",
    tags=row.get("tags"),
  )
