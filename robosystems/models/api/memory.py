"""Request/response models for AI semantic memory (per-graph LanceDB store).

Recall output reuses ``SearchHit``/``SearchResponse`` from ``models/api/search.py``
(the ranked-read contract) — memory recall is a scored top-k, the same shape as
document search. These models cover the write + governance surfaces.
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

# Freeform, kept short for MVP; the ontology of memory types is deferred.
_MEMORY_TYPE_DESC = "Freeform classifier (e.g. 'note', 'fact', 'preference')."
_SOURCE_DESC = "Origin of the memory (e.g. 'api', 'mcp', 'agent')."


class MemoryCreateRequest(BaseModel):
  """Body for creating (remembering) a memory."""

  text: str = Field(..., min_length=1, max_length=10_000, description="Memory content")
  source: str = Field(default="api", max_length=64, description=_SOURCE_DESC)
  memory_type: str = Field(default="note", max_length=64, description=_MEMORY_TYPE_DESC)
  tags: list[str] | None = Field(default=None, description="Optional labels")
  source_ref: str | None = Field(
    default=None, max_length=2048, description="Optional external reference/URI"
  )
  provenance: dict[str, Any] | None = Field(
    default=None,
    description="Opaque provenance metadata (captured, not yet reasoned over)",
  )


class MemoryUpdateRequest(BaseModel):
  """Partial update for a memory. Only supplied fields are changed."""

  text: str | None = Field(default=None, min_length=1, max_length=10_000)
  memory_type: str | None = Field(
    default=None, max_length=64, description=_MEMORY_TYPE_DESC
  )
  tags: list[str] | None = None
  source_ref: str | None = Field(default=None, max_length=2048)
  provenance: dict[str, Any] | None = None


class MemoryRecallRequest(BaseModel):
  """Body for recall (ranked semantic search over memory)."""

  query: str = Field(..., min_length=1, max_length=500, description="Recall query")
  k: int = Field(default=10, ge=1, le=50, description="Max results to return")
  memory_type: str | None = Field(default=None, description="Filter by memory type")
  source: str | None = Field(default=None, description="Filter by source")


class MemoryRecord(BaseModel):
  """A stored memory (never includes the raw embedding vector)."""

  id: str
  text: str
  source: str | None = None
  memory_type: str | None = None
  tags: list[str] | None = None
  source_ref: str | None = None
  provenance: dict[str, Any] | None = None
  created_by: str | None = None
  created_at: datetime | None = None
  updated_at: datetime | None = None


class MemoryListResponse(BaseModel):
  """Governance list of memories for a graph."""

  total: int
  memories: list[MemoryRecord]
  graph_id: str


class MemoryDeleteResponse(BaseModel):
  """Result of forgetting a memory."""

  id: str
  deleted: bool
