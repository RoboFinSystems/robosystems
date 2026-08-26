"""Request models for graph operations (POST /v1/graphs/{graph_id}/operations/)."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class DeleteSubgraphOp(BaseModel):
  """Body for the delete-subgraph operation."""

  subgraph_name: str = Field(
    ...,
    min_length=1,
    max_length=20,
    pattern="^[a-zA-Z0-9]+$",
    description="Subgraph name to delete (e.g., 'dev', 'staging')",
  )
  force: bool = Field(
    default=False,
    description="Delete even if subgraph contains data",
  )
  backup_first: bool = Field(
    default=True,
    description=(
      "Take a full backup of the subgraph before deleting it. The backup is "
      "registered on the parent graph's backup list, where it can be listed and "
      "downloaded after the subgraph is gone. If the backup fails the subgraph "
      "is not deleted."
    ),
  )


class ChangeTierOp(BaseModel):
  """Body for the change-tier operation (supports upgrades and downgrades)."""

  new_tier: Literal["ladybug-standard", "ladybug-large", "ladybug-xlarge"] = Field(
    ...,
    description="Target infrastructure tier",
  )


class UpdateGraphMetadataOp(BaseModel):
  """Body for the update-graph-metadata operation.

  Partial update — only supplied (non-null) fields change, so a caller
  editing just the display name need not resend the description and tags.
  Because ``None`` means "leave alone", clearing a field uses its empty
  value instead: pass ``""`` to clear the description and ``[]`` to clear
  the tags. ``graph_name`` cannot be cleared; it is the graph's label
  everywhere it is listed.

  This is the platform-level label for the graph, independent of the
  entity name shown on financial statements — change that through
  ``POST /extensions/roboledger/{graph_id}/operations/update-entity``.
  """

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {"graph_name": "Acme Consulting LLC"},
        {
          "graph_name": "Acme Consulting LLC",
          "description": "Primary operating entity, consolidated monthly",
          "tags": ["consulting", "production"],
        },
        {"description": "", "tags": []},
      ]
    }
  )

  graph_name: str | None = Field(
    default=None,
    min_length=1,
    max_length=255,
    description="New display name. Omit to leave unchanged; cannot be cleared.",
  )
  description: str | None = Field(
    default=None,
    max_length=1000,
    description="New description. Omit to leave unchanged; pass '' to clear.",
  )
  tags: list[str] | None = Field(
    default=None,
    max_length=20,
    description=(
      "Replaces the full tag list (not a merge). Omit to leave unchanged; "
      "pass [] to clear. Tags are trimmed, de-duplicated, and capped at 50 "
      "characters each."
    ),
  )

  @field_validator("graph_name")
  @classmethod
  def _strip_name(cls, value: str | None) -> str | None:
    if value is None:
      return None
    stripped = value.strip()
    if not stripped:
      raise ValueError("graph_name cannot be blank")
    return stripped

  @field_validator("tags")
  @classmethod
  def _normalize_tags(cls, value: list[str] | None) -> list[str] | None:
    if value is None:
      return None
    normalized: list[str] = []
    for tag in value:
      cleaned = tag.strip()
      if not cleaned:
        continue
      if len(cleaned) > 50:
        raise ValueError(f"Tag exceeds 50 characters: {cleaned[:50]}...")
      if cleaned not in normalized:
        normalized.append(cleaned)
    return normalized


class GraphMetadataResult(BaseModel):
  """Result payload for the update-graph-metadata operation."""

  graph_id: str = Field(description="Graph the metadata belongs to")
  graph_name: str = Field(description="Display name after the update")
  description: str = Field(
    default="", description="Description after the update ('' when unset)"
  )
  tags: list[str] = Field(
    default_factory=list, description="Tags after the update (empty when unset)"
  )
  updated_fields: list[str] = Field(
    default_factory=list,
    description=(
      "Fields this call actually changed. Empty when the submitted values "
      "already matched what was stored."
    ),
  )


class DeleteGraphOp(BaseModel):
  """Body for the delete-graph operation.

  Permanently destroys the graph and cancels its subscription. Two modes:

  - **Immediate** (default): subscription canceled now (`ends_at = now`) and
    fast-path deprovisioning fires within ~10 minutes. Use when you want
    the data gone and the slot freed right away.
  - **At period end** (`at_period_end=true`): subscription canceled but
    `ends_at = current_period_end` so the graph stays usable through the
    paid period. The existing suspend → deprovision sensor pipeline tears
    it down after the retention window once the period closes.

  Requires `confirm` to equal the URL `graph_id` as a guard against
  accidental destructive calls.
  """

  confirm: str = Field(
    ...,
    description=(
      "Must equal the graph_id in the URL — confirms the caller intends to "
      "destroy this specific graph."
    ),
  )
  at_period_end: bool = Field(
    default=False,
    description=(
      "If true, defer cancellation and teardown to the end of the current "
      "billing period (graph stays usable until then). If false (default), "
      "cancel and tear down immediately."
    ),
  )


class MaterializeOp(BaseModel):
  """Body for the materialize operation."""

  force: bool = Field(
    default=False, description="Force materialization even if already up to date"
  )
  rebuild: bool = Field(
    default=False,
    description=(
      "Rebuild the graph from scratch, dropping existing data. Required "
      "(staged source) when materializing new uploads into a graph that "
      "already contains materialized data — staging replays all uploaded "
      "files, so a non-rebuild pass would re-copy ingested rows (409)."
    ),
  )
  dry_run: bool = Field(
    default=False, description="Validate tables without writing to the graph"
  )
  source: str | None = Field(
    default=None,
    pattern="^(staged|extensions)$",
    description="Materialization source: 'extensions' for OLTP, omit for DuckDB staging tables",
  )
  materialize_embeddings: bool = Field(
    default=False, description="Generate vector embeddings during materialization"
  )


class RememberOp(BaseModel):
  """Body for the remember operation (write a semantic memory)."""

  text: str = Field(..., min_length=1, max_length=10_000, description="Memory content")
  source: str = Field(default="api", max_length=64, description="Origin of the memory")
  memory_type: str = Field(
    default="note", max_length=64, description="Freeform classifier"
  )
  tags: list[str] | None = Field(default=None, description="Optional labels")
  source_ref: str | None = Field(
    default=None, max_length=2048, description="Optional external reference/URI"
  )
  provenance: dict | None = Field(
    default=None, description="Opaque provenance metadata"
  )


class ForgetOp(BaseModel):
  """Body for the forget operation (delete a semantic memory by id)."""

  memory_id: str = Field(
    ...,
    pattern="^mem_[0-9a-f]{32}$",
    description="Server-generated memory id to forget",
  )


class UpdateMemoryOp(BaseModel):
  """Body for the update-memory operation (partial update of a stored memory).

  Only supplied fields are changed; the memory is re-embedded when ``text``
  changes.
  """

  memory_id: str = Field(
    ...,
    pattern="^mem_[0-9a-f]{32}$",
    description="Server-generated memory id to update",
  )
  text: str | None = Field(
    default=None, min_length=1, max_length=10_000, description="New memory content"
  )
  memory_type: str | None = Field(
    default=None, max_length=64, description="Freeform classifier"
  )
  tags: list[str] | None = Field(default=None, description="Optional labels")
  source_ref: str | None = Field(
    default=None, max_length=2048, description="Optional external reference/URI"
  )
  provenance: dict | None = Field(
    default=None, description="Opaque provenance metadata"
  )


class IndexDocumentOp(BaseModel):
  """Body for index-document (corpus content-op).

  Create a new document when ``document_id`` is absent; update the named
  document (partial — only supplied fields) when present.
  """

  document_id: str | None = Field(
    default=None,
    description="Present → update that document; absent → create a new one",
  )
  title: str | None = Field(
    default=None, max_length=500, description="Required when creating"
  )
  content: str | None = Field(
    default=None, max_length=500_000, description="Required when creating"
  )
  tags: list[str] | None = Field(default=None, description="Optional labels")
  folder: str | None = Field(default=None, description="Optional folder")
  external_id: str | None = Field(
    default=None, description="Upsert key (create): re-indexing the same id replaces"
  )


class DeleteDocumentOp(BaseModel):
  """Body for delete-document (corpus content-op)."""

  document_id: str = Field(..., min_length=1, description="Document id to delete")


class IngestFileOp(BaseModel):
  """Body for ingest-file (raw→staging content flow).

  Marks an uploaded file ready and triggers DuckDB staging. Set
  ``ingest_to_graph`` to auto-chain graph materialization after staging.
  """

  file_id: str = Field(..., min_length=1, description="Uploaded file id to ingest")
  ingest_to_graph: bool = Field(
    default=False,
    description="Auto-materialize into the graph after DuckDB staging",
  )


class DeleteFileOp(BaseModel):
  """Body for delete-file (raw content-op)."""

  file_id: str = Field(..., min_length=1, description="File id to delete")
  cascade: bool = Field(
    default=False,
    description="Also delete the file's rows from DuckDB tables and mark the graph stale",
  )
