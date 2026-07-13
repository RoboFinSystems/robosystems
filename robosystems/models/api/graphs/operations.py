"""Request models for graph operations (POST /v1/graphs/{graph_id}/operations/)."""

from typing import Literal

from pydantic import BaseModel, Field


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
    description="Create a backup before deleting",
  )


class RestoreBackupOp(BaseModel):
  """Body for the restore-backup operation."""

  backup_id: str = Field(
    ...,
    min_length=1,
    description="Backup identifier to restore from",
  )
  create_system_backup: bool = Field(
    default=True,
    description="Create a system backup of existing database before restore",
  )
  verify_after_restore: bool = Field(
    default=True,
    description="Verify database integrity after restore",
  )


class ChangeTierOp(BaseModel):
  """Body for the change-tier operation (supports upgrades and downgrades)."""

  new_tier: Literal["ladybug-standard", "ladybug-large", "ladybug-xlarge"] = Field(
    ...,
    description="Target infrastructure tier",
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
    default=False, description="Rebuild the graph from scratch, dropping existing data"
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
