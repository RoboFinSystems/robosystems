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


class UpgradeTierOp(BaseModel):
  """Body for the upgrade-tier operation."""

  new_tier: Literal["ladybug-standard", "ladybug-large", "ladybug-xlarge"] = Field(
    ...,
    description="Target infrastructure tier",
  )


class MaterializeOp(BaseModel):
  """Body for the materialize operation."""

  force: bool = Field(
    default=False, description="Force materialization even if already up to date"
  )
  rebuild: bool = Field(
    default=False, description="Rebuild the graph from scratch, dropping existing data"
  )
  ignore_errors: bool = Field(
    default=True, description="Continue past non-fatal row errors"
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
