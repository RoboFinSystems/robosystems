"""Request models for graph operations (POST /v1/graphs/{graph_id}/operations/)."""

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

  new_tier: str = Field(
    ...,
    description="Target tier: ladybug-standard, ladybug-large, ladybug-xlarge",
  )
