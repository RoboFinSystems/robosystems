"""
Pydantic models for LadybugDB version migration.

These models are used for the export/import pipeline that migrates
databases across LadybugDB version upgrades.
"""

from pydantic import BaseModel, Field


class DatabaseExportResult(BaseModel):
  """Result of exporting a single database."""

  graph_id: str
  export_path: str
  node_count: int
  relationship_tables: int
  size_bytes: int
  system_backup_key: str | None = None
  exported_at: str


class MigrationManifest(BaseModel):
  """Manifest describing an export for version migration."""

  source_version: str
  target_version: str
  created_at: str
  export_format: str = "parquet"
  databases: list[DatabaseExportResult]


class MigrationExportResponse(BaseModel):
  """Response from POST /migration/export."""

  task_id: str
  monitor_url: str


class MigrationImportResponse(BaseModel):
  """Response from POST /migration/import."""

  task_id: str
  monitor_url: str


class MigrationStatusResponse(BaseModel):
  """Response from GET /migration/status."""

  migration_pending: bool = Field(
    description="True if a migration.json manifest exists and databases need importing"
  )
  migration_in_progress: bool = Field(
    description="True if an import is currently running (health returns 503)"
  )
  manifest: MigrationManifest | None = None
  pre_migration_files: list[str] = Field(
    default_factory=list,
    description="List of .pre-migration files on disk (available for rollback)",
  )
  active_task_id: str | None = None
