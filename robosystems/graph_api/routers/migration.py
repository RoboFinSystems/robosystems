"""
Migration endpoints for LadybugDB version upgrades.

Instance-level endpoints called by Dagster jobs to orchestrate
export (pre-deploy) and import (post-deploy) of all databases.
"""

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query

from robosystems.graph_api.core.migration_service import (
  MigrationService,
  is_migration_in_progress,
)
from robosystems.graph_api.core.task_manager import migration_task_manager
from robosystems.graph_api.models.migration import (
  MigrationExportResponse,
  MigrationImportResponse,
  MigrationStatusResponse,
)

router = APIRouter(prefix="/migration", tags=["Migration"])

migration_service = MigrationService()


@router.post("/export")
async def export_for_migration(
  background_tasks: BackgroundTasks,
  source_version: str = Query(..., description="Current LadybugDB version"),
  target_version: str = Query(..., description="Target LadybugDB version"),
  bucket: str = Query(
    ...,
    description="S3 bucket for the system backup; the caller resolves "
    "USER_DATA_BUCKET (this writer container does not set it)",
  ),
) -> MigrationExportResponse:
  """
  Export all databases on this instance for version migration.

  Creates Parquet exports of each database and writes a migration.json
  manifest. Returns a task_id for SSE monitoring.

  This endpoint should be called BEFORE deploying the new version.
  """
  task_id = await migration_task_manager.create_task(
    task_type="migration",
    metadata={
      "operation": "export",
      "source_version": source_version,
      "target_version": target_version,
    },
  )
  background_tasks.add_task(
    migration_service.export_all_databases,
    task_id,
    source_version,
    target_version,
    bucket,
  )
  return MigrationExportResponse(
    task_id=task_id, monitor_url=f"/tasks/{task_id}/monitor"
  )


@router.post("/import")
async def import_after_migration(
  background_tasks: BackgroundTasks,
) -> MigrationImportResponse:
  """
  Import all databases from a previous export after version upgrade.

  Reads migration.json, creates fresh databases with the new version,
  and imports data from Parquet exports. Health returns 503 during import.

  This endpoint should be called AFTER deploying the new version.
  """
  task_id = await migration_task_manager.create_task(
    task_type="migration",
    metadata={"operation": "import"},
  )
  background_tasks.add_task(migration_service.import_all_databases, task_id)
  return MigrationImportResponse(
    task_id=task_id, monitor_url=f"/tasks/{task_id}/monitor"
  )


@router.get("/status")
async def migration_status() -> MigrationStatusResponse:
  """
  Check if a migration export is pending import on this instance.

  Returns manifest details, whether an import is in progress,
  and any .pre-migration files available for rollback.
  """
  status = migration_service.get_status()
  return MigrationStatusResponse(**status)


@router.post("/cleanup")
async def cleanup_pre_migration() -> dict:
  """
  Delete .pre-migration files after verifying the migration succeeded.

  System backups in S3 remain as the safety net. Call this after
  confirming the migrated databases are working correctly.

  Refuses to run if a migration is in progress or if a manifest
  still exists (indicating import hasn't completed).
  """
  if is_migration_in_progress():
    raise HTTPException(
      status_code=409,
      detail="Cannot cleanup while migration is in progress",
    )

  if migration_service.manifest_path.exists():
    raise HTTPException(
      status_code=409,
      detail="Cannot cleanup: migration.json still exists (import not yet completed)",
    )

  return migration_service.cleanup_pre_migration_files()
