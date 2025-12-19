"""
Backup statistics endpoint.
"""

from datetime import UTC, datetime

from fastapi import (
  APIRouter,
  Depends,
  HTTPException,
  Path,
  Request,
  status,
)
from sqlalchemy.orm import Session

from robosystems.database import get_async_db_session
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.graphs.backups import BackupStatsResponse
from robosystems.models.iam import User

# Constants
PERCENTAGE_MULTIPLIER = 100.0

# Create router
router = APIRouter()


@router.get(
  "/stats",
  response_model=BackupStatsResponse,
  operation_id="getBackupStats",
  summary="Get backup statistics",
  description="Get comprehensive backup statistics for the specified graph database",
  status_code=status.HTTP_200_OK,
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/graphs/{graph_id}/backup/stats",
  business_event_type="backup_stats_accessed",
)
async def get_backup_stats(
  request: Request,
  graph_id: str = Path(
    ..., description="Graph database identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_async_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> BackupStatsResponse:
  """
  Get comprehensive backup statistics for the specified graph database.

  Returns aggregated statistics including success rates, storage metrics,
  compression ratios, and backup format distribution.
  """
  try:
    logger.info(
      f"Starting get_backup_stats for graph_id: {graph_id}, user: {current_user.id}"
    )

    # Query database for backups instead of S3
    from robosystems.models.iam import BackupStatus, GraphBackup

    backup_records = (
      db.query(GraphBackup).filter(GraphBackup.graph_id == graph_id).all()
    )

    logger.info(
      f"Got {len(backup_records)} backups for stats calculation from database"
    )

    # Calculate statistics
    total_backups = len(backup_records)
    successful_backups = sum(
      1 for backup in backup_records if backup.status == BackupStatus.COMPLETED
    )
    failed_backups = sum(
      1 for backup in backup_records if backup.status == BackupStatus.FAILED
    )
    success_rate = (
      (successful_backups / total_backups * PERCENTAGE_MULTIPLIER)
      if total_backups > 0
      else 0.0
    )

    # Calculate storage metrics
    total_original_size = sum(
      backup.original_size_bytes or 0 for backup in backup_records
    )
    total_compressed_size = sum(
      backup.compressed_size_bytes or 0 for backup in backup_records
    )
    storage_saved = max(0, total_original_size - total_compressed_size)

    # Calculate average compression ratio
    compression_ratios = []
    for backup in backup_records:
      if (
        backup.original_size_bytes
        and backup.original_size_bytes > 0
        and backup.compressed_size_bytes is not None
      ):
        compression_ratios.append(
          backup.compressed_size_bytes / backup.original_size_bytes
        )

    avg_compression_ratio = (
      sum(compression_ratios) / len(compression_ratios) if compression_ratios else 1.0
    )

    # Get latest backup date
    latest_backup_date = None
    if backup_records:
      latest_backup = max(
        backup_records,
        key=lambda x: x.created_at or datetime.min.replace(tzinfo=UTC),
      )
      if latest_backup.created_at:
        latest_backup_date = latest_backup.created_at.isoformat()

    # Count backup formats
    backup_formats = {}
    for backup in backup_records:
      format_type = "full_dump"
      if backup.backup_metadata and "backup_format" in backup.backup_metadata:
        format_type = backup.backup_metadata["backup_format"]
      elif backup.s3_key:
        if ".csv.zip" in backup.s3_key:
          format_type = "csv"
        elif ".json.zip" in backup.s3_key:
          format_type = "json"
        elif ".parquet.zip" in backup.s3_key:
          format_type = "parquet"
      backup_formats[format_type] = backup_formats.get(format_type, 0) + 1

    return BackupStatsResponse(
      graph_id=graph_id,
      total_backups=total_backups,
      successful_backups=successful_backups,
      failed_backups=failed_backups,
      success_rate=round(success_rate, 2),
      total_original_size_bytes=total_original_size,
      total_compressed_size_bytes=total_compressed_size,
      storage_saved_bytes=storage_saved,
      average_compression_ratio=round(avg_compression_ratio, 3),
      latest_backup_date=latest_backup_date,
      backup_formats=backup_formats,
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to get backup stats for graph {graph_id}: {e!s}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to retrieve backup statistics",
    )
