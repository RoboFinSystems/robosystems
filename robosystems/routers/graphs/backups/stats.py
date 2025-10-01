"""
Backup statistics endpoint.
"""

from datetime import datetime, timezone
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
from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.graph import BackupStatsResponse
from robosystems.models.iam import User
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.logger import logger

from .utils import verify_graph_access, get_backup_manager

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
  graph_id: str = Path(..., description="Graph database identifier"),
  current_user: User = Depends(get_current_user),
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

    # Verify user has access to this graph
    verify_graph_access(current_user, graph_id, db)

    # Get all backups for statistics calculation
    logger.info(f"Calling backup manager list_backups for stats on graph: {graph_id}")
    backups_data = await get_backup_manager().list_backups(graph_id)
    logger.info(f"Got {len(backups_data)} backups for stats calculation")

    # Calculate statistics
    total_backups = len(backups_data)
    successful_backups = sum(
      1 for backup in backups_data if backup.get("status") != "failed"
    )
    failed_backups = total_backups - successful_backups
    success_rate = (
      (successful_backups / total_backups * 100) if total_backups > 0 else 0.0
    )

    # Calculate storage metrics
    total_original_size = sum(
      backup.get("original_size", backup.get("size", 0)) for backup in backups_data
    )
    total_compressed_size = sum(
      backup.get("compressed_size", backup.get("size", 0)) for backup in backups_data
    )
    storage_saved = max(0, total_original_size - total_compressed_size)

    # Calculate average compression ratio
    compression_ratios = []
    for backup in backups_data:
      original = backup.get("original_size", backup.get("size", 0))
      compressed = backup.get("compressed_size", backup.get("size", 0))
      if original > 0:
        compression_ratios.append(compressed / original)

    avg_compression_ratio = (
      sum(compression_ratios) / len(compression_ratios) if compression_ratios else 1.0
    )

    # Get latest backup date
    latest_backup_date = None
    if backups_data:
      latest_backup = max(
        backups_data,
        key=lambda x: x.get("last_modified", datetime.min.replace(tzinfo=timezone.utc)),
      )
      latest_backup_date = latest_backup.get("last_modified")
      if latest_backup_date:
        latest_backup_date = latest_backup_date.isoformat()

    # Count backup formats
    backup_formats = {}
    for backup in backups_data:
      format_type = backup.get("backup_format", "unknown")
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
    logger.error(f"Failed to get backup stats for graph {graph_id}: {str(e)}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to retrieve backup statistics",
    )
