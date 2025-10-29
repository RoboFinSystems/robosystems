"""
Backup download URL generation endpoint.
"""

from datetime import datetime, timezone
from typing import Dict, Any
from fastapi import (
  APIRouter,
  Depends,
  HTTPException,
  Query,
  Path,
  status,
)
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.iam import User
from robosystems.middleware.otel.metrics import (
  get_endpoint_metrics,
  endpoint_metrics_decorator,
)
from robosystems.logger import logger

from .utils import get_backup_manager

# Create router
router = APIRouter()


@router.get(
  "/{backup_id}/download",
  response_model=Dict[str, Any],
  operation_id="getBackupDownloadUrl",
  summary="Get temporary download URL for backup",
  description="Generate a temporary download URL for a backup (unencrypted, compressed .kuzu files only)",
  status_code=status.HTTP_200_OK,
  responses={
    200: {"description": "Download URL generated successfully"},
    403: {"description": "Access denied or backup is encrypted"},
    404: {"description": "Backup not found"},
    500: {"description": "Failed to generate download URL"},
  },
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/graphs/{graph_id}/backups/{backup_id}/download",
  business_event_type="backup_download_url_generated",
)
async def get_backup_download_url(
  backup_id: str = Path(..., description="Backup identifier"),
  graph_id: str = Path(..., description="Graph database identifier"),
  expires_in: int = Query(
    3600, ge=300, le=86400, description="URL expiration time in seconds"
  ),
  current_user: User = Depends(get_current_user_with_graph),
  session: Session = Depends(get_db_session),
  _: None = Depends(subscription_aware_rate_limit_dependency),
) -> Dict[str, Any]:
  """
  Generate a temporary download URL for a backup.

  This endpoint provides a secure, time-limited URL that allows direct download
  of compressed .kuzu backup files without going through the API.

  Requirements:
  - Only unencrypted backups can be downloaded
  - Backup must be in full_dump format (complete .kuzu file)
  - File will be compressed

  Args:
    backup_id: Backup identifier
    graph_id: Graph database identifier
    expires_in: URL expiration time in seconds (5 minutes to 24 hours)
    current_user: Authenticated user
    session: Database session

  Returns:
    Dictionary containing the download URL and expiration information
  """
  try:
    # Access validated by get_current_user_with_graph dependency

    # Get backup manager and generate download URL
    backup_manager = get_backup_manager()

    download_url = await backup_manager.get_backup_download_url(
      graph_id=graph_id, backup_id=backup_id, expires_in=expires_in
    )

    if not download_url:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Backup not found or cannot be downloaded",
      )

    # Record business event
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graph/backup/download",
      method="GET",
      event_type="backup_download_url_generated",
      event_data={
        "user_id": current_user.id,
        "graph_id": graph_id,
        "backup_id": backup_id,
        "expires_in": expires_in,
      },
      user_id=current_user.id,
    )

    return {
      "download_url": download_url,
      "expires_in": expires_in,
      "expires_at": (datetime.now(timezone.utc).timestamp() + expires_in),
      "backup_id": backup_id,
      "graph_id": graph_id,
    }

  except ValueError as e:
    if "encrypted" in str(e).lower():
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Cannot generate download URL for encrypted backup",
      )
    else:
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Invalid backup operation",
      )
  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to generate download URL for backup {backup_id}: {str(e)}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to generate download URL",
    )
