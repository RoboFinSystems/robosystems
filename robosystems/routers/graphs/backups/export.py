"""
Backup export endpoint.
"""

from fastapi import (
  APIRouter,
  Depends,
  HTTPException,
  Path,
  Request,
  Response,
  status,
)
from sqlalchemy.orm import Session

from robosystems.database import get_async_db_session
from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.iam import User
from robosystems.middleware.otel.metrics import (
  get_endpoint_metrics,
  endpoint_metrics_decorator,
)
from robosystems.logger import logger
from robosystems.security import SecurityAuditLogger, SecurityEventType

from .utils import verify_graph_access, get_backup_manager

# Create router
router = APIRouter()


@router.post(
  "/{backup_id}/export",
  operation_id="exportBackup",
  summary="Export Kuzu backup for download",
  description="Export a backup file for download (only available for non-encrypted, compressed .kuzu backups)",
  status_code=status.HTTP_200_OK,
  responses={
    200: {"description": "Backup exported successfully"},
    403: {"description": "Access denied or backup is encrypted"},
    404: {"description": "Backup not found"},
  },
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/graphs/{graph_id}/backups/{backup_id}/export",
  business_event_type="backup_exported",
)
async def export_backup(
  fastapi_request: Request,
  backup_id: str = Path(..., description="Backup identifier"),
  graph_id: str = Path(..., description="Graph database identifier"),
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_async_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> Response:
  """
  Export a backup file for download.

  This endpoint allows downloading backup files in their original compressed format.
  - Only unencrypted backups can be downloaded
  - Downloads the complete compressed .kuzu database file
  - No format conversion is supported (original format only)

  Note: Encrypted backups cannot be exported and will return an error.
  """
  try:
    # Backup operations are included - no credit consumption

    # Verify user has access to this graph
    verify_graph_access(current_user, graph_id, db)

    # Get backup manager
    backup_manager = get_backup_manager()

    try:
      # Download backup with optional format conversion
      backup_data, content_type, filename = await backup_manager.download_backup(
        graph_id=graph_id,
        backup_id=backup_id,
        target_format=None,  # Only original format supported for now
      )

      # Backup operations are included - no credit consumption

      # Record business event
      metrics_instance = get_endpoint_metrics()
      metrics_instance.record_business_event(
        endpoint="/v1/graph/backups/export",
        method="POST",
        event_type="backup_exported",
        event_data={
          "user_id": current_user.id,
          "graph_id": graph_id,
          "backup_id": backup_id,
          "export_format": "original",
          "file_size_bytes": len(backup_data),
          "content_type": content_type,
        },
        user_id=current_user.id,
      )

      # Log security event for download
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.AUTH_SUCCESS,
        user_id=str(current_user.id),
        ip_address=fastapi_request.client.host if fastapi_request.client else None,
        user_agent=fastapi_request.headers.get("user-agent"),
        endpoint=f"/v1/graphs/{graph_id}/backup/export",
        details={
          "action": "backup_downloaded",
          "graph_id": graph_id,
          "backup_id": backup_id,
          "export_format": "original",
          "file_size_bytes": len(backup_data),
          "content_type": content_type,
        },
        risk_level="medium",
      )

      return Response(
        content=backup_data,
        media_type=content_type,
        headers={
          "Content-Disposition": f"attachment; filename={filename}",
          "Content-Length": str(len(backup_data)),
          "Cache-Control": "no-cache, no-store, must-revalidate",
          "Pragma": "no-cache",
          "Expires": "0",
        },
      )

    except ValueError as e:
      # Handle specific backup errors (not found, encrypted, etc.)
      if "not found" in str(e).lower():
        raise HTTPException(
          status_code=status.HTTP_404_NOT_FOUND,
          detail="Backup not found",
        )
      elif "encrypted" in str(e).lower():
        raise HTTPException(
          status_code=status.HTTP_403_FORBIDDEN,
          detail="Cannot export encrypted backup",
        )
      else:
        raise HTTPException(
          status_code=status.HTTP_400_BAD_REQUEST,
          detail="Invalid backup operation",
        )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to export backup {backup_id}: {str(e)}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to export backup",
    )
