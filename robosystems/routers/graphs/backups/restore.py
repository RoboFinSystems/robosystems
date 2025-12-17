"""
Backup restore endpoint.
"""

from fastapi import (
  APIRouter,
  Depends,
  HTTPException,
  BackgroundTasks,
  Path,
  Request,
  status,
)
from sqlalchemy.orm import Session

from robosystems.database import get_async_db_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.iam import User
from robosystems.models.api.graphs.backups import BackupRestoreRequest
from robosystems.models.api.common import ErrorResponse
from robosystems.middleware.otel.metrics import (
  get_endpoint_metrics,
  endpoint_metrics_decorator,
)
from robosystems.middleware.graph.utils import MultiTenantUtils
from robosystems.logger import logger
from robosystems.security import SecurityAuditLogger, SecurityEventType
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN

from .utils import verify_admin_access

# Create router
router = APIRouter()


@router.post(
  "/{backup_id}/restore",
  response_model=None,
  status_code=status.HTTP_202_ACCEPTED,
  summary="Restore Encrypted Backup",
  description="""Restore a graph database from an encrypted backup.

Restores a complete graph database from an encrypted backup:
- **Format**: Only full_dump backups can be restored
- **Encryption**: Only encrypted backups can be restored (security requirement)
- **System Backup**: Creates automatic backup of existing database before restore
- **Verification**: Optionally verifies database integrity after restore

**Restore Features:**
- **Atomic Operation**: Complete replacement of database
- **Rollback Protection**: System backup created before restore
- **Data Integrity**: Verification ensures successful restore
- **Security**: Only encrypted backups to prevent data tampering

**Operation State Machine:**
```
pending → backing_up_current → downloading → restoring → verifying → completed
                                                                   ↘ failed
```
- **pending**: Restore queued, waiting to start
- **backing_up_current**: Creating safety backup of existing database
- **downloading**: Downloading backup from storage
- **restoring**: Replacing database with backup contents
- **verifying**: Verifying database integrity (if enabled)
- **completed**: Restore successful, database operational
- **failed**: Restore failed (rollback may be available)

**Expected Durations:**
Operation times vary by database size (includes backup + restore):
- **Small** (<1GB): 1-3 minutes
- **Medium** (1-10GB): 5-15 minutes
- **Large** (10-100GB): 20-45 minutes
- **Very Large** (>100GB): 45+ minutes

Note: Restore operations take longer than backups due to safety backup step.

**Progress Monitoring:**
Use the returned operation_id to connect to the SSE stream:
```javascript
const eventSource = new EventSource('/v1/operations/{operation_id}/stream');
eventSource.addEventListener('operation_progress', (event) => {
  const data = JSON.parse(event.data);
  console.log('Restore progress:', data.message);
  console.log('Status:', data.status); // Shows current state
});
```

**SSE Connection Limits:**
- Maximum 5 concurrent SSE connections per user
- Rate limited to 10 new connections per minute
- Automatic circuit breaker for Redis failures
- Graceful degradation if event system unavailable

**Important Notes:**
- Only encrypted backups can be restored (security measure)
- Existing database is backed up to S3 before restore
- Restore is a destructive operation - existing data is replaced
- System backups are stored separately for recovery

**Credit Consumption:**
- Base cost: 100.0 credits
- Large databases (>10GB): 200.0 credits
- Multiplied by graph tier

Returns operation details for SSE monitoring.""",
  operation_id="restoreBackup",
  responses={
    202: {"description": "Restore started"},
    400: {"description": "Invalid restore configuration", "model": ErrorResponse},
    403: {"description": "Access denied - admin role required", "model": ErrorResponse},
    404: {"description": "Backup not found", "model": ErrorResponse},
    500: {"description": "Failed to initiate restore", "model": ErrorResponse},
  },
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/graphs/{graph_id}/backup/restore",
  business_event_type="backup_restored",
)
async def restore_backup(
  background_tasks: BackgroundTasks,
  request: BackupRestoreRequest,
  fastapi_request: Request,
  backup_id: str = Path(..., description="Backup identifier"),
  graph_id: str = Path(
    ..., description="Graph database identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_async_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """
  Restore a graph database from an encrypted backup.

  This endpoint restores a complete graph database from an encrypted backup:
  - Only encrypted backups can be restored for security
  - Creates a system backup of existing database before restore
  - Verifies database integrity after restore

  Security constraints:
  - Only admin users can restore backups
  - Only encrypted backups are allowed
  - System backup ensures rollback capability
  """
  try:
    verify_admin_access(current_user, graph_id, db)

    # Block restore operations for shared repositories
    if MultiTenantUtils.is_shared_repository(graph_id):
      logger.warning(
        f"User {current_user.id} attempted restore operation on shared repository {graph_id}"
      )
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"Restore operations are not allowed on shared repository '{graph_id}'. "
        f"Shared repositories are managed by system administrators.",
      )

    # Verify backup exists and belongs to this graph
    from robosystems.models.iam import GraphBackup

    backup_record = GraphBackup.get_by_id(backup_id, db)
    if not backup_record:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Backup not found",
      )

    if backup_record.graph_id != graph_id:
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Backup does not belong to this graph",
      )

    # Verify backup is encrypted (security requirement)
    if not backup_record.encryption_enabled:
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Only encrypted backups can be restored for security reasons",
      )

    # Verify backup format is full_dump
    backup_format = backup_record.backup_metadata.get("backup_format", "full_dump")
    if backup_format != "full_dump":
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Only full database backups (full_dump) can be restored",
      )

    # Create SSE operation for restore tracking
    from robosystems.middleware.sse import create_operation_response

    sse_response = await create_operation_response(
      operation_type="backup_restore", user_id=current_user.id, graph_id=graph_id
    )

    # Execute restore via Dagster with SSE monitoring
    from robosystems.middleware.sse import (
      run_and_monitor_dagster_job,
      build_graph_job_config,
    )

    operation_id = sse_response["operation_id"]

    # Build Dagster job config
    run_config = build_graph_job_config(
      "restore_graph_job",
      graph_id=graph_id,
      backup_id=backup_id,
      user_id=str(current_user.id),
      create_system_backup=request.create_system_backup,
      verify_after_restore=request.verify_after_restore,
    )

    # Run Dagster job with SSE monitoring in background
    background_tasks.add_task(
      run_and_monitor_dagster_job,
      job_name="restore_graph_job",
      operation_id=operation_id,
      run_config=run_config,
    )

    logger.info(
      f"Scheduled Dagster restore job for graph {graph_id} with operation {operation_id}"
    )

    # Record business event
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graph/backup/restore",
      method="POST",
      event_type="backup_restore_requested",
      event_data={
        "user_id": current_user.id,
        "graph_id": graph_id,
        "backup_id": backup_id,
        "create_system_backup": request.create_system_backup,
        "verify_after_restore": request.verify_after_restore,
      },
      user_id=current_user.id,
    )

    # Log security event
    client_ip = fastapi_request.client.host if fastapi_request.client else None
    user_agent = fastapi_request.headers.get("user-agent")

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_SUCCESS,
      user_id=str(current_user.id),
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint=f"/v1/graphs/{graph_id}/backup/restore",
      details={
        "action": "backup_restore_initiated",
        "graph_id": graph_id,
        "backup_id": backup_id,
        "create_system_backup": request.create_system_backup,
      },
      risk_level="high",  # Restore is a high-risk operation
    )

    # Return SSE response directly
    return {
      **sse_response,
      "status": "pending",
      "message": f"graph database restore scheduled for graph '{graph_id}' from encrypted backup",
    }

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to restore backup for graph {graph_id}: {str(e)}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to schedule restore",
    )
