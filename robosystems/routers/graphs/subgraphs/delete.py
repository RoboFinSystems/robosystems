"""
Subgraph deletion endpoint.
"""

from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from robosystems.database import get_async_db_session
from robosystems.logger import api_logger, log_metric, logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_ID_PATTERN, SUBGRAPH_NAME_PATTERN
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.models.api.graphs.subgraphs import (
  DeleteSubgraphRequest,
  DeleteSubgraphResponse,
)
from robosystems.models.iam.graph_user import GraphUser
from robosystems.models.iam.user import User
from robosystems.security import SecurityAuditLogger, SecurityEventType

from .utils import (
  circuit_breaker,
  get_subgraph_by_name,
  get_subgraph_service,
  handle_circuit_breaker_check,
  record_operation_metrics,
  record_operation_start,
)

router = APIRouter()


@router.delete(
  "/{subgraph_name}",
  response_model=DeleteSubgraphResponse,
  operation_id="deleteSubgraph",
  summary="Delete Subgraph",
  description="""Delete a subgraph database.

**Requirements:**
- Must be a valid subgraph (not parent graph)
- User must have admin access to parent graph
- Subgraph name must be alphanumeric (1-20 characters)
- Optional backup before deletion

**Deletion Options:**
- `force`: Delete even if contains data
- `backup_first`: Create backup before deletion

**Warning:**
Deletion is permanent unless backup is created.
All data in the subgraph will be lost.

**Backup Location:**
If backup requested, stored in S3 graph database bucket at:
`s3://{graph_s3_bucket}/{instance_id}/{database_name}_{timestamp}.backup`

**Notes:**
- Use the subgraph name (e.g., 'dev', 'staging') not the full subgraph ID
- Deletion does not affect parent graph's credit pool or permissions
- Backup creation consumes credits from parent graph's allocation""",
  responses={
    200: {"description": "Subgraph deleted successfully"},
    400: {"description": "Invalid subgraph identifier"},
    403: {"description": "Insufficient permissions"},
    404: {"description": "Subgraph not found"},
    409: {"description": "Subgraph contains data (use force=true)"},
    500: {"description": "Internal server error"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/subgraphs/{subgraph_id}",
  business_event_type="subgraph_deleted",
)
async def delete_subgraph(
  graph_id: str = Path(
    ..., description="Parent graph identifier", pattern=GRAPH_ID_PATTERN
  ),
  subgraph_name: str = Path(
    ...,
    description="Subgraph name to delete (e.g., 'dev', 'staging')",
    pattern=SUBGRAPH_NAME_PATTERN,
  ),
  request: DeleteSubgraphRequest = ...,
  current_user: User = Depends(get_current_user_with_graph),
  session: Session = Depends(get_async_db_session),
) -> DeleteSubgraphResponse:
  """Delete a subgraph.

  Requirements:
  - Must be a subgraph (not a parent graph)
  - User must have admin access to parent graph
  - Optional backup before deletion
  """
  start_time = record_operation_start()

  # Check circuit breaker
  handle_circuit_breaker_check(graph_id, "subgraph_delete")

  try:
    # Get and verify subgraph using subgraph name
    subgraph = get_subgraph_by_name(graph_id, subgraph_name, session, current_user)

    user_graph = (
      session.query(GraphUser)
      .filter(
        GraphUser.user_id == current_user.id,
        GraphUser.graph_id == subgraph.parent_graph_id,
      )
      .first()
    )

    if not user_graph or user_graph.role != "admin":
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Admin access to parent graph required to delete subgraphs",
      )

    subgraph_id = subgraph.graph_id  # type: ignore[assignment]

    # Delete the graph database (handles backup if requested)
    subgraph_service = get_subgraph_service()
    deletion_result = await subgraph_service.delete_subgraph_database(
      subgraph_id=subgraph_id, force=request.force, create_backup=request.backup_first
    )

    backup_location = deletion_result.get("backup_location")

    # Delete from PostgreSQL
    subgraph.delete(session)

    # Log the deletion
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_SUCCESS,
      details={
        "action": "subgraph_deleted",
        "subgraph_id": subgraph_id,
        "parent_graph_id": subgraph.parent_graph_id,
        "user_id": current_user.id,
        "forced": request.force,
        "backup_created": request.backup_first,
      },
      risk_level="medium",
    )

    # Log successful deletion
    api_logger.info(f"Deleted subgraph {subgraph_id} by user {current_user.id}")

    # Record metrics
    record_operation_metrics(
      start_time,
      "deletion",
      graph_id,
      {
        "backup_created": str(request.backup_first),
        "forced": str(request.force),
      },
    )
    log_metric("subgraph_deleted", 1, {"parent_graph": graph_id})

    # Mark circuit breaker success
    circuit_breaker.record_success(graph_id, "subgraph_delete")

    return DeleteSubgraphResponse(
      graph_id=subgraph_id,
      status="deleted",
      backup_location=backup_location,
      deleted_at=datetime.now(UTC),
      message=f"Subgraph {subgraph_id} successfully deleted",
    )

  except HTTPException:
    raise
  except SQLAlchemyError as e:
    logger.error(f"Database error deleting subgraph: {e}")
    session.rollback()
    # Record failure metric
    log_metric("subgraph_deletion_failed", 1, {"error_type": "database"})
    # Mark circuit breaker failure
    circuit_breaker.record_failure(graph_id, "subgraph_delete")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to delete subgraph due to database error",
    )
  except Exception as e:
    logger.error(f"Unexpected error deleting subgraph: {e}")
    # Record failure metric
    log_metric("subgraph_deletion_failed", 1, {"error_type": "unexpected"})
    # Mark circuit breaker failure
    circuit_breaker.record_failure(graph_id, "subgraph_delete")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to delete subgraph: {e!s}",
    )
