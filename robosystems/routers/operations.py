"""
Unified operations endpoint for Server-Sent Events monitoring.

This module provides endpoints for monitoring all non-immediate operations
through a unified SSE interface, replacing the fragmented task monitoring system.
"""

from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request
from fastapi import status as http_status
from sse_starlette.sse import EventSourceResponse

from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.models.iam import User
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
  sse_connection_rate_limit_dependency,
)
from robosystems.middleware.sse.event_storage import get_event_storage, OperationStatus
from robosystems.middleware.sse.streaming import create_sse_response_starlette
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from robosystems.logger import logger

# Create router
router = APIRouter()


@router.get(
  "/operations/{operation_id}/stream",
  summary="Stream Operation Events",
  description="""Stream real-time events for an operation using Server-Sent Events (SSE).

This endpoint provides real-time monitoring for all non-immediate operations including:
- Graph creation and management
- Agent analysis processing
- Database backups and restores
- Data synchronization tasks

**Event Types:**
- `operation_started`: Operation began execution
- `operation_progress`: Progress update with details
- `operation_completed`: Operation finished successfully
- `operation_error`: Operation failed with error details
- `operation_cancelled`: Operation was cancelled

**Features:**
- **Event Replay**: Use `from_sequence` parameter to replay missed events
- **Automatic Reconnection**: Client can reconnect and resume from last seen event
- **Real-time Updates**: Live progress updates during execution
- **Timeout Handling**: 30-second keepalive messages prevent connection timeouts
- **Graceful Degradation**: Automatic fallback if Redis is unavailable

**Connection Limits:**
- Maximum 5 concurrent SSE connections per user
- Rate limited to 10 new connections per minute
- Automatic cleanup of stale connections
- Circuit breaker protection for Redis failures

**Client Usage:**
```javascript
const eventSource = new EventSource('/v1/operations/abc123/stream');
eventSource.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('Progress:', data);
};
eventSource.onerror = (error) => {
  // Handle connection errors or rate limits
  console.error('SSE Error:', error);
};
```

**Error Handling:**
- `429 Too Many Requests`: Connection limit or rate limit exceeded
- `503 Service Unavailable`: SSE system temporarily disabled
- Clients should implement exponential backoff on errors

**No credits are consumed for SSE connections.**""",
  operation_id="streamOperationEvents",
  responses={
    200: {
      "description": "SSE stream of operation events",
      "headers": {
        "Content-Type": {"schema": {"type": "string", "enum": ["text/event-stream"]}},
        "Cache-Control": {"schema": {"type": "string", "enum": ["no-cache"]}},
        "Connection": {"schema": {"type": "string", "enum": ["keep-alive"]}},
      },
    },
    403: {"description": "Access denied to operation"},
    404: {"description": "Operation not found"},
    500: {"description": "Failed to create event stream"},
  },
)
@endpoint_metrics_decorator(
  "/v1/operations/{operation_id}/stream",
  business_event_type="operation_stream_connected",
)
async def stream_operation_events(
  operation_id: str = Path(
    ...,
    description="Operation identifier from initial submission",
    pattern="^([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|q_[0-9a-f]{12})$",
  ),
  from_sequence: int = Query(
    0,
    description="Start streaming from this sequence number (0 = from beginning)",
    ge=0,
  ),
  current_user: User = Depends(get_current_user),
  request: Request = None,
  _rate_limit: None = Depends(sse_connection_rate_limit_dependency),
) -> EventSourceResponse:
  """
  Stream operation events using Server-Sent Events.

  Provides real-time monitoring of operation progress with event replay
  capability for reliable client connectivity.

  Args:
      operation_id: Unique operation identifier
      from_sequence: Sequence number to start from (enables replay)
      current_user: Authenticated user
      request: FastAPI request for disconnect detection

  Returns:
      EventSourceResponse: SSE stream of operation events

  Raises:
      HTTPException: For access errors or operation not found
  """
  try:
    # Verify operation exists and user has access
    event_storage = get_event_storage()
    metadata = await event_storage.get_operation_metadata(operation_id)

    if not metadata:
      # Record metrics for not found
      metrics_instance = get_endpoint_metrics()
      metrics_instance.record_business_event(
        endpoint="/v1/operations/{operation_id}/stream",
        method="GET",
        event_type="operation_not_found",
        event_data={
          "operation_id": operation_id,
          "from_sequence": from_sequence,
        },
        user_id=current_user.id,
      )

      raise HTTPException(
        status_code=http_status.HTTP_404_NOT_FOUND,
        detail="Operation not found. It may have expired or been cancelled.",
      )

    # Check user access
    if metadata.user_id != current_user.id:
      # Record metrics for access denied
      metrics_instance = get_endpoint_metrics()
      metrics_instance.record_business_event(
        endpoint="/v1/operations/{operation_id}/stream",
        method="GET",
        event_type="operation_access_denied",
        event_data={
          "operation_id": operation_id,
          "operation_user_id": metadata.user_id,
          "requesting_user_id": current_user.id,
        },
        user_id=current_user.id,
      )

      raise HTTPException(
        status_code=http_status.HTTP_403_FORBIDDEN,
        detail="Access denied to operation.",
      )

    # Record successful connection metrics
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/operations/{operation_id}/stream",
      method="GET",
      event_type="operation_stream_connected_details",
      event_data={
        "operation_id": operation_id,
        "operation_type": metadata.operation_type,
        "operation_status": metadata.status,
        "graph_id": metadata.graph_id,
        "from_sequence": from_sequence,
      },
      user_id=current_user.id,
    )

    logger.info(
      f"User {current_user.id} connected to SSE stream for operation {operation_id}"
    )

    # Create and return SSE response using sse-starlette
    return create_sse_response_starlette(
      operation_id=operation_id,
      user_id=current_user.id,
      from_sequence=from_sequence,
      request=request,
    )

  except HTTPException:
    raise

  except Exception as e:
    # Log unexpected errors
    logger.error(f"Unexpected error in operation stream endpoint: {e}")

    # Record error metrics
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/operations/{operation_id}/stream",
      method="GET",
      event_type="operation_stream_error",
      event_data={
        "operation_id": operation_id,
        "error_type": type(e).__name__,
        "error_message": str(e),
      },
      user_id=current_user.id,
    )

    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to create operation event stream",
    )


@router.get(
  "/operations/{operation_id}/status",
  summary="Get Operation Status",
  description="""Get current status and metadata for an operation.

Returns detailed information including:
- Current status (pending, running, completed, failed, cancelled)
- Creation and update timestamps
- Operation type and associated graph
- Result data (for completed operations)
- Error details (for failed operations)

This endpoint provides a point-in-time status check, while the `/stream` endpoint
provides real-time updates. Use this for polling or initial status checks.

**No credits are consumed for status checks.**""",
  operation_id="getOperationStatus",
  responses={
    200: {
      "description": "Operation status retrieved successfully",
      "content": {
        "application/json": {
          "example": {
            "operation_id": "abc123",
            "operation_type": "graph_creation",
            "status": "running",
            "created_at": "2024-01-01T12:00:00Z",
            "updated_at": "2024-01-01T12:01:30Z",
            "graph_id": "kg1a2b3c4d5e6f7g8",
            "_links": {
              "stream": "/v1/operations/abc123/stream",
              "cancel": "/v1/operations/abc123",
            },
          }
        }
      },
    },
    403: {"description": "Access denied to operation"},
    404: {"description": "Operation not found"},
    500: {"description": "Failed to retrieve operation status"},
  },
)
@endpoint_metrics_decorator(
  "/v1/operations/{operation_id}/status",
  business_event_type="operation_status_checked",
)
async def get_operation_status(
  operation_id: str = Path(
    ...,
    description="Operation identifier",
    pattern="^([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|q_[0-9a-f]{12})$",
  ),
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> Dict[str, Any]:
  """
  Get current status of an operation.

  Provides point-in-time status information for operation monitoring
  and client state management.

  Args:
      operation_id: Unique operation identifier
      current_user: Authenticated user

  Returns:
      Dict containing operation status and metadata

  Raises:
      HTTPException: For access errors or operation not found
  """
  try:
    # Get operation metadata
    event_storage = get_event_storage()
    metadata = await event_storage.get_operation_metadata(operation_id)

    if not metadata:
      # Record metrics for not found
      metrics_instance = get_endpoint_metrics()
      metrics_instance.record_business_event(
        endpoint="/v1/operations/{operation_id}/status",
        method="GET",
        event_type="operation_status_not_found",
        event_data={
          "operation_id": operation_id,
        },
        user_id=current_user.id,
      )

      raise HTTPException(
        status_code=http_status.HTTP_404_NOT_FOUND,
        detail="Operation not found. It may have expired or been cancelled.",
      )

    # Check user access
    if metadata.user_id != current_user.id:
      # Record metrics for access denied
      metrics_instance = get_endpoint_metrics()
      metrics_instance.record_business_event(
        endpoint="/v1/operations/{operation_id}/status",
        method="GET",
        event_type="operation_status_access_denied",
        event_data={
          "operation_id": operation_id,
          "operation_user_id": metadata.user_id,
          "requesting_user_id": current_user.id,
        },
        user_id=current_user.id,
      )

      raise HTTPException(
        status_code=http_status.HTTP_403_FORBIDDEN,
        detail="Access denied to operation.",
      )

    # Build response
    response = {
      "operation_id": metadata.operation_id,
      "operation_type": metadata.operation_type,
      "status": metadata.status,
      "created_at": metadata.created_at,
      "updated_at": metadata.updated_at,
      "graph_id": metadata.graph_id,
    }

    # Add result or error data
    if metadata.status == OperationStatus.COMPLETED and metadata.result_data:
      response["result"] = metadata.result_data
    elif metadata.status == OperationStatus.FAILED and metadata.error_message:
      response["error"] = metadata.error_message

    # Add helpful links
    links = {
      "stream": f"/v1/operations/{operation_id}/stream",
    }

    if metadata.status in [OperationStatus.PENDING, OperationStatus.RUNNING]:
      links["cancel"] = f"/v1/operations/{operation_id}"

    response["_links"] = links

    # Add status-specific messages
    if metadata.status == OperationStatus.PENDING:
      response["message"] = "Operation is pending execution"
    elif metadata.status == OperationStatus.RUNNING:
      response["message"] = "Operation is currently executing"
    elif metadata.status == OperationStatus.COMPLETED:
      response["message"] = "Operation completed successfully"
    elif metadata.status == OperationStatus.FAILED:
      response["message"] = "Operation execution failed"
    elif metadata.status == OperationStatus.CANCELLED:
      response["message"] = "Operation was cancelled"

    # Record successful status check metrics
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/operations/{operation_id}/status",
      method="GET",
      event_type="operation_status_checked_details",
      event_data={
        "operation_id": operation_id,
        "operation_type": metadata.operation_type,
        "operation_status": metadata.status,
        "graph_id": metadata.graph_id,
      },
      user_id=current_user.id,
    )

    return response

  except HTTPException:
    raise

  except Exception as e:
    # Log unexpected errors
    logger.error(f"Unexpected error in operation status endpoint: {e}")

    # Record error metrics
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/operations/{operation_id}/status",
      method="GET",
      event_type="operation_status_error",
      event_data={
        "operation_id": operation_id,
        "error_type": type(e).__name__,
        "error_message": str(e),
      },
      user_id=current_user.id,
    )

    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to retrieve operation status",
    )


@router.delete(
  "/operations/{operation_id}",
  summary="Cancel Operation",
  description="""Cancel a pending or running operation.

Cancels the specified operation if it's still in progress. Once cancelled,
the operation cannot be resumed and will emit a cancellation event to any
active SSE connections.

**Note**: Completed or already failed operations cannot be cancelled.

**No credits are consumed for cancellation requests.**""",
  operation_id="cancelOperation",
  responses={
    200: {"description": "Operation cancelled successfully"},
    403: {"description": "Access denied to operation"},
    404: {"description": "Operation not found"},
    409: {"description": "Operation cannot be cancelled (already completed)"},
    500: {"description": "Failed to cancel operation"},
  },
)
@endpoint_metrics_decorator(
  "/v1/operations/{operation_id}",
  business_event_type="operation_cancelled",
)
async def cancel_operation(
  operation_id: str = Path(
    ...,
    description="Operation identifier",
    pattern="^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
  ),
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> Dict[str, Any]:
  """
  Cancel a pending or running operation.

  Args:
      operation_id: Unique operation identifier
      current_user: Authenticated user

  Returns:
      Dict confirming cancellation

  Raises:
      HTTPException: For access errors, operation not found, or cannot cancel
  """
  try:
    # Get operation metadata
    event_storage = get_event_storage()
    metadata = await event_storage.get_operation_metadata(operation_id)

    if not metadata:
      raise HTTPException(
        status_code=http_status.HTTP_404_NOT_FOUND,
        detail="Operation not found. It may have expired or been cancelled.",
      )

    # Check user access
    if metadata.user_id != current_user.id:
      raise HTTPException(
        status_code=http_status.HTTP_403_FORBIDDEN,
        detail="Access denied to operation.",
      )

    # Check if operation can be cancelled
    if metadata.status in [
      OperationStatus.COMPLETED,
      OperationStatus.FAILED,
      OperationStatus.CANCELLED,
    ]:
      raise HTTPException(
        status_code=http_status.HTTP_409_CONFLICT,
        detail=f"Operation cannot be cancelled - current status is {metadata.status}",
      )

    # Cancel the operation
    await event_storage.cancel_operation(
      operation_id, reason="Cancelled by user request"
    )

    # Record cancellation metrics
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/operations/{operation_id}",
      method="DELETE",
      event_type="operation_cancelled_details",
      event_data={
        "operation_id": operation_id,
        "operation_type": metadata.operation_type,
        "previous_status": metadata.status,
        "graph_id": metadata.graph_id,
      },
      user_id=current_user.id,
    )

    logger.info(f"User {current_user.id} cancelled operation {operation_id}")

    return {
      "operation_id": operation_id,
      "status": "cancelled",
      "message": "Operation has been cancelled",
    }

  except HTTPException:
    raise

  except Exception as e:
    # Log unexpected errors
    logger.error(f"Unexpected error in cancel operation endpoint: {e}")

    # Record error metrics
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/operations/{operation_id}",
      method="DELETE",
      event_type="operation_cancel_error",
      event_data={
        "operation_id": operation_id,
        "error_type": type(e).__name__,
        "error_message": str(e),
      },
      user_id=current_user.id,
    )

    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to cancel operation",
    )
