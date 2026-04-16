"""Unified operations endpoint for Server-Sent Events monitoring."""

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request
from fastapi import status as http_status
from sse_starlette.sse import EventSourceResponse

from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import (
  get_current_user,
  get_current_user_sse,
)
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from robosystems.middleware.rate_limits import (
  sse_connection_rate_limit_dependency,
  subscription_aware_rate_limit_dependency,
)
from robosystems.middleware.sse.event_storage import OperationStatus, get_event_storage
from robosystems.middleware.sse.streaming import create_sse_response_starlette
from robosystems.models.api.common import RESOURCE_ERROR_RESPONSES
from robosystems.models.core import User

router = APIRouter()


@router.get(
  "/operations/{operation_id}/stream",
  summary="Stream Operation Events",
  description="Server-Sent Events stream for real-time operation progress. Use `from_sequence` to replay missed events on reconnect. Max 5 concurrent SSE connections per user. Consumes no credits.",
  operation_id="streamOperationEvents",
  responses={
    200: {"description": "SSE stream — Content-Type: text/event-stream"},
    **RESOURCE_ERROR_RESPONSES,
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
    pattern="^(op_[0-9A-Z]{26}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$",
  ),
  from_sequence: int = Query(
    0,
    description="Start streaming from this sequence number (0 = from beginning)",
    ge=0,
  ),
  current_user: User = Depends(get_current_user_sse),
  request: Request = None,
  _rate_limit: None = Depends(sse_connection_rate_limit_dependency),
) -> EventSourceResponse:
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
  description="Point-in-time status check. Use `/stream` for real-time updates. Consumes no credits.",
  operation_id="getOperationStatus",
  responses={**RESOURCE_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/v1/operations/{operation_id}/status",
  business_event_type="operation_status_checked",
)
async def get_operation_status(
  operation_id: str = Path(
    ...,
    description="Operation identifier",
    pattern="^(op_[0-9A-Z]{26}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$",
  ),
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> dict[str, Any]:
  try:
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
  description="Cancels a pending or running operation. Emits a cancellation event to any active SSE connections. Cannot cancel completed or failed operations.",
  operation_id="cancelOperation",
  responses={
    **RESOURCE_ERROR_RESPONSES,
    409: {"description": "Operation already completed or failed — cannot cancel"},
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
    pattern="^(op_[0-9A-Z]{26}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$",
  ),
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> dict[str, Any]:
  try:
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
