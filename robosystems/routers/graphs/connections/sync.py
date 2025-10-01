"""
Connection sync endpoint.
"""

from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.orm import Session

from robosystems.models.iam import User
from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.operations.connection_service import ConnectionService
from robosystems.database import get_db_session
from robosystems.logger import logger
from robosystems.models.api.connection import SyncConnectionRequest
from robosystems.models.api.common import (
  ErrorResponse,
  ErrorCode,
  create_error_response,
)

from .utils import (
  provider_registry,
  create_robustness_components,
  record_operation_start,
  record_operation_success,
  record_operation_failure,
)

import asyncio

router = APIRouter()


@router.post(
  "/{connection_id}/sync",
  summary="Sync Connection",
  description="""Trigger a data synchronization for the connection.

Initiates data sync based on provider type:

**SEC Sync**:
- Downloads latest filings from EDGAR
- Parses XBRL data and updates graph
- Typically completes in 5-10 minutes

**QuickBooks Sync**:
- Fetches latest transactions and balances
- Updates chart of accounts
- Generates fresh trial balance
- Duration depends on data volume

**Plaid Sync**:
- Retrieves recent bank transactions
- Updates account balances
- Categorizes new transactions

Note:
This operation is FREE - no credit consumption required.

Returns a task ID for monitoring sync progress.""",
  operation_id="syncConnection",
  responses={
    200: {
      "description": "Sync started successfully",
      "content": {
        "application/json": {
          "example": {
            "task_id": "task_123456",
            "status": "queued",
            "message": "Sync operation queued for processing",
          }
        }
      },
    },
    403: {"description": "Access denied - admin role required", "model": ErrorResponse},
    404: {"description": "Connection not found", "model": ErrorResponse},
    500: {"description": "Failed to start sync", "model": ErrorResponse},
  },
)
async def sync_connection(
  graph_id: str = Path(..., description="Graph database identifier"),
  connection_id: str = Path(..., description="Connection identifier"),
  request: SyncConnectionRequest = ...,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> dict:
  """
  Trigger a sync operation for a specific connection.

  This will queue the appropriate sync task based on the connection provider.
  """
  # Initialize robustness components
  components = create_robustness_components()
  operation_timeout = None

  # Record operation start metrics
  record_operation_start(
    operation_name="sync_connection",
    endpoint="/v1/graphs/{graph_id}/connections/{connection_id}/sync",
    graph_id=graph_id,
    user_id=current_user.id,
    metadata={"connection_id": connection_id},
  )

  try:
    # Check circuit breaker before processing
    components["circuit_breaker"].check_circuit(graph_id, "connection_sync")

    # Set up timeout coordination for sync operations (these can be long-running)
    operation_timeout = components["timeout_coordinator"].calculate_timeout(
      operation_type="external_service",
      complexity_factors={
        "operation": "sync_connection",
        "is_sync_operation": True,
        "expected_complexity": "high",  # Sync operations can be complex
      },
    )

    # Log the request with operation logger
    components["operation_logger"].log_external_service_call(
      endpoint="/v1/graphs/{graph_id}/connections/{connection_id}/sync",
      service_name="connection_service",
      operation="sync_connection",
      duration_ms=0.0,  # Will be updated on completion
      status="processing",
      graph_id=graph_id,
      user_id=current_user.id,
      metadata={"connection_id": connection_id},
    )

    # Get connection details
    connection = await ConnectionService.get_connection(connection_id, current_user.id)

    if not connection:
      raise create_error_response(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Connection not found",
        code=ErrorCode.NOT_FOUND,
      )

    provider = connection["provider"].lower()

    # Validate provider is enabled before any sync operations
    provider_registry.get_provider(provider)

    # Sync using provider registry with timeout coordination
    task_id = await asyncio.wait_for(
      provider_registry.sync_connection(
        provider, connection, request.sync_options, graph_id
      ),
      timeout=operation_timeout,
    )

    logger.info(f"Sync initiated for connection {connection_id}: task_id={task_id}")

    # Record successful operation
    record_operation_success(
      components=components,
      operation_name="sync_connection",
      endpoint="/v1/graphs/{graph_id}/connections/{connection_id}/sync",
      graph_id=graph_id,
      user_id=current_user.id,
      metadata={
        "connection_id": connection_id,
        "provider": provider,
        "task_id": task_id,
      },
    )

    return {
      "message": f"{provider.upper()} sync started",
      "connection_id": connection_id,
      "task_id": task_id,
      "status": "pending",
    }

  except asyncio.TimeoutError:
    # Record circuit breaker failure and timeout metrics
    record_operation_failure(
      components=components,
      operation_name="sync_connection",
      endpoint="/v1/graphs/{graph_id}/connections/{connection_id}/sync",
      graph_id=graph_id,
      user_id=current_user.id,
      error_type="timeout",
      timeout_seconds=operation_timeout,
    )

    timeout_str = f" after {operation_timeout}s" if operation_timeout else ""
    logger.error(f"Connection sync timeout{timeout_str} for user {current_user.id}")
    raise create_error_response(
      status_code=status.HTTP_504_GATEWAY_TIMEOUT,
      detail="Connection sync timed out",
      code=ErrorCode.TIMEOUT,
    )
  except HTTPException:
    # Record circuit breaker failure for HTTP exceptions
    record_operation_failure(
      components=components,
      operation_name="sync_connection",
      endpoint="/v1/graphs/{graph_id}/connections/{connection_id}/sync",
      graph_id=graph_id,
      user_id=current_user.id,
      error_type="http_exception",
    )
    raise
  except ValueError as e:
    # Handle disabled provider errors as client errors
    record_operation_failure(
      components=components,
      operation_name="sync_connection",
      endpoint="/v1/graphs/{graph_id}/connections/{connection_id}/sync",
      graph_id=graph_id,
      user_id=current_user.id,
      error_type="provider_disabled",
      error_message=str(e),
    )

    logger.warning(f"Provider not available for sync: {e}")
    raise create_error_response(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=str(e),
      code=ErrorCode.FORBIDDEN,
    )
  except Exception as e:
    # Record circuit breaker failure for general exceptions
    record_operation_failure(
      components=components,
      operation_name="sync_connection",
      endpoint="/v1/graphs/{graph_id}/connections/{connection_id}/sync",
      graph_id=graph_id,
      user_id=current_user.id,
      error_type=type(e).__name__,
      error_message=str(e),
    )

    logger.error(f"Failed to sync connection {connection_id}: {e}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to sync connection: {str(e)}",
      code=ErrorCode.INTERNAL_ERROR,
    )
