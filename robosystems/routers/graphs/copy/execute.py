"""
Main copy execution endpoint for data ingestion from various sources.

This module provides the primary copy execution endpoint that handles
data ingestion from S3, URLs, and other sources with proper authentication,
validation, rate limiting, and tier-based controls.
"""

import asyncio
import time

from fastapi import (
  APIRouter,
  Depends,
  HTTPException,
  Path,
  Request,
  status as http_status,
)
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.graph_api.client.factory import GraphClientFactory
from robosystems.middleware.graph.types import InstanceTier
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
)
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
)
from robosystems.middleware.robustness import CircuitBreakerManager
from robosystems.models.iam.user import User
from robosystems.security import SecurityAuditLogger, SecurityEventType
from robosystems.logger import logger, api_logger, log_metric

from .models import CopyRequest, CopyResponse, S3CopyRequest
from .strategies import CopyStrategyFactory
from .validation import (
  validate_copy_permissions,
  get_tier_limits,
  validate_size_limits,
  calculate_timeout,
)


# Initialize circuit breaker
circuit_breaker = CircuitBreakerManager()

# Create router for execute endpoint
router = APIRouter()


@router.post(
  "/copy",  # Full path without trailing slash
  response_model=CopyResponse,
  summary="Copy Data to Graph",
  description="""Copy data from external sources into the graph database.

This endpoint supports multiple data sources through a unified interface:
- **S3**: Copy from S3 buckets with user-provided credentials
- **URL** (future): Copy from HTTP(S) URLs
- **DataFrame** (future): Copy from uploaded DataFrames

**Security:**
- Requires write permissions to the target graph
- **Not allowed on shared repositories** (sec, industry, economic) - these are read-only
- User must provide their own AWS credentials for S3 access
- All operations are logged for audit purposes

**Tier Limits:**
- Standard: 10GB max file size, 15 min timeout
- Enterprise: 50GB max file size, 30 min timeout
- Premium: 100GB max file size, 60 min timeout

**Copy Options:**
- `ignore_errors`: Skip duplicate/invalid rows (enables upsert-like behavior). Note: When enabled, row counts may not be accurately reported
- `extended_timeout`: Use extended timeout for large datasets
- `validate_schema`: Validate source schema against target table

**Asynchronous Execution with SSE:**
For large data imports, this endpoint returns immediately with an operation ID
and SSE monitoring endpoint. Connect to the returned stream URL for real-time updates:

```javascript
const eventSource = new EventSource('/v1/operations/{operation_id}/stream');
eventSource.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('Progress:', data.message);
};
```

**SSE Events Emitted:**
- `operation_started`: Copy operation begins
- `operation_progress`: Progress updates during data transfer
- `operation_completed`: Copy successful with statistics
- `operation_error`: Copy failed with error details

**SSE Connection Limits:**
- Maximum 5 concurrent SSE connections per user
- Rate limited to 10 new connections per minute
- Automatic circuit breaker for Redis failures
- Graceful degradation if event system unavailable

**Error Handling:**
- `403 Forbidden`: Attempted copy to shared repository
- `408 Request Timeout`: Operation exceeded timeout limit
- `429 Too Many Requests`: Rate limit exceeded
- `503 Service Unavailable`: Circuit breaker open or service unavailable
- Clients should implement exponential backoff on errors

**Note:**
Copy operations are FREE - no credit consumption required.
All copy operations are performed asynchronously with progress monitoring.""",
  operation_id="copyDataToGraph",
  dependencies=[Depends(subscription_aware_rate_limit_dependency)],
  responses={
    200: {"description": "Copy operation accepted and started"},
    202: {"description": "Copy operation queued for execution"},
    400: {"description": "Invalid request parameters"},
    403: {"description": "Access denied or shared repository"},
    408: {"description": "Operation timeout"},
    429: {"description": "Rate limit exceeded"},
    500: {"description": "Internal error"},
    503: {"description": "Service unavailable"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/copy", business_event_type="copy_executed"
)
async def copy_data(
  request: CopyRequest,
  graph_id: str = Path(
    ...,
    description="Target graph identifier (user graphs only - shared repositories not allowed)",
    pattern=r"^(kg[a-z0-9]{10,20}|sec|industry|economic)$",
  ),
  current_user: User = Depends(get_current_user),
  session: Session = Depends(get_db_session),
  client_request: Request = None,
) -> CopyResponse:
  """
  Execute copy operation from external source to graph database.

  Args:
      request: Copy request with source configuration
      graph_id: Target graph identifier
      current_user: Authenticated user
      session: Database session
      client_request: FastAPI request object for client info

  Returns:
      CopyResponse with operation status and statistics

  Raises:
      HTTPException: For validation, permission, or execution errors
  """

  start_time = time.time()
  client_ip = (
    client_request.client.host
    if client_request and client_request.client
    else "unknown"
  )

  # Prevent copy operations on shared repositories
  from robosystems.config.billing.repositories import SharedRepository

  if graph_id in [repo.value for repo in SharedRepository]:
    api_logger.warning(
      f"Copy operation blocked on shared repository {graph_id}",
      extra={
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "source_type": request.source_type,
        "client_ip": client_ip,
      },
    )

    # Log security event for audit trail
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTHORIZATION_DENIED,
      details={
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "operation": "copy",
        "reason": "shared_repository_write_attempt",
        "source_type": request.source_type,
        "table_name": request.table_name,
        "client_ip": client_ip,
      },
      risk_level="medium",
    )

    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail=f"Copy operations are not allowed on shared repository '{graph_id}'. "
      f"Shared repositories are read-only and managed by the platform.",
    )

  # Check circuit breaker
  circuit_breaker.check_circuit(graph_id, "copy_operation")

  # Log the operation start
  api_logger.info(
    "Copy operation started",
    extra={
      "user_id": current_user.id,
      "graph_id": graph_id,
      "source_type": request.source_type,
      "table_name": request.table_name,
      "client_ip": client_ip,
    },
  )

  try:
    # 1. Validate permissions
    await validate_copy_permissions(
      graph_id=graph_id,
      current_user=current_user,
      session=session,
    )

    # 2. Get tier limits
    tier_limits = get_tier_limits(current_user)

    # 3. Validate request against tier limits
    if isinstance(request, S3CopyRequest):
      validate_size_limits(
        max_file_size_gb=request.max_file_size_gb,
        tier_limits=tier_limits,
        user_id=current_user.id,
      )

    # 4. Calculate timeout
    timeout_seconds = calculate_timeout(
      tier_limits=tier_limits,
      extended_timeout=request.extended_timeout,
    )

    # 5. Create strategy based on source type
    strategy = CopyStrategyFactory.create_strategy(request.source_type)

    # 6. Validate request with strategy
    await strategy.validate(
      request=request,
      user_id=current_user.id,
      graph_id=graph_id,
      client_ip=client_ip,
    )

    # 7. Get Graph client for the target graph
    graph_client = await GraphClientFactory.create_client(
      graph_id=graph_id,
      operation_type="write",
      tier=tier_limits.get("tier", InstanceTier.STANDARD)
      if hasattr(tier_limits, "get")
      else InstanceTier.STANDARD,
    )

    # 8. Execute copy operation
    logger.info(
      f"Executing {request.source_type} copy for user {current_user.id} "
      f"to {graph_id}.{request.table_name}"
    )

    try:
      result = await strategy.execute(
        request=request,
        graph_client=graph_client,
        graph_id=graph_id,
        user_id=current_user.id,
        timeout_seconds=timeout_seconds,
      )

      # 9. Process result and build response
      execution_time_ms = (time.time() - start_time) * 1000

      # Check if this is an SSE-monitored operation
      operation_id = result.get("operation_id")

      # Initialize variables for both sync and async cases
      rows_imported = 0
      rows_skipped = 0
      bytes_processed = None
      warnings = []

      if operation_id:
        # This is a long-running operation with SSE monitoring
        status_value = "accepted"
        rows_imported = 0  # Will be updated via SSE

        response = CopyResponse(
          status="accepted",
          operation_id=operation_id,
          sse_url=f"/v1/operations/{operation_id}/stream",
          source_type=request.source_type,
          message=f"Copy operation started. Monitor progress at /v1/operations/{operation_id}/stream",
          execution_time_ms=None,
          rows_imported=None,
          rows_skipped=None,
          warnings=None,
          error_details=None,
          bytes_processed=None,
        )
      else:
        # Synchronous operation - process normally
        # Extract statistics from result
        rows_imported = result.get("rows_imported", 0)
        rows_skipped = result.get("rows_skipped", 0)
        bytes_processed = result.get("bytes_processed")
        warnings = result.get("warnings", [])

        # Determine status
        if result.get("status") == "failed":
          status_value = "failed"
          message = result.get("message", "Copy operation failed")
        elif rows_skipped > 0 and rows_imported > 0:
          status_value = "partial"
          message = (
            f"Imported {rows_imported:,} rows, skipped {rows_skipped:,} due to errors"
          )
        elif rows_imported > 0:
          status_value = "completed"
          message = (
            f"Successfully imported {rows_imported:,} rows from {request.source_type}"
          )
        else:
          status_value = "failed"
          message = "No rows were imported"

        response = CopyResponse(
          status=status_value,
          source_type=request.source_type,
          execution_time_ms=execution_time_ms,
          message=message,
          rows_imported=rows_imported if rows_imported > 0 else None,
          rows_skipped=rows_skipped if rows_skipped > 0 else None,
          bytes_processed=bytes_processed,
          warnings=warnings if warnings else None,
          operation_id=None,
          sse_url=None,
          error_details=None,
        )

      # 10. Log success metrics
      log_metric(
        "copy_operation_execution_time",
        execution_time_ms,
        unit="milliseconds",
        component="copy",
        metadata={
          "source_type": request.source_type,
          "status": status_value,
          "rows_imported": rows_imported,
          "graph_id": graph_id,
          "user_tier": getattr(current_user, "subscription_tier", "standard"),
        },
      )

      # 11. Audit log for successful operation
      if status_value in ["completed", "partial"]:
        SecurityAuditLogger.log_security_event(
          event_type=SecurityEventType.DATA_IMPORT,
          details={
            "user_id": current_user.id,
            "graph_id": graph_id,
            "table_name": request.table_name,
            "source_type": request.source_type,
            "rows_imported": rows_imported,
            "rows_skipped": rows_skipped,
            "execution_time_ms": execution_time_ms,
            "client_ip": client_ip,
          },
          risk_level="low" if status_value == "completed" else "medium",
        )

      return response

    except asyncio.TimeoutError:
      # Handle timeout
      execution_time_ms = (time.time() - start_time) * 1000

      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.OPERATION_TIMEOUT,
        details={
          "user_id": current_user.id,
          "graph_id": graph_id,
          "table_name": request.table_name,
          "source_type": request.source_type,
          "timeout_seconds": timeout_seconds,
          "client_ip": client_ip,
        },
        risk_level="medium",
      )

      raise HTTPException(
        status_code=http_status.HTTP_504_GATEWAY_TIMEOUT,
        detail=f"Copy operation timed out after {timeout_seconds} seconds. "
        f"Consider using extended_timeout=true for large datasets.",
      )

    except Exception as e:
      # Handle execution errors
      execution_time_ms = (time.time() - start_time) * 1000

      logger.error(
        f"Copy operation failed for user {current_user.id}: {str(e)}",
        extra={
          "user_id": current_user.id,
          "graph_id": graph_id,
          "source_type": request.source_type,
          "error": str(e),
        },
      )

      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.OPERATION_FAILED,
        details={
          "user_id": current_user.id,
          "graph_id": graph_id,
          "table_name": request.table_name,
          "source_type": request.source_type,
          "error": str(e),
          "client_ip": client_ip,
        },
        risk_level="high",
      )

      # Return error response instead of raising exception
      return CopyResponse(
        status="failed",
        source_type=request.source_type,
        execution_time_ms=execution_time_ms,
        message=f"Copy operation failed: {str(e)}",
        error_details={"error": str(e), "type": type(e).__name__},
        operation_id=None,
        sse_url=None,
        rows_imported=None,
        rows_skipped=None,
        warnings=None,
        bytes_processed=None,
      )

  except HTTPException:
    # Re-raise HTTP exceptions
    raise

  except Exception as e:
    # Handle unexpected errors
    execution_time_ms = (time.time() - start_time) * 1000

    logger.error(
      f"Unexpected error in copy operation: {str(e)}",
      exc_info=True,
      extra={
        "user_id": current_user.id,
        "graph_id": graph_id,
        "source_type": getattr(request, "source_type", "unknown"),
      },
    )

    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Internal error during copy operation: {str(e)}",
    )
