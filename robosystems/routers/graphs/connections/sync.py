"""
Connection sync endpoint.
"""

from fastapi import APIRouter, Depends, Header, HTTPException, Path, status
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import (
  get_current_user_with_graph,
  require_graph_write_role,
)
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.operations import (
  IdempotencyCache,
  OperationEnvelope,
  check_idempotency,
  fingerprint_body,
  generate_operation_id,
  get_idempotency_cache,
  log_operation_audit,
  wrap_pending,
)
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.common import (
  OPERATION_ERROR_RESPONSES,
  ErrorCode,
  create_error_response,
)
from robosystems.models.api.graphs.connections import SyncConnectionRequest
from robosystems.models.core import User
from robosystems.operations.connection_service import (
  ConnectionNotFoundError,
  ProviderUnavailableError,
  SyncInProgressError,
  dispatch_connection_sync,
)

from .utils import (
  create_robustness_components,
  record_operation_failure,
  record_operation_start,
  record_operation_success,
)

router = APIRouter()


@router.post(
  "/{connection_id}/sync",
  summary="Sync Connection",
  description="SEC: downloads latest EDGAR filings (5-10 min). QuickBooks: fetches transactions, balances, and chart of accounts. Async — returns an `OperationEnvelope` with the provider task id; completion is reflected in the connection's `last_sync` timestamp. Supports `Idempotency-Key`.",
  response_model=OperationEnvelope,
  status_code=status.HTTP_202_ACCEPTED,
  operation_id="syncConnection",
  responses={
    **OPERATION_ERROR_RESPONSES,
    504: {"description": "Sync request timed out"},
  },
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/graphs/{graph_id}/connections/{connection_id}/sync",
  method="POST",
  business_event_type="connection_synced",
)
async def sync_connection(
  graph_id: str = Path(
    ..., description="Graph database identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  connection_id: str = Path(..., description="Connection identifier"),
  request: SyncConnectionRequest = ...,
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  op_name = "sync-connection"
  user_id = str(current_user.id)

  # A sync rewrites the graph's captured events (full_rebuild wipes and
  # reloads them); membership alone is not enough. Same gate the MCP
  # `sync-connection` tool clears through its write classification.
  require_graph_write_role(user_id, graph_id)

  body_fp = fingerprint_body(request)

  replay = await check_idempotency(
    cache, user_id, graph_id, op_name, idempotency_key, body_fp
  )
  if replay is not None:
    return replay

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

    # Validate, lock, and dispatch via the shared kernel (also behind
    # the `sync-connection` MCP tool); map domain exceptions to the
    # HTTP contract this endpoint has always had.
    try:
      dispatch = await dispatch_connection_sync(
        graph_id=graph_id,
        connection_id=connection_id,
        user_id=str(current_user.id),
        full_rebuild=request.full_rebuild,
        since_date=request.since_date,
        sync_options=request.sync_options,
        dispatch_timeout=operation_timeout,
      )
    except ConnectionNotFoundError:
      raise create_error_response(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Connection not found",
        code=ErrorCode.NOT_FOUND,
      )
    except SyncInProgressError as e:
      raise create_error_response(
        status_code=status.HTTP_409_CONFLICT,
        detail=str(e),
        code=ErrorCode.OPERATION_FAILED,
      )

    provider = dispatch["provider"]
    task_id = dispatch["task_id"]

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

    operation_id = generate_operation_id()
    envelope = wrap_pending(
      op_name,
      operation_id=operation_id,
      partial_result={
        "message": f"{provider.upper()} sync started",
        "connection_id": connection_id,
        "task_id": task_id,
      },
      created_by=user_id,
    )

    # Known limitation: cache.put is after task dispatch. See create_graph for details.
    if idempotency_key is not None:
      await cache.put(user_id, graph_id, op_name, idempotency_key, envelope, body_fp)

    log_operation_audit(
      operation_name=op_name,
      operation_id=operation_id,
      user_id=user_id,
      graph_id=graph_id,
      duration_ms=0.0,
      status="pending",
      idempotency_key=idempotency_key,
      event="graph.operation",
    )
    return envelope

  except TimeoutError:
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
      code=ErrorCode.OPERATION_FAILED,
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
  except (ProviderUnavailableError, ValueError) as e:
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
      detail="The requested provider is not available.",
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

    logger.error("Failed to sync connection %s", connection_id, exc_info=True)
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to sync connection",
      code=ErrorCode.INTERNAL_ERROR,
    )
