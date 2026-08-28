"""Operator execution: `POST /v1/graphs/{graph_id}/operator`.

Selects an operator from the query's intent, picks an execution strategy from
that operator's profile, and answers as JSON or SSE with progress. Operator
runs consume AI credits.
"""

from fastapi import (
  APIRouter,
  BackgroundTasks,
  Depends,
  HTTPException,
  Path,
  Query,
  Request,
)
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sse_starlette.sse import EventSourceResponse

from robosystems.config import env
from robosystems.database import get_db_session
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.common import RESOURCE_ERROR_RESPONSES
from robosystems.models.api.graphs.operator import (
  OperatorListResponse,
  OperatorMetadataResponse,
  OperatorMode,
  OperatorRequest,
  OperatorResponse,
)
from robosystems.models.core import User
from robosystems.operations.operators.base import (
  ExecutionProfile,
)
from robosystems.operations.operators.base import (
  OperatorMode as BaseOperatorMode,
)
from robosystems.operations.operators.operator_registry import (
  get_operator,
  list_operators,
)
from robosystems.operations.operators.orchestrator import (
  OperatorSelectionCriteria,
)

from .handlers import (
  handle_background_queue,
  handle_sse_streaming,
  handle_sync_execution,
)
from .strategies import (
  OperatorClientDetector,
  OperatorExecutionStrategy,
  OperatorStrategySelector,
  ResponseMode,
)

router = APIRouter()


async def _enforce_shared_repository_agent_limits(
  graph_id: str, current_user: User, db: Session
) -> None:
  """Apply the graph lifecycle gate, then shared-repository operator limits.

  Repository plans advertise ``agent_calls_per_*`` limits, and this is where
  the operator surface enforces them — the counterpart to the equivalent
  hooks on query, mcp and search. The lifecycle gate is the shared one, so a
  suspended or expired graph is refused here as it is everywhere else. Every
  entry point to a metered surface needs both; neither is optional.
  """
  from robosystems.middleware.billing.enforcement import require_graph_access
  from robosystems.routers.graphs.query.execute import (
    _check_shared_repository_limits,
  )

  # Lifecycle/subscription gate (read strength — write-capable operators run
  # the write-strength check through `enforce_operator_write_role`).
  require_graph_access(graph_id, db, require_write=False)

  await _check_shared_repository_limits(
    graph_id, current_user, db, endpoint="agent", operation="agent"
  )


def _check_operator_post_enabled():
  """Check if operator POST endpoints are enabled."""
  if not env.OPERATOR_POST_ENABLED:
    logger.warning("Operator POST operations blocked by feature flag")
    raise HTTPException(
      status_code=403,
      detail="Operator POST operations are currently disabled. Please contact support if you need assistance.",
    )


def _request_context(request: OperatorRequest) -> dict | None:
  """The free-form context dict handed to the operator as ctx.extra, with the
  typed per-question credit ceiling folded in when the caller set one."""
  if request.max_credits is None:
    return request.context
  return {**(request.context or {}), "max_credits": request.max_credits}


def _convert_operator_mode(mode: OperatorMode | None) -> BaseOperatorMode:
  """Convert API OperatorMode to base OperatorMode."""
  if mode is None:
    return BaseOperatorMode.STANDARD

  mode_mapping = {
    OperatorMode.QUICK: BaseOperatorMode.QUICK,
    OperatorMode.STANDARD: BaseOperatorMode.STANDARD,
    OperatorMode.EXTENDED: BaseOperatorMode.EXTENDED,
    OperatorMode.STREAMING: BaseOperatorMode.STREAMING,
  }
  return mode_mapping.get(mode, BaseOperatorMode.STANDARD)


@router.get(
  "/operator",
  response_model=OperatorListResponse,
  summary="List Available Operators",
  description="Filter by capability using the `capability` query param (e.g., `financial_analysis`, `rag_search`).",
  operation_id="listOperators",
  responses={**RESOURCE_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/operator", business_event_type="agent_list"
)
async def list_operators_endpoint(
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  capability: str | None = Query(
    None,
    description="Filter by capability (e.g., 'financial_analysis', 'rag_search')",
  ),
  _current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> OperatorListResponse:
  operators = list_operators()

  if capability:
    operators = {
      k: v for k, v in operators.items() if capability in v.get("capabilities", [])
    }

  return OperatorListResponse(operators=operators, total=len(operators))


@router.post(
  "/operator",
  response_model=OperatorResponse,
  summary="Auto-select Operator for Query",
  description=(
    "Routes to the best operator for your query. Operators: `cypher` "
    "(answers natural-language questions by querying the graph; supports "
    "`quick`, `standard`, `extended`) and `mapping` (autonomous Chart of "
    "Accounts → rs-gaap mapping; roboledger graphs only, `extended` only). "
    "`GET /v1/graphs/{graph_id}/operator` lists what is registered. Credits "
    "are consumed by actual token usage, not a fixed price per mode. "
    "Execution strategy (sync/SSE/async) auto-selected; override with "
    "`?mode=sync|async`."
  ),
  operation_id="autoSelectOperator",
  responses={
    **RESOURCE_ERROR_RESPONSES,
    202: {"description": "Query queued for async processing"},
    402: {"description": "Insufficient credits"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/operator", business_event_type="agent_query_auto"
)
async def auto_operator(
  request: OperatorRequest,
  full_request: Request,
  background_tasks: BackgroundTasks,
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  mode: ResponseMode | None = Query(
    None, description="Override execution mode: sync, async, stream, or auto"
  ),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> OperatorResponse | JSONResponse | EventSourceResponse:
  _check_operator_post_enabled()
  await _enforce_shared_repository_agent_limits(graph_id, current_user, db)

  try:
    # Detect client capabilities
    headers = dict(full_request.headers)
    client_info = OperatorClientDetector.detect_client_type(headers)

    # Use conservative execution profile for auto-routing
    base_mode = _convert_operator_mode(request.mode)
    conservative_profile = ExecutionProfile(
      min_time=5 if base_mode == BaseOperatorMode.QUICK else 8,
      max_time=15 if base_mode == BaseOperatorMode.QUICK else 25,
      avg_time=10 if base_mode == BaseOperatorMode.QUICK else 15,
      tool_calls=5,
    )

    # Select execution strategy
    strategy, strategy_metadata = OperatorStrategySelector.select_strategy(
      execution_profile=conservative_profile,
      client_info=client_info,
      mode_override=mode,
      force_extended=request.force_extended_analysis,
    )

    logger.info(
      f"Auto-operator strategy selected: {strategy.value} - {strategy_metadata['selection_reason']}"
    )

    # Convert selection criteria if provided
    selection_criteria = None
    if request.selection_criteria:
      from robosystems.operations.operators.base import OperatorCapability

      selection_criteria = OperatorSelectionCriteria(
        min_confidence=request.selection_criteria.min_confidence,
        required_capabilities=[
          OperatorCapability(cap)
          for cap in request.selection_criteria.required_capabilities
        ],
        preferred_mode=_convert_operator_mode(
          request.selection_criteria.preferred_mode
        ),
        max_response_time=request.selection_criteria.max_response_time,
        excluded_operators=request.selection_criteria.excluded_operators,
      )

    # Convert history to expected format
    history = [{"role": msg.role, "content": msg.content} for msg in request.history]

    # Prepare request data
    request_data = {
      "message": request.message,
      "mode": request.mode.value if request.mode else "standard",
      "history": history,
      "context": _request_context(request),
      "enable_rag": request.enable_rag,
      "force_extended_analysis": request.force_extended_analysis,
    }

    # Execute based on strategy
    if strategy == OperatorExecutionStrategy.SYNC_IMMEDIATE:
      return await handle_sync_execution(
        graph_id=graph_id,
        request_data=request_data,
        base_mode=base_mode,
        current_user=current_user,
        db=db,
        selection_criteria=selection_criteria,
      )

    elif strategy == OperatorExecutionStrategy.SSE_STREAMING:
      return await handle_sse_streaming(
        graph_id=graph_id,
        request_data=request_data,
        current_user=current_user,
        db=db,
      )

    elif strategy == OperatorExecutionStrategy.BACKGROUND_QUEUE:
      return await handle_background_queue(
        graph_id=graph_id,
        request_data=request_data,
        current_user=current_user,
        db=db,
        background_tasks=background_tasks,
      )

  except Exception as e:
    logger.error(f"Operator routing error: {e!s}", exc_info=True)
    raise HTTPException(
      status_code=500,
      detail="An internal error occurred while routing the operator request.",
    )


@router.get(
  "/operator/{operator_type}",
  response_model=OperatorMetadataResponse,
  summary="Get Operator Metadata",
  operation_id="getOperatorMetadata",
  responses={**RESOURCE_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/operator/{operator_type}",
  business_event_type="agent_metadata",
)
async def get_operator_metadata(
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  operator_type: str = Path(
    ...,
    description="Operator type identifier (e.g., 'financial', 'research', 'rag')",
    pattern="^[a-zA-Z][a-zA-Z0-9_]{2,32}$",
  ),
  _current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> OperatorMetadataResponse:
  operators = list_operators()
  metadata = operators.get(operator_type)

  if not metadata:
    raise HTTPException(status_code=404, detail=f"Operator '{operator_type}' not found")

  return OperatorMetadataResponse(**metadata)


@router.post(
  "/operator/{operator_type}",
  response_model=OperatorResponse,
  summary="Execute Specific Operator",
  description=(
    "Available: `cypher` (natural-language questions answered by querying the "
    "graph; RAG retrieval is one of its capabilities, not a separate operator) "
    "and `mapping` (Chart of Accounts → rs-gaap mapping, roboledger graphs "
    "only). `GET /v1/graphs/{graph_id}/operator` lists what is registered. "
    "Execution strategy auto-selected; override with `?mode=sync|async`."
  ),
  operation_id="executeSpecificOperator",
  responses={
    **RESOURCE_ERROR_RESPONSES,
    202: {"description": "Query queued for async processing"},
    402: {"description": "Insufficient credits"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/operator/{operator_type}",
  business_event_type="agent_query_specific",
)
async def specific_operator(
  operator_type: str,
  request: OperatorRequest,
  full_request: Request,
  background_tasks: BackgroundTasks,
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  mode: ResponseMode | None = Query(
    None, description="Override execution mode: sync, async, stream, or auto"
  ),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> OperatorResponse | JSONResponse | EventSourceResponse:
  _check_operator_post_enabled()
  await _enforce_shared_repository_agent_limits(graph_id, current_user, db)

  try:
    # Get operator to access execution profile (operators are lightweight, no graph/user needed)
    try:
      operator = get_operator(operator_type)
    except KeyError:
      raise HTTPException(
        status_code=404, detail=f"Operator type '{operator_type}' not found"
      )

    # Get execution profile for requested mode
    base_mode = _convert_operator_mode(request.mode)
    execution_profile = operator.spec.execution_profile.get(base_mode)

    # Detect client capabilities
    headers = dict(full_request.headers)
    client_info = OperatorClientDetector.detect_client_type(headers)

    # Select execution strategy
    strategy, strategy_metadata = OperatorStrategySelector.select_strategy(
      execution_profile=execution_profile,
      client_info=client_info,
      mode_override=mode,
      force_extended=request.force_extended_analysis,
    )

    logger.info(
      f"Operator '{operator_type}' strategy selected: {strategy.value} - {strategy_metadata['selection_reason']}"
    )

    # Convert history
    history = [{"role": msg.role, "content": msg.content} for msg in request.history]

    # Prepare request data
    request_data = {
      "message": request.message,
      "mode": request.mode.value if request.mode else "standard",
      "history": history,
      "context": _request_context(request),
      "enable_rag": request.enable_rag,
      "force_extended_analysis": request.force_extended_analysis,
    }

    # Execute based on strategy
    if strategy == OperatorExecutionStrategy.SYNC_IMMEDIATE:
      return await handle_sync_execution(
        graph_id=graph_id,
        request_data=request_data,
        base_mode=base_mode,
        current_user=current_user,
        db=db,
        operator_type=operator_type,
      )

    elif strategy == OperatorExecutionStrategy.SSE_STREAMING:
      return await handle_sse_streaming(
        graph_id=graph_id,
        request_data=request_data,
        current_user=current_user,
        db=db,
        operator_type=operator_type,
      )

    elif strategy == OperatorExecutionStrategy.BACKGROUND_QUEUE:
      return await handle_background_queue(
        graph_id=graph_id,
        request_data=request_data,
        current_user=current_user,
        db=db,
        background_tasks=background_tasks,
        operator_type=operator_type,
      )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Operator execution error: {e!s}", exc_info=True)
    raise HTTPException(
      status_code=500,
      detail="An internal error occurred while executing the operator.",
    )
