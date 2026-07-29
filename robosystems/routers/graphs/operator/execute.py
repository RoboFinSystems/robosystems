"""Operator execution endpoints with intelligent strategy selection."""

import asyncio

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
  BatchOperatorRequest,
  BatchOperatorResponse,
  OperatorListResponse,
  OperatorMetadataResponse,
  OperatorMode,
  OperatorRecommendation,
  OperatorRecommendationRequest,
  OperatorRecommendationResponse,
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
  OperatorOrchestrator,
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
  """Apply shared-repository access + per-plan agent volume limits.

  Repository plans advertise agent_calls_per_* limits; until this hook,
  nothing ever checked them (query/mcp/search had their equivalents, the
  operator surface did not), so the advertised numbers were unenforced.
  """
  from robosystems.routers.graphs.query.execute import (
    _check_shared_repository_limits,
  )

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
  description="Routes to the best operator for your query. Operators: `financial` (SEC, accounting), `research` (deep analysis), `rag` (knowledge base, free). Credit cost by mode: `quick` 5-10, `standard` 15-25, `extended` 30-75. Execution strategy (sync/SSE/async) auto-selected; override with `?mode=sync|async`.",
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
      "context": request.context,
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
  description="Available: `financial` (SEC filings, accounting), `research` (deep analysis), `rag` (retrieval, no credits). Execution strategy auto-selected; override with `?mode=sync|async`.",
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
      "context": request.context,
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


@router.post(
  "/operator/batch",
  response_model=BatchOperatorResponse,
  summary="Batch Process Queries",
  description="Process up to 10 queries sequentially or in parallel. Partial failure is supported — each result has individual error handling.",
  operation_id="batchProcessQueries",
  responses={
    **RESOURCE_ERROR_RESPONSES,
    402: {"description": "Insufficient credits"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/operator/batch", business_event_type="agent_batch_query"
)
async def batch_operator(
  request: BatchOperatorRequest,
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> BatchOperatorResponse:
  _check_operator_post_enabled()
  await _enforce_shared_repository_agent_limits(graph_id, current_user, db)

  import time

  start_time = time.time()
  orchestrator = OperatorOrchestrator(graph_id, current_user, db)

  async def process_single(query_request: OperatorRequest) -> OperatorResponse:
    """Process a single query."""
    history = [
      {"role": msg.role, "content": msg.content} for msg in query_request.history
    ]

    operator_response = await orchestrator.route_query(
      query=query_request.message,
      operator_type=query_request.operator_type,
      mode=_convert_operator_mode(query_request.mode),
      history=history,
      context=query_request.context,
    )

    return OperatorResponse(
      content=operator_response.content,
      operator_used=operator_response.operator_name,
      mode_used=OperatorMode(operator_response.mode_used.value),
      metadata=operator_response.metadata,
      tokens_used=operator_response.tokens_used,
      confidence_score=operator_response.confidence_score,
      error_details=operator_response.error_details,
      execution_time=operator_response.execution_time,
      operation_id=None,
      is_partial=False,
    )

  # Process queries
  if request.parallel:
    # Parallel processing
    tasks = [process_single(q) for q in request.queries]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Filter out exceptions
    valid_results = []
    for r in results:
      if isinstance(r, Exception):
        logger.error(f"Batch query failed: {r!s}")
        valid_results.append(
          OperatorResponse(
            content=f"Query failed: {r!s}",
            operator_used="error",
            mode_used=OperatorMode.STANDARD,
            error_details={"error": str(r)},
            metadata=None,
            tokens_used=None,
            confidence_score=None,
            operation_id=None,
            is_partial=False,
            execution_time=None,
          )
        )
      else:
        valid_results.append(r)

    results = valid_results
  else:
    # Sequential processing
    results = []
    for q in request.queries:
      try:
        result = await process_single(q)
        results.append(result)
      except Exception as e:
        logger.error(f"Batch query failed: {e!s}")
        results.append(
          OperatorResponse(
            content=f"Query failed: {e!s}",
            operator_used="error",
            mode_used=OperatorMode.STANDARD,
            error_details={"error": str(e)},
            metadata=None,
            tokens_used=None,
            confidence_score=None,
            operation_id=None,
            is_partial=False,
            execution_time=None,
          )
        )

  return BatchOperatorResponse(
    results=results,
    total_execution_time=time.time() - start_time,
    parallel_processed=request.parallel,
  )


@router.post(
  "/operator/recommend",
  response_model=OperatorRecommendationResponse,
  summary="Get Operator Recommendations",
  description="Returns operators ranked by confidence score for a query, with explanations. Use before execution when unsure which operator to pick.",
  operation_id="recommendOperator",
  responses={**RESOURCE_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/operator/recommend", business_event_type="agent_recommend"
)
async def recommend_operator(
  request: OperatorRecommendationRequest,
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> OperatorRecommendationResponse:
  _check_operator_post_enabled()

  orchestrator = OperatorOrchestrator(graph_id, current_user, db)
  recommendations_raw = orchestrator.get_operator_recommendations(
    request.query, request.context
  )

  # Convert to response format
  recommendations = [
    OperatorRecommendation(
      operator_type=r["operator_type"],
      operator_name=r["operator_name"],
      confidence=r["confidence"],
      capabilities=r["capabilities"],
      reason=r.get("reason"),
    )
    for r in recommendations_raw
  ]

  return OperatorRecommendationResponse(
    recommendations=recommendations, query=request.query
  )
