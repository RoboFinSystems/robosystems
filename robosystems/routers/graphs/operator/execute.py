"""Operator execution: `POST /v1/graphs/{graph_id}/operator`.

Every operator run executes on the background worker. The endpoint resolves
the operator, runs the gates that can refuse the request outright (graph
lifecycle, repository limits, write role, graph scope, credits), enqueues the
run and answers 202 with the operation's stream/status/cancel links. Under
`?mode=sync` it waits a bounded time for the worker and answers 200 with the
result when it lands in time. Operator runs consume AI credits.
"""

import asyncio
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from robosystems.config import env
from robosystems.database import get_db_session
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.execution_strategies import ResponseMode
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.middleware.sse.event_storage import (
  OperationMetadata,
  OperationStatus,
  get_event_storage,
)
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
  Operator,
  enforce_operator_graph_scope,
  enforce_operator_write_role,
)
from robosystems.operations.operators.base import (
  OperatorMode as BaseOperatorMode,
)
from robosystems.operations.operators.credit_preflight import (
  InsufficientOperatorCreditsError,
  enforce_operator_credits,
)
from robosystems.operations.operators.operator_registry import (
  get_operator,
  list_operators,
)
from robosystems.operations.operators.orchestrator import (
  OperatorOrchestrator,
  OrchestratorConfig,
  RoutingStrategy,
)
from robosystems.worker.client import enqueue_task

router = APIRouter()

# How long `?mode=sync` waits for the worker before answering 202 instead.
# Below the 60s idle timeout on the load balancers in front of the API: a
# wait that outlived it would 504 at the edge with the run still going.
SYNC_WAIT_SECONDS = 50
SYNC_POLL_INTERVAL_SECONDS = 0.5

# Statuses at which a sync wait stops: the run has an answer, or it has
# left the queue for a human decision and no wait will finish it.
_SETTLED_STATUSES = frozenset(
  {
    OperationStatus.COMPLETED,
    OperationStatus.FAILED,
    OperationStatus.CANCELLED,
    OperationStatus.AWAITING_INPUT,
  }
)

_MODE_DESCRIPTION = (
  "`sync` waits up to 50s for the answer and returns 200 with it (202 with "
  "the operation links if the worker is still busy). Anything else — "
  "`async`, `stream`, `auto` or unset — queues the run and returns 202; "
  "follow `_links.stream` for progress and the result."
)


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


def _api_mode(value: Any, fallback: BaseOperatorMode) -> OperatorMode:
  """The API mode enum for a worker-reported mode string, or the requested one."""
  try:
    return OperatorMode(value)
  except ValueError:
    return OperatorMode(fallback.value)


def _select_operator_type(
  graph_id: str, current_user: User, db: Session, request: OperatorRequest
) -> str:
  """Pick the operator for a query from the registry's own confidence ranking."""
  config = OrchestratorConfig(
    routing_strategy=RoutingStrategy.BEST_MATCH,
    enable_rag=request.enable_rag,
  )
  orchestrator = OperatorOrchestrator(graph_id, current_user, db, config)
  recommendations = orchestrator.get_operator_recommendations(
    request.message, request.context
  )

  criteria = request.selection_criteria
  if criteria:
    excluded = set(criteria.excluded_operators or [])
    recommendations = [
      r
      for r in recommendations
      if r["operator_type"] not in excluded
      and r.get("confidence", 0) >= criteria.min_confidence
    ]

  if not recommendations:
    raise HTTPException(status_code=404, detail="No suitable operator found for query")
  return recommendations[0]["operator_type"]


def _gate(
  operator: Operator,
  graph_id: str,
  current_user: User,
  db: Session,
  mode: BaseOperatorMode,
) -> None:
  """Refuse now what the worker would refuse later.

  The worker re-checks all three with its own session (a task can wait in
  the queue past a role change or a spent balance), but a caller should get
  the 403 or 402 on the request, not a failed operation to go and read.
  """
  enforce_operator_write_role(operator, graph_id, str(current_user.id))
  enforce_operator_graph_scope(operator, graph_id)
  try:
    enforce_operator_credits(operator, graph_id, str(current_user.id), db, mode)
  except InsufficientOperatorCreditsError as e:
    raise HTTPException(
      status_code=402,
      detail={
        "code": "INSUFFICIENT_CREDITS",
        "message": "Not enough credits to perform AI analysis",
        "required_credits": e.estimated_credits,
        "available_credits": e.available_credits,
      },
    ) from e


async def _wait_for_operation(operation_id: str) -> OperationMetadata | None:
  """Poll the operation until it settles or the sync budget runs out."""
  storage = get_event_storage()
  loop = asyncio.get_running_loop()
  deadline = loop.time() + SYNC_WAIT_SECONDS
  while True:
    metadata = await storage.get_operation_metadata(operation_id)
    if metadata is None or metadata.status in _SETTLED_STATUSES:
      return metadata
    if loop.time() >= deadline:
      return metadata
    await asyncio.sleep(SYNC_POLL_INTERVAL_SECONDS)


def _response_from_operation(
  metadata: OperationMetadata, operator_type: str, mode: BaseOperatorMode
) -> OperatorResponse:
  """The sync 200 body for a settled run.

  A failed or cancelled run answers 200 with `error_details`, the shape the
  console renders; the HTTP status describes the request, which succeeded.
  """
  if metadata.status == OperationStatus.COMPLETED:
    result = metadata.result_data or {}
    return OperatorResponse(
      content=result.get("content", ""),
      operator_used=result.get("operator_used") or operator_type,
      mode_used=_api_mode(result.get("mode_used"), mode),
      metadata=result.get("metadata") or {},
      tokens_used=result.get("tokens_used"),
      confidence_score=result.get("confidence_score"),
      error_details=result.get("error_details"),
      execution_time=result.get("execution_time"),
      operation_id=metadata.operation_id,
      is_partial=False,
    )

  cancelled = metadata.status == OperationStatus.CANCELLED
  message = metadata.error_message or (
    "Operator run was cancelled" if cancelled else "Operator run failed"
  )
  return OperatorResponse(
    content=message,
    operator_used=operator_type,
    mode_used=OperatorMode(mode.value),
    metadata={},
    error_details={
      "code": "OPERATOR_CANCELLED" if cancelled else "OPERATOR_FAILED",
      "message": message,
    },
    operation_id=metadata.operation_id,
    is_partial=False,
  )


async def _dispatch(
  graph_id: str,
  operator_type: str,
  request: OperatorRequest,
  response_mode: ResponseMode | None,
  current_user: User,
  db: Session,
) -> OperatorResponse | JSONResponse:
  """Gate, enqueue, and answer — 202 with links, or 200 under a sync wait."""
  operator = get_operator(operator_type)
  base_mode = (
    BaseOperatorMode.EXTENDED
    if request.force_extended_analysis
    else _convert_operator_mode(request.mode)
  )
  _gate(operator, graph_id, current_user, db, base_mode)

  params = {
    "operator_type": operator_type,
    "query": request.message,
    "mode": base_mode.value,
    "history": [{"role": m.role, "content": m.content} for m in request.history],
    "context": _request_context(request) or {},
    "enable_rag": request.enable_rag,
  }
  queued = await enqueue_task("operator", graph_id, str(current_user.id), params)
  operation_id = queued["operation_id"]
  logger.info(
    f"Queued operator '{operator_type}' ({base_mode.value}) for graph {graph_id} "
    f"as operation {operation_id}"
  )

  if response_mode == ResponseMode.SYNC:
    metadata = await _wait_for_operation(operation_id)
    if metadata is not None and metadata.status in (
      OperationStatus.COMPLETED,
      OperationStatus.FAILED,
      OperationStatus.CANCELLED,
    ):
      return _response_from_operation(metadata, operator_type, base_mode)
    # Still queued, running, or paused for input: hand back the links.

  return JSONResponse(status_code=202, content=queued)


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
    "The run executes on the background worker: the default answer is 202 "
    "with the operation's `_links` (stream, status, cancel); `?mode=sync` "
    "waits up to 50s and answers 200 with the result."
  ),
  operation_id="autoSelectOperator",
  responses={
    **RESOURCE_ERROR_RESPONSES,
    202: {"description": "Run queued on the worker — follow `_links.stream`"},
    402: {"description": "Insufficient credits"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/operator", business_event_type="agent_query_auto"
)
async def auto_operator(
  request: OperatorRequest,
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  mode: ResponseMode | None = Query(None, description=_MODE_DESCRIPTION),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> OperatorResponse | JSONResponse:
  _check_operator_post_enabled()
  await _enforce_shared_repository_agent_limits(graph_id, current_user, db)

  try:
    operator_type = _select_operator_type(graph_id, current_user, db, request)
    return await _dispatch(graph_id, operator_type, request, mode, current_user, db)
  except HTTPException:
    raise
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
    "The run executes on the background worker: the default answer is 202 "
    "with the operation's `_links` (stream, status, cancel); `?mode=sync` "
    "waits up to 50s and answers 200 with the result."
  ),
  operation_id="executeSpecificOperator",
  responses={
    **RESOURCE_ERROR_RESPONSES,
    202: {"description": "Run queued on the worker — follow `_links.stream`"},
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
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  mode: ResponseMode | None = Query(None, description=_MODE_DESCRIPTION),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> OperatorResponse | JSONResponse:
  _check_operator_post_enabled()
  await _enforce_shared_repository_agent_limits(graph_id, current_user, db)

  try:
    try:
      get_operator(operator_type)
    except KeyError:
      raise HTTPException(
        status_code=404, detail=f"Operator type '{operator_type}' not found"
      )
    return await _dispatch(graph_id, operator_type, request, mode, current_user, db)
  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Operator execution error: {e!s}", exc_info=True)
    raise HTTPException(
      status_code=500,
      detail="An internal error occurred while executing the operator.",
    )
