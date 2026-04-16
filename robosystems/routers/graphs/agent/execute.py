"""Agent execution endpoints with intelligent strategy selection."""

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
from robosystems.models.api.graphs.agent import (
  AgentListResponse,
  AgentMetadataResponse,
  AgentMode,
  AgentRecommendation,
  AgentRecommendationRequest,
  AgentRecommendationResponse,
  AgentRequest,
  AgentResponse,
  BatchAgentRequest,
  BatchAgentResponse,
)
from robosystems.models.core import User
from robosystems.operations.agents.agent_registry import get_agent, list_agents
from robosystems.operations.agents.base import (
  AgentMode as BaseAgentMode,
)
from robosystems.operations.agents.base import (
  ExecutionProfile,
)
from robosystems.operations.agents.orchestrator import (
  AgentOrchestrator,
  AgentSelectionCriteria,
)

from .handlers import (
  handle_background_queue,
  handle_sse_streaming,
  handle_sync_execution,
)
from .strategies import (
  AgentClientDetector,
  AgentExecutionStrategy,
  AgentStrategySelector,
  ResponseMode,
)

router = APIRouter()


def _check_agent_post_enabled():
  """Check if agent POST endpoints are enabled."""
  if not env.AGENT_POST_ENABLED:
    logger.warning("Agent POST operations blocked by feature flag")
    raise HTTPException(
      status_code=403,
      detail="Agent POST operations are currently disabled. Please contact support if you need assistance.",
    )


def _convert_agent_mode(mode: AgentMode | None) -> BaseAgentMode:
  """Convert API AgentMode to base AgentMode."""
  if mode is None:
    return BaseAgentMode.STANDARD

  mode_mapping = {
    AgentMode.QUICK: BaseAgentMode.QUICK,
    AgentMode.STANDARD: BaseAgentMode.STANDARD,
    AgentMode.EXTENDED: BaseAgentMode.EXTENDED,
    AgentMode.STREAMING: BaseAgentMode.STREAMING,
  }
  return mode_mapping.get(mode, BaseAgentMode.STANDARD)


@router.get(
  "/agent",
  response_model=AgentListResponse,
  summary="List Available Agents",
  description="Filter by capability using the `capability` query param (e.g., `financial_analysis`, `rag_search`).",
  operation_id="listAgents",
  responses={**RESOURCE_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/agent", business_event_type="agent_list"
)
async def list_agents_endpoint(
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
) -> AgentListResponse:
  agents = list_agents()

  if capability:
    agents = {
      k: v for k, v in agents.items() if capability in v.get("capabilities", [])
    }

  return AgentListResponse(agents=agents, total=len(agents))


@router.post(
  "/agent",
  response_model=AgentResponse,
  summary="Auto-select Agent for Query",
  description="Routes to the best agent for your query. Agents: `financial` (SEC, accounting), `research` (deep analysis), `rag` (knowledge base, free). Credit cost by mode: `quick` 5-10, `standard` 15-25, `extended` 30-75. Execution strategy (sync/SSE/async) auto-selected; override with `?mode=sync|async`.",
  operation_id="autoSelectAgent",
  responses={
    **RESOURCE_ERROR_RESPONSES,
    202: {"description": "Query queued for async processing"},
    402: {"description": "Insufficient credits"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/agent", business_event_type="agent_query_auto"
)
async def auto_agent(
  request: AgentRequest,
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
) -> AgentResponse | JSONResponse | EventSourceResponse:
  _check_agent_post_enabled()

  try:
    # Detect client capabilities
    headers = dict(full_request.headers)
    client_info = AgentClientDetector.detect_client_type(headers)

    # Use conservative execution profile for auto-routing
    base_mode = _convert_agent_mode(request.mode)
    conservative_profile = ExecutionProfile(
      min_time=5 if base_mode == BaseAgentMode.QUICK else 8,
      max_time=15 if base_mode == BaseAgentMode.QUICK else 25,
      avg_time=10 if base_mode == BaseAgentMode.QUICK else 15,
      tool_calls=5,
    )

    # Select execution strategy
    strategy, strategy_metadata = AgentStrategySelector.select_strategy(
      execution_profile=conservative_profile,
      client_info=client_info,
      mode_override=mode,
      force_extended=request.force_extended_analysis,
    )

    logger.info(
      f"Auto-agent strategy selected: {strategy.value} - {strategy_metadata['selection_reason']}"
    )

    # Convert selection criteria if provided
    selection_criteria = None
    if request.selection_criteria:
      from robosystems.operations.agents.base import AgentCapability

      selection_criteria = AgentSelectionCriteria(
        min_confidence=request.selection_criteria.min_confidence,
        required_capabilities=[
          AgentCapability(cap)
          for cap in request.selection_criteria.required_capabilities
        ],
        preferred_mode=_convert_agent_mode(request.selection_criteria.preferred_mode),
        max_response_time=request.selection_criteria.max_response_time,
        excluded_agents=request.selection_criteria.excluded_agents,
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
    if strategy == AgentExecutionStrategy.SYNC_IMMEDIATE:
      return await handle_sync_execution(
        graph_id=graph_id,
        request_data=request_data,
        base_mode=base_mode,
        current_user=current_user,
        db=db,
        selection_criteria=selection_criteria,
      )

    elif strategy == AgentExecutionStrategy.SSE_STREAMING:
      return await handle_sse_streaming(
        graph_id=graph_id,
        request_data=request_data,
        current_user=current_user,
        db=db,
      )

    elif strategy == AgentExecutionStrategy.BACKGROUND_QUEUE:
      return await handle_background_queue(
        graph_id=graph_id,
        request_data=request_data,
        current_user=current_user,
        db=db,
        background_tasks=background_tasks,
      )

  except Exception as e:
    logger.error(f"Agent routing error: {e!s}", exc_info=True)
    raise HTTPException(
      status_code=500,
      detail="An internal error occurred while routing the agent request.",
    )


@router.get(
  "/agent/{agent_type}",
  response_model=AgentMetadataResponse,
  summary="Get Agent Metadata",
  operation_id="getAgentMetadata",
  responses={**RESOURCE_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/agent/{agent_type}",
  business_event_type="agent_metadata",
)
async def get_agent_metadata(
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  agent_type: str = Path(
    ...,
    description="Agent type identifier (e.g., 'financial', 'research', 'rag')",
    pattern="^[a-zA-Z][a-zA-Z0-9_]{2,32}$",
  ),
  _current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> AgentMetadataResponse:
  agents = list_agents()
  metadata = agents.get(agent_type)

  if not metadata:
    raise HTTPException(status_code=404, detail=f"Agent '{agent_type}' not found")

  return AgentMetadataResponse(**metadata)


@router.post(
  "/agent/{agent_type}",
  response_model=AgentResponse,
  summary="Execute Specific Agent",
  description="Available: `financial` (SEC filings, accounting), `research` (deep analysis), `rag` (retrieval, no credits). Execution strategy auto-selected; override with `?mode=sync|async`.",
  operation_id="executeSpecificAgent",
  responses={
    **RESOURCE_ERROR_RESPONSES,
    202: {"description": "Query queued for async processing"},
    402: {"description": "Insufficient credits"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/agent/{agent_type}", business_event_type="agent_query_specific"
)
async def specific_agent(
  agent_type: str,
  request: AgentRequest,
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
) -> AgentResponse | JSONResponse | EventSourceResponse:
  _check_agent_post_enabled()

  try:
    # Get agent to access execution profile (agents are lightweight, no graph/user needed)
    try:
      agent = get_agent(agent_type)
    except KeyError:
      raise HTTPException(
        status_code=404, detail=f"Agent type '{agent_type}' not found"
      )

    # Get execution profile for requested mode
    base_mode = _convert_agent_mode(request.mode)
    execution_profile = agent.spec.execution_profile.get(base_mode)

    # Detect client capabilities
    headers = dict(full_request.headers)
    client_info = AgentClientDetector.detect_client_type(headers)

    # Select execution strategy
    strategy, strategy_metadata = AgentStrategySelector.select_strategy(
      execution_profile=execution_profile,
      client_info=client_info,
      mode_override=mode,
      force_extended=request.force_extended_analysis,
    )

    logger.info(
      f"Agent '{agent_type}' strategy selected: {strategy.value} - {strategy_metadata['selection_reason']}"
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
    if strategy == AgentExecutionStrategy.SYNC_IMMEDIATE:
      return await handle_sync_execution(
        graph_id=graph_id,
        request_data=request_data,
        base_mode=base_mode,
        current_user=current_user,
        db=db,
        agent_type=agent_type,
      )

    elif strategy == AgentExecutionStrategy.SSE_STREAMING:
      return await handle_sse_streaming(
        graph_id=graph_id,
        request_data=request_data,
        current_user=current_user,
        db=db,
        agent_type=agent_type,
      )

    elif strategy == AgentExecutionStrategy.BACKGROUND_QUEUE:
      return await handle_background_queue(
        graph_id=graph_id,
        request_data=request_data,
        current_user=current_user,
        db=db,
        background_tasks=background_tasks,
        agent_type=agent_type,
      )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Agent execution error: {e!s}", exc_info=True)
    raise HTTPException(
      status_code=500,
      detail="An internal error occurred while executing the agent.",
    )


@router.post(
  "/agent/batch",
  response_model=BatchAgentResponse,
  summary="Batch Process Queries",
  description="Process up to 10 queries sequentially or in parallel. Partial failure is supported — each result has individual error handling.",
  operation_id="batchProcessQueries",
  responses={
    **RESOURCE_ERROR_RESPONSES,
    402: {"description": "Insufficient credits"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/agent/batch", business_event_type="agent_batch_query"
)
async def batch_agent(
  request: BatchAgentRequest,
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> BatchAgentResponse:
  _check_agent_post_enabled()

  import time

  start_time = time.time()
  orchestrator = AgentOrchestrator(graph_id, current_user, db)

  async def process_single(query_request: AgentRequest) -> AgentResponse:
    """Process a single query."""
    history = [
      {"role": msg.role, "content": msg.content} for msg in query_request.history
    ]

    agent_response = await orchestrator.route_query(
      query=query_request.message,
      agent_type=query_request.agent_type,
      mode=_convert_agent_mode(query_request.mode),
      history=history,
      context=query_request.context,
    )

    return AgentResponse(
      content=agent_response.content,
      agent_used=agent_response.agent_name,
      mode_used=AgentMode(agent_response.mode_used.value),
      metadata=agent_response.metadata,
      tokens_used=agent_response.tokens_used,
      confidence_score=agent_response.confidence_score,
      error_details=agent_response.error_details,
      execution_time=agent_response.execution_time,
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
          AgentResponse(
            content=f"Query failed: {r!s}",
            agent_used="error",
            mode_used=AgentMode.STANDARD,
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
          AgentResponse(
            content=f"Query failed: {e!s}",
            agent_used="error",
            mode_used=AgentMode.STANDARD,
            error_details={"error": str(e)},
            metadata=None,
            tokens_used=None,
            confidence_score=None,
            operation_id=None,
            is_partial=False,
            execution_time=None,
          )
        )

  return BatchAgentResponse(
    results=results,
    total_execution_time=time.time() - start_time,
    parallel_processed=request.parallel,
  )


@router.post(
  "/agent/recommend",
  response_model=AgentRecommendationResponse,
  summary="Get Agent Recommendations",
  description="Returns agents ranked by confidence score for a query, with explanations. Use before execution when unsure which agent to pick.",
  operation_id="recommendAgent",
  responses={**RESOURCE_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/agent/recommend", business_event_type="agent_recommend"
)
async def recommend_agent(
  request: AgentRecommendationRequest,
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> AgentRecommendationResponse:
  _check_agent_post_enabled()

  orchestrator = AgentOrchestrator(graph_id, current_user, db)
  recommendations_raw = orchestrator.get_agent_recommendations(
    request.query, request.context
  )

  # Convert to response format
  recommendations = [
    AgentRecommendation(
      agent_type=r["agent_type"],
      agent_name=r["agent_name"],
      confidence=r["confidence"],
      capabilities=r["capabilities"],
      reason=r.get("reason"),
    )
    for r in recommendations_raw
  ]

  return AgentRecommendationResponse(
    recommendations=recommendations, query=request.query
  )
