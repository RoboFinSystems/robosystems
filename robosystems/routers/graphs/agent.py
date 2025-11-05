"""
Agent router - Multiagent system for intelligent analysis.

Provides endpoints for agent-based analysis with dynamic routing,
RAG enrichment, and multi-modal execution.
"""

import asyncio
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy.orm import Session

from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
)
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.database import get_db_session
from robosystems.models.iam import User
from robosystems.models.api.graphs.agent import (
  AgentRequest,
  AgentResponse,
  AgentListResponse,
  AgentMetadataResponse,
  AgentRecommendationRequest,
  AgentRecommendationResponse,
  AgentRecommendation,
  BatchAgentRequest,
  BatchAgentResponse,
  AgentMode,
)
from robosystems.operations.agents.orchestrator import (
  AgentOrchestrator,
  OrchestratorConfig,
  RoutingStrategy,
  AgentSelectionCriteria,
)
from robosystems.operations.agents.registry import AgentRegistry
from robosystems.operations.agents.base import AgentMode as BaseAgentMode
from robosystems.logger import logger
from robosystems.models.api.common import ErrorResponse
from robosystems.config import env


router = APIRouter(tags=["Agent"])


def _check_agent_post_enabled():
  """Check if agent POST endpoints are enabled."""
  if not env.AGENT_POST_ENABLED:
    logger.warning("Agent POST operations blocked by feature flag")
    raise HTTPException(
      status_code=403,
      detail="Agent POST operations are currently disabled. Please contact support if you need assistance.",
    )


def _convert_agent_mode(mode: Optional[AgentMode]) -> BaseAgentMode:
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


@router.post(
  "",
  response_model=AgentResponse,
  summary="Auto-select agent for query",
  description="""Automatically select the best agent for your query.

**Agent Selection Process:**

The orchestrator intelligently routes your query by:
1. Analyzing query intent and complexity
2. Enriching context with RAG if enabled
3. Evaluating all available agents against selection criteria
4. Selecting the best match based on confidence scores
5. Executing the query with the selected agent

**Available Agent Types:**
- `financial`: Financial analysis, SEC filings, company metrics
- `research`: General research, data exploration, trend analysis
- `rag`: Knowledge base search using RAG enrichment

**Execution Modes:**
- `quick`: Fast responses (~2-5s), suitable for simple queries
- `standard`: Balanced approach (~5-15s), default mode
- `extended`: Comprehensive analysis (~15-60s), deep research
- `streaming`: Real-time response streaming

**Confidence Score Interpretation:**
- `0.9-1.0`: High confidence, agent is ideal match
- `0.7-0.9`: Good confidence, agent is suitable
- `0.5-0.7`: Moderate confidence, agent can handle but may not be optimal
- `0.3-0.5`: Low confidence, fallback agent used
- `<0.3`: Very low confidence, consider using specific agent endpoint

**Credit Costs:**
- Quick mode: 5-10 credits per query
- Standard mode: 15-25 credits per query
- Extended mode: 30-75 credits per query
- RAG enrichment: +5-15 credits (if enabled)

**Use Cases:**
- Ask questions without specifying agent type
- Get intelligent routing for complex multi-domain queries
- Leverage conversation history for contextual understanding
- Enable RAG for knowledge base enrichment

See request/response examples in the "Examples" dropdown below.""",
  operation_id="autoSelectAgent",
  status_code=200,
  responses={
    200: {"description": "Query successfully processed by selected agent"},
    400: {"description": "Invalid request parameters"},
    402: {"description": "Insufficient credits for selected agent"},
    429: {"description": "Rate limit exceeded"},
    500: {"description": "Internal server error", "model": ErrorResponse},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/agent", business_event_type="agent_query_auto"
)
async def auto_agent(
  request: AgentRequest,
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern="^[a-zA-Z][a-zA-Z0-9_]{2,62}$",
  ),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> AgentResponse:
  """Automatically select the best agent for the query."""
  # Check if agent POST endpoints are enabled
  _check_agent_post_enabled()

  try:
    # Create orchestrator
    config = OrchestratorConfig(
      routing_strategy=RoutingStrategy.BEST_MATCH,
      enable_rag=request.enable_rag,
    )
    orchestrator = AgentOrchestrator(graph_id, current_user, db, config)

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

    # Route query
    agent_response = await orchestrator.route_query(
      query=request.message,
      mode=_convert_agent_mode(request.mode),
      history=history,
      context=request.context,
      selection_criteria=selection_criteria,
      force_extended=request.force_extended_analysis,
    )

    # Convert to API response
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

  except Exception as e:
    logger.error(f"Agent routing error: {str(e)}")
    raise HTTPException(status_code=500, detail=str(e))


@router.post(
  "/{agent_type}",
  response_model=AgentResponse,
  summary="Execute specific agent",
  description="""Execute a specific agent type directly.

Available agents:
- **financial**: Financial analysis, SEC filings, accounting data
- **research**: Deep research and comprehensive analysis
- **rag**: Fast retrieval without AI (no credits required)

Use this endpoint when you know which agent you want to use.""",
  operation_id="executeSpecificAgent",
  status_code=200,
  responses={
    200: {"description": "Query successfully processed by specified agent"},
    400: {"description": "Invalid agent type or request parameters"},
    402: {"description": "Insufficient credits for specified agent"},
    404: {"description": "Agent type not found"},
    429: {"description": "Rate limit exceeded"},
    500: {"description": "Internal server error", "model": ErrorResponse},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/agent/{agent_type}", business_event_type="agent_query_specific"
)
async def specific_agent(
  agent_type: str,
  request: AgentRequest,
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern="^[a-zA-Z][a-zA-Z0-9_]{2,62}$",
  ),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> AgentResponse:
  """Execute a specific agent type."""
  # Check if agent POST endpoints are enabled
  _check_agent_post_enabled()

  try:
    # Create orchestrator
    config = OrchestratorConfig(enable_rag=request.enable_rag)
    orchestrator = AgentOrchestrator(graph_id, current_user, db, config)

    # Convert history
    history = [{"role": msg.role, "content": msg.content} for msg in request.history]

    # Route to specific agent
    agent_response = await orchestrator.route_query(
      query=request.message,
      agent_type=agent_type,
      mode=_convert_agent_mode(request.mode),
      history=history,
      context=request.context,
      force_extended=request.force_extended_analysis,
    )

    # Convert to API response
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

  except Exception as e:
    logger.error(f"Agent execution error: {str(e)}")
    raise HTTPException(status_code=500, detail=str(e))


@router.post(
  "/batch",
  response_model=BatchAgentResponse,
  summary="Batch process multiple queries",
  description="""Process multiple queries either sequentially or in parallel.

**Features:**
- Process up to 10 queries in a single request
- Sequential or parallel execution modes
- Automatic error handling per query
- Credit checking before execution

**Use Cases:**
- Bulk analysis of multiple entities
- Comparative analysis across queries
- Automated report generation

Returns individual results for each query with execution metrics.""",
  operation_id="batchProcessQueries",
  responses={
    200: {"description": "Batch processing completed successfully"},
    400: {"description": "Invalid batch request or too many queries"},
    402: {"description": "Insufficient credits for batch processing"},
    500: {"description": "Internal server error during batch processing"},
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
    pattern="^[a-zA-Z][a-zA-Z0-9_]{2,62}$",
  ),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> BatchAgentResponse:
  """Process multiple queries in batch."""
  # Check if agent POST endpoints are enabled
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
        logger.error(f"Batch query failed: {str(r)}")
        # Add error response
        valid_results.append(
          AgentResponse(
            content=f"Query failed: {str(r)}",
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
        logger.error(f"Batch query failed: {str(e)}")
        results.append(
          AgentResponse(
            content=f"Query failed: {str(e)}",
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


@router.get(
  "/list",
  response_model=AgentListResponse,
  summary="List available agents",
  description="""Get a comprehensive list of all available agents with their metadata.

**Returns:**
- Agent types and names
- Capabilities and supported modes
- Version information
- Credit requirements

Use the optional `capability` filter to find agents with specific capabilities.""",
  operation_id="listAgents",
  responses={
    200: {"description": "List of agents retrieved successfully"},
    401: {"description": "Unauthorized - Invalid or missing authentication"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/agent/list", business_event_type="agent_list"
)
async def list_agents(
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern="^[a-zA-Z][a-zA-Z0-9_]{2,62}$",
  ),
  capability: Optional[str] = Query(
    None,
    description="Filter by capability (e.g., 'financial_analysis', 'rag_search')",
  ),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> AgentListResponse:
  """List all available agents."""
  registry = AgentRegistry()
  agents = registry.list_agents()

  # Filter by capability if requested
  if capability:
    agents = {
      k: v for k, v in agents.items() if capability in v.get("capabilities", [])
    }

  return AgentListResponse(agents=agents, total=len(agents))


@router.get(
  "/{agent_type}/metadata",
  response_model=AgentMetadataResponse,
  summary="Get agent metadata",
  description="""Get comprehensive metadata for a specific agent type.

**Returns:**
- Agent name and description
- Version information
- Supported capabilities and modes
- Credit requirements
- Author and tags
- Configuration options

Use this to understand agent capabilities before execution.""",
  operation_id="getAgentMetadata",
  responses={
    200: {"description": "Agent metadata retrieved successfully"},
    404: {"description": "Agent type not found"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/agent/{agent_type}/metadata",
  business_event_type="agent_metadata",
)
async def get_agent_metadata(
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern="^[a-zA-Z][a-zA-Z0-9_]{2,62}$",
  ),
  agent_type: str = Path(
    ...,
    description="Agent type identifier (e.g., 'financial', 'research', 'rag')",
    pattern="^[a-zA-Z][a-zA-Z0-9_]{2,32}$",
  ),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> AgentMetadataResponse:
  """Get metadata for a specific agent."""
  registry = AgentRegistry()
  metadata = registry.get_agent_metadata(agent_type)

  if not metadata:
    raise HTTPException(status_code=404, detail=f"Agent '{agent_type}' not found")

  return AgentMetadataResponse(**metadata)


@router.post(
  "/recommend",
  response_model=AgentRecommendationResponse,
  summary="Get agent recommendations",
  description="""Get intelligent agent recommendations for a specific query.

**How it works:**
1. Analyzes query content and structure
2. Evaluates agent capabilities
3. Calculates confidence scores
4. Returns ranked recommendations

**Use this when:**
- Unsure which agent to use
- Need to understand agent suitability
- Want confidence scores for decision making

Returns top agents ranked by confidence with explanations.""",
  operation_id="recommendAgent",
  responses={
    200: {"description": "Recommendations generated successfully"},
    400: {"description": "Invalid recommendation request"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/agent/recommend", business_event_type="agent_recommend"
)
async def recommend_agent(
  request: AgentRecommendationRequest,
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern="^[a-zA-Z][a-zA-Z0-9_]{2,62}$",
  ),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> AgentRecommendationResponse:
  """Get agent recommendations for a query."""
  # Check if agent POST endpoints are enabled
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
