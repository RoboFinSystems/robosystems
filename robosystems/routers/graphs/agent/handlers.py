"""
Agent execution handlers for different strategies.

Provides handler functions for sync, SSE, and Celery execution paths.
"""

from typing import Dict, Any
from fastapi.responses import JSONResponse
from sse_starlette.sse import EventSourceResponse
from sqlalchemy.orm import Session

from robosystems.models.iam import User
from robosystems.models.api.graphs.agent import AgentMode, AgentResponse
from robosystems.operations.agents.orchestrator import (
  AgentOrchestrator,
  OrchestratorConfig,
  RoutingStrategy,
  AgentSelectionCriteria,
)
from robosystems.operations.agents.base import AgentMode as BaseAgentMode
from robosystems.middleware.sse.operation_manager import create_operation_response
from robosystems.logger import logger

from .streaming import stream_agent_execution


async def handle_sync_execution(
  graph_id: str,
  request_data: Dict[str, Any],
  base_mode: BaseAgentMode,
  current_user: User,
  db: Session,
  selection_criteria: AgentSelectionCriteria = None,
  agent_type: str = None,
) -> AgentResponse:
  """
  Handle synchronous agent execution.

  Args:
      graph_id: Graph database identifier
      request_data: Agent request data
      base_mode: Agent execution mode
      current_user: Authenticated user
      db: Database session
      selection_criteria: Optional agent selection criteria
      agent_type: Optional specific agent type

  Returns:
      AgentResponse with result
  """
  config = OrchestratorConfig(
    routing_strategy=RoutingStrategy.BEST_MATCH,
    enable_rag=request_data.get("enable_rag", False),
  )
  orchestrator = AgentOrchestrator(graph_id, current_user, db, config)

  agent_response = await orchestrator.route_query(
    query=request_data["message"],
    agent_type=agent_type,
    mode=base_mode,
    history=request_data.get("history", []),
    context=request_data.get("context"),
    selection_criteria=selection_criteria,
    force_extended=request_data.get("force_extended_analysis", False),
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


async def handle_sse_streaming(
  graph_id: str,
  request_data: Dict[str, Any],
  current_user: User,
  db: Session,
  agent_type: str = None,
) -> EventSourceResponse:
  """
  Handle SSE streaming execution.

  Args:
      graph_id: Graph database identifier
      request_data: Agent request data
      current_user: Authenticated user
      db: Database session
      agent_type: Optional specific agent type (if None, will auto-select)

  Returns:
      EventSourceResponse with SSE stream
  """
  # If no agent_type specified, select one
  if not agent_type:
    config = OrchestratorConfig(
      routing_strategy=RoutingStrategy.BEST_MATCH,
      enable_rag=request_data.get("enable_rag", False),
    )
    orchestrator = AgentOrchestrator(graph_id, current_user, db, config)

    # Get agent recommendations to select best agent
    recommendations = orchestrator.get_agent_recommendations(
      request_data["message"], request_data.get("context")
    )
    if not recommendations:
      from fastapi import HTTPException

      raise HTTPException(status_code=500, detail="No suitable agent found for query")

    agent_type = recommendations[0]["agent_type"]

  # Stream execution with selected agent
  return await stream_agent_execution(
    agent_type=agent_type,
    graph_id=graph_id,
    request_data={
      "message": request_data["message"],
      "mode": request_data.get("mode", "standard"),
      "history": request_data.get("history", []),
      "context": request_data.get("context"),
    },
    current_user=current_user,
    db_session=db,
  )


async def handle_celery_queue(
  graph_id: str,
  request_data: Dict[str, Any],
  current_user: User,
  db: Session,
  agent_type: str = None,
) -> JSONResponse:
  """
  Handle async Celery execution.

  Args:
      graph_id: Graph database identifier
      request_data: Agent request data
      current_user: Authenticated user
      db: Database session
      agent_type: Optional specific agent type (if None, will auto-select)

  Returns:
      JSONResponse with 202 status and operation details
  """
  # If no agent_type specified, select one
  if not agent_type:
    config = OrchestratorConfig(
      routing_strategy=RoutingStrategy.BEST_MATCH,
      enable_rag=request_data.get("enable_rag", False),
    )
    orchestrator = AgentOrchestrator(graph_id, current_user, db, config)

    # Get agent recommendations to select best agent
    recommendations = orchestrator.get_agent_recommendations(
      request_data["message"], request_data.get("context")
    )
    if not recommendations:
      from fastapi import HTTPException

      raise HTTPException(status_code=500, detail="No suitable agent found for query")

    agent_type = recommendations[0]["agent_type"]

  # Create SSE operation
  sse_response = await create_operation_response(
    operation_type="agent_analysis",
    user_id=current_user.id,
    graph_id=graph_id,
  )

  # Queue task
  from robosystems.tasks.agents.analyze import analyze_agent_task

  task = analyze_agent_task.delay(
    agent_type=agent_type,
    graph_id=graph_id,
    request_data={
      "message": request_data["message"],
      "mode": request_data.get("mode", "standard"),
      "history": request_data.get("history", []),
      "context": request_data.get("context"),
    },
    operation_id=sse_response["operation_id"],
    user_id=str(current_user.id),
  )

  logger.info(
    f"Queued agent analysis task {task.id} for operation {sse_response['operation_id']}"
  )

  return JSONResponse(status_code=202, content=sse_response)
