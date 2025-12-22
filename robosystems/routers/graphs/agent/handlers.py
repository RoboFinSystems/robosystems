"""
Agent execution handlers for different strategies.

Provides handler functions for sync, SSE, and background queue execution paths.
"""

from typing import Any

from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sse_starlette.sse import EventSourceResponse

from robosystems.logger import logger
from robosystems.middleware.sse.operation_manager import create_operation_response
from robosystems.models.api.graphs.agent import AgentMode, AgentResponse
from robosystems.models.iam import User
from robosystems.operations.agents.base import AgentMode as BaseAgentMode
from robosystems.operations.agents.orchestrator import (
  AgentOrchestrator,
  AgentSelectionCriteria,
  OrchestratorConfig,
  RoutingStrategy,
)

from .streaming import stream_agent_execution


async def handle_sync_execution(
  graph_id: str,
  request_data: dict[str, Any],
  base_mode: BaseAgentMode,
  current_user: User,
  db: Session,
  selection_criteria: AgentSelectionCriteria = None,
  agent_type: str | None = None,
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
  request_data: dict[str, Any],
  current_user: User,
  db: Session,
  agent_type: str | None = None,
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


async def handle_background_queue(
  graph_id: str,
  request_data: dict[str, Any],
  current_user: User,
  db: Session,
  background_tasks,
  agent_type: str | None = None,
) -> JSONResponse:
  """
  Handle async background execution.

  Uses FastAPI background tasks to run agent analysis asynchronously
  while providing SSE progress updates.

  Args:
      graph_id: Graph database identifier
      request_data: Agent request data
      current_user: Authenticated user
      db: Database session
      background_tasks: FastAPI BackgroundTasks instance
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

  operation_id = sse_response["operation_id"]

  # Queue background task for agent analysis
  background_tasks.add_task(
    _run_agent_analysis_background,
    agent_type=agent_type,
    graph_id=graph_id,
    request_data={
      "message": request_data["message"],
      "mode": request_data.get("mode", "standard"),
      "history": request_data.get("history", []),
      "context": request_data.get("context"),
    },
    operation_id=operation_id,
    user_id=str(current_user.id),
  )

  logger.info(f"Queued background agent analysis for operation {operation_id}")

  return JSONResponse(status_code=202, content=sse_response)


async def _run_agent_analysis_background(
  agent_type: str,
  graph_id: str,
  request_data: dict[str, Any],
  operation_id: str,
  user_id: str,
) -> dict[str, Any]:
  """
  Run agent analysis in background with SSE progress updates.
  """
  import time

  from robosystems.database import get_db_session
  from robosystems.middleware.sse.event_storage import OperationStatus
  from robosystems.middleware.sse.operation_manager import emit_sse_event
  from robosystems.operations.agents.registry import AgentRegistry

  start_time = time.time()

  try:
    # Emit started event
    emit_sse_event(
      operation_id=operation_id,
      status=OperationStatus.RUNNING,
      data={"agent_type": agent_type, "graph_id": graph_id},
      message=f"Starting {agent_type} agent analysis",
      progress_percentage=0,
    )

    # Get database session
    db = next(get_db_session())
    try:
      # Get user
      from robosystems.models.iam import User

      user = db.query(User).filter(User.id == user_id).first()
      if not user:
        raise ValueError(f"User {user_id} not found")

      # Get agent instance
      registry = AgentRegistry()
      agent = registry.get_agent(
        agent_type=agent_type, graph_id=graph_id, user=user, db_session=db
      )
      if not agent:
        raise ValueError(f"Agent type '{agent_type}' not found")

      # Emit initialization complete
      emit_sse_event(
        operation_id=operation_id,
        status=OperationStatus.RUNNING,
        data={"agent_name": agent.metadata.name},
        message="Agent initialized, starting analysis",
        progress_percentage=10,
      )

      # Convert mode
      mode_str = request_data.get("mode", "standard")
      mode_map = {
        "quick": BaseAgentMode.QUICK,
        "standard": BaseAgentMode.STANDARD,
        "extended": BaseAgentMode.EXTENDED,
        "streaming": BaseAgentMode.STREAMING,
      }
      mode = mode_map.get(mode_str.lower(), BaseAgentMode.STANDARD)

      # Convert history
      history = request_data.get("history", [])

      # Create progress callback
      def progress_callback(stage: str, percentage: int, message: str):
        """Emit progress events during analysis."""
        emit_sse_event(
          operation_id=operation_id,
          status=OperationStatus.RUNNING,
          data={"stage": stage, "message": message},
          message=message,
          progress_percentage=min(10 + int(percentage * 0.8), 90),
        )

      # Execute agent analysis (already async)
      response = await agent.analyze(
        query=request_data["message"],
        mode=mode,
        history=history,
        context=request_data.get("context"),
        callback=progress_callback,
      )

      # Calculate execution time
      execution_time = time.time() - start_time

      # Prepare result
      result = {
        "content": response.content,
        "agent_used": response.agent_name,
        "mode_used": response.mode_used.value,
        "metadata": response.metadata,
        "tokens_used": response.tokens_used,
        "confidence_score": response.confidence_score,
        "error_details": response.error_details,
        "execution_time": execution_time,
        "tools_called": response.tools_called,
      }

      # Emit completion event
      emit_sse_event(
        operation_id=operation_id,
        status=OperationStatus.COMPLETED,
        data=result,
        message="Agent analysis completed successfully",
        progress_percentage=100,
      )

      logger.info(
        f"Agent analysis completed: {agent_type} for graph {graph_id} in {execution_time:.2f}s"
      )

      return result

    finally:
      db.close()

  except Exception as e:
    logger.error(f"Background agent analysis failed: {e!s}", exc_info=True)

    # Emit error event
    emit_sse_event(
      operation_id=operation_id,
      status=OperationStatus.FAILED,
      data={"error": str(e), "error_type": type(e).__name__},
      message=f"Agent analysis failed: {e!s}",
    )

    raise
