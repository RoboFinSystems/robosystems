"""Operator execution handlers for different strategies.

Provides handler functions for sync, SSE, and background queue execution paths.
"""

from typing import Any

from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sse_starlette.sse import EventSourceResponse

from robosystems.logger import logger
from robosystems.middleware.sse.operation_manager import create_operation_response
from robosystems.models.api.graphs.operator import OperatorMode, OperatorResponse
from robosystems.models.core import User
from robosystems.operations.operators.base import OperatorMode as BaseOperatorMode
from robosystems.operations.operators.orchestrator import (
  OperatorOrchestrator,
  OperatorSelectionCriteria,
  OrchestratorConfig,
  RoutingStrategy,
)

from .streaming import stream_operator_execution


async def handle_sync_execution(
  graph_id: str,
  request_data: dict[str, Any],
  base_mode: BaseOperatorMode,
  current_user: User,
  db: Session,
  selection_criteria: OperatorSelectionCriteria = None,
  operator_type: str | None = None,
) -> OperatorResponse:
  """Handle synchronous operator execution."""
  config = OrchestratorConfig(
    routing_strategy=RoutingStrategy.BEST_MATCH,
    enable_rag=request_data.get("enable_rag", False),
  )
  orchestrator = OperatorOrchestrator(graph_id, current_user, db, config)

  operator_response = await orchestrator.route_query(
    query=request_data["message"],
    operator_type=operator_type,
    mode=base_mode,
    history=request_data.get("history", []),
    context=request_data.get("context"),
    selection_criteria=selection_criteria,
    force_extended=request_data.get("force_extended_analysis", False),
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


async def handle_sse_streaming(
  graph_id: str,
  request_data: dict[str, Any],
  current_user: User,
  db: Session,
  operator_type: str | None = None,
) -> EventSourceResponse:
  """Handle SSE streaming execution."""
  if not operator_type:
    config = OrchestratorConfig(
      routing_strategy=RoutingStrategy.BEST_MATCH,
      enable_rag=request_data.get("enable_rag", False),
    )
    orchestrator = OperatorOrchestrator(graph_id, current_user, db, config)
    recommendations = orchestrator.get_operator_recommendations(
      request_data["message"], request_data.get("context")
    )
    if not recommendations:
      from fastapi import HTTPException

      raise HTTPException(
        status_code=500, detail="No suitable operator found for query"
      )
    operator_type = recommendations[0]["operator_type"]

  return await stream_operator_execution(
    operator_type=operator_type,
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
  operator_type: str | None = None,
) -> JSONResponse:
  """Handle async background execution."""
  if not operator_type:
    config = OrchestratorConfig(
      routing_strategy=RoutingStrategy.BEST_MATCH,
      enable_rag=request_data.get("enable_rag", False),
    )
    orchestrator = OperatorOrchestrator(graph_id, current_user, db, config)
    recommendations = orchestrator.get_operator_recommendations(
      request_data["message"], request_data.get("context")
    )
    if not recommendations:
      from fastapi import HTTPException

      raise HTTPException(
        status_code=500, detail="No suitable operator found for query"
      )
    operator_type = recommendations[0]["operator_type"]

  sse_response = await create_operation_response(
    operation_type="operator_analysis",
    user_id=current_user.id,
    graph_id=graph_id,
  )
  operation_id = sse_response["operation_id"]

  background_tasks.add_task(
    _run_operator_analysis_background,
    operator_type=operator_type,
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

  logger.info(f"Queued background operator analysis for operation {operation_id}")
  return JSONResponse(status_code=202, content=sse_response)


async def _run_operator_analysis_background(
  operator_type: str,
  graph_id: str,
  request_data: dict[str, Any],
  operation_id: str,
  user_id: str,
) -> dict[str, Any]:
  """Run operator analysis in background with SSE progress updates."""
  import time

  from robosystems.database import get_db_session
  from robosystems.middleware.sse.event_storage import OperationStatus
  from robosystems.middleware.sse.operation_manager import emit_sse_event
  from robosystems.operations.operators.adapters.api import run_operator_api
  from robosystems.operations.operators.operator_registry import get_operator

  start_time = time.time()

  try:
    emit_sse_event(
      operation_id=operation_id,
      status=OperationStatus.RUNNING,
      data={"operator_type": operator_type, "graph_id": graph_id},
      message=f"Starting {operator_type} operator analysis",
      progress_percentage=0,
    )

    db = next(get_db_session())
    try:
      from robosystems.models.core import User

      user = db.query(User).filter(User.id == user_id).first()
      if not user:
        raise ValueError(f"User {user_id} not found")

      operator = get_operator(operator_type)

      emit_sse_event(
        operation_id=operation_id,
        status=OperationStatus.RUNNING,
        data={"operator_name": operator.spec.name},
        message="Operator initialized, starting analysis",
        progress_percentage=10,
      )

      mode_str = request_data.get("mode", "standard")
      mode_map = {
        "quick": BaseOperatorMode.QUICK,
        "standard": BaseOperatorMode.STANDARD,
        "extended": BaseOperatorMode.EXTENDED,
        "streaming": BaseOperatorMode.STREAMING,
      }
      mode = mode_map.get(mode_str.lower(), BaseOperatorMode.STANDARD)

      def progress_callback(stage: str, percentage: int, message: str):
        emit_sse_event(
          operation_id=operation_id,
          status=OperationStatus.RUNNING,
          data={"stage": stage, "message": message},
          message=message,
          progress_percentage=min(10 + int(percentage * 0.8), 90),
        )

      result = await run_operator_api(
        operator=operator,
        graph_id=graph_id,
        user=user,
        query=request_data["message"],
        mode=mode,
        db_session=db,
        history=request_data.get("history", []),
        context=request_data.get("context"),
        callback=progress_callback,
      )

      execution_time = time.time() - start_time

      result_data = {
        "content": result.content,
        "operator_used": operator.spec.name,
        "mode_used": mode.value,
        "metadata": result.metadata,
        "tokens_used": result.metadata.get("tokens_used"),
        "confidence_score": result.confidence_score,
        "execution_time": execution_time,
        "tools_called": result.tools_called,
      }

      emit_sse_event(
        operation_id=operation_id,
        status=OperationStatus.COMPLETED,
        data=result_data,
        message="Operator analysis completed successfully",
        progress_percentage=100,
      )

      logger.info(
        f"Operator analysis completed: {operator_type} for graph {graph_id} in {execution_time:.2f}s"
      )
      return result_data

    finally:
      db.close()

  except Exception as e:
    logger.error(f"Background operator analysis failed: {e!s}", exc_info=True)
    emit_sse_event(
      operation_id=operation_id,
      status=OperationStatus.FAILED,
      data={"error": str(e), "error_type": type(e).__name__},
      message=f"Operator analysis failed: {e!s}",
    )
    raise
