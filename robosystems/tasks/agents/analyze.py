"""
Celery task for long-running agent analysis operations.

This task handles agent operations that exceed API timeout thresholds (>30s),
providing SSE progress updates and proper error handling.
"""

import time
from typing import Dict, Any
from celery import Task

from robosystems.logger import logger
from robosystems.middleware.sse.operation_manager import emit_sse_event
from robosystems.middleware.sse.event_storage import OperationStatus


class AgentAnalysisTask(Task):
  """Base task for agent analysis with SSE event publishing."""

  def on_failure(self, exc, task_id, args, kwargs, einfo):
    """Handle task failure."""
    operation_id = kwargs.get("operation_id") or (args[3] if len(args) > 3 else None)
    if operation_id:
      emit_sse_event(
        operation_id=operation_id,
        status=OperationStatus.FAILED,
        data={
          "error": str(exc),
          "error_type": type(exc).__name__,
          "task_id": task_id,
        },
        message=f"Agent analysis failed: {str(exc)}",
      )
    logger.error(f"Agent analysis task {task_id} failed: {exc}")


def analyze_agent_task_impl(
  agent_type: str,
  graph_id: str,
  request_data: Dict[str, Any],
  operation_id: str,
  user_id: str,
) -> Dict[str, Any]:
  """
  Execute agent analysis task with SSE progress updates.

  Args:
      agent_type: Type of agent to use
      graph_id: Graph database identifier
      request_data: Agent request data (message, mode, history, context)
      operation_id: SSE operation ID for progress tracking
      user_id: User ID for agent initialization

  Returns:
      Agent response data
  """
  from robosystems.database import get_db_session
  from robosystems.models.iam import User
  from robosystems.operations.agents.registry import AgentRegistry
  from robosystems.operations.agents.base import AgentMode

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
        "quick": AgentMode.QUICK,
        "standard": AgentMode.STANDARD,
        "extended": AgentMode.EXTENDED,
        "streaming": AgentMode.STREAMING,
      }
      mode = mode_map.get(mode_str.lower(), AgentMode.STANDARD)

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

      # Execute agent analysis
      response = None
      import asyncio

      async def run_analysis():
        return await agent.analyze(
          query=request_data["message"],
          mode=mode,
          history=history,
          context=request_data.get("context"),
          callback=progress_callback,
        )

      # Run async analysis in sync context
      try:
        loop = asyncio.get_running_loop()
      except RuntimeError:
        try:
          loop = asyncio.get_event_loop()
          if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        except RuntimeError:
          loop = asyncio.new_event_loop()
          asyncio.set_event_loop(loop)

      created_loop = False
      if loop.is_closed():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        created_loop = True

      try:
        response = loop.run_until_complete(run_analysis())
      finally:
        if created_loop:
          loop.close()

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
    logger.error(f"Agent analysis task failed: {str(e)}", exc_info=True)

    # Emit error event
    emit_sse_event(
      operation_id=operation_id,
      status=OperationStatus.FAILED,
      data={"error": str(e), "error_type": type(e).__name__},
      message=f"Agent analysis failed: {str(e)}",
    )

    raise


# Register task
from robosystems.celery import celery_app  # noqa: E402


def _analyze_agent_wrapper(
  self, agent_type, graph_id, request_data, operation_id, user_id
):
  """Wrapper function for Celery task registration."""
  return analyze_agent_task_impl(
    agent_type, graph_id, request_data, operation_id, user_id
  )


analyze_agent_task: Task = celery_app.task(
  bind=True,
  base=AgentAnalysisTask,
  name="robosystems.tasks.agents.analyze_agent",
  max_retries=2,
  default_retry_delay=60,
)(_analyze_agent_wrapper)  # type: ignore[assignment]
