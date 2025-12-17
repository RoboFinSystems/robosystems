"""
SSE streaming support for API-based agent operations.

Provides real-time progress updates for medium-duration agent operations
(5-30s) that run on the API but benefit from progress feedback.
"""

import asyncio
import json
from typing import Any, Dict
from datetime import datetime, timezone

from sse_starlette.sse import EventSourceResponse

from robosystems.operations.agents.base import AgentMode
from robosystems.operations.agents.registry import AgentRegistry
from robosystems.models.iam import User
from robosystems.logger import logger


async def stream_agent_execution(
  agent_type: str,
  graph_id: str,
  request_data: Dict[str, Any],
  current_user: User,
  db_session: Any,
) -> EventSourceResponse:
  """
  Stream agent execution with SSE progress updates.

  This function runs the agent on the API and provides
  real-time progress updates via SSE for operations expected to take 5-30s.

  Args:
      agent_type: Type of agent to execute
      graph_id: Graph database identifier
      request_data: Agent request data
      current_user: Authenticated user
      db_session: Database session

  Returns:
      EventSourceResponse with SSE stream
  """

  async def event_generator():
    """Generate SSE events during agent execution."""
    try:
      # Send initialization event
      yield {
        "event": "agent_started",
        "data": json.dumps(
          {
            "agent_type": agent_type,
            "graph_id": graph_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
          }
        ),
      }

      # Get agent instance
      registry = AgentRegistry()
      agent = registry.get_agent(
        agent_type=agent_type,
        graph_id=graph_id,
        user=current_user,
        db_session=db_session,
      )
      if not agent:
        yield {
          "event": "error",
          "data": json.dumps({"error": f"Agent type '{agent_type}' not found"}),
        }
        return

      yield {
        "event": "agent_initialized",
        "data": json.dumps(
          {
            "agent_name": agent.metadata.name,
            "description": agent.metadata.description,
          }
        ),
      }

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
      progress_events = []

      def progress_callback(stage: str, percentage: int, message: str):
        """Collect progress events."""
        progress_events.append(
          {
            "stage": stage,
            "percentage": percentage,
            "message": message,
            "timestamp": datetime.now(timezone.utc).isoformat(),
          }
        )

      # Execute agent analysis
      try:
        response = await agent.analyze(
          query=request_data["message"],
          mode=mode,
          history=history,
          context=request_data.get("context"),
          callback=progress_callback,
        )

        # Send any accumulated progress events
        for event in progress_events:
          yield {"event": "progress", "data": json.dumps(event)}
          await asyncio.sleep(0)  # Allow other tasks to run

        # Send completion event with result
        yield {
          "event": "agent_completed",
          "data": json.dumps(
            {
              "content": response.content,
              "agent_used": response.agent_name,
              "mode_used": response.mode_used.value,
              "metadata": response.metadata,
              "tokens_used": response.tokens_used,
              "confidence_score": response.confidence_score,
              "execution_time": response.execution_time,
              "tools_called": response.tools_called,
              "timestamp": datetime.now(timezone.utc).isoformat(),
            }
          ),
        }

      except Exception as e:
        logger.error(f"Agent execution error: {str(e)}", exc_info=True)
        yield {
          "event": "error",
          "data": json.dumps(
            {
              "error": str(e),
              "error_type": type(e).__name__,
              "timestamp": datetime.now(timezone.utc).isoformat(),
            }
          ),
        }

    except Exception as e:
      logger.error(f"SSE stream error: {str(e)}", exc_info=True)
      yield {
        "event": "error",
        "data": json.dumps(
          {
            "error": f"Stream error: {str(e)}",
            "error_type": type(e).__name__,
          }
        ),
      }

  return EventSourceResponse(
    event_generator(),
    headers={
      "Cache-Control": "no-cache",
      "X-Accel-Buffering": "no",
    },
  )
