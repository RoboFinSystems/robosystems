"""SSE streaming support for API-based AI Operator operations.

Provides real-time progress updates for medium-duration operator operations
(5-30s) that run on the API but benefit from progress feedback.
"""

import asyncio
import json
from datetime import UTC, datetime
from typing import Any

from sse_starlette.sse import EventSourceResponse

from robosystems.logger import logger
from robosystems.models.core import User
from robosystems.operations.operators.adapters.api import run_operator_api
from robosystems.operations.operators.base import OperatorMode
from robosystems.operations.operators.operator_registry import get_operator


async def stream_operator_execution(
  operator_type: str,
  graph_id: str,
  request_data: dict[str, Any],
  current_user: User,
  db_session: Any,
) -> EventSourceResponse:
  """Stream operator execution with SSE progress updates."""

  async def event_generator():
    try:
      yield {
        "event": "operator_started",
        "data": json.dumps(
          {
            "operator_type": operator_type,
            "graph_id": graph_id,
            "timestamp": datetime.now(UTC).isoformat(),
          }
        ),
      }

      operator = get_operator(operator_type)

      yield {
        "event": "operator_initialized",
        "data": json.dumps(
          {
            "operator_name": operator.spec.name,
            "description": operator.spec.description,
          }
        ),
      }

      mode_str = request_data.get("mode", "standard")
      mode_map = {
        "quick": OperatorMode.QUICK,
        "standard": OperatorMode.STANDARD,
        "extended": OperatorMode.EXTENDED,
        "streaming": OperatorMode.STREAMING,
      }
      mode = mode_map.get(mode_str.lower(), OperatorMode.STANDARD)

      progress_events = []

      def progress_callback(stage: str, percentage: int, message: str):
        progress_events.append(
          {
            "stage": stage,
            "percentage": percentage,
            "message": message,
            "timestamp": datetime.now(UTC).isoformat(),
          }
        )

      try:
        result = await run_operator_api(
          operator=operator,
          graph_id=graph_id,
          user=current_user,
          query=request_data["message"],
          mode=mode,
          db_session=db_session,
          history=request_data.get("history", []),
          context=request_data.get("context"),
          callback=progress_callback,
        )

        for event in progress_events:
          yield {"event": "progress", "data": json.dumps(event)}
          await asyncio.sleep(0)

        yield {
          "event": "operator_completed",
          "data": json.dumps(
            {
              "content": result.content,
              "operator_used": operator.spec.name,
              "mode_used": mode.value,
              "metadata": result.metadata,
              "tokens_used": result.metadata.get("tokens_used"),
              "confidence_score": result.confidence_score,
              "execution_time": result.metadata.get("execution_time"),
              "tools_called": result.tools_called,
              "timestamp": datetime.now(UTC).isoformat(),
            }
          ),
        }

      except Exception as e:
        logger.error(f"Operator execution error: {e!s}", exc_info=True)
        yield {
          "event": "error",
          "data": json.dumps(
            {
              "error": str(e),
              "error_type": type(e).__name__,
              "timestamp": datetime.now(UTC).isoformat(),
            }
          ),
        }

    except Exception as e:
      logger.error(f"SSE stream error: {e!s}", exc_info=True)
      yield {
        "event": "error",
        "data": json.dumps(
          {"error": f"Stream error: {e!s}", "error_type": type(e).__name__}
        ),
      }

  return EventSourceResponse(
    event_generator(),
    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
  )
