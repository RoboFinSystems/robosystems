"""Worker execution adapter — runs operators in worker context.

Constructs an OperatorContext with:
- DirectToolAccess (in-process tool classes, no HTTP)
- FactoryCreditConsumer (creates sessions per call)
- OperationManagerProgress (SSE progress + cancellation)

Used by the OperatorWorkerTask bridge to run operators inside the
existing worker consumer loop.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from robosystems.logger import logger
from robosystems.operations.operators.ai_client import AIClient
from robosystems.operations.operators.base import OperatorMode
from robosystems.operations.operators.credit_consumer import FactoryCreditConsumer
from robosystems.operations.operators.operator_context import OperatorContext
from robosystems.operations.operators.progress import OperationManagerProgress
from robosystems.operations.operators.tool_access import DirectToolAccess
from robosystems.operations.operators.tracked_ai import TrackedAIClient

if TYPE_CHECKING:
  from robosystems.middleware.sse.operation_manager import OperationManager
  from robosystems.operations.operators.base import Operator


async def run_operator_worker(
  operator: Operator,
  task_id: str,
  graph_id: str,
  user_id: str,
  params: dict[str, Any],
  manager: OperationManager,
) -> dict[str, Any]:
  """Run an operator in worker context.

  Handles tool access, credit tracking, and progress reporting
  for long-running background tasks.

  Args:
      operator: The operator to run.
      task_id: Worker task identifier.
      graph_id: Graph database identifier.
      user_id: User ID string.
      params: Task parameters (operator-specific).
      manager: SSE OperationManager for progress/cancellation.

  Returns:
      Result dict with operator output + credit summary.
  """
  tools = DirectToolAccess(graph_id)
  ai_client = AIClient()
  credit_consumer = FactoryCreditConsumer()

  tracked_ai = TrackedAIClient(
    ai_client=ai_client,
    graph_id=graph_id,
    user_id=user_id,
    credit_consumer=credit_consumer,
  )

  mode_str = params.get("mode", "standard")
  try:
    mode = OperatorMode(mode_str)
  except ValueError:
    mode = OperatorMode.STANDARD

  ctx = OperatorContext(
    graph_id=graph_id,
    user_id=user_id,
    query=params.get("query", ""),
    mode=mode,
    history=[],
    extra=params,
    ai=tracked_ai,
    tools=tools,
    progress=OperationManagerProgress(task_id, manager),
  )

  try:
    result = await operator.run(ctx)
    return {
      "content": result.content,
      **result.metadata,
      "total_credits_consumed": tracked_ai.total_credits,
      "total_tokens": tracked_ai.total_tokens.copy(),
    }

  except Exception as e:
    logger.error(f"Operator {operator.spec.name} failed in worker: {e}", exc_info=True)
    raise

  finally:
    await tools.close()
