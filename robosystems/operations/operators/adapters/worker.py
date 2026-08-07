"""Worker execution adapter — runs operators in worker context.

Builds an OperatorContext from `DirectToolAccess` (in-process tool classes, no
HTTP), `FactoryCreditConsumer` (a session per call), and
`OperationManagerProgress` (SSE progress + cancellation). Reached through the
`OperatorWorkerTask` bridge in `worker_task.py`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from robosystems.db.platform import SessionFactory
from robosystems.logger import logger
from robosystems.operations.operators.ai_client import AIClient
from robosystems.operations.operators.base import (
  OperatorMode,
  enforce_operator_write_role,
)
from robosystems.operations.operators.credit_consumer import FactoryCreditConsumer
from robosystems.operations.operators.credit_preflight import enforce_operator_credits
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
  """Run an operator in worker context for a long-running background task.

  `params` carries the operator-specific arguments and an optional `mode`
  (an unrecognized value falls back to STANDARD). Returns the operator's
  content and metadata merged with a credit/token summary.
  """
  mode_str = params.get("mode", "standard")
  try:
    mode = OperatorMode(mode_str)
  except ValueError:
    mode = OperatorMode.STANDARD

  # Both gates are re-checked here rather than trusted from the enqueuing
  # request: a task can sit in the queue, and the role that authorized it may
  # have been revoked — or the balance spent — in between. Same reasoning as
  # `GraphCreationService._validate_org`.
  enforce_operator_write_role(operator, graph_id, user_id)

  preflight_session = SessionFactory()
  try:
    enforce_operator_credits(operator, graph_id, user_id, preflight_session, mode)
  finally:
    preflight_session.close()

  tools = DirectToolAccess(graph_id)
  ai_client = AIClient()
  credit_consumer = FactoryCreditConsumer()

  tracked_ai = TrackedAIClient(
    ai_client=ai_client,
    graph_id=graph_id,
    user_id=user_id,
    credit_consumer=credit_consumer,
  )

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
