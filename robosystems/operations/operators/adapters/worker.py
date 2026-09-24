"""Worker execution adapter: `HttpToolAccess`, `FactoryCreditConsumer` and
`OperationManagerProgress`, reached through `OperatorWorkerTask`."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from robosystems.db.platform import SessionFactory
from robosystems.logger import logger
from robosystems.operations.operators.ai_client import get_ai_client
from robosystems.operations.operators.base import (
  OperatorMode,
  enforce_operator_graph_scope,
  enforce_operator_write_role,
)
from robosystems.operations.operators.credit_consumer import FactoryCreditConsumer
from robosystems.operations.operators.credit_preflight import enforce_operator_credits
from robosystems.operations.operators.operator_context import OperatorContext
from robosystems.operations.operators.progress import OperationManagerProgress
from robosystems.operations.operators.tool_access import HttpToolAccess
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

  An unrecognized `params["mode"]` falls back to STANDARD; `params` and its
  `context` dict both land in `ctx.extra`. The operator's metadata is also
  merged flat into the result, which the mapping operation's consumers read.
  """
  mode_str = params.get("mode", "standard")
  try:
    mode = OperatorMode(mode_str)
  except ValueError:
    mode = OperatorMode.STANDARD
  context = params.get("context") or {}
  if not isinstance(context, dict):
    context = {}

  # Re-checked, not trusted from the enqueuing request: the role or balance
  # may have changed while the task was queued.
  enforce_operator_write_role(operator, graph_id, user_id)
  enforce_operator_graph_scope(operator, graph_id)

  preflight_session = SessionFactory()
  try:
    enforce_operator_credits(operator, graph_id, user_id, preflight_session, mode)
  finally:
    preflight_session.close()

  # Not DirectToolAccess: a model-driven loop on it would see no tools.
  tools = HttpToolAccess(graph_id, read_only=operator.spec.read_only, user_id=user_id)
  ai_client = get_ai_client()
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
    history=list(params.get("history") or []),
    extra={**params, **context},
    ai=tracked_ai,
    tools=tools,
    progress=OperationManagerProgress(task_id, manager),
  )

  try:
    started = time.monotonic()
    result = await operator.run(ctx)
    execution_time = time.monotonic() - started
    tokens_used = tracked_ai.total_tokens.copy()
    metadata = {
      **result.metadata,
      "credits_consumed": tracked_ai.total_credits,
      "has_credit_tracking": True,
      "tokens_used": tokens_used,
      "call_count": tracked_ai.call_count,
    }
    return {
      "content": result.content,
      **result.metadata,
      "operator_used": operator.spec.name,
      "mode_used": mode.value,
      "metadata": metadata,
      "tokens_used": tokens_used,
      "confidence_score": result.confidence_score,
      "execution_time": execution_time,
      "tools_called": list(result.tools_called),
      "total_credits_consumed": tracked_ai.total_credits,
      "total_tokens": tokens_used,
    }

  except Exception as e:
    logger.error(f"Operator {operator.spec.name} failed in worker: {e}", exc_info=True)
    raise

  finally:
    await tools.close()
