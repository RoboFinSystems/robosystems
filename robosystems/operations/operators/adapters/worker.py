"""Worker execution adapter: `HttpToolAccess`, `FactoryCreditConsumer` and
`OperationManagerProgress`, reached through `OperatorWorkerTask`."""

from __future__ import annotations

import time
from datetime import UTC, datetime
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
from robosystems.operations.operators.run_record import (
  OperatorRunRecord,
  record_operator_run,
  result_summary,
)
from robosystems.operations.operators.tool_access import HttpToolAccess
from robosystems.operations.operators.tracked_ai import TrackedAIClient
from robosystems.security.operation_audit import AuditCaller

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
  landed_writes: list[dict[str, Any]] | None = None,
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
    enforce_operator_credits(
      operator,
      graph_id,
      user_id,
      preflight_session,
      mode,
      params.get("operator_type"),
    )
  finally:
    preflight_session.close()

  # Not DirectToolAccess: a model-driven loop on it would see no tools.
  tools = HttpToolAccess(
    graph_id,
    read_only=operator.spec.read_only,
    user_id=user_id,
    caller=AuditCaller(
      operator_type=params.get("operator_type") or operator.spec.name,
      operation_id=task_id,
    ),
  )
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
  if landed_writes is not None:
    ctx.writes = landed_writes

  # The run is recorded whatever its outcome: an answer, a failure, a
  # cancellation, or the task budget interrupting it.
  started_at = datetime.now(UTC)
  started = time.monotonic()
  status = "failed"
  answer: str | None = None
  error: str | None = None
  recorded_result: dict[str, Any] = {}
  try:
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
    status = "cancelled" if result.metadata.get("cancelled") else "completed"
    answer = result.content
    recorded_result = {
      **result_summary(metadata),
      "tools_called": list(result.tools_called),
      "confidence_score": result.confidence_score,
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
    error = str(e) or type(e).__name__
    logger.error(f"Operator {operator.spec.name} failed in worker: {e}", exc_info=True)
    raise

  except BaseException as e:
    # The consumer's task budget cancels the coroutine; the record says so
    # rather than reading as a run that never happened.
    error = f"Interrupted before an answer ({type(e).__name__})"
    raise

  finally:
    if not recorded_result:
      recorded_result = {
        "credits_consumed": tracked_ai.total_credits,
        "tokens_used": tracked_ai.total_tokens.copy(),
        "call_count": tracked_ai.call_count,
      }
    finished_at = datetime.now(UTC)
    record_operator_run(
      OperatorRunRecord(
        operation_id=task_id,
        thread_id=params.get("thread_id") or None,
        graph_id=graph_id,
        user_id=user_id,
        operator_type=params.get("operator_type") or operator.spec.name,
        mode=mode.value,
        status=status,
        question=ctx.query,
        history=list(ctx.history),
        transcript=list(tracked_ai.transcript),
        answer=answer,
        error=error,
        result=recorded_result,
        started_at=started_at,
        finished_at=finished_at,
        duration_ms=round((time.monotonic() - started) * 1000, 2),
      )
    )
    await tools.close()
