"""API execution adapter — runs operators in API request context.

Builds an OperatorContext from `HttpToolAccess` (MCP over HTTP),
`SessionCreditConsumer` (the request's own db session), and `CallbackProgress`.
Used by the orchestrator and by router handlers for sync/SSE execution.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from robosystems.logger import logger
from robosystems.operations.operators.ai_client import AIClient
from robosystems.operations.operators.base import (
  OperatorMode,
  OperatorResult,
  enforce_operator_write_role,
)
from robosystems.operations.operators.credit_consumer import SessionCreditConsumer
from robosystems.operations.operators.credit_preflight import enforce_operator_credits
from robosystems.operations.operators.operator_context import OperatorContext
from robosystems.operations.operators.progress import CallbackProgress
from robosystems.operations.operators.tool_access import HttpToolAccess
from robosystems.operations.operators.tracked_ai import TrackedAIClient

if TYPE_CHECKING:
  from collections.abc import Callable

  from robosystems.models.core import User
  from robosystems.operations.operators.base import Operator


async def run_operator_api(
  operator: Operator,
  graph_id: str,
  user: User,
  query: str,
  mode: OperatorMode = OperatorMode.STANDARD,
  db_session=None,
  history: list[dict[str, Any]] | None = None,
  context: dict[str, Any] | None = None,
  callback: Callable | None = None,
) -> OperatorResult:
  """Run an operator in API request context.

  Gates the run, wires up tools and credit tracking, and tears the tool
  connection down afterwards. Returns the operator's `OperatorResult` with
  credit and token metadata attached. Without `db_session` the run proceeds
  unbilled and un-pre-flighted — that path is logged loudly and should only
  ever be reached by tests.
  """
  # Before any tool access is constructed: the tool layer carries no user
  # identity, so this is the only point at which the caller's graph role can
  # be checked on this path.
  enforce_operator_write_role(operator, graph_id, str(user.id))

  # Credits are consumed after each Bedrock call returns, so this pre-flight is
  # what bounds spend. The SSE and background-queue strategies reach this
  # function without an orchestrator, so checking here rather than there is
  # what makes the gate cover every execution path.
  enforce_operator_credits(operator, graph_id, str(user.id), db_session, mode)

  # The tool surface must not exceed what the role gate above checked for:
  # a read-only operator skips the write-role gate, so it must also get a
  # read-only tool surface.
  tools = HttpToolAccess(graph_id, read_only=operator.spec.read_only)
  ai_client = AIClient()

  if db_session is None:
    logger.error(
      f"Operator running WITHOUT credit tracking (no db_session): "
      f"graph={graph_id} user={user.id} — Bedrock cost will not be attributed "
      f"and the pre-flight balance check was skipped"
    )
  credit_consumer = SessionCreditConsumer(db_session) if db_session else None

  tracked_ai = TrackedAIClient(
    ai_client=ai_client,
    graph_id=graph_id,
    user_id=str(user.id),
    credit_consumer=credit_consumer,
  )

  ctx = OperatorContext(
    graph_id=graph_id,
    user_id=str(user.id),
    query=query,
    mode=mode,
    history=history or [],
    extra=context or {},
    ai=tracked_ai,
    tools=tools,
    progress=CallbackProgress(callback),
  )

  try:
    result = await operator.run(ctx)

    result.metadata["credits_consumed"] = tracked_ai.total_credits
    result.metadata["has_credit_tracking"] = tracked_ai._credit_consumer is not None
    result.metadata["tokens_used"] = tracked_ai.total_tokens.copy()
    result.metadata["call_count"] = tracked_ai.call_count

    return result

  except Exception as e:
    logger.error(f"Operator {operator.spec.name} failed: {e}", exc_info=True)
    raise

  finally:
    await tools.close()
