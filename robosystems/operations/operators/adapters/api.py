"""API execution adapter — runs operators in API request context.

Constructs an OperatorContext with:
- HttpToolAccess (MCP via HTTP client)
- SessionCreditConsumer (reuses request db session)
- CallbackProgress (wraps optional callback function)

Used by the orchestrator and router handlers for sync/SSE execution.
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

  Handles tool initialization, credit tracking, and cleanup.
  Credits are consumed automatically via TrackedAIClient.

  Args:
      operator: The operator to run.
      graph_id: Graph database identifier.
      user: Authenticated user.
      query: User's query.
      mode: Execution mode.
      db_session: SQLAlchemy session for credit operations.
      history: Conversation history.
      context: Additional context / task parameters.
      callback: Optional progress callback.

  Returns:
      OperatorResult with domain content + runtime metadata attached.
  """
  # Before any tool access is constructed: the tool layer carries no user
  # identity, so this is the only point at which the caller's graph role can
  # be checked on this path.
  enforce_operator_write_role(operator, graph_id, str(user.id))

  tools = HttpToolAccess(graph_id)
  ai_client = AIClient()

  if db_session is None:
    logger.warning(
      f"Operator running WITHOUT credit tracking (no db_session): "
      f"graph={graph_id} user={user.id} — Bedrock cost will not be attributed"
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

    # Attach runtime metadata
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
