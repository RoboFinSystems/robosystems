"""API execution adapter — runs agents in API request context.

Constructs an AgentContext with:
- HttpToolAccess (MCP via HTTP client)
- SessionCreditConsumer (reuses request db session)
- CallbackProgress (wraps optional callback function)

Used by the orchestrator and router handlers for sync/SSE execution.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from robosystems.logger import logger
from robosystems.operations.agents.agent_context import AgentContext
from robosystems.operations.agents.ai_client import AIClient
from robosystems.operations.agents.base import AgentMode, AgentResult
from robosystems.operations.agents.credit_consumer import SessionCreditConsumer
from robosystems.operations.agents.progress import CallbackProgress
from robosystems.operations.agents.tool_access import HttpToolAccess
from robosystems.operations.agents.tracked_ai import TrackedAIClient

if TYPE_CHECKING:
  from collections.abc import Callable

  from robosystems.models.iam import User
  from robosystems.operations.agents.base import Agent


async def run_agent_api(
  agent: Agent,
  graph_id: str,
  user: User,
  query: str,
  mode: AgentMode = AgentMode.STANDARD,
  db_session=None,
  history: list[dict[str, Any]] | None = None,
  context: dict[str, Any] | None = None,
  callback: Callable | None = None,
) -> AgentResult:
  """Run an agent in API request context.

  Handles tool initialization, credit tracking, and cleanup.
  Credits are consumed automatically via TrackedAIClient.

  Args:
      agent: The agent to run.
      graph_id: Graph database identifier.
      user: Authenticated user.
      query: User's query.
      mode: Execution mode.
      db_session: SQLAlchemy session for credit operations.
      history: Conversation history.
      context: Additional context / task parameters.
      callback: Optional progress callback.

  Returns:
      AgentResult with domain content + runtime metadata attached.
  """
  tools = HttpToolAccess(graph_id)
  ai_client = AIClient()

  credit_consumer = SessionCreditConsumer(db_session) if db_session else None

  tracked_ai = TrackedAIClient(
    ai_client=ai_client,
    graph_id=graph_id,
    user_id=str(user.id),
    credit_consumer=credit_consumer,
  )

  ctx = AgentContext(
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
    result = await agent.run(ctx)

    # Attach runtime metadata
    result.metadata["credits_consumed"] = tracked_ai.total_credits
    result.metadata["has_credit_tracking"] = tracked_ai._credit_consumer is not None
    result.metadata["tokens_used"] = tracked_ai.total_tokens.copy()
    result.metadata["call_count"] = tracked_ai.call_count

    return result

  except Exception as e:
    logger.error(f"Agent {agent.spec.name} failed: {e}", exc_info=True)
    raise

  finally:
    await tools.close()
