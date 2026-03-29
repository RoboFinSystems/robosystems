"""Worker execution adapter — runs agents in worker context.

Constructs an AgentContext with:
- DirectToolAccess (in-process tool classes, no HTTP)
- FactoryCreditConsumer (creates sessions per call)
- OperationManagerProgress (SSE progress + cancellation)

Used by the AgentWorkerTask bridge to run agents inside the
existing worker consumer loop.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from robosystems.logger import logger
from robosystems.operations.agents.agent_context import AgentContext
from robosystems.operations.agents.ai_client import AIClient
from robosystems.operations.agents.base import AgentMode
from robosystems.operations.agents.credit_consumer import FactoryCreditConsumer
from robosystems.operations.agents.progress import OperationManagerProgress
from robosystems.operations.agents.tool_access import DirectToolAccess
from robosystems.operations.agents.tracked_ai import TrackedAIClient

if TYPE_CHECKING:
  from robosystems.middleware.sse.operation_manager import OperationManager
  from robosystems.operations.agents.base import Agent


async def run_agent_worker(
  agent: Agent,
  task_id: str,
  graph_id: str,
  user_id: str,
  params: dict[str, Any],
  manager: OperationManager,
) -> dict[str, Any]:
  """Run an agent in worker context.

  Handles tool access, credit tracking, and progress reporting
  for long-running background tasks.

  Args:
      agent: The agent to run.
      task_id: Worker task identifier.
      graph_id: Graph database identifier.
      user_id: User ID string.
      params: Task parameters (agent-specific).
      manager: SSE OperationManager for progress/cancellation.

  Returns:
      Result dict with agent output + credit summary.
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
    mode = AgentMode(mode_str)
  except ValueError:
    mode = AgentMode.STANDARD

  ctx = AgentContext(
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
    result = await agent.run(ctx)
    return {
      "content": result.content,
      **result.metadata,
      "total_credits_consumed": tracked_ai.total_credits,
      "total_tokens": tracked_ai.total_tokens.copy(),
    }

  except Exception as e:
    logger.error(f"Agent {agent.spec.name} failed in worker: {e}", exc_info=True)
    raise

  finally:
    await tools.close()
