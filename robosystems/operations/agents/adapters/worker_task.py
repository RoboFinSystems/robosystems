"""Bridge task — runs new-style agents inside the existing worker consumer.

Registered as task_type="agent" in the worker task registry. When the
consumer picks up a task with this type, it looks up the agent by
params["agent_type"] and delegates to run_agent_worker().

This allows the existing worker infrastructure (consumer loop, Valkey queue,
OTel tracing, Dagster reporting) to run new Agent implementations without
any changes to the worker code.
"""

from __future__ import annotations

from typing import Any

from robosystems.operations.agents.adapters.worker import run_agent_worker
from robosystems.worker.tasks import register_task
from robosystems.worker.tasks.base import BaseTask


@register_task("agent")
class AgentWorkerTask(BaseTask):
  """Bridge: runs a new-style Agent inside the existing worker consumer loop."""

  async def execute(self) -> dict[str, Any]:
    from robosystems.operations.agents.agent_registry import get_agent

    agent_type = self.params.get("agent_type")
    if not agent_type:
      return {"error": "Missing agent_type in task params"}

    agent = get_agent(agent_type)

    return await run_agent_worker(
      agent=agent,
      task_id=self.task_id,
      graph_id=self.graph_id,
      user_id=self.user_id,
      params=self.params,
      manager=self.manager,
    )
