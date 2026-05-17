"""Bridge task — runs new-style operators inside the existing worker consumer.

Registered as task_type="operator" in the worker task registry. When the
consumer picks up a task with this type, it looks up the operator by
params["operator_type"] and delegates to run_operator_worker().

This allows the existing worker infrastructure (consumer loop, Valkey queue,
OTel tracing, Dagster reporting) to run new Operator implementations without
any changes to the worker code.
"""

from __future__ import annotations

from typing import Any

from robosystems.operations.operators.adapters.worker import run_operator_worker
from robosystems.worker.tasks import register_task
from robosystems.worker.tasks.base import BaseTask


@register_task("operator")
class OperatorWorkerTask(BaseTask):
  """Bridge: runs a new-style Operator inside the existing worker consumer loop."""

  async def execute(self) -> dict[str, Any]:
    from robosystems.operations.operators.operator_registry import get_operator

    operator_type = self.params.get("operator_type")
    if not operator_type:
      return {"error": "Missing operator_type in task params"}

    operator = get_operator(operator_type)

    return await run_operator_worker(
      operator=operator,
      task_id=self.task_id,
      graph_id=self.graph_id,
      user_id=self.user_id,
      params=self.params,
      manager=self.manager,
    )
