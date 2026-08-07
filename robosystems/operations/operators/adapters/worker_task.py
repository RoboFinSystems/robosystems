"""Bridge task — runs operators inside the worker consumer loop.

Registered as task_type="operator". When the consumer picks up such a task it
looks the operator up by ``params["operator_type"]`` and delegates to
`run_operator_worker`, so an operator inherits the worker infrastructure
(consumer loop, Valkey queue, OTel tracing, Dagster reporting) without the
worker knowing anything about operators.
"""

from __future__ import annotations

from typing import Any

from robosystems.operations.operators.adapters.worker import run_operator_worker
from robosystems.worker.tasks import register_task
from robosystems.worker.tasks.base import BaseTask


@register_task("operator")
class OperatorWorkerTask(BaseTask):
  """Runs the operator named by ``params["operator_type"]``."""

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
