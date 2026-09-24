"""Worker task ``operator``: runs the operator named by
``params["operator_type"]`` through `run_operator_worker`."""

from __future__ import annotations

from typing import Any

from robosystems.operations.operators.adapters.worker import run_operator_worker
from robosystems.worker.tasks import register_task
from robosystems.worker.tasks.base import BaseTask


@register_task("operator")
class OperatorWorkerTask(BaseTask):
  async def execute(self) -> dict[str, Any]:
    from robosystems.operations.operators.operator_registry import get_operator

    operator_type = self.params.get("operator_type")
    if not operator_type:
      # Raise so the consumer records the operation FAILED; a returned dict
      # would be stored as a COMPLETED result with no content.
      raise ValueError("Missing operator_type in task params")

    operator = get_operator(operator_type)

    return await run_operator_worker(
      operator=operator,
      task_id=self.task_id,
      graph_id=self.graph_id,
      user_id=self.user_id,
      params=self.params,
      manager=self.manager,
    )
