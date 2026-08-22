"""Worker tasks for RoboLedger.

Each task extends ``BaseTask`` and registers itself with ``@register_task`` so
the worker consumer loop can dispatch it, mirroring
:mod:`robosystems.operations.graph.tasks`. The business logic stays in
``operations/``; the worker supplies only the infrastructure.
"""

# Imported for side effects: importing the module runs its @register_task.
from robosystems.operations.roboledger.tasks import period_close as period_close


def get_worker_components() -> dict[str, list[str]]:
  """Return worker task types registered by this module."""
  return {
    "task_types": [
      "period_close",
    ],
  }
