"""Worker tasks for RoboLedger, registered via ``@register_task``."""

# Imported for side effects: importing the module runs its @register_task.
from robosystems.operations.roboledger.tasks import period_close as period_close
from robosystems.operations.roboledger.tasks import (
  plan_history_backfill as plan_history_backfill,
)


def get_worker_components() -> dict[str, list[str]]:
  """Return worker task types registered by this module."""
  return {
    "task_types": [
      "period_close",
      "plan_history_backfill",
    ],
  }
