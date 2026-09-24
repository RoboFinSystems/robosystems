"""Task registry for the background worker.

Task handlers register themselves via the @register_task decorator.
The consumer loop looks up handlers by task_type string.

Platform tasks register via direct imports in worker/__init__.py.
Adapter tasks register via load_adapter_tasks(), which calls
get_worker_components() on each enabled adapter — the same pattern
as get_dagster_components() for Dagster pipelines.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from robosystems.logger import get_logger

if TYPE_CHECKING:
  from robosystems.worker.tasks.base import BaseTask

logger = get_logger(__name__)

TASK_REGISTRY: dict[str, type[BaseTask]] = {}
_adapter_tasks_loaded: list[bool] = []


def register_task(task_type: str):
  """Decorator to register a task handler class."""

  def decorator(cls: type[BaseTask]) -> type[BaseTask]:
    if task_type in TASK_REGISTRY:
      logger.warning(
        f"Overriding existing task registration '{task_type}': "
        f"{TASK_REGISTRY[task_type].__name__} -> {cls.__name__}"
      )
    TASK_REGISTRY[task_type] = cls
    # Lets a task size its own waits from its TASK_TIMEOUTS budget.
    cls.task_type = task_type
    return cls

  return decorator


def get_task_handler(task_type: str) -> type[BaseTask] | None:
  """Look up a registered task handler by task_type string."""
  return TASK_REGISTRY.get(task_type)


def load_adapter_tasks() -> None:
  """Load worker tasks from enabled adapters, once at worker startup.

  An adapter registers by calling its get_worker_components() here, gated on
  its flag; importing its task modules triggers @register_task. No adapter
  registers tasks today.
  """
  if _adapter_tasks_loaded:
    return
  _adapter_tasks_loaded.append(True)


def clear_registry() -> None:
  """Clear all registrations. For testing only."""
  TASK_REGISTRY.clear()
  _adapter_tasks_loaded.clear()
