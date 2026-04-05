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

if TYPE_CHECKING:
  from robosystems.worker.tasks.base import BaseTask

TASK_REGISTRY: dict[str, type[BaseTask]] = {}
_adapters_loaded = False


def register_task(task_type: str):
  """Decorator to register a task handler class."""

  def decorator(cls: type[BaseTask]) -> type[BaseTask]:
    TASK_REGISTRY[task_type] = cls
    return cls

  return decorator


def get_task_handler(task_type: str) -> type[BaseTask] | None:
  """Look up a registered task handler by task_type string."""
  return TASK_REGISTRY.get(task_type)


def load_adapter_tasks() -> None:
  """Load worker tasks from enabled adapters.

  Each adapter can expose a get_worker_components() function that
  returns {"task_types": [...]} after importing its task modules
  (which triggers @register_task side effects).

  Called once at worker startup from worker/__init__.py.
  """
  global _adapters_loaded
  if _adapters_loaded:
    return
  _adapters_loaded = True

  # Future adapters register tasks here:
  #
  # from robosystems.config import env
  #
  # if env.SEC_PIPELINE_ENABLED:
  #   from robosystems.adapters.sec.tasks import get_worker_components
  #   get_worker_components()  # triggers @register_task side effects
