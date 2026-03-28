"""Task registry for the background worker.

Task handlers register themselves via the @register_task decorator.
The consumer loop looks up handlers by task_type string.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
  from robosystems.worker.tasks.base import BaseTask

TASK_REGISTRY: dict[str, type[BaseTask]] = {}


def register_task(task_type: str):
  """Decorator to register a task handler class."""

  def decorator(cls: type[BaseTask]) -> type[BaseTask]:
    TASK_REGISTRY[task_type] = cls
    return cls

  return decorator


def get_task_handler(task_type: str) -> type[BaseTask] | None:
  """Look up a registered task handler by task_type string."""
  return TASK_REGISTRY.get(task_type)
