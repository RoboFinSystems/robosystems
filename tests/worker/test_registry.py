"""Tests for the worker task registry."""

from typing import Any

import pytest

from robosystems.worker.tasks import TASK_REGISTRY, get_task_handler, register_task
from robosystems.worker.tasks.base import BaseTask


@pytest.fixture(autouse=True)
def _clean_registry():
  """Remove test tasks from registry after each test."""
  original = dict(TASK_REGISTRY)
  yield
  TASK_REGISTRY.clear()
  TASK_REGISTRY.update(original)


class DummyTask(BaseTask):
  async def execute(self) -> dict[str, Any]:
    return {"ok": True}


def test_register_task_decorator():
  @register_task("test_dummy")
  class AnotherDummy(BaseTask):
    async def execute(self) -> dict[str, Any]:
      return {}

  assert get_task_handler("test_dummy") is AnotherDummy


def test_register_task_returns_class():
  cls = register_task("test_return")(DummyTask)
  assert cls is DummyTask


def test_get_task_handler_unknown_returns_none():
  assert get_task_handler("nonexistent_task_type") is None


def test_multiple_registrations():
  @register_task("task_a")
  class TaskA(BaseTask):
    async def execute(self) -> dict[str, Any]:
      return {}

  @register_task("task_b")
  class TaskB(BaseTask):
    async def execute(self) -> dict[str, Any]:
      return {}

  assert get_task_handler("task_a") is TaskA
  assert get_task_handler("task_b") is TaskB
