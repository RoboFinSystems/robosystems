"""A queued task runs on the user's current graph role, not the one they held
when they enqueued it."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.models.core.graph.graph_user import GraphRole
from robosystems.worker.consumer import _process_task
from robosystems.worker.tasks import TASK_REGISTRY, register_task
from robosystems.worker.tasks.base import BaseTask, reauthorize_task

GRAPH = "kg0123456789abcdef"


class _MemberTask(BaseTask):
  async def execute(self) -> dict[str, Any]:
    return {"ran": True}


class _AdminReadTask(BaseTask):
  required_role = "admin"
  require_write = False

  async def execute(self) -> dict[str, Any]:
    return {}


class _NoGraphTask(BaseTask):
  required_role = None

  async def execute(self) -> dict[str, Any]:
    return {}


def _reauthorize(
  task_cls, role, *, access_error: str | None = None, graph: str | None = GRAPH
):
  access = MagicMock()
  if access_error:
    access.side_effect = HTTPException(status_code=403, detail=access_error)
  with (
    patch("robosystems.database.SessionFactory", return_value=MagicMock()),
    patch(
      "robosystems.models.core.GraphUser.get_effective_role", return_value=(role, False)
    ) as role_lookup,
    patch("robosystems.middleware.billing.enforcement.require_graph_access", access),
    patch("robosystems.security.SecurityAuditLogger.log_authorization_denied"),
  ):
    denied = reauthorize_task(task_cls, graph, "user_queued", {})
  return denied, role_lookup, access


@pytest.mark.unit
class TestReauthorizeTask:
  def test_member_still_a_member_may_run(self):
    denied, _, access = _reauthorize(_MemberTask, GraphRole.MEMBER)
    assert denied is None
    access.assert_called_once()
    assert access.call_args.kwargs["require_write"] is True

  def test_member_downgraded_to_viewer_is_refused(self):
    denied, _, _ = _reauthorize(_MemberTask, GraphRole.VIEWER)
    assert denied and "removed or reduced" in denied

  def test_member_removed_from_graph_is_refused(self):
    denied, _, _ = _reauthorize(_MemberTask, None)
    assert denied and GRAPH in denied

  def test_lifecycle_refusal_is_reported(self):
    denied, _, _ = _reauthorize(
      _MemberTask, GraphRole.ADMIN, access_error="Graph suspended"
    )
    assert denied == "Graph suspended"

  def test_admin_read_task_skips_the_write_gate(self):
    denied, _, access = _reauthorize(_AdminReadTask, GraphRole.ADMIN)
    assert denied is None
    assert access.call_args.kwargs["require_write"] is False
    denied, _, _ = _reauthorize(_AdminReadTask, GraphRole.MEMBER)
    assert denied

  def test_no_graph_means_no_check(self):
    denied, role_lookup, _ = _reauthorize(_NoGraphTask, None)
    assert denied is None
    role_lookup.assert_not_called()
    denied, role_lookup, _ = _reauthorize(_MemberTask, None, graph=None)
    assert denied is None
    role_lookup.assert_not_called()


@pytest.fixture
def _registered_member_task():
  original = dict(TASK_REGISTRY)
  register_task("test_member_task")(_MemberTask)
  yield
  TASK_REGISTRY.clear()
  TASK_REGISTRY.update(original)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_consumer_refuses_a_task_whose_user_lost_access(_registered_member_task):
  manager = AsyncMock()
  manager.get_operation_status = AsyncMock(return_value="pending")
  queue = AsyncMock()
  task = {
    "task_id": "op_01REVOKED",
    "task_type": "test_member_task",
    "graph_id": GRAPH,
    "user_id": "user_removed",
    "params": {},
  }
  with (
    patch(
      "robosystems.worker.consumer.reauthorize_task", return_value="Access removed"
    ),
    patch("robosystems.worker.consumer.release_task_lock") as release,
  ):
    await _process_task(
      task, "{}", queue, "worker:inflight", manager, "w1", AsyncMock()
    )

  manager.mark_running.assert_not_called()
  manager.complete_operation.assert_not_called()
  manager.fail_operation.assert_awaited_once()
  assert manager.fail_operation.call_args.kwargs["error"] == "Access removed"
  assert (
    manager.fail_operation.call_args.kwargs["error_details"]["error_type"]
    == "AuthorizationRevoked"
  )
  release.assert_called_once()
  queue.lrem.assert_awaited_once()
