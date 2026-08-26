"""Tests for ECS task scale-in protection."""

import asyncio
import json
import os
from unittest.mock import MagicMock, patch

from robosystems.worker.constants import DEFAULT_TASK_TIMEOUT, TASK_TIMEOUTS
from robosystems.worker.task_protection import (
  MAX_METADATA_FAILURES,
  METADATA_URI_ENV,
  PROTECTION_EXPIRES_MINUTES,
  TaskProtectionManager,
)

CLUSTER = "arn:aws:ecs:us-east-1:123456789012:cluster/robosystems-dagster-prod"
TASK_ARN = "arn:aws:ecs:us-east-1:123456789012:task/robosystems-dagster-prod/abc123"


def _urlopen_returning(payload: dict):
  """Build a urlopen replacement whose context manager yields the payload."""
  resp = MagicMock()
  resp.read.return_value = json.dumps(payload).encode()
  cm = MagicMock()
  cm.__enter__.return_value = resp
  cm.__exit__.return_value = False
  return MagicMock(return_value=cm)


def _enabled_manager() -> TaskProtectionManager:
  with patch.dict(os.environ, {METADATA_URI_ENV: "http://169.254.170.2/v4/x"}):
    return TaskProtectionManager()


def test_disabled_outside_ecs():
  """Without the ECS metadata env var (local/dev/test), the manager is off."""
  mgr = TaskProtectionManager()
  assert mgr._enabled is False


def test_protect_noop_when_disabled():
  """A disabled manager never touches ECS."""
  mgr = TaskProtectionManager()
  mgr._set_protection_sync = MagicMock()
  asyncio.run(mgr.protect())
  mgr._set_protection_sync.assert_not_called()


def test_load_metadata_parses_cluster_and_task():
  mgr = _enabled_manager()
  with patch(
    "robosystems.worker.task_protection.urllib.request.urlopen",
    _urlopen_returning({"Cluster": CLUSTER, "TaskARN": TASK_ARN}),
  ):
    assert mgr._load_metadata() is True
  assert mgr._cluster == CLUSTER
  assert mgr._task_arn == TASK_ARN


def test_metadata_failure_retries_then_disables():
  """A single metadata blip retries; only repeated failures disable."""
  mgr = _enabled_manager()
  with patch(
    "robosystems.worker.task_protection.urllib.request.urlopen",
    side_effect=OSError("metadata unreachable"),
  ):
    assert mgr._load_metadata() is False
    assert mgr._enabled is True  # one blip does not disable
    for _ in range(MAX_METADATA_FAILURES):
      mgr._load_metadata()
  assert mgr._enabled is False  # repeated failures eventually disable


def test_protect_calls_update_task_protection_with_expiry():
  mgr = _enabled_manager()
  mgr._cluster = CLUSTER
  mgr._task_arn = TASK_ARN
  ecs = MagicMock()
  ecs.update_task_protection.return_value = {"failures": []}
  mgr._ecs_client = ecs

  mgr._set_protection_sync(True)

  ecs.update_task_protection.assert_called_once_with(
    cluster=CLUSTER,
    tasks=[TASK_ARN],
    protectionEnabled=True,
    expiresInMinutes=PROTECTION_EXPIRES_MINUTES,
  )


def test_unprotect_omits_expiry():
  """expiresInMinutes is only valid when enabling protection."""
  mgr = _enabled_manager()
  mgr._cluster = CLUSTER
  mgr._task_arn = TASK_ARN
  ecs = MagicMock()
  ecs.update_task_protection.return_value = {"failures": []}
  mgr._ecs_client = ecs

  mgr._set_protection_sync(False)

  kwargs = ecs.update_task_protection.call_args.kwargs
  assert kwargs["protectionEnabled"] is False
  assert "expiresInMinutes" not in kwargs


def test_set_protection_swallows_client_error():
  """A failing update_task_protection is logged, never raised (best-effort)."""
  mgr = _enabled_manager()
  mgr._cluster = CLUSTER
  mgr._task_arn = TASK_ARN
  ecs = MagicMock()
  ecs.update_task_protection.side_effect = RuntimeError("ECS throttled")
  mgr._ecs_client = ecs

  # Must not raise.
  asyncio.run(mgr.protect())
  asyncio.run(mgr.unprotect())


def test_protection_lease_outlives_longest_task():
  """The protection lease must exceed the longest task timeout."""
  longest = max([*TASK_TIMEOUTS.values(), DEFAULT_TASK_TIMEOUT])
  assert longest < PROTECTION_EXPIRES_MINUTES * 60


def test_protection_lease_outlives_a_thread_backed_task_and_its_grace():
  """A task that runs its work through ``BaseTask.run_blocking`` can hold the
  worker for its budget plus one more budget of thread-join grace. The lease
  must cover both, or the close it exists to protect is exposed to scale-in
  precisely when it is running long."""
  assert 2 * TASK_TIMEOUTS["period_close"] < PROTECTION_EXPIRES_MINUTES * 60
