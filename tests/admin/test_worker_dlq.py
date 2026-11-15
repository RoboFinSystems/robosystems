"""`worker dlq retry` re-seals only what the platform sealed."""

import json
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from robosystems.admin.commands.worker import dlq
from robosystems.worker.client import dump_task, task_is_sealed


class _Lists:
  def __init__(self, dlq_entries):
    self.lists = {"worker:dlq": list(dlq_entries), "worker:tasks": []}

  def llen(self, key):
    return len(self.lists[key])

  def lpop(self, key):
    return self.lists[key].pop(0) if self.lists[key] else None

  def rpush(self, key, value):
    self.lists[key].append(value)

  def close(self):
    pass


@pytest.mark.unit
def test_retry_requeues_sealed_entries_and_drops_unsealed_ones():
  task = {
    "task_id": "op_1",
    "task_type": "graph_materialization",
    "graph_id": "kg1",
    "user_id": "usr_1",
    "attempt": 3,
    "params": {},
  }
  forged = {**task, "task_id": "op_2"}
  queue = _Lists([dump_task(task), json.dumps(forged)])

  with patch(
    "robosystems.config.valkey_registry.create_redis_client", return_value=queue
  ):
    result = CliRunner().invoke(dlq, ["retry", "--yes"], obj=None)

  assert result.exit_code == 0, result.output
  requeued = [json.loads(t) for t in queue.lists["worker:tasks"]]
  assert [(t["task_id"], t["attempt"]) for t in requeued] == [("op_1", 1)]
  assert task_is_sealed(requeued[0])
