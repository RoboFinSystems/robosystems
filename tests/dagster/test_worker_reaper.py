"""The reaper's DLQ path writes the terminal status straight into the SSE
metadata key, bypassing the store's eviction hook — so it must evict the
idempotency envelope itself."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from robosystems.dagster.sensors.worker_reaper import _fail_operation_sync


def test_dlq_failure_evicts_the_idempotency_envelope():
  sse = MagicMock()
  sse.get.return_value = json.dumps({"status": "running"})
  with patch(
    "robosystems.middleware.operations.invalidate_operation_idempotency_sync"
  ) as evict:
    _fail_operation_sync(sse, "op_123", attempts=3)
  written = json.loads(sse.set.call_args.args[1])
  assert written["status"] == "failed"
  evict.assert_called_once_with("op_123")


def test_missing_metadata_evicts_nothing():
  sse = MagicMock()
  sse.get.return_value = None
  with patch(
    "robosystems.middleware.operations.invalidate_operation_idempotency_sync"
  ) as evict:
    _fail_operation_sync(sse, "op_missing", attempts=3)
  evict.assert_not_called()


def test_metadata_write_failure_does_not_evict():
  """If the status write fails the operation is not terminal yet; leaving the
  envelope in place is the honest state."""
  sse = MagicMock()
  sse.get.return_value = json.dumps({"status": "running"})
  sse.set.side_effect = RuntimeError("valkey down")
  with patch(
    "robosystems.middleware.operations.invalidate_operation_idempotency_sync"
  ) as evict:
    _fail_operation_sync(sse, "op_x", attempts=3)
  evict.assert_not_called()
