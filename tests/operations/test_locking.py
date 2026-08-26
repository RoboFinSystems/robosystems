"""Unit tests for the period-write fence and lock-connection hygiene."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import OperationalError

from robosystems.operations.locking import (
  RowLockedError,
  exclusive_period_fence,
  period_fence_ident,
)


def _pg_error(pgcode: str) -> OperationalError:
  orig = MagicMock()
  orig.pgcode = pgcode
  return OperationalError("lock", {}, orig)


class TestExclusivePeriodFence:
  def _connect(self, mock_engine):
    conn = MagicMock()
    mock_engine.return_value.connect.return_value = conn
    return conn

  @patch("robosystems.db.extensions.get_extensions_engine")
  def test_unlocks_after_the_body(self, mock_engine):
    conn = self._connect(mock_engine)
    with exclusive_period_fence("kgabc", "2026-01", detail="held"):
      pass
    sql = " ".join(str(call.args[0]) for call in conn.execute.call_args_list)
    assert "pg_advisory_lock" in sql
    assert "pg_advisory_unlock" in sql
    conn.close.assert_called_once()
    conn.invalidate.assert_not_called()

  @patch("robosystems.db.extensions.get_extensions_engine")
  def test_unlocks_when_the_body_raises(self, mock_engine):
    conn = self._connect(mock_engine)
    with pytest.raises(RuntimeError, match="boom"):
      with exclusive_period_fence("kgabc", "2026-01", detail="held"):
        raise RuntimeError("boom")
    sql = " ".join(str(call.args[0]) for call in conn.execute.call_args_list)
    assert "pg_advisory_unlock" in sql
    conn.close.assert_called_once()

  @patch("robosystems.db.extensions.get_extensions_engine")
  def test_timeout_is_row_locked_and_does_not_unlock(self, mock_engine):
    conn = self._connect(mock_engine)
    conn.execute.side_effect = [None, _pg_error("55P03")]
    with pytest.raises(RowLockedError, match="held"):
      with exclusive_period_fence("kgabc", "2026-01", detail="held"):
        pass
    sql = " ".join(
      str(call.args[0]) for call in conn.execute.call_args_list if call.args
    )
    assert "pg_advisory_unlock" not in sql
    conn.close.assert_called_once()

  @patch("robosystems.db.extensions.get_extensions_engine")
  def test_unlock_failure_invalidates_the_connection(self, mock_engine):
    conn = self._connect(mock_engine)

    def _execute(statement, *args, **kwargs):
      if "pg_advisory_unlock" in str(statement):
        raise RuntimeError("unlock failed")
      return MagicMock()

    conn.execute.side_effect = _execute
    with exclusive_period_fence("kgabc", "2026-01", detail="held"):
      pass
    conn.invalidate.assert_called_once()
    conn.close.assert_called_once()


def test_period_fence_ident_is_graph_and_period():
  assert period_fence_ident("kg1", "2026-01") == "kg1:2026-01"


class TestFenceWait:
  """Request handlers do not wait; background jobs do. The fence takes the
  wait from its caller so the worker close can pass its own budget."""

  def _connect(self, mock_engine):
    conn = MagicMock()
    mock_engine.return_value.connect.return_value = conn
    return conn

  def _set_statements(self, conn):
    return [
      str(call.args[0])
      for call in conn.execute.call_args_list
      if call.args and "lock_timeout" in str(call.args[0])
    ]

  @patch("robosystems.db.extensions.get_extensions_engine")
  def test_the_default_is_the_request_wait(self, mock_engine):
    from robosystems.operations.locking import _LOCK_TIMEOUT_MS

    conn = self._connect(mock_engine)
    with exclusive_period_fence("kgabc", "2026-01", detail="held"):
      pass
    assert self._set_statements(conn)[0] == f"SET lock_timeout = '{_LOCK_TIMEOUT_MS}ms'"

  @patch("robosystems.db.extensions.get_extensions_engine")
  def test_a_background_closer_waits_as_long_as_it_says(self, mock_engine):
    conn = self._connect(mock_engine)
    with exclusive_period_fence("kgabc", "2026-01", detail="held", wait_ms=600_000):
      pass
    statements = self._set_statements(conn)
    assert statements[0] == "SET lock_timeout = '600000ms'"
    # The longer wait is still scrubbed before the connection goes back.
    assert "RESET lock_timeout" in statements[-1]
