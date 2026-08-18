"""Unit tests for the closed-period write guard."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from robosystems.operations.roboledger.commands._guards import (
  ClosedPeriodError,
  assert_period_not_closed,
)


def _row(graph_id="kgabc", name="2026-01", status="open"):
  row = MagicMock()
  row.graph_id = graph_id
  row.name = name
  row.status = status
  return row


class TestAssertPeriodNotClosed:
  def test_no_period_is_a_noop(self):
    session = MagicMock()
    session.execute.return_value.fetchone.return_value = None
    assert_period_not_closed(session, date(2026, 1, 15))

  def test_open_period_takes_the_shared_fence(self):
    session = MagicMock()
    session.execute.return_value.fetchone.return_value = _row()
    with patch(
      "robosystems.operations.roboledger.commands._guards.acquire_shared_period_fence"
    ) as fence:
      assert_period_not_closed(session, date(2026, 1, 15))
    fence.assert_called_once()
    assert fence.call_args.args[1] == "kgabc"
    assert fence.call_args.args[2] == "2026-01"

  def test_closed_period_raises_after_the_fence(self):
    session = MagicMock()
    session.execute.return_value.fetchone.return_value = _row(status="closed")
    with (
      patch(
        "robosystems.operations.roboledger.commands._guards.acquire_shared_period_fence"
      ) as fence,
      pytest.raises(ClosedPeriodError) as exc,
    ):
      assert_period_not_closed(session, date(2026, 1, 15))
    fence.assert_called_once()
    assert exc.value.period_name == "2026-01"

  def test_two_dates_lock_in_sorted_order(self):
    session = MagicMock()
    jan = _row(name="2026-01")
    feb = _row(name="2026-02")
    session.execute.return_value.fetchone.side_effect = [
      feb,
      jan,  # discovery (input order is Feb, Jan)
      jan,
      feb,  # re-read after each lock, sorted
    ]
    order: list[str] = []

    def _fence(_session, _graph, period, *, detail):
      order.append(period)

    with patch(
      "robosystems.operations.roboledger.commands._guards.acquire_shared_period_fence",
      side_effect=_fence,
    ):
      assert_period_not_closed(session, date(2026, 2, 1), date(2026, 1, 15))
    assert order == ["2026-01", "2026-02"]
