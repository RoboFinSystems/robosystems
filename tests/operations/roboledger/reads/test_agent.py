"""Tests for agent read operations."""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

from robosystems.models.extensions.roboledger.agent import Agent
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.transaction import Transaction
from robosystems.operations.roboledger.reads.agent import get_agent_activity


def _scalar_rows_result(rows):
  result = MagicMock()
  result.scalars.return_value.all.return_value = rows
  return result


def _scalar_count_result(count: int):
  result = MagicMock()
  result.scalar_one.return_value = count
  return result


def test_get_agent_activity_counts_all_transactions_for_agent() -> None:
  session = MagicMock()
  session.get.return_value = Agent(
    id="agt_vendor",
    agent_type="vendor",
    name="Vendor Co",
    source="native",
    is_active=True,
    is_1099_recipient=False,
    created_by="usr_test",
  )

  recent_events = [
    Event(
      id="evt_recent_1",
      event_type="bill_recorded",
      event_category="purchase",
      occurred_at=datetime(2026, 4, 2, tzinfo=UTC),
      source="native",
      created_by="usr_test",
      agent_id="agt_vendor",
    ),
    Event(
      id="evt_recent_2",
      event_type="payment_recorded",
      event_category="purchase",
      occurred_at=datetime(2026, 4, 1, tzinfo=UTC),
      source="native",
      created_by="usr_test",
      agent_id="agt_vendor",
    ),
  ]
  recent_txn = Transaction(
    id="txn_recent_1",
    type="bill_recorded",
    amount=2500,
    currency="USD",
    date=date(2026, 4, 2),
    source="native",
    status="pending",
    created_by="usr_test",
    triggered_by_event_id="evt_recent_1",
  )

  session.execute.side_effect = [
    _scalar_rows_result(recent_events),
    _scalar_rows_result([recent_txn]),
    _scalar_count_result(5),
    _scalar_count_result(42),
  ]

  with patch(
    "robosystems.operations.roboledger.reads.event_block.list_event_blocks",
    return_value=[],
  ):
    activity = get_agent_activity(session, "agt_vendor", limit=2)

  assert activity is not None
  assert activity.transaction_count == 42
  assert len(activity.recent_transactions) == 1

  count_stmt = str(session.execute.call_args_list[3].args[0])
  assert "JOIN events" in count_stmt
  assert "events.agent_id" in count_stmt
