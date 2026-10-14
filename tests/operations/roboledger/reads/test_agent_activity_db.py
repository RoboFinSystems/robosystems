"""A counterparty's activity is read per entity, on real Postgres: the group
shares the agent; each entity's events with it, and the transactions they
triggered, are its own. Omitted, the scope is the group parent's."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from robosystems.models.extensions.roboledger.agent import Agent
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.transaction import Transaction
from robosystems.operations.roboledger.reads.agent import get_agent_activity

pytestmark = pytest.mark.unit


def _dealing(t, entity, *, day, amount):
  event = Event(
    entity_id=entity.id,
    event_type="bank_transaction",
    event_category="purchase",
    event_class="economic",
    agent_id=t.vendor.id,
    occurred_at=datetime(2026, 10, day, tzinfo=UTC),
    status="committed",
    source="plaid",
    amount=amount,
    created_by="test",
  )
  t.session.add(event)
  t.session.flush()
  t.session.add(
    Transaction(
      entity_id=entity.id,
      type="bank_transaction",
      amount=-amount,
      date=date(2026, 10, day),
      status="posted",
      triggered_by_event_id=event.id,
      created_by="test",
    )
  )
  t.session.flush()
  return event.id


@pytest.fixture()
def vendor(two_entities):
  t = two_entities
  t.vendor = Agent(
    id="agt_gusto",
    agent_type="vendor",
    name="Gusto",
    source="native",
    created_by="test",
  )
  t.session.add(t.vendor)
  t.session.flush()
  t.parent_events = [
    _dealing(t, t.parent, day=2, amount=-120_000),
    _dealing(t, t.parent, day=5, amount=-120_000),
  ]
  t.sub_events = [_dealing(t, t.sub, day=3, amount=-40_000)]
  t.session.commit()
  return t


def test_omitted_the_scope_is_the_group_parents(vendor):
  activity = get_agent_activity(vendor.session, vendor.vendor.id)

  assert activity is not None
  assert activity.entity_id == vendor.parent.id
  assert (activity.event_count, activity.transaction_count) == (2, 2)
  assert {e["id"] for e in activity.recent_events} == set(vendor.parent_events)
  assert {t.triggered_by_event_id for t in activity.recent_transactions} == set(
    vendor.parent_events
  )


def test_a_subsidiary_reads_its_own_dealings(vendor):
  activity = get_agent_activity(
    vendor.session, vendor.vendor.id, entity_id=vendor.sub.id
  )

  assert activity is not None
  assert activity.entity_id == vendor.sub.id
  assert (activity.event_count, activity.transaction_count) == (1, 1)
  assert [e["id"] for e in activity.recent_events] == vendor.sub_events
  assert [t.amount for t in activity.recent_transactions] == [40_000]
