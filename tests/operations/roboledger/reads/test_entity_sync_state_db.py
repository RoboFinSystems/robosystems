"""The close's sync gate reads every source that books for the entity: the
QuickBooks connection for the group parent, and each bank feed with an account
on the entity's own chart."""

from datetime import datetime

import pytest

from robosystems.models.core.connection.connection import Connection, ConnectionStatus
from robosystems.models.extensions.element import Element
from robosystems.operations.roboledger.reads.fiscal_calendar import entity_sync_state
from tests.ledger_entity import entity_account

pytestmark = pytest.mark.unit

# `last_sync` is a naive column.
EARLY = datetime(2026, 9, 2)
LATE = datetime(2026, 10, 7)


def _connection(test_db, graph_id, user_id, provider, *, last_sync, status=None):
  conn = Connection.create(
    graph_id=graph_id,
    user_id=user_id,
    provider=provider,
    session=test_db,
    status=status or ConnectionStatus.CONNECTED.value,
  )
  conn.last_sync = last_sync
  test_db.commit()
  return conn


def _feed_account(session, entity_id, name, connection_id):
  element = session.get(Element, entity_account(session, entity_id, name))
  element.metadata_ = {
    "bank_feed": {"provider": "plaid", "connection_id": connection_id}
  }
  session.flush()


def test_a_subsidiary_on_a_feed_waits_on_that_feed(
  two_entities, test_db, test_user, sample_graph
):
  t = two_entities
  feed = _connection(
    test_db, sample_graph.graph_id, test_user.id, "plaid", last_sync=EARLY
  )
  _feed_account(t.session, str(t.sub.id), "Checking", str(feed.id))

  assert entity_sync_state(
    t.session, test_db, sample_graph.graph_id, str(t.sub.id)
  ) == (True, EARLY)
  # The feed is the subsidiary's, not the parent's.
  assert entity_sync_state(
    t.session, test_db, sample_graph.graph_id, str(t.parent.id)
  ) == (False, None)


def test_a_feed_never_synced_holds_the_entity_stale(
  two_entities, test_db, test_user, sample_graph
):
  t = two_entities
  feed = _connection(
    test_db, sample_graph.graph_id, test_user.id, "plaid", last_sync=None
  )
  _feed_account(t.session, str(t.sub.id), "Checking", str(feed.id))

  assert entity_sync_state(
    t.session, test_db, sample_graph.graph_id, str(t.sub.id)
  ) == (True, None)


def test_the_parent_waits_on_the_stalest_of_quickbooks_and_its_feeds(
  two_entities, test_db, test_user, sample_graph
):
  t = two_entities
  _connection(
    test_db, sample_graph.graph_id, test_user.id, "quickbooks", last_sync=LATE
  )
  feed = _connection(
    test_db, sample_graph.graph_id, test_user.id, "plaid", last_sync=EARLY
  )
  _feed_account(t.session, str(t.parent.id), "Savings", str(feed.id))

  assert entity_sync_state(
    t.session, test_db, sample_graph.graph_id, str(t.parent.id)
  ) == (True, EARLY)


def test_a_disconnected_feed_no_longer_gates(
  two_entities, test_db, test_user, sample_graph
):
  t = two_entities
  feed = _connection(
    test_db,
    sample_graph.graph_id,
    test_user.id,
    "plaid",
    last_sync=EARLY,
    status=ConnectionStatus.DISCONNECTED.value,
  )
  _feed_account(t.session, str(t.sub.id), "Checking", str(feed.id))

  assert entity_sync_state(
    t.session, test_db, sample_graph.graph_id, str(t.sub.id)
  ) == (False, None)
