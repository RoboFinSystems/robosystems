"""The close sync gate counts only the connection that still keeps the entity's books."""

from datetime import UTC, datetime

import pytest

from robosystems.models.core.connection.connection import Connection, ConnectionStatus
from robosystems.operations.roboledger.reads.fiscal_calendar import keeper_sync_state

pytestmark = pytest.mark.unit

ENTITY = "ent_parent"


def _connection(test_db, graph_id, user_id, status, *, deleted=False):
  conn = Connection.create(
    graph_id=graph_id,
    user_id=user_id,
    provider="quickbooks",
    session=test_db,
    status=status,
    entity_id=ENTITY,
  )
  conn.keeps_books = True
  conn.last_sync = datetime(2026, 1, 5, tzinfo=UTC)
  if deleted:
    conn.deleted_at = datetime.now(UTC)
  test_db.commit()
  return conn


@pytest.mark.parametrize(
  ("status", "deleted"),
  [
    # Severed but not yet soft-deleted: sever and delete are separate steps.
    (ConnectionStatus.SEVERED.value, False),
    (ConnectionStatus.SEVERED.value, True),
    (ConnectionStatus.DISCONNECTED.value, True),
  ],
)
def test_ended_connection_does_not_gate_close(
  test_db, test_user, sample_graph, status, deleted
):
  _connection(test_db, sample_graph.graph_id, test_user.id, status, deleted=deleted)

  assert keeper_sync_state(test_db, sample_graph.graph_id, ENTITY) == (False, None)


def test_live_connection_still_gates_close(test_db, test_user, sample_graph):
  _connection(
    test_db, sample_graph.graph_id, test_user.id, ConnectionStatus.CONNECTED.value
  )

  has_connection, last_sync = keeper_sync_state(test_db, sample_graph.graph_id, ENTITY)
  assert has_connection is True
  assert last_sync is not None
