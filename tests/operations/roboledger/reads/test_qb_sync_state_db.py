"""The close sync gate counts only QuickBooks connections that still feed the ledger."""

from datetime import UTC, datetime

import pytest

from robosystems.models.core.connection.connection import Connection, ConnectionStatus
from robosystems.operations.roboledger.reads.fiscal_calendar import qb_sync_state

pytestmark = pytest.mark.unit


def _connection(test_db, graph_id, user_id, status, *, deleted=False):
  conn = Connection.create(
    graph_id=graph_id,
    user_id=user_id,
    provider="quickbooks",
    session=test_db,
    status=status,
  )
  conn.last_sync = datetime(2026, 1, 5, tzinfo=UTC)
  if deleted:
    conn.deleted_at = datetime.now(UTC)
  test_db.commit()
  return conn


@pytest.mark.parametrize(
  ("status", "deleted"),
  [(ConnectionStatus.SEVERED.value, True), (ConnectionStatus.DISCONNECTED.value, True)],
)
def test_ended_connection_does_not_gate_close(
  test_db, test_user, sample_graph, status, deleted
):
  _connection(test_db, sample_graph.graph_id, test_user.id, status, deleted=deleted)

  assert qb_sync_state(test_db, sample_graph.graph_id) == (False, None)


def test_live_connection_still_gates_close(test_db, test_user, sample_graph):
  _connection(
    test_db, sample_graph.graph_id, test_user.id, ConnectionStatus.CONNECTED.value
  )

  has_connection, last_sync = qb_sync_state(test_db, sample_graph.graph_id)
  assert has_connection is True
  assert last_sync is not None
