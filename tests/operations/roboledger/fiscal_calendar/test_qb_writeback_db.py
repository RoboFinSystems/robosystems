"""One live QuickBooks row decides both where the close publishes and whether
it shadows, against the platform database: the filters live in SQL, so the
mock-based tests beside this file cannot see them."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from robosystems.models.core.connection.connection import Connection, ConnectionStatus
from robosystems.operations.roboledger.fiscal_calendar.qb_writeback import (
  live_quickbooks_connection,
  resolve_writeback_connection,
  shadow_ledger,
)

pytestmark = pytest.mark.unit


def _connection(test_db, graph_id, user_id, status, policy, *, created_offset=0):
  conn = Connection.create(
    graph_id=graph_id,
    user_id=user_id,
    provider="quickbooks",
    session=test_db,
    status=status,
    write_policy=policy,
  )
  # Rows are created within the same second; order them explicitly.
  conn.created_at = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(days=created_offset)
  test_db.commit()
  return conn


def test_a_pending_reconnect_never_masks_the_live_shadow_row(
  test_db, test_user, sample_graph
):
  """A connect attempt still in OAuth carries the provider default and no
  credentials. Newest by creation, it used to be read as the ledger's
  policy, turning a shadow ledger into write-back for the next close."""
  live = _connection(
    test_db,
    sample_graph.graph_id,
    test_user.id,
    ConnectionStatus.CONNECTED.value,
    "shadow",
  )
  _connection(
    test_db,
    sample_graph.graph_id,
    test_user.id,
    ConnectionStatus.PENDING_OAUTH.value,
    "qb_authoritative",
    created_offset=1,
  )

  picked = live_quickbooks_connection(test_db, sample_graph.graph_id)
  assert picked is not None and picked.id == live.id
  assert shadow_ledger(test_db, sample_graph.graph_id) is True
  assert resolve_writeback_connection(test_db, sample_graph.graph_id) is None


def test_a_pending_reconnect_is_never_the_publish_target(
  test_db, test_user, sample_graph
):
  live = _connection(
    test_db,
    sample_graph.graph_id,
    test_user.id,
    ConnectionStatus.CONNECTED.value,
    "qb_authoritative",
  )
  _connection(
    test_db,
    sample_graph.graph_id,
    test_user.id,
    ConnectionStatus.PENDING_OAUTH.value,
    "qb_authoritative",
    created_offset=1,
  )

  target = resolve_writeback_connection(test_db, sample_graph.graph_id)
  assert target is not None and target.connection_id == live.id


@pytest.mark.parametrize(
  "status",
  [
    ConnectionStatus.PENDING_OAUTH.value,
    ConnectionStatus.DISCONNECTED.value,
    ConnectionStatus.SEVERED.value,
  ],
)
def test_a_row_that_is_no_connection_resolves_nothing(
  test_db, test_user, sample_graph, status
):
  _connection(test_db, sample_graph.graph_id, test_user.id, status, "qb_authoritative")

  assert live_quickbooks_connection(test_db, sample_graph.graph_id) is None
  assert resolve_writeback_connection(test_db, sample_graph.graph_id) is None
  assert shadow_ledger(test_db, sample_graph.graph_id) is False


def test_a_connection_needing_reauth_is_still_the_publish_target(
  test_db, test_user, sample_graph
):
  """Its credentials exist and may fail loudly at publish. Resolving to no
  target instead would post the drafts locally beside QuickBooks' books."""
  conn = _connection(
    test_db,
    sample_graph.graph_id,
    test_user.id,
    ConnectionStatus.NEEDS_REAUTH.value,
    "qb_authoritative",
  )

  target = resolve_writeback_connection(test_db, sample_graph.graph_id)
  assert target is not None and target.connection_id == conn.id
