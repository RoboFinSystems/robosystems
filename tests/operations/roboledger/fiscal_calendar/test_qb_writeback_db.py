"""One row, the entity's books keeper, decides both where its close publishes
and whether it shadows, against the platform database: the filters live in
SQL, so the mock-based tests beside this file cannot see them."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from robosystems.models.core.connection.connection import Connection, ConnectionStatus
from robosystems.operations.roboledger.fiscal_calendar.qb_writeback import (
  books_keeper,
  resolve_writeback_connection,
  shadow_close_for_entity,
)

pytestmark = pytest.mark.unit

PARENT = "ent_parent"
SUBSIDIARY = "ent_subsidiary"


def _connection(
  test_db,
  graph_id,
  user_id,
  status,
  policy,
  *,
  created_offset=0,
  entity_id=PARENT,
  keeper=None,
):
  """A QuickBooks row for ``entity_id``. It keeps the books once past OAuth,
  as the callback makes it, unless ``keeper`` says otherwise."""
  conn = Connection.create(
    graph_id=graph_id,
    user_id=user_id,
    provider="quickbooks",
    session=test_db,
    status=status,
    write_policy=policy,
    entity_id=entity_id,
  )
  # Rows are created within the same second; order them explicitly.
  conn.created_at = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(days=created_offset)
  test_db.commit()
  if status != ConnectionStatus.PENDING_OAUTH.value if keeper is None else keeper:
    conn.become_keeper(test_db)
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

  picked = books_keeper(test_db, sample_graph.graph_id, PARENT)
  assert picked is not None and picked.id == live.id
  assert shadow_close_for_entity(test_db, sample_graph.graph_id, PARENT) is True
  assert resolve_writeback_connection(test_db, sample_graph.graph_id, PARENT) is None


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

  target = resolve_writeback_connection(test_db, sample_graph.graph_id, PARENT)
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
  _connection(
    test_db,
    sample_graph.graph_id,
    test_user.id,
    status,
    "qb_authoritative",
    keeper=True,
  )

  assert books_keeper(test_db, sample_graph.graph_id, PARENT) is None
  assert resolve_writeback_connection(test_db, sample_graph.graph_id, PARENT) is None
  assert shadow_close_for_entity(test_db, sample_graph.graph_id, PARENT) is False


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

  target = resolve_writeback_connection(test_db, sample_graph.graph_id, PARENT)
  assert target is not None and target.connection_id == conn.id


def test_the_posture_is_the_entitys_own(test_db, test_user, sample_graph):
  """The parent's shadow keeper says nothing about a subsidiary, whose books
  have no keeper and are native; and a disconnected keeper is no keeper."""
  graph_id = sample_graph.graph_id
  conn = _connection(
    test_db, graph_id, test_user.id, ConnectionStatus.CONNECTED.value, "shadow"
  )

  assert shadow_close_for_entity(test_db, graph_id, PARENT) is True
  assert shadow_close_for_entity(test_db, graph_id, SUBSIDIARY) is False
  assert books_keeper(test_db, graph_id, SUBSIDIARY) is None

  conn.soft_delete(test_db)
  assert books_keeper(test_db, graph_id, PARENT) is None


def test_the_newest_authorization_takes_the_books(test_db, test_user, sample_graph):
  """Two live rows for one entity: the one authorized last is the keeper,
  and the flag moves rather than the index refusing the second."""
  graph_id = sample_graph.graph_id
  first = _connection(
    test_db, graph_id, test_user.id, ConnectionStatus.CONNECTED.value, "shadow"
  )
  second = _connection(
    test_db,
    graph_id,
    test_user.id,
    ConnectionStatus.CONNECTED.value,
    "qb_authoritative",
    created_offset=1,
  )

  test_db.refresh(first)
  assert (first.keeps_books, second.keeps_books) == (False, True)
  target = resolve_writeback_connection(test_db, graph_id, PARENT)
  assert target is not None and target.connection_id == second.id
  assert shadow_close_for_entity(test_db, graph_id, PARENT) is False


def test_a_revived_connection_meets_no_second_keeper(test_db, test_user, sample_graph):
  """A disconnected keeper still carries the flag, out of the index's sight.
  Another connection taking the books clears it, so reviving the first
  cannot put two keepers on the entity; it takes the books back when it
  authorizes again."""
  graph_id = sample_graph.graph_id
  first = _connection(
    test_db, graph_id, test_user.id, ConnectionStatus.CONNECTED.value, "shadow"
  )
  first.soft_delete(test_db)
  second = _connection(
    test_db,
    graph_id,
    test_user.id,
    ConnectionStatus.CONNECTED.value,
    "qb_authoritative",
    created_offset=1,
  )

  first.restore(test_db)
  assert (first.keeps_books, second.keeps_books) == (False, True)
  kept = books_keeper(test_db, graph_id, PARENT)
  assert kept is not None and kept.id == second.id

  first.become_keeper(test_db)
  test_db.refresh(second)
  kept = books_keeper(test_db, graph_id, PARENT)
  assert kept is not None and kept.id == first.id
  assert second.keeps_books is False
