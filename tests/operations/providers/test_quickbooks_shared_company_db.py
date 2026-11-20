"""Which connections share a QuickBooks company with another graph.

Runs against the real platform test database: the answer is a join over
connections and their credentials.
"""

from datetime import UTC, datetime

import pytest

from robosystems.models.core import Connection, ConnectionCredentials
from robosystems.models.core.graph.graph import Graph
from robosystems.operations.providers.quickbooks_provider import (
  _company_live_on_another_graph,
)

pytestmark = pytest.mark.integration

REALM = "9130000000000001"


def _graph(test_db, parent, suffix: str) -> Graph:
  return Graph.create(
    graph_id=f"kg{suffix * 16}",
    org_id=parent.org_id,
    graph_name=f"graph {suffix}",
    graph_type="generic",
    session=test_db,
  )


def _connection(test_db, graph_id, user, *, realm=REALM, with_token=True) -> Connection:
  connection = Connection.create(
    graph_id=graph_id,
    user_id=user.id,
    provider="quickbooks",
    session=test_db,
    status="connected",
    realm_id=realm,
  )
  if with_token:
    # The row is enough: the lookup never decrypts.
    test_db.add(
      ConnectionCredentials(
        connection_id=connection.id,
        provider="quickbooks",
        user_id=user.id,
        encrypted_credentials="sealed",
        is_active=True,
      )
    )
    test_db.commit()
  return connection


def test_another_graph_on_the_same_company_is_named(test_db, sample_graph, test_user):
  mine = _connection(test_db, sample_graph.graph_id, test_user)
  other = _graph(test_db, sample_graph, "a")
  _connection(test_db, other.graph_id, test_user)

  assert (
    _company_live_on_another_graph(mine.id, sample_graph.graph_id, test_db)
    == other.graph_id
  )


def test_a_connection_alone_on_its_company_has_none(test_db, sample_graph, test_user):
  mine = _connection(test_db, sample_graph.graph_id, test_user)
  other = _graph(test_db, sample_graph, "b")
  _connection(test_db, other.graph_id, test_user, realm="9130000000000002")

  assert _company_live_on_another_graph(mine.id, sample_graph.graph_id, test_db) is None


def test_a_disconnected_or_tokenless_sibling_does_not_count(
  test_db, sample_graph, test_user
):
  mine = _connection(test_db, sample_graph.graph_id, test_user)
  gone = _connection(test_db, _graph(test_db, sample_graph, "c").graph_id, test_user)
  gone.deleted_at = datetime.now(UTC)
  test_db.commit()
  _connection(
    test_db, _graph(test_db, sample_graph, "d").graph_id, test_user, with_token=False
  )

  assert _company_live_on_another_graph(mine.id, sample_graph.graph_id, test_db) is None
