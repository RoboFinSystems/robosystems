"""Deleting a subgraph releases what it holds outside its database first.

A connection, soft-deleted or not, references the subgraph's Graph row by FK,
so deleting the row without releasing it fails after the database is already
gone. Runs against the real platform test database.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.exc import IntegrityError

from robosystems.models.core.connection.connection import Connection
from robosystems.models.core.graph.graph import Graph
from robosystems.operations.graph.deprovision_service import GraphDeprovisionService

pytestmark = pytest.mark.integration


def _subgraph_with_a_deleted_connection(test_db, parent, user):
  subgraph = Graph.create(
    graph_id=f"{parent.graph_id}_dev",
    org_id=parent.org_id,
    graph_name="dev",
    graph_type="generic",
    session=test_db,
    parent_graph_id=parent.graph_id,
    subgraph_index=1,
    subgraph_name="dev",
    is_subgraph=True,
  )
  connection = Connection.create(
    graph_id=subgraph.graph_id,
    user_id=user.id,
    provider="external",
    session=test_db,
  )
  connection.deleted_at = datetime.now(UTC)
  test_db.commit()
  return subgraph


@pytest.mark.asyncio
async def test_released_subgraph_row_deletes(test_db, sample_graph, test_user):
  subgraph = _subgraph_with_a_deleted_connection(test_db, sample_graph, test_user)
  subgraph_id = subgraph.graph_id

  with patch(
    "robosystems.operations.providers.registry.provider_registry.cleanup_connection",
    new=AsyncMock(),
  ):
    await GraphDeprovisionService("test").release_subgraph_records(subgraph_id, test_db)
  subgraph.delete(test_db)

  assert test_db.query(Graph).filter(Graph.graph_id == subgraph_id).first() is None
  assert (
    test_db.query(Connection).filter(Connection.graph_id == subgraph_id).count() == 0
  )


def test_an_unreleased_subgraph_row_is_held_by_its_connection(
  test_db, sample_graph, test_user
):
  """The failure the release prevents: the row cannot go while a connection
  row, even a soft-deleted one, still references it."""
  subgraph = _subgraph_with_a_deleted_connection(test_db, sample_graph, test_user)
  with pytest.raises(IntegrityError):
    subgraph.delete(test_db)
