"""An external source registered as a ledger keeps its entity's books in
shadow, against the platform database."""

from __future__ import annotations

import pytest

from robosystems.models.api.graphs.connections import ExternalConnectionConfig
from robosystems.operations.connection_service import ConnectionService
from robosystems.operations.providers.external_provider import (
  create_external_connection,
)
from robosystems.operations.roboledger.fiscal_calendar.qb_writeback import (
  books_keeper,
  keeper_event_source,
  resolve_writeback_connection,
  shadow_close_for_entity,
)

pytestmark = pytest.mark.unit

SUBSIDIARY = "ent_subsidiary"


@pytest.mark.asyncio
async def test_a_ledger_source_keeps_its_entitys_books_in_shadow(
  test_db, test_user, sample_graph
):
  graph_id = sample_graph.graph_id
  await create_external_connection(
    None,
    ExternalConnectionConfig(
      source_name="netsuite-uk", ledger=True, entity_id=SUBSIDIARY
    ),
    str(test_user.id),
    graph_id,
    test_db,
  )

  keeper = books_keeper(test_db, graph_id, SUBSIDIARY)
  assert keeper is not None
  assert keeper.write_policy == "shadow"
  assert keeper_event_source(keeper) == "netsuite-uk"
  assert shadow_close_for_entity(test_db, graph_id, SUBSIDIARY) is True
  assert resolve_writeback_connection(test_db, graph_id, SUBSIDIARY) is None


@pytest.mark.asyncio
async def test_a_plain_source_keeps_nobodys_books(test_db, test_user, sample_graph):
  graph_id = sample_graph.graph_id
  connection_id = await create_external_connection(
    None,
    ExternalConnectionConfig(source_name="billing-system"),
    str(test_user.id),
    graph_id,
    test_db,
  )

  connection = await ConnectionService.get_connection(
    connection_id, str(test_user.id), graph_id=graph_id
  )
  assert connection is not None
  assert connection["keeps_books"] is False
  assert connection["write_policy"] == "native"
  assert connection.get("entity_id") is None


@pytest.mark.asyncio
async def test_it_cannot_be_turned_to_write_back(test_db, test_user, sample_graph):
  graph_id = sample_graph.graph_id
  connection_id = await create_external_connection(
    None,
    ExternalConnectionConfig(
      source_name="netsuite-uk", ledger=True, entity_id=SUBSIDIARY
    ),
    str(test_user.id),
    graph_id,
    test_db,
  )

  with pytest.raises(ValueError, match="written back"):
    await ConnectionService.set_write_policy(
      connection_id, "qb_authoritative", str(test_user.id), graph_id, test_db
    )


def test_an_entity_without_a_ledger_flag_is_refused():
  with pytest.raises(ValueError, match="set ledger"):
    ExternalConnectionConfig(source_name="netsuite-uk", entity_id=SUBSIDIARY)
