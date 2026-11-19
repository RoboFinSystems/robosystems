"""An external source registered as a ledger keeps its entity's books in
shadow, against the platform database."""

from __future__ import annotations

import pytest

from robosystems.models.api.graphs.connections import ExternalConnectionConfig
from robosystems.models.core.connection.connection import Connection
from robosystems.operations.connection_service import (
  ConnectionService,
  report_external_sync,
)
from robosystems.operations.providers.external_provider import (
  create_external_connection,
)
from robosystems.operations.roboledger.fiscal_calendar.qb_writeback import (
  books_keeper,
  keeper_event_source,
  resolve_writeback_connection,
  shadow_close_for_entity,
)
from robosystems.operations.roboledger.reads.fiscal_calendar import keeper_sync_state

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


async def _ledger(test_db, test_user, graph_id):
  return await create_external_connection(
    None,
    ExternalConnectionConfig(
      source_name="netsuite-uk", ledger=True, entity_id=SUBSIDIARY
    ),
    str(test_user.id),
    graph_id,
    test_db,
  )


@pytest.mark.asyncio
async def test_a_reported_run_is_what_the_close_reads_as_fresh(
  test_db, test_user, sample_graph
):
  graph_id = sample_graph.graph_id
  connection_id = await _ledger(test_db, test_user, graph_id)
  assert keeper_sync_state(test_db, graph_id, SUBSIDIARY) == (True, None)

  reported = await report_external_sync(
    connection_id,
    graph_id,
    succeeded=True,
    counts={"entries_sent": 12},
    message="October",
    db_session=test_db,
  )

  has_keeper, last_sync = keeper_sync_state(test_db, graph_id, SUBSIDIARY)
  assert has_keeper is True
  assert last_sync is not None
  assert reported is not None
  result = reported["metadata"]["last_sync_result"]
  assert result["status"] == "succeeded"
  assert result["reported"] is True
  assert result["counts"] == {"entries_sent": 12}


@pytest.mark.asyncio
async def test_a_failed_run_leaves_the_last_good_one_standing(
  test_db, test_user, sample_graph
):
  graph_id = sample_graph.graph_id
  connection_id = await _ledger(test_db, test_user, graph_id)
  await report_external_sync(
    connection_id, graph_id, succeeded=True, counts={}, message=None, db_session=test_db
  )
  _, good = keeper_sync_state(test_db, graph_id, SUBSIDIARY)

  failed = await report_external_sync(
    connection_id,
    graph_id,
    succeeded=False,
    counts={},
    message="source timed out",
    db_session=test_db,
  )

  assert keeper_sync_state(test_db, graph_id, SUBSIDIARY) == (True, good)
  assert failed is not None
  assert failed["metadata"]["last_sync_result"]["status"] == "failed"


@pytest.mark.asyncio
async def test_only_an_external_source_reports_its_own_run(
  test_db, test_user, sample_graph
):
  graph_id = sample_graph.graph_id
  quickbooks = Connection.create(
    graph_id=graph_id,
    user_id=str(test_user.id),
    provider="quickbooks",
    session=test_db,
    status="connected",
    entity_id="ent_parent",
  )
  with pytest.raises(ValueError, match="only an external source"):
    await report_external_sync(
      str(quickbooks.id),
      graph_id,
      succeeded=True,
      counts={},
      message=None,
      db_session=test_db,
    )
  assert quickbooks.last_sync is None


@pytest.mark.asyncio
async def test_a_connection_on_another_graph_is_not_found(
  test_db, test_user, sample_graph
):
  connection_id = await _ledger(test_db, test_user, sample_graph.graph_id)
  assert (
    await report_external_sync(
      connection_id,
      "kg_someone_elses",
      succeeded=True,
      counts={},
      message=None,
      db_session=test_db,
    )
    is None
  )
