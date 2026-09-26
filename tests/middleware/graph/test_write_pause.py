"""The maintenance pause refuses new graph writes until its timestamp.

A deliberate writer roll sets GRAPH_WRITES_PAUSED_UNTIL. A write is refused
where it is admitted (`begin_destructive_op`), never mid-operation: the Graph
API's per-table `instance_busy` lets admitted work finish so the roll's drain
can wait for it. The pause lapses on its own and fails open.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.config import parameter_store
from robosystems.middleware.graph import instance_busy as ib
from robosystems.middleware.graph import write_pause

pytestmark = pytest.mark.unit

NOW = datetime(2026, 9, 25, 22, 0, tzinfo=UTC)
PARAM = "/robosystems/prod/features/GRAPH_WRITES_PAUSED_UNTIL"


@pytest.fixture
def ssm(monkeypatch):
  """A real SSM API (moto), read through the manager's uncached path."""
  import boto3
  from moto import mock_aws

  monkeypatch.setenv("ENVIRONMENT", "prod")
  monkeypatch.setenv("AWS_REGION", "us-east-1")
  monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
  monkeypatch.setattr(parameter_store, "_fast_ssm_client", None)
  monkeypatch.setattr(parameter_store, "_parameter_manager", None)
  with mock_aws():
    yield boto3.client("ssm", region_name="us-east-1")
  monkeypatch.setattr(parameter_store, "_fast_ssm_client", None)
  monkeypatch.setattr(parameter_store, "_parameter_manager", None)


@pytest.mark.parametrize(
  "value, paused",
  [
    pytest.param("2026-09-25T23:00:00Z", True, id="future"),
    pytest.param("2026-09-25T21:00:00Z", False, id="lapsed"),
    pytest.param("tomorrow-ish", False, id="unparseable fails open"),
    pytest.param("2026-09-25T23:00:00", True, id="naive is UTC"),
  ],
)
def test_the_pause_holds_until_its_timestamp(ssm, value, paused):
  ssm.put_parameter(Name=PARAM, Value=value, Type="String")
  until = write_pause.graph_writes_paused_until(now=NOW)
  assert (until is not None) is paused


def test_no_parameter_means_no_pause(ssm):
  assert write_pause.graph_writes_paused_until(now=NOW) is None


@pytest.fixture
def paused(ssm):
  until = (datetime.now(UTC) + timedelta(minutes=30)).isoformat()
  ssm.put_parameter(Name=PARAM, Value=until, Type="String")
  yield


@pytest.fixture
def counter():
  with (
    patch.object(ib, "_update_counter_async") as async_update,
    patch.object(ib, "_update_counter") as sync_update,
  ):
    yield async_update, sync_update


async def test_a_new_write_is_refused_before_it_is_counted(paused, counter):
  async_update, _ = counter
  with pytest.raises(write_pause.GraphWritesPausedError):
    await ib.begin_destructive_op("i-abc", ib.OP_KIND_MATERIALIZATION)
  async_update.assert_not_called()


async def test_work_admitted_before_the_pause_runs_to_completion(paused, counter):
  """The Graph API marks each table call busy; a pause must not cut one off."""
  async_update, sync_update = counter
  async with ib.instance_busy("i-abc", ib.OP_KIND_MATERIALIZATION):
    pass
  with ib.instance_busy_sync("i-abc", ib.OP_KIND_MATERIALIZATION):
    pass
  assert async_update.await_count == 2
  assert sync_update.call_count == 2


async def test_a_refused_ledger_materialization_is_an_error_on_the_result(paused):
  from robosystems.operations.extensions.materialize import ExtensionsMaterializer

  client = AsyncMock()
  client._instance_id = "i-abc"
  client.__aenter__.return_value = client
  with (
    patch(
      "robosystems.graph_api.client.factory.get_graph_client",
      new=AsyncMock(return_value=client),
    ),
    patch.object(ib, "_update_counter_async") as counted,
  ):
    result = await ExtensionsMaterializer().materialize("kg0123456789abcdef01")

  assert result.status == "error"
  assert "paused for maintenance" in result.errors[0]
  client.__aexit__.assert_awaited()
  # The finally's decrement names no instance, which is a no-op.
  assert all(not c.args[0] for c in counted.call_args_list)


def test_the_stale_graph_sensor_waits_out_the_pause(paused):
  from dagster import SkipReason, build_sensor_context

  from robosystems.dagster.sensors.materialization import (
    stale_graph_materialization_sensor,
  )

  with patch(
    "robosystems.dagster.sensors.materialization.db_session_factory"
  ) as sessions:
    result = stale_graph_materialization_sensor(build_sensor_context())

  assert isinstance(result, SkipReason)
  sessions.assert_not_called()


async def test_the_materialize_api_answers_503_with_retry_after(paused):
  with pytest.raises(HTTPException) as refused:
    await write_pause.refuse_while_writes_paused()
  assert refused.value.status_code == 503
  assert int(refused.value.headers["Retry-After"]) > 60


async def test_an_ingest_into_the_graph_is_refused_before_anything_starts(paused):
  from robosystems.operations.graph.commands.ingest_file import ingest_file_cmd

  with (
    patch(
      "robosystems.operations.graph.commands.ingest_file.GraphFile.get_by_id"
    ) as file_lookup,
    pytest.raises(HTTPException) as refused,
  ):
    await ingest_file_cmd("kg1", "f1", True, AsyncMock(), AsyncMock(), AsyncMock())
  assert refused.value.status_code == 503
  file_lookup.assert_not_called()


async def test_a_forked_subgraph_is_refused_before_it_is_queued(paused):
  from robosystems.models.api.graphs.subgraphs import CreateSubgraphRequest
  from robosystems.routers.graphs.subgraphs import main

  parent = MagicMock()
  with (
    patch.object(main, "handle_circuit_breaker_check"),
    patch.object(main, "verify_parent_graph_access", return_value=parent),
    patch.object(main, "verify_subgraph_tier_support"),
    patch.object(main, "verify_parent_graph_active"),
    patch.object(main, "check_subgraph_quota", return_value=(0, 3, [])),
    patch.object(main, "validate_subgraph_name_unique"),
    patch("robosystems.worker.client.enqueue_task", new=AsyncMock()) as enqueue,
    pytest.raises(HTTPException) as refused,
  ):
    await main.create_subgraph(
      request=CreateSubgraphRequest(name="dev", display_name="Dev", fork_parent=True),
      graph_id="kg0123456789abcdef01",
      current_user=AsyncMock(),
      db=AsyncMock(),
    )
  assert refused.value.status_code == 503
  enqueue.assert_not_awaited()


async def test_a_queued_file_write_is_refused_at_its_start(paused, counter):
  from robosystems.dagster.jobs.graph import _counted_materialize_table

  client = AsyncMock()
  client._instance_id = "i-abc"
  with pytest.raises(write_pause.GraphWritesPausedError):
    await _counted_materialize_table(client, "kg1", "Entity", ["f1"])
  client.materialize_table.assert_not_awaited()
  counter[0].assert_not_called()


async def test_a_file_write_is_counted_for_the_drain(ssm, counter):
  from robosystems.dagster.jobs.graph import _counted_materialize_table

  async_update, _ = counter
  client = AsyncMock()
  client._instance_id = "i-abc"
  client.materialize_table.return_value = {"rows_ingested": 3}
  assert await _counted_materialize_table(client, "kg1", "Entity", ["f1"]) == {
    "rows_ingested": 3
  }
  assert [c.kwargs["delta"] for c in async_update.await_args_list] == [1, -1]


def test_a_deferred_ledger_run_does_not_fail_the_dagster_run(paused):
  """A refusal is planned maintenance: no RunFailure alert, graph stays stale."""
  from dagster import build_op_context

  from robosystems.dagster.jobs.extensions import (
    ExtensionsMaterializeConfig,
    materialize_extensions_to_graph,
  )
  from robosystems.operations.extensions.materialize import MaterializeResult

  deferred = MaterializeResult(graph_id="kg0123456789abcdef01", status="error")
  deferred.paused_until = datetime.now(UTC) + timedelta(minutes=30)
  with (
    patch(
      "robosystems.operations.extensions.materialize.ExtensionsMaterializer.materialize",
      new=AsyncMock(return_value=deferred),
    ),
    patch(
      "robosystems.middleware.graph.ingestion_limits.IngestionLimitChecker.check_instance_storage",
      new=AsyncMock(return_value={"allowed": True, "errors": []}),
    ),
    patch("robosystems.database.get_db_session") as sessions,
  ):
    sessions.side_effect = lambda: iter([MagicMock()])
    out = materialize_extensions_to_graph(
      build_op_context(),
      MagicMock(),
      MagicMock(),
      ExtensionsMaterializeConfig(graph_id="kg0123456789abcdef01"),
    )
  assert out["status"] == "deferred"


def test_a_broken_ssm_client_fails_open(monkeypatch):
  monkeypatch.setenv("ENVIRONMENT", "prod")
  monkeypatch.setattr(parameter_store, "_parameter_manager", None)

  def broken():
    raise KeyError("credential_provider")

  monkeypatch.setattr(parameter_store, "_get_fast_ssm_client", broken)
  assert write_pause.graph_writes_paused_until(now=NOW) is None


async def test_writes_proceed_without_a_pause(ssm, counter):
  async_update, _ = counter
  await ib.begin_destructive_op("i-abc", ib.OP_KIND_MATERIALIZATION)
  async_update.assert_awaited_once()
