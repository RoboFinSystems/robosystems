"""The maintenance pause refuses new graph writes until its timestamp.

A deliberate writer roll sets GRAPH_WRITES_PAUSED_UNTIL. A write is refused
where it is admitted (`begin_destructive_op`), never mid-operation: the Graph
API's per-table `instance_busy` lets admitted work finish so the roll's drain
can wait for it. The pause lapses on its own and fails open.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

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
  from robosystems.operations.graph.commands.materialize import (
    _refuse_while_writes_paused,
  )

  with pytest.raises(HTTPException) as refused:
    await _refuse_while_writes_paused()
  assert refused.value.status_code == 503
  assert int(refused.value.headers["Retry-After"]) > 60


async def test_writes_proceed_without_a_pause(ssm, counter):
  async_update, _ = counter
  await ib.begin_destructive_op("i-abc", ib.OP_KIND_MATERIALIZATION)
  async_update.assert_awaited_once()
