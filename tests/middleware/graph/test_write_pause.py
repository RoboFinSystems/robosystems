"""The maintenance pause refuses new graph writes until its timestamp.

A deliberate writer roll sets GRAPH_WRITES_PAUSED_UNTIL; new writes refuse
before they mark an instance busy, so the roll's drain can finish. The pause
lapses on its own and fails open when the value cannot be read.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.middleware.graph import instance_busy as ib
from robosystems.middleware.graph import write_pause

pytestmark = pytest.mark.unit

NOW = datetime(2026, 9, 25, 22, 0, tzinfo=UTC)


def _parameter(value: str):
  manager = MagicMock()
  manager.get_parameter_uncached.return_value = value
  return patch(
    "robosystems.config.parameter_store.get_parameter_manager", return_value=manager
  )


@pytest.mark.parametrize(
  "value, paused",
  [
    pytest.param("2026-09-25T23:00:00Z", True, id="future"),
    pytest.param("2026-09-25T21:00:00Z", False, id="lapsed"),
    pytest.param("", False, id="unset"),
    pytest.param("tomorrow-ish", False, id="unparseable fails open"),
    pytest.param("2026-09-25T23:00:00", True, id="naive is UTC"),
  ],
)
def test_the_pause_holds_until_its_timestamp(value, paused):
  with _parameter(value):
    until = write_pause.graph_writes_paused_until(now=NOW)
  assert (until is not None) is paused


@pytest.fixture
def paused():
  until = (datetime.now(UTC) + timedelta(minutes=30)).isoformat()
  with _parameter(until):
    yield


@pytest.fixture
def counter():
  with (
    patch.object(ib, "_update_counter_async") as async_update,
    patch.object(ib, "_update_counter") as sync_update,
  ):
    yield async_update, sync_update


async def test_a_write_refuses_before_it_marks_the_instance_busy(paused, counter):
  async_update, _ = counter
  with pytest.raises(write_pause.GraphWritesPausedError):
    await ib.begin_destructive_op("i-abc", ib.OP_KIND_MATERIALIZATION)
  with pytest.raises(write_pause.GraphWritesPausedError):
    async with ib.instance_busy("i-abc", ib.OP_KIND_MATERIALIZATION):
      pass
  async_update.assert_not_called()


def test_the_sync_form_refuses_too(paused, counter):
  _, sync_update = counter
  with pytest.raises(write_pause.GraphWritesPausedError):
    with ib.instance_busy_sync("i-abc", ib.OP_KIND_MATERIALIZATION):
      pass
  sync_update.assert_not_called()


async def test_writes_proceed_without_a_pause(counter):
  async_update, _ = counter
  with _parameter(""):
    await ib.begin_destructive_op("i-abc", ib.OP_KIND_MATERIALIZATION)
  async_update.assert_awaited_once()


def test_the_materialize_api_answers_503_with_retry_after(paused):
  from robosystems.operations.graph.commands.materialize import (
    _refuse_while_writes_paused,
  )

  with pytest.raises(HTTPException) as refused:
    _refuse_while_writes_paused()
  assert refused.value.status_code == 503
  assert int(refused.value.headers["Retry-After"]) > 60
