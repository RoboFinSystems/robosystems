"""The connect flows' sync goes through the per-connection lock."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from robosystems.operations import connection_service
from robosystems.operations.connection_service import (
  SyncInProgressError,
  dispatch_first_sync,
)

MODULE = "robosystems.operations.connection_service"


@pytest.mark.unit
@pytest.mark.asyncio
class TestDispatchFirstSync:
  async def test_a_dispatched_run_returns_its_id_under_the_lock(self):
    with patch(
      f"{MODULE}.dispatch_connection_sync",
      new_callable=AsyncMock,
      return_value={"dispatched": True, "task_id": "run_1", "message": None},
    ) as dispatch:
      task_id = await dispatch_first_sync(
        graph_id="kg_1", connection_id="conn_1", user_id="usr_1", full_rebuild=True
      )
    assert task_id == "run_1"
    dispatch.assert_awaited_once_with(
      graph_id="kg_1", connection_id="conn_1", user_id="usr_1", full_rebuild=True
    )

  async def test_a_sync_already_running_is_left_alone(self):
    with patch(
      f"{MODULE}.dispatch_connection_sync",
      new_callable=AsyncMock,
      side_effect=SyncInProgressError("conn_1", "holder", 900),
    ):
      assert (
        await dispatch_first_sync(
          graph_id="kg_1", connection_id="conn_1", user_id="usr_1", full_rebuild=False
        )
        is None
      )

  async def test_a_no_op_dispatch_is_no_run(self):
    with patch(
      f"{MODULE}.dispatch_connection_sync",
      new_callable=AsyncMock,
      return_value={"dispatched": False, "task_id": None, "message": "nothing"},
    ):
      assert (
        await dispatch_first_sync(
          graph_id="kg_1", connection_id="conn_1", user_id="usr_1", full_rebuild=False
        )
        is None
      )

  async def test_anything_else_propagates(self):
    with patch(
      f"{MODULE}.dispatch_connection_sync",
      new_callable=AsyncMock,
      side_effect=RuntimeError("dagster down"),
    ):
      with pytest.raises(RuntimeError, match="dagster down"):
        await dispatch_first_sync(
          graph_id="kg_1", connection_id="conn_1", user_id="usr_1", full_rebuild=True
        )

  def test_the_error_is_the_dispatchers_own(self):
    assert connection_service.SyncInProgressError is SyncInProgressError
