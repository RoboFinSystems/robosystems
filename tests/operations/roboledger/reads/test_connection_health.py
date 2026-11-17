"""``load_connection_health``: what the platform knows about each of a graph's
connections, read once for the bank account list on every surface."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from robosystems.operations.roboledger.reads.bank_accounts import (
  ConnectionHealth,
  load_connection_health,
)

pytestmark = pytest.mark.unit

_CONNECTION = "robosystems.models.core.connection.connection.Connection"


def _platform(session):
  cm = MagicMock()
  cm.__enter__ = MagicMock(return_value=session)
  cm.__exit__ = MagicMock(return_value=False)
  return patch("robosystems.db.platform.platform_session", return_value=cm)


def test_each_connection_is_keyed_by_its_id():
  synced = datetime(2026, 11, 15, 8, 30, tzinfo=UTC)
  conn = MagicMock(
    id="conn_1",
    status="active",
    institution_name=None,
    source_name="Mercury",
    last_sync=synced,
    last_sync_result={"status": "success"},
  )
  session = MagicMock()
  with (
    _platform(session),
    patch(f"{_CONNECTION}.get_all_for_graph", return_value=[conn]) as listed,
  ):
    health = load_connection_health("kg1")

  listed.assert_called_once_with("kg1", session)
  assert health == {
    "conn_1": ConnectionHealth(
      status="active",
      institution="Mercury",
      last_sync_at=synced,
      last_sync_status="success",
    )
  }


def test_an_unreadable_platform_lists_no_health():
  with (
    _platform(MagicMock()),
    patch(f"{_CONNECTION}.get_all_for_graph", side_effect=RuntimeError("down")),
  ):
    assert load_connection_health("kg1") == {}
