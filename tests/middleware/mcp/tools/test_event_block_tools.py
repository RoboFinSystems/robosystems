"""Unit tests for the Event Block MCP read tools.

Mocks live at the operations-layer boundary (``ops_list_event_blocks``) —
the tool is a thin shim, so we verify argument forwarding and response
shaping only. The reconciling-item filter matters most: ``is_reconciling_item=true``
is the post-sync reconciliation worklist, and both response modes must
surface the flag.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.event_block_tools import ListEventBlocksTool
from robosystems.models.api.event_block import EventBlockEnvelope

MODULE = "robosystems.middleware.mcp.tools.event_block_tools"


def _client() -> MagicMock:
  client = MagicMock()
  client.graph_id = "kg_test"
  return client


@contextmanager
def _patched_session():
  @contextmanager
  def fake_session(_graph_id):
    yield MagicMock()

  with patch(f"{MODULE}.extensions_session", fake_session):
    yield


def _envelope(
  *, id: str = "evt_01", is_reconciling_item: bool = False
) -> EventBlockEnvelope:
  return EventBlockEnvelope(
    id=id,
    event_type="journal_entry_recorded",
    event_category="adjustment",
    event_class="economic",
    status="committed",
    occurred_at=datetime(2026, 4, 15, 12, tzinfo=UTC),
    effective_at=None,
    source="quickbooks",
    external_id="JournalEntry_42",
    external_url=None,
    amount=12345,
    currency="USD",
    description="Widget sale",
    metadata={"drift_payload": {"amount": 99}} if is_reconciling_item else {},
    is_reconciling_item=is_reconciling_item,
    dimension_ids=[],
    agent_id=None,
    resource_type=None,
    resource_element_id=None,
    replaced_by_event_id=None,
    replaces_event_id=None,
    obligated_by_event_id=None,
    discharges_event_id=None,
    created_at=datetime(2026, 4, 16, 9, tzinfo=UTC),
    created_by="system",
  )


class TestListEventBlocksReconcilingItemFilter:
  @pytest.mark.asyncio
  async def test_is_reconciling_item_arg_forwards_to_ops(self) -> None:
    with (
      _patched_session(),
      patch(f"{MODULE}.ops_list_event_blocks", return_value=[]) as ops_mock,
    ):
      result = await ListEventBlocksTool(_client()).execute(
        {"is_reconciling_item": True}
      )

    assert result["event_count"] == 0
    assert ops_mock.call_args.kwargs["is_reconciling_item"] is True

  @pytest.mark.asyncio
  async def test_is_reconciling_item_omitted_forwards_none(self) -> None:
    with (
      _patched_session(),
      patch(f"{MODULE}.ops_list_event_blocks", return_value=[]) as ops_mock,
    ):
      await ListEventBlocksTool(_client()).execute({})

    assert ops_mock.call_args.kwargs["is_reconciling_item"] is None

  @pytest.mark.asyncio
  async def test_summary_mode_surfaces_drift_flag(self) -> None:
    envelopes = [
      _envelope(id="evt_ok"),
      _envelope(id="evt_drift", is_reconciling_item=True),
    ]
    with (
      _patched_session(),
      patch(f"{MODULE}.ops_list_event_blocks", return_value=envelopes),
    ):
      result = await ListEventBlocksTool(_client()).execute({})

    assert result["mode"] == "summary"
    by_id = {e["id"]: e for e in result["events"]}
    assert by_id["evt_ok"]["is_reconciling_item"] is False
    assert by_id["evt_drift"]["is_reconciling_item"] is True
    # Summary mode stays lean — the stashed payload is metadata-only.
    assert "drift_payload" not in by_id["evt_drift"]

  @pytest.mark.asyncio
  async def test_full_mode_carries_flag_and_stashed_payload(self) -> None:
    envelopes = [_envelope(id="evt_drift", is_reconciling_item=True)]
    with (
      _patched_session(),
      patch(f"{MODULE}.ops_list_event_blocks", return_value=envelopes),
    ):
      result = await ListEventBlocksTool(_client()).execute({"include_metadata": True})

    assert result["mode"] == "full"
    (event,) = result["events"]
    assert event["is_reconciling_item"] is True
    assert event["metadata"]["drift_payload"] == {"amount": 99}
