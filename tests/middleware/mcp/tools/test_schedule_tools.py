"""Tests for schedule MCP read tools.

Schedule writes have no dedicated ops — they go through
`create/update/delete-information-block` (`block_type='schedule'`),
registrar-generated; their execution path is covered by
`tests/middleware/mcp/test_registrar.py` + the ops-layer tests under
`tests/operations/roboledger/schedules/`. Closing-entry drafting
(schedule-derived and manual) runs through `create-event-block` with
`event_type='schedule_entry_due'` or `'journal_entry_recorded'`;
schedule termination is internal to the `asset_disposed` handler — see
the Python handler registry tests at
`tests/operations/event_block/python_handlers/`.

Mocks live at the **operations layer boundary** — the read tools are
thin shims that build arguments, call into `operations/roboledger/reads/
{schedules,period_drafts}.py`, and reshape responses.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.schedule_tools import (
  GetPeriodCloseStatusTool,
  ListPeriodDraftsTool,
)
from robosystems.models.api.extensions.fiscal_calendar import (
  DraftEntryResponse,
  DraftLineItem,
  PeriodDraftsResponse,
)
from robosystems.models.api.extensions.schedules import (
  PeriodCloseItemResponse,
  PeriodCloseStatusResponse,
)

MODULE = "robosystems.middleware.mcp.tools.schedule_tools"


@pytest.fixture
def mock_graph_client():
  client = MagicMock()
  client.graph_id = "kgtest123"
  return client


@contextmanager
def _patch_session():
  """Patch the extensions session context manager."""
  session = MagicMock()
  cm = MagicMock()
  cm.__enter__ = MagicMock(return_value=session)
  cm.__exit__ = MagicMock(return_value=False)
  with patch(f"{MODULE}.extensions_session", return_value=cm):
    yield session


# list-schedule-structures and get-schedule-facts tools were retired —
# schedule envelopes surface through the generic information-block reads
# (``list-information-blocks``, ``get-information-block``). Their tests
# live in ``tests/operations/information_block/`` and
# ``tests/middleware/mcp/tools/test_information_block_tools.py`` (if added
# — the MCP wiring is exercised via the router integration suite).


# ────────────────────────────────────────────────────────────────────────────
# get-period-close-status
# ────────────────────────────────────────────────────────────────────────────


class TestGetPeriodCloseStatusTool:
  def test_tool_definition(self, mock_graph_client):
    tool = GetPeriodCloseStatusTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "get-period-close-status"
    assert "period_start" in defn["inputSchema"]["required"]

  @pytest.mark.asyncio
  async def test_returns_status(self, mock_graph_client):
    response = PeriodCloseStatusResponse(
      fiscal_period_start=date(2026, 1, 1),
      fiscal_period_end=date(2026, 1, 31),
      period_status="open",
      schedules=[
        PeriodCloseItemResponse(
          structure_id="struct_01",
          structure_name="Depreciation",
          amount=416.67,
          status="pending",
          entry_id=None,
        ),
      ],
      total_draft=0,
      total_posted=0,
    )
    tool = GetPeriodCloseStatusTool(mock_graph_client)
    with (
      _patch_session(),
      patch(f"{MODULE}.ops_get_period_close_status", return_value=response),
    ):
      result = await tool.execute(
        {"period_start": "2026-01-01", "period_end": "2026-01-31"}
      )

    assert result["period_status"] == "open"
    assert result["schedules"]["total"] == 1
    assert result["schedules"]["pending"] == 1
    # An open period has no receipt, and saying so is the answer rather
    # than an omission — this is the key an operator polls after a close
    # hands back `in_progress`.
    assert result["close_receipt"] is None

  @pytest.mark.asyncio
  async def test_the_close_receipt_reaches_the_wire(self, mock_graph_client):
    """This tool is where the playbook sends an operator whose close
    outlived their client. The receipt was on the ops response and absent
    from the returned dict, so the instruction pointed at nothing."""
    from robosystems.models.api.extensions.schedules import CloseReceiptResponse

    receipt = CloseReceiptResponse(
      version=1,
      period="2026-01",
      closed_at=datetime(2026, 2, 1, 3, 35, tzinfo=UTC),
      closed_by="user_abc",
      actor_type="agent",
      was_reclose=False,
      entries_posted=34,
      entries_published_to_qb=31,
      entries_posted_locally=3,
      target_auto_advanced=True,
      statements_stamped=True,
    )
    response = PeriodCloseStatusResponse(
      fiscal_period_start=date(2026, 1, 1),
      fiscal_period_end=date(2026, 1, 31),
      period_status="closed",
      schedules=[],
      total_draft=0,
      total_posted=34,
      close_receipt=receipt,
    )
    tool = GetPeriodCloseStatusTool(mock_graph_client)
    with (
      _patch_session(),
      patch(f"{MODULE}.ops_get_period_close_status", return_value=response),
    ):
      result = await tool.execute(
        {"period_start": "2026-01-01", "period_end": "2026-01-31"}
      )

    assert result["period_status"] == "closed"
    assert result["close_receipt"]["entries_posted"] == 34
    assert result["close_receipt"]["entries_published_to_qb"] == 31
    assert result["close_receipt"]["statements_stamped"] is True

  @pytest.mark.asyncio
  async def test_includes_reversal_fields(self, mock_graph_client):
    response = PeriodCloseStatusResponse(
      fiscal_period_start=date(2026, 4, 1),
      fiscal_period_end=date(2026, 4, 30),
      period_status="open",
      schedules=[
        PeriodCloseItemResponse(
          structure_id="struct_accrual",
          structure_name="Payroll Accrual",
          amount=3200.00,
          status="drafted",
          entry_id="je_01ABC",
          reversal_entry_id="je_01DEF",
          reversal_status="draft",
        ),
        PeriodCloseItemResponse(
          structure_id="struct_depr",
          structure_name="Depreciation",
          amount=212.00,
          status="pending",
          entry_id=None,
        ),
      ],
      total_draft=1,
      total_posted=0,
    )
    tool = GetPeriodCloseStatusTool(mock_graph_client)
    with (
      _patch_session(),
      patch(f"{MODULE}.ops_get_period_close_status", return_value=response),
    ):
      result = await tool.execute(
        {"period_start": "2026-04-01", "period_end": "2026-04-30"}
      )

    details = result["schedules"]["details"]
    assert details[0]["reversal_entry_id"] == "je_01DEF"
    assert details[0]["reversal_status"] == "draft"
    assert details[1]["reversal_entry_id"] is None
    assert details[1]["reversal_status"] is None


# ────────────────────────────────────────────────────────────────────────────
# list-period-drafts
# ────────────────────────────────────────────────────────────────────────────


class TestListPeriodDraftsTool:
  def test_tool_definition(self, mock_graph_client):
    tool = ListPeriodDraftsTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "list-period-drafts"
    assert "period" in defn["inputSchema"]["required"]

  @pytest.mark.asyncio
  async def test_returns_drafts(self, mock_graph_client):
    response = PeriodDraftsResponse(
      period="2026-03",
      period_start=date(2026, 3, 1),
      period_end=date(2026, 3, 31),
      draft_count=1,
      total_debit=41667,
      total_credit=41667,
      all_balanced=True,
      drafts=[
        DraftEntryResponse(
          entry_id="je_01",
          posting_date=date(2026, 3, 31),
          type="closing",
          memo="Monthly depreciation",
          provenance="schedule",
          source_structure_id="struct_01",
          source_structure_name="Office Furniture Depreciation",
          line_items=[
            DraftLineItem(
              line_item_id="li_01",
              element_id="elem_depr",
              element_code="6510",
              element_name="Depreciation Expense",
              debit_amount=41667,
              credit_amount=0,
              description=None,
            ),
            DraftLineItem(
              line_item_id="li_02",
              element_id="elem_accum",
              element_code="1810",
              element_name="Accumulated Depreciation",
              debit_amount=0,
              credit_amount=41667,
              description=None,
            ),
          ],
          total_debit=41667,
          total_credit=41667,
          balanced=True,
        )
      ],
    )
    tool = ListPeriodDraftsTool(mock_graph_client)
    with _patch_session(), patch(f"{MODULE}.list_period_drafts", return_value=response):
      result = await tool.execute({"period": "2026-03"})

    assert result["draft_count"] == 1
    assert result["all_balanced"] is True
    assert result["drafts"][0]["entry_id"] == "je_01"
    assert len(result["drafts"][0]["line_items"]) == 2

  @pytest.mark.asyncio
  async def test_handles_error(self, mock_graph_client):
    tool = ListPeriodDraftsTool(mock_graph_client)
    with patch(f"{MODULE}.extensions_session", side_effect=Exception("db error")):
      result = await tool.execute({"period": "2026-03"})

    assert "error" in result
