"""Tests for Information Block MCP read tools.

Thin shims over ``operations/information_block/reads.py`` — the tool
code reshapes arguments, calls the ops function, and formats the
response. Mocks live at the operations-layer boundary.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.information_block_tools import (
  GetInformationBlockTool,
  ListInformationBlocksTool,
)
from robosystems.models.api.extensions.schedules import EntryTemplateRequest
from robosystems.models.api.information_block import (
  ArtifactResponse,
  InformationBlockEnvelope,
  InformationBlockSummary,
  InformationModelResponse,
  ScheduleMechanics,
)

_TEST_ENTRY_TEMPLATE = EntryTemplateRequest(
  debit_element_id="elem_dr", credit_element_id="elem_cr"
)

MODULE = "robosystems.middleware.mcp.tools.information_block_tools"


@pytest.fixture
def mock_graph_client():
  client = MagicMock()
  client.graph_id = "kgtest123"
  return client


@contextmanager
def _patch_session():
  session = MagicMock()
  cm = MagicMock()
  cm.__enter__ = MagicMock(return_value=session)
  cm.__exit__ = MagicMock(return_value=False)
  with patch(f"{MODULE}.extensions_session", return_value=cm):
    yield session


def _summary(structure_id: str = "struct_x") -> InformationBlockSummary:
  return InformationBlockSummary(
    id=structure_id,
    block_type="schedule",
    name="Test Schedule",
    display_name="Schedule",
    category="Close",
    taxonomy_id="tax_1",
    taxonomy_name="rs-gaap",
    entity_id="ent_parent",
    fact_count=12,
    connection_count=3,
  )


def _envelope(structure_id: str = "struct_x") -> InformationBlockEnvelope:
  return InformationBlockEnvelope(
    id=structure_id,
    block_type="schedule",
    name="Test Schedule",
    display_name="Schedule",
    category="Close",
    information_model=InformationModelResponse(concept_arrangement="roll_forward"),
    artifact=ArtifactResponse(
      mechanics=ScheduleMechanics(entry_template=_TEST_ENTRY_TEMPLATE)
    ),
  )


# ────────────────────────────────────────────────────────────────────────────
# get-information-block
# ────────────────────────────────────────────────────────────────────────────


class TestGetInformationBlockTool:
  def test_tool_definition(self, mock_graph_client):
    tool = GetInformationBlockTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "get-information-block"
    assert "id" in defn["inputSchema"]["required"]

  @pytest.mark.asyncio
  async def test_returns_envelope_dict_on_hit(self, mock_graph_client):
    tool = GetInformationBlockTool(mock_graph_client)
    envelope = _envelope("struct_hit")
    with (
      _patch_session(),
      patch(f"{MODULE}.ops_get_information_block", return_value=envelope),
    ):
      result = await tool.execute({"id": "struct_hit"})

    assert result["id"] == "struct_hit"
    assert result["block_type"] == "schedule"
    assert result["name"] == "Test Schedule"

  @pytest.mark.asyncio
  async def test_a_facts_descriptor_rides_only_when_asked(self, mock_graph_client):
    """The facts are the same either way; the descriptor and the freeze are
    the reader's to ask for, so a routine read stays small."""
    from datetime import UTC, date, datetime

    from robosystems.models.api.information_block import FactLite

    tool = GetInformationBlockTool(mock_graph_client)
    envelope = _envelope("struct_facts")
    envelope.facts = [
      FactLite(
        id="fact_1",
        element_id="el_rev",
        value=1_000.0,
        period_end=date(2026, 6, 30),
        period_type="duration",
        fact_scope="in_scope",
        provenance={"origin": "pivot", "mapping_id": "map_1", "period": "x"},
        frozen_at=datetime(2026, 7, 2, tzinfo=UTC),
      )
    ]
    with (
      _patch_session(),
      patch(f"{MODULE}.ops_get_information_block", return_value=envelope),
    ):
      plain = await tool.execute({"id": "struct_facts"})
      asked = await tool.execute({"id": "struct_facts", "include_provenance": True})

    assert plain["facts"][0]["value"] == 1_000.0
    assert "provenance" not in plain["facts"][0]
    assert "frozen_at" not in plain["facts"][0]
    assert asked["facts"][0]["provenance"]["origin"] == "pivot"
    assert asked["facts"][0]["frozen_at"] == "2026-07-02T00:00:00Z"

  @pytest.mark.asyncio
  async def test_returns_not_found_when_ops_returns_none(self, mock_graph_client):
    tool = GetInformationBlockTool(mock_graph_client)
    with (
      _patch_session(),
      patch(f"{MODULE}.ops_get_information_block", return_value=None),
    ):
      result = await tool.execute({"id": "struct_missing"})

    assert result["error"] == "not_found"
    assert "struct_missing" in result["message"]

  @pytest.mark.asyncio
  async def test_handles_unexpected_exception(self, mock_graph_client):
    tool = GetInformationBlockTool(mock_graph_client)
    with patch(f"{MODULE}.extensions_session", side_effect=Exception("db boom")):
      result = await tool.execute({"id": "struct_x"})

    assert result["error"] == "command_failed"
    assert "db boom" in result["message"]


# ────────────────────────────────────────────────────────────────────────────
# list-information-blocks
# ────────────────────────────────────────────────────────────────────────────


class TestListInformationBlocksTool:
  def test_tool_definition(self, mock_graph_client):
    tool = ListInformationBlocksTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "list-information-blocks"
    # No required fields — all filters are optional.
    assert defn["inputSchema"].get("required", []) == []

  @pytest.mark.asyncio
  async def test_summary_mode_reads_summaries_in_one_query(self, mock_graph_client):
    """The default projection is the one-query summaries read, never an
    envelope build per block."""
    tool = ListInformationBlocksTool(mock_graph_client)
    summaries = [_summary("struct_a"), _summary("struct_b")]
    with (
      _patch_session(),
      patch(f"{MODULE}.ops_list_information_block_summaries", return_value=summaries),
      patch(f"{MODULE}.ops_list_information_blocks") as full_read,
    ):
      result = await tool.execute({"block_type": "schedule"})

    full_read.assert_not_called()
    assert result["mode"] == "summary"
    assert result["block_count"] == 2
    assert [b["id"] for b in result["blocks"]] == ["struct_a", "struct_b"]
    assert result["blocks"][0]["fact_count"] == 12
    assert "elements" not in result["blocks"][0]

  @pytest.mark.asyncio
  async def test_include_atoms_returns_full_envelopes(self, mock_graph_client):
    tool = ListInformationBlocksTool(mock_graph_client)
    envelopes = [_envelope("struct_a"), _envelope("struct_b")]
    with (
      _patch_session(),
      patch(f"{MODULE}.ops_list_information_blocks", return_value=envelopes),
      patch(f"{MODULE}.ops_list_information_block_summaries") as summaries_read,
    ):
      result = await tool.execute({"block_type": "schedule", "include_atoms": True})

    summaries_read.assert_not_called()
    assert result["mode"] == "full"
    assert result["block_count"] == 2
    assert [b["id"] for b in result["blocks"]] == ["struct_a", "struct_b"]
    assert "elements" in result["blocks"][0]

  @pytest.mark.asyncio
  async def test_unknown_block_type_returns_invalid_arguments(self, mock_graph_client):
    """Ops layer raises ValueError for unknown block_type; tool shapes it."""
    tool = ListInformationBlocksTool(mock_graph_client)
    with (
      _patch_session(),
      patch(
        f"{MODULE}.ops_list_information_block_summaries",
        side_effect=ValueError("Unknown block_type: nonsense"),
      ),
    ):
      result = await tool.execute({"block_type": "nonsense"})

    assert result["error"] == "invalid_arguments"
    assert "nonsense" in result["message"]

  @pytest.mark.asyncio
  async def test_unexpected_exception_returns_command_failed(self, mock_graph_client):
    tool = ListInformationBlocksTool(mock_graph_client)
    with patch(f"{MODULE}.extensions_session", side_effect=Exception("db down")):
      result = await tool.execute({})

    assert result["error"] == "command_failed"
    assert "db down" in result["message"]

  @pytest.mark.asyncio
  async def test_passes_pagination_args_to_ops(self, mock_graph_client):
    tool = ListInformationBlocksTool(mock_graph_client)
    with (
      _patch_session() as session,
      patch(
        f"{MODULE}.ops_list_information_block_summaries", return_value=[]
      ) as ops_mock,
    ):
      await tool.execute(
        {"block_type": "schedule", "category": "Close", "limit": 10, "offset": 5}
      )

    ops_mock.assert_called_once_with(
      session,
      block_type="schedule",
      category="Close",
      limit=10,
      offset=5,
      library_sentinel=False,
      scenario_id=None,
      entity_id=None,
    )

  @pytest.mark.asyncio
  async def test_passes_library_sentinel_for_library_graph(self, mock_graph_client):
    mock_graph_client.graph_id = "library"
    tool = ListInformationBlocksTool(mock_graph_client)
    with (
      _patch_session() as session,
      patch(
        f"{MODULE}.ops_list_information_block_summaries", return_value=[]
      ) as ops_mock,
    ):
      await tool.execute({})

    ops_mock.assert_called_once_with(
      session,
      block_type=None,
      category=None,
      limit=50,
      offset=0,
      library_sentinel=True,
      scenario_id=None,
      entity_id=None,
    )
