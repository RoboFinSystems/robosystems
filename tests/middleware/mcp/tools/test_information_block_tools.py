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
  async def test_returns_block_count_and_envelopes(self, mock_graph_client):
    tool = ListInformationBlocksTool(mock_graph_client)
    envelopes = [_envelope("struct_a"), _envelope("struct_b")]
    with (
      _patch_session(),
      patch(f"{MODULE}.ops_list_information_blocks", return_value=envelopes),
    ):
      result = await tool.execute({"block_type": "schedule"})

    assert result["block_count"] == 2
    assert [b["id"] for b in result["blocks"]] == ["struct_a", "struct_b"]

  @pytest.mark.asyncio
  async def test_unknown_block_type_returns_invalid_arguments(self, mock_graph_client):
    """Ops layer raises ValueError for unknown block_type; tool shapes it."""
    tool = ListInformationBlocksTool(mock_graph_client)
    with (
      _patch_session(),
      patch(
        f"{MODULE}.ops_list_information_blocks",
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
      patch(f"{MODULE}.ops_list_information_blocks", return_value=[]) as ops_mock,
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
    )
