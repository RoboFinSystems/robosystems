"""Tests for the semantic-memory update-memory MCP tool.

Focused on ``update-memory`` (the write verb added alongside remember/recall/
forget). The tool is a thin adapter over ``MemoryService.update_memory`` — the
kernel itself is tested in the operations/memory suite.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.semantic_memory_tools import (
  SemanticUpdateMemoryTool,
)

SEM_MODULE = "robosystems.middleware.mcp.tools.semantic_memory_tools"
MEMORY_ID = "mem_" + "a" * 32


@pytest.fixture
def mock_graph_client():
  client = MagicMock()
  client.graph_id = "kgtest123"
  return client


def _patched_service(mock_service):
  """Patch the writer-client acquisition + kernel wiring used by _with_service."""
  mock_client = AsyncMock()
  return (
    patch(f"{SEM_MODULE}._block_shared_repo", return_value=None),
    patch(
      "robosystems.graph_api.client.factory.get_graph_client",
      AsyncMock(return_value=mock_client),
    ),
    patch(
      "robosystems.operations.memory.get_memory_service",
      return_value=mock_service,
    ),
  )


class TestSemanticUpdateMemoryTool:
  def test_tool_definition(self, mock_graph_client):
    tool = SemanticUpdateMemoryTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "update-memory"
    assert defn["inputSchema"]["required"] == ["memory_id"]
    assert "text" in defn["inputSchema"]["properties"]

  @pytest.mark.asyncio
  async def test_updates_memory(self, mock_graph_client):
    mock_record = MagicMock()
    mock_record.model_dump.return_value = {"id": MEMORY_ID, "text": "revised"}
    mock_service = MagicMock()
    mock_service.update_memory = AsyncMock(return_value=mock_record)

    tool = SemanticUpdateMemoryTool(mock_graph_client)
    p_block, p_client, p_service = _patched_service(mock_service)
    with p_block, p_client, p_service:
      result = await tool.execute({"memory_id": MEMORY_ID, "text": "revised"})

    assert result["success"] is True
    assert result["memory"]["text"] == "revised"
    # graph_id, memory_id, request forwarded to the kernel
    call = mock_service.update_memory.await_args
    assert call.args[0] == "kgtest123"
    assert call.args[1] == MEMORY_ID
    # only the field the caller set is present (partial update)
    request = call.args[2]
    assert request.model_fields_set == {"text"}

  @pytest.mark.asyncio
  async def test_rejects_no_fields(self, mock_graph_client):
    tool = SemanticUpdateMemoryTool(mock_graph_client)
    result = await tool.execute({"memory_id": MEMORY_ID})
    assert result["error"] == "invalid_input"

  @pytest.mark.asyncio
  async def test_requires_memory_id(self, mock_graph_client):
    tool = SemanticUpdateMemoryTool(mock_graph_client)
    result = await tool.execute({"text": "revised"})
    assert result["error"] == "invalid_input"

  @pytest.mark.asyncio
  async def test_returns_not_found(self, mock_graph_client):
    mock_service = MagicMock()
    mock_service.update_memory = AsyncMock(return_value=None)

    tool = SemanticUpdateMemoryTool(mock_graph_client)
    p_block, p_client, p_service = _patched_service(mock_service)
    with p_block, p_client, p_service:
      result = await tool.execute({"memory_id": MEMORY_ID, "text": "revised"})

    assert result["error"] == "not_found"
