"""The agent-activity tool: a thin shim over the read, which it hands the
entity scope and whose refusals it names."""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.agent_tools import AgentActivityTool
from robosystems.operations.roboledger.entity_scope import EntityNotInGraphError

MODULE = "robosystems.middleware.mcp.tools.agent_tools"


@pytest.fixture
def tool():
  client = MagicMock()
  client.graph_id = "kgtest123"
  return AgentActivityTool(client)


@contextmanager
def _patch_session():
  session = MagicMock()
  cm = MagicMock()
  cm.__enter__ = MagicMock(return_value=session)
  cm.__exit__ = MagicMock(return_value=False)
  with patch(f"{MODULE}.extensions_session", return_value=cm):
    yield session


def test_entity_id_is_offered_and_optional(tool):
  schema = tool.get_tool_definition()["inputSchema"]

  assert "entity_id" in schema["properties"]
  assert schema["required"] == ["id"]


def test_the_entity_reaches_the_read(tool):
  activity = MagicMock()
  activity.model_dump.return_value = {"entity_id": "ent_sub"}
  with (
    _patch_session() as session,
    patch(f"{MODULE}.ops_get_agent_activity", return_value=activity) as read,
  ):
    result = tool._execute_sync({"id": "agt_gusto", "entity_id": "ent_sub"})

  assert result == {"entity_id": "ent_sub"}
  read.assert_called_once_with(session, "agt_gusto", limit=100, entity_id="ent_sub")


def test_an_entity_outside_the_graph_is_named(tool):
  with (
    _patch_session(),
    patch(
      f"{MODULE}.ops_get_agent_activity",
      side_effect=EntityNotInGraphError("Entity 'ent_x' not found in this graph."),
    ),
  ):
    result = tool._execute_sync({"id": "agt_gusto", "entity_id": "ent_x"})

  assert result == {
    "error": "entity_not_found",
    "message": "Entity 'ent_x' not found in this graph.",
  }
