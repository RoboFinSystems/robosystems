"""The aged-balances tool: a thin shim over the aged read, or over one
counterparty's open items, which checks its arguments and names its refusals."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.aging_tools import AgedBalancesTool
from robosystems.operations.roboledger.entity_scope import EntityNotInGraphError

MODULE = "robosystems.middleware.mcp.tools.aging_tools"


@pytest.fixture
def tool():
  client = MagicMock()
  client.graph_id = "kgtest123"
  return AgedBalancesTool(client)


@contextmanager
def _patch_session():
  session = MagicMock()
  cm = MagicMock()
  cm.__enter__ = MagicMock(return_value=session)
  cm.__exit__ = MagicMock(return_value=False)
  with patch(f"{MODULE}.extensions_session", return_value=cm):
    yield session


def test_only_the_kind_is_required(tool):
  schema = tool.get_tool_definition()["inputSchema"]

  assert schema["required"] == ["kind"]
  assert {"as_of", "entity_id", "scope", "agent_id"} <= set(schema["properties"])


def test_the_summary_reads_the_named_date_and_scope(tool):
  aged = MagicMock()
  aged.model_dump.return_value = {"scope": "group"}
  with (
    _patch_session() as session,
    patch(f"{MODULE}.get_aged_balances", return_value=aged) as read,
  ):
    result = tool._execute_sync(
      {"kind": "payables", "as_of": "2026-09-30", "scope": "group"}
    )

  assert result == {"scope": "group"}
  read.assert_called_once_with(
    session, "payables", as_of=date(2026, 9, 30), entity_id=None, scope="group"
  )


def test_a_counterparty_reads_its_open_items(tool):
  item = MagicMock()
  item.model_dump.return_value = {"event_id": "evt_1"}
  with (
    _patch_session() as session,
    patch(f"{MODULE}.list_open_items", return_value=[item]) as read,
  ):
    result = tool._execute_sync(
      {
        "kind": "receivables",
        "as_of": "2026-09-30",
        "entity_id": "ent_sub",
        "agent_id": "agt_acme",
      }
    )

  assert result == {
    "kind": "receivables",
    "as_of": "2026-09-30",
    "agent_id": "agt_acme",
    "item_count": 1,
    "items": [{"event_id": "evt_1"}],
  }
  read.assert_called_once_with(
    session,
    "receivables",
    as_of=date(2026, 9, 30),
    entity_id="ent_sub",
    agent_id="agt_acme",
  )


@pytest.mark.parametrize(
  "arguments",
  [
    {"kind": "loans"},
    {"kind": "receivables", "scope": "world"},
    {"kind": "receivables", "scope": "group", "agent_id": "agt_acme"},
    {"kind": "receivables", "as_of": "30/09/2026"},
  ],
)
def test_bad_arguments_are_refused_before_the_read(tool, arguments):
  with patch(f"{MODULE}.extensions_session") as session:
    result = tool._execute_sync(arguments)

  assert result["error"] == "invalid_arguments"
  session.assert_not_called()


def test_an_entity_outside_the_graph_is_named(tool):
  with (
    _patch_session(),
    patch(
      f"{MODULE}.get_aged_balances",
      side_effect=EntityNotInGraphError("Entity 'ent_x' not found in this graph."),
    ),
  ):
    result = tool._execute_sync({"kind": "receivables", "entity_id": "ent_x"})

  assert result == {
    "error": "entity_not_found",
    "message": "Entity 'ent_x' not found in this graph.",
  }
