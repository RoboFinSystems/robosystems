"""The reporting group an MCP session is told about names its parent by the
resolver's rule, never by a row's own say-so."""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from robosystems.middleware.mcp.entities import ledger_entities

pytestmark = pytest.mark.unit

MODULE = "robosystems.middleware.mcp.entities"


def _row(id, name, *, is_parent, created_at, source="native", parent_entity_id=None):
  row = MagicMock()
  row.id, row.name, row.is_parent, row.created_at = id, name, is_parent, created_at
  row.source, row.parent_entity_id = source, parent_entity_id
  row.ticker, row.entity_type = None, "llc"
  return row


@contextmanager
def _rows(rows):
  cm = MagicMock()
  cm.__enter__ = MagicMock(return_value=MagicMock())
  cm.__exit__ = MagicMock(return_value=False)
  with (
    patch(f"{MODULE}.extensions_session", return_value=cm),
    patch(f"{MODULE}.list_entities", return_value=rows),
  ):
    yield


def test_the_parent_is_the_earliest_is_parent_row_that_is_not_linked():
  """A legacy row marked is_parent under a parent, or a linked counterparty
  made first, must not be reported as the group parent."""
  rows = [
    _row(
      "ent_linked",
      "Sender Co",
      is_parent=True,
      created_at="2026-01-01",
      source="linked",
    ),
    _row(
      "ent_sub",
      "Maple Court LLC",
      is_parent=False,
      created_at="2026-02-01",
      parent_entity_id="ent_parent",
    ),
    _row("ent_parent", "Harbor Holdings", is_parent=True, created_at="2026-01-15"),
    _row(
      "ent_legacy",
      "Old Row",
      is_parent=True,
      created_at="2026-03-01",
      parent_entity_id="ent_parent",
    ),
  ]
  with _rows(rows):
    group = ledger_entities("kg_x")

  assert group is not None
  assert [e["id"] for e in group] == ["ent_parent", "ent_sub", "ent_legacy"]
  assert [e["is_group_parent"] for e in group] == [True, False, False]


def test_a_graph_with_no_ledger_says_nothing():
  with patch(f"{MODULE}.extensions_session", side_effect=ValueError("no schema")):
    assert ledger_entities("kg_x") is None
