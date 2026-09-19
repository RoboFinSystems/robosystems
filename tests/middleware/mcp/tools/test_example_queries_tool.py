"""Tests for ExampleQueriesTool's ledger guidance."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from robosystems.middleware.mcp.tools.constants import LEDGER_AMOUNT_GUIDANCE
from robosystems.middleware.mcp.tools.example_queries_tool import ExampleQueriesTool


def _tool(labels: list[str]) -> ExampleQueriesTool:
  schema = [{"label": label, "type": "node"} for label in labels]
  client = SimpleNamespace(
    graph_id="kg1a0b70352e2fdcc071f1",
    get_schema=AsyncMock(return_value=schema),
  )
  return ExampleQueriesTool(client)


@pytest.mark.asyncio
async def test_ledger_graph_states_amounts_are_dollars():
  examples = await _tool(["Entry", "LineItem", "Element"]).execute({})

  ledger_infos = [e.get("info") for e in examples if e["category"] == "ledger"]
  assert LEDGER_AMOUNT_GUIDANCE in ledger_infos
  # Read before the first example query, alongside the anchor and status rules.
  first_query = next(i for i, info in enumerate(ledger_infos) if info is None)
  assert ledger_infos.index(LEDGER_AMOUNT_GUIDANCE) < first_query


@pytest.mark.asyncio
async def test_graph_without_ledger_spine_omits_it():
  examples = await _tool(["Entity", "Fact", "Element"]).execute({})

  assert all(e.get("info") != LEDGER_AMOUNT_GUIDANCE for e in examples)
