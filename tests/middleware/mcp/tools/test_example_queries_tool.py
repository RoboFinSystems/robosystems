"""Tests for ExampleQueriesTool's ledger guidance."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from robosystems.middleware.mcp.tools.constants import (
  INVESTOR_AMOUNT_GUIDANCE,
  LEDGER_AMOUNT_GUIDANCE,
)
from robosystems.middleware.mcp.tools.example_queries_tool import ExampleQueriesTool


def _tool(
  labels: list[str], graph_id: str = "kg1a0b70352e2fdcc071f1"
) -> ExampleQueriesTool:
  schema = [{"label": label, "type": "node"} for label in labels]
  client = SimpleNamespace(
    graph_id=graph_id,
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


@pytest.mark.asyncio
async def test_investor_graph_states_position_amounts_are_dollars():
  examples = await _tool(["Portfolio", "Position", "Security"]).execute({})

  investor = [e for e in examples if e["category"] == "investor"]
  assert [e.get("info") for e in investor] == [INVESTOR_AMOUNT_GUIDANCE]


@pytest.mark.asyncio
async def test_graph_without_positions_omits_investor_guidance():
  examples = await _tool(["Entry", "LineItem", "Element"]).execute({})

  assert all(e.get("info") != INVESTOR_AMOUNT_GUIDANCE for e in examples)


@pytest.mark.asyncio
async def test_shared_repo_examples_pass_the_repository_read_limits():
  """An example the repository would refuse teaches agents a refused shape."""
  from robosystems.middleware.graph.statement_kernel import (
    shared_repository_read_refusal,
  )

  labels = ["Entity", "Report", "Fact", "Element", "Period", "Dimension", "Structure"]
  examples = await _tool(labels, graph_id="sec").execute({})

  queries = [e["query"] for e in examples if e.get("query")]
  assert any("Fact" in q for q in queries), "expected the financial examples"
  refused = {q: r for q in queries if (r := shared_repository_read_refusal("sec", q))}
  assert refused == {}
