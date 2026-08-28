"""CypherOperator orientation prefetch — schema/examples in the system prefix.

Schema and example queries are deterministic per graph, so the operator
fetches them once up front and renders them into the (cached) system prompt
instead of leaving them as tools: the model gets a complete schema rather
than a truncated tool result, and a standard question loses two orientation
model calls. The tool-driven path survives as the fallback when the
prefetch fails.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.operations.operators.base import OperatorMode
from robosystems.operations.operators.implementations.cypher import CypherOperator
from robosystems.operations.operators.operator_context import OperatorContext
from robosystems.operations.operators.progress import NoOpProgress
from robosystems.operations.operators.tool_loop import ToolLoopResult

pytestmark = pytest.mark.asyncio

SCHEMA = [
  {"type": "node", "label": "Entity", "properties": [{"name": "name"}]},
  {"type": "relationship", "label": "ENTITY_HAS_REPORT", "from": "Entity"},
]
EXAMPLES = [{"category": "exploration", "query": "MATCH (e:Entity) RETURN e LIMIT 5"}]


def _tools(available: list[str], call_results: dict[str, object]) -> MagicMock:
  tools = MagicMock()
  tools.get_tool_schemas = AsyncMock(
    return_value=[
      {"name": name, "description": "d", "input_schema": {"type": "object"}}
      for name in available
    ]
  )

  async def call_tool(name, arguments, return_raw=False):
    result = call_results[name]
    if isinstance(result, Exception):
      raise result
    return result

  tools.call_tool = AsyncMock(side_effect=call_tool)
  return tools


def _ctx(tools: MagicMock) -> OperatorContext:
  return OperatorContext(
    graph_id="kg_test",
    user_id="u",
    query="Total expenses in July?",
    mode=OperatorMode.STANDARD,
    history=[],
    ai=MagicMock(),
    tools=tools,
    progress=NoOpProgress(),
  )


def _loop_result() -> ToolLoopResult:
  return ToolLoopResult(text="answer", iterations=2)


async def _run(tools: MagicMock):
  """Run the operator with the loop patched out; return the loop's kwargs."""
  with patch(
    "robosystems.operations.operators.implementations.cypher.run_tool_loop",
    AsyncMock(return_value=_loop_result()),
  ) as loop:
    await CypherOperator().run(_ctx(tools))
  return loop.await_args.kwargs


async def test_orientation_is_prefetched_into_the_system_prompt():
  tools = _tools(
    ["get-graph-schema", "get-example-queries", "read-graph-cypher"],
    {"get-graph-schema": SCHEMA, "get-example-queries": EXAMPLES},
  )
  kwargs = await _run(tools)

  system = kwargs["system"]
  assert "GRAPH SCHEMA" in system
  assert json.dumps(SCHEMA) in system
  assert "EXAMPLE QUERIES" in system
  assert json.dumps(EXAMPLES) in system
  # The prompt no longer routes the model through the orientation tools...
  assert "call `get-graph-schema` first" not in system
  # ...and the loop no longer offers them.
  assert kwargs["tool_names"] == [
    t
    for t in CypherOperator.READ_ONLY_TOOLS
    if t not in ("get-graph-schema", "get-example-queries")
  ]


async def test_prefetch_failure_falls_back_to_tool_driven_orientation():
  tools = _tools(
    ["get-graph-schema", "get-example-queries", "read-graph-cypher"],
    {
      "get-graph-schema": RuntimeError("graph API down"),
      "get-example-queries": EXAMPLES,
    },
  )
  kwargs = await _run(tools)

  assert "GRAPH SCHEMA" not in kwargs["system"]
  assert "call `get-graph-schema` first" in kwargs["system"]
  assert kwargs["tool_names"] == CypherOperator.READ_ONLY_TOOLS


async def test_error_dict_schema_falls_back():
  """Registrar-style tools report failure as {"error": ...} without raising —
  that must not end up rendered into the prompt as a schema."""
  tools = _tools(
    ["get-graph-schema", "read-graph-cypher"],
    {"get-graph-schema": {"error": "not ready"}},
  )
  kwargs = await _run(tools)

  assert "GRAPH SCHEMA" not in kwargs["system"]
  assert kwargs["tool_names"] == CypherOperator.READ_ONLY_TOOLS


async def test_graph_without_examples_gets_schema_only():
  """A generic graph exposes no get-example-queries; the schema section still
  lands and no examples section is emitted."""
  tools = _tools(
    ["get-graph-schema", "read-graph-cypher"],
    {"get-graph-schema": SCHEMA},
  )
  kwargs = await _run(tools)

  assert "GRAPH SCHEMA" in kwargs["system"]
  assert "EXAMPLE QUERIES (working patterns" not in kwargs["system"]
  tools.call_tool.assert_awaited_once()


async def test_oversized_orientation_is_truncated_deterministically():
  big = [{"label": "N" * 1000} for _ in range(100)]
  text = CypherOperator._serialize_orientation(big)
  assert len(text) <= 48000 + len("\n… [truncated]")
  assert text.endswith("[truncated]")


async def test_curated_tools_are_routed_ahead_of_cypher():
  """Where the graph exposes the curated financial reads, the prompt routes
  statement/balance/period questions to them first, and the loop offers
  them."""
  tools = _tools(
    [
      "get-graph-schema",
      "read-graph-cypher",
      "live-financial-statement",
      "get-fiscal-calendar",
    ],
    {"get-graph-schema": SCHEMA},
  )
  kwargs = await _run(tools)

  system = kwargs["system"]
  assert "CURATED TOOLS" in system
  assert "`live-financial-statement`" in system
  assert "`get-fiscal-calendar`" in system
  # Hints only for tools this graph actually exposes.
  assert "`build-fact-grid`" not in system
  assert "Call a curated tool where one fits" in system
  assert "live-financial-statement" in kwargs["tool_names"]


async def test_no_curated_section_without_curated_tools():
  tools = _tools(
    ["get-graph-schema", "read-graph-cypher"],
    {"get-graph-schema": SCHEMA},
  )
  kwargs = await _run(tools)
  assert "CURATED TOOLS" not in kwargs["system"]


async def test_max_credits_reaches_the_loop_and_garbage_is_ignored():
  tools = _tools(
    ["get-graph-schema", "read-graph-cypher"],
    {"get-graph-schema": SCHEMA},
  )
  with patch(
    "robosystems.operations.operators.implementations.cypher.run_tool_loop",
    AsyncMock(return_value=_loop_result()),
  ) as loop:
    ctx = _ctx(tools)
    ctx.extra["max_credits"] = 25
    await CypherOperator().run(ctx)
  assert loop.await_args.kwargs["max_credits"] == 25.0

  # Tenant-supplied garbage must not shape the loop.
  assert CypherOperator._get_max_credits(_ctx(tools)) is None
  for bad in ("abc", -5, 0, None, {"x": 1}):
    ctx = _ctx(tools)
    ctx.extra["max_credits"] = bad
    assert CypherOperator._get_max_credits(ctx) is None, bad
