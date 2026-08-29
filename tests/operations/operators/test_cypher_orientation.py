"""CypherOperator prefetches — schema/examples in the system prefix, memories
in the user turn.

Schema and example queries are deterministic per graph, so the operator
fetches them once up front and renders them into the (cached) system prompt
instead of leaving them as tools: the model gets a complete schema rather
than a truncated tool result, and a standard question loses two orientation
model calls. The tool-driven path survives as the fallback when the
prefetch fails.

Semantic memory is prefetched the same way but lands in the opposite place:
the hits depend on the question, and they are tenant data, so they are
rendered ahead of the question in the user turn and never enter the cached
prefix.
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


# ── Semantic memory prefetch ─────────────────────────────────────────────

MEMORIES = {
  "total": 2,
  "results": [
    {
      "id": "m1",
      "score": 0.82,
      "text": "Software subscriptions are coded to 6200 Dues & Subscriptions,\nnot 6100.",
      "tags": ["coa"],
    },
    {
      "id": "m2",
      "score": 0.71,
      "text": "July 2026 close: Amex feed lagged.",
      "tags": None,
    },
  ],
}


async def test_memories_are_prefetched_into_the_user_turn_not_the_prefix():
  tools = _tools(
    ["get-graph-schema", "read-graph-cypher", "recall"],
    {"get-graph-schema": SCHEMA, "recall": MEMORIES},
  )
  kwargs = await _run(tools)

  user_message = kwargs["user_message"]
  assert user_message.startswith("REMEMBERED CONTEXT")
  # Whitespace is collapsed, tags ride along, the question closes the turn.
  assert (
    "- Software subscriptions are coded to 6200 Dues & Subscriptions, not 6100. (tags: coa)"
    in user_message
  )
  assert "- July 2026 close: Amex feed lagged.\n" in user_message
  assert user_message.endswith("QUESTION: Total expenses in July?")
  # Tenant data never enters the cached system prefix; the routing note does.
  assert "6200 Dues" not in kwargs["system"]
  assert "MEMORY (" in kwargs["system"]
  # The question was recalled with the question, and `recall` stays offered
  # for a follow-up on a term that surfaces mid-investigation.
  tools.call_tool.assert_any_await(
    "recall", {"query": "Total expenses in July?", "k": 5}, return_raw=True
  )
  assert "recall" in kwargs["tool_names"]


async def test_empty_memory_store_sends_the_bare_question():
  tools = _tools(
    ["get-graph-schema", "read-graph-cypher", "recall"],
    {"get-graph-schema": SCHEMA, "recall": {"total": 0, "results": []}},
  )
  kwargs = await _run(tools)
  assert kwargs["user_message"] is None
  assert "MEMORY (" in kwargs["system"]


@pytest.mark.parametrize(
  "failure",
  [
    RuntimeError("memory store down"),
    {"error": "disabled", "message": "Semantic memory is not enabled."},
    {"results": [{"id": "m", "text": "   "}]},
    "not a dict",
  ],
)
async def test_memory_prefetch_failure_is_silent(failure):
  """A failed or empty recall must never cost the question — orientation
  still lands and the question goes in bare."""
  tools = _tools(
    ["get-graph-schema", "read-graph-cypher", "recall"],
    {"get-graph-schema": SCHEMA, "recall": failure},
  )
  kwargs = await _run(tools)
  assert kwargs["user_message"] is None
  assert "GRAPH SCHEMA" in kwargs["system"]


async def test_graph_without_recall_gets_no_memory_prefetch():
  """SEC and memory-disabled deployments expose no `recall`: no call, no note."""
  tools = _tools(
    ["get-graph-schema", "read-graph-cypher"],
    {"get-graph-schema": SCHEMA},
  )
  kwargs = await _run(tools)
  assert kwargs["user_message"] is None
  assert "MEMORY (" not in kwargs["system"]
  assert all(c.args[0] != "recall" for c in tools.call_tool.await_args_list)


def test_oversized_memories_are_capped_per_hit():
  hits = [{"id": str(i), "text": "x" * 5000, "tags": None} for i in range(5)]
  message = CypherOperator._build_user_message("q", hits)
  assert message is not None
  assert message.count("x" * 800 + "…") == 5
  assert "x" * 801 not in message


async def test_document_search_prompt_says_when_to_go_semantic():
  """`search-documents` is BM25 unless told otherwise; the prompt has to say so
  or meaning-shaped questions under-retrieve on keyword matching."""
  tools = _tools(
    ["get-graph-schema", "read-graph-cypher", "search-documents"],
    {"get-graph-schema": SCHEMA},
  )
  kwargs = await _run(tools)
  assert "keyword (BM25) search by default; pass `semantic=true`" in kwargs["system"]
