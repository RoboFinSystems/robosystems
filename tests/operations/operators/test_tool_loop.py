"""Tests for the bounded tool-use loop (run_tool_loop).

The load-bearing behavior is error feedback: when a tool call fails, the loop
must feed the error back to the model as an ``is_error`` tool_result so it can
self-correct — the thing the old single-shot pipeline never did.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from robosystems.operations.operators.ai_client import AIResponse
from robosystems.operations.operators.base import OperatorMode
from robosystems.operations.operators.operator_context import OperatorContext
from robosystems.operations.operators.progress import NoOpProgress
from robosystems.operations.operators.tool_loop import run_tool_loop

pytestmark = pytest.mark.asyncio


def _tool_use(tool_id: str, name: str, **inp) -> AIResponse:
  """A model turn that calls one tool."""
  return AIResponse(
    content="",
    model="m",
    input_tokens=10,
    output_tokens=5,
    stop_reason="tool_use",
    content_blocks=[{"type": "tool_use", "id": tool_id, "name": name, "input": inp}],
  )


def _final(text: str) -> AIResponse:
  """A model turn that answers in natural language."""
  return AIResponse(
    content=text,
    model="m",
    input_tokens=10,
    output_tokens=5,
    stop_reason="end_turn",
    content_blocks=[{"type": "text", "text": text}],
  )


def _tools_mock(call_results: list) -> MagicMock:
  tools = MagicMock()
  tools.get_tool_schemas = AsyncMock(
    return_value=[
      {
        "name": "read-graph-cypher",
        "description": "run cypher",
        "input_schema": {"type": "object"},
      }
    ]
  )
  tools.call_tool = AsyncMock(side_effect=call_results)
  return tools


def _ctx(ai: MagicMock, tools: MagicMock, query: str = "How many companies?"):
  return OperatorContext(
    graph_id="kg_test",
    user_id="u",
    query=query,
    mode=OperatorMode.STANDARD,
    history=[],
    ai=ai,
    tools=tools,
    progress=NoOpProgress(),
  )


def _tool_result_blocks(messages: list) -> list[dict]:
  """Flatten every tool_result block across a message list."""
  return [
    block
    for msg in messages
    if isinstance(msg.content, list)
    for block in msg.content
    if isinstance(block, dict) and block.get("type") == "tool_result"
  ]


async def test_happy_path_captures_rows_and_cypher():
  ai = MagicMock()
  ai.create_message = AsyncMock(
    side_effect=[
      _tool_use("t1", "read-graph-cypher", query="MATCH (e:Entity) RETURN count(e)"),
      _final("There are 42 companies."),
    ]
  )
  rows = [{"count": 42}]
  tools = _tools_mock(call_results=[rows])
  ctx = _ctx(ai, tools)

  result = await run_tool_loop(
    ctx,
    system="sys",
    tool_names=["read-graph-cypher"],
    max_iterations=5,
    max_tokens=1000,
  )

  assert result.text == "There are 42 companies."
  assert result.rows == rows
  assert result.cypher == "MATCH (e:Entity) RETURN count(e)"
  assert result.tools_called == ["read-graph-cypher"]
  assert result.iterations == 2
  assert result.hit_cap is False
  tools.get_tool_schemas.assert_awaited_once_with(["read-graph-cypher"])
  # Tools were handed to the model on the first turn.
  assert ai.create_message.call_args_list[0].kwargs["tools"]


async def test_tool_error_is_fed_back_and_model_retries():
  """A raised tool error (cypher_tool raises ValueError on a bad query) is
  returned to the model as an is_error tool_result, and the model retries."""
  ai = MagicMock()
  ai.create_message = AsyncMock(
    side_effect=[
      _tool_use("t1", "read-graph-cypher", query="MATCH bad syntax"),
      _tool_use("t2", "read-graph-cypher", query="MATCH (e:Entity) RETURN e LIMIT 1"),
      _final("Fixed the query and found the answer."),
    ]
  )
  rows = [{"e": 1}]
  tools = _tools_mock(call_results=[ValueError("Invalid Cypher near 'bad'"), rows])
  ctx = _ctx(ai, tools)

  result = await run_tool_loop(
    ctx,
    system="sys",
    tool_names=["read-graph-cypher"],
    max_iterations=5,
    max_tokens=1000,
  )

  assert result.text == "Fixed the query and found the answer."
  assert result.rows == rows
  assert result.cypher == "MATCH (e:Entity) RETURN e LIMIT 1"
  assert result.tools_called == ["read-graph-cypher", "read-graph-cypher"]
  assert result.iterations == 3
  assert result.hit_cap is False

  # The SECOND model call must have seen the error as an is_error tool_result.
  second_call_messages = ai.create_message.call_args_list[1].kwargs["messages"]
  blocks = _tool_result_blocks(second_call_messages)
  assert any(b.get("is_error") for b in blocks)
  assert any("Invalid Cypher" in b.get("content", "") for b in blocks)


async def test_empty_followup_does_not_clobber_captured_rows():
  """A later zero-row read-graph-cypher must not wipe the rows from an earlier
  successful query (finding #2 — last-non-empty wins)."""
  ai = MagicMock()
  ai.create_message = AsyncMock(
    side_effect=[
      _tool_use("t1", "read-graph-cypher", query="MATCH (e:Entity) RETURN e LIMIT 5"),
      _tool_use(
        "t2", "read-graph-cypher", query="MATCH (e:Entity) WHERE e.name='nope' RETURN e"
      ),
      _final("Here are the entities."),
    ]
  )
  good_rows = [{"e": 1}, {"e": 2}, {"e": 3}]
  tools = _tools_mock(call_results=[good_rows, []])  # second query returns []
  ctx = _ctx(ai, tools)

  result = await run_tool_loop(
    ctx,
    system="s",
    tool_names=["read-graph-cypher"],
    max_iterations=5,
    max_tokens=100,
  )

  assert result.rows == good_rows  # not clobbered by the empty follow-up
  assert result.cypher == "MATCH (e:Entity) RETURN e LIMIT 5"


async def test_error_dict_result_is_flagged_and_not_captured_as_rows():
  """Registrar/domain tools report failure as {"error": ...} rather than
  raising — that path must also be fed back as is_error and not mistaken for
  a result set."""
  ai = MagicMock()
  ai.create_message = AsyncMock(
    side_effect=[
      _tool_use("t1", "read-graph-cypher", query="MATCH (n) RETURN n"),
      _final("done"),
    ]
  )
  tools = _tools_mock(call_results=[{"error": "boom", "message": "bad request"}])
  ctx = _ctx(ai, tools)

  result = await run_tool_loop(
    ctx,
    system="s",
    tool_names=["read-graph-cypher"],
    max_iterations=5,
    max_tokens=100,
  )

  assert result.rows is None  # error dict is not a result set
  blocks = _tool_result_blocks(ai.create_message.call_args_list[1].kwargs["messages"])
  assert any(b.get("is_error") for b in blocks)


async def test_hits_iteration_cap_then_forces_a_final_answer():
  ai = MagicMock()
  # The model never stops asking for tools → the loop must cap it.
  ai.create_message = AsyncMock(
    side_effect=[
      _tool_use("t1", "read-graph-cypher", query="MATCH (n) RETURN n LIMIT 1"),
      _tool_use("t2", "read-graph-cypher", query="MATCH (n) RETURN n LIMIT 1"),
      _final("Best-effort answer from what I gathered."),
    ]
  )
  rows = [{"n": 1}]
  tools = _tools_mock(call_results=[rows, rows])
  ctx = _ctx(ai, tools)

  result = await run_tool_loop(
    ctx,
    system="s",
    tool_names=["read-graph-cypher"],
    max_iterations=2,
    max_tokens=100,
  )

  assert result.hit_cap is True
  assert result.iterations == 2
  assert result.text == "Best-effort answer from what I gathered."
  # 2 loop iterations + 1 forced-final turn.
  assert ai.create_message.await_count == 3

  final_kwargs = ai.create_message.call_args_list[2].kwargs
  assert final_kwargs["tools"]  # transcript stays valid
  # The answer-now nudge is appended to the trailing user turn (no second
  # consecutive user message).
  last_user = final_kwargs["messages"][-1]
  assert last_user.role == "user"
  assert any(
    isinstance(b, dict)
    and b.get("type") == "text"
    and "step limit" in b.get("text", "")
    for b in last_user.content
  )


async def test_unadvertised_tool_is_refused_without_dispatch():
  """The advertised set is enforced at dispatch, not just at advertisement.

  get_tool_schemas filters what the model is SHOWN, but the model can emit
  any name it likes — and call_tool dispatches whatever the underlying
  manager exposes, including write tools when the surface was built
  write-capable. A name outside the advertised set must be refused before
  call_tool, not after.
  """
  ai = MagicMock()
  ai.create_message = AsyncMock(
    side_effect=[
      _tool_use("t1", "delete-journal-entry", entry_id="je_1"),
      _final("That tool isn't available."),
    ]
  )
  tools = _tools_mock(call_results=[])
  ctx = _ctx(ai, tools)

  result = await run_tool_loop(
    ctx,
    system="sys",
    tool_names=["read-graph-cypher"],
    max_iterations=5,
    max_tokens=1000,
  )

  tools.call_tool.assert_not_awaited()
  assert result.text == "That tool isn't available."

  # The refusal is fed back to the model as an is_error tool_result so the
  # transcript stays valid and the model can continue.
  second_turn_messages = ai.create_message.call_args_list[1].kwargs["messages"]
  blocks = _tool_result_blocks(second_turn_messages)
  assert len(blocks) == 1
  assert blocks[0]["is_error"] is True
  assert "not available" in blocks[0]["content"]
