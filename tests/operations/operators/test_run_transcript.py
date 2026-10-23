"""The tracked client keeps the run's transcript: one entry per model call,
carrying only what the call added when it continues the previous one."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from robosystems.operations.operators import tracked_ai as tracked_module
from robosystems.operations.operators.ai_client import AIMessage, AIResponse
from robosystems.operations.operators.credit_consumer import NoOpCreditConsumer
from robosystems.operations.operators.tracked_ai import TrackedAIClient

pytestmark = pytest.mark.asyncio


def _response(
  content: str = "",
  blocks: list[dict] | None = None,
  stop_reason: str = "end_turn",
) -> AIResponse:
  return AIResponse(
    content=content,
    model="us.anthropic.claude-sonnet-5-5",
    input_tokens=10,
    output_tokens=5,
    stop_reason=stop_reason,
    cache_read_input_tokens=100,
    content_blocks=blocks or ([{"text": content}] if content else []),
  )


def _client(responses: list[AIResponse]) -> TrackedAIClient:
  ai = MagicMock()
  ai.create_message = AsyncMock(side_effect=responses)
  return TrackedAIClient(
    ai_client=ai,
    graph_id="kg_test",
    user_id="usr_test",
    credit_consumer=NoOpCreditConsumer(),
  )


async def test_a_continuing_call_records_only_what_it_added():
  tool_use = [
    {"toolUse": {"toolUseId": "t1", "name": "read-graph-cypher", "input": {"q": 1}}}
  ]
  tracked = _client(
    [_response(blocks=tool_use, stop_reason="tool_use"), _response("42")]
  )
  tools = [{"name": "read-graph-cypher"}, {"name": "get-graph-schema"}]

  messages = [AIMessage(role="user", content="How many?")]
  first = await tracked.create_message(messages=messages, system="SYSTEM", tools=tools)
  # The tool loop replays the output verbatim, then appends the tool results.
  messages.append(AIMessage(role="assistant", content=first.content_blocks))
  messages.append(
    AIMessage(
      role="user", content=[{"toolResult": {"toolUseId": "t1", "content": "[]"}}]
    )
  )
  await tracked.create_message(messages=messages, system="SYSTEM", tools=tools)

  assert [e["call"] for e in tracked.transcript] == [1, 2]
  opening, follow = tracked.transcript
  assert opening["continues"] is False
  assert opening["input"] == [{"role": "user", "content": "How many?"}]
  assert opening["output"] == tool_use
  assert opening["stop_reason"] == "tool_use"
  assert opening["tools"] == ["get-graph-schema", "read-graph-cypher"]
  assert opening["system_chars"] == len("SYSTEM")
  assert opening["usage"] == {
    "input": 10,
    "output": 5,
    "cache_read": 100,
    "cache_write": 0,
  }
  assert opening["billed"] is True

  assert follow["continues"] is True
  assert follow["input"] == [
    {"role": "user", "content": [{"toolResult": {"toolUseId": "t1", "content": "[]"}}]}
  ]
  assert follow["output"] == [{"text": "42"}]
  # Unchanged prefix and tools are not repeated.
  assert "tools" not in follow
  assert "system_sha256" not in follow


async def test_a_fresh_conversation_records_its_whole_input():
  tracked = _client([_response("a"), _response("b")])
  await tracked.create_message(messages=[AIMessage(role="user", content="batch 1")])
  await tracked.create_message(messages=[AIMessage(role="user", content="batch 2")])

  assert [e["continues"] for e in tracked.transcript] == [False, False]
  assert tracked.transcript[1]["input"] == [{"role": "user", "content": "batch 2"}]


async def test_a_system_prompt_change_is_noted():
  tracked = _client([_response("a"), _response("b")])
  await tracked.create_message(
    messages=[AIMessage(role="user", content="x")], system="one"
  )
  await tracked.create_message(
    messages=[AIMessage(role="user", content="x")], system="two"
  )

  assert (
    tracked.transcript[0]["system_sha256"] != tracked.transcript[1]["system_sha256"]
  )


async def test_an_unbilled_call_is_recorded_as_such():
  consumer = MagicMock()
  consumer.consume = AsyncMock(side_effect=RuntimeError("ledger down"))
  ai = MagicMock()
  ai.create_message = AsyncMock(return_value=_response("a"))
  tracked = TrackedAIClient(ai, "kg_test", "usr_test", credit_consumer=consumer)

  await tracked.create_message(messages=[AIMessage(role="user", content="x")])

  assert tracked.transcript[0]["billed"] is False
  assert tracked.transcript[0]["credits"] is None
  assert tracked.last_call_unbilled


async def test_inputs_are_elided_past_the_budget(monkeypatch):
  monkeypatch.setattr(tracked_module, "TRANSCRIPT_CHAR_BUDGET", 300)
  tracked = _client([_response("a"), _response("b")])
  big = "x" * 400
  await tracked.create_message(messages=[AIMessage(role="user", content=big)])
  await tracked.create_message(messages=[AIMessage(role="user", content=big)])

  first, second = tracked.transcript
  assert first["input"] == tracked_module._ELIDED
  assert second["input"] == tracked_module._ELIDED
  # Usage survives elision, so the cost record stays whole.
  assert second["usage"]["input"] == 10
  json.dumps(tracked.transcript)
