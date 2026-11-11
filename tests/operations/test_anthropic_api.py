"""Tests for the Anthropic API provider (local development).

As with the self-hosted provider, Converse stays the canonical transcript:
the tool loop builds and replays Converse blocks, and this provider must
translate them to the Messages API and back without the loop noticing. Here
a thinking block must also survive the round trip exactly, since the API
checks its signature when it is replayed.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import anthropic
import httpx
import pytest

from robosystems.config.operators import (
  ModelProvider,
  OperatorModel,
  anthropic_api_models,
)
from robosystems.operations.operators import anthropic_api
from robosystems.operations.operators.ai_client import (
  AIClient,
  AIMessage,
  AIProviderError,
  text_block,
  tool_result_block,
)
from robosystems.operations.operators.anthropic_api import (
  AnthropicAPIClient,
  build_messages_request,
  parse_messages_response,
  to_converse_blocks,
  to_messages_content,
)

MODELS = anthropic_api_models(True, "sk-test", "dev")
SONNET = MODELS[OperatorModel.SONNET_5_5]
SONNET_4_6 = MODELS[OperatorModel.SONNET_4_6]

CYPHER_TOOL = {
  "name": "read-graph-cypher",
  "description": "run cypher",
  "inputSchema": {"type": "object", "properties": {"query": {"type": "string"}}},
}

THINKING = {"type": "thinking", "thinking": "", "signature": "sig-abc"}
TOOL_USE = {
  "type": "tool_use",
  "id": "toolu_1",
  "name": "read-graph-cypher",
  "input": {"query": "MATCH (n) RETURN n"},
}


def _payload(content: list[dict], stop_reason: str = "end_turn", **usage) -> dict:
  return {
    "content": content,
    "stop_reason": stop_reason,
    "usage": {"input_tokens": 10, "output_tokens": 5, **usage},
  }


class TestRequest:
  def test_tools_system_and_the_registry_fields_take_their_messages_api_form(self):
    request = build_messages_request(
      SONNET, [AIMessage("user", "hi")], "be brief", 4000, 0.7, [CYPHER_TOOL]
    )

    assert request["model"] == "claude-sonnet-5-5"
    assert request["max_tokens"] == 4000
    assert request["thinking"] == {"type": "adaptive"}
    assert "temperature" not in request
    assert request["system"] == [
      {"type": "text", "text": "be brief", "cache_control": {"type": "ephemeral"}}
    ]
    assert request["tools"] == [
      {
        "name": "read-graph-cypher",
        "description": "run cypher",
        "input_schema": CYPHER_TOOL["inputSchema"],
      }
    ]
    assert request["messages"] == [
      {"role": "user", "content": [{"type": "text", "text": "hi"}]}
    ]

  def test_effort_reaches_only_a_row_that_takes_it(self):
    with_effort = build_messages_request(
      SONNET, [AIMessage("user", "hi")], None, 100, 0.7, None, effort="low"
    )
    without = build_messages_request(
      SONNET_4_6, [AIMessage("user", "hi")], None, 100, 0.7, None, effort="low"
    )

    assert with_effort["output_config"] == {"effort": "low"}
    assert "output_config" not in without
    assert without["temperature"] == 0.7

  def test_caching_the_conversation_marks_only_the_last_block_of_the_request(self):
    messages = [
      AIMessage("user", "first"),
      AIMessage("assistant", [text_block("ok")]),
      AIMessage("user", [tool_result_block("toolu_1", "rows"), text_block("go on")]),
    ]

    request = build_messages_request(
      SONNET, messages, None, 100, 0.7, None, cache_conversation=True
    )

    marked = [
      block
      for message in request["messages"]
      for block in message["content"]
      if "cache_control" in block
    ]
    assert marked == [
      {"type": "text", "text": "go on", "cache_control": {"type": "ephemeral"}}
    ]
    # The transcript the loop keeps is untouched.
    assert messages[-1].content[-1] == {"text": "go on"}


class TestTranslation:
  def test_a_tool_turn_round_trips(self):
    blocks = to_converse_blocks(
      [THINKING, {"type": "text", "text": "Looking."}, TOOL_USE]
    )

    assert blocks[1] == {"text": "Looking."}
    assert blocks[2] == {
      "toolUse": {
        "toolUseId": "toolu_1",
        "name": "read-graph-cypher",
        "input": {"query": "MATCH (n) RETURN n"},
      }
    }
    assert to_messages_content(blocks) == [
      THINKING,
      {"type": "text", "text": "Looking."},
      TOOL_USE,
    ]

  def test_redacted_thinking_and_unknown_blocks_go_back_as_they_came(self):
    redacted = {"type": "redacted_thinking", "data": "opaque"}
    unknown = {"type": "server_tool_use", "id": "srv_1", "name": "x", "input": {}}

    assert to_messages_content(to_converse_blocks([redacted, unknown])) == [
      redacted,
      unknown,
    ]

  def test_an_error_result_still_reads_as_an_error(self):
    content = to_messages_content(
      [tool_result_block("toolu_1", "no such label", is_error=True)]
    )

    assert content == [
      {
        "type": "tool_result",
        "tool_use_id": "toolu_1",
        "content": [{"type": "text", "text": "no such label"}],
        "is_error": True,
      }
    ]

  def test_bedrocks_cache_marker_is_dropped(self):
    assert to_messages_content(
      [text_block("a"), {"cachePoint": {"type": "default"}}]
    ) == [{"type": "text", "text": "a"}]


class TestResponse:
  def test_usage_text_and_tool_calls_are_read(self):
    response = parse_messages_response(
      _payload(
        [{"type": "text", "text": "Looking."}, TOOL_USE],
        stop_reason="tool_use",
        cache_read_input_tokens=900,
        cache_creation_input_tokens=30,
      ),
      SONNET,
    )

    assert response.content == "Looking."
    assert response.model == "claude-sonnet-5-5"
    assert response.stop_reason == "tool_use"
    assert (response.input_tokens, response.output_tokens) == (10, 5)
    assert response.cache_read_input_tokens == 900
    assert response.cache_creation_input_tokens == 30
    assert [(c.id, c.name, c.input) for c in response.tool_calls] == [
      ("toolu_1", "read-graph-cypher", {"query": "MATCH (n) RETURN n"})
    ]

  def test_a_refusal_reads_as_the_stop_reason_the_loop_knows(self):
    response = parse_messages_response(_payload([], stop_reason="refusal"), SONNET)

    assert response.stop_reason == "content_filtered"


class _FakeMessage:
  def __init__(self, payload: dict) -> None:
    self._payload = payload

  def to_dict(self) -> dict:
    return self._payload


def _status_error(status: int) -> anthropic.APIStatusError:
  request = httpx.Request("POST", "https://api.anthropic.com/v1/messages")
  return anthropic.APIStatusError(
    "refused", response=httpx.Response(status, request=request), body=None
  )


def _fake_sdk(outcomes: list[Any], seen: list[dict]):
  """An AsyncAnthropic stand-in that answers each call with the next outcome."""

  class _Messages:
    async def create(self, **request):
      seen.append(request)
      outcome = outcomes.pop(0)
      if isinstance(outcome, Exception):
        raise outcome
      return _FakeMessage(outcome)

  class _Client:
    def __init__(self, **kwargs) -> None:
      self.kwargs = kwargs
      self.messages = _Messages()

    async def __aenter__(self):
      return self

    async def __aexit__(self, *exc) -> None:
      return None

  return _Client


async def _call(client: AnthropicAPIClient):
  return await client.create_message(
    SONNET, [AIMessage("user", "hi")], None, 100, 0.7, None, False, None
  )


class TestClient:
  @pytest.fixture(autouse=True)
  def _no_backoff(self, monkeypatch):
    async def _instant(_seconds):
      return None

    monkeypatch.setattr(anthropic_api.asyncio, "sleep", _instant)

  @pytest.mark.asyncio
  async def test_a_call_returns_the_parsed_answer(self):
    seen: list[dict] = []
    sdk = _fake_sdk([_payload([{"type": "text", "text": "hello"}])], seen)
    with patch.object(anthropic_api.anthropic, "AsyncAnthropic", sdk):
      response = await _call(AnthropicAPIClient("sk-test"))

    assert response.content == "hello"
    assert seen[0]["model"] == "claude-sonnet-5-5"

  @pytest.mark.asyncio
  async def test_a_refusal_before_generation_is_retried(self):
    seen: list[dict] = []
    sdk = _fake_sdk(
      [_status_error(529), _payload([{"type": "text", "text": "hello"}])], seen
    )
    with patch.object(anthropic_api.anthropic, "AsyncAnthropic", sdk):
      response = await _call(AnthropicAPIClient("sk-test"))

    assert response.content == "hello"
    assert len(seen) == 2

  @pytest.mark.asyncio
  @pytest.mark.parametrize("status", [400, 401, 403, 404])
  async def test_a_refused_call_fails_the_operation_at_once(self, status):
    seen: list[dict] = []
    sdk = _fake_sdk([_status_error(status)], seen)
    with (
      patch.object(anthropic_api.anthropic, "AsyncAnthropic", sdk),
      pytest.raises(AIProviderError, match=f"HTTP {status}"),
    ):
      await _call(AnthropicAPIClient("sk-test"))

    assert len(seen) == 1

  @pytest.mark.asyncio
  async def test_a_timeout_is_not_sent_again(self):
    seen: list[dict] = []
    request = httpx.Request("POST", "https://api.anthropic.com/v1/messages")
    sdk = _fake_sdk([anthropic.APITimeoutError(request=request)], seen)
    with (
      patch.object(anthropic_api.anthropic, "AsyncAnthropic", sdk),
      pytest.raises(AIProviderError, match="not re-sent"),
    ):
      await _call(AnthropicAPIClient("sk-test"))

    assert len(seen) == 1


class TestRouting:
  @pytest.mark.asyncio
  async def test_a_row_served_by_the_api_never_reaches_bedrock(self):
    client = AIClient.__new__(AIClient)
    client.client = None
    client._self_hosted = None
    client._anthropic = AnthropicAPIClient("sk-test")
    seen: list[dict] = []
    sdk = _fake_sdk([_payload([{"type": "text", "text": "hello"}])], seen)
    with (
      patch.object(anthropic_api.anthropic, "AsyncAnthropic", sdk),
      patch(
        "robosystems.operations.operators.ai_client.OperatorConfig.resolve_model",
        return_value=SONNET,
      ),
    ):
      response = await client.create_message([AIMessage("user", "hi")], effort="low")

    assert response.content == "hello"
    assert seen[0]["output_config"] == {"effort": "low"}

  @pytest.mark.asyncio
  async def test_a_row_served_by_the_api_with_the_route_off_is_refused(self):
    client = AIClient.__new__(AIClient)
    client._self_hosted = None
    client._anthropic = None
    with (
      patch(
        "robosystems.operations.operators.ai_client.OperatorConfig.resolve_model",
        return_value=SONNET,
      ),
      pytest.raises(AIProviderError, match="ANTHROPIC_API_ENABLED is off"),
    ):
      await client.create_message([AIMessage("user", "hi")])

  def test_the_rows_keep_everything_but_the_wire_id_and_the_provider(self):
    assert SONNET.provider is ModelProvider.ANTHROPIC
    assert SONNET.pricing_key == "anthropic_claude_5_sonnet"
    assert SONNET.supports_effort is True
