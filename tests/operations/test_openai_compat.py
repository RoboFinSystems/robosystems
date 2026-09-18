"""Tests for the self-hosted OpenAI-compatible provider.

The load-bearing property is that Converse stays the canonical transcript:
the tool loop builds and replays Converse blocks, and this provider must
translate them to Chat Completions and back without the loop noticing —
tool calls round-trip, error results still read as errors, and reasoning
never gets replayed.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from robosystems.config.operators import (
  ModelProfile,
  OperatorConfig,
  OperatorModel,
  self_hosted_model_spec,
)
from robosystems.operations.operators.ai_client import (
  AIClient,
  AIMessage,
  AIProviderError,
)
from robosystems.operations.operators.base import OperatorMode
from robosystems.operations.operators.openai_compat import (
  UNPARSED_ARGUMENTS_KEY,
  OpenAICompatClient,
  build_chat_request,
  parse_chat_response,
  to_chat_messages,
)
from robosystems.operations.operators.operator_context import OperatorContext
from robosystems.operations.operators.progress import NoOpProgress
from robosystems.operations.operators.tool_loop import run_tool_loop
from robosystems.operations.operators.tracked_ai import TrackedAIClient

SPEC = self_hosted_model_spec("qwen3:32b", 0)
URL = "http://llm.local:8080/v1"

CYPHER_TOOL = {
  "name": "read-graph-cypher",
  "description": "run cypher",
  "inputSchema": {"type": "object", "properties": {"query": {"type": "string"}}},
}


def _completion(
  content: str | None = None,
  tool_calls: list[dict] | None = None,
  finish_reason: str = "stop",
  usage: dict | None = None,
) -> dict:
  message: dict = {"role": "assistant", "content": content}
  if tool_calls:
    message["tool_calls"] = tool_calls
  return {
    "choices": [{"index": 0, "message": message, "finish_reason": finish_reason}],
    "usage": usage or {"prompt_tokens": 100, "completion_tokens": 20},
  }


def _call(call_id: str, name: str, arguments: str) -> dict:
  return {
    "id": call_id,
    "type": "function",
    "function": {"name": name, "arguments": arguments},
  }


class TestToChatMessages:
  def test_system_and_plain_turns(self):
    out = to_chat_messages(
      [AIMessage(role="user", content="hi"), AIMessage(role="assistant", content="yo")],
      system="be brief",
    )
    assert out == [
      {"role": "system", "content": "be brief"},
      {"role": "user", "content": "hi"},
      {"role": "assistant", "content": "yo"},
    ]

  def test_assistant_tool_use_becomes_tool_calls_and_reasoning_is_dropped(self):
    blocks = [
      {"reasoningContent": {"reasoningText": {"text": "hmm", "signature": "s"}}},
      {"text": "Let me check."},
      {"toolUse": {"toolUseId": "t1", "name": "read-graph-cypher", "input": {"q": 1}}},
    ]
    [message] = to_chat_messages([AIMessage(role="assistant", content=blocks)], None)
    assert message == {
      "role": "assistant",
      "content": "Let me check.",
      "tool_calls": [
        {
          "id": "t1",
          "type": "function",
          "function": {"name": "read-graph-cypher", "arguments": '{"q": 1}'},
        }
      ],
    }

  def test_tool_only_assistant_turn_has_null_content(self):
    blocks = [{"toolUse": {"toolUseId": "t1", "name": "x", "input": {}}}]
    [message] = to_chat_messages([AIMessage(role="assistant", content=blocks)], None)
    assert message["content"] is None
    assert message["tool_calls"][0]["function"]["arguments"] == "{}"

  def test_tool_results_come_first_errors_say_so_and_text_trails(self):
    blocks = [
      {
        "toolResult": {
          "toolUseId": "t1",
          "content": [{"text": "[1, 2]"}],
          "status": "success",
        }
      },
      {
        "toolResult": {
          "toolUseId": "t2",
          "content": [{"json": {"error": "bad cypher"}}],
          "status": "error",
        }
      },
      {"cachePoint": {"type": "default"}},
      {"text": "Answer now."},
    ]
    out = to_chat_messages([AIMessage(role="user", content=blocks)], None)
    assert out == [
      {"role": "tool", "tool_call_id": "t1", "content": "[1, 2]"},
      {
        "role": "tool",
        "tool_call_id": "t2",
        "content": 'Error: {"error": "bad cypher"}',
      },
      {"role": "user", "content": "Answer now."},
    ]


class TestBuildChatRequest:
  def test_tools_and_sampling(self):
    request = build_chat_request(
      SPEC, [AIMessage(role="user", content="q")], "sys", 4000, 0.3, [CYPHER_TOOL]
    )
    assert request["model"] == "qwen3:32b"
    assert request["max_tokens"] == 4000
    assert request["temperature"] == 0.3
    assert request["tool_choice"] == "auto"
    assert request["tools"] == [
      {
        "type": "function",
        "function": {
          "name": "read-graph-cypher",
          "description": "run cypher",
          "parameters": CYPHER_TOOL["inputSchema"],
        },
      }
    ]

  def test_no_tools_no_tool_fields(self):
    request = build_chat_request(
      SPEC, [AIMessage(role="user", content="q")], None, 100, 0.7, None
    )
    assert "tools" not in request
    assert "tool_choice" not in request

  def test_output_cap_clamps(self):
    capped = self_hosted_model_spec("glm-4.7-flash", 4096)
    request = build_chat_request(
      capped, [AIMessage(role="user", content="q")], None, 8000, 0.7, None
    )
    assert request["max_tokens"] == 4096


class TestParseChatResponse:
  def test_text_answer(self):
    response = parse_chat_response(_completion("There are 3."), SPEC)
    assert response.content == "There are 3."
    assert response.stop_reason == "end_turn"
    assert response.content_blocks == [{"text": "There are 3."}]
    assert response.model == "qwen3:32b"
    assert (response.input_tokens, response.output_tokens) == (100, 20)
    assert response.tool_calls == []

  def test_tool_calls_are_tool_use_even_when_the_server_says_stop(self):
    payload = _completion(
      None, [_call("c1", "read-graph-cypher", '{"query": "MATCH (n) RETURN n"}')]
    )
    response = parse_chat_response(payload, SPEC)
    assert response.stop_reason == "tool_use"
    [call] = response.tool_calls
    assert (call.id, call.name, call.input) == (
      "c1",
      "read-graph-cypher",
      {"query": "MATCH (n) RETURN n"},
    )

  def test_unparseable_arguments_are_kept_for_the_tool_to_reject(self):
    payload = _completion(None, [_call("c1", "x", "{not json")], "tool_calls")
    [call] = parse_chat_response(payload, SPEC).tool_calls
    assert call.input == {UNPARSED_ARGUMENTS_KEY: "{not json"}

  def test_missing_call_id_is_generated(self):
    payload = _completion(None, [{"type": "function", "function": {"name": "x"}}])
    [call] = parse_chat_response(payload, SPEC).tool_calls
    assert call.id.startswith("call_")
    assert call.input == {}

  def test_inline_reasoning_is_stripped(self):
    response = parse_chat_response(
      _completion("<think>let me think\nmore</think>\n\nThe answer is 7."), SPEC
    )
    assert response.content == "The answer is 7."

  def test_cached_prompt_tokens_are_reported_as_cache_reads(self):
    payload = _completion(
      "ok",
      usage={
        "prompt_tokens": 1000,
        "completion_tokens": 10,
        "prompt_tokens_details": {"cached_tokens": 800},
      },
    )
    response = parse_chat_response(payload, SPEC)
    assert response.input_tokens == 200
    assert response.cache_read_input_tokens == 800
    assert response.cache_creation_input_tokens == 0

  def test_length_maps_to_max_tokens(self):
    response = parse_chat_response(_completion("partial", finish_reason="length"), SPEC)
    assert response.stop_reason == "max_tokens"

  def test_no_choices_is_a_provider_error(self):
    with pytest.raises(AIProviderError):
      parse_chat_response({"choices": []}, SPEC)


def _client(handler) -> OpenAICompatClient:
  return OpenAICompatClient(URL, "sk-local", 30, transport=httpx.MockTransport(handler))


@pytest.mark.asyncio
class TestOpenAICompatClient:
  async def test_posts_chat_completions_with_the_key(self):
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
      seen.append(request)
      return httpx.Response(200, json=_completion("hello"))

    response = await _client(handler).create_message(
      SPEC, [AIMessage(role="user", content="hi")], "sys", 100, 0.7, None
    )
    assert response.content == "hello"
    [request] = seen
    assert str(request.url) == f"{URL}/chat/completions"
    assert request.headers["authorization"] == "Bearer sk-local"
    body = json.loads(request.content)
    assert body["messages"][0] == {"role": "system", "content": "sys"}

  async def test_no_key_sends_no_authorization(self):
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
      seen.append(request)
      return httpx.Response(200, json=_completion("ok"))

    client = OpenAICompatClient(URL, "", 30, transport=httpx.MockTransport(handler))
    await client.create_message(
      SPEC, [AIMessage(role="user", content="hi")], None, 100, 0.7, None
    )
    assert "authorization" not in seen[0].headers

  @pytest.mark.parametrize("status", [400, 401, 404, 500, 503])
  async def test_http_error_is_a_provider_error(self, status):
    client = _client(lambda r: httpx.Response(status, text="nope"))
    with pytest.raises(AIProviderError, match=f"HTTP {status}"):
      await client.create_message(
        SPEC, [AIMessage(role="user", content="hi")], None, 100, 0.7, None
      )

  async def test_unreachable_server_is_a_provider_error(self):
    def handler(request: httpx.Request) -> httpx.Response:
      raise httpx.ConnectError("connection refused", request=request)

    with pytest.raises(AIProviderError, match="failed before answering"):
      await _client(handler).create_message(
        SPEC, [AIMessage(role="user", content="hi")], None, 100, 0.7, None
      )

  async def test_non_json_body_is_a_provider_error(self):
    client = _client(lambda r: httpx.Response(200, text="<html>"))
    with pytest.raises(AIProviderError, match="non-JSON"):
      await client.create_message(
        SPEC, [AIMessage(role="user", content="hi")], None, 100, 0.7, None
      )


@pytest.fixture
def self_hosted_balanced(monkeypatch):
  """A deployment that registered its self-hosted model and runs `balanced`
  on it — what OPENAI_COMPAT_ENABLED + OPERATOR_PROFILE_BALANCED produce."""
  monkeypatch.setitem(OperatorConfig.MODEL_REGISTRY, OperatorModel.OPENAI_COMPAT, SPEC)
  monkeypatch.setitem(
    OperatorConfig.PROFILE_MODELS, ModelProfile.BALANCED, OperatorModel.OPENAI_COMPAT
  )


def _ai_client(handler) -> tuple[AIClient, MagicMock]:
  bedrock = MagicMock()
  with patch("boto3.client", return_value=bedrock):
    client = AIClient()
  client._self_hosted = _client(handler)
  return client, bedrock


@pytest.mark.asyncio
class TestAIClientDispatch:
  async def test_self_hosted_row_never_reaches_bedrock(self, self_hosted_balanced):
    client, bedrock = _ai_client(lambda r: httpx.Response(200, json=_completion("hi")))
    response = await client.create_message([AIMessage(role="user", content="q")])
    assert response.content == "hi"
    assert response.model == "qwen3:32b"
    bedrock.converse.assert_not_called()

  async def test_bedrock_rows_still_go_to_bedrock(self, self_hosted_balanced):
    client, bedrock = _ai_client(lambda r: pytest.fail("self-hosted called"))
    bedrock.converse.return_value = {
      "output": {"message": {"content": [{"text": "from claude"}]}},
      "usage": {"inputTokens": 1, "outputTokens": 1},
      "stopReason": "end_turn",
    }
    response = await client.create_message(
      [AIMessage(role="user", content="q")], model=ModelProfile.QUALITY
    )
    assert response.content == "from claude"
    bedrock.converse.assert_called_once()

  async def test_self_hosted_row_without_a_client_is_a_provider_error(
    self, self_hosted_balanced
  ):
    client, _ = _ai_client(lambda r: httpx.Response(200, json=_completion("hi")))
    client._self_hosted = None
    with pytest.raises(AIProviderError, match="OPENAI_COMPAT_ENABLED is off"):
      await client.create_message([AIMessage(role="user", content="q")])

  async def test_meter_prices_the_self_hosted_model(self, self_hosted_balanced):
    assert OperatorConfig.pricing_key_for("qwen3:32b") == "openai_compat"


@pytest.mark.asyncio
async def test_tool_loop_round_trips_through_chat_completions(self_hosted_balanced):
  """The analyst loop, unchanged, on the self-hosted path: a failed query is
  fed back as an error, the model corrects, and the answer lands."""
  replies = iter(
    [
      _completion(None, [_call("c1", "read-graph-cypher", '{"query": "BAD"}')]),
      _completion(
        None,
        [_call("c2", "read-graph-cypher", '{"query": "MATCH (e) RETURN count(e)"}')],
        "tool_calls",
      ),
      _completion("There are 42 companies."),
    ]
  )
  requests: list[dict] = []

  def handler(request: httpx.Request) -> httpx.Response:
    requests.append(json.loads(request.content))
    return httpx.Response(200, json=next(replies))

  client, bedrock = _ai_client(handler)
  tools = MagicMock()
  tools.get_tool_schemas = AsyncMock(return_value=[CYPHER_TOOL])
  tools.call_tool = AsyncMock(side_effect=[{"error": "syntax error"}, [{"count": 42}]])
  ctx = OperatorContext(
    graph_id="kg_test",
    user_id="u",
    query="How many companies?",
    mode=OperatorMode.STANDARD,
    history=[],
    ai=TrackedAIClient(client, "kg_test", "u"),
    tools=tools,
    progress=NoOpProgress(),
  )

  result = await run_tool_loop(
    ctx,
    system="You are an analyst.",
    tool_names=["read-graph-cypher"],
    max_iterations=5,
    max_tokens=1000,
  )

  assert result.text == "There are 42 companies."
  assert result.rows == [{"count": 42}]
  assert result.cypher == "MATCH (e) RETURN count(e)"
  assert len(requests) == 3
  bedrock.converse.assert_not_called()

  first, second, third = requests
  assert first["messages"][0] == {"role": "system", "content": "You are an analyst."}
  assert first["tools"][0]["function"]["name"] == "read-graph-cypher"
  # The failed call comes back as an error the model can read.
  assert second["messages"][-2]["tool_calls"][0]["id"] == "c1"
  assert second["messages"][-1]["role"] == "tool"
  assert second["messages"][-1]["tool_call_id"] == "c1"
  assert second["messages"][-1]["content"].startswith("Error:")
  # The corrected call's rows reach the model on the final turn.
  assert third["messages"][-1]["tool_call_id"] == "c2"
  assert "42" in third["messages"][-1]["content"]
  assert ctx.ai.total_tokens["input"] == 300
