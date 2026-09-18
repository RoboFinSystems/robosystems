"""Unit tests for the AI client module.

Covers Bedrock client initialization, the Converse request shape per model,
response parsing, model resolution, and error handling.
"""

from dataclasses import replace
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from robosystems.config.operators import ModelProfile, OperatorConfig, OperatorModel

# Module paths for patching
AI_CLIENT_MODULE = "robosystems.operations.operators.ai_client"

CACHE_POINT = {"cachePoint": {"type": "default"}}


def _make_ai_client():
  """Helper to create an AIClient with mocked boto3 (handles lazy import).

  Returns (client, mock_bedrock_client).
  """
  mock_bedrock_client = MagicMock()

  with (
    patch(f"{AI_CLIENT_MODULE}.env") as mock_env,
    patch("boto3.client", return_value=mock_bedrock_client),
  ):
    mock_env.ENVIRONMENT = "dev"
    mock_env.AWS_BEDROCK_REGION = "us-east-1"
    mock_env.AWS_BEDROCK_ACCESS_KEY_ID = "test"
    mock_env.AWS_BEDROCK_SECRET_ACCESS_KEY = "test"

    from robosystems.operations.operators.ai_client import AIClient

    client = AIClient()

  return client, mock_bedrock_client


def _converse_response(
  blocks: list[dict],
  stop_reason: str | None = "end_turn",
  usage: dict | None = None,
) -> dict:
  """A Converse response body as boto3 returns it."""
  response = {
    "output": {"message": {"role": "assistant", "content": blocks}},
    "usage": usage or {"inputTokens": 10, "outputTokens": 5},
  }
  if stop_reason is not None:
    response["stopReason"] = stop_reason
  return response


def _text_response(text: str, **kwargs) -> dict:
  return _converse_response([{"text": text}], **kwargs)


class TestAIMessage:
  """Test the AIMessage dataclass."""

  @pytest.mark.unit
  def test_message_creation(self):
    from robosystems.operations.operators.ai_client import AIMessage

    msg = AIMessage(role="user", content="Hello, world!")
    assert msg.role == "user"
    assert msg.content == "Hello, world!"

  @pytest.mark.unit
  def test_message_with_block_content(self):
    from robosystems.operations.operators.ai_client import AIMessage

    blocks = [{"text": "a"}, {"toolUse": {"toolUseId": "t", "name": "n", "input": {}}}]
    msg = AIMessage(role="assistant", content=blocks)
    assert msg.content == blocks


class TestAIResponse:
  """Test the AIResponse dataclass."""

  @pytest.mark.unit
  def test_response_creation(self):
    from robosystems.operations.operators.ai_client import AIResponse

    resp = AIResponse(content="hi", model="m", input_tokens=1, output_tokens=2)
    assert resp.stop_reason is None
    assert resp.cache_read_input_tokens == 0
    assert resp.cache_creation_input_tokens == 0
    assert resp.content_blocks == []
    assert resp.tool_calls == []

  @pytest.mark.unit
  def test_tool_calls_are_read_from_tool_use_blocks(self):
    """Text and reasoning blocks are skipped; only toolUse blocks become
    calls, with the provider shape stripped."""
    from robosystems.operations.operators.ai_client import AIResponse, ToolCall

    resp = AIResponse(
      content="Let me check.",
      model="m",
      input_tokens=1,
      output_tokens=2,
      stop_reason="tool_use",
      content_blocks=[
        {"reasoningContent": {"reasoningText": {"text": "…"}}},
        {"text": "Let me check."},
        {
          "toolUse": {
            "toolUseId": "t1",
            "name": "read-graph-cypher",
            "input": {"query": "MATCH (n) RETURN n"},
          }
        },
        {"toolUse": {"toolUseId": "t2", "name": "get-graph-schema"}},
      ],
    )
    assert resp.tool_calls == [
      ToolCall(
        id="t1", name="read-graph-cypher", input={"query": "MATCH (n) RETURN n"}
      ),
      ToolCall(id="t2", name="get-graph-schema", input={}),
    ]


class TestBlockHelpers:
  @pytest.mark.unit
  def test_tool_result_block_carries_status(self):
    from robosystems.operations.operators.ai_client import tool_result_block

    assert tool_result_block("t1", "[]") == {
      "toolResult": {
        "toolUseId": "t1",
        "content": [{"text": "[]"}],
        "status": "success",
      }
    }
    assert tool_result_block("t1", "boom", is_error=True)["toolResult"]["status"] == (
      "error"
    )


class TestAIClientInitialization:
  """Test AIClient initialization and Bedrock client setup."""

  @pytest.mark.unit
  def test_initialization_dev_with_credentials(self):
    """Test AIClient initializes with dev credentials when available."""
    mock_client = MagicMock()

    with (
      patch(f"{AI_CLIENT_MODULE}.env") as mock_env,
      patch("boto3.client", return_value=mock_client) as mock_boto3_client,
    ):
      mock_env.ENVIRONMENT = "dev"
      mock_env.AWS_BEDROCK_REGION = "us-east-1"
      mock_env.AWS_BEDROCK_ACCESS_KEY_ID = "test-access-key"
      mock_env.AWS_BEDROCK_SECRET_ACCESS_KEY = "test-secret-key"

      from robosystems.operations.operators.ai_client import AIClient

      ai_client = AIClient()

      assert ai_client.backend == "bedrock"
      assert ai_client.client is mock_client
      mock_boto3_client.assert_called_once_with(
        service_name="bedrock-runtime",
        region_name="us-east-1",
        endpoint_url="https://bedrock-runtime.us-east-1.amazonaws.com",
        aws_access_key_id="test-access-key",
        aws_secret_access_key="test-secret-key",
      )

  @pytest.mark.unit
  def test_initialization_dev_without_credentials(self):
    """Test AIClient initializes without explicit credentials in dev mode."""
    mock_client = MagicMock()

    with (
      patch(f"{AI_CLIENT_MODULE}.env") as mock_env,
      patch("boto3.client", return_value=mock_client) as mock_boto3_client,
    ):
      mock_env.ENVIRONMENT = "dev"
      mock_env.AWS_BEDROCK_REGION = "us-west-2"
      mock_env.AWS_BEDROCK_ACCESS_KEY_ID = ""
      mock_env.AWS_BEDROCK_SECRET_ACCESS_KEY = ""

      from robosystems.operations.operators.ai_client import AIClient

      ai_client = AIClient()

      assert ai_client.backend == "bedrock"
      # Should NOT include access key params when empty
      mock_boto3_client.assert_called_once_with(
        service_name="bedrock-runtime",
        region_name="us-west-2",
        endpoint_url="https://bedrock-runtime.us-west-2.amazonaws.com",
      )

  @pytest.mark.unit
  def test_initialization_prod_with_iam_role(self):
    """Test AIClient initializes with IAM role credentials in prod."""
    mock_bedrock_client = MagicMock()
    mock_sts_client = MagicMock()

    def side_effect(**kwargs):
      if kwargs.get("service_name") == "bedrock-runtime":
        return mock_bedrock_client
      elif kwargs.get("service_name") == "sts":
        return mock_sts_client
      return MagicMock()

    with (
      patch(f"{AI_CLIENT_MODULE}.env") as mock_env,
      patch("boto3.client", side_effect=side_effect),
    ):
      mock_env.ENVIRONMENT = "prod"
      mock_env.AWS_BEDROCK_REGION = "us-east-1"
      mock_env.AWS_BEDROCK_ACCESS_KEY_ID = ""
      mock_env.AWS_BEDROCK_SECRET_ACCESS_KEY = ""

      from robosystems.operations.operators.ai_client import AIClient

      ai_client = AIClient()

      assert ai_client.backend == "bedrock"
      assert ai_client.client is mock_bedrock_client
      # Verify STS call was made for identity verification
      mock_sts_client.get_caller_identity.assert_called_once()

  @pytest.mark.unit
  def test_initialization_failure_raises_value_error(self):
    """Test AIClient raises ValueError when Bedrock initialization fails."""
    with (
      patch(f"{AI_CLIENT_MODULE}.env") as mock_env,
      patch("boto3.client", side_effect=Exception("Invalid credentials")),
    ):
      mock_env.ENVIRONMENT = "dev"
      mock_env.AWS_BEDROCK_REGION = "us-east-1"
      mock_env.AWS_BEDROCK_ACCESS_KEY_ID = "bad-key"
      mock_env.AWS_BEDROCK_SECRET_ACCESS_KEY = "bad-secret"

      from robosystems.operations.operators.ai_client import AIClient

      with pytest.raises(ValueError, match="Failed to initialize AWS Bedrock client"):
        AIClient()

  @pytest.mark.unit
  def test_initialization_prod_sts_failure_raises_value_error(self):
    """Test AIClient raises ValueError when STS verification fails in prod."""
    mock_bedrock_client = MagicMock()
    mock_sts_client = MagicMock()
    mock_sts_client.get_caller_identity.side_effect = Exception("STS error")

    def side_effect(**kwargs):
      if kwargs.get("service_name") == "bedrock-runtime":
        return mock_bedrock_client
      elif kwargs.get("service_name") == "sts":
        return mock_sts_client
      return MagicMock()

    with (
      patch(f"{AI_CLIENT_MODULE}.env") as mock_env,
      patch("boto3.client", side_effect=side_effect),
    ):
      mock_env.ENVIRONMENT = "prod"
      mock_env.AWS_BEDROCK_REGION = "us-east-1"
      mock_env.AWS_BEDROCK_ACCESS_KEY_ID = ""
      mock_env.AWS_BEDROCK_SECRET_ACCESS_KEY = ""

      from robosystems.operations.operators.ai_client import AIClient

      with pytest.raises(ValueError, match="Failed to initialize AWS Bedrock client"):
        AIClient()


class TestAIClientCreateMessage:
  """The Converse request shape and response parsing."""

  @pytest.mark.unit
  async def test_create_message_basic(self):
    """Default model (Sonnet 5): text in, text out, tokens and stop reason
    parsed from the Converse envelope."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    mock_bedrock.converse.return_value = _text_response(
      "Financial analysis complete.",
      usage={"inputTokens": 150, "outputTokens": 75},
    )

    result = await client.create_message(
      messages=[AIMessage(role="user", content="Analyze revenue trends")],
      max_tokens=2000,
      temperature=0.5,
    )

    assert result.content == "Financial analysis complete."
    assert result.model == "us.anthropic.claude-sonnet-5"
    assert result.input_tokens == 150
    assert result.output_tokens == 75
    assert result.stop_reason == "end_turn"

    request = mock_bedrock.converse.call_args.kwargs
    assert request["modelId"] == "us.anthropic.claude-sonnet-5"
    assert request["messages"] == [
      {"role": "user", "content": [{"text": "Analyze revenue trends"}]}
    ]
    assert request["inferenceConfig"] == {"maxTokens": 2000}

  @pytest.mark.unit
  async def test_system_prompt_carries_the_prefix_cache_point(self):
    """One cache point at the end of `system` caches tools + system together
    (Converse evaluates tools -> system -> messages cumulatively)."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    mock_bedrock.converse.return_value = _text_response("ok")

    await client.create_message(
      messages=[AIMessage(role="user", content="q")],
      system="You are a financial analyst.",
    )
    request = mock_bedrock.converse.call_args.kwargs
    assert request["system"] == [{"text": "You are a financial analyst."}, CACHE_POINT]

  @pytest.mark.unit
  async def test_no_system_prompt_omits_the_field(self):
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    mock_bedrock.converse.return_value = _text_response("ok")
    await client.create_message(messages=[AIMessage(role="user", content="q")])
    assert "system" not in mock_bedrock.converse.call_args.kwargs

  @pytest.mark.unit
  async def test_tools_are_wrapped_as_tool_specs_and_tool_use_is_parsed(self):
    """MCP-shaped definitions become Converse toolSpecs; a tool-use turn
    exposes the calls and the full block list for the loop to replay."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    mock_bedrock.converse.return_value = _converse_response(
      [
        {"text": "Let me check."},
        {
          "toolUse": {
            "toolUseId": "toolu_1",
            "name": "read-graph-cypher",
            "input": {"query": "MATCH (n) RETURN n"},
          }
        },
      ],
      stop_reason="tool_use",
      usage={"inputTokens": 120, "outputTokens": 30},
    )

    tools = [
      {
        "name": "read-graph-cypher",
        "description": "run cypher",
        "inputSchema": {"type": "object"},
      }
    ]
    result = await client.create_message(
      messages=[AIMessage(role="user", content="how many nodes?")],
      tools=tools,
    )

    request = mock_bedrock.converse.call_args.kwargs
    assert request["toolConfig"] == {
      "tools": [
        {
          "toolSpec": {
            "name": "read-graph-cypher",
            "description": "run cypher",
            "inputSchema": {"json": {"type": "object"}},
          }
        }
      ]
    }
    assert "toolChoice" not in request["toolConfig"]

    assert result.stop_reason == "tool_use"
    # `content` is the joined text blocks; toolUse carries no text.
    assert result.content == "Let me check."
    assert len(result.content_blocks) == 2
    assert result.tool_calls[0].name == "read-graph-cypher"
    assert result.tool_calls[0].id == "toolu_1"
    assert result.tool_calls[0].input == {"query": "MATCH (n) RETURN n"}

  @pytest.mark.unit
  async def test_no_tools_omits_tool_config(self):
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    mock_bedrock.converse.return_value = _text_response("hi")
    await client.create_message(messages=[AIMessage(role="user", content="hi")])
    assert "toolConfig" not in mock_bedrock.converse.call_args.kwargs

  @pytest.mark.unit
  async def test_cache_conversation_marks_the_trailing_turn(self):
    """cache_conversation appends a cache point to the trailing message —
    string content is wrapped into a text block — and the caller's message
    objects are never mutated (a persisted marker on every past turn would
    exceed the 4-breakpoint limit)."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import (
      AIMessage,
      tool_result_block,
    )

    mock_bedrock.converse.return_value = _text_response("hi")

    tool_result = tool_result_block("t1", "[]")
    trailing = [tool_result]
    messages = [
      AIMessage(role="user", content="question"),
      AIMessage(
        role="assistant",
        content=[{"toolUse": {"toolUseId": "t1", "name": "n", "input": {}}}],
      ),
      AIMessage(role="user", content=trailing),
    ]

    await client.create_message(messages=messages, cache_conversation=True)

    sent = mock_bedrock.converse.call_args.kwargs["messages"]
    assert sent[-1]["content"] == [tool_result, CACHE_POINT]
    assert all("cachePoint" not in str(m) for m in sent[:-1])
    # The caller's list was copied, not mutated.
    assert trailing == [tool_result]

  @pytest.mark.unit
  async def test_cache_conversation_wraps_string_content(self):
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    mock_bedrock.converse.return_value = _text_response("hi")
    await client.create_message(
      messages=[AIMessage(role="user", content="just a question")],
      cache_conversation=True,
    )
    sent = mock_bedrock.converse.call_args.kwargs["messages"]
    assert sent[-1]["content"] == [{"text": "just a question"}, CACHE_POINT]

  @pytest.mark.unit
  async def test_no_conversation_marker_by_default(self):
    """Single-shot callers pay the cache-write premium with nothing ever
    reading the entry — the trailing-turn marker is opt-in."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    mock_bedrock.converse.return_value = _text_response("hi")
    await client.create_message(messages=[AIMessage(role="user", content="hi")])
    sent = mock_bedrock.converse.call_args.kwargs["messages"]
    assert sent == [{"role": "user", "content": [{"text": "hi"}]}]

  @pytest.mark.unit
  async def test_cache_token_counts_are_parsed_from_usage(self):
    """With caching in play `inputTokens` is the uncached remainder, so the
    cache read and write counts must be carried to the meter."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    mock_bedrock.converse.return_value = _text_response(
      "cached",
      usage={
        "inputTokens": 337,
        "outputTokens": 50,
        "cacheReadInputTokens": 3905,
        "cacheWriteInputTokens": 12,
      },
    )
    result = await client.create_message(
      messages=[AIMessage(role="user", content="hi")]
    )
    assert result.input_tokens == 337
    assert result.cache_read_input_tokens == 3905
    assert result.cache_creation_input_tokens == 12

  @pytest.mark.unit
  async def test_cache_token_counts_default_to_zero_when_absent(self):
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    mock_bedrock.converse.return_value = _text_response("hi")
    result = await client.create_message(
      messages=[AIMessage(role="user", content="hi")]
    )
    assert result.cache_read_input_tokens == 0
    assert result.cache_creation_input_tokens == 0

  @pytest.mark.unit
  async def test_claude_4_family_sends_temperature_and_no_extra_fields(self):
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    mock_bedrock.converse.return_value = _text_response("hi")
    await client.create_message(
      messages=[AIMessage(role="user", content="hi")],
      model="claude-sonnet-4-6",
      temperature=0.3,
      max_tokens=1000,
    )
    request = mock_bedrock.converse.call_args.kwargs
    assert request["modelId"] == "us.anthropic.claude-sonnet-4-6"
    assert request["inferenceConfig"] == {"maxTokens": 1000, "temperature": 0.3}
    assert "additionalModelRequestFields" not in request

  @pytest.mark.unit
  async def test_claude_5_family_omits_temperature_and_disables_thinking(self):
    """Claude 5-family models 400 on `temperature` and run adaptive thinking
    unless explicitly disabled — thinking tokens would bill as output and eat
    max_tokens."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    mock_bedrock.converse.return_value = _text_response("hi")
    await client.create_message(
      messages=[AIMessage(role="user", content="hi")],
      model="claude-sonnet-5",
      temperature=0.3,
    )
    request = mock_bedrock.converse.call_args.kwargs
    assert request["inferenceConfig"] == {"maxTokens": 4000}
    assert request["additionalModelRequestFields"] == {"thinking": {"type": "disabled"}}

  @pytest.mark.unit
  async def test_luna_sends_no_cache_points_no_temperature_no_extra_fields(self):
    """GPT-5.6 over Converse rejects explicit cache points and `temperature`
    (verified live 2026-09-15); it caches implicitly and reports the reads."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    mock_bedrock.converse.return_value = _text_response(
      "42",
      usage={
        "inputTokens": 2,
        "outputTokens": 5,
        "cacheReadInputTokens": 1815,
        "cacheWriteInputTokens": 64,
      },
    )
    result = await client.create_message(
      messages=[AIMessage(role="user", content="hi")],
      system="s" * 10,
      model=ModelProfile.ECONOMY,
      temperature=0.3,
      cache_conversation=True,
    )
    request = mock_bedrock.converse.call_args.kwargs
    assert request["modelId"] == "us.openai.gpt-5.6-luna"
    assert request["system"] == [{"text": "s" * 10}]
    assert request["messages"][-1]["content"] == [{"text": "hi"}]
    assert request["inferenceConfig"] == {"maxTokens": 4000}
    assert "additionalModelRequestFields" not in request
    assert result.cache_read_input_tokens == 1815
    assert result.cache_creation_input_tokens == 64

  @pytest.mark.unit
  async def test_reasoning_blocks_are_kept_for_replay_but_not_in_content(self):
    """Reasoning models return a reasoningContent block; the loop replays it
    verbatim (the signature is checked), but it is not the answer text."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    reasoning = {"reasoningContent": {"reasoningText": {"text": "…", "signature": "x"}}}
    mock_bedrock.converse.return_value = _converse_response([reasoning, {"text": "42"}])
    result = await client.create_message(
      messages=[AIMessage(role="user", content="hi")]
    )
    assert result.content == "42"
    assert result.content_blocks == [reasoning, {"text": "42"}]

  @pytest.mark.unit
  async def test_profile_and_wire_id_resolve(self):
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    mock_bedrock.converse.return_value = _text_response("hi")
    await client.create_message(
      messages=[AIMessage(role="user", content="hi")], model="quality"
    )
    assert mock_bedrock.converse.call_args.kwargs["modelId"] == (
      "us.anthropic.claude-opus-5"
    )
    await client.create_message(
      messages=[AIMessage(role="user", content="hi")],
      model="us.anthropic.claude-sonnet-4-20250514-v1:0",
    )
    assert mock_bedrock.converse.call_args.kwargs["modelId"] == (
      "us.anthropic.claude-sonnet-4-20250514-v1:0"
    )

  @pytest.mark.unit
  async def test_unknown_model_raises_before_any_call(self):
    """An unregistered name is a configuration error; it never silently runs
    the default (the old client warned and fell back)."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    with pytest.raises(ValueError, match="Unknown model or profile"):
      await client.create_message(
        messages=[AIMessage(role="user", content="hi")], model="not-a-real-model"
      )
    mock_bedrock.converse.assert_not_called()

  @pytest.mark.unit
  async def test_operator_type_override_is_honored(self, monkeypatch):
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    monkeypatch.setitem(
      OperatorConfig.OPERATOR_MODEL_OVERRIDES, "financial", ModelProfile.ECONOMY
    )
    mock_bedrock.converse.return_value = _text_response("hi")
    result = await client.create_message(
      messages=[AIMessage(role="user", content="hi")], operator_type="financial"
    )
    assert result.model == "us.openai.gpt-5.6-luna"

  @pytest.mark.unit
  async def test_max_output_tokens_clamps_the_request(self, monkeypatch):
    """A model that caps output below what the execution profile asks for
    is clamped at request build rather than truncated silently."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    spec = OperatorConfig.MODEL_REGISTRY[OperatorModel.SONNET_4_6]
    monkeypatch.setitem(
      OperatorConfig.MODEL_REGISTRY,
      OperatorModel.SONNET_4_6,
      replace(spec, max_output_tokens=1000),
    )
    mock_bedrock.converse.return_value = _text_response("hi")
    await client.create_message(
      messages=[AIMessage(role="user", content="hi")],
      model=OperatorModel.SONNET_4_6,
      max_tokens=8000,
    )
    assert mock_bedrock.converse.call_args.kwargs["inferenceConfig"]["maxTokens"] == (
      1000
    )

  @pytest.mark.unit
  async def test_stop_reason_none_when_absent(self):
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    mock_bedrock.converse.return_value = _text_response("partial", stop_reason=None)
    result = await client.create_message(
      messages=[AIMessage(role="user", content="hi")]
    )
    assert result.stop_reason is None


class TestAIClientErrors:
  """Provider refusals surface as a typed error; everything else propagates."""

  @pytest.mark.unit
  async def test_access_denied_becomes_a_provider_error(self):
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage, AIProviderError

    mock_bedrock.converse.side_effect = ClientError(
      {
        "Error": {
          "Code": "AccessDeniedException",
          "Message": "You don't have access to the model",
        }
      },
      "Converse",
    )
    with pytest.raises(AIProviderError) as exc:
      await client.create_message(messages=[AIMessage(role="user", content="hi")])
    assert "us.anthropic.claude-sonnet-5" in str(exc.value)
    assert "AccessDeniedException" in str(exc.value)
    assert "You don't have access" in str(exc.value)

  @pytest.mark.unit
  async def test_validation_error_becomes_a_provider_error(self):
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage, AIProviderError

    mock_bedrock.converse.side_effect = ClientError(
      {"Error": {"Code": "ValidationException", "Message": "bad request"}},
      "Converse",
    )
    with pytest.raises(AIProviderError):
      await client.create_message(messages=[AIMessage(role="user", content="hi")])

  @pytest.mark.unit
  async def test_throttling_propagates_as_the_client_error(self):
    """Retryable service errors keep their botocore type so callers can
    distinguish them from a refused call."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    mock_bedrock.converse.side_effect = ClientError(
      {"Error": {"Code": "ThrottlingException", "Message": "slow down"}},
      "Converse",
    )
    with pytest.raises(ClientError, match="ThrottlingException"):
      await client.create_message(messages=[AIMessage(role="user", content="hi")])

  @pytest.mark.unit
  async def test_unexpected_exceptions_propagate(self):
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    mock_bedrock.converse.side_effect = Exception("Bedrock throttling error")
    with pytest.raises(Exception, match="Bedrock throttling error"):
      await client.create_message(messages=[AIMessage(role="user", content="hi")])


class TestAIClientBedrockEndpoint:
  """Test Bedrock endpoint URL construction."""

  @pytest.mark.unit
  def test_endpoint_url_construction(self):
    """Test that Bedrock endpoint URL is correctly constructed from region."""
    mock_client = MagicMock()

    with (
      patch(f"{AI_CLIENT_MODULE}.env") as mock_env,
      patch("boto3.client", return_value=mock_client) as mock_boto3_client,
    ):
      mock_env.ENVIRONMENT = "dev"
      mock_env.AWS_BEDROCK_REGION = "eu-west-1"
      mock_env.AWS_BEDROCK_ACCESS_KEY_ID = "test"
      mock_env.AWS_BEDROCK_SECRET_ACCESS_KEY = "test"

      from robosystems.operations.operators.ai_client import AIClient

      AIClient()

      call_args = mock_boto3_client.call_args
      assert (
        call_args[1]["endpoint_url"]
        == "https://bedrock-runtime.eu-west-1.amazonaws.com"
      )

  @pytest.mark.unit
  def test_endpoint_uses_bedrock_region(self):
    """Test that endpoint uses AWS_BEDROCK_REGION, not generic AWS_REGION."""
    mock_client = MagicMock()

    with (
      patch(f"{AI_CLIENT_MODULE}.env") as mock_env,
      patch("boto3.client", return_value=mock_client) as mock_boto3_client,
    ):
      mock_env.ENVIRONMENT = "dev"
      mock_env.AWS_BEDROCK_REGION = "ap-southeast-1"
      mock_env.AWS_BEDROCK_ACCESS_KEY_ID = "test"
      mock_env.AWS_BEDROCK_SECRET_ACCESS_KEY = "test"

      from robosystems.operations.operators.ai_client import AIClient

      AIClient()

      call_args = mock_boto3_client.call_args
      assert call_args[1]["region_name"] == "ap-southeast-1"
      assert "ap-southeast-1" in call_args[1]["endpoint_url"]


class TestAIClientOffloadsBedrock:
  """The synchronous botocore call must run off the event loop.

  A model call can take minutes; running it inline held the single-worker
  loop — and every tenant on the task — for the whole call. It goes through
  `asyncio.to_thread`.
  """

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_converse_runs_in_a_thread(self):
    from robosystems.operations.operators.ai_client import AIMessage

    client, mock_bedrock = _make_ai_client()
    mock_bedrock.converse.return_value = _text_response(
      "hi", usage={"inputTokens": 3, "outputTokens": 1}
    )

    with patch(f"{AI_CLIENT_MODULE}.asyncio.to_thread") as mock_to_thread:

      async def _fake_to_thread(fn, *args, **kwargs):
        # Prove the blocking call is the one being offloaded, and run it.
        assert fn == client._converse_sync
        return fn(*args, **kwargs)

      mock_to_thread.side_effect = _fake_to_thread

      resp = await client.create_message(
        messages=[AIMessage(role="user", content="hi")]
      )

    mock_to_thread.assert_called_once()
    assert resp.content == "hi"
    assert resp.input_tokens == 3 and resp.output_tokens == 1


class TestSharedAIClient:
  """`get_ai_client` returns one process-wide instance, so operator requests do
  not each rebuild a boto3 client + STS call on the loop."""

  @pytest.mark.unit
  def test_returns_a_singleton(self):
    import robosystems.operations.operators.ai_client as mod

    mod._shared_client = None
    with patch.object(mod, "AIClient") as cls:
      cls.side_effect = lambda: MagicMock()
      a = mod.get_ai_client()
      b = mod.get_ai_client()
    assert a is b
    cls.assert_called_once()
    mod._shared_client = None

  @pytest.mark.unit
  def test_a_failed_build_is_not_cached(self):
    import robosystems.operations.operators.ai_client as mod

    mod._shared_client = None
    with patch.object(mod, "AIClient", side_effect=ValueError("no creds")):
      with pytest.raises(ValueError):
        mod.get_ai_client()
    # Next call retries rather than returning a broken/cached client.
    with patch.object(mod, "AIClient") as cls:
      cls.side_effect = lambda: MagicMock()
      client = mod.get_ai_client()
    assert client is not None
    mod._shared_client = None
