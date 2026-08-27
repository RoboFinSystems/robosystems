"""Comprehensive unit tests for the AI client module.

Tests AWS Bedrock AI client initialization, configuration,
message creation, model resolution, and error handling.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from robosystems.config.operators import BedrockModel, OperatorConfig

# Module paths for patching
AI_CLIENT_MODULE = "robosystems.operations.operators.ai_client"


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


class TestAIMessage:
  """Test the AIMessage dataclass."""

  @pytest.mark.unit
  def test_message_creation(self):
    """Test basic AIMessage creation with role and content."""
    from robosystems.operations.operators.ai_client import AIMessage

    msg = AIMessage(role="user", content="Hello, world!")
    assert msg.role == "user"
    assert msg.content == "Hello, world!"

  @pytest.mark.unit
  def test_message_with_assistant_role(self):
    """Test AIMessage with assistant role."""
    from robosystems.operations.operators.ai_client import AIMessage

    msg = AIMessage(role="assistant", content="I can help with that.")
    assert msg.role == "assistant"
    assert msg.content == "I can help with that."

  @pytest.mark.unit
  def test_message_with_empty_content(self):
    """Test AIMessage with empty content string."""
    from robosystems.operations.operators.ai_client import AIMessage

    msg = AIMessage(role="user", content="")
    assert msg.content == ""


class TestAIResponse:
  """Test the AIResponse dataclass."""

  @pytest.mark.unit
  def test_response_creation(self):
    """Test basic AIResponse creation."""
    from robosystems.operations.operators.ai_client import AIResponse

    resp = AIResponse(
      content="Analysis complete.",
      model="us.anthropic.claude-sonnet-4-6",
      input_tokens=100,
      output_tokens=50,
    )
    assert resp.content == "Analysis complete."
    assert resp.model == "us.anthropic.claude-sonnet-4-6"
    assert resp.input_tokens == 100
    assert resp.output_tokens == 50
    assert resp.stop_reason is None

  @pytest.mark.unit
  def test_response_with_stop_reason(self):
    """Test AIResponse with explicit stop_reason."""
    from robosystems.operations.operators.ai_client import AIResponse

    resp = AIResponse(
      content="Done.",
      model="us.anthropic.claude-sonnet-4-6",
      input_tokens=50,
      output_tokens=25,
      stop_reason="end_turn",
    )
    assert resp.stop_reason == "end_turn"

  @pytest.mark.unit
  def test_response_with_zero_tokens(self):
    """Test AIResponse with zero token counts."""
    from robosystems.operations.operators.ai_client import AIResponse

    resp = AIResponse(
      content="",
      model="test-model",
      input_tokens=0,
      output_tokens=0,
    )
    assert resp.input_tokens == 0
    assert resp.output_tokens == 0


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


class TestAIClientGetModelId:
  """Test model ID resolution in AIClient."""

  @pytest.mark.unit
  def test_default_model_id(self):
    """Test that default model ID is returned when no override specified."""
    client, _ = _make_ai_client()
    model_id = client._get_model_id()

    expected = OperatorConfig.get_bedrock_model_id()
    assert model_id == expected

  @pytest.mark.unit
  def test_explicit_valid_model(self):
    """Test model ID when a valid model name is provided."""
    client, _ = _make_ai_client()
    model_id = client._get_model_id(model=BedrockModel.SONNET_4.value)

    expected = OperatorConfig.get_bedrock_model_id(model=BedrockModel.SONNET_4)
    assert model_id == expected

  @pytest.mark.unit
  def test_invalid_model_falls_back_to_default(self):
    """Test that an invalid model name falls back to default."""
    client, _ = _make_ai_client()
    model_id = client._get_model_id(model="not-a-real-model")

    # Should use default when model is invalid
    expected = OperatorConfig.get_bedrock_model_id()
    assert model_id == expected

  @pytest.mark.unit
  def test_agent_type_override(self):
    """Test model ID with operator_type parameter."""
    client, _ = _make_ai_client()
    model_id = client._get_model_id(operator_type="financial")

    expected = OperatorConfig.get_bedrock_model_id(operator_type="financial")
    assert model_id == expected

  @pytest.mark.unit
  def test_model_and_agent_type_model_takes_precedence(self):
    """Test that explicit model overrides operator_type."""
    client, _ = _make_ai_client()
    model_id = client._get_model_id(
      model=BedrockModel.SONNET_4.value, operator_type="financial"
    )

    expected = OperatorConfig.get_bedrock_model_id(model=BedrockModel.SONNET_4)
    assert model_id == expected

  @pytest.mark.unit
  def test_none_model_and_none_agent_type(self):
    """Test default model returned when both params are None."""
    client, _ = _make_ai_client()
    model_id = client._get_model_id(model=None, operator_type=None)

    expected = OperatorConfig.get_bedrock_model_id()
    assert model_id == expected


class TestAIClientCreateMessage:
  """Test the create_message method."""

  @pytest.mark.unit
  async def test_create_message_basic(self):
    """Test basic message creation through Bedrock."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    response_body = {
      "content": [{"text": "Financial analysis complete."}],
      "usage": {"input_tokens": 150, "output_tokens": 75},
      "stop_reason": "end_turn",
    }
    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps(response_body).encode()
    mock_bedrock.invoke_model.return_value = {"body": mock_body}

    messages = [AIMessage(role="user", content="Analyze revenue trends")]

    result = await client.create_message(
      messages=messages,
      max_tokens=2000,
      temperature=0.5,
    )

    assert result.content == "Financial analysis complete."
    assert result.input_tokens == 150
    assert result.output_tokens == 75
    assert result.stop_reason == "end_turn"

  @pytest.mark.unit
  async def test_create_message_with_system_prompt(self):
    """Test message creation with a system prompt."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    response_body = {
      "content": [{"text": "Response with system context."}],
      "usage": {"input_tokens": 200, "output_tokens": 100},
    }
    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps(response_body).encode()
    mock_bedrock.invoke_model.return_value = {"body": mock_body}

    messages = [AIMessage(role="user", content="Test query")]

    result = await client.create_message(
      messages=messages,
      system="You are a financial analyst.",
      max_tokens=4000,
      temperature=0.7,
    )

    call_args = mock_bedrock.invoke_model.call_args
    request_body = json.loads(call_args[1]["body"])
    assert request_body["system"] == "You are a financial analyst."
    assert result.content == "Response with system context."

  @pytest.mark.unit
  async def test_create_message_without_system_prompt(self):
    """Test message creation without a system prompt excludes it from request."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    response_body = {
      "content": [{"text": "Response without system."}],
      "usage": {"input_tokens": 100, "output_tokens": 50},
    }
    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps(response_body).encode()
    mock_bedrock.invoke_model.return_value = {"body": mock_body}

    messages = [AIMessage(role="user", content="Test query")]

    await client.create_message(messages=messages)

    call_args = mock_bedrock.invoke_model.call_args
    request_body = json.loads(call_args[1]["body"])
    assert "system" not in request_body

  @pytest.mark.unit
  async def test_create_message_with_tools_and_tool_use_response(self):
    """Tools are sent in the request body; a tool_use response exposes
    content_blocks and stop_reason so the tool loop can drive it."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    response_body = {
      "content": [
        {"type": "text", "text": "Let me check."},
        {
          "type": "tool_use",
          "id": "toolu_1",
          "name": "read-graph-cypher",
          "input": {"query": "MATCH (n) RETURN n"},
        },
      ],
      "usage": {"input_tokens": 120, "output_tokens": 30},
      "stop_reason": "tool_use",
    }
    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps(response_body).encode()
    mock_bedrock.invoke_model.return_value = {"body": mock_body}

    tools = [
      {
        "name": "read-graph-cypher",
        "description": "run cypher",
        "input_schema": {"type": "object"},
      }
    ]
    result = await client.create_message(
      messages=[AIMessage(role="user", content="how many nodes?")],
      tools=tools,
    )

    request_body = json.loads(mock_bedrock.invoke_model.call_args[1]["body"])
    assert request_body["tools"] == tools

    assert result.stop_reason == "tool_use"
    # `content` is the joined text blocks; tool_use carries no text.
    assert result.content == "Let me check."
    assert len(result.content_blocks) == 2
    assert result.content_blocks[1]["name"] == "read-graph-cypher"

  @pytest.mark.unit
  async def test_create_message_without_tools_omits_tools_key(self):
    """No tools param → no `tools` key in the Bedrock request body."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    response_body = {
      "content": [{"type": "text", "text": "hi"}],
      "usage": {"input_tokens": 10, "output_tokens": 5},
      "stop_reason": "end_turn",
    }
    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps(response_body).encode()
    mock_bedrock.invoke_model.return_value = {"body": mock_body}

    await client.create_message(messages=[AIMessage(role="user", content="hi")])
    request_body = json.loads(mock_bedrock.invoke_model.call_args[1]["body"])
    assert "tools" not in request_body

  @pytest.mark.unit
  async def test_create_message_sends_tool_choice_alongside_tools(self):
    """tool_choice rides in the request body with the tools it constrains —
    the tool loop's final nudge uses {"type": "none"} to keep the transcript
    valid while forbidding another tool_use turn."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    response_body = {
      "content": [{"type": "text", "text": "final"}],
      "usage": {"input_tokens": 10, "output_tokens": 5},
      "stop_reason": "end_turn",
    }
    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps(response_body).encode()
    mock_bedrock.invoke_model.return_value = {"body": mock_body}

    tools = [{"name": "t", "description": "d", "input_schema": {"type": "object"}}]
    await client.create_message(
      messages=[AIMessage(role="user", content="answer now")],
      tools=tools,
      tool_choice={"type": "none"},
    )
    request_body = json.loads(mock_bedrock.invoke_model.call_args[1]["body"])
    assert request_body["tools"] == tools
    assert request_body["tool_choice"] == {"type": "none"}

  @pytest.mark.unit
  async def test_tool_choice_without_tools_is_dropped(self):
    """tool_choice is meaningless (and rejected by the API) without tools."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    response_body = {
      "content": [{"type": "text", "text": "hi"}],
      "usage": {"input_tokens": 10, "output_tokens": 5},
      "stop_reason": "end_turn",
    }
    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps(response_body).encode()
    mock_bedrock.invoke_model.return_value = {"body": mock_body}

    await client.create_message(
      messages=[AIMessage(role="user", content="hi")],
      tool_choice={"type": "none"},
    )
    request_body = json.loads(mock_bedrock.invoke_model.call_args[1]["body"])
    assert "tool_choice" not in request_body

  @pytest.mark.unit
  async def test_cache_token_counts_are_parsed_from_usage(self):
    """Bedrock reports cache reads/writes in usage; with caching in play
    `input_tokens` is the uncached remainder, so all three must be carried."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    response_body = {
      "content": [{"type": "text", "text": "cached"}],
      "usage": {
        "input_tokens": 337,
        "output_tokens": 50,
        "cache_read_input_tokens": 3905,
        "cache_creation_input_tokens": 12,
      },
      "stop_reason": "end_turn",
    }
    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps(response_body).encode()
    mock_bedrock.invoke_model.return_value = {"body": mock_body}

    result = await client.create_message(
      messages=[AIMessage(role="user", content="hi")]
    )
    assert result.input_tokens == 337
    assert result.cache_read_input_tokens == 3905
    assert result.cache_creation_input_tokens == 12

  @pytest.mark.unit
  async def test_cache_token_counts_default_to_zero_when_absent(self):
    """Older recorded responses (and non-caching models) carry no cache
    fields — they must read as zero, not KeyError."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    response_body = {
      "content": [{"type": "text", "text": "hi"}],
      "usage": {"input_tokens": 10, "output_tokens": 5},
      "stop_reason": "end_turn",
    }
    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps(response_body).encode()
    mock_bedrock.invoke_model.return_value = {"body": mock_body}

    result = await client.create_message(
      messages=[AIMessage(role="user", content="hi")]
    )
    assert result.cache_read_input_tokens == 0
    assert result.cache_creation_input_tokens == 0

  @pytest.mark.unit
  async def test_claude_4_family_sends_temperature_without_thinking(self):
    """The 4.x family accepts temperature and does not run adaptive thinking,
    so no thinking override is sent."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    response_body = {
      "content": [{"type": "text", "text": "hi"}],
      "usage": {"input_tokens": 10, "output_tokens": 5},
      "stop_reason": "end_turn",
    }
    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps(response_body).encode()
    mock_bedrock.invoke_model.return_value = {"body": mock_body}

    await client.create_message(
      messages=[AIMessage(role="user", content="hi")],
      model="claude-sonnet-4-6",
      temperature=0.3,
    )
    request_body = json.loads(mock_bedrock.invoke_model.call_args[1]["body"])
    assert request_body["temperature"] == 0.3
    assert "thinking" not in request_body

  @pytest.mark.unit
  async def test_sonnet_5_family_omits_temperature_and_disables_thinking(self):
    """Claude 5-family models 400 on `temperature` and run adaptive thinking
    unless explicitly disabled — thinking tokens would bill as output and eat
    max_tokens."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    response_body = {
      "content": [{"type": "text", "text": "hi"}],
      "usage": {"input_tokens": 10, "output_tokens": 5},
      "stop_reason": "end_turn",
    }
    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps(response_body).encode()
    mock_bedrock.invoke_model.return_value = {"body": mock_body}

    await client.create_message(
      messages=[AIMessage(role="user", content="hi")],
      model="claude-sonnet-5",
      temperature=0.3,
    )
    request_body = json.loads(mock_bedrock.invoke_model.call_args[1]["body"])
    assert "temperature" not in request_body
    assert request_body["thinking"] == {"type": "disabled"}

  @pytest.mark.unit
  async def test_create_message_formats_messages_correctly(self):
    """Test that messages are formatted into the correct dict structure."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    response_body = {
      "content": [{"text": "OK"}],
      "usage": {"input_tokens": 50, "output_tokens": 10},
    }
    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps(response_body).encode()
    mock_bedrock.invoke_model.return_value = {"body": mock_body}

    messages = [
      AIMessage(role="user", content="First message"),
      AIMessage(role="assistant", content="Response"),
      AIMessage(role="user", content="Follow up"),
    ]

    await client.create_message(messages=messages, max_tokens=1000, temperature=0.3)

    call_args = mock_bedrock.invoke_model.call_args
    request_body = json.loads(call_args[1]["body"])

    assert len(request_body["messages"]) == 3
    assert request_body["messages"][0] == {
      "role": "user",
      "content": "First message",
    }
    assert request_body["messages"][1] == {
      "role": "assistant",
      "content": "Response",
    }
    assert request_body["messages"][2] == {
      "role": "user",
      "content": "Follow up",
    }
    assert request_body["max_tokens"] == 1000
    assert request_body["temperature"] == 0.3
    assert request_body["anthropic_version"] == "bedrock-2023-05-31"

  @pytest.mark.unit
  async def test_create_message_with_model_override(self):
    """Test message creation with explicit model override."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    response_body = {
      "content": [{"text": "OK"}],
      "usage": {"input_tokens": 50, "output_tokens": 10},
    }
    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps(response_body).encode()
    mock_bedrock.invoke_model.return_value = {"body": mock_body}

    messages = [AIMessage(role="user", content="Test")]

    result = await client.create_message(
      messages=messages,
      model=BedrockModel.SONNET_4.value,
    )

    call_args = mock_bedrock.invoke_model.call_args
    expected_model_id = OperatorConfig.get_bedrock_model_id(model=BedrockModel.SONNET_4)
    assert call_args[1]["modelId"] == expected_model_id
    assert result.model == expected_model_id

  @pytest.mark.unit
  async def test_create_message_bedrock_api_error(self):
    """Test that Bedrock API errors propagate correctly."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    mock_bedrock.invoke_model.side_effect = Exception("Bedrock throttling error")

    messages = [AIMessage(role="user", content="Test")]

    with pytest.raises(Exception, match="Bedrock throttling error"):
      await client.create_message(messages=messages)

  @pytest.mark.unit
  async def test_create_message_stop_reason_none(self):
    """Test response when stop_reason is not in the response body."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    response_body = {
      "content": [{"text": "Partial response"}],
      "usage": {"input_tokens": 80, "output_tokens": 40},
    }
    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps(response_body).encode()
    mock_bedrock.invoke_model.return_value = {"body": mock_body}

    messages = [AIMessage(role="user", content="Test")]

    result = await client.create_message(messages=messages)

    assert result.stop_reason is None

  @pytest.mark.unit
  async def test_create_message_with_agent_type(self):
    """Test message creation passes operator_type for model resolution."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    response_body = {
      "content": [{"text": "OK"}],
      "usage": {"input_tokens": 50, "output_tokens": 10},
    }
    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps(response_body).encode()
    mock_bedrock.invoke_model.return_value = {"body": mock_body}

    messages = [AIMessage(role="user", content="Test")]

    result = await client.create_message(
      messages=messages,
      operator_type="financial",
    )

    expected_model_id = OperatorConfig.get_bedrock_model_id(operator_type="financial")
    assert result.model == expected_model_id

  @pytest.mark.unit
  async def test_create_message_default_parameters(self):
    """Test create_message uses correct default parameters."""
    client, mock_bedrock = _make_ai_client()
    from robosystems.operations.operators.ai_client import AIMessage

    response_body = {
      "content": [{"text": "OK"}],
      "usage": {"input_tokens": 10, "output_tokens": 5},
    }
    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps(response_body).encode()
    mock_bedrock.invoke_model.return_value = {"body": mock_body}

    messages = [AIMessage(role="user", content="Test")]

    await client.create_message(messages=messages)

    call_args = mock_bedrock.invoke_model.call_args
    request_body = json.loads(call_args[1]["body"])
    # Defaults: max_tokens=4000, temperature=0.7
    assert request_body["max_tokens"] == 4000
    assert request_body["temperature"] == 0.7


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

  `_bedrock_create_message` is `async def` and a model call can take minutes;
  running `invoke_model` inline held the single-worker loop — and every tenant
  on the task — for the whole call. It now goes through `asyncio.to_thread`.
  """

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_invoke_model_runs_in_a_thread(self):
    from robosystems.operations.operators.ai_client import AIMessage

    client, mock_bedrock = _make_ai_client()

    body = MagicMock()
    body.read.return_value = json.dumps(
      {
        "content": [{"type": "text", "text": "hi"}],
        "usage": {"input_tokens": 3, "output_tokens": 1},
        "stop_reason": "end_turn",
      }
    )
    mock_bedrock.invoke_model.return_value = {"body": body}

    with patch(f"{AI_CLIENT_MODULE}.asyncio.to_thread") as mock_to_thread:

      async def _fake_to_thread(fn, *args, **kwargs):
        # Prove the blocking call is the one being offloaded, and run it.
        assert fn == client._invoke_model_sync
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
