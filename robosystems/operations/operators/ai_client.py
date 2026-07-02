"""
AI Client - AWS Bedrock interface for Claude models.

Production-grade AI client using AWS Bedrock exclusively for:
- Cost tracking in AWS Cost Explorer
- CloudWatch metrics and monitoring
- IAM-based access control
"""

import json
from dataclasses import dataclass, field
from typing import Any

from robosystems.config import BedrockModel, OperatorConfig, env
from robosystems.logger import logger


@dataclass
class AIMessage:
  role: str
  # A turn is either plain text or a list of Anthropic content blocks
  # (tool_use / tool_result). Tool-use loops append block lists; simple
  # single-shot callers still pass a string.
  content: str | list[dict[str, Any]]


@dataclass
class AIResponse:
  content: str
  model: str
  input_tokens: int
  output_tokens: int
  stop_reason: str | None = None
  # Full response content blocks (text + tool_use). Populated for tool-use
  # loops; `content` above is the concatenated text for simple callers.
  content_blocks: list[dict[str, Any]] = field(default_factory=list)


class AIClient:
  """
  AWS Bedrock AI client for Claude models.

  Uses AWS Bedrock exclusively for all AI operations to ensure:
  - All costs appear in AWS Cost Explorer
  - CloudWatch metrics for token usage
  - IAM-based access control
  """

  def __init__(self):
    self.backend = "bedrock"
    self.client = self._initialize_bedrock_client()
    logger.info("Initialized AI client with AWS Bedrock")

  def _initialize_bedrock_client(self):
    """Initialize AWS Bedrock client."""
    import boto3

    # Build real AWS endpoint URL (bypass LocalStack's AWS_ENDPOINT_URL env var)
    bedrock_endpoint = f"https://bedrock-runtime.{env.AWS_BEDROCK_REGION}.amazonaws.com"

    kwargs = {
      "service_name": "bedrock-runtime",
      "region_name": env.AWS_BEDROCK_REGION,
      "endpoint_url": bedrock_endpoint,  # IMPORTANT: Bypass LocalStack, go directly to AWS
    }

    # In dev: use explicit credentials (AWS_BEDROCK_ACCESS_KEY_ID)
    # In prod/staging: use IAM role credentials (ECS task role / EC2 instance profile)
    if env.ENVIRONMENT == "dev" and env.AWS_BEDROCK_ACCESS_KEY_ID:
      kwargs["aws_access_key_id"] = env.AWS_BEDROCK_ACCESS_KEY_ID
      kwargs["aws_secret_access_key"] = env.AWS_BEDROCK_SECRET_ACCESS_KEY
      logger.info("Using Bedrock with dev credentials (AWS_BEDROCK_ACCESS_KEY_ID)")
    else:
      logger.info(
        f"Using Bedrock with IAM role credentials (environment: {env.ENVIRONMENT})"
      )

    try:
      client = boto3.client(**kwargs)
      # Verify credentials work (skip in dev - LocalStack doesn't have STS)
      if env.ENVIRONMENT != "dev":
        sts_kwargs = {"service_name": "sts", "region_name": env.AWS_BEDROCK_REGION}
        if env.AWS_BEDROCK_ACCESS_KEY_ID:
          sts_kwargs["aws_access_key_id"] = env.AWS_BEDROCK_ACCESS_KEY_ID
          sts_kwargs["aws_secret_access_key"] = env.AWS_BEDROCK_SECRET_ACCESS_KEY
        boto3.client(**sts_kwargs).get_caller_identity()
      return client
    except Exception as e:
      raise ValueError(
        f"Failed to initialize AWS Bedrock client: {e}\n"
        "Ensure AWS credentials are configured (aws configure) or set:\n"
        "  AWS_BEDROCK_ACCESS_KEY_ID and AWS_BEDROCK_SECRET_ACCESS_KEY"
      )

  def _get_model_id(
    self, model: str | None = None, operator_type: str | None = None
  ) -> str:
    """
    Get the Bedrock model ID.

    Args:
        model: Optional model name override (e.g., 'claude-3-5-sonnet-20241022')
        operator_type: Optional operator type to check for overrides

    Returns:
        Bedrock model ID string
    """
    if model:
      try:
        model_enum = BedrockModel(model)
      except ValueError:
        logger.warning(f"Invalid model '{model}', using default")
        model_enum = None
    else:
      model_enum = None

    bedrock_id = OperatorConfig.get_bedrock_model_id(
      model=model_enum, operator_type=operator_type
    )
    logger.debug(f"Using Bedrock model: {bedrock_id}")
    return bedrock_id

  async def create_message(
    self,
    messages: list[AIMessage],
    system: str | None = None,
    max_tokens: int = 4000,
    temperature: float = 0.7,
    model: str | None = None,
    operator_type: str | None = None,
    tools: list[dict[str, Any]] | None = None,
  ) -> AIResponse:
    """
    Create a message using AWS Bedrock.

    Args:
        messages: List of conversation messages
        system: Optional system prompt
        max_tokens: Maximum tokens in response
        temperature: Sampling temperature (0-1)
        model: Optional model name override
        operator_type: Optional operator type for model override lookup
        tools: Optional Anthropic tool definitions (name/description/
            input_schema). When provided the model may return tool_use
            blocks and stop_reason "tool_use" — see AIResponse.content_blocks.

    Returns:
        AIResponse with content and token usage
    """
    model_id = self._get_model_id(model, operator_type)
    return await self._bedrock_create_message(
      messages, system, max_tokens, temperature, model_id, tools
    )

  async def _bedrock_create_message(
    self,
    messages: list[AIMessage],
    system: str | None,
    max_tokens: int,
    temperature: float,
    model: str,
    tools: list[dict[str, Any]] | None = None,
  ) -> AIResponse:
    """Create message using AWS Bedrock."""
    message_dicts = [{"role": msg.role, "content": msg.content} for msg in messages]

    request_body: dict[str, Any] = {
      "anthropic_version": "bedrock-2023-05-31",
      "max_tokens": max_tokens,
      "temperature": temperature,
      "messages": message_dicts,
    }

    if system:
      request_body["system"] = system
    if tools:
      request_body["tools"] = tools

    response = self.client.invoke_model(
      modelId=model,
      body=json.dumps(request_body),
    )

    response_body = json.loads(response["body"].read())

    # A response may interleave text and tool_use blocks; the first block
    # is not guaranteed to be text (a pure tool_use turn has none). Join every
    # text-bearing block for the back-compat `content` string (tool_use blocks
    # carry no "text" key), and hand back the full block list for tool loops.
    blocks = response_body.get("content", [])
    text = "".join(
      b.get("text", "") for b in blocks if isinstance(b, dict) and "text" in b
    )

    return AIResponse(
      content=text,
      model=model,
      input_tokens=response_body["usage"]["input_tokens"],
      output_tokens=response_body["usage"]["output_tokens"],
      stop_reason=response_body.get("stop_reason"),
      content_blocks=blocks,
    )
