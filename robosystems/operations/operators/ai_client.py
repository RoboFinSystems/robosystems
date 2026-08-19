"""Claude access via AWS Bedrock.

Bedrock is the only path, deliberately: it puts model spend in AWS Cost
Explorer alongside everything else, emits CloudWatch token metrics, and lets
IAM rather than a shared API key control who can call a model.
"""

import asyncio
import json
import threading
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
  """Untracked Bedrock access.

  Callers on a billable path use `TrackedAIClient`, which wraps this and
  consumes credits per call.
  """

  def __init__(self):
    self.backend = "bedrock"
    self.client = self._initialize_bedrock_client()
    logger.info("Initialized AI client with AWS Bedrock")

  def _initialize_bedrock_client(self):
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
      # Fail at construction rather than on the first (billable) call. Skipped
      # in dev, where LocalStack has no STS to call.
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
    """Resolve a Bedrock model id from an optional override and operator type.

    An unrecognized `model` falls back to the configured default rather than
    raising — see `OperatorConfig.get_bedrock_model_id`.
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
    """Send one Bedrock request.

    `tools` takes Anthropic tool definitions (name/description/input_schema);
    with them the model may stop with reason "tool_use" and return tool_use
    blocks in `AIResponse.content_blocks`.
    """
    model_id = self._get_model_id(model, operator_type)
    return await self._bedrock_create_message(
      messages, system, max_tokens, temperature, model_id, tools
    )

  def _invoke_model_sync(
    self, model: str, request_body: dict[str, Any]
  ) -> dict[str, Any]:
    response = self.client.invoke_model(
      modelId=model,
      body=json.dumps(request_body),
    )
    return json.loads(response["body"].read())

  async def _bedrock_create_message(
    self,
    messages: list[AIMessage],
    system: str | None,
    max_tokens: int,
    temperature: float,
    model: str,
    tools: list[dict[str, Any]] | None = None,
  ) -> AIResponse:
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

    # botocore is synchronous and a model call can take minutes; run it on a
    # worker thread so the event loop — shared by every tenant on this task —
    # is not held for the duration. The default executor, not `run_off_loop`:
    # that limiter fronts short OLTP work and must not be exhausted by
    # minutes-long calls.
    response_body = await asyncio.to_thread(
      self._invoke_model_sync, model, request_body
    )

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


_shared_client: AIClient | None = None
_shared_client_lock = threading.Lock()


def get_ai_client() -> AIClient:
  """Process-wide `AIClient`.

  boto3 clients are thread-safe, and constructing one per request put a
  synchronous client build plus an STS round-trip on the event loop every
  time an operator ran. A failed construction is not cached, so the next
  call retries.
  """
  global _shared_client
  if _shared_client is None:
    with _shared_client_lock:
      if _shared_client is None:
        _shared_client = AIClient()
  return _shared_client
