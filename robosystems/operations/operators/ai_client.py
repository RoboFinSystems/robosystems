"""Model access via AWS Bedrock Converse, plus an optional self-hosted
OpenAI-compatible model (`openai_compat.py`) that speaks the same blocks.

Which model runs is a registry row in `config/operators.py`, not a code path.
Messages carry Converse content blocks; a reasoning block the model returned
must be replayed verbatim or the transcript is rejected.
"""

import asyncio
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from botocore.exceptions import BotoCoreError, ClientError, ReadTimeoutError

from robosystems.config import (
  ModelProfile,
  ModelProvider,
  ModelSpec,
  OperatorConfig,
  OperatorModel,
  env,
)
from robosystems.logger import logger

if TYPE_CHECKING:
  from robosystems.operations.operators.openai_compat import OpenAICompatClient


class AIProviderError(Exception):
  """The provider refused or could not serve the call (auth, entitlement,
  request shape). Callers must fail the operation, never treat it as an empty
  result."""


# A generation is billed once it starts, so botocore must never re-send one:
# its default 60s read timeout plus retries re-bought every long answer. The
# read timeout covers the longest generation; only refusals that happen before
# a generation starts are retried, by `create_message`.
_BEDROCK_READ_TIMEOUT = 900
_UNSTARTED_ERROR_CODES = frozenset(
  {"ThrottlingException", "ServiceUnavailableException", "ModelNotReadyException"}
)
_UNSTARTED_MAX_ATTEMPTS = 3

_PROVIDER_ERROR_CODES = frozenset(
  {
    "AccessDeniedException",
    "ResourceNotFoundException",
    "ValidationException",
    "UnrecognizedClientException",
    "ExpiredTokenException",
    "InvalidSignatureException",
  }
)


@dataclass
class AIMessage:
  role: str
  # Plain text, or a list of Converse content blocks.
  content: str | list[dict[str, Any]]


@dataclass(frozen=True)
class ToolCall:
  """One tool request from the model, provider shape stripped."""

  id: str
  name: str
  input: dict[str, Any]


@dataclass
class AIResponse:
  content: str
  model: str
  # UNCACHED input only; all three input counts must be billed.
  input_tokens: int
  output_tokens: int
  stop_reason: str | None = None
  cache_read_input_tokens: int = 0
  cache_creation_input_tokens: int = 0
  # Full output as returned, replayed verbatim by tool loops; `content` is
  # just the joined text.
  content_blocks: list[dict[str, Any]] = field(default_factory=list)

  @property
  def tool_calls(self) -> list[ToolCall]:
    return [
      ToolCall(
        id=block["toolUse"].get("toolUseId", ""),
        name=block["toolUse"].get("name", ""),
        input=block["toolUse"].get("input") or {},
      )
      for block in self.content_blocks
      if isinstance(block, dict) and "toolUse" in block
    ]


def text_block(text: str) -> dict[str, Any]:
  return {"text": text}


def tool_result_block(
  tool_use_id: str, content: str, is_error: bool = False
) -> dict[str, Any]:
  return {
    "toolResult": {
      "toolUseId": tool_use_id,
      "content": [{"text": content}],
      "status": "error" if is_error else "success",
    }
  }


_CACHE_POINT: dict[str, Any] = {"cachePoint": {"type": "default"}}


class AIClient:
  """Untracked model access; billable paths wrap it in `TrackedAIClient`."""

  def __init__(self):
    self.backend = "bedrock"
    self.client = self._initialize_bedrock_client()
    logger.info("Initialized AI client with AWS Bedrock")
    self._self_hosted = self._initialize_self_hosted_client()

  def _initialize_bedrock_client(self):
    import boto3

    from robosystems.operations.aws.long_call import long_call_client

    # Explicit endpoint so LocalStack's AWS_ENDPOINT_URL is bypassed.
    bedrock_endpoint = f"https://bedrock-runtime.{env.AWS_BEDROCK_REGION}.amazonaws.com"

    kwargs = {
      "region_name": env.AWS_BEDROCK_REGION,
      "endpoint_url": bedrock_endpoint,
    }

    if env.ENVIRONMENT == "dev" and env.AWS_BEDROCK_ACCESS_KEY_ID:
      kwargs["aws_access_key_id"] = env.AWS_BEDROCK_ACCESS_KEY_ID
      kwargs["aws_secret_access_key"] = env.AWS_BEDROCK_SECRET_ACCESS_KEY
      logger.info("Using Bedrock with dev credentials (AWS_BEDROCK_ACCESS_KEY_ID)")
    else:
      logger.info(
        f"Using Bedrock with IAM role credentials (environment: {env.ENVIRONMENT})"
      )

    try:
      client = long_call_client("bedrock-runtime", _BEDROCK_READ_TIMEOUT, **kwargs)
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

  @staticmethod
  def _initialize_self_hosted_client() -> "OpenAICompatClient | None":
    if not env.OPENAI_COMPAT_ENABLED:
      return None
    from robosystems.operations.operators.openai_compat import OpenAICompatClient

    tiers = sorted(
      profile.value
      for profile, model in OperatorConfig.PROFILE_MODELS.items()
      if model is OperatorModel.OPENAI_COMPAT
    )
    logger.info(
      f"Self-hosted model enabled: {env.OPENAI_COMPAT_MODEL} "
      f"(tiers: {tiers or 'none'}; credits per 1K in/out: "
      f"{env.OPENAI_COMPAT_CREDITS_PER_1K_INPUT}/"
      f"{env.OPENAI_COMPAT_CREDITS_PER_1K_OUTPUT})"
    )
    return OpenAICompatClient(
      base_url=env.OPENAI_COMPAT_BASE_URL,
      api_key=env.OPENAI_COMPAT_API_KEY,
      timeout_seconds=env.OPENAI_COMPAT_TIMEOUT_SECONDS,
    )

  async def create_message(
    self,
    messages: list[AIMessage],
    system: str | None = None,
    max_tokens: int = 4000,
    temperature: float = 0.7,
    model: str | OperatorModel | ModelProfile | None = None,
    operator_type: str | None = None,
    tools: list[dict[str, Any]] | None = None,
    cache_conversation: bool = False,
  ) -> AIResponse:
    """Send one Converse request.

    `model` is a profile, registered short name, or wire id; an unregistered
    name raises. `tools` takes MCP-shaped definitions. Converse cannot forbid
    tool use, so a loop wanting a final answer asks in the prompt and treats a
    stray tool call as terminal. `cache_conversation` caches the trailing turn;
    only worth the cache-write premium for multi-call loops.
    """
    spec = OperatorConfig.resolve_model(model, operator_type)
    if spec.provider is ModelProvider.OPENAI_COMPAT:
      if self._self_hosted is None:
        raise AIProviderError(
          f"{spec.model_id} is a self-hosted model but OPENAI_COMPAT_ENABLED is off"
        )
      logger.debug(f"Using self-hosted model: {spec.model_id}")
      return await self._self_hosted.create_message(
        spec, messages, system, max_tokens, temperature, tools
      )

    logger.debug(f"Using Bedrock model: {spec.model_id}")
    request = self._build_request(
      spec, messages, system, max_tokens, temperature, tools, cache_conversation
    )

    # Minutes-long sync call: off the event loop, on the default executor
    # rather than `run_off_loop`, whose limiter is sized for short OLTP work.
    try:
      response = await self._converse_with_retry(request)
    except ReadTimeoutError as e:
      raise AIProviderError(
        f"Bedrock call to {spec.model_id} produced no response within "
        f"{_BEDROCK_READ_TIMEOUT}s. The model may have run (and been "
        "billed); it was not re-sent."
      ) from e
    except ClientError as e:
      code = e.response.get("Error", {}).get("Code", "")
      if code in _PROVIDER_ERROR_CODES:
        message = e.response.get("Error", {}).get("Message", str(e))
        raise AIProviderError(
          f"Bedrock refused the call to {spec.model_id} ({code}): {message}. "
          "Check the model entitlement and the caller's credentials "
          "(locally: `just setup-bedrock`)."
        ) from e
      raise
    except BotoCoreError as e:
      raise AIProviderError(
        f"Bedrock call to {spec.model_id} failed before reaching the model: {e}"
      ) from e

    return self._parse_response(response, spec)

  async def _converse_with_retry(self, request: dict[str, Any]) -> dict[str, Any]:
    """Retry only refusals that precede generation (nothing was billed)."""
    for attempt in range(1, _UNSTARTED_MAX_ATTEMPTS + 1):
      try:
        return await asyncio.to_thread(self._converse_sync, request)
      except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code not in _UNSTARTED_ERROR_CODES or attempt == _UNSTARTED_MAX_ATTEMPTS:
          raise
        await asyncio.sleep(2**attempt)
    raise AssertionError("unreachable")

  def _converse_sync(self, request: dict[str, Any]) -> dict[str, Any]:
    return self.client.converse(**request)

  @staticmethod
  def _build_request(
    spec: ModelSpec,
    messages: list[AIMessage],
    system: str | None,
    max_tokens: int,
    temperature: float,
    tools: list[dict[str, Any]] | None,
    cache_conversation: bool,
  ) -> dict[str, Any]:
    message_dicts: list[dict[str, Any]] = [
      {
        "role": msg.role,
        "content": [text_block(msg.content)]
        if isinstance(msg.content, str)
        else list(msg.content),
      }
      for msg in messages
    ]

    if cache_conversation and spec.cache_points and message_dicts:
      # Added per request, never persisted to the transcript: markers on
      # every past turn would exceed the 4-breakpoint limit.
      message_dicts[-1]["content"].append(dict(_CACHE_POINT))

    max_out = max_tokens
    if spec.max_output_tokens is not None:
      max_out = min(max_tokens, spec.max_output_tokens)
    inference: dict[str, Any] = {"maxTokens": max_out}
    if spec.accepts_sampling_params:
      inference["temperature"] = temperature

    request: dict[str, Any] = {
      "modelId": spec.model_id,
      "messages": message_dicts,
      "inferenceConfig": inference,
    }
    if spec.additional_request_fields:
      request["additionalModelRequestFields"] = dict(spec.additional_request_fields)

    if system:
      # Caches tools and system together (Converse orders tools -> system ->
      # messages).
      system_blocks: list[dict[str, Any]] = [text_block(system)]
      if spec.cache_points:
        system_blocks.append(dict(_CACHE_POINT))
      request["system"] = system_blocks

    if tools:
      request["toolConfig"] = {
        "tools": [
          {
            "toolSpec": {
              "name": tool["name"],
              "description": tool["description"],
              "inputSchema": {"json": tool["inputSchema"]},
            }
          }
          for tool in tools
        ]
      }
    return request

  @staticmethod
  def _parse_response(response: dict[str, Any], spec: ModelSpec) -> AIResponse:
    # The first block need not be text; join every text block.
    blocks = response.get("output", {}).get("message", {}).get("content", [])
    text = "".join(
      b.get("text", "") for b in blocks if isinstance(b, dict) and "text" in b
    )

    usage = response["usage"]
    return AIResponse(
      content=text,
      model=spec.model_id,
      input_tokens=usage["inputTokens"],
      output_tokens=usage["outputTokens"],
      stop_reason=response.get("stopReason"),
      cache_read_input_tokens=usage.get("cacheReadInputTokens", 0),
      cache_creation_input_tokens=usage.get("cacheWriteInputTokens", 0),
      content_blocks=blocks,
    )


_shared_client: AIClient | None = None
_shared_client_lock = threading.Lock()


def get_ai_client() -> AIClient:
  """Process-wide `AIClient`; a failed construction is not cached."""
  global _shared_client
  if _shared_client is None:
    with _shared_client_lock:
      if _shared_client is None:
        _shared_client = AIClient()
  return _shared_client
