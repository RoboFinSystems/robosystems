"""Model access via AWS Bedrock Converse.

Bedrock is the platform's path, deliberately: it puts model spend in AWS Cost
Explorer alongside everything else, emits CloudWatch token metrics, and lets
IAM rather than a shared API key control who can call a model. Converse is
the one request shape Bedrock serves every model through — Claude, GPT-5.6,
the open-weight families — with a single tool protocol and usage block, so
which model runs is a registry row (`config/operators.py`), not a code path.

The one other path is a deployment's self-hosted model behind an
OpenAI-compatible endpoint (`openai_compat.py`), registered only when the
deployment turns it on. It speaks the same Converse blocks to its callers.

Messages carry Converse content blocks: `{"text": ...}`, `{"toolUse": ...}`,
`{"toolResult": ...}`, and whatever the model returned (a reasoning block
must be replayed verbatim or the transcript is rejected).
"""

import asyncio
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from botocore.exceptions import BotoCoreError, ClientError

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
  """The provider refused or could not serve the call.

  Auth, entitlement, or request-shape failures — not a model answer. Callers
  must surface it as a failed operation, never count it as an empty result:
  a per-batch swallow turned a total Bedrock outage into a "successful" run
  that mapped nothing.
  """


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
  # A turn is either plain text or a list of Converse content blocks.
  # Tool-use loops append block lists; single-shot callers pass a string.
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
  # With prompt caching in play, `input_tokens` is the UNCACHED input only;
  # the true input is input + cache_read + cache_creation. All three must
  # reach the meter or cached tokens go unbilled.
  input_tokens: int
  output_tokens: int
  stop_reason: str | None = None
  cache_read_input_tokens: int = 0
  cache_creation_input_tokens: int = 0
  # The model's full output content, as Converse returned it. A tool loop
  # replays it verbatim as the assistant turn; `content` above is the
  # concatenated text for simple callers.
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
  """A tool result the model can read back, flagged when the tool failed so
  it can correct itself."""
  return {
    "toolResult": {
      "toolUseId": tool_use_id,
      "content": [{"text": content}],
      "status": "error" if is_error else "success",
    }
  }


_CACHE_POINT: dict[str, Any] = {"cachePoint": {"type": "default"}}


class AIClient:
  """Untracked model access: Bedrock, plus the self-hosted model when enabled.

  Callers on a billable path use `TrackedAIClient`, which wraps this and
  consumes credits per call.
  """

  def __init__(self):
    self.backend = "bedrock"
    self.client = self._initialize_bedrock_client()
    logger.info("Initialized AI client with AWS Bedrock")
    self._self_hosted = self._initialize_self_hosted_client()

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

    `model` is a profile, a registered short name, or a wire id; unset, the
    operator class's override or the platform default applies. An
    unregistered name raises rather than silently running something else.

    `tools` takes MCP-shaped definitions (name / description / inputSchema);
    with them the model may stop with reason "tool_use" and the calls are on
    `AIResponse.tool_calls`. Tool choice is always the model's: Converse has
    no "forbid tools" option, and the transcript must keep `toolConfig` while
    it carries tool blocks, so a loop that wants a final answer says so in
    the prompt and treats a stray tool call as terminal.

    `cache_conversation` adds a cache breakpoint on the trailing user turn.
    Only worth it for multi-call loops over a growing transcript, where the
    next call re-reads everything up to that turn; a single-shot caller would
    pay the 1.25x cache-write premium with nothing ever reading the entry.
    """
    spec = OperatorConfig.resolve_model(model, operator_type)
    if spec.provider is ModelProvider.OPENAI_COMPAT:
      # The row is registered only when enabled, which is also when the
      # client is built; reaching here without one is a wiring fault.
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

    # botocore is synchronous and a model call can take minutes; run it on a
    # worker thread so the event loop — shared by every tenant on this task —
    # is not held for the duration. The default executor, not `run_off_loop`:
    # that limiter fronts short OLTP work and must not be exhausted by
    # minutes-long calls.
    try:
      response = await asyncio.to_thread(self._converse_sync, request)
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
      # Cache breakpoint on the trailing turn. Applied here at request build,
      # never persisted into the caller's transcript — a marker left on every
      # past turn would exceed the 4-breakpoint limit. The moved breakpoint
      # still hits: the lookup resolves the longest previously cached prefix,
      # so each call reads the entry the previous one wrote and extends it.
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
      # One cache breakpoint at the end of `system` caches the tool
      # definitions and the system prompt together (Converse evaluates
      # tools -> system -> messages, cumulatively). Below the model's
      # minimum cacheable prefix the marker is accepted and silently caches
      # nothing, which costs nothing extra.
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
    # A response may interleave text, tool-use, and reasoning blocks; the
    # first block is not guaranteed to be text (a pure tool-use turn has
    # none). Join every text block for the back-compat `content` string and
    # hand back the full block list for tool loops to replay.
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
      # Present on every response for caching models (zeros when nothing
      # cached); .get keeps models that omit them reading as zero.
      cache_read_input_tokens=usage.get("cacheReadInputTokens", 0),
      cache_creation_input_tokens=usage.get("cacheWriteInputTokens", 0),
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
