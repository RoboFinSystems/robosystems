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

from robosystems.config import (
  BedrockModel,
  OperatorConfig,
  env,
  model_accepts_sampling_params,
)
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
  # With prompt caching in play, `input_tokens` is the UNCACHED input only;
  # the true input is input + cache_read + cache_creation. All three must
  # reach the meter or cached tokens go unbilled.
  input_tokens: int
  output_tokens: int
  stop_reason: str | None = None
  cache_read_input_tokens: int = 0
  cache_creation_input_tokens: int = 0
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
    tool_choice: dict[str, Any] | None = None,
    cache_conversation: bool = False,
  ) -> AIResponse:
    """Send one Bedrock request.

    `tools` takes Anthropic tool definitions (name/description/input_schema);
    with them the model may stop with reason "tool_use" and return tool_use
    blocks in `AIResponse.content_blocks`. `tool_choice` is the Anthropic
    tool_choice object (e.g. ``{"type": "none"}`` to keep the tool definitions
    — required while the transcript carries tool_use blocks — but forbid
    further calls). Only meaningful alongside `tools`.

    `cache_conversation` adds a cache breakpoint on the trailing user turn.
    Only worth it for multi-call loops over a growing transcript, where the
    next call re-reads everything up to that turn; a single-shot caller would
    pay the 1.25x cache-write premium with nothing ever reading the entry.
    """
    model_id = self._get_model_id(model, operator_type)
    return await self._bedrock_create_message(
      messages,
      system,
      max_tokens,
      temperature,
      model_id,
      tools,
      tool_choice,
      cache_conversation,
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
    tool_choice: dict[str, Any] | None = None,
    cache_conversation: bool = False,
  ) -> AIResponse:
    message_dicts: list[dict[str, Any]] = [
      {"role": msg.role, "content": msg.content} for msg in messages
    ]

    if cache_conversation and message_dicts:
      # Cache breakpoint on the trailing user turn. Markers are applied here
      # at request build, never persisted into the caller's transcript — a
      # marker left on every past turn would exceed the 4-breakpoint limit.
      # The moved breakpoint still hits: the lookup resolves the longest
      # previously cached prefix, so each call reads the entry the previous
      # one wrote and extends it.
      last = message_dicts[-1]
      content = last["content"]
      if isinstance(content, str):
        content = [{"type": "text", "text": content}] if content else []
      else:
        content = list(content)
      if content:
        content[-1] = {**content[-1], "cache_control": {"type": "ephemeral"}}
        message_dicts[-1] = {"role": last["role"], "content": content}

    request_body: dict[str, Any] = {
      "anthropic_version": "bedrock-2023-05-31",
      "max_tokens": max_tokens,
      "messages": message_dicts,
    }

    if model_accepts_sampling_params(model):
      request_body["temperature"] = temperature
    else:
      # Claude 5-family models 400 on `temperature` and run adaptive thinking
      # by default; thinking tokens would bill as output and eat max_tokens,
      # so it is explicitly disabled until adopted deliberately. (Bedrock also
      # requires thinking disabled whenever tool_choice forces a tool.)
      request_body["thinking"] = {"type": "disabled"}

    if system:
      # One cache breakpoint at the end of `system` caches the tool
      # definitions and the system prompt together (the request renders
      # tools -> system -> messages). Below Sonnet's 1,024-token minimum
      # cacheable prefix the marker is accepted and silently caches nothing,
      # which costs nothing extra.
      request_body["system"] = [
        {"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}
      ]
    if tools:
      request_body["tools"] = tools
      if tool_choice:
        request_body["tool_choice"] = tool_choice

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

    usage = response_body["usage"]
    return AIResponse(
      content=text,
      model=model,
      input_tokens=usage["input_tokens"],
      output_tokens=usage["output_tokens"],
      stop_reason=response_body.get("stop_reason"),
      # Bedrock returns these on every response (zeros while no cache_control
      # is sent); .get keeps old recorded fixtures working.
      cache_read_input_tokens=usage.get("cache_read_input_tokens", 0),
      cache_creation_input_tokens=usage.get("cache_creation_input_tokens", 0),
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
