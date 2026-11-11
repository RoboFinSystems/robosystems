"""Claude through the Anthropic API on a developer's own key, off unless
``ANTHROPIC_API_ENABLED`` and refused outside local development. Converse
stays the canonical transcript; this module translates at the edge in both
directions."""

import asyncio
from typing import Any

import anthropic

from robosystems.config import ModelSpec
from robosystems.operations.operators.ai_client import (
  AIMessage,
  AIProviderError,
  AIResponse,
)

# A generation is billed once it starts, so the SDK must never re-send one;
# only refusals that precede a generation are retried, here.
_READ_TIMEOUT_SECONDS = 900
_UNSTARTED_STATUS_CODES = frozenset({429, 503, 529})
_UNSTARTED_MAX_ATTEMPTS = 3

_CACHE_CONTROL: dict[str, Any] = {"type": "ephemeral"}

# A block with no Converse form is carried under this key and sent back as it
# came.
_NATIVE_BLOCK_KEY = "anthropicBlock"

# Messages stop reasons the tool loop does not know, as the Converse ones it
# does.
_STOP_REASONS = {"refusal": "content_filtered"}


class AnthropicAPIClient:
  def __init__(self, api_key: str, base_url: str | None = None) -> None:
    self._api_key = api_key
    self._base_url = base_url

  async def create_message(
    self,
    spec: ModelSpec,
    messages: list[AIMessage],
    system: str | None,
    max_tokens: int,
    temperature: float,
    tools: list[dict[str, Any]] | None,
    cache_conversation: bool,
    effort: str | None,
  ) -> AIResponse:
    request = build_messages_request(
      spec, messages, system, max_tokens, temperature, tools, cache_conversation, effort
    )
    # A client per call: the process-wide AIClient outlives any one event loop,
    # and the SDK's connection pool is bound to the loop that opened it.
    async with anthropic.AsyncAnthropic(
      api_key=self._api_key,
      base_url=self._base_url,
      max_retries=0,
      timeout=_READ_TIMEOUT_SECONDS,
    ) as client:
      response = await self._create_with_retry(client, request, spec)
    return parse_messages_response(response.to_dict(), spec)

  @staticmethod
  async def _create_with_retry(
    client: anthropic.AsyncAnthropic, request: dict[str, Any], spec: ModelSpec
  ) -> Any:
    for attempt in range(1, _UNSTARTED_MAX_ATTEMPTS + 1):
      try:
        return await client.messages.create(**request)
      except anthropic.APITimeoutError as e:
        raise AIProviderError(
          f"Anthropic API call to {spec.model_id} produced no response within "
          f"{_READ_TIMEOUT_SECONDS}s. The model may have run (and been "
          "billed); it was not re-sent."
        ) from e
      except anthropic.APIConnectionError as e:
        raise AIProviderError(
          f"Anthropic API call to {spec.model_id} failed before reaching the model: {e}"
        ) from e
      except anthropic.APIStatusError as e:
        unstarted = e.status_code in _UNSTARTED_STATUS_CODES
        if unstarted and attempt < _UNSTARTED_MAX_ATTEMPTS:
          await asyncio.sleep(2**attempt)
          continue
        raise AIProviderError(
          f"The Anthropic API refused the call to {spec.model_id} "
          f"(HTTP {e.status_code}): {e.message}. Check ANTHROPIC_API_KEY and "
          "the account's credit balance."
        ) from e
    raise AssertionError("unreachable")


def build_messages_request(
  spec: ModelSpec,
  messages: list[AIMessage],
  system: str | None,
  max_tokens: int,
  temperature: float,
  tools: list[dict[str, Any]] | None,
  cache_conversation: bool = False,
  effort: str | None = None,
) -> dict[str, Any]:
  wire_messages = [
    {"role": message.role, "content": to_messages_content(message.content)}
    for message in messages
  ]
  if cache_conversation and spec.cache_points and wire_messages:
    # Added per request, never persisted to the transcript: markers on every
    # past turn would exceed the 4-breakpoint limit.
    last = wire_messages[-1]["content"]
    if last:
      last[-1] = {**last[-1], "cache_control": dict(_CACHE_CONTROL)}

  max_out = max_tokens
  if spec.max_output_tokens is not None:
    max_out = min(max_tokens, spec.max_output_tokens)
  request: dict[str, Any] = {
    "model": spec.model_id,
    "max_tokens": max_out,
    "messages": wire_messages,
    # The registry's request fields (thinking) are top-level here.
    **spec.additional_request_fields,
  }
  if spec.accepts_sampling_params:
    request["temperature"] = temperature
  if effort and spec.supports_effort:
    request["output_config"] = {"effort": effort}

  if system:
    # Caches tools and system together (the API orders tools -> system ->
    # messages).
    block: dict[str, Any] = {"type": "text", "text": system}
    if spec.cache_points:
      block["cache_control"] = dict(_CACHE_CONTROL)
    request["system"] = [block]

  if tools:
    request["tools"] = [
      {
        "name": tool["name"],
        "description": tool["description"],
        "input_schema": tool["inputSchema"],
      }
      for tool in tools
    ]
  return request


def to_messages_content(content: str | list[dict[str, Any]]) -> list[dict[str, Any]]:
  """Converse content blocks as Messages API ones."""
  if isinstance(content, str):
    return [{"type": "text", "text": content}]
  out: list[dict[str, Any]] = []
  for block in content:
    if _NATIVE_BLOCK_KEY in block:
      out.append(dict(block[_NATIVE_BLOCK_KEY]))
    elif "text" in block:
      out.append({"type": "text", "text": block["text"]})
    elif "toolUse" in block:
      use = block["toolUse"]
      out.append(
        {
          "type": "tool_use",
          "id": use.get("toolUseId", ""),
          "name": use.get("name", ""),
          "input": use.get("input") or {},
        }
      )
    elif "toolResult" in block:
      result = block["toolResult"]
      out.append(
        {
          "type": "tool_result",
          "tool_use_id": result.get("toolUseId", ""),
          "content": [
            {"type": "text", "text": part["text"]}
            for part in result.get("content") or []
            if isinstance(part, dict) and "text" in part
          ],
          "is_error": result.get("status") == "error",
        }
      )
    elif "reasoningContent" in block:
      out.append(_thinking_block(block["reasoningContent"]))
    # A cachePoint is Bedrock's marker; this route sets its own.
  return out


def _thinking_block(reasoning: dict[str, Any]) -> dict[str, Any]:
  if "redactedContent" in reasoning:
    return {"type": "redacted_thinking", "data": reasoning["redactedContent"]}
  text = reasoning.get("reasoningText") or {}
  return {
    "type": "thinking",
    "thinking": text.get("text", ""),
    "signature": text.get("signature", ""),
  }


def to_converse_blocks(content: list[dict[str, Any]]) -> list[dict[str, Any]]:
  """A Messages API response's content as Converse blocks. A thinking block
  keeps its text and signature exactly, since it is replayed on the next
  call."""
  out: list[dict[str, Any]] = []
  for block in content:
    kind = block.get("type")
    if kind == "text":
      out.append({"text": block.get("text", "")})
    elif kind == "tool_use":
      out.append(
        {
          "toolUse": {
            "toolUseId": block.get("id", ""),
            "name": block.get("name", ""),
            "input": block.get("input") or {},
          }
        }
      )
    elif kind == "thinking":
      out.append(
        {
          "reasoningContent": {
            "reasoningText": {
              "text": block.get("thinking", ""),
              "signature": block.get("signature", ""),
            }
          }
        }
      )
    elif kind == "redacted_thinking":
      out.append({"reasoningContent": {"redactedContent": block.get("data", "")}})
    else:
      out.append({_NATIVE_BLOCK_KEY: dict(block)})
  return out


def parse_messages_response(payload: dict[str, Any], spec: ModelSpec) -> AIResponse:
  content = payload.get("content") or []
  usage = payload.get("usage") or {}
  stop_reason = payload.get("stop_reason")
  return AIResponse(
    content="".join(b.get("text", "") for b in content if b.get("type") == "text"),
    model=spec.model_id,
    input_tokens=usage.get("input_tokens") or 0,
    output_tokens=usage.get("output_tokens") or 0,
    stop_reason=_STOP_REASONS.get(stop_reason, stop_reason),
    cache_read_input_tokens=usage.get("cache_read_input_tokens") or 0,
    cache_creation_input_tokens=usage.get("cache_creation_input_tokens") or 0,
    content_blocks=to_converse_blocks(content),
  )
