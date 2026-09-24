"""Model access via a self-hosted Chat Completions endpoint (vLLM, Ollama, NIM,
...), off unless ``OPENAI_COMPAT_ENABLED``. Converse stays the canonical
transcript; this module translates at the edge in both directions."""

import json
import re
import uuid
from typing import Any

import httpx

from robosystems.config import ModelSpec
from robosystems.operations.operators.ai_client import (
  AIMessage,
  AIProviderError,
  AIResponse,
)

# Where a tool call's arguments land when the model emitted something that is
# not a JSON object. The tool rejects the unknown argument and the model reads
# that error back — the same self-correction path as any other bad call.
UNPARSED_ARGUMENTS_KEY = "_unparsed_arguments"

# Chat Completions finish reasons → the Converse stop reasons the tool loop
# reads. A turn carrying tool calls is "tool_use" whatever the server says:
# some servers report "stop" alongside them.
_STOP_REASONS = {
  "tool_calls": "tool_use",
  "function_call": "tool_use",
  "length": "max_tokens",
  "stop": "end_turn",
  "content_filter": "content_filtered",
}

# Reasoning models served without a reasoning parser put their thinking in the
# answer text. It is not answer, and it must not be replayed.
_THINK_BLOCK = re.compile(r"<think>.*?</think>\s*", re.DOTALL)


class OpenAICompatClient:
  def __init__(
    self,
    base_url: str,
    api_key: str,
    timeout_seconds: int,
    transport: httpx.AsyncBaseTransport | None = None,
  ) -> None:
    self._url = f"{base_url.rstrip('/')}/chat/completions"
    self._headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
    self._timeout = timeout_seconds
    self._transport = transport

  async def create_message(
    self,
    spec: ModelSpec,
    messages: list[AIMessage],
    system: str | None,
    max_tokens: int,
    temperature: float,
    tools: list[dict[str, Any]] | None,
  ) -> AIResponse:
    request = build_chat_request(spec, messages, system, max_tokens, temperature, tools)
    # A client per call: the process-wide AIClient outlives any one event loop,
    # and an httpx connection pool is bound to the loop that opened it.
    try:
      async with httpx.AsyncClient(
        headers=self._headers, timeout=self._timeout, transport=self._transport
      ) as http:
        response = await http.post(self._url, json=request)
    except httpx.HTTPError as e:
      raise AIProviderError(
        f"Self-hosted model {spec.model_id} failed before answering: {e!r}. "
        "Check OPENAI_COMPAT_BASE_URL and that the server is running."
      ) from e

    if response.status_code >= 400:
      raise AIProviderError(
        f"Self-hosted endpoint refused the call to {spec.model_id} "
        f"(HTTP {response.status_code}): {response.text[:500]}"
      )
    try:
      payload = response.json()
    except ValueError as e:
      raise AIProviderError(
        f"Self-hosted endpoint returned a non-JSON body for {spec.model_id}"
      ) from e
    return parse_chat_response(payload, spec)


def build_chat_request(
  spec: ModelSpec,
  messages: list[AIMessage],
  system: str | None,
  max_tokens: int,
  temperature: float,
  tools: list[dict[str, Any]] | None,
) -> dict[str, Any]:
  max_out = max_tokens
  if spec.max_output_tokens is not None:
    max_out = min(max_tokens, spec.max_output_tokens)

  request: dict[str, Any] = {
    "model": spec.model_id,
    "messages": to_chat_messages(messages, system),
    "max_tokens": max_out,
  }
  if spec.accepts_sampling_params:
    request["temperature"] = temperature
  if tools:
    request["tools"] = [
      {
        "type": "function",
        "function": {
          "name": tool["name"],
          "description": tool["description"],
          "parameters": tool["inputSchema"],
        },
      }
      for tool in tools
    ]
    request["tool_choice"] = "auto"
  return request


def to_chat_messages(
  messages: list[AIMessage], system: str | None
) -> list[dict[str, Any]]:
  """Converse turns → Chat Completions messages.

  Only text, tool-use, and tool-result blocks carry over. Reasoning blocks
  are dropped (a server that emitted one does not want it replayed, and
  several reject it), and cache points mean nothing here.
  """
  out: list[dict[str, Any]] = []
  if system:
    out.append({"role": "system", "content": system})
  for msg in messages:
    if isinstance(msg.content, str):
      out.append({"role": msg.role, "content": msg.content})
    elif msg.role == "assistant":
      out.append(_assistant_message(msg.content))
    else:
      out.extend(_user_messages(msg.content))
  return out


def _assistant_message(blocks: list[dict[str, Any]]) -> dict[str, Any]:
  text = "".join(b["text"] for b in blocks if isinstance(b, dict) and "text" in b)
  tool_calls = [
    {
      "id": b["toolUse"]["toolUseId"],
      "type": "function",
      "function": {
        "name": b["toolUse"]["name"],
        "arguments": json.dumps(b["toolUse"].get("input") or {}),
      },
    }
    for b in blocks
    if isinstance(b, dict) and "toolUse" in b
  ]
  message: dict[str, Any] = {
    "role": "assistant",
    "content": text or (None if tool_calls else ""),
  }
  if tool_calls:
    message["tool_calls"] = tool_calls
  return message


def _user_messages(blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
  """One Converse user turn → tool messages, then any text as a user message.

  Tool messages must directly follow the assistant turn that called them, so
  they go first; the tool loop's wrap-up nudge rides as trailing text.
  """
  out: list[dict[str, Any]] = []
  texts: list[str] = []
  for block in blocks:
    if not isinstance(block, dict):
      continue
    if "toolResult" in block:
      result = block["toolResult"]
      body = "".join(_result_text(part) for part in result.get("content", []))
      # Chat Completions has no error flag on a tool message; say it in words
      # so the model still knows to correct itself.
      if result.get("status") == "error":
        body = f"Error: {body}"
      out.append({"role": "tool", "tool_call_id": result["toolUseId"], "content": body})
    elif "text" in block:
      texts.append(block["text"])
  if texts:
    out.append({"role": "user", "content": "\n\n".join(texts)})
  return out


def _result_text(part: Any) -> str:
  if not isinstance(part, dict):
    return ""
  if "text" in part:
    return part["text"]
  if "json" in part:
    return json.dumps(part["json"], default=str)
  return ""


def parse_chat_response(payload: dict[str, Any], spec: ModelSpec) -> AIResponse:
  """Chat Completions response → an `AIResponse` carrying Converse blocks."""
  choices = payload.get("choices") or []
  if not choices:
    raise AIProviderError(
      f"Self-hosted endpoint returned no choices for {spec.model_id}"
    )
  choice = choices[0]
  message = choice.get("message") or {}

  content = message.get("content") or ""
  if isinstance(content, list):
    content = "".join(
      part.get("text", "") for part in content if isinstance(part, dict)
    )
  text = _THINK_BLOCK.sub("", content).strip()

  blocks: list[dict[str, Any]] = []
  if text:
    blocks.append({"text": text})
  for call in message.get("tool_calls") or []:
    function = call.get("function") or {}
    blocks.append(
      {
        "toolUse": {
          # Some servers omit ids; the replayed tool message needs one.
          "toolUseId": call.get("id") or f"call_{uuid.uuid4().hex[:12]}",
          "name": function.get("name", ""),
          "input": _parse_arguments(function.get("arguments")),
        }
      }
    )

  has_tool_calls = any("toolUse" in b for b in blocks)
  finish = choice.get("finish_reason")
  stop_reason = "tool_use" if has_tool_calls else _STOP_REASONS.get(finish, finish)

  usage = payload.get("usage") or {}
  prompt_tokens = int(usage.get("prompt_tokens") or 0)
  cached = int((usage.get("prompt_tokens_details") or {}).get("cached_tokens") or 0)
  return AIResponse(
    content=text,
    model=spec.model_id,
    # Same convention as Bedrock: input is the uncached remainder, and the
    # cached part is reported (and billed) separately.
    input_tokens=max(prompt_tokens - cached, 0),
    output_tokens=int(usage.get("completion_tokens") or 0),
    stop_reason=stop_reason,
    cache_read_input_tokens=cached,
    content_blocks=blocks,
  )


def _parse_arguments(raw: Any) -> dict[str, Any]:
  if isinstance(raw, dict):
    return raw
  if not raw:
    return {}
  try:
    parsed = json.loads(raw)
  except (TypeError, ValueError):
    return {UNPARSED_ARGUMENTS_KEY: str(raw)}
  return parsed if isinstance(parsed, dict) else {UNPARSED_ARGUMENTS_KEY: str(raw)}
