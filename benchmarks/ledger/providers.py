"""Model providers behind one interface, so the loop is the same for every column.

Two routes: Anthropic's API for Claude, and any OpenAI-compatible endpoint for
open-weight models (OpenRouter with a pinned host, or a model server on a box).
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from typing import Any, Protocol

import anthropic
import httpx

from .mcp import ToolDef

MAX_TOKENS = 16000


@dataclass(frozen=True)
class ToolCall:
  """`error` is set when the model's call could not be read (arguments that
  are not JSON); the loop answers it with that error and does not run a tool."""

  id: str
  name: str
  args: dict[str, Any]
  error: str | None = None


@dataclass
class Usage:
  input_tokens: int = 0
  output_tokens: int = 0
  cache_read_tokens: int = 0
  cache_write_tokens: int = 0
  # What the provider says it charged for the turn, where it says.
  cost_usd: float | None = None

  def as_dict(self) -> dict[str, Any]:
    return dict(self.__dict__)


@dataclass
class Turn:
  text: str
  tool_calls: list[ToolCall]
  usage: Usage
  stop_reason: str
  # The host that served the turn, where a router reports it.
  served_by: str | None = None


class CannotAttempt(Exception):
  """The row does not fit this model (the tool surface exceeds its context)."""


class Provider(Protocol):
  name: str
  model: str

  def start(self, system: str, user_text: str, tools: list[ToolDef]) -> Any: ...

  def step(self, conversation: Any) -> Turn: ...

  def add_tool_results(
    self, conversation: Any, results: list[tuple[ToolCall, str, bool]]
  ) -> None: ...

  def serialize(self, conversation: Any) -> list[dict[str, Any]]: ...


@dataclass
class _Conversation:
  system: list[dict[str, Any]]
  tools: list[dict[str, Any]]
  messages: list[dict[str, Any]] = field(default_factory=list)


class AnthropicProvider:
  name = "anthropic"

  def __init__(self, model: str, effort: str | None = None) -> None:
    self.model = model
    self.effort = effort
    self._client = anthropic.Anthropic(max_retries=6, timeout=600)

  def start(self, system: str, user_text: str, tools: list[ToolDef]) -> _Conversation:
    # Render order is tools, system, messages. The breakpoint on the system block
    # caches the tool surface for the whole trial; the request-level one below
    # moves with the conversation.
    return _Conversation(
      system=[{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
      tools=[t.as_dict() for t in tools],
      messages=[{"role": "user", "content": user_text}],
    )

  def step(self, conversation: _Conversation) -> Turn:
    kwargs: dict[str, Any] = {
      "model": self.model,
      "max_tokens": MAX_TOKENS,
      "system": conversation.system,
      "tools": conversation.tools,
      "messages": conversation.messages,
      "cache_control": {"type": "ephemeral"},
    }
    if self.effort:
      kwargs["output_config"] = {"effort": self.effort}
    try:
      response = self._client.messages.create(**kwargs)
    except anthropic.BadRequestError as exc:
      if "prompt is too long" in str(exc).lower():
        raise CannotAttempt(f"{self.model}: {exc.message}") from exc
      raise
    # Thinking blocks go back unchanged, so the whole content list is appended.
    content = response.to_dict()["content"]
    conversation.messages.append({"role": "assistant", "content": content})
    usage = response.usage
    return Turn(
      text="".join(b.get("text", "") for b in content if b.get("type") == "text"),
      tool_calls=[
        ToolCall(b["id"], b["name"], dict(b.get("input") or {}))
        for b in content
        if b.get("type") == "tool_use"
      ],
      usage=Usage(
        input_tokens=usage.input_tokens,
        output_tokens=usage.output_tokens,
        cache_read_tokens=usage.cache_read_input_tokens or 0,
        cache_write_tokens=usage.cache_creation_input_tokens or 0,
      ),
      stop_reason=str(response.stop_reason),
    )

  def add_tool_results(
    self, conversation: _Conversation, results: list[tuple[ToolCall, str, bool]]
  ) -> None:
    conversation.messages.append(
      {
        "role": "user",
        "content": [
          {
            "type": "tool_result",
            "tool_use_id": call.id,
            "content": text,
            "is_error": is_error,
          }
          for call, text, is_error in results
        ],
      }
    )

  def serialize(self, conversation: _Conversation) -> list[dict[str, Any]]:
    return conversation.messages


OPENROUTER_URL = "https://openrouter.ai/api/v1"
_RETRY_STATUS = frozenset({408, 409, 429, 500, 502, 503, 504, 529})
_FINISH = {"stop": "end_turn", "tool_calls": "tool_use", "length": "max_tokens"}


@dataclass
class _ChatConversation:
  tools: list[dict[str, Any]]
  messages: list[dict[str, Any]]


class OpenAICompatProvider:
  """Chat completions with tools, for a router or a local model server.

  `provider_order` pins the hosts a router may use, in order, with no fallback
  to others: an unpinned route can change quantisation between two requests.
  """

  def __init__(
    self,
    model: str,
    base_url: str,
    api_key: str,
    *,
    name: str,
    provider_order: list[str] | None = None,
    effort: str | None = None,
    transport: httpx.BaseTransport | None = None,
  ) -> None:
    self.name = name
    self.model = model
    self.effort = effort
    self.provider_order = provider_order
    self._is_openrouter = "openrouter.ai" in base_url
    self._http = httpx.Client(
      base_url=base_url.rstrip("/"),
      timeout=600,
      headers={"Authorization": f"Bearer {api_key}"},
      transport=transport,
    )

  def start(
    self, system: str, user_text: str, tools: list[ToolDef]
  ) -> _ChatConversation:
    return _ChatConversation(
      tools=[
        {
          "type": "function",
          "function": {
            "name": t.name,
            "description": t.description,
            "parameters": t.input_schema,
          },
        }
        for t in tools
      ],
      messages=[
        {"role": "system", "content": system},
        {"role": "user", "content": user_text},
      ],
    )

  def _body(self, conversation: _ChatConversation) -> dict[str, Any]:
    body: dict[str, Any] = {
      "model": self.model,
      "messages": conversation.messages,
      "tools": conversation.tools,
      "max_tokens": MAX_TOKENS,
    }
    if self.effort:
      body["reasoning_effort"] = self.effort
    if self._is_openrouter:
      # The credits actually charged come back in each turn's usage.
      body["usage"] = {"include": True}
      if self.provider_order:
        body["provider"] = {"order": self.provider_order, "allow_fallbacks": False}
    return body

  def _post(self, body: dict[str, Any]) -> dict[str, Any]:
    for attempt in range(6):
      response = self._http.post("/chat/completions", json=body)
      if response.status_code in _RETRY_STATUS and attempt < 5:
        time.sleep(min(60, 2**attempt))
        continue
      if response.status_code == 400 and "context" in response.text.lower():
        raise CannotAttempt(f"{self.model}: {response.text[:300]}")
      response.raise_for_status()
      data = response.json()
      # A router can answer 200 with an error body from the host behind it.
      if "choices" not in data:
        if attempt < 5:
          time.sleep(min(60, 2**attempt))
          continue
        raise RuntimeError(f"{self.model}: no choices in {json.dumps(data)[:300]}")
      return data
    raise AssertionError("unreachable")

  def step(self, conversation: _ChatConversation) -> Turn:
    data = self._post(self._body(conversation))
    choice = data["choices"][0]
    message = choice["message"]
    # The assistant turn goes back as it came, reasoning included: a reasoning
    # model's next step depends on it.
    kept = {
      key: message[key]
      for key in ("role", "content", "tool_calls", "reasoning", "reasoning_details")
      if message.get(key) is not None
    }
    kept.setdefault("role", "assistant")
    kept.setdefault("content", "")
    conversation.messages.append(kept)

    calls: list[ToolCall] = []
    for raw in message.get("tool_calls") or []:
      function = raw.get("function") or {}
      arguments = function.get("arguments") or "{}"
      try:
        parsed = json.loads(arguments) if isinstance(arguments, str) else arguments
        if not isinstance(parsed, dict):
          raise ValueError("arguments are not an object")
        calls.append(ToolCall(raw.get("id", ""), function.get("name", ""), parsed))
      except ValueError as exc:
        calls.append(
          ToolCall(
            raw.get("id", ""),
            function.get("name", ""),
            {},
            error=f"tool arguments were not valid JSON ({exc}): {arguments[:200]}",
          )
        )

    usage = data.get("usage") or {}
    cached = (usage.get("prompt_tokens_details") or {}).get("cached_tokens") or 0
    prompt = usage.get("prompt_tokens") or 0
    finish = choice.get("finish_reason") or ""
    return Turn(
      text=message.get("content") or "",
      tool_calls=calls,
      usage=Usage(
        input_tokens=prompt - cached,
        output_tokens=usage.get("completion_tokens") or 0,
        cache_read_tokens=cached,
        cost_usd=usage.get("cost"),
      ),
      stop_reason=_FINISH.get(finish, finish),
      served_by=data.get("provider"),
    )

  def add_tool_results(
    self, conversation: _ChatConversation, results: list[tuple[ToolCall, str, bool]]
  ) -> None:
    for call, text, _is_error in results:
      conversation.messages.append(
        {"role": "tool", "tool_call_id": call.id, "content": text}
      )

  def serialize(self, conversation: _ChatConversation) -> list[dict[str, Any]]:
    return conversation.messages


def make_provider(
  model: str,
  effort: str | None,
  *,
  base_url: str | None = None,
  provider_order: list[str] | None = None,
) -> Provider:
  """Claude models go to Anthropic. Anything else goes to `base_url` (a model
  server, keyed by `LOCAL_LLM_API_KEY` if it wants one) or to OpenRouter."""
  if model.startswith("claude-") and base_url is None:
    return AnthropicProvider(model, effort)
  if base_url is not None:
    return OpenAICompatProvider(
      model,
      base_url,
      os.environ.get("LOCAL_LLM_API_KEY", "none"),
      name="local",
      effort=effort,
    )
  key = os.environ.get("OPENROUTER_API_KEY")
  if not key:
    raise SystemExit("OPENROUTER_API_KEY is not set")
  return OpenAICompatProvider(
    model,
    OPENROUTER_URL,
    key,
    name="openrouter",
    provider_order=provider_order,
    effort=effort,
  )
