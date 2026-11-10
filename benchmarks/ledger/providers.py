"""Model providers behind one interface, so the loop is the same for every column.

Only the Anthropic route exists today. A second route (an OpenAI-compatible
endpoint with a base URL, for open-weight models) implements the same Protocol.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

import anthropic

from .mcp import ToolDef

MAX_TOKENS = 16000


@dataclass(frozen=True)
class ToolCall:
  id: str
  name: str
  args: dict[str, Any]


@dataclass
class Usage:
  input_tokens: int = 0
  output_tokens: int = 0
  cache_read_tokens: int = 0
  cache_write_tokens: int = 0

  def as_dict(self) -> dict[str, int]:
    return dict(self.__dict__)


@dataclass
class Turn:
  text: str
  tool_calls: list[ToolCall]
  usage: Usage
  stop_reason: str


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


def make_provider(model: str, effort: str | None) -> Provider:
  if model.startswith("claude-"):
    return AnthropicProvider(model, effort)
  raise SystemExit(f"no provider route for model {model!r} yet")
