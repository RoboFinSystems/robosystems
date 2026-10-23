"""TrackedAIClient — AIClient that bills every call through the injected
`CreditConsumer`, so no operator can forget to, and keeps the run's
transcript: every model call's new input, output, usage and credits."""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any

from robosystems.logger import logger
from robosystems.operations.operators.ai_client import AIClient, AIMessage, AIResponse

if TYPE_CHECKING:
  from robosystems.operations.operators.credit_consumer import CreditConsumer

# A run's transcript is stored whole; past this many serialized characters
# the inputs of further calls are elided (usage and outputs stay), so one
# pathological run cannot write an unbounded row.
TRANSCRIPT_CHAR_BUDGET = 4_000_000
_ELIDED = "[elided: transcript budget exhausted]"


class UnbilledAICallError(Exception):
  """A prior AI call in this run could not be billed; stops further spend."""

  def __init__(self, detail: str) -> None:
    super().__init__(
      f"Refusing further AI calls: a previous call could not be billed ({detail})"
    )


class TrackedAIClient:
  """One instance per operator run: totals, the transcript and the unbilled
  latch are per run."""

  def __init__(
    self,
    ai_client: AIClient,
    graph_id: str,
    user_id: str,
    credit_consumer: CreditConsumer | None = None,
  ) -> None:
    self._ai = ai_client
    self._graph_id = graph_id
    self._user_id = user_id
    self._credit_consumer = credit_consumer

    # "input" is uncached input only; cache reads/writes are separate.
    self.total_tokens: dict[str, int] = {
      "input": 0,
      "output": 0,
      "cache_read": 0,
      "cache_write": 0,
    }
    self.total_credits: float = 0.0
    self.call_count: int = 0
    self._unbilled_call: str | None = None

    # One entry per model call, in order. A call that continues the previous
    # one (the tool loop) records only what was added since; a fresh
    # conversation (a mapping batch) records its whole input.
    self.transcript: list[dict[str, Any]] = []
    self._last_messages: list[AIMessage] = []
    self._last_output: list[dict[str, Any]] | None = None
    self._last_system_sha: str | None = None
    self._last_tool_names: list[str] | None = None
    self._transcript_chars = 0

  async def create_message(
    self,
    messages: list[AIMessage],
    system: str | None = None,
    max_tokens: int = 4000,
    temperature: float = 0.7,
    model: str | None = None,
    operator_type: str | None = None,
    operation_description: str = "Operator AI call",
    tools: list[dict[str, Any]] | None = None,
    cache_conversation: bool = False,
    effort: str | None = None,
  ) -> AIResponse:
    """Call the model and bill it; `operation_description` lands in the credit
    audit trail. Raises `UnbilledAICallError` if an earlier call went unbilled.
    The response of a call that fails to bill is still returned.
    """
    if self._unbilled_call is not None:
      raise UnbilledAICallError(self._unbilled_call)

    response = await self._ai.create_message(
      messages=messages,
      system=system,
      max_tokens=max_tokens,
      temperature=temperature,
      model=model,
      operator_type=operator_type,
      tools=tools,
      cache_conversation=cache_conversation,
      effort=effort,
    )

    self.total_tokens["input"] += response.input_tokens
    self.total_tokens["output"] += response.output_tokens
    self.total_tokens["cache_read"] += response.cache_read_input_tokens
    self.total_tokens["cache_write"] += response.cache_creation_input_tokens
    self.call_count += 1

    credits: float | None = None
    billed = True
    if self._credit_consumer is not None:
      try:
        credits = await self._credit_consumer.consume(
          graph_id=self._graph_id,
          user_id=self._user_id,
          input_tokens=response.input_tokens,
          output_tokens=response.output_tokens,
          model=response.model,
          operation_description=operation_description,
          cache_read_input_tokens=response.cache_read_input_tokens,
          cache_creation_input_tokens=response.cache_creation_input_tokens,
        )
        self.total_credits += credits
      except Exception as e:
        billed = False
        self._mark_unbilled(response, str(e))

    self._record_call(messages, system, tools, response, credits, billed)
    return response

  def _record_call(
    self,
    messages: list[AIMessage],
    system: str | None,
    tools: list[dict[str, Any]] | None,
    response: AIResponse,
    credits: float | None,
    billed: bool,
  ) -> None:
    """Append this call to the transcript. The tool loop replays the previous
    output verbatim as the assistant turn, so a continuing call is the
    previous messages + that turn + new input; only the new input is kept."""
    prior = len(self._last_messages)
    continues = (
      self._last_output is not None
      and len(messages) > prior
      and messages[:prior] == self._last_messages
      and messages[prior].role == "assistant"
      and messages[prior].content == self._last_output
    )
    new_input = messages[prior + 1 :] if continues else messages
    output: list[dict[str, Any]] = response.content_blocks or (
      [{"text": response.content}] if response.content else []
    )

    entry: dict[str, Any] = {
      "call": self.call_count,
      "model": response.model,
      "continues": continues,
      "input": [{"role": m.role, "content": m.content} for m in new_input],
      "output": output,
      "stop_reason": response.stop_reason,
      "usage": {
        "input": response.input_tokens,
        "output": response.output_tokens,
        "cache_read": response.cache_read_input_tokens,
        "cache_write": response.cache_creation_input_tokens,
      },
      "credits": credits,
      "billed": billed,
    }
    # The system prompt is our own code's output and large (the cached
    # prefix), so the record carries its fingerprint, noted when it changes.
    system_sha = hashlib.sha256(system.encode("utf-8")).hexdigest() if system else None
    if system_sha != self._last_system_sha or not self.transcript:
      entry["system_sha256"] = system_sha
      entry["system_chars"] = len(system or "")
      self._last_system_sha = system_sha
    tool_names = sorted(t.get("name", "") for t in tools) if tools else None
    if tool_names != self._last_tool_names or not self.transcript:
      entry["tools"] = tool_names
      self._last_tool_names = tool_names

    size = len(json.dumps(entry, default=str))
    if self._transcript_chars + size > TRANSCRIPT_CHAR_BUDGET:
      entry["input"] = _ELIDED
      size = len(json.dumps(entry, default=str))
    self._transcript_chars += size
    self.transcript.append(entry)

    self._last_messages = list(messages)
    self._last_output = response.content_blocks or None

  def _mark_unbilled(self, response: AIResponse, reason: str) -> None:
    """Logged at ERROR: this is real unrecovered spend."""
    detail = (
      f"graph={self._graph_id} user={self._user_id} "
      f"tokens=({response.input_tokens}/{response.output_tokens}"
      f"+cache {response.cache_read_input_tokens}r/"
      f"{response.cache_creation_input_tokens}w): {reason}"
    )
    self._unbilled_call = detail
    logger.error(f"AI call completed but could not be billed — {detail}")

  @property
  def last_call_unbilled(self) -> bool:
    """Whether a call has gone unbilled; its tool calls must not write."""
    return self._unbilled_call is not None

  @property
  def credit_summary(self) -> dict[str, Any]:
    return {
      "total_credits_consumed": self.total_credits,
      "total_tokens": self.total_tokens.copy(),
      "call_count": self.call_count,
    }
