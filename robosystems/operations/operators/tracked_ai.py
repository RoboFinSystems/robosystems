"""TrackedAIClient — AIClient plus automatic credit tracking.

Each `create_message()` invokes Bedrock, accumulates the reported token counts,
and consumes credits through the injected `CreditConsumer`. Operators never
call consume themselves, so no operator can forget to bill.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from robosystems.logger import logger
from robosystems.operations.operators.ai_client import AIClient, AIMessage, AIResponse

if TYPE_CHECKING:
  from robosystems.operations.operators.credit_consumer import CreditConsumer


class UnbilledAICallError(Exception):
  """Raised when a prior AI call in this run could not be billed.

  Stops a tool-use loop from continuing to spend after billing has broken,
  which is the difference between one unbilled call and an unbounded number.
  """

  def __init__(self, detail: str) -> None:
    super().__init__(
      f"Refusing further AI calls: a previous call could not be billed ({detail})"
    )


class TrackedAIClient:
  """AI client that tracks tokens and consumes credits per call.

  One instance per operator run: the accumulated totals and the unbilled-call
  latch are both per-run state.
  """

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

    # Accumulated totals across all calls in this context. "input" is the
    # uncached input only — cache reads/writes are tracked (and billed)
    # separately, mirroring how Bedrock reports and prices them.
    self.total_tokens: dict[str, int] = {
      "input": 0,
      "output": 0,
      "cache_read": 0,
      "cache_write": 0,
    }
    self.total_credits: float = 0.0
    self.call_count: int = 0
    self._unbilled_call: str | None = None

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
    tool_choice: dict[str, Any] | None = None,
    cache_conversation: bool = False,
  ) -> AIResponse:
    """Call the model and consume credits for it.

    `operation_description` is what lands in the credit audit trail. Every call
    in a tool-use loop flows through here, so tokens and credits accumulate
    across iterations without the loop doing anything.

    Raises `UnbilledAICallError` if an earlier call in this run went unbilled.
    """
    # A tool-use loop can make dozens of calls. If one of them could not be
    # billed, every later call in the same run would be unbilled too, so stop
    # here rather than after the spend. The already-returned answer from the
    # failed call is kept — Bedrock has been paid for it either way, and
    # discarding it would waste the money without recovering it.
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
      tool_choice=tool_choice,
      cache_conversation=cache_conversation,
    )

    self.total_tokens["input"] += response.input_tokens
    self.total_tokens["output"] += response.output_tokens
    self.total_tokens["cache_read"] += response.cache_read_input_tokens
    self.total_tokens["cache_write"] += response.cache_creation_input_tokens
    self.call_count += 1

    # No consumer means no billing at all — tests, and contexts with no
    # platform DB session to bill against.
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
        self._mark_unbilled(response, str(e))

    return response

  def _mark_unbilled(self, response: AIResponse, reason: str) -> None:
    """Latch that a completed AI call could not be billed.

    ERROR, not WARNING: this is real unrecovered spend, not a degraded read,
    and it must not blend into routine log noise.
    """
    detail = (
      f"graph={self._graph_id} user={self._user_id} "
      f"tokens=({response.input_tokens}/{response.output_tokens}"
      f"+cache {response.cache_read_input_tokens}r/"
      f"{response.cache_creation_input_tokens}w): {reason}"
    )
    self._unbilled_call = detail
    logger.error(f"AI call completed but could not be billed — {detail}")

  @property
  def credit_summary(self) -> dict[str, Any]:
    """Summary of accumulated credit usage."""
    return {
      "total_credits_consumed": self.total_credits,
      "total_tokens": self.total_tokens.copy(),
      "call_count": self.call_count,
    }
