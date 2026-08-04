"""TrackedAIClient — wraps AIClient with automatic credit tracking.

Every create_message() call automatically:
1. Invokes the underlying AIClient (Bedrock)
2. Accumulates token counts
3. Consumes credits via the injected CreditConsumer

Agents never call consume_credits() themselves — it's built in.
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
  """AI client wrapper that automatically tracks tokens and consumes credits."""

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

    # Accumulated totals across all calls in this context
    self.total_tokens: dict[str, int] = {"input": 0, "output": 0}
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
  ) -> AIResponse:
    """Call AI and automatically consume credits.

    Args:
        messages: Conversation messages.
        system: System prompt.
        max_tokens: Maximum output tokens.
        temperature: Sampling temperature.
        model: Optional model override.
        operator_type: Optional operator type for model override lookup.
        operation_description: Description for credit audit trail.
        tools: Optional Anthropic tool definitions. Each call in a tool-use
            loop flows through here, so tokens and credits accumulate across
            iterations automatically.

    Returns:
        AIResponse with content and token counts.
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
    )

    # Track tokens
    self.total_tokens["input"] += response.input_tokens
    self.total_tokens["output"] += response.output_tokens
    self.call_count += 1

    # Consume credits automatically (skip if no consumer — e.g. tests)
    if self._credit_consumer is not None:
      try:
        credits = await self._credit_consumer.consume(
          graph_id=self._graph_id,
          user_id=self._user_id,
          input_tokens=response.input_tokens,
          output_tokens=response.output_tokens,
          model=response.model,
          operation_description=operation_description,
        )
        self.total_credits += credits
      except Exception as e:
        self._mark_unbilled(response, str(e))

    return response

  def _mark_unbilled(self, response: AIResponse, reason: str) -> None:
    """Record that a completed AI call could not be billed.

    Logged at ERROR because this is real unrecovered spend, not a degraded
    read — the previous WARNING made an unbounded billing failure look like
    routine noise.
    """
    detail = (
      f"graph={self._graph_id} user={self._user_id} "
      f"tokens=({response.input_tokens}/{response.output_tokens}): {reason}"
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
