"""TrackedAIClient — AIClient that bills every call through the injected
`CreditConsumer`, so no operator can forget to."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from robosystems.logger import logger
from robosystems.operations.operators.ai_client import AIClient, AIMessage, AIResponse

if TYPE_CHECKING:
  from robosystems.operations.operators.credit_consumer import CreditConsumer


class UnbilledAICallError(Exception):
  """A prior AI call in this run could not be billed; stops further spend."""

  def __init__(self, detail: str) -> None:
    super().__init__(
      f"Refusing further AI calls: a previous call could not be billed ({detail})"
    )


class TrackedAIClient:
  """One instance per operator run: totals and the unbilled latch are per run."""

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
    )

    self.total_tokens["input"] += response.input_tokens
    self.total_tokens["output"] += response.output_tokens
    self.total_tokens["cache_read"] += response.cache_read_input_tokens
    self.total_tokens["cache_write"] += response.cache_creation_input_tokens
    self.call_count += 1

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
  def credit_summary(self) -> dict[str, Any]:
    return {
      "total_credits_consumed": self.total_credits,
      "total_tokens": self.total_tokens.copy(),
      "call_count": self.call_count,
    }
