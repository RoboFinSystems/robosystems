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
from robosystems.operations.agents.ai_client import AIClient, AIMessage, AIResponse

if TYPE_CHECKING:
  from robosystems.operations.agents.credit_consumer import CreditConsumer


class TrackedAIClient:
  """AI client wrapper that automatically tracks tokens and consumes credits."""

  def __init__(
    self,
    ai_client: AIClient,
    graph_id: str,
    user_id: str,
    credit_consumer: CreditConsumer,
  ) -> None:
    self._ai = ai_client
    self._graph_id = graph_id
    self._user_id = user_id
    self._credit_consumer = credit_consumer

    # Accumulated totals across all calls in this context
    self.total_tokens: dict[str, int] = {"input": 0, "output": 0}
    self.total_credits: float = 0.0
    self.call_count: int = 0

  async def create_message(
    self,
    messages: list[AIMessage],
    system: str | None = None,
    max_tokens: int = 4000,
    temperature: float = 0.7,
    model: str | None = None,
    agent_type: str | None = None,
    operation_description: str = "Agent AI call",
  ) -> AIResponse:
    """Call AI and automatically consume credits.

    Args:
        messages: Conversation messages.
        system: System prompt.
        max_tokens: Maximum output tokens.
        temperature: Sampling temperature.
        model: Optional model override.
        agent_type: Optional agent type for model override lookup.
        operation_description: Description for credit audit trail.

    Returns:
        AIResponse with content and token counts.
    """
    response = await self._ai.create_message(
      messages=messages,
      system=system,
      max_tokens=max_tokens,
      temperature=temperature,
      model=model,
      agent_type=agent_type,
    )

    # Track tokens
    self.total_tokens["input"] += response.input_tokens
    self.total_tokens["output"] += response.output_tokens
    self.call_count += 1

    # Consume credits automatically
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
      logger.warning(
        f"Credit consumption failed for graph={self._graph_id} "
        f"tokens=({response.input_tokens}/{response.output_tokens}): {e}"
      )

    return response

  @property
  def credit_summary(self) -> dict[str, Any]:
    """Summary of accumulated credit usage."""
    return {
      "total_credits_consumed": self.total_credits,
      "total_tokens": self.total_tokens.copy(),
      "call_count": self.call_count,
    }
