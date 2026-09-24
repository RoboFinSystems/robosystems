"""Credit pre-flight for operator execution.

Credits are debited only after each model call returns, so this check is what
keeps an under-funded graph from spending. Fail-closed: an error resolving the
balance denies the run.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any

from robosystems.logger import logger

if TYPE_CHECKING:
  from robosystems.operations.operators.base import Operator, OperatorMode


class InsufficientOperatorCreditsError(Exception):
  """Raised before execution when a graph cannot fund an operator run."""

  def __init__(
    self,
    operator_name: str,
    estimated_credits: float,
    available_credits: float,
    reason: str | None = None,
  ) -> None:
    self.operator_name = operator_name
    self.estimated_credits = estimated_credits
    self.available_credits = available_credits
    self.reason = reason
    super().__init__(
      f"Insufficient credits for {operator_name}. "
      f"Required: {estimated_credits:.0f}, Available: {available_credits:.0f}"
      + (f" ({reason})" if reason else "")
    )


_MODE_ESTIMATES: dict[str, dict[str, int]] = {
  "quick": {"input": 2000, "output": 500},
  "standard": {"input": 5000, "output": 1500},
  "extended": {"input": 15000, "output": 3000},
  "streaming": {"input": 8000, "output": 2000},
}


def estimate_operator_tokens(operator: Operator, mode: OperatorMode) -> dict[str, int]:
  """Rough estimate that only sizes the pre-flight; billing uses real counts."""
  estimate = dict(_MODE_ESTIMATES.get(mode.value, {"input": 5000, "output": 1500}))

  if "financial" in operator.spec.name.lower():
    estimate["input"] = int(estimate["input"] * 1.5)
    estimate["output"] = int(estimate["output"] * 1.5)

  return estimate


_FALLBACK_PRICING = {"input": Decimal("3.3"), "output": Decimal("16.5")}


def estimate_operator_credits(
  operator: Operator, mode: OperatorMode, operator_type: str | None = None
) -> Decimal:
  """Priced at the model the run resolves to, as `AIClient.create_message` does."""
  from robosystems.config.billing.ai import AIBillingConfig
  from robosystems.config.operators import OperatorConfig

  tokens = estimate_operator_tokens(operator, mode)
  pricing_key = OperatorConfig.resolve_model(operator_type=operator_type).pricing_key
  pricing = AIBillingConfig.TOKEN_PRICING.get(pricing_key, _FALLBACK_PRICING)

  input_cost = (Decimal(tokens["input"]) / 1000) * pricing["input"]
  output_cost = (Decimal(tokens["output"]) / 1000) * pricing["output"]
  return input_cost + output_cost


def check_operator_credits(
  operator: Operator,
  graph_id: str,
  user_id: str,
  session: Any,
  mode: OperatorMode,
  operator_type: str | None = None,
) -> dict[str, Any]:
  """`CreditService.check_credit_balance` plus `estimated_credits`; denies on
  an unexpected failure."""
  from robosystems.operations.graph.credit_service import CreditService

  estimated_cost = estimate_operator_credits(operator, mode, operator_type)

  try:
    result = CreditService(session).check_credit_balance(
      graph_id=graph_id,
      required_credits=estimated_cost,
      user_id=user_id,
      operation_type="agent_call",
    )
  except Exception as e:
    logger.error(
      f"Credit pre-flight failed for graph={graph_id} operator={operator.spec.name}; "
      f"denying the run: {e}",
      exc_info=True,
    )
    return {
      "has_sufficient_credits": False,
      "available_credits": 0,
      "estimated_credits": float(estimated_cost),
      "error": f"Credit check failed: {e!s}",
    }

  result["estimated_credits"] = float(estimated_cost)
  return result


def enforce_operator_credits(
  operator: Operator,
  graph_id: str,
  user_id: str,
  session: Any,
  mode: OperatorMode,
  operator_type: str | None = None,
) -> None:
  """Raise `InsufficientOperatorCreditsError` if the graph cannot fund the run.

  No-op when `requires_credits=False` or without a session (tests).
  """
  if not operator.spec.requires_credits or session is None:
    return

  result = check_operator_credits(
    operator, graph_id, user_id, session, mode, operator_type
  )
  if result.get("has_sufficient_credits"):
    return

  raise InsufficientOperatorCreditsError(
    operator_name=operator.spec.name,
    estimated_credits=float(result.get("estimated_credits", 0)),
    available_credits=float(result.get("available_credits", 0)),
    reason=result.get("error"),
  )
