"""Credit pre-flight for operator execution.

Credits are consumed *after* a Bedrock call returns, so this check is the only
thing standing between an under-funded graph and real spend. It belongs in the
execution adapters rather than the orchestrator: two of the three API execution
strategies (SSE streaming and background queue) call the adapters directly and
never construct an orchestrator, so a check at that layer would cover one path
in three.

Fail-closed on purpose: an error resolving the balance denies the run. The
ordinary "no credit pool" and "no shared-repository access" cases already come
back as a well-formed negative answer from `CreditService.check_credit_balance`,
so reaching the exception path means the balance genuinely could not be
established — and allowing the run there would hand out free AI.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any

from robosystems.logger import logger

if TYPE_CHECKING:
  from robosystems.operations.operators.base import Operator, OperatorMode


class InsufficientOperatorCreditsError(Exception):
  """Raised before execution when a graph cannot fund an operator run.

  Carries the numbers so callers can render them — the orchestrator turns this
  back into a graceful `INSUFFICIENT_CREDITS` response for the sync path, while
  the streaming and queued paths surface it as an error to the client.
  """

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
  """Rough per-run token estimate, used only to size the pre-flight check.

  Actual billing comes from the token counts Bedrock reports, so an inaccurate
  estimate only shifts where the balance floor sits.
  """
  estimate = dict(_MODE_ESTIMATES.get(mode.value, {"input": 5000, "output": 1500}))

  if "financial" in operator.spec.name.lower():
    estimate["input"] = int(estimate["input"] * 1.5)
    estimate["output"] = int(estimate["output"] * 1.5)

  return estimate


def estimate_operator_credits(operator: Operator, mode: OperatorMode) -> Decimal:
  """Estimated credit cost of one operator run."""
  from robosystems.config.billing.ai import AIBillingConfig

  tokens = estimate_operator_tokens(operator, mode)
  pricing = AIBillingConfig.TOKEN_PRICING.get(
    "anthropic_claude_4_sonnet",
    {"input": Decimal("3"), "output": Decimal("15")},
  )

  input_cost = (Decimal(tokens["input"]) / 1000) * pricing["input"]
  output_cost = (Decimal(tokens["output"]) / 1000) * pricing["output"]
  return input_cost + output_cost


def check_operator_credits(
  operator: Operator,
  graph_id: str,
  user_id: str,
  session: Any,
  mode: OperatorMode,
) -> dict[str, Any]:
  """Resolve whether `graph_id` can fund one run of `operator`.

  Returns the `CreditService.check_credit_balance` result augmented with
  `estimated_credits`. On an unexpected failure the result denies rather than
  allows — see the module docstring.
  """
  from robosystems.operations.graph.credit_service import CreditService

  estimated_cost = estimate_operator_credits(operator, mode)

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
) -> None:
  """Deny an operator run the graph cannot fund.

  No-ops for operators whose spec sets `requires_credits=False`, and when no
  session is available to resolve a balance (tests and any context without the
  platform DB) — the adapter logs that case separately, since it also means
  consumption cannot be recorded.

  Raises:
      InsufficientOperatorCreditsError: if the graph cannot fund the run.
  """
  if not operator.spec.requires_credits or session is None:
    return

  result = check_operator_credits(operator, graph_id, user_id, session, mode)
  if result.get("has_sufficient_credits"):
    return

  raise InsufficientOperatorCreditsError(
    operator_name=operator.spec.name,
    estimated_credits=float(result.get("estimated_credits", 0)),
    available_credits=float(result.get("available_credits", 0)),
    reason=result.get("error"),
  )
