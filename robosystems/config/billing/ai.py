"""
AI Billing Configuration - Token-based credit system for AI operations.

Credits are ONLY consumed for operations that incur external AI API costs.
All database, MCP, and infrastructure operations are included with the
subscription — credits are reserved exclusively for AI agent calls.

TOKEN PRICING:
==============
Credits per 1K tokens, indexed on what Bedrock actually bills us (the
``us.*`` regional inference profiles carry a 10% premium over Anthropic
list price — $3.30/$16.50 per MTok for Sonnet 4.x, not $3.00/$15.00).
1 credit ~ $0.001, so the rates below are an exact cost passthrough.

Cache rates mirror Bedrock's own multipliers (read 0.1x, 5-minute write
1.25x the input rate) — the discount is passed through to the customer
rather than kept as margin.

Rates are per (provider, model family); an entry carries all four
dimensions. Never add a silent default entry — an unknown model should
surface, not underbill (see specs/ai-operators/llm-provider-abstraction).
"""

from decimal import Decimal


class AIBillingConfig:
  """Configuration for AI-specific billing."""

  # Minimum credit charge per operation (rounds up to this minimum)
  MINIMUM_CHARGE = Decimal("1")

  # Token-based pricing: credits per 1K tokens
  # All operators use Claude via AWS Bedrock (us.* regional profiles)
  TOKEN_PRICING = {
    "anthropic_claude_4_sonnet": {
      "input": Decimal("3.3"),
      "output": Decimal("16.5"),
      "cache_read": Decimal("0.33"),  # 0.1x input
      "cache_write": Decimal("4.125"),  # 1.25x input (5-minute TTL)
    },
    # Prep for the operator model upgrade (specs/ai-operators/
    # operator-model-upgrade.md) — nothing sends this model yet.
    "anthropic_claude_5_sonnet": {
      "input": Decimal("2.2"),
      "output": Decimal("11"),
      "cache_read": Decimal("0.22"),
      "cache_write": Decimal("2.75"),
    },
  }

  @classmethod
  def apply_minimum_charge(cls, cost: Decimal) -> Decimal:
    """
    Apply minimum charge, rounding up to at least MINIMUM_CHARGE.

    Args:
        cost: Calculated cost in credits

    Returns:
        Cost rounded up to minimum charge
    """
    if cost <= 0:
      return Decimal("0")
    return max(cost, cls.MINIMUM_CHARGE)
