"""
AI Billing Configuration - Token-based credit system for AI operations.

Credits are ONLY consumed for operations that incur external AI API costs.
All database, MCP, and infrastructure operations are included with the
subscription — credits are reserved exclusively for AI agent calls.

TOKEN PRICING:
==============
Credits per 1K tokens, indexed on what Bedrock actually bills us: the
``us.*`` regional inference profiles carry a 10% premium over the vendor's
list price (Sonnet 4.x: $3.30/$16.50 per MTok, not $3.00/$15.00; GPT-5.6
Luna: $0.22/$1.32, not $0.20/$1.20). 1 credit ~ $0.001, so the rates below
are an exact cost passthrough. Rates were read from the Bedrock agreement
rate cards (Claude) and the model card (GPT-5.6) on 2026-09-15.

Cache rates mirror Bedrock's own multipliers (read 0.1x, 5-minute write
1.25x the input rate; GPT-5.6's 30-minute write is also 1.25x) — the
discount is passed through to the customer rather than kept as margin.

Rates are per (provider, model family); an entry carries all four
dimensions. Which model bills under which key is the model registry's
business (``config/operators.py`` ``MODEL_REGISTRY``, field ``pricing_key``);
an unregistered model raises at billing time rather than underbilling.
Never add a silent default entry here (see
specs/ai-operators/llm-provider-abstraction).
"""

from decimal import Decimal


class AIBillingConfig:
  """Configuration for AI-specific billing."""

  # Minimum credit charge per operation (rounds up to this minimum)
  MINIMUM_CHARGE = Decimal("1")

  # Token-based pricing: credits per 1K tokens, us.* regional profiles.
  TOKEN_PRICING = {
    "anthropic_claude_4_sonnet": {
      "input": Decimal("3.3"),
      "output": Decimal("16.5"),
      "cache_read": Decimal("0.33"),  # 0.1x input
      "cache_write": Decimal("4.125"),  # 1.25x input (5-minute TTL)
    },
    "anthropic_claude_5_sonnet": {
      "input": Decimal("2.2"),
      "output": Decimal("11"),
      "cache_read": Decimal("0.22"),
      "cache_write": Decimal("2.75"),
    },
    "anthropic_claude_5_opus": {
      "input": Decimal("5.5"),
      "output": Decimal("27.5"),
      "cache_read": Decimal("0.55"),
      "cache_write": Decimal("6.875"),
    },
    "openai_gpt_5_6_luna": {
      "input": Decimal("0.22"),
      "output": Decimal("1.32"),
      "cache_read": Decimal("0.022"),
      "cache_write": Decimal("0.275"),
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
