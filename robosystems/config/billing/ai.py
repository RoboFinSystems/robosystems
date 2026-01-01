"""
AI Billing Configuration - Simplified credit system for AI operations only.

This module defines the billing model for AI operations that consume external
API resources (Anthropic Claude 4/4.1, OpenAI GPT, etc.).

BILLING PHILOSOPHY:
===================
Credits are ONLY consumed for operations that incur external API costs.
Operations using pre-provisioned LadybugDB instance resources don't consume credits.

When a user provisions a LadybugDB instance, they get:
- Dedicated or shared compute (CPU/Memory)
- Storage allocation
- Network bandwidth
- Database operations

Since these resources are already paid for through instance provisioning,
we don't charge credits for using them. Credits are reserved exclusively
for AI operations that call external APIs.
"""

from decimal import Decimal


class AIBillingConfig:
  """Configuration for AI-specific billing."""

  # Minimum credit charge per operation (rounds up to this minimum)
  MINIMUM_CHARGE = Decimal("0.01")

  # Token-based pricing (for dynamic cost calculation)
  # 3.33x markup over Anthropic API costs for SaaS margins
  # Anthropic base: $3/M input, $15/M output
  # RoboSystems: $10/M input, $50/M output
  TOKEN_PRICING = {
    "anthropic_claude_4_sonnet": {
      "input": Decimal(
        "0.01"
      ),  # Credits per 1K input tokens ($10/M tokens, 3.33x markup)
      "output": Decimal(
        "0.05"
      ),  # Credits per 1K output tokens ($50/M tokens, 3.33x markup)
    },
    "anthropic_claude_3_sonnet": {
      "input": Decimal(
        "0.01"
      ),  # Credits per 1K input tokens ($10/M tokens, 3.33x markup)
      "output": Decimal(
        "0.05"
      ),  # Credits per 1K output tokens ($50/M tokens, 3.33x markup)
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
