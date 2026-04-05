"""
AI Billing Configuration - Token-based credit system for AI operations.

Credits are ONLY consumed for operations that incur external AI API costs.
All database, MCP, and infrastructure operations are included with the
subscription — credits are reserved exclusively for AI agent calls.

TOKEN PRICING:
==============
Credits per 1K tokens (Sonnet via AWS Bedrock):
  - Input: 3 credits per 1K tokens
  - Output: 15 credits per 1K tokens
  - Typical agent call (~5K input, ~1.5K output): ~38 credits
"""

from decimal import Decimal


class AIBillingConfig:
  """Configuration for AI-specific billing."""

  # Minimum credit charge per operation (rounds up to this minimum)
  MINIMUM_CHARGE = Decimal("1")

  # Token-based pricing: credits per 1K tokens
  # All agents use Sonnet via AWS Bedrock
  TOKEN_PRICING = {
    "anthropic_claude_4_sonnet": {
      "input": Decimal("3"),  # 3 credits per 1K input tokens
      "output": Decimal("15"),  # 15 credits per 1K output tokens
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
