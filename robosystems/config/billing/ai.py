"""
AI Billing Configuration - Simplified credit system for AI operations only.

This module defines the billing model for AI operations that consume external
API resources (Anthropic Claude 4/4.1, OpenAI GPT, etc.).

CREDIT VALUE ANCHOR:
====================
1 credit = 1 GB/day of storage overage = ~$0.00333 (1/3 of a cent)

This anchors credits to a tangible resource (storage) with a small margin:
- EBS cost: $0.08/GB/month
- Our price: $0.10/GB/month (30 credits x $0.00333)
- Storage margin: ~25%

AI operations are priced relative to this anchor with a 3.33x markup:
- AI margin: ~70%

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
  MINIMUM_CHARGE = Decimal("1")

  # Token-based pricing (for dynamic cost calculation)
  # 3.33x markup over API costs for SaaS margins
  #
  # Credit value anchor: 1 credit = $0.00333 (1 GB/day storage = 1 credit)
  #
  # ANTHROPIC PRICING (with 3.33x markup):
  # - Sonnet: $3/M input, $15/M output → $10/M, $50/M → 3/15 credits per 1K
  # - Opus: $15/M input, $75/M output → $50/M, $250/M → 15/75 credits per 1K
  #
  # OPENAI PRICING (with 3.33x markup):
  # - GPT-4: $30/M input, $60/M output → $100/M, $200/M → 30/60 credits per 1K
  # - GPT-3.5: $0.50/M input, $1.50/M output → $1.67/M, $5/M → 0.5/1.5 credits per 1K
  #
  # Typical agent call (~5K input, ~1.5K output):
  # - Sonnet: ~38 credits
  # - Opus: ~188 credits
  TOKEN_PRICING = {
    # Claude 4/4.1 Opus (premium tier)
    "anthropic_claude_4_opus": {
      "input": Decimal("15"),  # Credits per 1K input tokens
      "output": Decimal("75"),  # Credits per 1K output tokens
    },
    "anthropic_claude_4.1_opus": {
      "input": Decimal("15"),  # Credits per 1K input tokens
      "output": Decimal("75"),  # Credits per 1K output tokens
    },
    # Claude 4/4.1 Sonnet (standard tier)
    "anthropic_claude_4_sonnet": {
      "input": Decimal("3"),  # Credits per 1K input tokens
      "output": Decimal("15"),  # Credits per 1K output tokens
    },
    # Claude 3 Opus (legacy premium)
    "anthropic_claude_3_opus": {
      "input": Decimal("15"),  # Credits per 1K input tokens
      "output": Decimal("75"),  # Credits per 1K output tokens
    },
    # Claude 3/3.5 Sonnet (legacy standard)
    "anthropic_claude_3_sonnet": {
      "input": Decimal("3"),  # Credits per 1K input tokens
      "output": Decimal("15"),  # Credits per 1K output tokens
    },
    # OpenAI GPT-4
    "openai_gpt4": {
      "input": Decimal("30"),  # Credits per 1K input tokens
      "output": Decimal("60"),  # Credits per 1K output tokens
    },
    # OpenAI GPT-3.5 Turbo
    "openai_gpt35": {
      "input": Decimal("0.5"),  # Credits per 1K input tokens
      "output": Decimal("1.5"),  # Credits per 1K output tokens
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
