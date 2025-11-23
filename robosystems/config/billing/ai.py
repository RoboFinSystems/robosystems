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
from enum import Enum


# Note: This enum is kept for potential future use but is not currently referenced
# Consider removing if not needed for future AI operation types
class AIOperationType(Enum):
  """Types of AI operations that consume credits."""

  AGENT_SIMPLE = "agent_simple"  # Simple AI agent query
  AGENT_COMPLEX = "agent_complex"  # Complex multi-step AI analysis
  EMBEDDING = "embedding"  # Text embedding generation
  COMPLETION = "completion"  # Direct AI completion
  VISION = "vision"  # Image analysis with AI
  SUMMARIZATION = "summarization"  # Document summarization


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

  # Note: The following methods and attributes are currently unused but kept for potential future use:
  # - calculate_ai_cost(): Could be used for fixed-cost AI operations
  # - is_ai_operation(): Could be used to identify AI operations automatically
  # - get_monthly_allocation(): Could be used for tier-based allocations
  # - estimate_ai_usage(): Could be used for usage estimation tools
  # Consider removing these if not needed in the near future
