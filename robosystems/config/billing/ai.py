"""
AI Billing Configuration - Simplified credit system for AI operations only.

This module defines the billing model for AI operations that consume external
API resources (Anthropic Claude 4/4.1, OpenAI GPT, etc.).

BILLING PHILOSOPHY:
===================
Credits are ONLY consumed for operations that incur external API costs.
Operations using pre-provisioned Kuzu instance resources don't consume credits.

When a user provisions a Kuzu instance, they get:
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

  # Token-based pricing (for dynamic cost calculation)
  # Based on actual Anthropic API pricing for Claude 4/4.1 models
  TOKEN_PRICING = {
    "anthropic_claude_4_opus": {
      "input": Decimal("0.015"),  # Credits per 1K input tokens (~$15/M tokens)
      "output": Decimal("0.075"),  # Credits per 1K output tokens (~$75/M tokens)
    },
    "anthropic_claude_4.1_opus": {
      "input": Decimal("0.015"),  # Credits per 1K input tokens
      "output": Decimal("0.075"),  # Credits per 1K output tokens
    },
    "anthropic_claude_4_sonnet": {
      "input": Decimal("0.003"),  # Credits per 1K input tokens (~$3/M tokens)
      "output": Decimal("0.015"),  # Credits per 1K output tokens (~$15/M tokens)
    },
    # Legacy Claude 3 models (for backwards compatibility)
    "anthropic_claude_3_opus": {
      "input": Decimal("0.015"),  # Credits per 1K input tokens
      "output": Decimal("0.075"),  # Credits per 1K output tokens
    },
    "anthropic_claude_3_sonnet": {
      "input": Decimal("0.003"),  # Credits per 1K input tokens
      "output": Decimal("0.015"),  # Credits per 1K output tokens
    },
    "openai_gpt4": {
      "input": Decimal("0.03"),  # Credits per 1K input tokens
      "output": Decimal("0.06"),  # Credits per 1K output tokens
    },
    "openai_gpt35": {
      "input": Decimal("0.0015"),  # Credits per 1K input tokens
      "output": Decimal("0.002"),  # Credits per 1K output tokens
    },
  }

  # Note: The following methods and attributes are currently unused but kept for potential future use:
  # - calculate_ai_cost(): Could be used for fixed-cost AI operations
  # - is_ai_operation(): Could be used to identify AI operations automatically
  # - get_monthly_allocation(): Could be used for tier-based allocations
  # - estimate_ai_usage(): Could be used for usage estimation tools
  # Consider removing these if not needed in the near future
