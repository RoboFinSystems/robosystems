"""Token-based credit pricing for AI operations (the only thing that costs
credits).

Rates are credits per 1K tokens at exact cost passthrough (1 credit ~ $0.001),
indexed on what Bedrock bills for the ``us.*`` regional profiles (10% over
list). Cache rates pass Bedrock's per-model rates through: write is 1.25x
input for every family, read is 0.1x except Opus 5.5 (0.05x). A model priced
by prompt length also has a LONG_CONTEXT_PRICING entry; `rates_for` picks the
card per call.

A model bills under its registry ``pricing_key`` (config/operators.py); an
unregistered model raises at billing time. Never add a silent default entry.
``openai_compat`` exists only when the self-hosted model is enabled.
"""

from decimal import Decimal, InvalidOperation

from robosystems.config.env import env


def self_hosted_rates(input_per_1k: str, output_per_1k: str) -> dict[str, Decimal]:
  """Rates for a deployment's self-hosted model, credits per 1K tokens.

  Cache reads and writes bill at the input rate: an OpenAI-compatible
  server's caching discount, if it has one, is not ours to assume.
  """
  try:
    input_rate = Decimal(input_per_1k)
    output_rate = Decimal(output_per_1k)
  except InvalidOperation:
    raise ValueError(
      f"Self-hosted model rates must be numbers, got {input_per_1k!r} / "
      f"{output_per_1k!r}"
    ) from None
  if not (input_rate.is_finite() and output_rate.is_finite()):
    raise ValueError("Self-hosted model rates must be finite")
  if input_rate < 0 or output_rate < 0:
    raise ValueError("Self-hosted model rates cannot be negative")
  return {
    "input": input_rate,
    "output": output_rate,
    "cache_read": input_rate,
    "cache_write": input_rate,
  }


class AIBillingConfig:
  """Configuration for AI-specific billing."""

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
    # AWS Pricing API, us-east-1 regional, read 2026-09-29. Cache read is
    # 0.05x input on this model, not the 0.1x the older rows carry.
    "anthropic_claude_5_5_opus": {
      "input": Decimal("4.4"),
      "output": Decimal("22"),
      "cache_read": Decimal("0.22"),
      "cache_write": Decimal("5.5"),
    },
    # AWS Pricing API, us-east-1 regional, read 2026-10-09.
    "anthropic_claude_5_5_haiku": {
      "input": Decimal("0.11"),
      "output": Decimal("0.55"),
      "cache_read": Decimal("0.011"),
      "cache_write": Decimal("0.1375"),
    },
  }

  # Rates for a call whose prompt (uncached + cache read + cache write)
  # exceeds the threshold. Same source and date as the base row.
  LONG_CONTEXT_PRICING: dict[str, tuple[int, dict[str, Decimal]]] = {
    "anthropic_claude_5_5_haiku": (
      100_000,
      {
        "input": Decimal("0.55"),
        "output": Decimal("2.75"),
        "cache_read": Decimal("0.055"),
        "cache_write": Decimal("0.6875"),
      },
    ),
  }
  if env.OPENAI_COMPAT_ENABLED:
    TOKEN_PRICING["openai_compat"] = self_hosted_rates(
      env.OPENAI_COMPAT_CREDITS_PER_1K_INPUT,
      env.OPENAI_COMPAT_CREDITS_PER_1K_OUTPUT,
    )

  @classmethod
  def rates_for(cls, pricing_key: str, prompt_tokens: int) -> dict[str, Decimal]:
    """The rate card for one call, by its total prompt size."""
    long_context = cls.LONG_CONTEXT_PRICING.get(pricing_key)
    if long_context is not None and prompt_tokens > long_context[0]:
      return long_context[1]
    return cls.TOKEN_PRICING[pricing_key]

  @classmethod
  def apply_minimum_charge(cls, cost: Decimal) -> Decimal:
    """Round a positive cost up to MINIMUM_CHARGE; non-positive is 0."""
    if cost <= 0:
      return Decimal("0")
    return max(cost, cls.MINIMUM_CHARGE)
