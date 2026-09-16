from decimal import Decimal

import pytest

from robosystems.config.billing.ai import AIBillingConfig


def test_token_pricing_matches_bedrock_us_profile_cost():
  """Rates are indexed on what Bedrock actually bills (us.* regional profiles
  carry a 10% premium over vendor list), read from the Bedrock agreement rate
  cards and model cards on 2026-09-15 — not on list price, which was a
  structural -10% margin."""
  pricing = AIBillingConfig.TOKEN_PRICING

  assert pricing["anthropic_claude_4_sonnet"]["input"] == Decimal("3.3")
  assert pricing["anthropic_claude_4_sonnet"]["output"] == Decimal("16.5")
  assert pricing["anthropic_claude_5_sonnet"]["input"] == Decimal("2.2")
  assert pricing["anthropic_claude_5_sonnet"]["output"] == Decimal("11")
  assert pricing["anthropic_claude_5_opus"]["input"] == Decimal("5.5")
  assert pricing["anthropic_claude_5_opus"]["output"] == Decimal("27.5")
  assert pricing["openai_gpt_5_6_luna"]["input"] == Decimal("0.22")
  assert pricing["openai_gpt_5_6_luna"]["output"] == Decimal("1.32")


def test_cache_rates_mirror_bedrock_multipliers():
  """The cache discount is passed through: reads at 0.1x the input rate,
  writes at 1.25x — Bedrock's own multipliers, for every registered family."""
  for model, prices in AIBillingConfig.TOKEN_PRICING.items():
    assert prices["cache_read"] == prices["input"] * Decimal("0.1"), model
    assert prices["cache_write"] == prices["input"] * Decimal("1.25"), model


def test_token_pricing_only_has_registered_families():
  """One key per (provider, model family) the registry can run — no silent
  default entry. An unknown model should surface, not underbill."""
  assert set(AIBillingConfig.TOKEN_PRICING.keys()) == {
    "anthropic_claude_4_sonnet",
    "anthropic_claude_5_sonnet",
    "anthropic_claude_5_opus",
    "openai_gpt_5_6_luna",
  }


def test_token_pricing_has_all_rate_dimensions():
  for model, prices in AIBillingConfig.TOKEN_PRICING.items():
    assert set(prices.keys()) == {
      "input",
      "output",
      "cache_read",
      "cache_write",
    }, f"{model} missing keys"


def test_unknown_model_returns_keyerror():
  with pytest.raises(KeyError):
    _ = AIBillingConfig.TOKEN_PRICING["nonexistent_model"]


def test_minimum_charge():
  assert AIBillingConfig.apply_minimum_charge(Decimal("0")) == Decimal("0")
  assert AIBillingConfig.apply_minimum_charge(Decimal("0.5")) == Decimal("1")
  assert AIBillingConfig.apply_minimum_charge(Decimal("5")) == Decimal("5")
