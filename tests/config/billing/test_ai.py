from decimal import Decimal

import pytest

from robosystems.config.billing.ai import AIBillingConfig


def test_token_pricing_contains_sonnet():
  """Token pricing has Sonnet rates configured correctly."""
  pricing = AIBillingConfig.TOKEN_PRICING

  assert pricing["anthropic_claude_4_sonnet"]["input"] == Decimal("3")
  assert pricing["anthropic_claude_4_sonnet"]["output"] == Decimal("15")


def test_token_pricing_only_has_active_models():
  """Only Sonnet is configured — Opus and OpenAI are not used."""
  assert set(AIBillingConfig.TOKEN_PRICING.keys()) == {"anthropic_claude_4_sonnet"}


def test_token_pricing_has_input_output_keys():
  for model, prices in AIBillingConfig.TOKEN_PRICING.items():
    assert set(prices.keys()) == {"input", "output"}, f"{model} missing keys"


def test_unknown_model_returns_keyerror():
  with pytest.raises(KeyError):
    _ = AIBillingConfig.TOKEN_PRICING["nonexistent_model"]


def test_minimum_charge():
  assert AIBillingConfig.apply_minimum_charge(Decimal("0")) == Decimal("0")
  assert AIBillingConfig.apply_minimum_charge(Decimal("0.5")) == Decimal("1")
  assert AIBillingConfig.apply_minimum_charge(Decimal("5")) == Decimal("5")
