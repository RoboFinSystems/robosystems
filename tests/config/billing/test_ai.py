from decimal import Decimal

import pytest

from robosystems.config.billing.ai import AIBillingConfig


def test_token_pricing_contains_expected_models():
  pricing = AIBillingConfig.TOKEN_PRICING

  assert pricing["anthropic_claude_4_sonnet"]["input"] == Decimal("0.01")
  assert pricing["anthropic_claude_4_sonnet"]["output"] == Decimal("0.05")
  assert pricing["anthropic_claude_3_sonnet"]["input"] == Decimal("0.01")
  assert pricing["anthropic_claude_3_sonnet"]["output"] == Decimal("0.05")


@pytest.mark.parametrize(
  "model,expected_keys",
  [
    ("anthropic_claude_4_sonnet", {"input", "output"}),
    ("anthropic_claude_3_sonnet", {"input", "output"}),
  ],
)
def test_token_pricing_models_have_input_output(model, expected_keys):
  assert set(AIBillingConfig.TOKEN_PRICING[model].keys()) == expected_keys


def test_unknown_model_returns_keyerror():
  with pytest.raises(KeyError):
    _ = AIBillingConfig.TOKEN_PRICING["nonexistent_model"]
