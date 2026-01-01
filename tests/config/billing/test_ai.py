from decimal import Decimal

import pytest

from robosystems.config.billing.ai import AIBillingConfig


def test_token_pricing_contains_expected_models():
  """Test token pricing values are correctly configured.

  Credit anchor: 1 credit = 1 GB/day storage = ~$0.00333
  With 3.33x AI markup: 3 credits/1K input, 15 credits/1K output
  Typical agent call (~5K input, ~1.5K output): ~38 credits
  """
  pricing = AIBillingConfig.TOKEN_PRICING

  # 3 credits per 1K input tokens ($0.01 / $0.00333 per credit)
  assert pricing["anthropic_claude_4_sonnet"]["input"] == Decimal("3")
  # 15 credits per 1K output tokens ($0.05 / $0.00333 per credit)
  assert pricing["anthropic_claude_4_sonnet"]["output"] == Decimal("15")
  assert pricing["anthropic_claude_3_sonnet"]["input"] == Decimal("3")
  assert pricing["anthropic_claude_3_sonnet"]["output"] == Decimal("15")


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
