from decimal import Decimal

import pytest

from robosystems.config.billing.ai import AIBillingConfig


def test_token_pricing_contains_expected_models():
  """Test token pricing values are correctly configured.

  Credit anchor: 1 credit = 1 GB/day storage = ~$0.00333
  Sonnet (3.33x markup): 3 credits/1K input, 15 credits/1K output
  Opus (3.33x markup): 15 credits/1K input, 75 credits/1K output
  Typical Sonnet call (~5K input, ~1.5K output): ~38 credits
  Typical Opus call (~5K input, ~1.5K output): ~188 credits
  """
  pricing = AIBillingConfig.TOKEN_PRICING

  # Sonnet models: 3/15 credits per 1K tokens
  assert pricing["anthropic_claude_4_sonnet"]["input"] == Decimal("3")
  assert pricing["anthropic_claude_4_sonnet"]["output"] == Decimal("15")
  assert pricing["anthropic_claude_3_sonnet"]["input"] == Decimal("3")
  assert pricing["anthropic_claude_3_sonnet"]["output"] == Decimal("15")

  # Opus models: 15/75 credits per 1K tokens (5x Sonnet)
  assert pricing["anthropic_claude_4_opus"]["input"] == Decimal("15")
  assert pricing["anthropic_claude_4_opus"]["output"] == Decimal("75")
  assert pricing["anthropic_claude_4.1_opus"]["input"] == Decimal("15")
  assert pricing["anthropic_claude_4.1_opus"]["output"] == Decimal("75")
  assert pricing["anthropic_claude_3_opus"]["input"] == Decimal("15")
  assert pricing["anthropic_claude_3_opus"]["output"] == Decimal("75")

  # OpenAI models
  assert pricing["openai_gpt4"]["input"] == Decimal("30")
  assert pricing["openai_gpt4"]["output"] == Decimal("60")
  assert pricing["openai_gpt35"]["input"] == Decimal("0.5")
  assert pricing["openai_gpt35"]["output"] == Decimal("1.5")


@pytest.mark.parametrize(
  "model,expected_keys",
  [
    ("anthropic_claude_4_opus", {"input", "output"}),
    ("anthropic_claude_4.1_opus", {"input", "output"}),
    ("anthropic_claude_4_sonnet", {"input", "output"}),
    ("anthropic_claude_3_opus", {"input", "output"}),
    ("anthropic_claude_3_sonnet", {"input", "output"}),
    ("openai_gpt4", {"input", "output"}),
    ("openai_gpt35", {"input", "output"}),
  ],
)
def test_token_pricing_models_have_input_output(model, expected_keys):
  assert set(AIBillingConfig.TOKEN_PRICING[model].keys()) == expected_keys


def test_unknown_model_returns_keyerror():
  with pytest.raises(KeyError):
    _ = AIBillingConfig.TOKEN_PRICING["nonexistent_model"]
