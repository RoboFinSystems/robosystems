from decimal import Decimal

import pytest

from robosystems.config.billing.ai import AIBillingConfig, AIOperationType


def test_token_pricing_contains_expected_models():
  pricing = AIBillingConfig.TOKEN_PRICING

  assert pricing["anthropic_claude_4_sonnet"]["input"] == Decimal("0.003")
  assert pricing["openai_gpt4"]["output"] == Decimal("0.06")


@pytest.mark.parametrize(
  "model,expected_keys",
  [
    ("anthropic_claude_4_opus", {"input", "output"}),
    ("openai_gpt35", {"input", "output"}),
  ],
)
def test_token_pricing_models_have_input_output(model, expected_keys):
  assert set(AIBillingConfig.TOKEN_PRICING[model].keys()) == expected_keys


def test_unknown_model_returns_keyerror():
  with pytest.raises(KeyError):
    _ = AIBillingConfig.TOKEN_PRICING["nonexistent_model"]


def test_ai_operation_type_enum_contains_expected_values():
  values = {operation.value for operation in AIOperationType}

  for expected in [
    "agent_simple",
    "agent_complex",
    "embedding",
    "completion",
    "vision",
    "summarization",
  ]:
    assert expected in values
