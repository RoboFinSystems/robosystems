from decimal import Decimal

import pytest

from robosystems.config.billing.ai import AIBillingConfig, self_hosted_rates


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


# AWS Pricing API (AmazonBedrockFoundationModels, us-east-1 regional
# "Standard"), read 2026-09-29, in credits per 1K tokens ($ per MTok / 1000 x
# 1000 credits per $). Each family is pinned on its own: Bedrock's cache
# multipliers are per model, so a shared 0.1x rule overbilled Opus 5.5 reads.
BEDROCK_US_EAST_1 = {
  "anthropic_claude_4_sonnet": ("3.3", "16.5", "0.33", "4.125"),
  "anthropic_claude_5_sonnet": ("2.2", "11", "0.22", "2.75"),
  "anthropic_claude_5_opus": ("5.5", "27.5", "0.55", "6.875"),
  "anthropic_claude_5_5_opus": ("4.4", "22", "0.22", "5.5"),
  # Read 2026-10-09.
  "anthropic_claude_5_5_haiku": ("0.11", "0.55", "0.011", "0.1375"),
}


@pytest.mark.parametrize("family", sorted(BEDROCK_US_EAST_1))
def test_claude_rates_match_the_bedrock_price_list(family):
  input_, output, cache_read, cache_write = BEDROCK_US_EAST_1[family]
  prices = AIBillingConfig.TOKEN_PRICING[family]
  assert prices["input"] == Decimal(input_)
  assert prices["output"] == Decimal(output)
  assert prices["cache_read"] == Decimal(cache_read)
  assert prices["cache_write"] == Decimal(cache_write)


def test_token_pricing_only_has_registered_families():
  """One key per (provider, model family) the registry can run — no silent
  default entry. An unknown model should surface, not underbill."""
  assert set(AIBillingConfig.TOKEN_PRICING.keys()) == {
    "anthropic_claude_4_sonnet",
    "anthropic_claude_5_sonnet",
    "anthropic_claude_5_opus",
    "anthropic_claude_5_5_opus",
    "anthropic_claude_5_5_haiku",
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


class TestSelfHostedRates:
  """A deployment's self-hosted model bills at the rates it configures; the
  default 0 is the honest passthrough of a GPU with no per-token cost."""

  def test_default_zero(self):
    assert self_hosted_rates("0", "0") == {
      "input": Decimal("0"),
      "output": Decimal("0"),
      "cache_read": Decimal("0"),
      "cache_write": Decimal("0"),
    }

  def test_configured_rates_and_no_assumed_cache_discount(self):
    rates = self_hosted_rates("0.5", "2")
    assert rates["input"] == Decimal("0.5")
    assert rates["output"] == Decimal("2")
    assert rates["cache_read"] == rates["cache_write"] == Decimal("0.5")

  @pytest.mark.parametrize(("inp", "out"), [("-1", "0"), ("0", "-0.1")])
  def test_negative_rate_fails(self, inp, out):
    with pytest.raises(ValueError, match="negative"):
      self_hosted_rates(inp, out)

  @pytest.mark.parametrize(("inp", "out"), [("free", "0"), ("0", "NaN"), ("inf", "0")])
  def test_malformed_rate_fails(self, inp, out):
    with pytest.raises(ValueError):
      self_hosted_rates(inp, out)


def test_long_context_rates_apply_only_above_the_threshold():
  base = AIBillingConfig.TOKEN_PRICING["anthropic_claude_5_5_haiku"]
  threshold, long_rates = AIBillingConfig.LONG_CONTEXT_PRICING[
    "anthropic_claude_5_5_haiku"
  ]
  assert threshold == 100_000
  assert AIBillingConfig.rates_for("anthropic_claude_5_5_haiku", 100_000) is base
  assert AIBillingConfig.rates_for("anthropic_claude_5_5_haiku", 100_001) is long_rates
  assert long_rates["input"] == Decimal("0.55")
  assert long_rates["output"] == Decimal("2.75")
  assert long_rates["cache_read"] == Decimal("0.055")
  assert long_rates["cache_write"] == Decimal("0.6875")


def test_models_without_long_context_rates_keep_one_card():
  assert (
    AIBillingConfig.rates_for("anthropic_claude_5_sonnet", 900_000)
    is AIBillingConfig.TOKEN_PRICING["anthropic_claude_5_sonnet"]
  )


def test_long_context_rates_extend_registered_keys():
  assert set(AIBillingConfig.LONG_CONTEXT_PRICING) <= set(AIBillingConfig.TOKEN_PRICING)
