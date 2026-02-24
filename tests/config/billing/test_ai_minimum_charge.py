"""AI billing minimum charge tests."""

from decimal import Decimal

from robosystems.config.billing.ai import AIBillingConfig


class TestApplyMinimumCharge:
  """Tests for AIBillingConfig.apply_minimum_charge."""

  def test_zero_cost_stays_zero(self):
    assert AIBillingConfig.apply_minimum_charge(Decimal("0")) == Decimal("0")

  def test_negative_cost_stays_zero(self):
    assert AIBillingConfig.apply_minimum_charge(Decimal("-5")) == Decimal("0")

  def test_below_minimum_rounded_up(self):
    result = AIBillingConfig.apply_minimum_charge(Decimal("0.5"))
    assert result == AIBillingConfig.MINIMUM_CHARGE

  def test_above_minimum_unchanged(self):
    result = AIBillingConfig.apply_minimum_charge(Decimal("10"))
    assert result == Decimal("10")

  def test_exactly_minimum(self):
    result = AIBillingConfig.apply_minimum_charge(AIBillingConfig.MINIMUM_CHARGE)
    assert result == AIBillingConfig.MINIMUM_CHARGE
