"""
Tests for Storage Billing Configuration.

Storage limits by tier (derived from DEFAULT_GRAPH_BILLING_PLANS in core.py).
Overage is billed via credits (see test_credits.py).
"""

from robosystems.config.billing.core import StorageBillingConfig


class TestStorageBillingConfig:
  """Test StorageBillingConfig class."""

  def test_storage_included_constants(self):
    """Test STORAGE_INCLUDED constants."""
    assert StorageBillingConfig.STORAGE_INCLUDED["ladybug-standard"] == 10
    assert StorageBillingConfig.STORAGE_INCLUDED["ladybug-large"] == 50
    assert StorageBillingConfig.STORAGE_INCLUDED["ladybug-xlarge"] == 100

  def test_get_included_storage_known_tiers(self):
    """Test get_included_storage for known tiers."""
    assert StorageBillingConfig.get_included_storage("ladybug-standard") == 10
    assert StorageBillingConfig.get_included_storage("ladybug-large") == 50
    assert StorageBillingConfig.get_included_storage("ladybug-xlarge") == 100

  def test_get_included_storage_unknown_tier(self):
    """Test get_included_storage defaults to 100 GB for unknown tiers."""
    assert StorageBillingConfig.get_included_storage("unknown-tier") == 100
    assert StorageBillingConfig.get_included_storage("") == 100

  def test_storage_tiers_increase_with_plan(self):
    """Test that higher tiers have more included storage."""
    standard = StorageBillingConfig.get_included_storage("ladybug-standard")
    large = StorageBillingConfig.get_included_storage("ladybug-large")
    xlarge = StorageBillingConfig.get_included_storage("ladybug-xlarge")

    assert standard < large < xlarge
