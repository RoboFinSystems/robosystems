"""
Tests for Storage Billing Configuration.

Comprehensive test coverage for storage billing calculations and limits.
"""

import pytest
from decimal import Decimal

from robosystems.config.billing.storage import StorageBillingConfig


class TestStorageBillingConfig:
  """Test StorageBillingConfig class."""

  def test_storage_included_constants(self):
    """Test STORAGE_INCLUDED constants."""
    assert StorageBillingConfig.STORAGE_INCLUDED["kuzu-standard"] == 100
    assert StorageBillingConfig.STORAGE_INCLUDED["kuzu-large"] == 500
    assert StorageBillingConfig.STORAGE_INCLUDED["kuzu-xlarge"] == 2000

  def test_overage_costs_constants(self):
    """Test OVERAGE_COSTS constants."""
    assert StorageBillingConfig.OVERAGE_COSTS["kuzu-standard"] == Decimal("1.00")
    assert StorageBillingConfig.OVERAGE_COSTS["kuzu-large"] == Decimal("0.50")
    assert StorageBillingConfig.OVERAGE_COSTS["kuzu-xlarge"] == Decimal("0.25")

  def test_storage_types_constants(self):
    """Test STORAGE_TYPES constants."""
    assert StorageBillingConfig.STORAGE_TYPES["standard"] == Decimal("1.0")
    assert StorageBillingConfig.STORAGE_TYPES["cold"] == Decimal("0.2")
    assert StorageBillingConfig.STORAGE_TYPES["hot"] == Decimal("2.0")


class TestCalculateStorageOverage:
  """Test calculate_storage_overage method."""

  def test_no_overage_standard_tier(self):
    """Test no overage for kuzu-standard tier within limits."""
    result = StorageBillingConfig.calculate_storage_overage(
      storage_gb=Decimal("50"), tier="kuzu-standard"
    )

    expected = {
      "included_gb": Decimal("100"),
      "used_gb": Decimal("50"),
      "overage_gb": Decimal("0"),
      "overage_cost": Decimal("0"),
    }

    assert result == expected

  def test_no_overage_exactly_at_limit(self):
    """Test no overage when exactly at included limit."""
    result = StorageBillingConfig.calculate_storage_overage(
      storage_gb=Decimal("100"), tier="kuzu-standard"
    )

    expected = {
      "included_gb": Decimal("100"),
      "used_gb": Decimal("100"),
      "overage_gb": Decimal("0"),
      "overage_cost": Decimal("0"),
    }

    assert result == expected

  def test_overage_standard_tier(self):
    """Test overage calculation for kuzu-standard tier."""
    result = StorageBillingConfig.calculate_storage_overage(
      storage_gb=Decimal("150"), tier="kuzu-standard"
    )

    expected = {
      "included_gb": Decimal("100"),
      "used_gb": Decimal("150"),
      "overage_gb": Decimal("50"),
      "overage_cost": Decimal("50.00"),  # 50 GB * $1.00/GB
      "cost_per_gb": Decimal("1.00"),
    }

    assert result == expected

  def test_overage_enterprise_tier(self):
    """Test overage calculation for kuzu-large tier."""
    result = StorageBillingConfig.calculate_storage_overage(
      storage_gb=Decimal("600"), tier="kuzu-large"
    )

    expected = {
      "included_gb": Decimal("500"),
      "used_gb": Decimal("600"),
      "overage_gb": Decimal("100"),
      "overage_cost": Decimal("50.00"),  # 100 GB * $0.50/GB
      "cost_per_gb": Decimal("0.50"),
    }

    assert result == expected

  def test_overage_premium_tier(self):
    """Test overage calculation for kuzu-xlarge tier."""
    result = StorageBillingConfig.calculate_storage_overage(
      storage_gb=Decimal("2100"), tier="kuzu-xlarge"
    )

    expected = {
      "included_gb": Decimal("2000"),
      "used_gb": Decimal("2100"),
      "overage_gb": Decimal("100"),
      "overage_cost": Decimal("25.00"),  # 100 GB * $0.25/GB
      "cost_per_gb": Decimal("0.25"),
    }

    assert result == expected

  def test_overage_with_cold_storage(self):
    """Test overage calculation with cold storage type."""
    result = StorageBillingConfig.calculate_storage_overage(
      storage_gb=Decimal("150"), tier="kuzu-standard", storage_type="cold"
    )

    expected = {
      "included_gb": Decimal("100"),
      "used_gb": Decimal("150"),
      "overage_gb": Decimal("50"),
      "overage_cost": Decimal("10.00"),  # 50 GB * $1.00/GB * 0.2 multiplier
      "cost_per_gb": Decimal("0.20"),  # $1.00 * 0.2
    }

    assert result == expected

  def test_overage_with_hot_storage(self):
    """Test overage calculation with hot storage type."""
    result = StorageBillingConfig.calculate_storage_overage(
      storage_gb=Decimal("150"), tier="kuzu-standard", storage_type="hot"
    )

    expected = {
      "included_gb": Decimal("100"),
      "used_gb": Decimal("150"),
      "overage_gb": Decimal("50"),
      "overage_cost": Decimal("100.00"),  # 50 GB * $1.00/GB * 2.0 multiplier
      "cost_per_gb": Decimal("2.00"),  # $1.00 * 2.0
    }

    assert result == expected

  def test_overage_unknown_tier(self):
    """Test overage calculation with unknown tier defaults to 100GB included."""
    result = StorageBillingConfig.calculate_storage_overage(
      storage_gb=Decimal("150"), tier="unknown_tier"
    )

    expected = {
      "included_gb": Decimal("100"),  # Default
      "used_gb": Decimal("150"),
      "overage_gb": Decimal("50"),
      "overage_cost": Decimal("50.00"),  # 50 GB * $1.00/GB (default cost)
      "cost_per_gb": Decimal("1.00"),  # Default cost
    }

    assert result == expected

  def test_overage_unknown_storage_type(self):
    """Test overage calculation with unknown storage type defaults to 1.0 multiplier."""
    result = StorageBillingConfig.calculate_storage_overage(
      storage_gb=Decimal("150"), tier="kuzu-standard", storage_type="unknown_type"
    )

    expected = {
      "included_gb": Decimal("100"),
      "used_gb": Decimal("150"),
      "overage_gb": Decimal("50"),
      "overage_cost": Decimal("50.00"),  # 50 GB * $1.00/GB * 1.0 (default multiplier)
      "cost_per_gb": Decimal("1.00"),  # $1.00 * 1.0
    }

    assert result == expected

  def test_large_overage_calculation(self):
    """Test overage calculation with very large storage amounts."""
    result = StorageBillingConfig.calculate_storage_overage(
      storage_gb=Decimal("10000"),  # 10 TB
      tier="kuzu-xlarge",
    )

    expected = {
      "included_gb": Decimal("2000"),
      "used_gb": Decimal("10000"),
      "overage_gb": Decimal("8000"),
      "overage_cost": Decimal("2000.00"),  # 8000 GB * $0.25/GB
      "cost_per_gb": Decimal("0.25"),
    }

    assert result == expected

  def test_fractional_storage_amounts(self):
    """Test overage calculation with fractional GB amounts."""
    result = StorageBillingConfig.calculate_storage_overage(
      storage_gb=Decimal("150.75"), tier="kuzu-standard"
    )

    expected = {
      "included_gb": Decimal("100"),
      "used_gb": Decimal("150.75"),
      "overage_gb": Decimal("50.75"),
      "overage_cost": Decimal("50.75"),  # 50.75 GB * $1.00/GB
      "cost_per_gb": Decimal("1.00"),
    }

    assert result == expected

  def test_zero_storage(self):
    """Test overage calculation with zero storage."""
    result = StorageBillingConfig.calculate_storage_overage(
      storage_gb=Decimal("0"), tier="kuzu-standard"
    )

    expected = {
      "included_gb": Decimal("100"),
      "used_gb": Decimal("0"),
      "overage_gb": Decimal("0"),
      "overage_cost": Decimal("0"),
    }

    assert result == expected


class TestGetStorageLimits:
  """Test get_storage_limits method."""

  def test_storage_limits_standard_tier(self):
    """Test storage limits for kuzu-standard tier."""
    result = StorageBillingConfig.get_storage_limits("kuzu-standard")

    expected = {
      "included_gb": 100,
      "soft_limit_gb": 200,  # 2x included
      "hard_limit_gb": 1000,  # 10x included
      "recommended_archive_gb": 80,  # 80% of included
    }

    assert result == expected

  def test_storage_limits_enterprise_tier(self):
    """Test storage limits for kuzu-large tier."""
    result = StorageBillingConfig.get_storage_limits("kuzu-large")

    expected = {
      "included_gb": 500,
      "soft_limit_gb": 1000,  # 2x included
      "hard_limit_gb": 5000,  # 10x included
      "recommended_archive_gb": 400,  # 80% of included
    }

    assert result == expected

  def test_storage_limits_premium_tier(self):
    """Test storage limits for kuzu-xlarge tier."""
    result = StorageBillingConfig.get_storage_limits("kuzu-xlarge")

    expected = {
      "included_gb": 2000,
      "soft_limit_gb": 4000,  # 2x included
      "hard_limit_gb": 20000,  # 10x included
      "recommended_archive_gb": 1600,  # 80% of included
    }

    assert result == expected

  def test_storage_limits_unknown_tier(self):
    """Test storage limits for unknown tier defaults to 100GB."""
    result = StorageBillingConfig.get_storage_limits("unknown_tier")

    expected = {
      "included_gb": 100,  # Default
      "soft_limit_gb": 200,  # 2x default
      "hard_limit_gb": 1000,  # 10x default
      "recommended_archive_gb": 80,  # 80% of default
    }

    assert result == expected

  def test_all_limits_are_integers(self):
    """Test that all storage limits are returned as integers."""
    for tier in ["kuzu-standard", "kuzu-large", "kuzu-xlarge"]:
      result = StorageBillingConfig.get_storage_limits(tier)

      for key, value in result.items():
        assert isinstance(value, int), f"{key} should be an integer, got {type(value)}"

  def test_limit_relationships(self):
    """Test relationships between different storage limits."""
    for tier in ["kuzu-standard", "kuzu-large", "kuzu-xlarge"]:
      result = StorageBillingConfig.get_storage_limits(tier)

      included = result["included_gb"]
      soft_limit = result["soft_limit_gb"]
      hard_limit = result["hard_limit_gb"]
      archive_threshold = result["recommended_archive_gb"]

      # Verify relationships
      assert soft_limit == included * 2
      assert hard_limit == included * 10
      assert archive_threshold == int(included * 0.8)

      # Verify ordering
      assert archive_threshold < included < soft_limit < hard_limit


class TestEdgeCases:
  """Test edge cases and boundary conditions."""

  def test_very_small_decimal_amounts(self):
    """Test with very small decimal amounts."""
    result = StorageBillingConfig.calculate_storage_overage(
      storage_gb=Decimal("100.001"), tier="kuzu-standard"
    )

    # Should have minimal overage
    assert result["overage_gb"] == Decimal("0.001")
    assert result["overage_cost"] == Decimal("0.001")

  def test_precision_preservation(self):
    """Test that decimal precision is preserved in calculations."""
    result = StorageBillingConfig.calculate_storage_overage(
      storage_gb=Decimal("123.456"), tier="kuzu-large", storage_type="cold"
    )

    # 123.456 - 500 = -376.544 (no overage, but test precision)
    assert result["used_gb"] == Decimal("123.456")
    assert result["overage_gb"] == Decimal("0")
    assert result["overage_cost"] == Decimal("0")

  def test_complex_calculation_precision(self):
    """Test precision in complex overage calculations."""
    result = StorageBillingConfig.calculate_storage_overage(
      storage_gb=Decimal("523.789"), tier="kuzu-large", storage_type="cold"
    )

    # Overage: 523.789 - 500 = 23.789 GB
    # Cost: 23.789 * 0.50 * 0.2 = 2.3789
    expected_overage = Decimal("23.789")
    expected_cost = Decimal("2.3789")

    assert result["overage_gb"] == expected_overage
    assert result["overage_cost"] == expected_cost
    assert result["cost_per_gb"] == Decimal("0.10")  # 0.50 * 0.2

  def test_string_tier_handling(self):
    """Test that string tier names are handled correctly."""
    # Test case sensitivity (should be case sensitive)
    result_lower = StorageBillingConfig.calculate_storage_overage(
      storage_gb=Decimal("150"), tier="kuzu-standard"
    )

    result_upper = StorageBillingConfig.calculate_storage_overage(
      storage_gb=Decimal("150"),
      tier="KUZU-STANDARD",  # Uppercase - should default to unknown tier
    )

    # Lower case should work normally
    assert result_lower["included_gb"] == Decimal("100")

    # Upper case should default to 100GB included (unknown tier behavior)
    assert result_upper["included_gb"] == Decimal("100")

  def test_none_values_handling(self):
    """Test handling of None or invalid values."""
    # These should raise appropriate errors rather than crash
    with pytest.raises((TypeError, AttributeError)):
      StorageBillingConfig.calculate_storage_overage(
        storage_gb=None, tier="kuzu-standard"
      )

  def test_negative_storage_amounts(self):
    """Test handling of negative storage amounts."""
    # This should behave as if storage is 0 or less than included
    result = StorageBillingConfig.calculate_storage_overage(
      storage_gb=Decimal("-10"), tier="kuzu-standard"
    )

    # Negative storage should result in no overage
    assert result["overage_gb"] == Decimal("0")
    assert result["overage_cost"] == Decimal("0")


class TestCostCalculationAccuracy:
  """Test cost calculation accuracy and edge cases."""

  def test_exact_dollar_amounts(self):
    """Test calculations that should result in exact dollar amounts."""
    result = StorageBillingConfig.calculate_storage_overage(
      storage_gb=Decimal("200"),  # 100 GB overage
      tier="kuzu-standard",
    )

    assert result["overage_cost"] == Decimal("100.00")

  def test_cent_precision(self):
    """Test that calculations maintain cent precision."""
    result = StorageBillingConfig.calculate_storage_overage(
      storage_gb=Decimal("501"),  # 1 GB overage above kuzu-large limit (500 GB)
      tier="kuzu-large",
      storage_type="standard",
    )

    # 1 GB * $0.50/GB = $0.50
    assert result["overage_cost"] == Decimal("0.50")

  def test_sub_cent_calculations(self):
    """Test calculations that result in sub-cent amounts."""
    result = StorageBillingConfig.calculate_storage_overage(
      storage_gb=Decimal("2000.1"),  # 0.1 GB overage above kuzu-xlarge limit (2000 GB)
      tier="kuzu-xlarge",
      storage_type="cold",
    )

    # 0.1 GB * $0.25/GB * 0.2 = $0.005
    assert result["overage_cost"] == Decimal("0.005")

  def test_large_cost_calculations(self):
    """Test calculations with large storage amounts."""
    result = StorageBillingConfig.calculate_storage_overage(
      storage_gb=Decimal("100000"),  # Very large amount
      tier="kuzu-standard",
    )

    # 99,900 GB overage * $1.00/GB = $99,900.00
    expected_cost = Decimal("99900.00")
    assert result["overage_cost"] == expected_cost
