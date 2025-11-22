"""
Storage Billing Configuration.

This module defines billing for storage overages.
Each tier includes a certain amount of storage, with overages billed separately.
"""

from decimal import Decimal
from typing import Dict


class StorageBillingConfig:
  """Configuration for storage-based billing."""

  # Storage included by tier (in GB) - technical tier names
  STORAGE_INCLUDED = {
    "ladybug-standard": 100,  # 100 GB included
    "ladybug-large": 500,  # 500 GB included
    "ladybug-xlarge": 2000,  # 2 TB included
  }

  # Overage costs per GB per month (in dollars)
  OVERAGE_COSTS = {
    "ladybug-standard": Decimal("1.00"),  # $1.00/GB/month
    "ladybug-large": Decimal("0.50"),  # $0.50/GB/month
    "ladybug-xlarge": Decimal("0.25"),  # $0.25/GB/month
  }

  # Storage types and their multipliers
  STORAGE_TYPES = {
    "standard": Decimal("1.0"),  # Standard storage
    "cold": Decimal("0.2"),  # Cold/archived storage (80% cheaper)
    "hot": Decimal("2.0"),  # High-performance storage (2x cost)
  }

  @classmethod
  def calculate_storage_overage(
    cls,
    storage_gb: Decimal,
    tier: str,
    storage_type: str = "standard",
  ) -> Dict[str, Decimal]:
    """
    Calculate monthly storage overage charges.

    Args:
        storage_gb: Total storage used in GB
        tier: Subscription tier
        storage_type: Type of storage (standard/cold/hot)

    Returns:
        Dict with overage details and cost
    """
    # Get included storage
    included_gb = cls.STORAGE_INCLUDED.get(tier, 100)

    # Calculate overage
    if storage_gb <= included_gb:
      return {
        "included_gb": Decimal(included_gb),
        "used_gb": storage_gb,
        "overage_gb": Decimal("0"),
        "overage_cost": Decimal("0"),
      }

    overage_gb = storage_gb - included_gb
    base_cost_per_gb = cls.OVERAGE_COSTS.get(tier, Decimal("1.00"))
    type_multiplier = cls.STORAGE_TYPES.get(storage_type, Decimal("1.0"))

    overage_cost = overage_gb * base_cost_per_gb * type_multiplier

    return {
      "included_gb": Decimal(included_gb),
      "used_gb": storage_gb,
      "overage_gb": overage_gb,
      "overage_cost": overage_cost,
      "cost_per_gb": base_cost_per_gb * type_multiplier,
    }

  @classmethod
  def get_storage_limits(cls, tier: str) -> Dict[str, int]:
    """
    Get storage limits and recommendations for a tier.

    Args:
        tier: Subscription tier

    Returns:
        Dict with storage limits and recommendations
    """
    included = cls.STORAGE_INCLUDED.get(tier, 100)

    return {
      "included_gb": included,
      "soft_limit_gb": included * 2,  # Alert at 2x included
      "hard_limit_gb": included * 10,  # Hard limit at 10x included
      "recommended_archive_gb": int(included * 0.8),  # Archive when 80% full
    }
