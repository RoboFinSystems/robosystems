"""
Unified billing configuration module.

This module consolidates all billing, credit, and subscription configuration
to eliminate duplication and ensure consistency across the platform.

All tier configuration (pricing, credits, storage) is defined in core.py
as the single source of truth via DEFAULT_GRAPH_BILLING_PLANS.
"""

from .ai import AIBillingConfig
from .core import (
  DEFAULT_GRAPH_BILLING_PLANS,
  STORAGE_INCLUDED,
  TIER_CREDIT_ALLOCATIONS,
  BillingConfig,
  StorageBillingConfig,
  get_included_storage,
  get_tier_credit_allocation,
)
from .repositories import RepositoryBillingConfig, SharedRepository

__all__ = [
  # Core billing (single source of truth)
  "DEFAULT_GRAPH_BILLING_PLANS",
  "TIER_CREDIT_ALLOCATIONS",
  "STORAGE_INCLUDED",
  "BillingConfig",
  "StorageBillingConfig",
  "get_tier_credit_allocation",
  "get_included_storage",
  # AI billing
  "AIBillingConfig",
  # Repository billing
  "RepositoryBillingConfig",
  "SharedRepository",
]
