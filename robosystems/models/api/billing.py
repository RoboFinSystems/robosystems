"""Billing API models.

This module contains Pydantic models for billing-related API operations including
subscription upgrades, payment processing, and billing management.
"""

from pydantic import BaseModel


class UpgradeSubscriptionRequest(BaseModel):
  """Request to upgrade a graph database subscription."""

  plan_name: str
  payment_method_id: str | None = None  # Optional in dev mode
