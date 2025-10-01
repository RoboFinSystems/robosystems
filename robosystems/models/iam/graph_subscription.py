"""Graph subscription model for billing management."""

import secrets
from datetime import datetime, timezone
from enum import Enum

from sqlalchemy import Column, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import ENUM as SQLEnum

from ...database import Base


class SubscriptionStatus(str, Enum):
  """Subscription status enumeration."""

  ACTIVE = "active"
  PAST_DUE = "past_due"
  CANCELED = "canceled"
  UNPAID = "unpaid"


class GraphSubscription(Base):
  """Graph subscription model for billing management."""

  __tablename__ = "graph_subscriptions"

  id = Column(
    String, primary_key=True, default=lambda: f"gsub_{secrets.token_urlsafe(16)}"
  )

  user_id = Column(String, ForeignKey("users.id"), nullable=False)
  graph_id = Column(String, nullable=False)
  plan_name = Column(String, nullable=True)  # e.g., "standard", "enterprise", "premium"
  status = Column(
    SQLEnum(SubscriptionStatus), nullable=False, default=SubscriptionStatus.ACTIVE
  )

  # Billing information
  current_period_start = Column(DateTime(timezone=True), nullable=True)
  current_period_end = Column(DateTime(timezone=True), nullable=True)

  # Timestamps
  created_at = Column(
    DateTime(timezone=True), nullable=False, default=datetime.now(timezone.utc)
  )
  updated_at = Column(
    DateTime(timezone=True),
    nullable=False,
    default=datetime.now(timezone.utc),
    onupdate=datetime.now(timezone.utc),
  )

  # Relationships
  user = relationship("User", back_populates="graph_subscriptions")

  def __repr__(self):
    return f"<GraphSubscription(id={self.id}, user={self.user_id}, graph={self.graph_id}, status={self.status})>"

  @property
  def is_active(self) -> bool:
    """Check if subscription is active."""
    return self.status == SubscriptionStatus.ACTIVE
