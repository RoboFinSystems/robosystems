"""
User Repository Access Model

This model manages user access to repositories (formerly "shared repositories")
including subscriptions, permissions, and billing information.
"""

import secrets
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, Dict, Any, Sequence, cast
from enum import Enum

from sqlalchemy import (
  Column,
  String,
  Integer,
  DateTime,
  ForeignKey,
  Boolean,
  Text,
  Index,
  UniqueConstraint,
  Enum as SQLEnum,
)
from sqlalchemy.orm import relationship, Session
from sqlalchemy.exc import SQLAlchemyError

from ...database import Model
from ...config.graph_tier import GraphTier
import logging

logger = logging.getLogger(__name__)


# Type-safe helpers for SQLAlchemy model attributes
def safe_str(value: Any) -> str:
  """Safely convert SQLAlchemy model attributes to string."""
  return str(value) if value is not None else ""


def safe_bool(value: Any) -> bool:
  """Safely convert SQLAlchemy model attributes to boolean."""
  return bool(value) if value is not None else False


class RepositoryType(str, Enum):
  """Types of shared repositories."""

  SEC = "sec"  # SEC public entity filings
  INDUSTRY = "industry"  # Industry benchmarking data
  ECONOMIC = "economic"  # Economic indicators and metrics


class RepositoryAccessLevel(str, Enum):
  """Repository access levels."""

  NONE = "none"  # No access to repository
  READ = "read"  # Read-only access
  WRITE = "write"  # Read/write access (for data contributions)
  ADMIN = "admin"  # Full admin access including user management


class RepositoryPlan(str, Enum):
  """Repository access plans for shared data."""

  STARTER = "starter"  # Limited credits, basic access
  ADVANCED = "advanced"  # More credits, full access
  UNLIMITED = "unlimited"  # High credits, priority access


class UserRepository(Model):
  """
  Model for user repository access combining subscription management
  with permission-based access control.
  """

  __tablename__ = "user_repository"

  id = Column(
    String, primary_key=True, default=lambda: f"usra_{secrets.token_urlsafe(16)}"
  )

  # User reference
  user_id = Column(String, ForeignKey("users.id"), nullable=False)

  # Repository identification
  repository_type = Column(SQLEnum(RepositoryType), nullable=False)
  repository_name = Column(
    String, ForeignKey("graphs.graph_id", ondelete="RESTRICT"), nullable=False
  )

  # Access control (from UserRepositoryAccess)
  access_level = Column(
    SQLEnum(RepositoryAccessLevel), nullable=False, default=RepositoryAccessLevel.NONE
  )

  # Repository plan management
  repository_plan = Column(
    SQLEnum(RepositoryPlan), nullable=False, default=RepositoryPlan.STARTER
  )

  # Status and lifecycle
  is_active = Column(Boolean, nullable=False, default=True)
  activated_at = Column(
    DateTime(timezone=True), nullable=False, default=datetime.now(timezone.utc)
  )
  expires_at = Column(DateTime(timezone=True), nullable=True)  # None = no expiration

  # Billing management
  monthly_price_cents = Column(Integer, nullable=False, default=0)
  billing_cycle_day = Column(Integer, nullable=True)  # Day of month for billing
  last_billed_at = Column(DateTime(timezone=True), nullable=True)
  next_billing_at = Column(DateTime(timezone=True), nullable=True)

  # Credit allocation
  monthly_credit_allocation = Column(Integer, nullable=False, default=0)

  # Administrative tracking
  granted_by = Column(String, ForeignKey("users.id"), nullable=True)
  granted_at = Column(DateTime(timezone=True), nullable=True)

  # Configuration
  access_scope = Column(
    String, nullable=True
  )  # JSON string for repository-specific access rules
  quota_limits = Column(
    String, nullable=True
  )  # JSON string for usage quotas/rate limits
  extra_metadata = Column(Text, nullable=True)  # JSON metadata for extensibility

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
  user = relationship("User", foreign_keys=[user_id])
  granter = relationship("User", foreign_keys=[granted_by], post_update=True)
  user_credits = relationship(
    "UserRepositoryCredits", back_populates="user_repository", uselist=False
  )
  graph = relationship(
    "Graph",
    foreign_keys=[repository_name],
    primaryjoin="foreign(UserRepository.repository_name)==Graph.graph_id",
  )

  # Constraints and Indexes
  __table_args__ = (
    UniqueConstraint(
      "user_id", "repository_type", "repository_name", name="uq_user_repo_access"
    ),
    Index("idx_user_shared_repo_user_active", "user_id", "is_active"),
    Index(
      "idx_user_shared_repo_type_level", "repository_type", "access_level", "is_active"
    ),
    Index("idx_user_shared_repo_expires", "expires_at", "is_active"),
    Index("idx_user_shared_repo_billing", "next_billing_at", "is_active"),
    Index("idx_user_shared_repo_plan", "repository_plan", "is_active"),
  )

  def __repr__(self):
    return (
      f"<UserRepository(user={self.user_id}, "
      f"repo={self.repository_name}, level={self.access_level}, "
      f"plan={self.repository_plan})>"
    )

  @classmethod
  def create_access(
    cls,
    user_id: str,
    repository_type: RepositoryType,
    repository_name: str,
    access_level: RepositoryAccessLevel,
    repository_plan: RepositoryPlan,
    session: Session,
    granted_by: Optional[str] = None,
    monthly_price_cents: int = 0,
    monthly_credits: int = 0,
    expires_at: Optional[datetime] = None,
    metadata: Optional[Dict[str, Any]] = None,
  ) -> "UserRepository":
    """Create or update repository access for a user."""
    import json
    from datetime import timedelta

    # Check if access already exists
    existing = cls.get_by_user_and_repository(user_id, repository_name, session)

    now = datetime.now(timezone.utc)

    if existing:
      # Update existing access
      existing.repository_type = repository_type
      existing.access_level = access_level
      existing.repository_plan = repository_plan
      existing.granted_by = granted_by
      existing.granted_at = now
      existing.is_active = True
      existing.monthly_price_cents = monthly_price_cents
      existing.monthly_credit_allocation = monthly_credits
      existing.expires_at = expires_at
      existing.updated_at = now

      if metadata:
        existing.extra_metadata = json.dumps(metadata)

      access = existing
    else:
      # Create new access
      access = cls(
        user_id=user_id,
        repository_type=repository_type,
        repository_name=repository_name,
        access_level=access_level,
        repository_plan=repository_plan,
        granted_by=granted_by,
        granted_at=now,
        activated_at=now,
        is_active=True,
        monthly_price_cents=monthly_price_cents,
        monthly_credit_allocation=monthly_credits,
        extra_metadata=json.dumps(metadata) if metadata else None,
      )

      if expires_at:
        access.expires_at = expires_at

      # Set billing cycle
      if monthly_price_cents > 0:
        access.billing_cycle_day = now.day
        access.next_billing_at = now + timedelta(days=30)

      session.add(access)

    try:
      session.commit()
      session.refresh(access)

      # Create or update associated credit pool
      if monthly_credits > 0:
        from .user_repository_credits import UserRepositoryCredits

        if access.user_credits:
          # Update existing credit pool
          access.user_credits.update_monthly_allocation(
            new_allocation=Decimal(str(monthly_credits)), session=session
          )
        else:
          # Create new credit pool
          UserRepositoryCredits.create_for_access(
            access_id=cast(str, access.id),
            repository_type=repository_type,
            repository_plan=repository_plan,
            monthly_allocation=monthly_credits,
            session=session,
          )

    except SQLAlchemyError:
      session.rollback()
      raise

    return access

  @classmethod
  def get_by_user_and_repository(
    cls, user_id: str, repository_name: str, session: Session
  ) -> Optional["UserRepository"]:
    """Get access record for a user and repository."""
    return (
      session.query(cls)
      .filter(cls.user_id == user_id, cls.repository_name == repository_name)
      .first()
    )

  @classmethod
  def user_has_access(
    cls, user_id: str, repository_name: str, session: Session
  ) -> bool:
    """Check if a user has any access to a repository."""
    access = cls.get_by_user_and_repository(user_id, repository_name, session)
    if not access or not safe_bool(access.is_active):
      return False

    # Check if expired
    if access.expires_at and access.expires_at < datetime.now(timezone.utc):
      return False

    return access.access_level != RepositoryAccessLevel.NONE

  @classmethod
  def get_user_access_level(
    cls, user_id: str, repository_name: str, session: Session
  ) -> RepositoryAccessLevel:
    """Get the user's access level for a repository."""
    access = cls.get_by_user_and_repository(user_id, repository_name, session)
    if not access or not safe_bool(access.is_active):
      return RepositoryAccessLevel.NONE

    # Check if expired
    if access.expires_at and access.expires_at < datetime.now(timezone.utc):
      return RepositoryAccessLevel.NONE

    return cast(RepositoryAccessLevel, access.access_level)

  @classmethod
  def get_user_repositories(
    cls, user_id: str, session: Session, active_only: bool = True
  ) -> Sequence["UserRepository"]:
    """Get all repositories a user has access to."""
    query = session.query(cls).filter(cls.user_id == user_id)

    if active_only:
      query = query.filter(
        cls.is_active,
        cls.access_level != RepositoryAccessLevel.NONE,
      )

    return query.order_by(cls.repository_type, cls.repository_name).all()

  @classmethod
  def get_repository_users(
    cls, repository_name: str, session: Session
  ) -> Sequence["UserRepository"]:
    """Get all users with access to a repository."""
    return (
      session.query(cls)
      .filter(
        cls.repository_name == repository_name,
        cls.is_active,
        cls.access_level != RepositoryAccessLevel.NONE,
      )
      .order_by(cls.user_id)
      .all()
    )

  @classmethod
  def get_by_repository_type(
    cls, repository_type: RepositoryType, session: Session
  ) -> Sequence["UserRepository"]:
    """Get all access records for a repository type."""
    return (
      session.query(cls)
      .filter(
        cls.repository_type == repository_type,
        cls.is_active,
        cls.access_level != RepositoryAccessLevel.NONE,
      )
      .order_by(cls.user_id, cls.repository_name)
      .all()
    )

  def revoke_access(self, session: Session) -> None:
    """Revoke repository access for a user."""
    self.is_active = False
    self.expires_at = datetime.now(timezone.utc)
    self.updated_at = datetime.now(timezone.utc)

    # Also deactivate the credit pool
    if self.user_credits:
      self.user_credits.is_active = False

    try:
      session.commit()
    except SQLAlchemyError:
      session.rollback()
      raise

  def upgrade_tier(
    self,
    new_plan: RepositoryPlan,
    session: Session,
    new_price_cents: Optional[int] = None,
    new_credits: Optional[int] = None,
  ) -> None:
    """
    Upgrade or downgrade repository subscription plan.

    This updates the repository plan (STARTER, ADVANCED, UNLIMITED) and optionally
    adjusts pricing and credit allocations. When credits are updated, the method also
    synchronizes the UserRepositoryCredits record to reflect the new allocation.

    Use cases:
    - User upgrades from STARTER to ADVANCED for more features
    - User downgrades from UNLIMITED to ADVANCED to reduce costs
    - Price adjustments due to promotional pricing
    - Credit allocation changes without plan changes

    Args:
        new_plan: Target repository plan (STARTER, ADVANCED, or UNLIMITED)
        session: Database session for the transaction
        new_price_cents: Optional new monthly price in cents (overrides plan default)
        new_credits: Optional new monthly credit allocation (overrides plan default)

    Raises:
        SQLAlchemyError: If the database update fails
    """
    old_plan = self.repository_plan
    self.repository_plan = new_plan
    self.updated_at = datetime.now(timezone.utc)

    if new_price_cents is not None:
      self.monthly_price_cents = new_price_cents

    if new_credits is not None:
      self.monthly_credit_allocation = new_credits

      # Update credit allocation
      if self.user_credits:
        self.user_credits.update_monthly_allocation(
          new_allocation=Decimal(str(new_credits)), session=session
        )

    try:
      session.commit()
      logger.info(
        f"Upgraded access {self.id} from {old_plan} to {new_plan} "
        f"for user {self.user_id} repository {self.repository_name}"
      )
    except SQLAlchemyError:
      session.rollback()
      raise

  def is_expired(self) -> bool:
    """Check if the access has expired."""
    if self.expires_at is None:
      return False
    return self.expires_at < datetime.now(timezone.utc)

  def can_read(self) -> bool:
    """Check if user can read from repository."""
    if not safe_bool(self.is_active) or self.is_expired():
      return False
    return self.access_level in [
      RepositoryAccessLevel.READ,
      RepositoryAccessLevel.WRITE,
      RepositoryAccessLevel.ADMIN,
    ]

  def can_write(self) -> bool:
    """Check if user can write to repository."""
    if not safe_bool(self.is_active) or self.is_expired():
      return False
    return self.access_level in [
      RepositoryAccessLevel.WRITE,
      RepositoryAccessLevel.ADMIN,
    ]

  def can_admin(self) -> bool:
    """Check if user has admin access to repository."""
    if not safe_bool(self.is_active) or self.is_expired():
      return False
    return self.access_level == RepositoryAccessLevel.ADMIN  # type: ignore[return-value]

  def get_graph_connection_info(self) -> Dict[str, Any]:
    """
    Get graph database connection information for this repository.

    Pulls infrastructure metadata from the Graph table via relationship.
    """
    if self.graph:
      graph_tier = self.graph.graph_tier
      if isinstance(graph_tier, str):
        graph_tier = GraphTier(graph_tier)  # type: ignore[misc]
      return {
        "instance_id": self.graph.graph_instance_id,
        "cluster_region": self.graph.graph_cluster_region,
        "instance_tier": graph_tier,
        "repository_name": self.repository_name,
        "repository_type": self.repository_type.value,
      }

    return {
      "instance_id": "kuzu-shared-prod",
      "cluster_region": None,
      "instance_tier": GraphTier.KUZU_SHARED,
      "repository_name": self.repository_name,
      "repository_type": self.repository_type.value,
    }

  def get_repository_plan_config(self) -> Dict[str, Any]:
    """
    Get repository plan configuration for this repository type and plan.

    Returns hardcoded plan configurations including pricing, credit allocations,
    and access levels. These configurations define the feature set for each
    repository subscription tier (STARTER, ADVANCED, UNLIMITED).

    Note: These configurations are currently hardcoded in the model but should
    ideally be moved to a configuration file or database table for easier
    updates without code changes.

    Returns:
        Dict containing plan configuration:
        - name: Human-readable plan name
        - monthly_credits: Credit allocation per month
        - price_monthly: Monthly subscription price in dollars
        - access_level: RepositoryAccessLevel (READ, WRITE, or ADMIN)

        Empty dict if repository type or plan is not configured.
    """
    configs = {
      RepositoryType.SEC: {
        "enabled": True,
        "plans": {
          RepositoryPlan.STARTER: {
            "name": "SEC Data Starter",
            "monthly_credits": 5000,
            "price_monthly": 29.99,
            "access_level": RepositoryAccessLevel.READ,
          },
          RepositoryPlan.ADVANCED: {
            "name": "SEC Data Advanced",
            "monthly_credits": 25000,
            "price_monthly": 99.99,
            "access_level": RepositoryAccessLevel.WRITE,
          },
          RepositoryPlan.UNLIMITED: {
            "name": "SEC Data Unlimited",
            "monthly_credits": 100000,
            "price_monthly": 299.99,
            "access_level": RepositoryAccessLevel.ADMIN,
          },
        },
      },
      RepositoryType.INDUSTRY: {
        "enabled": False,
        "coming_soon": True,
        "plans": {
          RepositoryPlan.STARTER: {
            "name": "Industry Benchmarks Starter",
            "monthly_credits": 3000,
            "price_monthly": 19.99,
            "access_level": RepositoryAccessLevel.READ,
          },
        },
      },
      RepositoryType.ECONOMIC: {
        "enabled": False,
        "coming_soon": True,
        "plans": {
          RepositoryPlan.STARTER: {
            "name": "Economic Indicators Starter",
            "monthly_credits": 2000,
            "price_monthly": 14.99,
            "access_level": RepositoryAccessLevel.READ,
          },
        },
      },
    }

    repo_config = configs.get(cast(RepositoryType, self.repository_type), {})
    if "plans" in repo_config:
      return repo_config["plans"].get(cast(RepositoryPlan, self.repository_plan), {})
    return {}

  @classmethod
  def get_all_repository_configs(cls) -> Dict[str, Dict[str, Any]]:
    """Get all repository configurations including enabled status."""
    return {
      RepositoryType.SEC: {
        "enabled": True,
        "plans": {
          RepositoryPlan.STARTER: {
            "name": "SEC Data Starter",
            "monthly_credits": 5000,
            "price_monthly": 29.99,
            "access_level": RepositoryAccessLevel.READ,
          },
          RepositoryPlan.ADVANCED: {
            "name": "SEC Data Advanced",
            "monthly_credits": 25000,
            "price_monthly": 99.99,
            "access_level": RepositoryAccessLevel.WRITE,
          },
          RepositoryPlan.UNLIMITED: {
            "name": "SEC Data Unlimited",
            "monthly_credits": 100000,
            "price_monthly": 299.99,
            "access_level": RepositoryAccessLevel.ADMIN,
          },
        },
      },
      RepositoryType.INDUSTRY: {
        "enabled": False,
        "coming_soon": True,
        "plans": {
          RepositoryPlan.STARTER: {
            "name": "Industry Benchmarks Starter",
            "monthly_credits": 3000,
            "price_monthly": 19.99,
            "access_level": RepositoryAccessLevel.READ,
          },
        },
      },
      RepositoryType.ECONOMIC: {
        "enabled": False,
        "coming_soon": True,
        "plans": {
          RepositoryPlan.STARTER: {
            "name": "Economic Indicators Starter",
            "monthly_credits": 2000,
            "price_monthly": 14.99,
            "access_level": RepositoryAccessLevel.READ,
          },
        },
      },
    }

  @classmethod
  def is_repository_enabled(cls, repository_type: RepositoryType) -> bool:
    """Check if a repository type is enabled for subscriptions."""
    configs = cls.get_all_repository_configs()
    return configs.get(repository_type, {}).get("enabled", False)

  def to_dict(self) -> Dict[str, Any]:
    """Convert to dictionary for API responses."""
    import json

    config = self.get_repository_plan_config()

    return {
      "id": self.id,
      "user_id": self.user_id,
      "repository_type": self.repository_type.value,
      "repository_name": self.repository_name,
      "access_level": self.access_level.value,
      "repository_plan": self.repository_plan.value,
      "is_active": safe_bool(self.is_active),
      "activated_at": self.activated_at.isoformat(),
      "expires_at": self.expires_at.isoformat() if self.expires_at else None,
      "monthly_price_cents": self.monthly_price_cents,
      "monthly_credit_allocation": self.monthly_credit_allocation,
      "config": config,
      "metadata": json.loads(safe_str(self.extra_metadata))
      if self.extra_metadata is not None
      else {},
      "credits": self.user_credits.get_summary() if self.user_credits else None,
      "graph_connection": self.get_graph_connection_info(),
    }
