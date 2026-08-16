"""A user's subscription to a shared repository: plan, permissions, billing.

One row per (user, repository) pair, carrying the access level, the plan and
its price, and the monthly credit allocation that seeds the paired
``UserRepositoryCredits`` pool.
"""

import logging
from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Optional, cast

from sqlalchemy import (
  Boolean,
  Column,
  DateTime,
  ForeignKey,
  Index,
  Integer,
  String,
  Text,
  UniqueConstraint,
)
from sqlalchemy import (
  Enum as SQLEnum,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, relationship

from robosystems.config.graph_tier import GraphTier
from robosystems.config.shared_repositories import get_plan_details as _get_plan_details
from robosystems.database import Model
from robosystems.utils.ulid import generate_prefixed_ulid

logger = logging.getLogger(__name__)


class RepositoryType(str, Enum):
  """Known shared repository identifiers.

  Convenience enum for type-safe references in code. The DB column is a plain
  String so new shared repositories can be added via manifests without a
  migration. Values here match the shared repository registry IDs.
  """

  SEC = "sec"
  INDUSTRY = "industry"
  ECONOMIC = "economic"


def safe_str(value: Any) -> str:
  """Safely convert SQLAlchemy model attributes to string."""
  return str(value) if value is not None else ""


def safe_bool(value: Any) -> bool:
  """Safely convert SQLAlchemy model attributes to boolean."""
  return bool(value) if value is not None else False


class RepositoryAccessLevel(str, Enum):
  """Repository access levels."""

  NONE = "none"  # No access to repository
  READ = "read"  # Read-only access
  WRITE = "write"  # Read/write access (for data contributions)
  ADMIN = "admin"  # Full admin access including user management


class UserRepository(Model):
  """A user's access to one shared repository: subscription plus permission."""

  __tablename__ = "user_repository"

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("usra"))

  # User reference
  user_id = Column(String, ForeignKey("users.id"), nullable=False)

  # Repository identification (plain String — new repos added via manifests, no migration needed)
  repository_type = Column(String, nullable=False)
  # Holds the graph_id slug (e.g. "sec"), not a display name — it is the FK
  # target in graphs.graph_id.
  repository_name = Column(
    String, ForeignKey("graphs.graph_id", ondelete="RESTRICT"), nullable=False
  )

  access_level = Column(
    SQLEnum(RepositoryAccessLevel), nullable=False, default=RepositoryAccessLevel.NONE
  )

  # Repository plan management (plain String — plans defined in adapter manifests)
  repository_plan = Column(String, nullable=False, default="starter")

  # Status and lifecycle
  is_active = Column(Boolean, nullable=False, default=True)
  activated_at = Column(
    DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
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
    DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
  )
  updated_at = Column(
    DateTime(timezone=True),
    nullable=False,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
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
    Index("idx_user_shared_repo_name_active", "repository_name", "is_active"),
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
    repository_plan: str,
    session: Session,
    granted_by: str | None = None,
    monthly_price_cents: int = 0,
    monthly_credits: int = 0,
    expires_at: datetime | None = None,
    metadata: dict[str, Any] | None = None,
  ) -> "UserRepository":
    """Create or update repository access for a user.

    Upserts on the (user, repository) pair, then creates or resizes the paired
    credit pool when ``monthly_credits`` is non-zero.
    """
    import json
    from datetime import timedelta

    existing = cls.get_by_user_and_repository(user_id, repository_name, session)

    now = datetime.now(UTC)

    if existing:
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

      if monthly_price_cents > 0:
        access.billing_cycle_day = now.day
        access.next_billing_at = now + timedelta(days=30)

      session.add(access)

    try:
      session.commit()
      session.refresh(access)

      if monthly_credits > 0:
        from .user_repository_credits import UserRepositoryCredits

        if access.user_credits:
          access.user_credits.update_monthly_allocation(
            new_allocation=Decimal(str(monthly_credits)), session=session
          )
        else:
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

    access.invalidate_access_cache()

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

    if access.expires_at and access.expires_at < datetime.now(UTC):
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

    if access.expires_at and access.expires_at < datetime.now(UTC):
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

  def revoke_access(self, session: Session, reason: str | None = None) -> None:
    """Revoke repository access and deactivate the paired credit pool.

    Stamps ``suspended_at``/``suspension_reason`` on the pool alongside
    ``is_active``. Without them the pool records *that* it was suspended and
    never *why* or *when*, which is the state an operator actually needs when
    a customer asks why their credits stopped working.
    """
    now = datetime.now(UTC)
    self.is_active = False
    self.expires_at = now
    self.updated_at = now

    if self.user_credits:
      self.user_credits.is_active = False
      self.user_credits.suspended_at = now
      self.user_credits.suspension_reason = reason or "Repository access revoked"

    try:
      session.commit()
    except SQLAlchemyError:
      session.rollback()
      raise

    self.invalidate_access_cache()

  def invalidate_access_cache(self) -> None:
    """Drop the user's cached access decisions after this grant changes.

    Both auth paths cache the allow/deny answer per graph for the TTL, so the
    grant row is only authoritative once those entries are gone: a denial
    recorded before the subscription existed would keep refusing a paying
    subscriber, and an allow recorded before a revocation would keep admitting
    a former one — the endpoint's ``immediate=true`` promise is only as good
    as this call. Sweeps every entry the user owns rather than the one
    repository, because a subgraph of the repository is cached under its own
    id. Best-effort: the row has committed and the entries lapse on TTL.
    """
    try:
      import importlib

      cache_module = importlib.import_module("robosystems.middleware.auth.cache")
      cache_module.api_key_cache.invalidate_user_data(str(self.user_id))
    except Exception as e:
      logger.warning(
        f"Failed to invalidate access cache for user {self.user_id}, "
        f"repository {self.repository_name}: {e}"
      )

  def upgrade_tier(
    self,
    new_plan: str,
    session: Session,
    new_price_cents: int | None = None,
    new_credits: int | None = None,
  ) -> None:
    """Move the subscription to another plan, in either direction.

    ``new_price_cents`` and ``new_credits`` override the plan's own defaults —
    which is also how a price or allocation is adjusted without changing plan.
    A credit change propagates to the paired ``UserRepositoryCredits`` pool.
    """
    old_plan = self.repository_plan
    self.repository_plan = new_plan
    self.updated_at = datetime.now(UTC)

    if new_price_cents is not None:
      self.monthly_price_cents = new_price_cents

    if new_credits is not None:
      self.monthly_credit_allocation = new_credits

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
    return self.expires_at < datetime.now(UTC)

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

  def get_graph_connection_info(self) -> dict[str, Any]:
    """Graph-database connection info, read from the related ``Graph`` row.

    Falls back to the shared-tier defaults when the ``Graph`` row is absent.
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
        "repository_type": self.repository_type,
      }

    return {
      "instance_id": "ladybug-shared-prod",
      "cluster_region": None,
      "instance_tier": GraphTier.LADYBUG_SHARED,
      "repository_name": self.repository_name,
      "repository_type": self.repository_type,
    }

  def get_repository_plan_config(self) -> dict[str, Any]:
    """Plan configuration from ``config/shared_repositories.py``.

    That registry is the source of truth for pricing, credits, and access
    level. Returns an empty dict when the plan is not registered.
    """
    plan_details = _get_plan_details(self.repository_plan)
    if not plan_details:
      return {}

    access_level_str = plan_details.get("access_level", "READ")
    try:
      access_level = RepositoryAccessLevel(access_level_str.lower())
    except (ValueError, AttributeError):
      access_level = RepositoryAccessLevel.READ

    return {
      "name": plan_details.get("name", ""),
      "monthly_credits": plan_details.get("monthly_credits", 0),
      "price_monthly": plan_details.get("price_monthly", 0.0),
      "price_cents": plan_details.get("price_cents", 0),
      "access_level": access_level,
    }

  def to_dict(self) -> dict[str, Any]:
    """Convert to dictionary for API responses."""
    import json

    config = self.get_repository_plan_config()

    return {
      "id": self.id,
      "user_id": self.user_id,
      "repository_type": self.repository_type,
      "repository_name": self.repository_name,
      "access_level": self.access_level.value,
      "repository_plan": self.repository_plan,
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
