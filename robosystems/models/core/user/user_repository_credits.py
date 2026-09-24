"""Credit pools backing a user's shared-repository subscriptions.

One pool per ``UserRepository`` row, so a user with several repository
subscriptions holds several pools — unlike ``GraphCredits``, which is scoped to
a single graph.
"""

import logging
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
  Numeric,
  String,
  Text,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, relationship

from robosystems.database import Base
from robosystems.utils.ulid import generate_prefixed_ulid

logger = logging.getLogger(__name__)


def _first_of_next_month(now: datetime) -> datetime:
  """Midnight UTC on the 1st of the month after ``now``.

  The allocation cron runs on the 1st; any other due date is one the cron
  never lands on.
  """
  if now.month == 12:
    return now.replace(
      year=now.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0
    )
  return now.replace(
    month=now.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0
  )


def safe_float(value: Any) -> float:
  """Safely convert SQLAlchemy model attributes to float."""
  return float(value) if value is not None else 0.0


def safe_str(value: Any) -> str:
  """Safely convert SQLAlchemy model attributes to string."""
  return str(value) if value is not None else ""


def safe_bool(value: Any) -> bool:
  """Safely convert SQLAlchemy model attributes to boolean."""
  return bool(value) if value is not None else False


class UserRepositoryCreditTransactionType(str, Enum):
  """Types of user repository credit transactions."""

  ALLOCATION = "allocation"  # Monthly credit allocation
  CONSUMPTION = "consumption"  # Query/operation consumption
  BONUS = "bonus"  # Bonus credits
  REFUND = "refund"  # Credit refund
  ROLLOVER = "rollover"  # Unused credits rolled over
  EXPIRATION = "expiration"  # Credit expiration


class UserRepositoryCredits(Base):
  """Credit pool for one repository subscription (one per ``UserRepository``).

  ``allows_rollover`` / ``max_rollover_credits`` / ``rollover_credits`` exist on
  the table but are held at no-rollover: the monthly allocation replaces the
  balance rather than adding to it, matching ``GraphCredits``.
  """

  __tablename__ = "user_repository_credits"

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("crd"))

  user_repository_id = Column(
    String, ForeignKey("user_repository.id"), nullable=False, unique=True
  )

  # Credit balance
  current_balance = Column(Numeric(10, 2), nullable=False, default=0)
  monthly_allocation = Column(Numeric(10, 2), nullable=False, default=0)

  # Usage tracking
  credits_consumed_this_month = Column(Numeric(10, 2), nullable=False, default=0)
  last_consumption_at = Column(DateTime(timezone=True), nullable=True)

  # Allocation tracking
  last_allocation_date = Column(DateTime(timezone=True), nullable=True)
  next_allocation_date = Column(DateTime(timezone=True), nullable=True)

  # Rollover settings
  allows_rollover = Column(Boolean, nullable=False, default=False)
  max_rollover_credits = Column(Numeric(10, 2), nullable=True)  # None = unlimited
  rollover_credits = Column(Numeric(10, 2), nullable=False, default=0)
  is_active = Column(Boolean, nullable=False, default=True)
  suspended_at = Column(DateTime(timezone=True), nullable=True)
  suspension_reason = Column(String, nullable=True)
  created_at = Column(
    DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
  )
  updated_at = Column(
    DateTime(timezone=True),
    nullable=False,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
  )
  user_repository = relationship("UserRepository", back_populates="user_credits")
  transactions = relationship(
    "UserRepositoryCreditTransaction", back_populates="credit_pool"
  )
  __table_args__ = (
    Index("idx_user_repo_credits_access", "user_repository_id"),
    Index("idx_user_repo_credits_allocation", "next_allocation_date", "is_active"),
    Index("idx_user_repo_credits_balance", "current_balance"),
  )

  def __repr__(self):
    return f"<UserRepositoryCredits(user_repo={self.user_repository_id}, balance={self.current_balance})>"

  @classmethod
  def create_for_access(
    cls,
    access_id: str,
    repository_type: str,
    repository_plan: str,
    monthly_allocation: int,
    session: Session,
  ) -> "UserRepositoryCredits":
    """Create credit pool for a new access record."""
    allows_rollover = False
    max_rollover = Decimal("0")

    now = datetime.now(UTC)

    credits = cls(
      user_repository_id=access_id,
      current_balance=Decimal(str(monthly_allocation)),
      monthly_allocation=Decimal(str(monthly_allocation)),
      allows_rollover=allows_rollover,
      max_rollover_credits=max_rollover,
      last_allocation_date=now,
      # The allocation cron only runs on the 1st.
      next_allocation_date=_first_of_next_month(now),
    )

    session.add(credits)

    try:
      session.commit()
      session.refresh(credits)

      UserRepositoryCreditTransaction.create_transaction(
        credit_pool_id=cast(str, credits.id),
        transaction_type=UserRepositoryCreditTransactionType.ALLOCATION,
        amount=cast(Decimal, credits.monthly_allocation),
        description=f"Initial allocation for {repository_type} {repository_plan}",
        session=session,
      )

    except SQLAlchemyError:
      session.rollback()
      raise

    return credits

  def consume_credits(
    self,
    amount: Decimal,
    repository_name: str,
    operation_type: str,
    session: Session,
    metadata: dict[str, Any] | None = None,
    drain_on_shortfall: bool = False,
  ) -> bool:
    """Consume credits for a repository operation.

    Returns False rather than raising when the pool is inactive or short. The
    balance check rides inside the UPDATE's WHERE clause, so concurrent callers
    cannot both pass it.

    ``drain_on_shortfall`` (see ``GraphCredits.consume_credits_atomic``):
    default leaves a short pool untouched and just refuses; set it only on a
    post-hoc AI charge, where the pool is instead drained to zero and the
    shortfall recorded. Draining a pre-check would destroy credits the user
    still holds.
    """
    if not self.is_active:
      logger.warning(f"Attempted to consume credits from inactive pool {self.id}")
      return False

    from sqlalchemy import text

    now = datetime.now(UTC)
    result = session.execute(
      text("""
        UPDATE user_repository_credits
        SET current_balance = current_balance - :amount,
            credits_consumed_this_month = credits_consumed_this_month + :amount,
            last_consumption_at = :now,
            updated_at = :now
        WHERE id = :credit_id
          AND is_active = true
          AND current_balance >= :amount
      """),
      {"amount": float(amount), "now": now, "credit_id": self.id},
    )

    if result.rowcount == 0:
      # The WHERE clause covers both causes; re-read to say which one it was.
      session.refresh(self)
      if not self.is_active:
        logger.warning(f"Attempted to consume credits from inactive pool {self.id}")
        return False

      if drain_on_shortfall:
        self._drain_for_shortfall(
          amount=amount,
          repository_name=repository_name,
          operation_type=operation_type,
          session=session,
          metadata=metadata,
          now=now,
        )
      else:
        # Pre-check semantics: refuse, leave the remainder for a cheaper op.
        logger.warning(
          f"Insufficient credits in pool {self.id}: need {amount}, have {self.current_balance}"
        )
      return False

    session.refresh(self)

    transaction_metadata = {
      "repository": repository_name,
      "operation_type": operation_type,
    }
    if metadata:
      transaction_metadata.update(metadata)

    UserRepositoryCreditTransaction.create_transaction(
      credit_pool_id=cast(str, self.id),
      transaction_type=UserRepositoryCreditTransactionType.CONSUMPTION,
      amount=-amount,
      description=f"{repository_name} {operation_type}",
      metadata=transaction_metadata,
      session=session,
    )

    from robosystems.security import SecurityAuditLogger

    SecurityAuditLogger.log_financial_transaction(
      user_id=self.user_repository.user_id,
      transaction_type="credit_consumption",
      amount=float(amount),
      balance_before=float(self.current_balance + amount),
      balance_after=float(self.current_balance),
      metadata={
        "repository": repository_name,
        "repository_type": self.user_repository.repository_type,
        "operation": operation_type,
        "credit_pool_id": self.id,
      },
    )

    return True

  def _drain_for_shortfall(
    self,
    *,
    amount: Decimal,
    repository_name: str,
    operation_type: str,
    session: Session,
    metadata: dict[str, Any] | None,
    now: datetime,
  ) -> None:
    """Debit whatever an active-but-short pool still holds, down to zero.

    Records the shortfall on the transaction so the call is billed for what
    was there and the next pre-flight, reading an empty pool, denies. A pool
    already at zero records nothing.
    """
    from sqlalchemy import text

    drain = session.execute(
      text("""
        WITH prev AS (
          SELECT current_balance AS old_balance
          FROM user_repository_credits
          WHERE id = :credit_id
          FOR UPDATE
        )
        UPDATE user_repository_credits
        SET current_balance = 0,
            credits_consumed_this_month = credits_consumed_this_month + prev.old_balance,
            last_consumption_at = :now,
            updated_at = :now
        FROM prev
        WHERE user_repository_credits.id = :credit_id
          AND user_repository_credits.is_active = true
          AND prev.old_balance > 0
          AND prev.old_balance < :amount
        RETURNING prev.old_balance AS drained
      """),
      {"amount": float(amount), "now": now, "credit_id": self.id},
    )
    drained_row = drain.fetchone()
    session.refresh(self)

    if not drained_row:
      logger.warning(
        f"Insufficient credits in pool {self.id}: need {amount}, have {self.current_balance}"
      )
      return

    drained = Decimal(str(drained_row.drained))
    shortfall = amount - drained

    # Built-in keys win over caller metadata: they are the audit record.
    transaction_metadata = {
      **(metadata or {}),
      "repository": repository_name,
      "operation_type": operation_type,
      "drained_to_zero": True,
      "true_cost": str(amount),
      "shortfall": str(shortfall),
    }

    UserRepositoryCreditTransaction.create_transaction(
      credit_pool_id=cast(str, self.id),
      transaction_type=UserRepositoryCreditTransactionType.CONSUMPTION,
      amount=-drained,
      description=f"{repository_name} {operation_type} (pool drained; shortfall {shortfall})",
      metadata=transaction_metadata,
      session=session,
    )

    logger.warning(
      f"Credit pool {self.id} drained to zero: call cost {amount}, "
      f"billed {drained}, shortfall {shortfall}"
    )

  def allocate_monthly_credits(self, session: Session) -> bool:
    """Allocate monthly credits if due. Credits do not roll over.

    The balance is replaced by ``monthly_allocation``, matching ``GraphCredits``
    and the no-rollover promise on the offering page.
    """
    now = datetime.now(UTC)

    # One allocation per calendar month, matching the 1st-of-month cron. A
    # day-count gate drifts off the 1st and silently skips months.
    if self.last_allocation_date is not None:
      last = self.last_allocation_date
      if (last.year, last.month) == (now.year, now.month):
        return False

    self.credits_consumed_this_month = Decimal("0")

    MAX_BALANCE = Decimal("99999999.99")  # Ceiling of the Numeric(10, 2) column
    new_balance = self.monthly_allocation
    allocation_amount = self.monthly_allocation

    if new_balance > MAX_BALANCE:
      logger.warning(
        f"Credit balance overflow prevented for user pool {self.id}. "
        f"Would have been {new_balance}, capped at {MAX_BALANCE}"
      )
      new_balance = MAX_BALANCE

    self.current_balance = new_balance
    self.rollover_credits = Decimal("0")
    self.last_allocation_date = now
    # Read by the Dagster op's due-pool filter.
    self.next_allocation_date = _first_of_next_month(now)
    self.updated_at = now

    UserRepositoryCreditTransaction.create_transaction(
      credit_pool_id=cast(str, self.id),
      transaction_type=UserRepositoryCreditTransactionType.ALLOCATION,
      amount=cast(Decimal, allocation_amount),
      description="Monthly credit allocation",
      metadata={"rollover_credits": "0"},
      session=session,
    )

    return True

  def update_monthly_allocation(
    self,
    new_allocation: Decimal,
    session: Session,
    immediate_credit: bool = True,
  ) -> None:
    """Update the monthly allocation, as on a plan change.

    With ``immediate_credit`` the balance moves by the delta now. The
    adjustment is symmetric (a downgrade takes back what an upgrade granted,
    or plan cycling would compound grants), never drives the balance below
    zero, and is idempotent for a given target.
    """
    old_allocation = self.monthly_allocation
    difference = new_allocation - old_allocation

    # A downgrade cannot reclaim spent credits, so an upgrade grants only what
    # this period has not already credited.
    grant = difference
    if immediate_credit and difference > 0:
      headroom = new_allocation - self._credited_this_period(session)
      grant = min(difference, max(Decimal("0"), headroom))

    self.monthly_allocation = new_allocation
    self.updated_at = datetime.now(UTC)

    if immediate_credit and difference > 0 and grant == 0:
      logger.info(
        f"Upgrade on user pool {self.id} granted nothing: this period has "
        f"already credited {new_allocation}"
      )

    elif immediate_credit and difference > 0:
      MAX_BALANCE = Decimal("99999999.99")  # Ceiling of the Numeric(10, 2) column
      new_balance = self.current_balance + grant
      if new_balance > MAX_BALANCE:
        logger.warning(
          f"Credit balance overflow prevented for user pool {self.id}. "
          f"Would have been {new_balance}, capped at {MAX_BALANCE}"
        )
        new_balance = MAX_BALANCE
      self.current_balance = new_balance

      UserRepositoryCreditTransaction.create_transaction(
        credit_pool_id=self.id,
        transaction_type=UserRepositoryCreditTransactionType.BONUS,
        amount=grant,
        description="Tier upgrade credit adjustment",
        metadata={
          "old_allocation": str(old_allocation),
          "new_allocation": str(new_allocation),
        },
        session=session,
      )

    elif immediate_credit and difference < 0:
      # Clamped at zero: a plan change never claws back credits already used.
      available = max(Decimal("0"), cast(Decimal, self.current_balance))
      deduction = min(-difference, available)

      if deduction > 0:
        self.current_balance = cast(Decimal, self.current_balance) - deduction

        UserRepositoryCreditTransaction.create_transaction(
          credit_pool_id=cast(str, self.id),
          transaction_type=UserRepositoryCreditTransactionType.BONUS,
          amount=-deduction,
          description="Tier downgrade credit adjustment",
          metadata={
            "old_allocation": str(old_allocation),
            "new_allocation": str(new_allocation),
            "uncollected": str(-difference - deduction),
          },
          session=session,
        )
      else:
        logger.info(
          f"Downgrade on user pool {self.id} deducted nothing: balance "
          f"{self.current_balance} leaves no headroom against a "
          f"{-difference} reduction"
        )

    try:
      session.commit()
    except SQLAlchemyError:
      session.rollback()
      raise

  def _credited_this_period(self, session: Session) -> Decimal:
    """Credits granted since the last monthly allocation: that allocation
    plus every plan-change adjustment (net of downgrade deductions)."""
    query = session.query(UserRepositoryCreditTransaction).filter(
      UserRepositoryCreditTransaction.credit_pool_id == self.id,
      UserRepositoryCreditTransaction.transaction_type.in_(
        [
          UserRepositoryCreditTransactionType.ALLOCATION.value,
          UserRepositoryCreditTransactionType.BONUS.value,
        ]
      ),
    )
    if self.last_allocation_date is not None:
      query = query.filter(
        UserRepositoryCreditTransaction.created_at >= self.last_allocation_date
      )

    credited = Decimal("0")
    for txn in query.all():
      is_plan_change = "new_allocation" in txn.get_metadata()
      if (
        txn.transaction_type == UserRepositoryCreditTransactionType.ALLOCATION.value
        or is_plan_change
      ):
        credited += cast(Decimal, txn.amount)
    return credited

  def get_summary(self) -> dict[str, Any]:
    """Get credit summary for API responses."""
    return {
      "current_balance": safe_float(self.current_balance),
      "monthly_allocation": safe_float(self.monthly_allocation),
      "consumed_this_month": safe_float(self.credits_consumed_this_month),
      "usage_percentage": safe_float(
        self.credits_consumed_this_month / self.monthly_allocation * 100
        if safe_float(self.monthly_allocation) > 0
        else 0
      ),
      "rollover_credits": safe_float(self.rollover_credits),
      "allows_rollover": safe_bool(self.allows_rollover),
      "last_allocation_date": self.last_allocation_date.isoformat()
      if self.last_allocation_date is not None
      else None,
      "next_allocation_date": self.next_allocation_date.isoformat()
      if self.next_allocation_date is not None
      else None,
      "is_active": safe_bool(self.is_active),
    }

  @classmethod
  def get_user_repository_credits(
    cls,
    user_id: str,
    repository_type: str,
    session: Session,
  ) -> Optional["UserRepositoryCredits"]:
    """Get repository credits for a specific repository type."""
    from .user_repository import UserRepository

    access_record = (
      session.query(UserRepository)
      .filter(
        UserRepository.user_id == user_id,
        UserRepository.repository_type == repository_type,
        UserRepository.is_active,
      )
      .first()
    )

    if not access_record:
      return None

    return access_record.user_credits


class UserRepositoryCreditTransaction(Base):
  """Append-only log of movements in a repository credit pool."""

  __tablename__ = "user_repository_credit_transactions"

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("txn"))

  credit_pool_id = Column(
    String, ForeignKey("user_repository_credits.id"), nullable=False
  )

  # Transaction details
  transaction_type = Column(
    String, nullable=False
  )  # UserRepositoryCreditTransactionType
  amount = Column(
    Numeric(10, 2), nullable=False
  )  # Positive for credits, negative for consumption
  description = Column(String(500), nullable=False)
  transaction_metadata = Column("metadata", Text, nullable=True)  # JSON
  created_at = Column(
    DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
  )
  credit_pool = relationship("UserRepositoryCredits", back_populates="transactions")
  __table_args__ = (
    Index("idx_user_repo_credit_trans_pool", "credit_pool_id"),
    Index("idx_user_repo_credit_trans_type", "transaction_type"),
    Index("idx_user_repo_credit_trans_created", "created_at"),
  )

  def __repr__(self):
    return f"<UserRepositoryCreditTransaction(type={self.transaction_type}, amount={self.amount})>"

  @classmethod
  def create_transaction(
    cls,
    credit_pool_id: str,
    transaction_type: UserRepositoryCreditTransactionType,
    amount: Decimal,
    description: str,
    metadata: dict[str, Any] | None = None,
    session: Session | None = None,
  ) -> "UserRepositoryCreditTransaction":
    """Create a new transaction record."""
    import json

    transaction = cls(
      credit_pool_id=credit_pool_id,
      transaction_type=transaction_type.value,
      amount=amount,
      description=description,
      transaction_metadata=json.dumps(metadata) if metadata else None,
    )

    if session:
      session.add(transaction)
      try:
        session.commit()
        session.refresh(transaction)
      except SQLAlchemyError:
        session.rollback()
        raise

    return transaction

  def get_metadata(self) -> dict[str, Any]:
    """Parse transaction metadata."""
    if not self.transaction_metadata:
      return {}

    try:
      import json

      return json.loads(self.transaction_metadata)
    except Exception:
      return {}
