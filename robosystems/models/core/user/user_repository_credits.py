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

  # Status
  is_active = Column(Boolean, nullable=False, default=True)
  suspended_at = Column(DateTime(timezone=True), nullable=True)
  suspension_reason = Column(String, nullable=True)

  # Metadata
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
  user_repository = relationship("UserRepository", back_populates="user_credits")
  transactions = relationship(
    "UserRepositoryCreditTransaction", back_populates="credit_pool"
  )

  # Indexes
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
      # Aligned with the 1st-of-month allocation cron — a +30d seed lands
      # mid-month, where the cron never fires, and the first refill silently
      # skips a month.
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

    # Built-ins win on collision: these are the audit-critical fields the drain
    # exists to record, so a caller-supplied `metadata` key must not overwrite
    # them. Matches GraphCredits._drain_for_shortfall (the success path above
    # spreads caller-last, but that record is not audit-critical the way this
    # one is).
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

    # One allocation per calendar month, matching the 1st-of-month cron that
    # drives it (GraphCredits carries the same gate). The previous day-count
    # gate (now < next_allocation_date, re-armed at +30 days) drifted off the
    # 1st: a mid-month purchase re-armed to a mid-month date the cron never
    # runs on, silently skipping that month's refill — and a 1-Feb allocation
    # re-armed to 3-Mar, skipping March for every active pool, every year.
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
    # Kept for the Dagster op's due-pool filter and its index; pinned to the
    # 1st so the date can never drift away from the cron again.
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

    With ``immediate_credit`` the balance moves by the change in allocation
    right away, rather than waiting for the next monthly allocation, so a
    same-day plan change takes effect the day it is bought.

    The adjustment is **symmetric**: a downgrade takes back what an upgrade
    would have granted. Granting on the way up without deducting on the way
    down does not merely under-charge — it compounds, because the next
    upgrade's delta is computed against the new, lower allocation and grants
    the difference a second time for the same net position.

    Two properties bound it. The deduction never drives the balance below
    zero, so a downgrade cannot manufacture an overage out of credits already
    spent; and the operation is idempotent for a given target, since a repeat
    call finds ``monthly_allocation`` already at ``new_allocation`` and
    computes a zero delta. That makes a retried plan change a no-op without
    needing a separate idempotency record.
    """
    old_allocation = self.monthly_allocation
    difference = new_allocation - old_allocation

    self.monthly_allocation = new_allocation
    self.updated_at = datetime.now(UTC)

    if immediate_credit and difference > 0:
      MAX_BALANCE = Decimal("99999999.99")  # Ceiling of the Numeric(10, 2) column
      new_balance = self.current_balance + difference
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
        amount=difference,
        description="Tier upgrade credit adjustment",
        metadata={
          "old_allocation": str(old_allocation),
          "new_allocation": str(new_allocation),
        },
        session=session,
      )

    elif immediate_credit and difference < 0:
      # Clamped to what is actually there. A balance already drawn down (or
      # negative from an overage) must not be pushed further down by a plan
      # change — the user keeps what they have already used.
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

  def reserve_credits_atomic(
    self,
    amount: Decimal,
    operation_type: str,
    session: Session,
    reservation_id: str,
    request_id: str | None = None,
    user_id: str | None = None,
    timeout_seconds: int = 300,
  ) -> dict[str, Any]:
    """Reserve credits ahead of an operation, to be confirmed or cancelled.

    The credits leave the balance immediately, so a reservation that is never
    resolved strands them until it is cancelled — hence ``timeout_seconds``,
    after which ``confirm_credit_reservation`` refuses and refunds.
    """
    from datetime import timedelta

    from sqlalchemy import text

    actual_cost = amount
    expires_at = datetime.now(UTC) + timedelta(seconds=timeout_seconds)

    try:
      # Balance check rides in the WHERE clause so concurrent reservations
      # cannot both succeed against the same remaining balance.
      result = session.execute(
        text("""
          UPDATE user_repository_credits
          SET current_balance = current_balance - :actual_cost,
              updated_at = :updated_at
          WHERE id = :credits_id
            AND current_balance >= :actual_cost
            AND is_active = true
          RETURNING current_balance + :actual_cost as old_balance, current_balance as new_balance
        """),
        {
          "actual_cost": actual_cost,
          "updated_at": datetime.now(UTC),
          "credits_id": self.id,
        },
      )

      reservation_result = result.fetchone()

      if not reservation_result:
        current_result = session.execute(
          text(
            "SELECT current_balance, is_active FROM user_repository_credits WHERE id = :credits_id"
          ),
          {"credits_id": self.id},
        )
        current_balance_result = current_result.fetchone()
        available_balance = (
          float(current_balance_result[0]) if current_balance_result else 0
        )
        is_active = current_balance_result[1] if current_balance_result else False

        error_msg = "Insufficient credits for reservation"
        if not is_active:
          error_msg = "Repository credit pool is inactive"

        return {
          "success": False,
          "error": error_msg,
          "required_credits": float(actual_cost),
          "available_credits": available_balance,
          "reservation_id": reservation_id,
        }

      UserRepositoryCreditTransaction.create_transaction(
        credit_pool_id=self.id,
        transaction_type=UserRepositoryCreditTransactionType.CONSUMPTION,
        amount=-actual_cost,
        description=f"RESERVED: {operation_type} operation",
        metadata={
          "operation_type": operation_type,
          "repository_type": self.user_repository.repository_type,
          "reservation_id": reservation_id,
          "reservation_status": "reserved",
          "expires_at": expires_at.isoformat(),
          "atomic_reservation": True,
          "request_id": request_id,
          "user_id": user_id or self.user_repository.user_id,
        },
        session=session,
      )

      # The atomic UPDATE already persisted the reservation debit. Writing the
      # RETURNING value back through the ORM would emit an absolute SET on
      # commit and revert any concurrent movement (lost update) — expire so
      # the next access reloads instead.
      if self in session:
        session.expire(self, ["current_balance", "updated_at"])

      session.commit()

      return {
        "success": True,
        "reservation_id": reservation_id,
        "credits_reserved": float(actual_cost),
        "old_balance": float(reservation_result.old_balance),
        "new_balance": float(reservation_result.new_balance),
        "expires_at": expires_at.isoformat(),
        "timeout_seconds": timeout_seconds,
      }

    except Exception as e:
      logger.error(f"Error reserving repository credits for pool {self.id}: {e}")
      session.rollback()
      return {
        "success": False,
        "error": f"Reservation failed: {e!s}",
        "reservation_id": reservation_id,
      }

  def confirm_credit_reservation(
    self,
    reservation_id: str,
    operation_type: str,
    session: Session,
    final_metadata: dict[str, Any] | None = None,
  ) -> dict[str, Any]:
    """Finalize a reservation, turning the held credits into consumption.

    An expired reservation is cancelled (credits refunded) and reported as a
    failure rather than confirmed.
    """
    try:
      reservation_transaction = (
        session.query(UserRepositoryCreditTransaction)
        .filter(
          UserRepositoryCreditTransaction.credit_pool_id == self.id,
          UserRepositoryCreditTransaction.transaction_metadata.contains(
            f'"reservation_id": "{reservation_id}"'
          ),
          UserRepositoryCreditTransaction.transaction_type
          == UserRepositoryCreditTransactionType.CONSUMPTION.value,
        )
        .first()
      )

      if not reservation_transaction:
        return {
          "success": False,
          "error": "Reservation not found",
          "reservation_id": reservation_id,
        }

      metadata = reservation_transaction.get_metadata()
      if metadata and metadata.get("expires_at"):
        expires_at = datetime.fromisoformat(
          metadata["expires_at"].replace("Z", "+00:00")
        )
        if datetime.now(UTC) > expires_at:
          self.cancel_credit_reservation(reservation_id, session, "expired")
          return {
            "success": False,
            "error": "Reservation expired",
            "reservation_id": reservation_id,
            "expired_at": expires_at.isoformat(),
          }

      updated_metadata = metadata.copy() if metadata else {}
      updated_metadata.update(
        {
          "reservation_status": "confirmed",
          "confirmed_at": datetime.now(UTC).isoformat(),
        }
      )
      if final_metadata:
        updated_metadata.update(final_metadata)

      import json

      reservation_transaction.description = f"{operation_type} operation (confirmed)"
      reservation_transaction.transaction_metadata = json.dumps(updated_metadata)
      reservation_transaction.created_at = datetime.now(UTC)

      session.commit()

      return {
        "success": True,
        "reservation_id": reservation_id,
        "credits_consumed": float(abs(reservation_transaction.amount)),
        "confirmed_at": updated_metadata["confirmed_at"],
      }

    except Exception as e:
      logger.error(f"Error confirming reservation {reservation_id}: {e}")
      session.rollback()
      return {
        "success": False,
        "error": f"Confirmation failed: {e!s}",
        "reservation_id": reservation_id,
      }

  def cancel_credit_reservation(
    self,
    reservation_id: str,
    session: Session,
    reason: str = "cancelled",
  ) -> dict[str, Any]:
    """Cancel a reservation and return the held credits to the balance.

    Posts a REFUND transaction rather than reversing the original row, so the
    reservation and its unwind both stay in the ledger.
    """
    from sqlalchemy import text

    try:
      reservation_transaction = (
        session.query(UserRepositoryCreditTransaction)
        .filter(
          UserRepositoryCreditTransaction.credit_pool_id == self.id,
          UserRepositoryCreditTransaction.transaction_metadata.contains(
            f'"reservation_id": "{reservation_id}"'
          ),
          UserRepositoryCreditTransaction.transaction_type
          == UserRepositoryCreditTransactionType.CONSUMPTION.value,
        )
        .first()
      )

      if not reservation_transaction:
        return {
          "success": False,
          "error": "Reservation not found",
          "reservation_id": reservation_id,
        }

      # The reservation row is negative; refund its magnitude.
      refund_amount = abs(reservation_transaction.amount)

      result = session.execute(
        text("""
          UPDATE user_repository_credits
          SET current_balance = current_balance + :refund_amount,
              updated_at = :updated_at
          WHERE id = :credits_id
          RETURNING current_balance - :refund_amount as old_balance, current_balance as new_balance
        """),
        {
          "refund_amount": refund_amount,
          "updated_at": datetime.now(UTC),
          "credits_id": self.id,
        },
      )

      result.fetchone()

      UserRepositoryCreditTransaction.create_transaction(
        credit_pool_id=self.id,
        transaction_type=UserRepositoryCreditTransactionType.REFUND,
        amount=refund_amount,
        description=f"REFUND: Cancelled reservation {reservation_id}",
        metadata={
          "reservation_id": reservation_id,
          "cancellation_reason": reason,
          "original_transaction_id": reservation_transaction.id,
          "cancelled_at": datetime.now(UTC).isoformat(),
        },
        session=session,
      )

      metadata = reservation_transaction.get_metadata()
      if metadata:
        metadata.update(
          {
            "reservation_status": "cancelled",
            "cancelled_at": datetime.now(UTC).isoformat(),
            "cancellation_reason": reason,
          }
        )
        import json

        reservation_transaction.transaction_metadata = json.dumps(metadata)

      # Refund already applied by the atomic UPDATE — expire, never write the
      # absolute value back (same lost-update hazard as the reserve path).
      if self in session:
        session.expire(self, ["current_balance", "updated_at"])

      session.commit()

      return {
        "success": True,
        "reservation_id": reservation_id,
        "credits_refunded": float(refund_amount),
        "cancelled_at": datetime.now(UTC).isoformat(),
        "reason": reason,
      }

    except Exception as e:
      logger.error(f"Error cancelling reservation {reservation_id}: {e}")
      session.rollback()
      return {
        "success": False,
        "error": f"Cancellation failed: {e!s}",
        "reservation_id": reservation_id,
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

  # Metadata
  transaction_metadata = Column("metadata", Text, nullable=True)  # JSON
  created_at = Column(
    DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
  )

  # Relationships
  credit_pool = relationship("UserRepositoryCredits", back_populates="transactions")

  # Indexes
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
