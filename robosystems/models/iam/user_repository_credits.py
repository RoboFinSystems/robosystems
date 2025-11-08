"""
User Repository Credits Model

This model tracks credit pools for accessing repositories through user subscriptions.
Unlike graph credits which are tied to specific graphs, these credits are user-level
and can be used across any repository the user has access to.
"""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, Dict, Any, cast
from enum import Enum

from sqlalchemy import (
  Column,
  String,
  DateTime,
  ForeignKey,
  Numeric,
  Boolean,
  Text,
  Index,
)
from sqlalchemy.orm import relationship, Session
from sqlalchemy.exc import SQLAlchemyError

from ...database import Base
from ...utils.ulid import generate_prefixed_ulid
from .user_repository import RepositoryType, RepositoryPlan

import logging

logger = logging.getLogger(__name__)


# Type-safe helpers for SQLAlchemy model attributes
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
  """
  Credit balance tracking for user repository access.

  Each user subscription gets its own credit pool that can be used
  for accessing the corresponding repository.
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
    DateTime(timezone=True), nullable=False, default=datetime.now(timezone.utc)
  )
  updated_at = Column(
    DateTime(timezone=True),
    nullable=False,
    default=datetime.now(timezone.utc),
    onupdate=datetime.now(timezone.utc),
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
    repository_type: "RepositoryType",
    repository_plan: "RepositoryPlan",
    monthly_allocation: int,
    session: Session,
  ) -> "UserRepositoryCredits":
    """Create credit pool for a new access record."""
    from datetime import timedelta
    from .user_repository import RepositoryPlan

    # Convert string values to enums if needed
    if isinstance(repository_plan, str):
      repository_plan = RepositoryPlan(repository_plan)

    # No rollover for repository credits - same as user graph credits
    allows_rollover = False
    max_rollover = Decimal("0")

    now = datetime.now(timezone.utc)

    credits = cls(
      user_repository_id=access_id,
      current_balance=Decimal(str(monthly_allocation)),
      monthly_allocation=Decimal(str(monthly_allocation)),
      allows_rollover=allows_rollover,
      max_rollover_credits=max_rollover,
      last_allocation_date=now,
      next_allocation_date=now + timedelta(days=30),
    )

    session.add(credits)

    try:
      session.commit()
      session.refresh(credits)

      # Record initial allocation
      UserRepositoryCreditTransaction.create_transaction(
        credit_pool_id=cast(str, credits.id),
        transaction_type=UserRepositoryCreditTransactionType.ALLOCATION,
        amount=cast(Decimal, credits.monthly_allocation),
        description=f"Initial allocation for {repository_type.value} {repository_plan.value}",
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
    metadata: Optional[Dict[str, Any]] = None,
  ) -> bool:
    """
    Consume credits for a repository operation.

    Returns True if successful, False if insufficient credits.
    """
    if not self.is_active:
      logger.warning(f"Attempted to consume credits from inactive pool {self.id}")
      return False

    # Use atomic update to prevent race conditions
    from sqlalchemy import text

    now = datetime.now(timezone.utc)
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
      # Check if it was due to insufficient balance or inactive status
      session.refresh(self)
      if not self.is_active:
        logger.warning(f"Attempted to consume credits from inactive pool {self.id}")
      else:
        logger.warning(
          f"Insufficient credits in pool {self.id}: need {amount}, have {self.current_balance}"
        )
      return False

    # Update the instance with new values
    session.refresh(self)

    # Record transaction
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

    # Audit log the credit consumption
    from robosystems.security import SecurityAuditLogger

    SecurityAuditLogger.log_financial_transaction(
      user_id=self.user_repository.user_id,
      transaction_type="credit_consumption",
      amount=float(amount),
      balance_before=float(self.current_balance + amount),
      balance_after=float(self.current_balance),
      metadata={
        "repository": repository_name,
        "repository_type": self.user_repository.repository_type.value,
        "operation": operation_type,
        "credit_pool_id": self.id,
      },
    )

    return True

  def allocate_monthly_credits(self, session: Session) -> bool:
    """Allocate monthly credits if due - no rollover, same as user graphs."""
    from datetime import timedelta

    now = datetime.now(timezone.utc)

    # Check if allocation is due
    if self.next_allocation_date and now < self.next_allocation_date:
      return False

    # No rollover - credits reset each month like user graph credits
    # Reset monthly consumption counter
    self.credits_consumed_this_month = Decimal("0")

    # Set new balance to monthly allocation (no rollover)
    MAX_BALANCE = Decimal("99999999.99")  # Max value for Numeric(10,2) field
    new_balance = self.monthly_allocation
    allocation_amount = self.monthly_allocation

    if new_balance > MAX_BALANCE:
      logger.warning(
        f"Credit balance overflow prevented for user pool {self.id}. "
        f"Would have been {new_balance}, capped at {MAX_BALANCE}"
      )
      new_balance = MAX_BALANCE

    self.current_balance = new_balance
    self.rollover_credits = Decimal("0")  # No rollover
    self.last_allocation_date = now
    self.next_allocation_date = now + timedelta(days=30)
    self.updated_at = now

    # Record allocation transaction
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
    """Update monthly allocation (e.g., after tier upgrade)."""
    old_allocation = self.monthly_allocation
    difference = new_allocation - old_allocation

    self.monthly_allocation = new_allocation
    self.updated_at = datetime.now(timezone.utc)

    # If immediate credit, add the difference to current balance with overflow protection
    if immediate_credit and difference > 0:
      MAX_BALANCE = Decimal("99999999.99")  # Max value for Numeric(10,2) field
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

    try:
      session.commit()
    except SQLAlchemyError:
      session.rollback()
      raise

  def get_summary(self) -> Dict[str, Any]:
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
    request_id: Optional[str] = None,
    user_id: Optional[str] = None,
    timeout_seconds: int = 300,
  ) -> Dict[str, Any]:
    """
    Atomically reserve credits for a repository operation with timeout protection.

    This creates a pending reservation that must be confirmed or cancelled.
    Reservations automatically expire after timeout_seconds.

    Args:
        amount: Credit amount to reserve (no multiplier for shared repositories)
        operation_type: Type of operation
        session: Database session
        reservation_id: Unique reservation identifier
        request_id: HTTP request ID for tracing
        user_id: User performing the operation
        timeout_seconds: Reservation timeout (default: 5 minutes)

    Returns:
        Dict with reservation results and status
    """
    from sqlalchemy import text
    from datetime import timedelta

    # For shared repositories, no credit multiplier - use base amount
    actual_cost = amount
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=timeout_seconds)

    try:
      # Use atomic SELECT FOR UPDATE with immediate reservation
      # This prevents race conditions by locking the row during the transaction
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
          "updated_at": datetime.now(timezone.utc),
          "credits_id": self.id,
        },
      )

      reservation_result = result.fetchone()

      # Check if the update affected any rows (i.e., if we had sufficient credits)
      if not reservation_result:
        # Get current balance for error message
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

      # Create reservation transaction record
      UserRepositoryCreditTransaction.create_transaction(
        credit_pool_id=self.id,
        transaction_type=UserRepositoryCreditTransactionType.CONSUMPTION,
        amount=-actual_cost,
        description=f"RESERVED: {operation_type} operation",
        metadata={
          "operation_type": operation_type,
          "repository_type": self.user_repository.repository_type.value,
          "reservation_id": reservation_id,
          "reservation_status": "reserved",
          "expires_at": expires_at.isoformat(),
          "atomic_reservation": True,
          "request_id": request_id,
          "user_id": user_id or self.user_repository.user_id,
        },
        session=session,
      )

      # Update the local object to match database state
      self.current_balance = reservation_result.new_balance
      self.updated_at = datetime.now(timezone.utc)

      # Commit the reservation
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
        "error": f"Reservation failed: {str(e)}",
        "reservation_id": reservation_id,
      }

  def confirm_credit_reservation(
    self,
    reservation_id: str,
    operation_type: str,
    session: Session,
    final_metadata: Optional[Dict[str, Any]] = None,
  ) -> Dict[str, Any]:
    """
    Confirm a credit reservation and finalize the consumption.

    Args:
        reservation_id: The reservation ID to confirm
        operation_type: Type of operation being confirmed
        session: Database session
        final_metadata: Final metadata to add to the transaction

    Returns:
        Dict with confirmation results
    """
    try:
      # Find the reservation transaction
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

      # Check if reservation has expired
      metadata = reservation_transaction.get_metadata()
      if metadata and metadata.get("expires_at"):
        expires_at = datetime.fromisoformat(
          metadata["expires_at"].replace("Z", "+00:00")
        )
        if datetime.now(timezone.utc) > expires_at:
          # Reservation expired - rollback the credits
          self.cancel_credit_reservation(reservation_id, session, "expired")
          return {
            "success": False,
            "error": "Reservation expired",
            "reservation_id": reservation_id,
            "expired_at": expires_at.isoformat(),
          }

      # Update transaction to mark as confirmed
      updated_metadata = metadata.copy() if metadata else {}
      updated_metadata.update(
        {
          "reservation_status": "confirmed",
          "confirmed_at": datetime.now(timezone.utc).isoformat(),
        }
      )
      if final_metadata:
        updated_metadata.update(final_metadata)

      import json

      reservation_transaction.description = f"{operation_type} operation (confirmed)"
      reservation_transaction.transaction_metadata = json.dumps(updated_metadata)
      reservation_transaction.created_at = datetime.now(
        timezone.utc
      )  # Update timestamp

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
        "error": f"Confirmation failed: {str(e)}",
        "reservation_id": reservation_id,
      }

  def cancel_credit_reservation(
    self,
    reservation_id: str,
    session: Session,
    reason: str = "cancelled",
  ) -> Dict[str, Any]:
    """
    Cancel a credit reservation and return the credits to the balance.

    Args:
        reservation_id: The reservation ID to cancel
        session: Database session
        reason: Reason for cancellation

    Returns:
        Dict with cancellation results
    """
    from sqlalchemy import text

    try:
      # Find the reservation transaction
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

      # Get the amount to refund (should be negative, so we add it back)
      refund_amount = abs(reservation_transaction.amount)

      # Atomically return the credits
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
          "updated_at": datetime.now(timezone.utc),
          "credits_id": self.id,
        },
      )

      refund_result = result.fetchone()

      # Create a refund transaction
      UserRepositoryCreditTransaction.create_transaction(
        credit_pool_id=self.id,
        transaction_type=UserRepositoryCreditTransactionType.REFUND,
        amount=refund_amount,
        description=f"REFUND: Cancelled reservation {reservation_id}",
        metadata={
          "reservation_id": reservation_id,
          "cancellation_reason": reason,
          "original_transaction_id": reservation_transaction.id,
          "cancelled_at": datetime.now(timezone.utc).isoformat(),
        },
        session=session,
      )

      # Mark original reservation as cancelled
      metadata = reservation_transaction.get_metadata()
      if metadata:
        metadata.update(
          {
            "reservation_status": "cancelled",
            "cancelled_at": datetime.now(timezone.utc).isoformat(),
            "cancellation_reason": reason,
          }
        )
        import json

        reservation_transaction.transaction_metadata = json.dumps(metadata)

      # Update the local object to match database state
      if refund_result:
        self.current_balance = refund_result.new_balance
      self.updated_at = datetime.now(timezone.utc)

      session.commit()

      return {
        "success": True,
        "reservation_id": reservation_id,
        "credits_refunded": float(refund_amount),
        "cancelled_at": datetime.now(timezone.utc).isoformat(),
        "reason": reason,
      }

    except Exception as e:
      logger.error(f"Error cancelling reservation {reservation_id}: {e}")
      session.rollback()
      return {
        "success": False,
        "error": f"Cancellation failed: {str(e)}",
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

    # Map repository types to repository types
    repo_to_type = {
      "sec": RepositoryType.SEC,
      "industry": RepositoryType.INDUSTRY,
      "economic": RepositoryType.ECONOMIC,
    }

    repo_type = repo_to_type.get(repository_type)
    if not repo_type:
      return None

    # Find user's access record for this type
    from .user_repository import UserRepository

    access_record = (
      session.query(UserRepository)
      .filter(
        UserRepository.user_id == user_id,
        UserRepository.repository_type == repo_type,
        UserRepository.is_active,
      )
      .first()
    )

    if not access_record:
      return None

    return access_record.user_credits


class UserRepositoryCreditTransaction(Base):
  """
  Transaction log for user repository credit movements.
  """

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
    DateTime(timezone=True), nullable=False, default=datetime.now(timezone.utc)
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
    metadata: Optional[Dict[str, Any]] = None,
    session: Optional[Session] = None,
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

  def get_metadata(self) -> Dict[str, Any]:
    """Parse transaction metadata."""
    if not self.transaction_metadata:
      return {}

    try:
      import json

      return json.loads(self.transaction_metadata)
    except Exception:
      return {}
