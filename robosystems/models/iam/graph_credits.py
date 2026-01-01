"""
Graph credit system models for tracking AI operation credits.

This module implements the simplified credit system where:
- Each graph has its own credit pool for AI operations
- Only AI operations (Anthropic/OpenAI) consume credits
- All database operations are included
- Credits are consumed post-operation based on actual token usage
"""

import logging
from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Optional

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
from sqlalchemy.orm import Session, relationship

from ...config.billing.core import StorageBillingConfig
from ...config.graph_tier import GraphTier
from ...database import Base
from ...utils.ulid import generate_prefixed_ulid

logger = logging.getLogger(__name__)


def safe_float(value: Any) -> float:
  """Safely convert SQLAlchemy model attributes to float."""
  if value is None:
    return 0.0
  return float(value)


class CreditTransactionType(str, Enum):
  """Types of credit transactions."""

  ALLOCATION = "allocation"  # Monthly credit allocation
  CONSUMPTION = "consumption"  # API call consumption
  BONUS = "bonus"  # Bonus credits (referrals, etc.)
  REFUND = "refund"  # Credit refund
  EXPIRATION = "expiration"  # Credit expiration


class GraphCredits(Base):
  """
  Credit balance tracking for each graph database.

  Each graph has its own credit pool that gets allocated monthly
  based on the subscription tier.
  """

  __tablename__ = "graph_credits"

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("crd"))
  graph_id = Column(String, ForeignKey("graphs.graph_id"), nullable=False, unique=True)
  user_id = Column(String, ForeignKey("users.id"), nullable=False)

  # Current credit balance
  current_balance = Column(Numeric(10, 2), nullable=False, default=0)

  # Monthly allocation based on subscription tier
  monthly_allocation = Column(Numeric(10, 2), nullable=False, default=0)

  # Storage limits and management
  storage_limit_gb = Column(
    Numeric(10, 2), nullable=False, default=500
  )  # Default storage limit
  storage_override_gb = Column(Numeric(10, 2), nullable=True)  # Admin override limit
  auto_expand_enabled = Column(
    Boolean, nullable=False, default=False
  )  # Future: auto-expansion
  last_storage_warning_at = Column(DateTime(timezone=True), nullable=True)
  storage_warning_threshold = Column(
    Numeric(3, 2), nullable=False, default=0.8
  )  # 80% warning

  # Billing admin (who pays for this graph)
  billing_admin_id = Column(String, ForeignKey("users.id"), nullable=False)

  # Last allocation date
  last_allocation_date = Column(DateTime(timezone=True), nullable=True)

  # Tracking
  created_at = Column(
    DateTime(timezone=True), nullable=False, default=datetime.now(UTC)
  )
  updated_at = Column(
    DateTime(timezone=True),
    nullable=False,
    default=datetime.now(UTC),
    onupdate=datetime.now(UTC),
  )

  # Relationships
  user = relationship("User", foreign_keys=[user_id])
  billing_admin = relationship("User", foreign_keys=[billing_admin_id])
  transactions = relationship("GraphCreditTransaction", back_populates="graph_credits")
  graph = relationship("Graph", foreign_keys=[graph_id])

  # Indexes
  __table_args__ = (
    Index("idx_graph_credits_user_id", user_id),
    Index("idx_graph_credits_billing_admin", billing_admin_id),
    Index("idx_graph_credits_graph_id", graph_id),
    Index("idx_graph_credits_storage_monitoring", storage_limit_gb),
  )

  def __repr__(self):
    return f"<GraphCredits(graph_id={self.graph_id}, balance={self.current_balance})>"

  @property
  def graph_tier(self) -> str:
    """Get graph tier from the related Graph model."""
    if self.graph:
      return self.graph.graph_tier
    # Fallback for backwards compatibility during migration
    return GraphTier.LADYBUG_STANDARD.value

  @classmethod
  def get_by_graph_id(cls, graph_id: str, session: Session) -> Optional["GraphCredits"]:
    """Get credit record for a specific graph."""
    return session.query(cls).filter(cls.graph_id == graph_id).first()

  @classmethod
  def create_for_graph(
    cls,
    graph_id: str,
    user_id: str,
    billing_admin_id: str,
    monthly_allocation: Decimal,
    session: Session,
  ) -> "GraphCredits":
    """Create credit record for a new graph."""
    from .graph import Graph

    # Get the graph to determine tier
    graph = Graph.get_by_id(graph_id, session)
    if not graph:
      raise ValueError(f"Graph {graph_id} not found")

    graph_tier = GraphTier(graph.graph_tier)

    # In simplified model, no multipliers are used
    # All tiers have 1.0 multiplier

    # Get storage limit from billing configuration
    storage_limit_gb = StorageBillingConfig.STORAGE_INCLUDED.get(graph_tier.value, 100)

    credits = cls(
      id=generate_prefixed_ulid("crd"),
      graph_id=graph_id,
      user_id=user_id,
      billing_admin_id=billing_admin_id,
      storage_limit_gb=Decimal(str(storage_limit_gb)),
      monthly_allocation=monthly_allocation,
      current_balance=monthly_allocation,  # Start with full allocation
      last_allocation_date=datetime.now(UTC),
    )

    session.add(credits)
    session.commit()

    # Record initial allocation transaction with idempotency
    idempotency_key = f"initial_allocation_{graph_id}"

    GraphCreditTransaction.create_transaction(
      graph_credits_id=credits.id,
      transaction_type=CreditTransactionType.ALLOCATION,
      amount=monthly_allocation,
      description=f"Initial credit allocation for {graph_tier.value} tier",
      metadata={
        "allocation_type": "initial",
        "graph_tier": graph_tier.value,
      },
      session=session,
      idempotency_key=idempotency_key,
      graph_id=graph_id,
      user_id=user_id,
    )

    return credits

  def consume_credits_atomic(
    self,
    amount: Decimal,
    operation_type: str,
    operation_description: str,
    session: Session,
    request_id: str | None = None,
    user_id: str | None = None,
  ) -> dict[str, Any]:
    """
    Atomically consume credits for AI operations.

    In the simplified model, credits are only consumed for AI operations
    and are consumed post-operation based on actual token usage.

    Args:
        amount: Credit amount to consume (based on actual token usage)
        operation_type: Type of operation (should be AI-related)
        operation_description: Human-readable description
        session: Database session
        request_id: HTTP request ID for tracing
        user_id: User performing the operation

    Returns:
        Dict with consumption results
    """
    from sqlalchemy import text

    from ...utils import generate_prefixed_ulid

    transaction_id = generate_prefixed_ulid("tx")

    try:
      # In simplified model, no multipliers are applied
      actual_cost = amount

      # Atomically deduct credits
      result = session.execute(
        text("""
          UPDATE graph_credits
          SET current_balance = current_balance - :actual_cost,
              updated_at = :updated_at
          WHERE id = :credits_id
            AND current_balance >= :actual_cost
          RETURNING current_balance + :actual_cost as old_balance, current_balance as new_balance
        """),
        {
          "actual_cost": actual_cost,
          "updated_at": datetime.now(UTC),
          "credits_id": self.id,
        },
      )

      consumption_result = result.fetchone()

      if not consumption_result:
        # Insufficient credits
        current_result = session.execute(
          text("SELECT current_balance FROM graph_credits WHERE id = :credits_id"),
          {"credits_id": self.id},
        )
        current_balance = current_result.fetchone()
        available_balance = float(current_balance[0]) if current_balance else 0

        return {
          "success": False,
          "error": "Insufficient credits",
          "required_credits": float(actual_cost),
          "available_credits": available_balance,
        }

      # Create consumption transaction record
      GraphCreditTransaction.create_transaction(
        graph_credits_id=self.id,
        transaction_type=CreditTransactionType.CONSUMPTION,
        amount=-actual_cost,
        description=operation_description,
        metadata={
          "operation_type": operation_type,
          "base_cost": str(amount),
          "transaction_id": transaction_id,
        },
        session=session,
        idempotency_key=f"consume_{transaction_id}",
        request_id=request_id,
        operation_id=transaction_id,
        graph_id=self.graph_id,
        user_id=user_id or self.user_id,
      )

      # Update the local object
      self.current_balance = consumption_result.new_balance
      self.updated_at = datetime.now(UTC)

      session.commit()

      return {
        "success": True,
        "credits_consumed": float(actual_cost),
        "base_cost": float(amount),
        "old_balance": float(consumption_result.old_balance),
        "new_balance": float(consumption_result.new_balance),
        "transaction_id": transaction_id,
      }

    except Exception as e:
      logger.error(f"Error consuming credits for graph {self.graph_id}: {e}")
      session.rollback()
      return {
        "success": False,
        "error": f"Credit consumption failed: {e!s}",
      }

  def allocate_monthly_credits(self, session: Session) -> bool:
    """Allocate monthly credits if due."""
    now = datetime.now(UTC)

    # Check if allocation is due (monthly)
    if self.last_allocation_date is not None:
      days_since_last = (now - self.last_allocation_date).days
      if days_since_last < 30:  # Not due yet
        return False

    # Add monthly allocation with overflow protection
    MAX_BALANCE = Decimal("99999999.99")  # Max value for Numeric(10,2) field
    new_balance = self.current_balance + self.monthly_allocation
    if new_balance > MAX_BALANCE:
      logger.warning(
        f"Credit balance overflow prevented for graph {self.graph_id}. "
        f"Would have been {new_balance}, capped at {MAX_BALANCE}"
      )
      new_balance = MAX_BALANCE

    self.current_balance = new_balance
    self.last_allocation_date = now
    self.updated_at = now

    # Record transaction with idempotency based on month
    allocation_month = now.strftime("%Y-%m")
    idempotency_key = f"monthly_allocation_{self.graph_id}_{allocation_month}"

    GraphCreditTransaction.create_transaction(
      graph_credits_id=self.id,
      transaction_type=CreditTransactionType.ALLOCATION,
      amount=self.monthly_allocation,
      description="Monthly credit allocation",
      metadata={
        "allocation_month": allocation_month,
        "allocation_type": "monthly",
      },
      session=session,
      idempotency_key=idempotency_key,
      graph_id=self.graph_id,
      user_id=self.user_id,
    )

    return True

  def get_usage_summary(self, session: Session) -> dict[str, Any]:
    """Get usage summary for this graph."""
    from sqlalchemy import func

    # Get usage for current month
    now = datetime.now(UTC)
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    transactions = (
      session.query(
        func.sum(GraphCreditTransaction.amount).label("total_consumed"),
        func.count(GraphCreditTransaction.id).label("transaction_count"),
      )
      .filter(
        GraphCreditTransaction.graph_credits_id == self.id,
        GraphCreditTransaction.transaction_type
        == CreditTransactionType.CONSUMPTION.value,
        GraphCreditTransaction.created_at >= month_start,
      )
      .first()
    )

    consumed_this_month = (
      abs(transactions.total_consumed)
      if transactions and transactions.total_consumed
      else Decimal("0")
    )

    # Calculate the actual current balance based on allocation minus consumption
    actual_current_balance = self.monthly_allocation - consumed_this_month

    return {
      "graph_id": self.graph_id,
      "graph_tier": self.graph_tier,  # This now uses the property that gets it from graph
      "current_balance": safe_float(actual_current_balance),
      "monthly_allocation": safe_float(self.monthly_allocation),
      "consumed_this_month": safe_float(consumed_this_month),
      "transaction_count": transactions.transaction_count if transactions else 0,
      "usage_percentage": safe_float(
        consumed_this_month / self.monthly_allocation * 100
      )
      if self.monthly_allocation is not None and safe_float(self.monthly_allocation) > 0
      else 0.0,
      "last_allocation_date": self.last_allocation_date.isoformat()
      if self.last_allocation_date is not None
      else None,
      "storage_limit_gb": safe_float(self.storage_limit_gb),
      "storage_override_gb": safe_float(self.storage_override_gb)
      if self.storage_override_gb is not None
      else None,
      "effective_storage_limit_gb": safe_float(self.get_effective_storage_limit()),
      "auto_expand_enabled": self.auto_expand_enabled,
    }

  def get_effective_storage_limit(self) -> Decimal:
    """Get the effective storage limit (override or default)."""
    return (
      self.storage_override_gb
      if self.storage_override_gb is not None
      else self.storage_limit_gb
    )

  def check_storage_limit(self, current_storage_gb: Decimal) -> dict[str, Any]:
    """Check if current storage is within limits."""
    effective_limit = self.get_effective_storage_limit()
    usage_percentage = (
      (current_storage_gb / effective_limit) * 100 if effective_limit > 0 else 0
    )

    return {
      "current_storage_gb": safe_float(current_storage_gb),
      "effective_limit_gb": safe_float(effective_limit),
      "usage_percentage": safe_float(usage_percentage),
      "within_limit": current_storage_gb <= effective_limit,
      "approaching_limit": usage_percentage
      >= safe_float(self.storage_warning_threshold) * 100,
      "needs_warning": usage_percentage
      >= safe_float(self.storage_warning_threshold) * 100
      and (
        self.last_storage_warning_at is None
        or (datetime.now(UTC) - self.last_storage_warning_at).days >= 1
      ),
      "has_override": self.storage_override_gb is not None,
    }

  def set_storage_override(
    self, new_limit_gb: Decimal, admin_user_id: str, reason: str, session: Session
  ) -> None:
    """Set storage override limit (admin only)."""
    old_limit = self.get_effective_storage_limit()
    self.storage_override_gb = new_limit_gb
    self.updated_at = datetime.now(UTC)

    # Record transaction for audit trail
    GraphCreditTransaction.create_transaction(
      graph_credits_id=self.id,
      transaction_type=CreditTransactionType.BONUS,  # Using bonus type for admin actions
      amount=Decimal("0"),  # No credit change
      description=f"Storage limit override: {old_limit}GB → {new_limit_gb}GB",
      metadata={
        "admin_user_id": admin_user_id,
        "reason": reason,
        "old_limit_gb": str(old_limit),
        "new_limit_gb": str(new_limit_gb),
        "action_type": "storage_override",
      },
      session=session,
    )

  def update_storage_warning(self, session: Session) -> None:
    """Update last storage warning timestamp."""
    self.last_storage_warning_at = datetime.now(UTC)
    session.commit()


class GraphCreditTransaction(Base):
  """
  Individual credit transactions for tracking usage.

  Records all credit movements including allocations, consumption, and refunds.
  Includes idempotency support to prevent duplicate transactions.
  """

  __tablename__ = "graph_credit_transactions"

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("txn"))
  graph_credits_id = Column(String, ForeignKey("graph_credits.id"), nullable=False)

  # Direct graph_id reference for easier querying
  graph_id = Column(String, nullable=False)

  # Transaction details
  transaction_type = Column(String, nullable=False)
  amount = Column(
    Numeric(10, 2), nullable=False
  )  # Positive for additions, negative for consumption
  description = Column(String(500), nullable=False)

  # Idempotency support
  idempotency_key = Column(String(255), nullable=True)
  request_id = Column(String(255), nullable=True)

  # Operation tracking
  operation_id = Column(String(255), nullable=True)  # Links related operations
  user_id = Column(String, nullable=True)  # Direct user reference

  # Optional metadata (JSON)
  transaction_metadata = Column("metadata", Text, nullable=True)

  # Tracking
  created_at = Column(
    DateTime(timezone=True), nullable=False, default=datetime.now(UTC)
  )

  # Relationships
  graph_credits = relationship("GraphCredits", back_populates="transactions")

  # Indexes and constraints
  __table_args__ = (
    Index("idx_credit_transactions_graph_credits", graph_credits_id),
    Index("idx_credit_transactions_type", transaction_type),
    Index("idx_credit_transactions_created", created_at),
    Index("idx_credit_transactions_graph_id", graph_id),
    Index("idx_credit_transactions_user_id", user_id),
    Index("idx_credit_transactions_operation_id", operation_id),
    # Unique constraint on idempotency key to prevent duplicates
    Index("idx_credit_transactions_idempotency", idempotency_key, unique=True),
    # Composite index for efficient duplicate checking
    Index(
      "idx_credit_transactions_dedup",
      graph_credits_id,
      transaction_type,
      amount,
      created_at,
      operation_id,
    ),
  )

  def __repr__(self):
    return f"<GraphCreditTransaction(id={self.id}, type={self.transaction_type}, amount={self.amount})>"

  @classmethod
  def create_transaction(
    cls,
    graph_credits_id: str,
    transaction_type: CreditTransactionType,
    amount: Decimal,
    description: str,
    metadata: dict[str, Any] | None = None,
    session: Session | None = None,
    idempotency_key: str | None = None,
    request_id: str | None = None,
    operation_id: str | None = None,
    graph_id: str | None = None,
    user_id: str | None = None,
  ) -> "GraphCreditTransaction":
    """
    Create a new credit transaction with idempotency support.

    Args:
        graph_credits_id: The graph credits record ID
        transaction_type: Type of transaction (allocation, consumption, etc.)
        amount: Transaction amount (negative for consumption)
        description: Human-readable description
        metadata: Optional metadata dictionary
        session: Database session
        idempotency_key: Unique key to prevent duplicate transactions
        request_id: HTTP request ID for tracing
        operation_id: ID to link related operations
        graph_id: Direct graph ID reference
        user_id: Direct user ID reference

    Returns:
        Created transaction or existing transaction if idempotency key exists
    """
    import json

    from sqlalchemy.exc import IntegrityError

    # If idempotency key provided, check for existing transaction
    if idempotency_key and session:
      existing = (
        session.query(cls).filter(cls.idempotency_key == idempotency_key).first()
      )
      if existing:
        logger.info(
          f"Found existing transaction for idempotency key: {idempotency_key}"
        )
        return existing

    # Get graph_id from credits if not provided
    if not graph_id and session:
      credits_record = (
        session.query(GraphCredits).filter(GraphCredits.id == graph_credits_id).first()
      )
      if credits_record:
        graph_id = str(credits_record.graph_id)

    transaction = cls(
      id=generate_prefixed_ulid("txn"),
      graph_credits_id=graph_credits_id,
      graph_id=graph_id or "unknown",
      transaction_type=transaction_type.value,
      amount=amount,
      description=description,
      transaction_metadata=json.dumps(metadata) if metadata else None,
      idempotency_key=idempotency_key,
      request_id=request_id,
      operation_id=operation_id,
      user_id=user_id,
    )

    if session:
      try:
        session.add(transaction)
        session.commit()
      except IntegrityError as e:
        session.rollback()
        # Handle race condition - another request created the transaction
        if idempotency_key and "idx_credit_transactions_idempotency" in str(e):
          existing = (
            session.query(cls).filter(cls.idempotency_key == idempotency_key).first()
          )
          if existing:
            logger.info(
              f"Race condition handled for idempotency key: {idempotency_key}"
            )
            return existing
        raise

    return transaction

  @classmethod
  def get_transactions_for_graph(
    cls,
    graph_credits_id: str,
    transaction_type: CreditTransactionType | None = None,
    limit: int = 100,
    session: Session | None = None,
  ) -> Sequence["GraphCreditTransaction"]:
    """Get transactions for a graph."""
    if not session:
      return []
    query = session.query(cls).filter(cls.graph_credits_id == graph_credits_id)

    if transaction_type:
      query = query.filter(cls.transaction_type == transaction_type.value)

    return query.order_by(cls.created_at.desc()).limit(limit).all()

  def get_metadata(self) -> dict[str, Any]:
    """Parse metadata JSON."""
    if self.transaction_metadata is None:
      return {}

    try:
      import json

      return json.loads(str(self.transaction_metadata or "{}"))
    except Exception:
      return {}

  def set_metadata(self, metadata: dict[str, Any]) -> None:
    """Set metadata as JSON string."""
    try:
      import json

      self.transaction_metadata = json.dumps(metadata) if metadata else None
    except Exception as e:
      logger.error(f"Failed to set metadata: {e}")
      raise
