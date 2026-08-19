"""Per-graph credit pools for AI operations.

Only AI operations (Anthropic Claude via Bedrock) consume credits; database
operations are free. Consumption happens *after* the operation, priced off
actual token usage, so a call is never pre-authorized against the pool.
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

from robosystems.config.graph_tier import GraphTier, GraphTierConfig
from robosystems.database import Base
from robosystems.utils.ulid import generate_prefixed_ulid

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
  """A graph's credit pool, refilled monthly from its subscription tier."""

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
    """Tier of the related Graph, defaulting to standard if it is missing."""
    if self.graph:
      return self.graph.graph_tier
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

    graph = Graph.get_by_id(graph_id, session)
    if not graph:
      raise ValueError(f"Graph {graph_id} not found")

    graph_tier = GraphTier(graph.graph_tier)

    # Storage is included in the tier rather than billed, so the backup cap
    # doubles as the storage safety cap.
    backup_limits = GraphTierConfig.get_backup_limits(graph_tier.value)
    storage_limit_gb = backup_limits.get("max_backup_size_gb", 10)

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
    metadata: dict[str, Any] | None = None,
    drain_on_shortfall: bool = False,
  ) -> dict[str, Any]:
    """Atomically deduct credits for a completed operation.

    The deduction and its balance check happen in one conditional UPDATE, so
    concurrent callers cannot drive the pool negative. Returns
    ``{"success": False, "error": "Insufficient credits", ...}`` rather than
    raising when the pool cannot cover ``amount``.

    ``drain_on_shortfall`` selects what happens when the pool is short. By
    default the balance is **left untouched** — the operation is simply
    refused, and any remainder stays available for a cheaper one. Set it only
    on a **post-hoc** charge (an AI call already paid for by the time this
    runs), where instead the pool is **drained to zero**: what is there is
    debited, the shortfall is recorded, and the result still reports
    ``success: False`` so the caller stops. Draining there stops the same
    request re-entering the band above the pre-flight estimate and below one
    call's true cost, billed nothing; draining a *pre*-check would wrongly
    destroy credits the user holds.

    ``metadata`` (token counts and the like) is merged into the transaction
    record; the built-in keys win on collision.
    """
    from sqlalchemy import text

    from robosystems.utils import generate_prefixed_ulid

    transaction_id = generate_prefixed_ulid("tx")

    try:
      actual_cost = amount

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
        if drain_on_shortfall:
          return self._drain_for_shortfall(
            actual_cost=actual_cost,
            operation_type=operation_type,
            operation_description=operation_description,
            session=session,
            request_id=request_id,
            user_id=user_id,
            metadata=metadata,
            transaction_id=transaction_id,
          )
        # Pre-check semantics: refuse, leave the balance for a cheaper op.
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

      GraphCreditTransaction.create_transaction(
        graph_credits_id=self.id,
        transaction_type=CreditTransactionType.CONSUMPTION,
        amount=-actual_cost,
        description=operation_description,
        metadata={
          **(metadata or {}),
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

  def _drain_for_shortfall(
    self,
    *,
    actual_cost: Decimal,
    operation_type: str,
    operation_description: str,
    session: Session,
    request_id: str | None,
    user_id: str | None,
    metadata: dict[str, Any] | None,
    transaction_id: str,
  ) -> dict[str, Any]:
    """The insufficient-balance branch of :meth:`consume_credits_atomic`.

    Debits whatever the pool still holds (to exactly zero) and records the
    shortfall, so a completed call is never billed nothing and the next
    pre-flight — which reads the now-empty balance — denies. A pool already
    at zero records nothing. The ``FOR UPDATE`` read and the conditional
    ``UPDATE`` run as one statement so a concurrent drain cannot double-take.
    """
    from sqlalchemy import text

    drain = session.execute(
      text("""
        WITH prev AS (
          SELECT current_balance AS old_balance
          FROM graph_credits
          WHERE id = :credits_id
          FOR UPDATE
        )
        UPDATE graph_credits
        SET current_balance = 0,
            updated_at = :updated_at
        FROM prev
        WHERE graph_credits.id = :credits_id
          AND prev.old_balance > 0
          AND prev.old_balance < :actual_cost
        RETURNING prev.old_balance AS drained
      """),
      {
        "actual_cost": actual_cost,
        "updated_at": datetime.now(UTC),
        "credits_id": self.id,
      },
    )
    drained_row = drain.fetchone()

    if not drained_row:
      # Nothing to take: the pool was already empty (or a concurrent caller
      # drained it first). Report the balance as it stands.
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
        "credits_consumed": 0.0,
        "shortfall": float(actual_cost) - available_balance,
      }

    drained = Decimal(str(drained_row.drained))
    shortfall = actual_cost - drained

    GraphCreditTransaction.create_transaction(
      graph_credits_id=self.id,
      transaction_type=CreditTransactionType.CONSUMPTION,
      amount=-drained,
      description=operation_description,
      metadata={
        **(metadata or {}),
        "operation_type": operation_type,
        "base_cost": str(actual_cost),
        "transaction_id": transaction_id,
        "drained_to_zero": True,
        "shortfall": str(shortfall),
      },
      session=session,
      idempotency_key=f"consume_{transaction_id}",
      request_id=request_id,
      operation_id=transaction_id,
      graph_id=self.graph_id,
      user_id=user_id or self.user_id,
    )

    self.current_balance = Decimal("0")
    self.updated_at = datetime.now(UTC)

    session.commit()

    logger.warning(
      f"Credit pool for graph {self.graph_id} drained to zero: "
      f"call cost {actual_cost}, billed {drained}, shortfall {shortfall}"
    )

    return {
      "success": False,
      "error": "Insufficient credits",
      "required_credits": float(actual_cost),
      "available_credits": 0.0,
      "credits_consumed": float(drained),
      "shortfall": float(shortfall),
      "drained_to_zero": True,
      "base_cost": float(actual_cost),
      "transaction_id": transaction_id,
    }

  def _expire_unspent(
    self,
    session: Session,
    *,
    description: str,
    metadata: dict[str, Any],
    idempotency_key: str | None,
  ) -> Decimal:
    """Record the forfeiture of the current balance before a reset.

    A reset discards whatever is left in the pool; without this row the
    ledger stops footing against the balance (`SUM(transactions)` drifts by
    every discarded remainder, permanently). Returns the forfeited amount.
    """
    remainder = Decimal(str(self.current_balance or 0))
    if remainder <= 0:
      return Decimal("0")

    GraphCreditTransaction.create_transaction(
      graph_credits_id=self.id,
      transaction_type=CreditTransactionType.EXPIRATION,
      amount=-remainder,
      description=description,
      metadata=metadata,
      session=session,
      idempotency_key=idempotency_key,
      graph_id=self.graph_id,
      user_id=self.user_id,
    )
    return remainder

  def allocate_monthly_credits(self, session: Session) -> bool:
    """Allocate monthly credits if due. Credits do not roll over.

    The balance is *replaced* by ``monthly_allocation``, never added to — the
    offering page promises no rollover (``routers/offering.py``) and this table
    has no ``rollover_credits`` column. Accumulating instead would let
    ``current_balance`` drift away from the ``monthly_allocation -
    consumed_this_month`` figure the read paths report, so the same pool would
    answer differently depending on which side you asked.

    The discarded remainder — unspent allocation and unspent bonus credits
    alike — is recorded as an EXPIRATION transaction so the ledger keeps
    footing against the balance through every reset.
    """
    now = datetime.now(UTC)

    # One allocation per calendar month, matching both the monthly cron that
    # drives bulk allocation and the transaction idempotency key below. A
    # day-count gate here would refuse the 1-Mar run for anything allocated on
    # 1-Feb (28 days elapsed), silently skipping March fleet-wide.
    if self.last_allocation_date is not None:
      last = self.last_allocation_date
      if (last.year, last.month) == (now.year, now.month):
        return False

    allocation_month = now.strftime("%Y-%m")

    self._expire_unspent(
      session,
      description="Unspent credits forfeited at monthly reset (no rollover)",
      metadata={
        "allocation_month": allocation_month,
        "expiration_type": "monthly_reset",
      },
      idempotency_key=f"monthly_expiration_{self.graph_id}_{allocation_month}",
    )

    self.current_balance = self.monthly_allocation
    self.last_allocation_date = now
    self.updated_at = now

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
      idempotency_key=f"monthly_allocation_{self.graph_id}_{allocation_month}",
      graph_id=self.graph_id,
      user_id=self.user_id,
    )

    return True

  def reset_pool(
    self,
    session: Session,
    *,
    initiated_by: str,
    reason: str | None = None,
  ) -> Decimal:
    """Admin-initiated mid-month reset: forfeit the remainder, refill to the
    monthly allocation, both recorded in the ledger.

    Does not touch `last_allocation_date` — this is not a scheduled monthly
    allocation, and the calendar-month refill gate must still fire normally
    when the month turns. Returns the forfeited amount.
    """
    description = reason or "Credit pool reset by administrator"
    forfeited = self._expire_unspent(
      session,
      description=description,
      metadata={"expiration_type": "manual_reset", "initiated_by": initiated_by},
      idempotency_key=None,
    )

    self.current_balance = self.monthly_allocation
    self.updated_at = datetime.now(UTC)

    GraphCreditTransaction.create_transaction(
      graph_credits_id=self.id,
      transaction_type=CreditTransactionType.ALLOCATION,
      amount=self.monthly_allocation,
      description=description,
      metadata={"allocation_type": "manual_reset", "initiated_by": initiated_by},
      session=session,
      idempotency_key=None,
      graph_id=self.graph_id,
      user_id=self.user_id,
    )

    return forfeited

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

    # Report `current_balance` verbatim: it is the column the atomic consume
    # path decrements and gates on, so it is the only balance a caller can
    # actually spend. Recomputing it from allocation minus consumption would
    # disagree with the real column.
    return {
      "graph_id": self.graph_id,
      "graph_tier": self.graph_tier,
      "current_balance": safe_float(self.current_balance),
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

    # Zero-amount BONUS row: carries the admin action into the audit trail
    # without moving the balance.
    GraphCreditTransaction.create_transaction(
      graph_credits_id=self.id,
      transaction_type=CreditTransactionType.BONUS,
      amount=Decimal("0"),
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
  """Append-only ledger of credit movements for a graph's pool.

  ``idempotency_key`` is uniquely indexed, so a retried allocation or
  consumption resolves to the existing row instead of double-posting.
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
    DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
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
    """Post a credit transaction; ``amount`` is negative for consumption.

    When ``idempotency_key`` is supplied, an existing row with that key is
    returned untouched — both on the up-front lookup and on the unique-index
    violation that a concurrent writer can raise.
    """
    import json

    from sqlalchemy.exc import IntegrityError

    if idempotency_key and session:
      existing = (
        session.query(cls).filter(cls.idempotency_key == idempotency_key).first()
      )
      if existing:
        logger.info(
          f"Found existing transaction for idempotency key: {idempotency_key}"
        )
        return existing

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
