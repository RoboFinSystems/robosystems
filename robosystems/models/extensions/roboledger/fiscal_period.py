"""FiscalPeriod model — per-tenant fiscal period records.

Tenant-scoped: lives in each `kg*` schema, not in public. Used for
period close controls (prevent posting to closed periods). The `graph_id`
column is retained as a defensive discriminator but the primary tenant
isolation comes from the schema boundary.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  CheckConstraint,
  Column,
  Date,
  DateTime,
  Index,
  String,
  UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class FiscalPeriod(ExtensionsBase):
  __tablename__ = "fiscal_periods"
  __table_args__ = (
    UniqueConstraint("graph_id", "name", name="uq_fiscal_period_graph_name"),
    Index("idx_fiscal_periods_graph", "graph_id"),
    Index("idx_fiscal_periods_dates", "graph_id", "start_date", "end_date"),
    CheckConstraint(
      "status IN ('open', 'closing', 'closed')",
      name="check_fiscal_period_status",
    ),
    CheckConstraint(
      "start_date < end_date",
      name="check_fiscal_period_dates",
    ),
  )

  # Identity
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("fp"))
  graph_id = Column(String, nullable=False)
  name = Column(String, nullable=False)

  # Period
  start_date = Column(Date, nullable=False)
  end_date = Column(Date, nullable=False)
  period_type = Column(String, nullable=False)

  # State
  status = Column(String, nullable=False, default="open")
  closed_at = Column(DateTime, nullable=True)
  closed_by = Column(String, nullable=True)

  # The close receipt, stamped in the same transaction as the status flip
  # above. Without it the close result exists only in the HTTP response,
  # so a transport failure on a close that SUCCEEDED leaves the operator
  # reconstructing what happened from four separate state reads. Written
  # by PeriodCloseService.close(); read back by get-period-close-status
  # and the fiscal-calendar response. Nullable because periods closed
  # before this shipped (and those seeded closed by initialize_ledger)
  # have no receipt.
  close_receipt = Column(JSONB, nullable=True)

  # Timestamps
  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  updated_at = Column(
    DateTime,
    nullable=False,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
  )

  def __repr__(self) -> str:
    return f"<FiscalPeriod {self.graph_id} {self.name} {self.status}>"
