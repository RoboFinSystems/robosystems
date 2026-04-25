"""Event model — real-world business event layer (event-driven-ledger.md).

Status lifecycle: captured → classified → committed → pending → fulfilled,
with voided and superseded as terminal off-ramps. The triggered_by_event_id
column on transactions and entries links GL rows back to the originating
event for the audit chain.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  BigInteger,
  CheckConstraint,
  Column,
  DateTime,
  Index,
  String,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class Event(ExtensionsBase):
  __tablename__ = "events"
  __table_args__ = (
    Index("idx_events_type", "event_type"),
    Index("idx_events_category", "event_category"),
    Index("idx_events_occurred_at", "occurred_at"),
    Index("idx_events_status", "status"),
    Index("idx_events_agent", "agent_id"),
    Index(
      "idx_events_source_external",
      "source",
      "external_id",
      unique=True,
      postgresql_where="external_id IS NOT NULL",
    ),
    CheckConstraint(
      "status IN ('captured', 'classified', 'committed', 'pending', 'fulfilled', 'voided', 'superseded')",
      name="check_event_status",
    ),
    CheckConstraint(
      "event_category IN ('sales', 'purchase', 'financing', 'payroll', 'treasury', 'adjustment', 'recognition', 'other')",
      name="check_event_category",
    ),
    CheckConstraint(
      "resource_type IN ('goods', 'services', 'money', 'right', 'obligation', 'information', 'labor') OR resource_type IS NULL",
      name="check_event_resource_type",
    ),
  )

  # Identity
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("evt"))

  # Event identity
  event_type = Column(String, nullable=False)
  event_category = Column(String, nullable=False)

  # REA primitives. agent_id FK to agents(id) enforced at the DB level.
  agent_id = Column(String, nullable=True)
  resource_type = Column(String, nullable=True)
  resource_element_id = Column(String, nullable=True)

  # Occurrence
  occurred_at = Column(DateTime, nullable=False)
  effective_at = Column(DateTime, nullable=True)

  # Lifecycle
  status = Column(String, nullable=False, default="captured")

  # Provenance
  source = Column(String, nullable=False)
  external_id = Column(String, nullable=True)
  external_url = Column(String, nullable=True)

  # Correction chain (self-referential FKs — enforced via migration, not ORM relationship)
  replaced_by_event_id = Column(String, nullable=True)
  replaces_event_id = Column(String, nullable=True)

  # Economic value (minor currency units — cents, signed)
  amount = Column(BigInteger, nullable=True)
  currency = Column(String, nullable=False, default="USD")

  # Narrative
  description = Column(String, nullable=True)

  # Event-type-specific payload
  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)

  # Timestamps
  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  created_by = Column(String, nullable=False)

  def __repr__(self) -> str:
    return f"<Event {self.id} {self.event_type} {self.status}>"
