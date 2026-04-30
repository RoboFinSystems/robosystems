"""Agent model — counterparties (customers, vendors, employees, etc.).

REA-aligned: Agent is the counterparty in a business event. Distinct from
Entity (the reporting entity / graph owner). Agents are the external actors
events are exchanged with. ``events.agent_id`` carries a FK to this table.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  Boolean,
  CheckConstraint,
  Column,
  DateTime,
  Index,
  String,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class Agent(ExtensionsBase):
  __tablename__ = "agents"
  __table_args__ = (
    Index("idx_agents_type", "agent_type"),
    Index(
      "idx_agents_source_external",
      "source",
      "connection_id",
      "external_id",
      unique=True,
      postgresql_where="external_id IS NOT NULL AND connection_id IS NOT NULL",
    ),
    CheckConstraint(
      "agent_type IN ('customer', 'vendor', 'employee', 'owner', "
      "'supplier', 'government', 'lender', 'self', 'other')",
      name="check_agent_type",
    ),
  )

  # Identity — ULID prefix "agt"
  id = Column(
    String,
    primary_key=True,
    default=lambda: generate_prefixed_ulid("agt"),
  )
  agent_type = Column(String, nullable=False)
  name = Column(String, nullable=False)
  legal_name = Column(String, nullable=True)

  # Economic identifiers
  tax_id = Column(String, nullable=True)
  registration_number = Column(String, nullable=True)
  duns = Column(String, nullable=True)
  lei = Column(String, nullable=True)

  # Contact
  email = Column(String, nullable=True)
  phone = Column(String, nullable=True)
  address = Column(JSONB, nullable=True)

  # Source system linkage — (source, external_id) is the dedup key
  source = Column(String, nullable=False, default="native")
  external_id = Column(String, nullable=True)
  # Scopes agents to a connection so two QB connections on one graph don't share agents.
  # Nullable for native / library-origin agents that don't come from a connector.
  connection_id = Column(String, nullable=True)

  # State
  is_active = Column(Boolean, nullable=False, default=True)
  is_1099_recipient = Column(Boolean, nullable=False, default=False)

  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)

  # Timestamps
  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  updated_at = Column(
    DateTime,
    nullable=False,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
  )
  created_by = Column(String, nullable=False)

  def __repr__(self) -> str:
    return f"<Agent {self.id} {self.agent_type} {self.name!r}>"
