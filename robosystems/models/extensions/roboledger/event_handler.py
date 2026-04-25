"""EventHandler model — dynamic rule registry for event → transaction transformation.

Each row declares which events it handles (via match criteria) and how to
produce GL transactions (via the ``transaction_template`` DSL). The handler
engine (``operations/event_block/registry.py`` + ``engine.py``) evaluates
these rows when ``create-event-block`` fires with ``apply_handlers=True``.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  Boolean,
  CheckConstraint,
  Column,
  DateTime,
  Float,
  Index,
  Integer,
  String,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class EventHandler(ExtensionsBase):
  __tablename__ = "event_handlers"
  __table_args__ = (
    Index("idx_handlers_event_type", "event_type", "is_active"),
    Index("idx_handlers_priority", "priority"),
    Index(
      "idx_handlers_unapproved",
      "approved_by",
      postgresql_where="approved_by IS NULL AND suggested_by = 'ai'",
    ),
    CheckConstraint(
      "origin IN ('hub', 'tenant')",
      name="check_handler_origin",
    ),
  )

  # Identity — ULID prefix "hdl"
  id = Column(
    String,
    primary_key=True,
    default=lambda: generate_prefixed_ulid("hdl"),
  )
  name = Column(String, nullable=False)
  description = Column(String, nullable=True)

  # Match criteria (all AND-joined; null = match anything)
  event_type = Column(String, nullable=False)
  event_category = Column(String, nullable=True)
  match_source = Column(String, nullable=True)
  match_agent_type = Column(String, nullable=True)
  match_resource_type = Column(String, nullable=True)
  # Simple JSONPath-like equality map against event.metadata.
  # Accepts both {"foo": "bar"} and {"metadata.foo": "bar"}.
  match_metadata_expression = Column(JSONB, nullable=True)

  # DSL — shape: {"transactions": [{"entry_template": {"debit": {...}, "credit": {...}}}]}
  transaction_template = Column(JSONB, nullable=False)

  # Priority + lifecycle
  priority = Column(Integer, nullable=False, default=0)
  is_active = Column(Boolean, nullable=False, default=True)
  # 'hub' = platform-seeded; 'tenant' = customer-defined
  origin = Column(String, nullable=False, default="tenant")

  # AI provenance — same pattern as MappingAssociation
  suggested_by = Column(String, nullable=True)
  confidence = Column(Float, nullable=True)
  approved_by = Column(String, nullable=True)
  approved_at = Column(DateTime, nullable=True)

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
    return f"<EventHandler {self.id} {self.event_type} {self.name!r}>"
