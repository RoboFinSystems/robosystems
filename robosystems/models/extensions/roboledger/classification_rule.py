"""ClassificationRule model — AI-informed transaction classification.

Rules that automatically map bank feed transactions to elements.
Inspired by Modern Treasury's Ledger Event Handlers and SoftLedger's
Bank Rules.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  BigInteger,
  Boolean,
  Column,
  DateTime,
  Float,
  ForeignKey,
  Index,
  Integer,
  String,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class ClassificationRule(ExtensionsBase):
  __tablename__ = "classification_rules"
  __table_args__ = (
    Index(
      "idx_rules_active",
      "is_active",
      "priority",
      postgresql_where="is_active = true",
    ),
    Index("idx_rules_source", "match_source"),
  )

  # Identity
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("rule"))
  name = Column(String, nullable=False)
  description = Column(String, nullable=True)

  # Matching criteria (all AND-joined; null = don't filter)
  match_source = Column(String, nullable=True)
  match_type = Column(String, nullable=True)
  match_merchant = Column(String, nullable=True)
  match_category = Column(String, nullable=True)
  match_min_amount = Column(BigInteger, nullable=True)
  match_max_amount = Column(BigInteger, nullable=True)

  # Result
  target_element_id = Column(String, ForeignKey("elements.id"), nullable=False)

  # Entry template
  entry_type = Column(String, nullable=False, default="standard")
  debit_element_id = Column(String, ForeignKey("elements.id"), nullable=False)
  credit_element_id = Column(String, ForeignKey("elements.id"), nullable=False)

  # Dimension assignments
  dimensions = Column(JSONB, nullable=False, default=list)

  # AI provenance
  suggested_by = Column(String, nullable=True)
  confidence = Column(Float, nullable=True)
  approved_by = Column(String, nullable=True)
  approved_at = Column(DateTime, nullable=True)

  # State
  is_active = Column(Boolean, nullable=False, default=True)
  priority = Column(Integer, nullable=False, default=0)

  # Metadata
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
    return f"<ClassificationRule {self.name}>"
