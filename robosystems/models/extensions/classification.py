"""Structural pattern classifications attached to associations via
`association_classifications`: `concept_arrangement`, `member_arrangement`,
and `named_disclosure` (the SEC disclosure mechanics catalog).

Element-level traits are :class:`Trait` instead. `id` is shared with the
graph `Classification` node.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  CheckConstraint,
  Column,
  DateTime,
  Float,
  Index,
  String,
  UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class Classification(ExtensionsBase):
  __tablename__ = "classifications"
  __table_args__ = (
    UniqueConstraint(
      "category",
      "identifier",
      "type",
      name="uq_classification_category_identifier_type",
    ),
    Index("idx_classifications_category", "category"),
    Index("idx_classifications_type", "type"),
    CheckConstraint(
      "category IN ("
      # Association-level structural pattern categories
      "'concept_arrangement', 'member_arrangement', 'named_disclosure'"
      ")",
      name="check_classification_category",
    ),
  )
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("cls"))

  # Axis membership
  category = Column(String, nullable=False)
  identifier = Column(String, nullable=False)

  # Where this classification comes from: 'fac' | 'sec' (SEC disclosure
  # mechanics) | 'system' (built-in) | 'user'.
  type = Column(String, nullable=False, default="system")
  name = Column(String, nullable=True)
  description = Column(String, nullable=True)

  # Confidence (optional, for AI-suggested classifications)
  confidence = Column(Float, nullable=True)

  # Source provenance (optional free-form: URL, spec section, adapter name)
  source = Column(String, nullable=True)
  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)
  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  updated_at = Column(
    DateTime,
    nullable=False,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
  )
  created_by = Column(String, nullable=False, default="system")

  def __repr__(self) -> str:
    return f"<Classification {self.category}:{self.identifier}>"
