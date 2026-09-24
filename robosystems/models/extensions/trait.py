"""Trait: a member of a controlled element-classification axis, e.g.
``(category='liquidity', identifier='current')``.

Element assignments live in ``element_traits``. Structural patterns applied
to associations are :class:`Classification` instead. ``id`` is shared with
the graph ``Trait`` node.
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


class Trait(ExtensionsBase):
  __tablename__ = "traits"
  __table_args__ = (
    UniqueConstraint(
      "category",
      "identifier",
      "type",
      name="uq_trait_category_identifier_type",
    ),
    Index("idx_traits_category", "category"),
    Index("idx_traits_type", "type"),
    CheckConstraint(
      "category IN ("
      # FASB us-gaap metamodel trait axes (24)
      "'elementsOfFinancialStatements', 'liquidity', 'activityType', "
      "'operatingNonoperating', 'operatingIntent', 'realizationStatus', "
      "'recordedValue', 'restriction', 'hedging', 'leaseType', "
      "'interestRateType', 'convertibility', 'creditStructure', "
      "'debtGuarantee', 'derivativeContract', 'derivativeInstrument', "
      "'accrualOrPayable', 'priority', 'estimatedFutureActivity', "
      "'statisticalMeasurement', 'taxComponents', 'threshold', 'use', "
      "'indirectCashFlowReconcilingItem', "
      # Flow classification (FASB instant-* arcroles)
      "'flowClassification', "
      # RS extension: earnings persistence
      "'recurrence'"
      ")",
      name="check_trait_category",
    ),
  )
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("trt"))

  category = Column(String, nullable=False)
  identifier = Column(String, nullable=False)

  # 'fac-traits' | 'fac' | 'rs-gaap' | 'system' | 'user'
  type = Column(String, nullable=False, default="system")
  name = Column(String, nullable=True)
  description = Column(String, nullable=True)

  # For AI-suggested traits.
  confidence = Column(Float, nullable=True)

  # Free-form: URL, spec section, adapter name.
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
    return f"<Trait {self.category}:{self.identifier}>"
