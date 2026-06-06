"""Trait — FASB us-gaap metamodel vocabulary members.

A Trait is a named member of a controlled classification vocabulary axis:
``(category='elementsOfFinancialStatements', identifier='asset')``,
``(category='liquidity', identifier='current')``,
``(category='flowClassification', identifier='inflow')``, etc.

The 25 categories cover the 24 orthogonal FASB metamodel trait axes plus
``flowClassification`` (derived from FASB instant-* arcroles). Element
assignments live in ``element_traits`` via :class:`ElementTrait`.

This is distinct from :class:`Classification` (association-side), which
covers structural patterns (``concept_arrangement``, ``member_arrangement``,
``named_disclosure``) applied to Associations, not Elements.

``id`` is shared with the graph ``Trait`` node so OLTP rows and graph
counterparts refer to the same trait.
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
      # RS analytical extension — earnings persistence (normalized earnings / NOPAT)
      "'recurrence'"
      ")",
      name="check_trait_category",
    ),
  )

  # Identity
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("trt"))

  # Axis membership
  category = Column(String, nullable=False)
  identifier = Column(String, nullable=False)

  # Where this trait comes from: 'fac-traits' | 'fac' |
  # 'rs-gaap' | 'system' (built-in) | 'user'.
  type = Column(String, nullable=False, default="system")

  # Display
  name = Column(String, nullable=True)
  description = Column(String, nullable=True)

  # Confidence (optional, for AI-suggested traits)
  confidence = Column(Float, nullable=True)

  # Source provenance (optional free-form: URL, spec section, adapter name)
  source = Column(String, nullable=True)

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
  created_by = Column(String, nullable=False, default="system")

  def __repr__(self) -> str:
    return f"<Trait {self.category}:{self.identifier}>"
