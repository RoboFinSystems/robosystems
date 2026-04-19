"""Classification model — OLTP mirror of the graph `Classification` node.

The graph already has `Classification` in `schemas/base.py`
(properties: identifier, type, source, confidence) attached to
associations via `ASSOCIATION_HAS_CLASSIFICATION`. This table is the
OLTP counterpart, extended with a `category` axis per the
`information-modeling.md` spec.

A Classification is a named tag in a specific category:
`(category='economic_nature', identifier='asset')`,
`(category='concept_arrangement', identifier='RollUp')`, etc. Categories
currently recognized:

- `economic_nature`     — SFAC 6 primitive types (asset, revenue, …)
- `statement_context`   — which report an element belongs to (bs, is, cf, …)
- `derivation_role`     — primitive / subtotal / total / reconciliation / …
- `concept_arrangement` — Charlie's patterns (RollUp, Adjustment, …)
- `member_arrangement`  — Aggregation / Nonaggregation
- `named_disclosure`    — SEC disclosure mechanics catalog

The first three attach to Elements (via `element_classifications`); the
last three attach to Associations (via `association_classifications`,
coming per the spec). The single registry keeps one vocabulary for
both surfaces.

`id` is shared with the graph node so an OLTP row and its graph
counterpart refer to the same classification.
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
      "'economic_nature', 'statement_context', 'derivation_role', "
      "'concept_arrangement', 'member_arrangement', 'named_disclosure'"
      ")",
      name="check_classification_category",
    ),
  )

  # Identity
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("cls"))

  # Axis membership
  category = Column(String, nullable=False)
  identifier = Column(String, nullable=False)

  # Where this classification comes from: 'sfac6' | 'fac' | 'rs-gaap' |
  # 'sec' (SEC disclosure mechanics) | 'system' (built-in) | 'user'.
  type = Column(String, nullable=False, default="system")

  # Display
  name = Column(String, nullable=True)
  description = Column(String, nullable=True)

  # Confidence (optional, for AI-suggested classifications)
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
    return f"<Classification {self.category}:{self.identifier}>"
