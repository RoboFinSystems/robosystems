"""Element model — unified taxonomy element.

Holds Chart of Accounts entries (from QuickBooks, Xero, native), US GAAP
reporting concepts (SFAC 6, rs-gaap), and any future taxonomy elements.
All materialize to Element nodes in the graph via the postgres_scanner →
DuckDB → LadybugDB pipeline.

Only XBRL-intrinsic attributes live on this table (name, qname,
namespace, balance_type, period_type, abstract, monetary, element_type,
substitution_group). Classifications — including SFAC 6 primitive type
(elementsOfFinancialStatements), liquidity, activityType,
operatingNonoperating, flowClassification, and the other 20 FASB
metamodel trait axes — live in ``classifications`` +
``element_classifications``, mirroring XBRL's linkbase model.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  Boolean,
  CheckConstraint,
  Column,
  DateTime,
  ForeignKey,
  Index,
  Integer,
  String,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class Element(ExtensionsBase):
  __tablename__ = "elements"
  __table_args__ = (
    Index("idx_elements_parent", "parent_id"),
    Index("idx_elements_external", "external_id", "external_source"),
    Index(
      "idx_elements_active",
      "is_active",
      postgresql_where="is_active = true",
    ),
    Index("idx_elements_taxonomy", "taxonomy_id"),
    Index("idx_elements_source", "source"),
    Index(
      "idx_elements_qname",
      "qname",
      unique=True,
      postgresql_where="qname IS NOT NULL",
    ),
    Index(
      "idx_elements_namespace",
      "namespace",
      postgresql_where="namespace IS NOT NULL",
    ),
    Index(
      "idx_elements_substitution_group",
      "substitution_group",
      postgresql_where="substitution_group IS NOT NULL",
    ),
    CheckConstraint(
      "balance_type IN ('debit', 'credit')",
      name="check_element_balance_type",
    ),
    CheckConstraint(
      "period_type IN ('duration', 'instant')",
      name="check_element_period_type",
    ),
    CheckConstraint(
      "element_type IN ('concept', 'abstract', 'axis', 'member', 'hypercube')",
      name="check_element_type",
    ),
    CheckConstraint(
      # 'system' is reserved for internal FK-anchor elements created by the
      # taxonomy seed (e.g., struct_balance_sheet) and is intentionally NOT
      # in COA_SOURCES so those rows never appear in the Chart of Accounts.
      "source IN ('fac', 'rs-gaap', 'us-gaap', 'ifrs', "
      "'quickbooks', 'xero', 'plaid', 'native', 'import', 'system')",
      name="check_element_source",
    ),
  )

  # Identity
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("elem"))
  code = Column(String, nullable=True)
  name = Column(String, nullable=False)
  description = Column(String, nullable=True)

  # XBRL Alignment — intrinsic concept declaration attributes only.
  qname = Column(String, nullable=True)
  namespace = Column(String, nullable=True)
  uri = Column(String, nullable=True)
  balance_type = Column(String, nullable=False, default="debit")
  period_type = Column(String, nullable=False, default="duration")
  substitution_group = Column(String, nullable=True)

  # Element Type (XBRL substitution-group derived)
  is_abstract = Column(Boolean, nullable=False, default=False)
  is_monetary = Column(Boolean, nullable=False, default=True)
  element_type = Column(String, nullable=False, default="concept")

  # Hierarchy (parent element — separate from class-subclass classification
  # hierarchy, which lives in associations)
  parent_id = Column(String, ForeignKey("elements.id"), nullable=True)
  depth = Column(Integer, nullable=False, default=0)
  path = Column(String, nullable=False, default="")

  # Taxonomy Membership
  taxonomy_id = Column(String, ForeignKey("taxonomies.id"), nullable=True)
  source = Column(String, nullable=False, default="native")

  # Currency
  currency = Column(String, nullable=False, default="USD")

  # State
  is_active = Column(Boolean, nullable=False, default=True)
  is_placeholder = Column(Boolean, nullable=False, default=False)

  # External mapping (QB, Xero, etc.)
  external_id = Column(String, nullable=True)
  external_source = Column(String, nullable=True)

  # Metadata
  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)
  version = Column(Integer, nullable=False, default=1)

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
    return f"<Element {self.qname or self.code} {self.name}>"


# Backward compatibility alias
Account = Element
