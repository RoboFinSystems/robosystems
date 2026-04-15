"""Element model — unified taxonomy element.

Holds Chart of Accounts entries (from QuickBooks, Xero, native), US GAAP
reporting concepts (SFAC 6, us-gaap), and any future taxonomy elements.
All materialize to Element nodes in the graph via the postgres_scanner →
DuckDB → LadybugDB pipeline.
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
    Index("idx_elements_classification", "classification"),
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
    CheckConstraint(
      "classification IN ('asset', 'liability', 'equity', 'revenue', 'expense')",
      name="check_element_classification",
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
      "source IN ('sfac6', 'us-gaap', 'ifrs', 'quickbooks', 'xero', "
      "'plaid', 'native', 'import', 'system')",
      name="check_element_source",
    ),
  )

  # Identity
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("elem"))
  code = Column(String, nullable=True)
  name = Column(String, nullable=False)
  description = Column(String, nullable=True)

  # XBRL Alignment
  qname = Column(String, nullable=True)
  namespace = Column(String, nullable=True)
  uri = Column(String, nullable=True)

  # Classification
  classification = Column(String, nullable=False)
  sub_classification = Column(String, nullable=True)
  balance_type = Column(String, nullable=False, default="debit")
  period_type = Column(String, nullable=False, default="duration")

  # Element Type
  is_abstract = Column(Boolean, nullable=False, default=False)
  is_monetary = Column(Boolean, nullable=False, default=True)
  element_type = Column(String, nullable=False, default="concept")

  # Hierarchy
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
