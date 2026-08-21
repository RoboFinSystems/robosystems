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
``element_traits``, mirroring XBRL's traitConcept linkbase model.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  Boolean,
  CheckConstraint,
  Column,
  DateTime,
  Float,
  ForeignKey,
  Index,
  Integer,
  String,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid

# The `elements.source` vocabulary — the single source for the model CHECK and
# for the tenant-provisioning widen step (`db.extensions._widen_library_checks`).
# Both read this; neither may keep its own copy.
#
# 'system' is reserved for internal FK-anchor elements created by the taxonomy
# seed (e.g., struct_balance_sheet) and is intentionally NOT in COA_SOURCES so
# those rows never appear in the Chart of Accounts.
# 'disclosures' / 'checklist' / 'styles' — rs-gaap framework extension packages
# anchored to sibling namespaces of rs-gaap.
# 'cm' — Conceptual Model posting-role concepts (cm:Debit/cm:Credit),
# tenant-copied with the default pin so schedule has-part arcs resolve.
# 'rs-metric' — the metric catalog package (seeded at 0002 on fresh databases,
# backfilled by 0022 on existing ones).
# 'rs-driver' — the forecast lever catalog package (seeded at 0024).
# 'linked' — a concept that arrived with a report shared from another graph.
# Deliberately NOT in COA_SOURCES: the sender's reporting extension has to
# exist here for their facts to mean anything, but their revenue accounts are
# not the recipient's chart of accounts. Mirrors Entity.source='linked'.
ELEMENT_SOURCE_VALUES: tuple[str, ...] = (
  "fac",
  "rs-gaap",
  "us-gaap",
  "ifrs",
  "quickbooks",
  "xero",
  "plaid",
  "native",
  "import",
  "system",
  "disclosures",
  "checklist",
  "styles",
  "rs-metric",
  "rs-driver",
  "cm",
  "linked",
)


class Element(ExtensionsBase):
  __tablename__ = "elements"
  __table_args__ = (
    Index("idx_elements_parent", "parent_id"),
    Index("idx_elements_external", "external_id", "external_source"),
    # Scopes the OLTPLoader's per-connection delete during re-sync — without
    # this the DELETE is a seq scan over the elements table.
    Index(
      "idx_elements_external_source_connection",
      "external_source",
      "connection_id",
      postgresql_where="connection_id IS NOT NULL",
    ),
    # UPSERT key for the OLTPLoader: lookup-then-update on re-sync keeps
    # `elem_*` ULIDs stable across syncs (downstream Associations point at
    # these IDs). Partial — library-origin elements have connection_id NULL
    # and aren't subject to this constraint.
    Index(
      "idx_elements_upsert_key",
      "external_source",
      "connection_id",
      "external_id",
      unique=True,
      postgresql_where="external_id IS NOT NULL AND connection_id IS NOT NULL",
    ),
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
    Index(
      "idx_elements_agent_id",
      "agent_id",
      postgresql_where="agent_id IS NOT NULL",
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
      "source IN (" + ", ".join(f"'{v}'" for v in ELEMENT_SOURCE_VALUES) + ")",
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
  # Value domain (orthogonal to element_type, which is the structural role).
  # Open XBRL item-type vocabulary — intended values: monetary | string |
  # date | boolean | shares | decimal | integer | text_block, plus the
  # metric format families ratio | percent | multiple | days. NULL means
  # untyped; consumers fall back to is_monetary.
  item_type = Column(String, nullable=True)

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
  # Connection scope. Multi-connection graphs (two QB books in one
  # roboledger graph, e.g. parent + sub) need elements segregated per
  # connection — without this, a re-sync of one connection's CoA would
  # delete or stomp the other connection's elements. Library-origin
  # elements (rs-gaap, us-gaap, FAC) and tenant-native elements have
  # connection_id NULL.
  connection_id = Column(String, nullable=True)

  # Cross-tenant canonical concept linkage via ``agent_id``: elements
  # pointing at the same canonical concept share the value. ``aliases``
  # carries alternate spellings and qname variants (e.g. us-gaap-2024 vs
  # us-gaap-2020) that agents should treat as the same concept.
  # ``embedding`` holds a 1024-dimensional sentence embedding the
  # MappingOperator + classifier use for similarity lookups; nullable
  # because the seed content lands incrementally.
  agent_id = Column(String, nullable=True)
  aliases = Column(ARRAY(String), nullable=False, default=list)
  embedding = Column(ARRAY(Float), nullable=True)

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


# ``Account`` is the chart-of-accounts name for the same table; Element is the
# ontology name. Both are exported.
Account = Element
