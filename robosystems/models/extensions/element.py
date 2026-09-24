"""Element model: chart-of-accounts entries and reporting concepts alike.

Only XBRL-intrinsic attributes live here. Classifications (SFAC 6 type,
liquidity, the FASB trait axes) live in ``classifications`` +
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

# `elements.source` vocabulary: the single source for the CHECK and for the
# tenant-provisioning widen step (`db.extensions._widen_library_checks`).
#
# 'system': internal FK-anchor elements from the taxonomy seed; not in
#   COA_SOURCES, so they never appear in the chart of accounts.
# 'disclosures' / 'checklist' / 'styles': rs-gaap extension packages.
# 'cm': Conceptual Model posting-role concepts (cm:Debit/cm:Credit).
# 'rs-metric' / 'rs-driver': the metric and forecast-lever catalogs.
# 'linked': arrived with a report shared from another graph; not in
#   COA_SOURCES, since the sender's accounts are not the recipient's chart.
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
    # Scopes the OLTPLoader's per-connection delete during re-sync.
    Index(
      "idx_elements_external_source_connection",
      "external_source",
      "connection_id",
      postgresql_where="connection_id IS NOT NULL",
    ),
    # OLTPLoader UPSERT key: keeps `elem_*` ids stable across re-syncs, since
    # associations point at them. Library elements (no connection) are exempt.
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

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("elem"))
  code = Column(String, nullable=True)
  name = Column(String, nullable=False)
  description = Column(String, nullable=True)

  qname = Column(String, nullable=True)
  namespace = Column(String, nullable=True)
  uri = Column(String, nullable=True)
  balance_type = Column(String, nullable=False, default="debit")
  period_type = Column(String, nullable=False, default="duration")
  substitution_group = Column(String, nullable=True)

  is_abstract = Column(Boolean, nullable=False, default=False)
  is_monetary = Column(Boolean, nullable=False, default=True)
  element_type = Column(String, nullable=False, default="concept")
  # Value domain, orthogonal to element_type (the structural role). Open
  # vocabulary: monetary | string | date | boolean | shares | decimal |
  # integer | text_block | ratio | percent | multiple | days. NULL falls back
  # to is_monetary.
  item_type = Column(String, nullable=True)

  # Parent element; classification hierarchy lives in associations instead.
  parent_id = Column(String, ForeignKey("elements.id"), nullable=True)
  depth = Column(Integer, nullable=False, default=0)
  path = Column(String, nullable=False, default="")

  taxonomy_id = Column(String, ForeignKey("taxonomies.id"), nullable=True)
  source = Column(String, nullable=False, default="native")

  currency = Column(String, nullable=False, default="USD")

  is_active = Column(Boolean, nullable=False, default=True)
  is_placeholder = Column(Boolean, nullable=False, default=False)

  external_id = Column(String, nullable=True)
  external_source = Column(String, nullable=True)
  # Segregates elements per connection so re-syncing one QB book cannot stomp
  # another's in the same graph. NULL for library and native elements.
  connection_id = Column(String, nullable=True)

  # agent_id: shared by elements denoting the same canonical concept across
  # tenants. aliases: alternate spellings and qname variants of that concept.
  # embedding: 1024-dim sentence embedding for mapping similarity lookups.
  agent_id = Column(String, nullable=True)
  aliases = Column(ARRAY(String), nullable=False, default=list)
  embedding = Column(ARRAY(Float), nullable=True)

  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)
  version = Column(Integer, nullable=False, default=1)

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
