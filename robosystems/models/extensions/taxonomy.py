"""Taxonomy model — base ontology concept.

Lives at the extensions top level because taxonomies are universal across
the ontology (any extension can adopt a taxonomy via EntityTaxonomy).
Historically lived under roboledger because roboledger was the first
consumer.

Defines named taxonomy collections (Chart of Accounts, US GAAP Reporting,
CoA→GAAP Mapping, entity extensions). Tenant-scoped — per-graph tables
live in each graph's schema; shared taxonomies live in the public schema.

Supports an extension chain via parent_taxonomy_id: a taxonomy can extend
a parent (version upgrade us-gaap-2024 → us-gaap-2023, entity extension,
industry overlay). This is distinct from source_taxonomy_id /
target_taxonomy_id, which encode mapping relationships between chart of
accounts and reporting taxonomies.

NOTE: parent_taxonomy_id is a scalar FK by design — a taxonomy has
exactly one parent in its extension chain. A taxonomy can't simultaneously
"extend" us-gaap-2024 AS a version upgrade AND myindustry-overlay AS an
industry overlay via this field. If multi-parent extension semantics are
needed in the future, model the secondary relationship as a mapping
taxonomy via source_taxonomy_id / target_taxonomy_id, or escalate to a
dedicated taxonomy_extensions join table — but only once there's a
concrete use case requiring it.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  Boolean,
  CheckConstraint,
  Column,
  Date,
  DateTime,
  ForeignKey,
  Index,
  String,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class Taxonomy(ExtensionsBase):
  __tablename__ = "taxonomies"
  __table_args__ = (
    Index("idx_taxonomies_type", "taxonomy_type"),
    Index(
      "idx_taxonomies_standard",
      "standard",
      postgresql_where="standard IS NOT NULL",
    ),
    Index(
      "idx_taxonomies_parent",
      "parent_taxonomy_id",
      postgresql_where="parent_taxonomy_id IS NOT NULL",
    ),
    CheckConstraint(
      "taxonomy_type IN ('chart_of_accounts', 'reporting', 'mapping', 'schedule')",
      name="check_taxonomy_type",
    ),
    CheckConstraint(
      "extension_type IS NULL OR extension_type IN "
      "('version', 'entity_extension', 'industry', 'jurisdiction')",
      name="check_taxonomy_extension_type",
    ),
    # No schema= specified — tenant table, created per-graph by provision_tenant_schema.
    # Shared taxonomies (US GAAP, SFAC 6) live in the public schema copy, visible
    # to all tenants via search_path = '{graph_id}, public'.
    # Tenant-specific taxonomies (CoA, mappings) live in the tenant schema.
  )

  # Identity
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("tax"))
  name = Column(String, nullable=False)
  description = Column(String, nullable=True)

  # Type
  taxonomy_type = Column(String, nullable=False)
  version = Column(String, nullable=True)

  # Standard taxonomy reference
  standard = Column(String, nullable=True)
  namespace_uri = Column(String, nullable=True)

  # Scope
  is_shared = Column(Boolean, nullable=False, default=False)

  # For mapping taxonomies (CoA → GAAP etc.)
  source_taxonomy_id = Column(
    String, ForeignKey("taxonomies.id", use_alter=True), nullable=True
  )
  target_taxonomy_id = Column(
    String, ForeignKey("taxonomies.id", use_alter=True), nullable=True
  )

  # Extension chain (TAXONOMY_EXTENDS_TAXONOMY in graph) — version upgrades,
  # entity extensions, industry overlays. Distinct from mapping relationships.
  parent_taxonomy_id = Column(
    String, ForeignKey("taxonomies.id", use_alter=True), nullable=True
  )
  extension_type = Column(
    String, nullable=True
  )  # version | entity_extension | industry | jurisdiction
  effective_date = Column(Date, nullable=True)

  # State
  is_active = Column(Boolean, nullable=False, default=True)
  is_locked = Column(Boolean, nullable=False, default=False)

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
    return f"<Taxonomy {self.name} ({self.taxonomy_type})>"
