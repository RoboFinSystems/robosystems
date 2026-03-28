"""Taxonomy model — shared across tenants.

Lives in the public schema of the extensions database. Defines named
taxonomy collections (Chart of Accounts, US GAAP Reporting, CoA→GAAP Mapping).
"""

from datetime import UTC, datetime

from sqlalchemy import (
  Boolean,
  CheckConstraint,
  Column,
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
    CheckConstraint(
      "taxonomy_type IN ('chart_of_accounts', 'reporting', 'mapping')",
      name="check_taxonomy_type",
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

  # For mapping taxonomies
  source_taxonomy_id = Column(
    String, ForeignKey("taxonomies.id", use_alter=True), nullable=True
  )
  target_taxonomy_id = Column(
    String, ForeignKey("taxonomies.id", use_alter=True), nullable=True
  )

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
