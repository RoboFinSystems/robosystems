"""Taxonomy: a named collection of elements (chart of accounts, a reporting
standard, a CoA→GAAP mapping, an entity extension).

Tenant taxonomies live in the graph's schema; shared ones in public, visible
through search_path. ``parent_taxonomy_id`` is the single-parent extension
chain (version, entity extension, industry overlay); mapping relationships
use ``source_taxonomy_id`` / ``target_taxonomy_id`` instead.
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

# `taxonomies.taxonomy_type` vocabulary for the CHECK and the tenant widen
# step. 'reporting' survives only on rows copied from an un-backfilled public
# schema; new writes use the reporting_* / custom_ontology values.
TAXONOMY_TYPE_VALUES: tuple[str, ...] = (
  "chart_of_accounts",
  "reporting",
  "mapping",
  "schedule",
  "trait-vocabulary",
  "trait-assignment",
  "classification-vocabulary",
  "classification-assignment",
  "rules",
  "reporting_standard",
  "reporting_extension",
  "custom_ontology",
)


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
      "taxonomy_type IN (" + ", ".join(f"'{v}'" for v in TAXONOMY_TYPE_VALUES) + ")",
      name="check_taxonomy_type",
    ),
    CheckConstraint(
      "extension_type IS NULL OR extension_type IN "
      "('version', 'entity_extension', 'industry', 'jurisdiction')",
      name="check_taxonomy_extension_type",
    ),
  )

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("tax"))
  name = Column(String, nullable=False)
  description = Column(String, nullable=True)

  taxonomy_type = Column(String, nullable=False)
  version = Column(String, nullable=True)

  standard = Column(String, nullable=True)
  namespace_uri = Column(String, nullable=True)

  is_shared = Column(Boolean, nullable=False, default=False)

  # Mapping taxonomies (CoA → GAAP).
  source_taxonomy_id = Column(
    String, ForeignKey("taxonomies.id", use_alter=True), nullable=True
  )
  target_taxonomy_id = Column(
    String, ForeignKey("taxonomies.id", use_alter=True), nullable=True
  )

  # TAXONOMY_EXTENDS_TAXONOMY in the graph.
  parent_taxonomy_id = Column(
    String, ForeignKey("taxonomies.id", use_alter=True), nullable=True
  )
  extension_type = Column(
    String, nullable=True
  )  # version | entity_extension | industry | jurisdiction
  effective_date = Column(Date, nullable=True)

  is_active = Column(Boolean, nullable=False, default=True)
  is_locked = Column(Boolean, nullable=False, default=False)

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
    return f"<Taxonomy {self.name} ({self.taxonomy_type})>"
