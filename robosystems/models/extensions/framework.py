"""Framework — named composition of taxonomy packages and bridges.

A Framework is the addressable, versioned bundle that pins specific
``(package, version)`` and ``(bridge, version)`` tuples together. The
deliverable that says "rs-gaap v1 = these N packages + these M
bridges at these versions, in this load order."

Frameworks live in the **public schema only** — they're library
metadata, not per-tenant content. Tenants reference them by name
through ``Graph.taxonomy_pin = {"framework": "rs-gaap@v1"}``;
the resolver expands the framework into a flat ``{standard: version}``
pin which drives the existing tenant-copy machinery.

The architecture maps 1:1 to Charlie Hoffman's Seattle Method: his
"Reporting Scheme" (e.g. ``us-gaap-theory.xsd``) is a framework
manifest; his individual XSDs are packages; his
``disclosure-equivalentTextblock`` arcs are bridges.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  Boolean,
  CheckConstraint,
  Column,
  DateTime,
  Index,
  String,
  UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase


class Framework(ExtensionsBase):
  """Named composition that pins a specific set of packages + bridges."""

  __tablename__ = "frameworks"
  __table_args__ = (
    UniqueConstraint("framework", "version", name="uq_frameworks_framework_version"),
    Index("idx_frameworks_active", "is_active"),
    CheckConstraint(
      "framework_type IN ('reporting', 'extension', 'custom')",
      name="check_framework_type",
    ),
    {"schema": "public"},
  )

  # Identity — UUID5 derived from (framework, version) by the migration
  # so the same manifest always produces the same row id.
  id = Column(String, primary_key=True)

  framework = Column(String, nullable=False)
  version = Column(String, nullable=False)

  title = Column(String, nullable=True)
  description = Column(String, nullable=True)

  framework_type = Column(String, nullable=False, default="reporting")

  is_active = Column(Boolean, nullable=False, default=True)

  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)

  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  updated_at = Column(
    DateTime,
    nullable=False,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
  )
  created_by = Column(String, nullable=False, default="library-seeder")

  def __repr__(self) -> str:
    return f"<Framework {self.framework}@{self.version}>"
