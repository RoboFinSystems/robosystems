"""Framework: a versioned bundle pinning ``(package, version)`` and
``(bridge, version)`` tuples in load order.

Public schema only. Tenants reference one through
``Graph.taxonomy_pin = {"framework": "rs-gaap@v1"}``, which the resolver
expands into a flat ``{standard: version}`` pin.
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

  # UUID5 of (framework, version), so a manifest always yields the same id.
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
