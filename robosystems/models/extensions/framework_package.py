"""FrameworkPackage — junction pinning a package version into a framework.

``ordinal`` is load order; ``is_required=False`` lets a manifest name a
package not yet authored. Public schema only. No FK to ``taxonomies``
because packages and frameworks load in either order; resolution is by
``(standard, version)`` at provision time.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  Boolean,
  Column,
  DateTime,
  ForeignKey,
  Index,
  Integer,
  String,
  UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase


class FrameworkPackage(ExtensionsBase):
  """One row per (framework, package, version) pin."""

  __tablename__ = "framework_packages"
  __table_args__ = (
    UniqueConstraint(
      "framework_id",
      "package_standard",
      "package_version",
      name="uq_framework_packages_framework_pkg",
    ),
    Index("idx_framework_packages_framework", "framework_id"),
    Index("idx_framework_packages_pkg", "package_standard", "package_version"),
    {"schema": "public"},
  )

  # Identity — UUID5(framework_id, package_standard, package_version).
  id = Column(String, primary_key=True)

  framework_id = Column(String, ForeignKey("public.frameworks.id"), nullable=False)
  package_standard = Column(String, nullable=False)
  package_version = Column(String, nullable=False)

  ordinal = Column(Integer, nullable=False, default=0)
  is_required = Column(Boolean, nullable=False, default=True)

  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)

  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  created_by = Column(String, nullable=False, default="library-seeder")

  def __repr__(self) -> str:
    return (
      f"<FrameworkPackage {self.framework_id}: "
      f"{self.package_standard}@{self.package_version} "
      f"ord={self.ordinal} required={self.is_required}>"
    )
