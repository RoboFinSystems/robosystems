"""FrameworkPackage — junction pinning a package version into a framework.

A Framework declares which taxonomy packages it composes (and at which
version) via FrameworkPackage rows. ``ordinal`` encodes load order;
``is_required`` lets a manifest declare optional/aspirational packages
without blocking the framework's load when those packages haven't been
authored yet.

Public-schema only — same as :class:`Framework`. No FK to ``taxonomies``
because packages and frameworks may load in either order; resolution
happens at provision time via the ``(standard, version)`` tuple.
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
