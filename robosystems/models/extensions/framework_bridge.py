"""FrameworkBridge — junction pinning a bridge into a framework.

Mirrors :class:`FrameworkPackage` but for bridges. A Framework declares
which cross-namespace bridges it includes (and at which version) via
FrameworkBridge rows.

Public-schema only.
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


class FrameworkBridge(ExtensionsBase):
  """One row per (framework, bridge) pin."""

  __tablename__ = "framework_bridges"
  __table_args__ = (
    UniqueConstraint(
      "framework_id",
      "bridge_id",
      name="uq_framework_bridges_framework_bridge",
    ),
    Index("idx_framework_bridges_framework", "framework_id"),
    Index("idx_framework_bridges_bridge", "bridge_id"),
    {"schema": "public"},
  )

  # Identity — UUID5(framework_id, bridge_id).
  id = Column(String, primary_key=True)

  framework_id = Column(String, ForeignKey("public.frameworks.id"), nullable=False)
  bridge_id = Column(String, ForeignKey("public.bridges.id"), nullable=False)

  ordinal = Column(Integer, nullable=False, default=0)
  is_required = Column(Boolean, nullable=False, default=True)

  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)

  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  created_by = Column(String, nullable=False, default="library-seeder")

  def __repr__(self) -> str:
    return (
      f"<FrameworkBridge {self.framework_id}: bridge={self.bridge_id} "
      f"ord={self.ordinal} required={self.is_required}>"
    )
