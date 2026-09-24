"""Bridge — cross-namespace equivalence taxonomy.

A Bridge is an ordinary taxonomy (mostly ``equivalence`` arcs) relating
qnames across namespaces; this public-schema row is a metadata overlay so
tools can list a framework's bridges without scanning taxonomies.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  Boolean,
  Column,
  DateTime,
  Index,
  String,
  UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase


class Bridge(ExtensionsBase):
  """Cross-namespace equivalence taxonomy."""

  __tablename__ = "bridges"
  __table_args__ = (
    UniqueConstraint("bridge", "version", name="uq_bridges_bridge_version"),
    Index("idx_bridges_active", "is_active"),
    Index("idx_bridges_namespaces", "source_namespace", "target_namespace"),
    {"schema": "public"},
  )

  # UUID5(bridge, version).
  id = Column(String, primary_key=True)

  bridge = Column(String, nullable=False)
  version = Column(String, nullable=False)

  title = Column(String, nullable=True)
  description = Column(String, nullable=True)

  source_namespace = Column(String, nullable=False)
  target_namespace = Column(String, nullable=False)

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
    return (
      f"<Bridge {self.bridge}@{self.version} "
      f"({self.source_namespace} → {self.target_namespace})>"
    )
