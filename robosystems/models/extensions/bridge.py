"""Bridge — cross-namespace equivalence taxonomy.

A Bridge is a taxonomy whose job is to relate qnames in one sovereign
namespace to qnames in another. It ships through the same loader and
sits in the same ``taxonomies`` / ``associations`` tables as any
package; the Bridge model is a metadata overlay so admin tools can
query "what bridges does this framework pin?" without scanning every
taxonomy.

Bridges typically have ``taxonomy_type='mapping'`` and their content
is dominated by ``association_type='equivalence'`` arcs. The
``source_namespace`` / ``target_namespace`` columns name the two ends
of the bridge for human review and for routing decisions in mappers.

Public-schema only — same as :class:`Framework`.
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

  # Identity — UUID5(bridge, version).
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
