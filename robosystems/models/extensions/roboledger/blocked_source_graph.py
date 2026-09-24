"""BlockedSourceGraph model — the recipient's deny list for cross-graph shares.

Sharing is capability-style (holding a graph_id is the handshake), so the
recipient must be able to refuse. ``share-report`` checks this list first and
tells a blocked sender rather than silently dropping the share. Lives in the
recipient's tenant schema.
"""

from datetime import UTC, datetime

from sqlalchemy import Column, DateTime, Index, String, UniqueConstraint

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class BlockedSourceGraph(ExtensionsBase):
  __tablename__ = "blocked_source_graphs"
  __table_args__ = (
    UniqueConstraint("source_graph_id", name="uq_blocked_source_graphs_source"),
    Index("idx_blocked_source_graphs_source", "source_graph_id"),
  )

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("blk"))
  source_graph_id = Column(String, nullable=False)
  blocked_by = Column(String, nullable=False)  # user ID
  blocked_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  reason = Column(String, nullable=True)

  def __repr__(self) -> str:
    return f"<BlockedSourceGraph {self.source_graph_id}>"
