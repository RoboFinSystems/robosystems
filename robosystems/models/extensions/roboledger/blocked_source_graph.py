"""BlockedSourceGraph model — the recipient's deny list for cross-graph shares.

Cross-graph report sharing is capability-style: a company that holds a
subscriber's ``graph_id`` can share published reports into that subscriber's
tenant schema, and the handover of the id out-of-band is the handshake. That
model is only sound if the recipient can refuse — a capability that cannot be
declined is an obligation.

A row here is the recipient saying "no more from this sender". ``share-report``
consults it before copying anything in, and a blocked sender is told, rather
than silently dropped: under the capability model the two parties already had a
relationship, so a bounce is more honest than a shadow ban and stops the sender
retrying forever.

The table lives in the **recipient's** tenant schema, not the platform DB — it
is the recipient's own state about their own graph and needs no cross-tenant
coordination, mirroring how publish lists live in the sender's schema.
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
