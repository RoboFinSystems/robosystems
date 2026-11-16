"""The system prompts and tool sets operator runs were given, stored once per
graph by their SHA-256. A run's transcript names them by hash; this is where
the hash resolves to the text, so a run can be rebuilt as the model saw it.

Per graph, not global: an analyst prompt embeds the graph's own schema and
example queries, so it is the tenant's data and goes with the graph.
"""

from datetime import UTC, datetime

from sqlalchemy import Column, DateTime, ForeignKey, Index, String
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.database import Base


class OperatorRunContext(Base):
  __tablename__ = "operator_run_contexts"

  graph_id = Column(
    String,
    ForeignKey("graphs.graph_id", ondelete="CASCADE"),
    primary_key=True,
  )
  sha256 = Column(String(64), primary_key=True)
  # system | tools
  kind = Column(String(16), nullable=False)
  # The system prompt as a string, or the tools array as passed to the model.
  content = Column(JSONB, nullable=False)

  first_seen_at = Column(
    DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
  )
  # Retention prunes by this: a context lives as long as a run that used it.
  last_seen_at = Column(
    DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
  )

  __table_args__ = (Index("idx_operator_run_contexts_last_seen", last_seen_at),)

  def __repr__(self) -> str:
    return f"<OperatorRunContext {self.kind} {self.sha256[:12]} graph={self.graph_id}>"
