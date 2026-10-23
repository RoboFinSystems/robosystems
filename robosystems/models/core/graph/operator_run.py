"""One row per in-app AI operator run: the question, the history it was
given, every model call with its tool calls and results, and the answer.

This is the paper trail behind `operation_mutation_audit`: an audit row names
the run that made a write (`operation_id`); this row says what the run was
asked, what it did, and what it answered. Unlike the audit table it holds
payloads, because the transcript *is* the record. It is the tenant's own
graph data, so `graph_id` cascades: the trail goes with the graph.

A `thread_id` groups the runs of one console conversation; a run made over
the API without one stands alone.
"""

from datetime import UTC, datetime

from sqlalchemy import Column, DateTime, Float, ForeignKey, Index, String, Text
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.database import Base


class OperatorRun(Base):
  __tablename__ = "operator_runs"

  # The operation id the run was queued under (`op_…`); every audit row the
  # run produced carries the same value.
  operation_id = Column(String, primary_key=True)
  thread_id = Column(String(64), nullable=True)
  graph_id = Column(
    String, ForeignKey("graphs.graph_id", ondelete="CASCADE"), nullable=False
  )
  org_id = Column(String, nullable=True)
  user_id = Column(String, nullable=False)

  operator_type = Column(String, nullable=False)
  mode = Column(String, nullable=False)
  # completed | failed | cancelled
  status = Column(String, nullable=False)

  question = Column(Text, nullable=False)
  # The prior turns the run was given, as role/content pairs.
  history = Column(JSONB, nullable=False, default=list)
  # One entry per model call: the new input since the previous call, the
  # model's output blocks, usage and credits.
  transcript = Column(JSONB, nullable=False, default=list)
  answer = Column(Text, nullable=True)
  error = Column(Text, nullable=True)
  # The response envelope's metadata minus result rows: tokens, credits,
  # tools called, the Cypher run, the writes made, confidence, loop flags.
  result = Column(JSONB, nullable=False, default=dict)

  started_at = Column(
    DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
  )
  finished_at = Column(DateTime(timezone=True), nullable=False)
  duration_ms = Column(Float, nullable=False)

  __table_args__ = (
    Index("idx_operator_runs_graph_time", graph_id, started_at),
    Index("idx_operator_runs_thread_time", thread_id, started_at),
    Index("idx_operator_runs_user_time", user_id, started_at),
  )

  def __repr__(self) -> str:
    return (
      f"<OperatorRun {self.operation_id} {self.operator_type} {self.status} "
      f"graph={self.graph_id}>"
    )
