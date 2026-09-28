"""One row per mutating MCP tool call, from any caller: an external MCP
client or an in-app operator. Arguments are kept only as a fingerprint, so
no tenant ledger content lands in the platform database.

`graph_id` carries no foreign key on purpose: an audit row outlives the
graph it describes.
"""

from datetime import UTC, datetime

from sqlalchemy import Column, DateTime, Float, Index, String
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.database import Base
from robosystems.utils.ulid import generate_prefixed_ulid


class McpMutationAudit(Base):
  __tablename__ = "mcp_mutation_audit"

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("mcpa"))
  occurred_at = Column(
    DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
  )
  duration_ms = Column(Float, nullable=False)

  graph_id = Column(String, nullable=False)
  org_id = Column(String, nullable=True)
  user_id = Column(String, nullable=True)

  tool_name = Column(String, nullable=False)
  # "completed" or "failed"; a failed call changed nothing it reports.
  status = Column(String, nullable=False)
  error_code = Column(String, nullable=True)
  surface = Column(String, nullable=False, default="mcp")

  # "client" (an external MCP client) or "operator" (an in-app operator run).
  caller_kind = Column(String, nullable=False)
  auth_method = Column(String, nullable=True)
  api_key_prefix = Column(String, nullable=True)
  request_id = Column(String, nullable=True)
  operator_type = Column(String, nullable=True)
  operation_id = Column(String, nullable=True)

  arguments_sha256 = Column(String(64), nullable=False)
  object_ids = Column(JSONB, nullable=False, default=list)

  __table_args__ = (
    Index("idx_mcp_mutation_audit_graph_time", graph_id, occurred_at),
    Index("idx_mcp_mutation_audit_user_time", user_id, occurred_at),
    Index("idx_mcp_mutation_audit_operation", operation_id),
    Index("idx_mcp_mutation_audit_time", occurred_at),
  )
