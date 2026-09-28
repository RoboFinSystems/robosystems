"""One row per mutating call on a graph, from any surface: the REST API, an
external MCP client, or an in-app AI operator. Arguments are kept only as a
fingerprint, so no tenant ledger content lands in the platform database.

`graph_id` carries no foreign key on purpose: an audit row outlives the
graph it describes.
"""

from datetime import UTC, datetime
from enum import Enum

from sqlalchemy import Column, DateTime, Float, Index, String
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.database import Base
from robosystems.utils.ulid import generate_prefixed_ulid


class MutationSurface(str, Enum):
  API = "api"  # a REST operation call
  MCP = "mcp"  # an external MCP client (Claude, ChatGPT, any client)
  OPERATOR = "operator"  # an in-app AI operator run, e.g. the console's /do


class OperationMutationAudit(Base):
  __tablename__ = "operation_mutation_audit"

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("oma"))
  occurred_at = Column(
    DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
  )
  duration_ms = Column(Float, nullable=False)

  graph_id = Column(String, nullable=False)
  org_id = Column(String, nullable=True)
  user_id = Column(String, nullable=True)

  surface = Column(String, nullable=False)
  # The REST operation or the MCP tool name.
  operation_name = Column(String, nullable=False)
  status = Column(String, nullable=False)
  error_code = Column(String, nullable=True)

  auth_method = Column(String, nullable=True)
  api_key_prefix = Column(String, nullable=True)
  request_id = Column(String, nullable=True)
  # The REST operation's envelope id, or the operator run's operation id.
  operation_id = Column(String, nullable=True)
  operator_type = Column(String, nullable=True)

  arguments_fingerprint = Column(String(64), nullable=True)
  object_ids = Column(JSONB, nullable=False, default=list)

  __table_args__ = (
    Index("idx_operation_mutation_audit_graph_time", graph_id, occurred_at),
    Index("idx_operation_mutation_audit_user_time", user_id, occurred_at),
    Index("idx_operation_mutation_audit_operation", operation_id),
    Index("idx_operation_mutation_audit_time", occurred_at),
  )
