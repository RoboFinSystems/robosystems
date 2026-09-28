"""Audit retention: rows older than MCP_AUDIT_RETENTION_DAYS go, newer stay."""

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

from dagster import build_op_context
from sqlalchemy.orm import sessionmaker

from robosystems.dagster.jobs.audit_retention import prune_mcp_mutation_audit
from robosystems.models.core import McpMutationAudit


def _row(id_: str, age_days: int) -> McpMutationAudit:
  return McpMutationAudit(
    id=id_,
    occurred_at=datetime.now(UTC) - timedelta(days=age_days),
    duration_ms=1.0,
    graph_id="kg_retention",
    tool_name="create-agent",
    status="completed",
    caller_kind="client",
    arguments_sha256="0" * 64,
    object_ids=[],
  )


def test_prunes_rows_past_retention(test_db):
  factory = sessionmaker(bind=test_db.get_bind())
  seed = factory()
  seed.add_all([_row("mcpa_old", 400), _row("mcpa_new", 10)])
  seed.commit()
  seed.close()

  @contextmanager
  def get_session():
    session = factory()
    try:
      yield session
      session.commit()
    finally:
      session.close()

  db = MagicMock()
  db.get_session = get_session
  with patch(
    "robosystems.dagster.jobs.audit_retention.env.MCP_AUDIT_RETENTION_DAYS", 396
  ):
    result = prune_mcp_mutation_audit(build_op_context(), db)

  assert result["deleted"] == 1
  check = factory()
  try:
    remaining = {
      r.id
      for r in check.query(McpMutationAudit).filter(
        McpMutationAudit.graph_id == "kg_retention"
      )
    }
  finally:
    check.close()
  assert remaining == {"mcpa_new"}
