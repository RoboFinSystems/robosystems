"""Audit retention: rows older than OPERATION_AUDIT_RETENTION_DAYS go, newer stay."""

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

from dagster import build_op_context
from sqlalchemy.orm import sessionmaker

from robosystems.dagster.jobs.audit_retention import prune_operation_mutation_audit
from robosystems.models.core import OperationMutationAudit


def _row(id_: str, age_days: int) -> OperationMutationAudit:
  return OperationMutationAudit(
    id=id_,
    occurred_at=datetime.now(UTC) - timedelta(days=age_days),
    duration_ms=1.0,
    graph_id="kg_retention",
    surface="api",
    operation_name="create-agent",
    status="completed",
    object_ids=[],
  )


def test_prunes_rows_past_retention(test_db):
  factory = sessionmaker(bind=test_db.get_bind())
  seed = factory()
  seed.add_all([_row("oma_old", 400), _row("oma_new", 10)])
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
    "robosystems.dagster.jobs.audit_retention.env.OPERATION_AUDIT_RETENTION_DAYS", 396
  ):
    result = prune_operation_mutation_audit(build_op_context(), db)

  assert result["deleted"] == 1
  check = factory()
  try:
    remaining = {
      r.id
      for r in check.query(OperationMutationAudit).filter(
        OperationMutationAudit.graph_id == "kg_retention"
      )
    }
  finally:
    check.close()
  assert remaining == {"oma_new"}


def test_prunes_operator_runs_past_retention(test_db):
  from datetime import UTC, datetime, timedelta
  from uuid import uuid4

  from robosystems.dagster.jobs.audit_retention import prune_operator_runs
  from robosystems.models.core import Graph, OperatorRun, Org, OrgType

  factory = sessionmaker(bind=test_db.get_bind())
  seed = factory()
  org = Org.create(
    name=f"Retention {uuid4().hex[:6]}", org_type=OrgType.TEAM, session=seed
  )
  graph = Graph.create(
    graph_id=f"kg{uuid4().hex[:16]}",
    org_id=org.id,
    graph_name="Retention Graph",
    graph_type="generic",
    session=seed,
  )
  graph_id = graph.graph_id

  def run(id_: str, age_days: int) -> OperatorRun:
    started = datetime.now(UTC) - timedelta(days=age_days)
    return OperatorRun(
      operation_id=id_,
      graph_id=graph_id,
      user_id="usr_retention",
      operator_type="analyst",
      mode="standard",
      status="completed",
      question="q",
      history=[],
      transcript=[],
      result={},
      started_at=started,
      finished_at=started,
      duration_ms=1.0,
    )

  seed.add_all([run("op_old", 400), run("op_new", 10)])
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
    "robosystems.dagster.jobs.audit_retention.env.OPERATION_AUDIT_RETENTION_DAYS", 396
  ):
    result = prune_operator_runs(build_op_context(), db)

  assert result["deleted"] == 1
  check = factory()
  try:
    remaining = {
      r.operation_id
      for r in check.query(OperatorRun).filter(OperatorRun.graph_id == graph_id)
    }
  finally:
    check.close()
  assert remaining == {"op_new"}
