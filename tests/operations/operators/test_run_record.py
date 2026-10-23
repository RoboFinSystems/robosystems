"""The run record lands in operator_runs whole, and a second write under the
same operation id (a resumed run) replaces the first."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch
from uuid import uuid4

from robosystems.models.core import Graph, OperatorRun, Org, OrgRole, OrgType, OrgUser
from robosystems.operations.operators.run_record import (
  OperatorRunRecord,
  result_summary,
  write_run_record,
)


def _graph(session, owner_id: str) -> Graph:
  org = Org.create(
    name=f"Run Org {uuid4().hex[:6]}", org_type=OrgType.TEAM, session=session
  )
  OrgUser.create(org_id=org.id, user_id=owner_id, role=OrgRole.OWNER, session=session)
  return Graph.create(
    graph_id=f"kg{uuid4().hex[:16]}",
    org_id=org.id,
    graph_name="Recorded Graph",
    graph_type="generic",
    session=session,
  )


def _record(
  graph_id: str, user_id: str, operation_id: str, **overrides
) -> OperatorRunRecord:
  started = datetime.now(UTC)
  values = {
    "operation_id": operation_id,
    "graph_id": graph_id,
    "user_id": user_id,
    "operator_type": "analyst",
    "mode": "standard",
    "status": "completed",
    "question": "Total expenses July?",
    "thread_id": "thr_1",
    "history": [
      {"role": "user", "content": "hi"},
      {"role": "assistant", "content": "hello"},
    ],
    "transcript": [
      {"call": 1, "input": [], "output": [{"text": "42"}], "usage": {"input": 1}}
    ],
    "answer": "42",
    "result": {
      "credits_consumed": 12.5,
      "tools_called": ["read-graph-cypher"],
      "rows": [],
    },
    "started_at": started,
    "finished_at": started + timedelta(seconds=3),
    "duration_ms": 3000.0,
  }
  values.update(overrides)
  return OperatorRunRecord(**values)


def test_result_summary_drops_rows():
  assert result_summary({"rows": [{"n": 1}], "cypher": "MATCH", "result_count": 1}) == {
    "cypher": "MATCH",
    "result_count": 1,
  }


def test_write_run_record_persists_the_run(test_db, test_user):
  graph = _graph(test_db, test_user.id)
  op_id = f"op_{uuid4().hex}"
  with patch("robosystems.db.platform.SessionFactory", return_value=test_db):
    with patch.object(test_db, "close"):
      write_run_record(_record(graph.graph_id, test_user.id, op_id))

  row = test_db.query(OperatorRun).filter(OperatorRun.operation_id == op_id).one()
  assert row.org_id == graph.org_id
  assert row.thread_id == "thr_1"
  assert row.question == "Total expenses July?"
  assert row.answer == "42"
  assert row.transcript[0]["output"] == [{"text": "42"}]
  assert row.history[1]["content"] == "hello"
  assert row.result["credits_consumed"] == 12.5
  assert row.status == "completed"


def test_a_second_write_under_the_same_id_replaces_the_first(test_db, test_user):
  graph = _graph(test_db, test_user.id)
  op_id = f"op_{uuid4().hex}"
  with patch("robosystems.db.platform.SessionFactory", return_value=test_db):
    with patch.object(test_db, "close"):
      write_run_record(
        _record(graph.graph_id, test_user.id, op_id, status="failed", answer=None)
      )
      write_run_record(_record(graph.graph_id, test_user.id, op_id, status="completed"))

  rows = test_db.query(OperatorRun).filter(OperatorRun.operation_id == op_id).all()
  assert len(rows) == 1
  assert rows[0].status == "completed"
  assert rows[0].answer == "42"


def test_non_json_values_and_long_errors_are_made_storable(test_db, test_user):
  graph = _graph(test_db, test_user.id)
  op_id = f"op_{uuid4().hex}"
  record = _record(
    graph.graph_id,
    test_user.id,
    op_id,
    status="failed",
    error="e" * 5000,
    transcript=[{"call": 1, "input": [{"when": datetime(2026, 1, 1, tzinfo=UTC)}]}],
  )
  with patch("robosystems.db.platform.SessionFactory", return_value=test_db):
    with patch.object(test_db, "close"):
      write_run_record(record)

  row = test_db.query(OperatorRun).filter(OperatorRun.operation_id == op_id).one()
  assert len(row.error) == 2000
  assert row.transcript[0]["input"][0]["when"].startswith("2026-01-01")


def test_a_failed_write_never_raises(test_db, test_user):
  graph = _graph(test_db, test_user.id)
  record = _record(graph.graph_id, test_user.id, f"op_{uuid4().hex}")
  with patch(
    "robosystems.db.platform.SessionFactory", side_effect=RuntimeError("down")
  ):
    write_run_record(record)
