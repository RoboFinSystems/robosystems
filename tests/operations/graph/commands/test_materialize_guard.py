"""Tests for the non-rebuild re-materialization guard (staged source).

DuckDB staging tables rebuild from *all* uploaded files, so once any file
has been materialized into the graph, a later non-rebuild materialize
replays the whole table into a graph that already holds those rows —
duplicate node keys hard-fail the COPY and duplicate relationships load
silently. The guard converts that into an explicit 409 asking for
rebuild=true, while leaving first-time materializes and no-new-data
repeat calls untouched.
"""

import uuid

import pytest
from fastapi import HTTPException
from sqlalchemy.orm import Session

from robosystems.models.core import GraphFile, GraphTable
from robosystems.models.core.graph.graph import Graph
from robosystems.operations.graph.commands.materialize import (
  _require_rebuild_for_populated_graph,
)

pytestmark = pytest.mark.unit


@pytest.fixture()
def staged_graph(db_session: Session):
  """A generic graph with one table and helpers to add files in a state."""
  graph_id = f"kg{uuid.uuid4().hex[:14]}"
  graph = Graph(graph_id=graph_id, graph_name="Guard Test", graph_type="generic")
  db_session.add(graph)
  db_session.flush()

  table = GraphTable(
    graph_id=graph_id,
    table_name="Item",
    table_type="node",
    schema_json={"columns": []},
  )
  db_session.add(table)
  db_session.flush()

  def add_file(duckdb_status: str, graph_status: str) -> GraphFile:
    file = GraphFile(
      graph_id=graph_id,
      table_id=table.id,
      file_name=f"{uuid.uuid4().hex[:8]}.parquet",
      s3_key=f"test/{graph_id}/{uuid.uuid4().hex[:8]}.parquet",
      file_format="parquet",
      file_size_bytes=1024,
      upload_method="direct",
      duckdb_status=duckdb_status,
      graph_status=graph_status,
    )
    db_session.add(file)
    db_session.flush()
    return file

  return {"graph_id": graph_id, "add_file": add_file}


class TestRequireRebuildForPopulatedGraph:
  def test_first_materialize_is_allowed(self, db_session, staged_graph):
    staged_graph["add_file"]("staged", "pending")

    _require_rebuild_for_populated_graph(
      db_session, staged_graph["graph_id"], rebuild=False
    )

  def test_new_uploads_into_populated_graph_require_rebuild(
    self, db_session, staged_graph
  ):
    staged_graph["add_file"]("staged", "ingested")
    staged_graph["add_file"]("staged", "pending")

    with pytest.raises(HTTPException) as exc:
      _require_rebuild_for_populated_graph(
        db_session, staged_graph["graph_id"], rebuild=False
      )

    assert exc.value.status_code == 409
    assert "rebuild=true" in str(exc.value.detail)

  def test_rebuild_bypasses_the_guard(self, db_session, staged_graph):
    staged_graph["add_file"]("staged", "ingested")
    staged_graph["add_file"]("staged", "pending")

    _require_rebuild_for_populated_graph(
      db_session, staged_graph["graph_id"], rebuild=True
    )

  def test_repeat_call_with_no_new_files_is_allowed(self, db_session, staged_graph):
    """Nothing pending means nothing re-copies; the existing not-stale skip
    keeps handling the benign repeat call."""
    staged_graph["add_file"]("staged", "ingested")

    _require_rebuild_for_populated_graph(
      db_session, staged_graph["graph_id"], rebuild=False
    )

  def test_failed_previous_materialize_can_retry_without_rebuild(
    self, db_session, staged_graph
  ):
    """A failed first materialize marked nothing ingested — retrying must
    not demand a rebuild."""
    staged_graph["add_file"]("staged", "failed")
    staged_graph["add_file"]("staged", "pending")

    _require_rebuild_for_populated_graph(
      db_session, staged_graph["graph_id"], rebuild=False
    )

  def test_other_graphs_files_do_not_trip_the_guard(self, db_session, staged_graph):
    other_id = f"kg{uuid.uuid4().hex[:14]}"
    other = Graph(graph_id=other_id, graph_name="Other", graph_type="generic")
    db_session.add(other)
    db_session.flush()
    other_table = GraphTable(
      graph_id=other_id,
      table_name="Item",
      table_type="node",
      schema_json={"columns": []},
    )
    db_session.add(other_table)
    db_session.flush()
    db_session.add(
      GraphFile(
        graph_id=other_id,
        table_id=other_table.id,
        file_name="x.parquet",
        s3_key=f"test/{other_id}/x.parquet",
        file_format="parquet",
        file_size_bytes=1024,
        upload_method="direct",
        duckdb_status="staged",
        graph_status="ingested",
      )
    )
    db_session.flush()
    staged_graph["add_file"]("staged", "pending")

    _require_rebuild_for_populated_graph(
      db_session, staged_graph["graph_id"], rebuild=False
    )
