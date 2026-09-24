"""Deleting a graph with staged files, tables and schemas (a subgraph that
has had an upload) lets the database cascade remove the children."""

from __future__ import annotations

from uuid import uuid4

import pytest

from robosystems.models.core import Graph, GraphFile, GraphTable, Org, OrgType
from robosystems.models.core.graph.graph_schema import GraphSchema
from robosystems.models.core.graph.source_file import SourceFile


@pytest.mark.unit
def test_a_graph_with_uploads_deletes_with_its_children(test_db):
  org = Org.create(
    name=f"Del {uuid4().hex[:6]}", org_type=OrgType.TEAM, session=test_db
  )
  graph = Graph.create(
    graph_id=f"kg{uuid4().hex[:16]}",
    org_id=org.id,
    graph_name="Uploads",
    graph_type="generic",
    session=test_db,
  )
  table = GraphTable(
    graph_id=graph.graph_id, table_name="Entity", table_type="node", schema_json={}
  )
  test_db.add(table)
  test_db.flush()
  test_db.add_all(
    [
      GraphFile(
        graph_id=graph.graph_id,
        table_id=table.id,
        file_name="e.parquet",
        s3_key="k",
        file_format="parquet",
        file_size_bytes=1,
        upload_method="presigned",
      ),
      GraphSchema(graph_id=graph.graph_id, schema_type="custom", schema_ddl="-"),
      SourceFile(graph_id=graph.graph_id, storage_key="s", file_type="pdf"),
    ]
  )
  test_db.commit()
  # Load the backrefs, as a request that touched them would.
  assert graph.tables and graph.files and graph.schemas and graph.source_files

  graph.delete(test_db)

  test_db.expire_all()
  assert Graph.get_by_id(graph.graph_id, test_db, include_deprovisioned=True) is None
  for model in (GraphTable, GraphFile, GraphSchema, SourceFile):
    assert test_db.query(model).filter(model.graph_id == graph.graph_id).count() == 0
