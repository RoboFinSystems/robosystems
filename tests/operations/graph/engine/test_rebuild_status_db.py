"""Every way out of a rebuild clears ``rebuilding`` (which blocks queries)."""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from robosystems.models.core import Graph, GraphFile, GraphTable, Org, OrgType
from robosystems.models.core.graph.graph_schema import GraphSchema
from robosystems.operations.graph.engine.direct_materialization import (
  materialize_graph_directly,
)

MODULE = "robosystems.operations.graph.engine.direct_materialization"


def _graph(session, *, staged: bool) -> Graph:
  org = Org.create(name=f"Rb {uuid4().hex[:6]}", org_type=OrgType.TEAM, session=session)
  graph = Graph.create(
    graph_id=f"kg{uuid4().hex[:16]}",
    org_id=org.id,
    graph_name="Rebuild",
    graph_type="generic",
    session=session,
  )
  session.add(
    GraphSchema(graph_id=graph.graph_id, schema_type="custom", schema_ddl="-")
  )
  if staged:
    table = GraphTable(
      graph_id=graph.graph_id, table_name="Entity", table_type="node", schema_json={}
    )
    session.add(table)
    session.flush()
    session.add(
      GraphFile(
        graph_id=graph.graph_id,
        table_id=table.id,
        file_name="e.parquet",
        s3_key="k",
        file_format="parquet",
        file_size_bytes=1,
        upload_method="presigned",
        duckdb_status="staged",
      )
    )
  session.commit()
  return graph


@contextmanager
def _graph_api(materialize):
  client = MagicMock()
  client._instance_id = ""
  client.delete_database = AsyncMock()
  client.create_database = AsyncMock()
  client.close = AsyncMock()
  with (
    patch(
      "robosystems.graph_api.client.factory.GraphClientFactory.create_client",
      new=AsyncMock(return_value=client),
    ),
    patch(f"{MODULE}.begin_destructive_op", new=AsyncMock()),
    patch(f"{MODULE}.end_destructive_op", new=AsyncMock()),
    patch(
      "robosystems.operations.graph.engine.chunked_materialization.materialize_table_chunked",
      new=materialize,
    ),
    patch(
      "robosystems.dagster.reporting.report_asset_materialization", new=AsyncMock()
    ),
  ):
    yield


def _status(session, graph_id):
  session.expire_all()
  return (Graph.get_by_id(graph_id, session).graph_metadata or {}).get("status")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_a_rebuild_with_nothing_staged_ends_available(test_db):
  graph = _graph(test_db, staged=False)
  with _graph_api(AsyncMock()):
    await materialize_graph_directly(test_db, graph.graph_id, rebuild=True)
  assert _status(test_db, graph.graph_id) == "available"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_a_rebuild_that_fails_midway_ends_failed(test_db):
  graph = _graph(test_db, staged=True)
  failing = AsyncMock(side_effect=RuntimeError("copy failed"))
  with _graph_api(failing):
    result = await materialize_graph_directly(test_db, graph.graph_id, rebuild=True)
  assert result["status"] == "error"
  assert _status(test_db, graph.graph_id) == "rebuild_failed"
