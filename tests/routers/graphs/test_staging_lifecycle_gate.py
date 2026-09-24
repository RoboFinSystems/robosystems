"""Staging reads (SQL, tables, schema) honour the graph lifecycle gate."""

from __future__ import annotations

from uuid import uuid4

import pytest

from robosystems.models.core import (
  Graph,
  GraphRole,
  GraphUser,
  Org,
  OrgRole,
  OrgType,
  OrgUser,
)
from robosystems.models.core.graph.graph import GraphStatus

pytestmark = pytest.mark.asyncio


@pytest.fixture
def suspended_graph(test_db, test_user):
  org = Org.create(
    name=f"Lifecycle Org {uuid4().hex[:6]}", org_type=OrgType.TEAM, session=test_db
  )
  OrgUser.create(
    org_id=org.id, user_id=test_user.id, role=OrgRole.OWNER, session=test_db
  )
  graph = Graph.create(
    org_id=org.id,
    graph_id=f"kg{uuid4().hex[:16]}",
    graph_name="Suspended Graph",
    graph_type="generic",
    session=test_db,
  )
  GraphUser.create(
    user_id=test_user.id,
    graph_id=graph.graph_id,
    role=GraphRole.ADMIN,
    session=test_db,
  )
  graph.status = GraphStatus.SUSPENDED.value
  test_db.commit()
  return graph


@pytest.mark.parametrize(
  ("method", "path", "body"),
  [
    ("post", "/query/sql", {"sql": "SELECT 1"}),
    ("get", "/tables", None),
    ("get", "/schema", None),
  ],
)
async def test_suspended_graph_refuses_staging_reads(
  async_client, suspended_graph, method, path, body
):
  url = f"/v1/graphs/{suspended_graph.graph_id}{path}"
  if body is None:
    response = await getattr(async_client, method)(url)
  else:
    response = await getattr(async_client, method)(url, json=body)

  assert response.status_code == 403, response.text
