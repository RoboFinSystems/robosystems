"""GET /v1/graphs/{graph_id}/audit/mutations — graph admins read the graph's
mutation audit, newest first; nobody else does, and no graph sees another's."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from robosystems.models.core import (
  Graph,
  GraphRole,
  GraphUser,
  OperationMutationAudit,
  Org,
  OrgRole,
  OrgType,
  OrgUser,
  User,
)

pytestmark = pytest.mark.asyncio


def _kg_id() -> str:
  return f"kg{uuid4().hex[:16]}"


def _graph(session, owner_id: str, owner_role: OrgRole = OrgRole.OWNER) -> Graph:
  org = Org.create(
    name=f"Audit Org {uuid4().hex[:6]}", org_type=OrgType.TEAM, session=session
  )
  OrgUser.create(org_id=org.id, user_id=owner_id, role=owner_role, session=session)
  return Graph.create(
    graph_id=_kg_id(),
    org_id=org.id,
    graph_name="Audited Graph",
    graph_type="generic",
    session=session,
  )


def _seed(session, graph_id: str) -> list[str]:
  """Three rows, one per surface, a minute apart; returns ids newest first."""
  now = datetime.now(UTC)
  rows = [
    ("api", "create-agent", None, None, 3),
    ("mcp", "create-agent", None, None, 2),
    ("operator", "remember", "author", "op_run_1", 1),
  ]
  ids = []
  for surface, name, operator_type, operation_id, minutes_ago in rows:
    row = OperationMutationAudit(
      id=f"oma_{uuid4().hex}",
      occurred_at=now - timedelta(minutes=minutes_ago),
      duration_ms=5.0,
      graph_id=graph_id,
      surface=surface,
      operation_name=name,
      status="completed",
      operator_type=operator_type,
      operation_id=operation_id,
      object_ids=[f"obj_{surface}"],
    )
    session.add(row)
    ids.append(row.id)
  session.commit()
  return list(reversed(ids))


class TestListGraphMutations:
  async def test_an_admin_reads_newest_first(self, async_client, test_db, test_user):
    graph = _graph(test_db, test_user.id)
    newest_first = _seed(test_db, graph.graph_id)

    response = await async_client.get(f"/v1/graphs/{graph.graph_id}/audit/mutations")

    assert response.status_code == 200
    data = response.json()
    assert data["graph_id"] == graph.graph_id
    assert [e["id"] for e in data["entries"]] == newest_first
    assert [e["surface"] for e in data["entries"]] == ["operator", "mcp", "api"]
    assert data["entries"][0]["object_ids"] == ["obj_operator"]
    assert data["next_cursor"] is None

  async def test_pages_without_skipping_or_repeating(
    self, async_client, test_db, test_user
  ):
    graph = _graph(test_db, test_user.id)
    newest_first = _seed(test_db, graph.graph_id)
    url = f"/v1/graphs/{graph.graph_id}/audit/mutations"

    first = (await async_client.get(url, params={"limit": 2})).json()
    second = (
      await async_client.get(url, params={"limit": 2, "cursor": first["next_cursor"]})
    ).json()

    seen = [e["id"] for e in first["entries"]] + [e["id"] for e in second["entries"]]
    assert seen == newest_first
    assert second["next_cursor"] is None

  async def test_filters_by_surface_and_run(self, async_client, test_db, test_user):
    graph = _graph(test_db, test_user.id)
    _seed(test_db, graph.graph_id)
    url = f"/v1/graphs/{graph.graph_id}/audit/mutations"

    by_surface = (await async_client.get(url, params={"surface": "mcp"})).json()
    by_run = (await async_client.get(url, params={"operation_id": "op_run_1"})).json()

    assert [e["surface"] for e in by_surface["entries"]] == ["mcp"]
    assert [e["operation_name"] for e in by_run["entries"]] == ["remember"]

  async def test_another_graphs_rows_are_not_visible(
    self, async_client, test_db, test_user
  ):
    mine = _graph(test_db, test_user.id)
    theirs = _graph(test_db, test_user.id)
    _seed(test_db, theirs.graph_id)

    response = await async_client.get(f"/v1/graphs/{mine.graph_id}/audit/mutations")

    assert response.status_code == 200
    assert response.json()["entries"] == []

  async def test_a_member_who_is_not_admin_is_refused(
    self, async_client, test_db, test_user
  ):
    owner = User(
      email=f"auditowner+{uuid4().hex[:8]}@example.com",
      name="Audit Owner",
      password_hash=test_user.password_hash,
    )
    test_db.add(owner)
    test_db.commit()
    graph = _graph(test_db, owner.id)
    OrgUser.create(
      org_id=graph.org_id, user_id=test_user.id, role=OrgRole.MEMBER, session=test_db
    )
    GraphUser.create(
      user_id=test_user.id,
      graph_id=graph.graph_id,
      role=GraphRole.MEMBER,
      session=test_db,
    )
    _seed(test_db, graph.graph_id)

    response = await async_client.get(f"/v1/graphs/{graph.graph_id}/audit/mutations")

    assert response.status_code == 403

  async def test_a_forged_cursor_is_a_bad_request(
    self, async_client, test_db, test_user
  ):
    graph = _graph(test_db, test_user.id)

    response = await async_client.get(
      f"/v1/graphs/{graph.graph_id}/audit/mutations", params={"cursor": "not-ours"}
    )

    assert response.status_code == 400
