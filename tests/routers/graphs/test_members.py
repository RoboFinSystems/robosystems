"""
Tests for graph member management endpoints.

Covers the grant lifecycle (add, list, update, remove), the graph-admin gate
(explicit or implicit via org role), the org-roster restriction, and the
parent-graph-only rule.
"""

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
  User,
)

pytestmark = pytest.mark.asyncio


def _kg_id() -> str:
  return f"kg{uuid4().hex[:16]}"


def _create_user(session, password_hash: str) -> User:
  suffix = uuid4().hex[:8]
  user = User(
    email=f"graphmember+{suffix}@example.com",
    name=f"Graph Member {suffix}",
    password_hash=password_hash,
  )
  session.add(user)
  session.commit()
  session.refresh(user)
  return user


def _setup_org_graph(session, owner_id: str, owner_role: OrgRole = OrgRole.OWNER):
  """Create an org with the given membership for owner_id, plus an org graph."""
  org = Org.create(
    name=f"Graph Org {uuid4().hex[:6]}",
    org_type=OrgType.TEAM,
    session=session,
  )
  OrgUser.create(org_id=org.id, user_id=owner_id, role=owner_role, session=session)
  graph = Graph.create(
    graph_id=_kg_id(),
    org_id=org.id,
    graph_name="Team Graph",
    graph_type="generic",
    session=session,
  )
  return org, graph


class TestListGraphMembers:
  async def test_owner_lists_explicit_and_implicit_members(
    self, async_client, test_db, test_user
  ):
    org, graph = _setup_org_graph(test_db, test_user.id)
    teammate = _create_user(test_db, test_user.password_hash)
    OrgUser.create(
      org_id=org.id, user_id=teammate.id, role=OrgRole.MEMBER, session=test_db
    )
    GraphUser.create(
      user_id=teammate.id,
      graph_id=graph.graph_id,
      role=GraphRole.VIEWER,
      session=test_db,
    )

    response = await async_client.get(f"/v1/graphs/{graph.graph_id}/members")

    assert response.status_code == 200
    data = response.json()
    assert data["graph_id"] == graph.graph_id
    members = {m["user_id"]: m for m in data["members"]}
    # test_user is org OWNER with no explicit row → implicit admin
    assert members[test_user.id]["source"] == "org_role"
    assert members[test_user.id]["role"] == "admin"
    assert members[teammate.id]["source"] == "explicit"
    assert members[teammate.id]["role"] == "viewer"

  async def test_non_admin_cannot_list(self, async_client, test_db, test_user):
    """A plain graph member (not admin, not org owner/admin) is rejected."""
    other_owner_org = Org.create(
      name=f"Other Org {uuid4().hex[:6]}", org_type=OrgType.TEAM, session=test_db
    )
    owner = _create_user(test_db, test_user.password_hash)
    OrgUser.create(
      org_id=other_owner_org.id, user_id=owner.id, role=OrgRole.OWNER, session=test_db
    )
    OrgUser.create(
      org_id=other_owner_org.id,
      user_id=test_user.id,
      role=OrgRole.MEMBER,
      session=test_db,
    )
    graph = Graph.create(
      graph_id=_kg_id(),
      org_id=other_owner_org.id,
      graph_name="Not Mine",
      graph_type="generic",
      session=test_db,
    )
    GraphUser.create(
      user_id=test_user.id,
      graph_id=graph.graph_id,
      role=GraphRole.MEMBER,
      session=test_db,
    )

    response = await async_client.get(f"/v1/graphs/{graph.graph_id}/members")

    assert response.status_code == 403


class TestAddGraphMember:
  async def test_add_org_member_at_viewer(self, async_client, test_db, test_user):
    org, graph = _setup_org_graph(test_db, test_user.id)
    teammate = _create_user(test_db, test_user.password_hash)
    OrgUser.create(
      org_id=org.id, user_id=teammate.id, role=OrgRole.MEMBER, session=test_db
    )

    response = await async_client.post(
      f"/v1/graphs/{graph.graph_id}/members",
      json={"user_id": teammate.id, "role": "viewer"},
    )

    assert response.status_code == 201
    data = response.json()
    assert data["user_id"] == teammate.id
    assert data["role"] == "viewer"
    assert data["source"] == "explicit"

    assert GraphUser.user_has_access(teammate.id, graph.graph_id, test_db)
    assert not GraphUser.user_has_write_access(teammate.id, graph.graph_id, test_db)

  async def test_add_rejects_user_outside_org(self, async_client, test_db, test_user):
    _, graph = _setup_org_graph(test_db, test_user.id)
    outsider = _create_user(test_db, test_user.password_hash)

    response = await async_client.post(
      f"/v1/graphs/{graph.graph_id}/members",
      json={"user_id": outsider.id, "role": "member"},
    )

    assert response.status_code == 400
    assert "graph's organization" in response.json()["detail"]

  async def test_add_unknown_user_returns_404(self, async_client, test_db, test_user):
    _, graph = _setup_org_graph(test_db, test_user.id)

    response = await async_client.post(
      f"/v1/graphs/{graph.graph_id}/members",
      json={"user_id": "usr_does_not_exist", "role": "member"},
    )

    assert response.status_code == 404

  async def test_add_duplicate_grant_conflicts(self, async_client, test_db, test_user):
    org, graph = _setup_org_graph(test_db, test_user.id)
    teammate = _create_user(test_db, test_user.password_hash)
    OrgUser.create(
      org_id=org.id, user_id=teammate.id, role=OrgRole.MEMBER, session=test_db
    )
    GraphUser.create(
      user_id=teammate.id,
      graph_id=graph.graph_id,
      role=GraphRole.MEMBER,
      session=test_db,
    )

    response = await async_client.post(
      f"/v1/graphs/{graph.graph_id}/members",
      json={"user_id": teammate.id, "role": "admin"},
    )

    assert response.status_code == 409

  async def test_subgraph_is_rejected(self, async_client, test_db, test_user):
    org, graph = _setup_org_graph(test_db, test_user.id)
    subgraph = Graph.create(
      graph_id=f"{graph.graph_id}_dev",
      org_id=org.id,
      graph_name="Dev Subgraph",
      graph_type="generic",
      session=test_db,
    )
    subgraph.is_subgraph = True
    subgraph.parent_graph_id = graph.graph_id
    subgraph.subgraph_index = 1
    subgraph.subgraph_name = "dev"
    test_db.commit()

    teammate = _create_user(test_db, test_user.password_hash)
    OrgUser.create(
      org_id=org.id, user_id=teammate.id, role=OrgRole.MEMBER, session=test_db
    )

    response = await async_client.post(
      f"/v1/graphs/{subgraph.graph_id}/members",
      json={"user_id": teammate.id, "role": "member"},
    )

    assert response.status_code == 400
    assert "parent graph" in response.json()["detail"]


class TestUpdateGraphMemberRole:
  async def test_update_explicit_member_role(self, async_client, test_db, test_user):
    org, graph = _setup_org_graph(test_db, test_user.id)
    teammate = _create_user(test_db, test_user.password_hash)
    OrgUser.create(
      org_id=org.id, user_id=teammate.id, role=OrgRole.MEMBER, session=test_db
    )
    GraphUser.create(
      user_id=teammate.id,
      graph_id=graph.graph_id,
      role=GraphRole.VIEWER,
      session=test_db,
    )

    response = await async_client.put(
      f"/v1/graphs/{graph.graph_id}/members/{teammate.id}",
      json={"role": "member"},
    )

    assert response.status_code == 200
    assert response.json()["role"] == "member"
    assert GraphUser.user_has_write_access(teammate.id, graph.graph_id, test_db)

  async def test_update_implicit_member_returns_404(
    self, async_client, test_db, test_user
  ):
    """Org owner/admin access has no row to edit — managed via org roles."""
    org, graph = _setup_org_graph(test_db, test_user.id)
    org_admin = _create_user(test_db, test_user.password_hash)
    OrgUser.create(
      org_id=org.id, user_id=org_admin.id, role=OrgRole.ADMIN, session=test_db
    )

    response = await async_client.put(
      f"/v1/graphs/{graph.graph_id}/members/{org_admin.id}",
      json={"role": "viewer"},
    )

    assert response.status_code == 404
    assert "organization roles" in response.json()["detail"]


class TestRemoveGraphMember:
  async def test_remove_explicit_member(self, async_client, test_db, test_user):
    org, graph = _setup_org_graph(test_db, test_user.id)
    teammate = _create_user(test_db, test_user.password_hash)
    OrgUser.create(
      org_id=org.id, user_id=teammate.id, role=OrgRole.MEMBER, session=test_db
    )
    GraphUser.create(
      user_id=teammate.id,
      graph_id=graph.graph_id,
      role=GraphRole.MEMBER,
      session=test_db,
    )

    response = await async_client.delete(
      f"/v1/graphs/{graph.graph_id}/members/{teammate.id}"
    )

    assert response.status_code == 204
    assert not GraphUser.user_has_access(teammate.id, graph.graph_id, test_db)

  async def test_remove_without_grant_returns_404(
    self, async_client, test_db, test_user
  ):
    org, graph = _setup_org_graph(test_db, test_user.id)
    teammate = _create_user(test_db, test_user.password_hash)
    OrgUser.create(
      org_id=org.id, user_id=teammate.id, role=OrgRole.MEMBER, session=test_db
    )

    response = await async_client.delete(
      f"/v1/graphs/{graph.graph_id}/members/{teammate.id}"
    )

    assert response.status_code == 404
