"""
Tests for organization router endpoints responsible for org metadata.

These tests focus on ensuring that the high-impact org management APIs
expose membership, limits, and graph state correctly for the authenticated user.
"""

from __future__ import annotations

from unittest.mock import patch
from uuid import uuid4

import pytest

from robosystems.models.iam import (
  Graph,
  Org,
  OrgLimits,
  OrgRole,
  OrgType,
  OrgUser,
  User,
)

pytestmark = pytest.mark.asyncio


def _create_member(session, password_hash: str) -> User:
  """Create a throwaway user that can join organizations in tests."""
  suffix = uuid4().hex[:8]
  return User.create(
    email=f"member+{suffix}@example.com",
    name=f"Member {suffix}",
    password_hash=password_hash,
    session=session,
  )


class TestOrgRouter:
  async def test_list_user_orgs_includes_member_and_graph_counts(
    self, async_client, test_db, test_user
  ):
    """Ensure the org listing reflects membership role and resource counts."""
    org = Org.create(
      name=f"Coverage Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.ADMIN, session=test_db
    )

    teammate = _create_member(test_db, test_user.password_hash)
    OrgUser.create(
      org_id=org.id, user_id=teammate.id, role=OrgRole.MEMBER, session=test_db
    )

    Graph.create(
      graph_id=f"graph_{uuid4().hex[:8]}",
      org_id=org.id,
      graph_name="Analytics",
      graph_type="generic",
      session=test_db,
    )

    response = await async_client.get("/v1/orgs")

    assert response.status_code == 200
    payload = response.json()
    org_entry = next((item for item in payload["orgs"] if item["id"] == org.id), None)

    assert org_entry is not None
    assert org_entry["role"] == OrgRole.ADMIN.value
    assert org_entry["member_count"] == 2
    assert org_entry["graph_count"] == 1

  async def test_update_org_requires_admin_privileges(
    self, async_client, test_db, test_user
  ):
    """Members without admin/owner role should be blocked from updates."""
    owner = _create_member(test_db, test_user.password_hash)
    org = Org.create(
      name=f"Read Only Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    OrgUser.create(org_id=org.id, user_id=owner.id, role=OrgRole.OWNER, session=test_db)
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.MEMBER, session=test_db
    )

    response = await async_client.put(
      f"/v1/orgs/{org.id}",
      json={"name": "Attempted Update"},
    )

    assert response.status_code == 403
    assert (
      response.json()["detail"]
      == "Only admins and owners can update organization details"
    )

  async def test_update_org_admin_cannot_change_org_type(
    self, async_client, test_db, test_user
  ):
    """Admins can rename an org but only owners may change the org type."""
    org = Org.create(
      name=f"Team Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.ADMIN, session=test_db
    )

    response = await async_client.put(
      f"/v1/orgs/{org.id}",
      json={"name": "Renamed Org", "org_type": OrgType.ENTERPRISE.value},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "Renamed Org"
    assert body["org_type"] == OrgType.TEAM.value

    test_db.refresh(org)
    assert org.name == "Renamed Org"
    assert org.org_type == OrgType.TEAM

  async def test_owner_can_update_org_type(self, async_client, test_db, test_user):
    """Owners should be able to change org type."""
    org = Org.create(
      name=f"Upgrade Org {uuid4().hex[:6]}",
      org_type=OrgType.PERSONAL,
      session=test_db,
    )
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.OWNER, session=test_db
    )

    response = await async_client.put(
      f"/v1/orgs/{org.id}",
      json={"org_type": OrgType.ENTERPRISE.value},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["org_type"] == OrgType.ENTERPRISE.value

    test_db.refresh(org)
    assert org.org_type == OrgType.ENTERPRISE

  async def test_get_org_returns_members_graphs_and_limits(
    self, async_client, test_db, test_user
  ):
    """Org detail endpoint should surface members, graphs, and limits."""
    org = Org.create(
      name=f"Insights Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.OWNER, session=test_db
    )
    teammate = _create_member(test_db, test_user.password_hash)
    OrgUser.create(
      org_id=org.id, user_id=teammate.id, role=OrgRole.MEMBER, session=test_db
    )

    graph_id = f"graph_{uuid4().hex[:8]}"
    Graph.create(
      graph_id=graph_id,
      org_id=org.id,
      graph_name="Usage Graph",
      graph_type="generic",
      session=test_db,
    )

    limits = OrgLimits.create_default_limits(org_id=org.id, session=test_db)

    response = await async_client.get(f"/v1/orgs/{org.id}")

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == org.id
    assert body["user_role"] == OrgRole.OWNER.value
    assert {member["user_id"] for member in body["members"]} == {
      test_user.id,
      teammate.id,
    }
    assert any(graph["graph_id"] == graph_id for graph in body["graphs"])
    assert body["limits"]["max_graphs"] == limits.max_graphs

  async def test_list_org_graphs_includes_credit_totals(
    self, async_client, test_db, test_user
  ):
    """Graph listing should merge credit availability for each org graph."""
    org = Org.create(
      name=f"Graph Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.ADMIN, session=test_db
    )

    graph_id = f"graph_{uuid4().hex[:8]}"
    Graph.create(
      graph_id=graph_id,
      org_id=org.id,
      graph_name="Credit Graph",
      graph_type="generic",
      session=test_db,
    )

    class DummyCredits:
      def __init__(self):
        self.available_credits = 75.0
        self.total_consumed = 25.0

    with patch(
      "robosystems.models.iam.graph_credits.GraphCredits.get_by_graph_id",
      return_value=DummyCredits(),
    ):
      response = await async_client.get(f"/v1/orgs/{org.id}/graphs")

    assert response.status_code == 200
    graphs = response.json()
    assert len(graphs) == 1
    assert graphs[0]["graph_id"] == graph_id
    assert graphs[0]["credits_available"] == 75.0
    assert graphs[0]["credits_used"] == 25.0

  async def test_list_org_graphs_forbids_non_members(
    self, async_client, test_db, test_user
  ):
    """Graph list should reject users outside the org."""
    outsider = _create_member(test_db, test_user.password_hash)
    org = Org.create(
      name=f"Private Graph Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    OrgUser.create(
      org_id=org.id, user_id=outsider.id, role=OrgRole.ADMIN, session=test_db
    )

    response = await async_client.get(f"/v1/orgs/{org.id}/graphs")

    assert response.status_code == 403
    assert response.json()["detail"] == "You are not a member of this organization"

  async def test_create_org_assigns_owner_and_limits(
    self, async_client, test_db, test_user
  ):
    """Creating an org should assign the requester as owner with default limits."""
    payload = {
      "name": f"Created Org {uuid4().hex[:6]}",
      "org_type": OrgType.TEAM.value,
    }

    response = await async_client.post("/v1/orgs", json=payload)

    assert response.status_code == 201
    body = response.json()
    org_id = body["id"]
    assert body["name"] == payload["name"]

    membership = OrgUser.get_by_org_and_user(org_id, test_user.id, test_db)
    assert membership is not None
    assert membership.role == OrgRole.OWNER

    limits = OrgLimits.get_by_org_id(org_id, test_db)
    assert limits is not None
    assert limits.max_graphs > 0

  async def test_get_org_denies_non_member_access(
    self, async_client, test_db, test_user
  ):
    """Users outside an org should receive 403."""
    outsider = _create_member(test_db, test_user.password_hash)
    org = Org.create(
      name=f"Private Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    OrgUser.create(
      org_id=org.id, user_id=outsider.id, role=OrgRole.MEMBER, session=test_db
    )

    response = await async_client.get(f"/v1/orgs/{org.id}")

    assert response.status_code == 403
    assert response.json()["detail"] == "You are not a member of this organization"
