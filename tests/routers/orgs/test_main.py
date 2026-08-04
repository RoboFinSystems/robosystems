"""
Tests for organization router endpoints responsible for org metadata.

These tests focus on ensuring that the high-impact org management APIs
expose membership, limits, and graph state correctly for the authenticated user.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from robosystems.models.core import (
  Graph,
  GraphRole,
  GraphUser,
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
    """Graph listing merges credit availability from a REAL credits row.

    The previous version stubbed a credits object carrying attribute names
    `GraphCredits` has never had (`available_credits`, `total_consumed`),
    which kept an AttributeError → 500 green for any org whose graph had a
    real credits row — the fifth stub-asserts-the-bug instance across three
    reviews. The endpoint now goes through `get_usage_summary`, and this
    test would 500 if the field names drift again.
    """
    from decimal import Decimal

    from robosystems.models.core import GraphCredits

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

    credits = GraphCredits(
      graph_id=graph_id,
      user_id=test_user.id,
      billing_admin_id=test_user.id,
      current_balance=Decimal("100"),
      monthly_allocation=Decimal("100"),
    )
    test_db.add(credits)
    test_db.commit()
    consumption = credits.consume_credits_atomic(
      amount=Decimal("25"),
      operation_type="agent_call",
      operation_description="AI call",
      session=test_db,
    )
    assert consumption["success"] is True

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


class TestOrgGraphVisibility:
  """Org membership alone grants no graph access, so the org views must not
  disclose graphs the caller cannot reach.

  Every org had exactly one member (its owner) until multi-user orgs shipped,
  which made "all org graphs" and "my graphs" the same set and hid this. The
  second member is what separates them.
  """

  async def test_member_sees_only_granted_graphs(
    self, async_client, test_db, test_user
  ):
    """A plain member sees graphs they hold a GraphUser row for — not the rest."""
    owner = _create_member(test_db, test_user.password_hash)
    org = Org.create(
      name=f"Visibility Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    OrgUser.create(org_id=org.id, user_id=owner.id, role=OrgRole.OWNER, session=test_db)
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.MEMBER, session=test_db
    )

    granted = Graph.create(
      graph_id=f"graph_{uuid4().hex[:8]}",
      org_id=org.id,
      graph_name="Granted",
      graph_type="generic",
      session=test_db,
    )
    hidden = Graph.create(
      graph_id=f"graph_{uuid4().hex[:8]}",
      org_id=org.id,
      graph_name="Hidden",
      graph_type="generic",
      session=test_db,
    )
    GraphUser.create(
      user_id=test_user.id,
      graph_id=granted.graph_id,
      role=GraphRole.MEMBER,
      session=test_db,
    )

    response = await async_client.get(f"/v1/orgs/{org.id}")

    assert response.status_code == 200
    visible = {g["graph_id"] for g in response.json()["graphs"]}
    assert visible == {granted.graph_id}
    assert hidden.graph_id not in visible

  async def test_member_without_grants_sees_no_graphs(
    self, async_client, test_db, test_user
  ):
    """An invited member who has not been granted anything sees an empty list."""
    owner = _create_member(test_db, test_user.password_hash)
    org = Org.create(
      name=f"Ungranted Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    OrgUser.create(org_id=org.id, user_id=owner.id, role=OrgRole.OWNER, session=test_db)
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.MEMBER, session=test_db
    )
    Graph.create(
      graph_id=f"graph_{uuid4().hex[:8]}",
      org_id=org.id,
      graph_name="Owner Only",
      graph_type="generic",
      session=test_db,
    )

    response = await async_client.get(f"/v1/orgs/{org.id}")

    assert response.status_code == 200
    assert response.json()["graphs"] == []

  async def test_owner_sees_every_org_graph_without_explicit_grants(
    self, async_client, test_db, test_user
  ):
    """Owners are implicit graph admins, so they see the org's graphs with no
    GraphUser rows of their own."""
    org = Org.create(
      name=f"Owner View Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.OWNER, session=test_db
    )
    first = Graph.create(
      graph_id=f"graph_{uuid4().hex[:8]}",
      org_id=org.id,
      graph_name="First",
      graph_type="generic",
      session=test_db,
    )
    second = Graph.create(
      graph_id=f"graph_{uuid4().hex[:8]}",
      org_id=org.id,
      graph_name="Second",
      graph_type="generic",
      session=test_db,
    )

    response = await async_client.get(f"/v1/orgs/{org.id}")

    assert response.status_code == 200
    assert {g["graph_id"] for g in response.json()["graphs"]} == {
      first.graph_id,
      second.graph_id,
    }

  async def test_list_org_graphs_endpoint_applies_the_same_rule(
    self, async_client, test_db, test_user
  ):
    """The dedicated graphs listing must not be a way around the detail view."""
    owner = _create_member(test_db, test_user.password_hash)
    org = Org.create(
      name=f"Listing Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    OrgUser.create(org_id=org.id, user_id=owner.id, role=OrgRole.OWNER, session=test_db)
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.MEMBER, session=test_db
    )
    Graph.create(
      graph_id=f"graph_{uuid4().hex[:8]}",
      org_id=org.id,
      graph_name="Not Yours",
      graph_type="generic",
      session=test_db,
    )

    response = await async_client.get(f"/v1/orgs/{org.id}/graphs")

    assert response.status_code == 200
    assert response.json() == []
