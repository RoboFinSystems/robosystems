"""
Tests for organization member management endpoints.

Validates membership listings plus the strict role/owner safety checks on update
and removal flows to protect billing-linked org resources.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from robosystems.models.core import Org, OrgRole, OrgType, OrgUser, User

pytestmark = pytest.mark.asyncio


def _create_user(session, password_hash: str, *, active: bool = True) -> User:
  """Create a unique user for org membership scenarios."""
  suffix = uuid4().hex[:10]
  user = User(
    email=f"member+{suffix}@example.com",
    name=f"Member {suffix}",
    password_hash=password_hash,
    is_active=active,
  )
  session.add(user)
  session.commit()
  session.refresh(user)
  return user


class TestOrgMembersRouter:
  async def test_list_members_returns_full_roster(
    self, async_client, test_db, test_user
  ):
    """Ensure listing members returns all org users with status flags."""
    org = Org.create(
      name=f"Roster Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.ADMIN, session=test_db
    )

    inactive_member = _create_user(test_db, test_user.password_hash, active=False)
    OrgUser.create(
      org_id=org.id,
      user_id=inactive_member.id,
      role=OrgRole.MEMBER,
      session=test_db,
    )

    response = await async_client.get(f"/v1/orgs/{org.id}/members")

    assert response.status_code == 200
    data = response.json()
    assert data["org_id"] == org.id
    assert data["total"] == 2
    members = {member["user_id"]: member for member in data["members"]}
    assert members[test_user.id]["role"] == OrgRole.ADMIN.value
    assert members[inactive_member.id]["is_active"] is False

  async def test_update_member_role_requires_admin_privileges(
    self, async_client, test_db, test_user
  ):
    """Members should be denied when attempting to change roles."""
    org = Org.create(
      name=f"Role Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    owner = _create_user(test_db, test_user.password_hash)
    OrgUser.create(org_id=org.id, user_id=owner.id, role=OrgRole.OWNER, session=test_db)
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.MEMBER, session=test_db
    )

    teammate = _create_user(test_db, test_user.password_hash)
    OrgUser.create(
      org_id=org.id, user_id=teammate.id, role=OrgRole.MEMBER, session=test_db
    )

    response = await async_client.put(
      f"/v1/orgs/{org.id}/members/{teammate.id}",
      json={"role": OrgRole.ADMIN.value},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Only admins and owners can update member roles"

  async def test_update_member_role_only_owner_can_manage_owner_states(
    self, async_client, test_db, test_user
  ):
    """Admins cannot promote or modify owner roles."""
    org = Org.create(
      name=f"Owner Guard {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    owner = _create_user(test_db, test_user.password_hash)
    OrgUser.create(org_id=org.id, user_id=owner.id, role=OrgRole.OWNER, session=test_db)
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.ADMIN, session=test_db
    )

    response = await async_client.put(
      f"/v1/orgs/{org.id}/members/{owner.id}",
      json={"role": OrgRole.MEMBER.value},
    )

    assert response.status_code == 400
    assert "Cannot change owner role through this endpoint" in response.json()["detail"]

  async def test_update_member_role_owner_cannot_leave_org_without_successor(
    self, async_client, test_db, test_user
  ):
    """Owners cannot demote themselves if they are the last owner."""
    org = Org.create(
      name=f"Sole Owner Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.OWNER, session=test_db
    )

    response = await async_client.put(
      f"/v1/orgs/{org.id}/members/{test_user.id}",
      json={"role": OrgRole.ADMIN.value},
    )

    assert response.status_code == 400
    assert "Cannot change owner role through this endpoint" in response.json()["detail"]

  async def test_remove_member_blocks_non_privileged_users(
    self, async_client, test_db, test_user
  ):
    """Regular members cannot remove others."""
    org = Org.create(
      name=f"Removal Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    owner = _create_user(test_db, test_user.password_hash)
    OrgUser.create(org_id=org.id, user_id=owner.id, role=OrgRole.OWNER, session=test_db)
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.MEMBER, session=test_db
    )

    target = _create_user(test_db, test_user.password_hash)
    OrgUser.create(
      org_id=org.id, user_id=target.id, role=OrgRole.MEMBER, session=test_db
    )

    response = await async_client.delete(f"/v1/orgs/{org.id}/members/{target.id}")

    assert response.status_code == 403
    assert response.json()["detail"] == "Only admins and owners can remove members"

  async def test_remove_member_admin_cannot_remove_self(
    self, async_client, test_db, test_user
  ):
    """Admins must ask another privileged user to remove them."""
    org = Org.create(
      name=f"Self Removal Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    owner = _create_user(test_db, test_user.password_hash)
    OrgUser.create(org_id=org.id, user_id=owner.id, role=OrgRole.OWNER, session=test_db)
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.ADMIN, session=test_db
    )

    response = await async_client.delete(f"/v1/orgs/{org.id}/members/{test_user.id}")

    assert response.status_code == 400
    assert response.json()["detail"] == "Admins and owners cannot remove themselves"

  async def test_update_member_role_owner_can_promote_member(
    self, async_client, test_db, test_user
  ):
    """Owners should successfully update other members' roles."""
    org = Org.create(
      name=f"Promotion Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.OWNER, session=test_db
    )

    teammate = _create_user(test_db, test_user.password_hash)
    OrgUser.create(
      org_id=org.id, user_id=teammate.id, role=OrgRole.MEMBER, session=test_db
    )

    response = await async_client.put(
      f"/v1/orgs/{org.id}/members/{teammate.id}",
      json={"role": OrgRole.ADMIN.value},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["role"] == OrgRole.ADMIN.value

    test_db.refresh(OrgUser.get_by_org_and_user(org.id, teammate.id, test_db))
    updated_membership = OrgUser.get_by_org_and_user(org.id, teammate.id, test_db)
    assert updated_membership.role == OrgRole.ADMIN

  async def test_owner_can_remove_member_successfully(
    self, async_client, test_db, test_user
  ):
    """Owner removing a regular member should succeed with 204."""
    org = Org.create(
      name=f"Removal Success Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.OWNER, session=test_db
    )

    teammate = _create_user(test_db, test_user.password_hash)
    OrgUser.create(
      org_id=org.id, user_id=teammate.id, role=OrgRole.MEMBER, session=test_db
    )

    response = await async_client.delete(f"/v1/orgs/{org.id}/members/{teammate.id}")

    assert response.status_code == 204
    assert OrgUser.get_by_org_and_user(org.id, teammate.id, test_db) is None

  async def test_member_can_remove_self(self, async_client, test_db, test_user):
    """Members should be able to remove themselves without admin help."""
    org = Org.create(
      name=f"Self Exit Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    owner = _create_user(test_db, test_user.password_hash)
    OrgUser.create(org_id=org.id, user_id=owner.id, role=OrgRole.OWNER, session=test_db)
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.MEMBER, session=test_db
    )

    response = await async_client.delete(f"/v1/orgs/{org.id}/members/{test_user.id}")

    assert response.status_code == 204
    assert OrgUser.get_by_org_and_user(org.id, test_user.id, test_db) is None

    # Owner should remain unaffected
    assert OrgUser.get_by_org_and_user(org.id, owner.id, test_db) is not None

  async def test_update_member_role_target_not_found(
    self, async_client, test_db, test_user
  ):
    """Updating role for non-member should return 404."""
    org = Org.create(
      name=f"Missing Member Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.ADMIN, session=test_db
    )

    response = await async_client.put(
      f"/v1/orgs/{org.id}/members/non-existent",
      json={"role": OrgRole.MEMBER.value},
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "User is not a member of this organization"

  async def test_remove_member_non_owner_cannot_remove_owner(
    self, async_client, test_db, test_user
  ):
    """Admins should be blocked from removing owners."""
    org = Org.create(
      name=f"Owner Guard Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    owner = _create_user(test_db, test_user.password_hash)
    OrgUser.create(org_id=org.id, user_id=owner.id, role=OrgRole.OWNER, session=test_db)
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.ADMIN, session=test_db
    )

    response = await async_client.delete(f"/v1/orgs/{org.id}/members/{owner.id}")

    assert response.status_code == 403
    assert response.json()["detail"] == "Only owners can remove other owners"

  async def test_remove_member_revokes_org_graph_access(
    self, async_client, test_db, test_user
  ):
    """Removing a member also deletes their grants on org-owned graphs,
    while grants on graphs outside the org are untouched."""
    from robosystems.models.core import Graph, GraphUser

    org = Org.create(
      name=f"Cascade Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.ADMIN, session=test_db
    )
    member = _create_user(test_db, test_user.password_hash)
    OrgUser.create(
      org_id=org.id, user_id=member.id, role=OrgRole.MEMBER, session=test_db
    )

    other_org = Org.create(
      name=f"Other Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )

    org_graph = Graph.create(
      graph_id=f"graph_{uuid4().hex[:8]}",
      org_id=org.id,
      graph_name="Org Graph",
      graph_type="generic",
      session=test_db,
    )
    outside_graph = Graph.create(
      graph_id=f"graph_{uuid4().hex[:8]}",
      org_id=other_org.id,
      graph_name="Outside Graph",
      graph_type="generic",
      session=test_db,
    )
    GraphUser.create(
      user_id=member.id, graph_id=org_graph.graph_id, role="member", session=test_db
    )
    GraphUser.create(
      user_id=member.id,
      graph_id=outside_graph.graph_id,
      role="member",
      session=test_db,
    )

    response = await async_client.delete(f"/v1/orgs/{org.id}/members/{member.id}")

    assert response.status_code == 204
    assert OrgUser.get_by_org_and_user(org.id, member.id, test_db) is None
    assert (
      GraphUser.get_by_user_and_graph(member.id, org_graph.graph_id, test_db) is None
    )
    assert (
      GraphUser.get_by_user_and_graph(member.id, outside_graph.graph_id, test_db)
      is not None
    )

  async def test_remove_member_cancels_their_repository_subscriptions(
    self, async_client, test_db, test_user
  ):
    """The org stops paying for a member's repository access when they leave."""
    from robosystems.models.core import BillingSubscription, Graph
    from robosystems.models.core.user.user_repository import (
      RepositoryAccessLevel,
      RepositoryType,
      UserRepository,
    )

    if Graph.get_by_id("sec", test_db) is None:
      Graph.create(
        graph_id="sec",
        org_id=None,
        graph_name="SEC",
        graph_type="repository",
        session=test_db,
      )

    org = Org.create(
      name=f"Billing Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.OWNER, session=test_db
    )
    member = _create_user(test_db, test_user.password_hash)
    OrgUser.create(
      org_id=org.id, user_id=member.id, role=OrgRole.MEMBER, session=test_db
    )

    subscription = BillingSubscription.create_subscription(
      org_id=org.id,
      resource_type="repository",
      resource_id="sec",
      plan_name="sec-starter",
      base_price_cents=4900,
      session=test_db,
      user_id=member.id,
    )
    subscription.activate(test_db)
    access = UserRepository.create_access(
      user_id=member.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      access_level=RepositoryAccessLevel.READ,
      repository_plan="sec-starter",
      session=test_db,
    )

    response = await async_client.delete(f"/v1/orgs/{org.id}/members/{member.id}")

    assert response.status_code == 204
    test_db.refresh(subscription)
    test_db.refresh(access)
    assert subscription.status == "canceled"
    assert access.is_active is False
