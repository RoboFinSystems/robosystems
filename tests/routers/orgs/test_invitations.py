"""
Tests for organization invitation endpoints.

Covers the invite lifecycle (create, list, revoke, resend), its role gating and
feature flag, and the public token-preview endpoint used by the register page.
"""

from __future__ import annotations

from unittest.mock import patch
from uuid import uuid4

import pytest

from robosystems.config import env
from robosystems.models.core import (
  InvitationStatus,
  Org,
  OrgInvitation,
  OrgRole,
  OrgType,
  OrgUser,
)

pytestmark = pytest.mark.asyncio

DISPATCH = "robosystems.routers.orgs.invitations.run_and_monitor_dagster_job"


def _make_org(session, user_id: str, role: OrgRole) -> Org:
  """Create an org with the given user membership."""
  org = Org.create(
    name=f"Invite Org {uuid4().hex[:6]}",
    org_type=OrgType.TEAM,
    session=session,
  )
  OrgUser.create(org_id=org.id, user_id=user_id, role=role, session=session)
  return org


class TestCreateInvitation:
  async def test_returns_501_when_disabled(self, async_client, test_db, test_user):
    org = _make_org(test_db, test_user.id, OrgRole.OWNER)

    response = await async_client.post(
      f"/v1/orgs/{org.id}/invitations",
      json={"email": "newuser@example.com"},
    )

    assert response.status_code == 501
    assert (
      response.json()["detail"] == "Organization member invitations are not enabled"
    )

  async def test_owner_can_invite_and_email_is_queued(
    self, async_client, test_db, test_user
  ):
    org = _make_org(test_db, test_user.id, OrgRole.OWNER)

    with (
      patch.object(env, "ORG_MEMBER_INVITATIONS_ENABLED", True),
      patch(DISPATCH) as mock_dispatch,
    ):
      response = await async_client.post(
        f"/v1/orgs/{org.id}/invitations",
        json={"email": "Invitee@Example.com", "role": OrgRole.ADMIN.value},
      )

    assert response.status_code == 201
    data = response.json()
    assert data["email"] == "invitee@example.com"
    assert data["role"] == OrgRole.ADMIN.value
    assert data["status"] == InvitationStatus.PENDING.value
    assert data["invited_by_user_id"] == test_user.id
    assert data["is_expired"] is False

    invitation = OrgInvitation.get_by_id(data["id"], test_db)
    assert invitation is not None
    assert invitation.org_id == org.id

    assert mock_dispatch.call_count == 1
    email_config = mock_dispatch.call_args.kwargs["run_config"]["ops"]["send_email_op"][
      "config"
    ]
    assert email_config["email_type"] == "org_invitation"
    assert email_config["to_email"] == "invitee@example.com"
    assert email_config["org_name"] == org.name
    assert email_config["inviter_name"] == test_user.name
    assert email_config["token"]

  async def test_member_cannot_invite(self, async_client, test_db, test_user):
    org = _make_org(test_db, test_user.id, OrgRole.MEMBER)

    with patch.object(env, "ORG_MEMBER_INVITATIONS_ENABLED", True):
      response = await async_client.post(
        f"/v1/orgs/{org.id}/invitations",
        json={"email": "newuser@example.com"},
      )

    assert response.status_code == 403
    assert "admins and owners" in response.json()["detail"]

  async def test_existing_user_email_is_rejected(
    self, async_client, test_db, test_user
  ):
    org = _make_org(test_db, test_user.id, OrgRole.OWNER)

    with patch.object(env, "ORG_MEMBER_INVITATIONS_ENABLED", True):
      response = await async_client.post(
        f"/v1/orgs/{org.id}/invitations",
        json={"email": test_user.email},
      )

    assert response.status_code == 409
    assert "already exists" in response.json()["detail"]

  async def test_duplicate_pending_invitation_is_rejected(
    self, async_client, test_db, test_user
  ):
    org = _make_org(test_db, test_user.id, OrgRole.OWNER)
    OrgInvitation.create_invitation(
      org_id=org.id,
      email="pending@example.com",
      role=OrgRole.MEMBER,
      invited_by=test_user.id,
      session=test_db,
    )

    with patch.object(env, "ORG_MEMBER_INVITATIONS_ENABLED", True):
      response = await async_client.post(
        f"/v1/orgs/{org.id}/invitations",
        json={"email": "pending@example.com"},
      )

    assert response.status_code == 409
    assert "already pending" in response.json()["detail"]

  async def test_owner_role_is_not_invitable(self, async_client, test_db, test_user):
    org = _make_org(test_db, test_user.id, OrgRole.OWNER)

    with patch.object(env, "ORG_MEMBER_INVITATIONS_ENABLED", True):
      response = await async_client.post(
        f"/v1/orgs/{org.id}/invitations",
        json={"email": "newuser@example.com", "role": OrgRole.OWNER.value},
      )

    assert response.status_code == 422


class TestListInvitations:
  async def test_admin_sees_pending_invitations(self, async_client, test_db, test_user):
    org = _make_org(test_db, test_user.id, OrgRole.ADMIN)
    invitation, _ = OrgInvitation.create_invitation(
      org_id=org.id,
      email="listed@example.com",
      role=OrgRole.MEMBER,
      invited_by=test_user.id,
      session=test_db,
    )

    response = await async_client.get(f"/v1/orgs/{org.id}/invitations")

    assert response.status_code == 200
    data = response.json()
    assert data["org_id"] == org.id
    assert data["total"] == 1
    assert data["invitations"][0]["id"] == invitation.id
    assert data["invitations"][0]["invited_by_name"] == test_user.name

  async def test_member_cannot_list_invitations(self, async_client, test_db, test_user):
    org = _make_org(test_db, test_user.id, OrgRole.MEMBER)

    response = await async_client.get(f"/v1/orgs/{org.id}/invitations")

    assert response.status_code == 403


class TestRevokeInvitation:
  async def test_revoke_pending_invitation(self, async_client, test_db, test_user):
    org = _make_org(test_db, test_user.id, OrgRole.OWNER)
    invitation, raw_token = OrgInvitation.create_invitation(
      org_id=org.id,
      email="revoke@example.com",
      role=OrgRole.MEMBER,
      invited_by=test_user.id,
      session=test_db,
    )

    response = await async_client.delete(
      f"/v1/orgs/{org.id}/invitations/{invitation.id}"
    )

    assert response.status_code == 204
    test_db.refresh(invitation)
    assert invitation.status == InvitationStatus.REVOKED.value
    assert OrgInvitation.get_valid_by_token(raw_token, test_db) is None

  async def test_revoke_settled_invitation_conflicts(
    self, async_client, test_db, test_user
  ):
    org = _make_org(test_db, test_user.id, OrgRole.OWNER)
    invitation, _ = OrgInvitation.create_invitation(
      org_id=org.id,
      email="settled@example.com",
      role=OrgRole.MEMBER,
      invited_by=test_user.id,
      session=test_db,
    )
    invitation.revoke(test_db)

    response = await async_client.delete(
      f"/v1/orgs/{org.id}/invitations/{invitation.id}"
    )

    assert response.status_code == 409

  async def test_revoke_unknown_invitation_returns_404(
    self, async_client, test_db, test_user
  ):
    org = _make_org(test_db, test_user.id, OrgRole.OWNER)

    response = await async_client.delete(
      f"/v1/orgs/{org.id}/invitations/orginv_does_not_exist"
    )

    assert response.status_code == 404


class TestResendInvitation:
  async def test_resend_rotates_token_and_requeues_email(
    self, async_client, test_db, test_user
  ):
    org = _make_org(test_db, test_user.id, OrgRole.OWNER)
    invitation, old_token = OrgInvitation.create_invitation(
      org_id=org.id,
      email="resend@example.com",
      role=OrgRole.MEMBER,
      invited_by=test_user.id,
      session=test_db,
    )
    old_hash = invitation.token_hash

    with (
      patch.object(env, "ORG_MEMBER_INVITATIONS_ENABLED", True),
      patch(DISPATCH) as mock_dispatch,
    ):
      response = await async_client.post(
        f"/v1/orgs/{org.id}/invitations/{invitation.id}/resend"
      )

    assert response.status_code == 200
    test_db.refresh(invitation)
    assert invitation.token_hash != old_hash
    assert OrgInvitation.get_valid_by_token(old_token, test_db) is None
    assert mock_dispatch.call_count == 1


class TestInvitationPreview:
  async def test_preview_returns_org_details(self, async_client, test_db, test_user):
    org = _make_org(test_db, test_user.id, OrgRole.OWNER)
    _, raw_token = OrgInvitation.create_invitation(
      org_id=org.id,
      email="preview@example.com",
      role=OrgRole.ADMIN,
      invited_by=test_user.id,
      session=test_db,
    )

    response = await async_client.get(f"/v1/auth/invitations/{raw_token}")

    assert response.status_code == 200
    data = response.json()
    assert data["org_name"] == org.name
    assert data["email"] == "preview@example.com"
    assert data["role"] == OrgRole.ADMIN.value

  async def test_preview_unknown_token_returns_404(self, async_client):
    response = await async_client.get("/v1/auth/invitations/not-a-real-token")

    assert response.status_code == 404

  async def test_preview_revoked_token_returns_404(
    self, async_client, test_db, test_user
  ):
    org = _make_org(test_db, test_user.id, OrgRole.OWNER)
    invitation, raw_token = OrgInvitation.create_invitation(
      org_id=org.id,
      email="gone@example.com",
      role=OrgRole.MEMBER,
      invited_by=test_user.id,
      session=test_db,
    )
    invitation.revoke(test_db)

    response = await async_client.get(f"/v1/auth/invitations/{raw_token}")

    assert response.status_code == 404
