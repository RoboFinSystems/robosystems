"""Tests for the OrgInvitation model."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.exc import IntegrityError

from robosystems.models.core import (
  InvitationStatus,
  OrgInvitation,
  OrgRole,
)


class TestOrgInvitationModel:
  def test_create_invitation_returns_raw_token(self, test_db, test_user, test_org):
    invitation, raw_token = OrgInvitation.create_invitation(
      org_id=test_org.id,
      email="Invited.User@Example.com",
      role=OrgRole.MEMBER,
      invited_by=test_user.id,
      session=test_db,
    )

    assert invitation.id.startswith("orginv_")
    assert invitation.email == "invited.user@example.com"
    assert invitation.role == OrgRole.MEMBER
    assert invitation.status == InvitationStatus.PENDING.value
    assert invitation.invited_by == test_user.id
    assert invitation.token_hash != raw_token
    assert not invitation.is_expired

  def test_get_valid_by_token_roundtrip(self, test_db, test_user, test_org):
    invitation, raw_token = OrgInvitation.create_invitation(
      org_id=test_org.id,
      email="roundtrip@example.com",
      role=OrgRole.ADMIN,
      invited_by=test_user.id,
      session=test_db,
    )

    found = OrgInvitation.get_valid_by_token(raw_token, test_db)
    assert found is not None
    assert found.id == invitation.id

    assert OrgInvitation.get_valid_by_token("not-a-real-token", test_db) is None

  def test_revoked_invitation_token_is_invalid(self, test_db, test_user, test_org):
    invitation, raw_token = OrgInvitation.create_invitation(
      org_id=test_org.id,
      email="revoked@example.com",
      role=OrgRole.MEMBER,
      invited_by=test_user.id,
      session=test_db,
    )

    invitation.revoke(test_db)

    assert invitation.status == InvitationStatus.REVOKED.value
    assert invitation.revoked_at is not None
    assert OrgInvitation.get_valid_by_token(raw_token, test_db) is None

  def test_expired_invitation_token_is_invalid(self, test_db, test_user, test_org):
    invitation, raw_token = OrgInvitation.create_invitation(
      org_id=test_org.id,
      email="expired@example.com",
      role=OrgRole.MEMBER,
      invited_by=test_user.id,
      session=test_db,
    )

    invitation.expires_at = datetime.now(UTC) - timedelta(days=1)
    test_db.commit()

    assert invitation.is_expired
    assert OrgInvitation.get_valid_by_token(raw_token, test_db) is None

  def test_mark_accepted_records_user(self, test_db, test_user, test_org):
    invitation, _ = OrgInvitation.create_invitation(
      org_id=test_org.id,
      email="accepted@example.com",
      role=OrgRole.MEMBER,
      invited_by=test_user.id,
      session=test_db,
    )

    invitation.mark_accepted(test_user.id, test_db)

    assert invitation.status == InvitationStatus.ACCEPTED.value
    assert invitation.accepted_user_id == test_user.id
    assert invitation.accepted_at is not None

  def test_rotate_token_invalidates_old_token(self, test_db, test_user, test_org):
    invitation, old_token = OrgInvitation.create_invitation(
      org_id=test_org.id,
      email="rotate@example.com",
      role=OrgRole.MEMBER,
      invited_by=test_user.id,
      session=test_db,
    )
    old_expiry = invitation.expires_at

    new_token = invitation.rotate_token(test_db)

    assert new_token != old_token
    assert OrgInvitation.get_valid_by_token(old_token, test_db) is None
    found = OrgInvitation.get_valid_by_token(new_token, test_db)
    assert found is not None
    assert found.id == invitation.id
    assert invitation.expires_at >= old_expiry

  def test_one_pending_invitation_per_org_and_email(self, test_db, test_user, test_org):
    OrgInvitation.create_invitation(
      org_id=test_org.id,
      email="unique@example.com",
      role=OrgRole.MEMBER,
      invited_by=test_user.id,
      session=test_db,
    )

    with pytest.raises(IntegrityError):
      OrgInvitation.create_invitation(
        org_id=test_org.id,
        email="unique@example.com",
        role=OrgRole.ADMIN,
        invited_by=test_user.id,
        session=test_db,
      )

    test_db.rollback()

    # A revoked invitation frees the slot for a fresh one
    pending = OrgInvitation.get_pending_by_org_and_email(
      test_org.id, "unique@example.com", test_db
    )
    assert pending is not None
    pending.revoke(test_db)

    replacement, _ = OrgInvitation.create_invitation(
      org_id=test_org.id,
      email="unique@example.com",
      role=OrgRole.MEMBER,
      invited_by=test_user.id,
      session=test_db,
    )
    assert replacement.status == InvitationStatus.PENDING.value

  def test_get_pending_for_org_excludes_settled(self, test_db, test_user, test_org):
    pending, _ = OrgInvitation.create_invitation(
      org_id=test_org.id,
      email="pending@example.com",
      role=OrgRole.MEMBER,
      invited_by=test_user.id,
      session=test_db,
    )
    settled, _ = OrgInvitation.create_invitation(
      org_id=test_org.id,
      email="settled@example.com",
      role=OrgRole.MEMBER,
      invited_by=test_user.id,
      session=test_db,
    )
    settled.mark_accepted(test_user.id, test_db)

    listed = OrgInvitation.get_pending_for_org(test_org.id, test_db)
    listed_ids = {i.id for i in listed}

    assert pending.id in listed_ids
    assert settled.id not in listed_ids
