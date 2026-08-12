"""Tests for the user-provisioning kernel.

This is the one place accounts are created — password registration and SCIM
both delegate here — so what matters is that every org-attachment branch
lands complete (user + membership + limits where applicable) and that a
failure anywhere leaves no partial account behind.
"""

from uuid import uuid4

import pytest

from robosystems.models.core import (
  InvitationStatus,
  Org,
  OrgInvitation,
  OrgLimits,
  OrgRole,
  OrgType,
  OrgUser,
  User,
  UserToken,
)
from robosystems.operations.user_provisioning import (
  EmailAlreadyRegisteredError,
  provision_user,
)


def _unique_email(tag: str) -> str:
  return f"{tag}+{uuid4().hex[:8]}@example.com"


def _create_org(session, name: str = "Inviting Org") -> Org:
  return Org.create(name=name, org_type=OrgType.TEAM, session=session)


class TestSelfRegistrationPath:
  def test_creates_user_with_personal_org_and_limits(self, test_db):
    email = _unique_email("selfreg")

    result = provision_user(
      test_db,
      email=email,
      name="Self Registrant",
      password_hash="hashed-password",
      email_verified=False,
    )

    assert result.user.email == email
    assert result.user.password_hash == "hashed-password"
    assert result.user.email_verified is False
    assert result.org_role == OrgRole.OWNER
    assert result.verification_token is None

    # The org attachment is complete: personal org, OWNER membership, limits.
    org = Org.get_by_id(result.org.id, test_db)
    assert org is not None
    assert org.org_type == OrgType.PERSONAL
    membership = OrgUser.get_by_org_and_user(org.id, result.user.id, test_db)
    assert membership is not None
    assert membership.role == OrgRole.OWNER
    assert OrgLimits.get_by_org_id(org.id, test_db) is not None

  def test_email_is_lowercased(self, test_db):
    email = _unique_email("MixedCase")

    result = provision_user(
      test_db,
      email=email.upper(),
      name="Mixed Case",
      password_hash="hashed-password",
      email_verified=True,
    )

    assert result.user.email == email.lower()
    assert User.get_by_email(email, test_db) is not None

  def test_verification_token_is_created_when_asked(self, test_db):
    email = _unique_email("verify")

    result = provision_user(
      test_db,
      email=email,
      name="Needs Verification",
      password_hash="hashed-password",
      email_verified=False,
      create_verification_token=True,
    )

    assert result.verification_token is not None
    assert (
      UserToken.validate_token(result.verification_token, "email_verification", test_db)
      == result.user.id
    )


class TestInvitedPath:
  def _make_invitation(self, session, email: str, role: OrgRole = OrgRole.ADMIN):
    org = _create_org(session)
    inviter = User.create(
      email=_unique_email("inviter"),
      name="Inviter",
      password_hash="hashed-password",
      session=session,
    )
    invitation, _raw = OrgInvitation.create_invitation(
      org_id=org.id, email=email, role=role, invited_by=inviter.id, session=session
    )
    return org, invitation

  def test_joins_inviting_org_at_invited_role(self, test_db):
    email = _unique_email("invited")
    org, invitation = self._make_invitation(test_db, email)

    result = provision_user(
      test_db,
      email=email,
      name="Invited User",
      password_hash="hashed-password",
      email_verified=True,
      invitation=invitation,
      invited_org=org,
    )

    assert result.org.id == org.id
    assert result.org_role == OrgRole.ADMIN
    membership = OrgUser.get_by_org_and_user(org.id, result.user.id, test_db)
    assert membership is not None
    assert membership.role == OrgRole.ADMIN

    # No personal org was minted alongside.
    assert len(OrgUser.get_user_orgs(result.user.id, test_db)) == 1

    test_db.refresh(invitation)
    assert invitation.status == InvitationStatus.ACCEPTED.value
    assert invitation.accepted_user_id == result.user.id


class TestIdpProvisionedPath:
  def test_scim_shape_lands_passwordless_in_target_org(self, test_db):
    email = _unique_email("scim")
    org = _create_org(test_db, name="Enterprise Org")

    result = provision_user(
      test_db,
      email=email,
      name="Provisioned Staffer",
      password_hash=None,
      email_verified=True,
      target_org=org,
      target_org_role=OrgRole.MEMBER,
      external_id=f"okta-{uuid4().hex[:8]}",
    )

    assert result.user.password_hash is None
    assert result.user.external_id is not None
    assert result.user.email_verified is True
    assert result.org.id == org.id
    assert result.org_role == OrgRole.MEMBER

    membership = OrgUser.get_by_org_and_user(org.id, result.user.id, test_db)
    assert membership is not None
    assert membership.role == OrgRole.MEMBER
    # The target org owns limits/billing; no personal org, no new limits row.
    assert len(OrgUser.get_user_orgs(result.user.id, test_db)) == 1


class TestDuplicateEmail:
  def test_duplicate_raises_and_creates_nothing(self, test_db):
    email = _unique_email("dupe")
    provision_user(
      test_db,
      email=email,
      name="First",
      password_hash="hashed-password",
      email_verified=True,
    )

    with pytest.raises(EmailAlreadyRegisteredError):
      provision_user(
        test_db,
        email=email.upper(),  # case-insensitive duplicate
        name="Second",
        password_hash="hashed-password",
        email_verified=True,
      )


class TestRollback:
  def test_failure_leaves_no_partial_account(self, test_db, monkeypatch):
    """A failure after user creation must roll the whole transaction back.

    The org-limits step is the last write in the self-registration branch, so
    failing it exercises the longest partial state: user + org + membership
    all pending. None of it may survive.
    """
    email = _unique_email("rollback")

    def _boom(*args, **kwargs):
      raise RuntimeError("limits table unavailable")

    monkeypatch.setattr(OrgLimits, "create_default_limits", _boom)

    with pytest.raises(RuntimeError):
      provision_user(
        test_db,
        email=email,
        name="Doomed",
        password_hash="hashed-password",
        email_verified=True,
      )

    assert User.get_by_email(email, test_db) is None
