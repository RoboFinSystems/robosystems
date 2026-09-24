"""The one place a user account is created (password registration and SCIM).

Only mutates the database, in one transaction; callers own validation, rate
limits, emails and audit.
"""

from dataclasses import dataclass

from sqlalchemy.orm import Session

from robosystems.config.constants import EMAIL_TOKEN_EXPIRY_HOURS
from robosystems.logger import logger
from robosystems.models.core import (
  Org,
  OrgInvitation,
  OrgLimits,
  OrgRole,
  OrgUser,
  User,
  UserToken,
)


class UserProvisioningError(Exception):
  """Base class for provisioning failures."""


class EmailAlreadyRegisteredError(UserProvisioningError):
  """The email already belongs to an existing user."""


@dataclass(frozen=True)
class ProvisionedUser:
  """Result of a successful provisioning transaction."""

  user: User
  org: Org
  org_role: OrgRole
  # Set only when requested; the caller sends it.
  verification_token: str | None


def provision_user(
  session: Session,
  *,
  email: str,
  name: str,
  password_hash: str | None,
  email_verified: bool,
  invitation: OrgInvitation | None = None,
  invited_org: Org | None = None,
  target_org: Org | None = None,
  target_org_role: OrgRole = OrgRole.MEMBER,
  external_id: str | None = None,
  create_verification_token: bool = False,
  ip_address: str | None = None,
  user_agent: str | None = None,
) -> ProvisionedUser:
  """Create a user and attach them to exactly one org, atomically.

  Org attachment, by precedence:

  - ``invitation`` + ``invited_org``: join at the invited role and accept the
    invitation (the caller has already validated email match and expiry).
  - ``target_org`` (SCIM): join at ``target_org_role``; pass
    ``password_hash=None``, an ``external_id``, and ``email_verified=True``.
  - neither: mint a personal org with the user as OWNER.

  Rolls back on any failure, leaving no partial account.
  """
  if User.get_by_email(email, session) is not None:
    raise EmailAlreadyRegisteredError("Email already registered")

  try:
    user = User.create(
      email=email,
      name=name,
      password_hash=password_hash,
      session=session,
      external_id=external_id,
      auto_commit=False,
    )

    if email_verified:
      user.email_verified = True

    verification_token: str | None = None
    if create_verification_token:
      verification_token = UserToken.create_token(
        user_id=user.id,
        token_type="email_verification",
        hours=EMAIL_TOKEN_EXPIRY_HOURS,
        session=session,
        ip_address=ip_address,
        user_agent=user_agent,
        auto_commit=False,
      )

    if invitation is not None and invited_org is not None:
      OrgUser.create(
        org_id=invited_org.id,
        user_id=user.id,
        role=invitation.role,
        session=session,
        auto_commit=False,
      )
      invitation.mark_accepted(user.id, session, auto_commit=False)
      org = invited_org
      org_role = invitation.role
    elif target_org is not None:
      OrgUser.create(
        org_id=target_org.id,
        user_id=user.id,
        role=target_org_role,
        session=session,
        auto_commit=False,
      )
      org = target_org
      org_role = target_org_role
    else:
      org = Org.create_personal_org_for_user(
        user_id=user.id,
        user_name=name,
        session=session,
        auto_commit=False,
      )
      OrgLimits.create_default_limits(org.id, session, auto_commit=False)
      org_role = OrgRole.OWNER

    session.commit()
  except Exception:
    session.rollback()
    raise

  logger.info(
    f"Provisioned user {user.id} into org {org.id}",
    extra={
      "user_id": user.id,
      "org_id": org.id,
      "org_role": org_role.value,
      "email_verified": bool(user.email_verified),
      "idp_provisioned": external_id is not None,
      "invited": invitation is not None,
    },
  )

  return ProvisionedUser(
    user=user,
    org=org,
    org_role=org_role,
    verification_token=verification_token,
  )
