"""Bootstrap a dedicated tenant's SCIM provisioning.

Creates the ``ENTERPRISE`` org SCIM users land in and mints the IdP's bearer
token, returned once and never recoverable.
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy.orm import Session

from robosystems.config import env
from robosystems.logger import logger
from robosystems.models.core import Org, OrgLimits, OrgType, ScimToken
from robosystems.models.core.user.scim_token import DEFAULT_TOKEN_LIFETIME_DAYS


class ScimBootstrapError(Exception):
  """Base class for bootstrap failures."""


class OrgNotFoundError(ScimBootstrapError):
  """A supplied org id does not resolve."""


class OrgBoundaryError(ScimBootstrapError):
  """The deployment is pinned to one enterprise org and this isn't it."""


@dataclass(frozen=True)
class ScimBootstrapResult:
  org_id: str
  org_name: str
  scim_token_id: str
  # Shown once; only its hash is stored.
  raw_token: str
  expires_at: datetime


def bootstrap_scim(
  session: Session,
  *,
  org_name: str | None = None,
  org_id: str | None = None,
  token_name: str = "scim-provisioning",
  expires_in_days: int = DEFAULT_TOKEN_LIFETIME_DAYS,
) -> ScimBootstrapResult:
  """Create-or-reuse the enterprise org and mint a SCIM token for it.

  Pass ``org_id`` to attach a token to an existing org, or ``org_name`` to
  create a fresh ``ENTERPRISE`` org (with default limits) first.

  Every token expires (no "never" option). Rotate by overlap: mint, swap into
  the IdP, then revoke the old one.
  """
  # A pinned deployment mints only for its own org.
  if env.ENTERPRISE_ORG_ID:
    if org_id != env.ENTERPRISE_ORG_ID:
      raise OrgBoundaryError(
        f"This deployment is pinned to org {env.ENTERPRISE_ORG_ID}; "
        "pass that org_id (org_name creation is disabled once pinned)"
      )

  if org_id is not None:
    org = Org.get_by_id(org_id, session)
    if org is None:
      raise OrgNotFoundError(f"Org {org_id} not found")
  else:
    org = Org.create(
      name=org_name or "Enterprise",
      org_type=OrgType.ENTERPRISE,
      session=session,
    )
    OrgLimits.create_default_limits(org.id, session)

  expires_at = datetime.now(UTC) + timedelta(days=expires_in_days)
  token, raw_token = ScimToken.create(
    org.id, token_name, session, expires_at=expires_at
  )

  logger.info(
    f"Bootstrapped SCIM for org {org.id}",
    extra={"org_id": org.id, "scim_token_id": token.id, "token_name": token_name},
  )
  return ScimBootstrapResult(
    org_id=str(org.id),
    org_name=str(org.name),
    scim_token_id=str(token.id),
    raw_token=raw_token,
    expires_at=expires_at,
  )


def revoke_scim_token(session: Session, token_id: str) -> bool:
  """Revoke a SCIM token by id. Returns False if it does not exist."""
  token = ScimToken.get_by_id(token_id, session)
  if token is None:
    return False
  token.revoke(session)
  return True
