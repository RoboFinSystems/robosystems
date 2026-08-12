"""Bootstrap a dedicated tenant's SCIM provisioning.

Stands up the one ``ENTERPRISE`` org SCIM users land in, and mints the bearer
token the customer's IdP presents. A step in the dedicated-tenant lifecycle,
run once per tenant from the admin CLI. The raw token is returned exactly once
— it is never recoverable after this call.
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.core import Org, OrgLimits, OrgType, ScimToken

DEFAULT_TOKEN_LIFETIME_DAYS = 365


class ScimBootstrapError(Exception):
  """Base class for bootstrap failures."""


class OrgNotFoundError(ScimBootstrapError):
  """A supplied org id does not resolve."""


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

  Every token expires — there is deliberately no "never" option (RFC 7644
  expects limited-lifetime bearers). Rotation is overlap-based: mint the
  replacement, swap it into the IdP, then revoke the old token; two live
  tokens are legal during the swap.
  """
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
