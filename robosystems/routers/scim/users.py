"""SCIM 2.0 Users endpoints, scoped to the bearer token's org.

Create/Read/Update/Deactivate plus the one filter Okta sends before creating
(``userName eq``), which is the brownfield dedupe path. Everything delegates
to ``operations/user_provisioning`` for create and to ``User.deactivate`` /
``User.activate`` for the active toggle — the latter is the offboarding
kill-switch (session_version bump + API-key revocation) auditors buy.
"""

import re
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request, status
from sqlalchemy.orm import Session

from ...config import env
from ...db.platform import get_db_session
from ...logger import get_logger
from ...models.api.scim import (
  ERROR_SCHEMA,
  ScimEmail,
  ScimListResponse,
  ScimMeta,
  ScimName,
  ScimPatchRequest,
  ScimUserCreateRequest,
  ScimUserResource,
)
from ...models.core import Org, OrgRole, OrgUser, User
from ...operations.user_provisioning import (
  EmailAlreadyRegisteredError,
  provision_user,
)
from ...security.audit_logger import SecurityAuditLogger, SecurityEventType

logger = get_logger(__name__)

router = APIRouter()

# `userName eq "value"` — the only filter Okta/Entra send. Anything else is
# unsupported (an empty ListResponse, never a 500).
_USERNAME_EQ = re.compile(r'^\s*userName\s+eq\s+"(?P<value>[^"]*)"\s*$', re.IGNORECASE)


def _scim_error(
  status_code: int, detail: str, scim_type: str | None = None
) -> HTTPException:
  body: dict[str, Any] = {
    "schemas": [ERROR_SCHEMA],
    "detail": detail,
    "status": str(status_code),
  }
  if scim_type:
    body["scimType"] = scim_type
  return HTTPException(status_code=status_code, detail=body)


def _default_role() -> OrgRole:
  # Boot validation rejects anything outside member/admin; this is the
  # runtime belt so a bypassed check can never mint owners.
  try:
    role = OrgRole(env.SSO_DEFAULT_ROLE)
  except ValueError:
    return OrgRole.MEMBER
  return role if role in (OrgRole.MEMBER, OrgRole.ADMIN) else OrgRole.MEMBER


def _to_resource(user: User) -> ScimUserResource:
  location = f"{env.ROBOSYSTEMS_API_URL}/scim/v2/Users/{user.id}"
  created = user.created_at.isoformat() if user.created_at else None
  modified = user.updated_at.isoformat() if user.updated_at else None
  return ScimUserResource(
    id=str(user.id),
    external_id=user.external_id,
    user_name=user.email,
    name=ScimName(formatted=user.name),
    emails=[ScimEmail(value=user.email, primary=True)],
    active=bool(user.is_active),
    meta=ScimMeta(location=location, created=created, last_modified=modified),
  )


def _org_user_or_404(org_id: str, user_id: str, session: Session) -> User:
  """Resolve a user that belongs to the token's org, else 404.

  Membership is the scope boundary: a token for org A must never read or
  mutate a user in org B, and must not leak that user's existence — so a
  cross-org id is indistinguishable from an unknown one.
  """
  membership = OrgUser.get_by_org_and_user(org_id, user_id, session)
  if membership is None:
    raise _scim_error(status.HTTP_404_NOT_FOUND, "User not found")
  user = User.get_by_id(user_id, session)
  if user is None:
    raise _scim_error(status.HTTP_404_NOT_FOUND, "User not found")
  return user


def _coerce_active(patch: ScimPatchRequest) -> bool | None:
  """Extract the desired ``active`` state from a PATCH, tolerating Entra.

  Okta is RFC-compliant; Entra (without the compat flag) sends capitalized
  ``op`` and ``active`` as the string ``"False"``. Accept both shapes for the
  one operation that must never silently fail — deactivation.
  """
  for operation in patch.operations:
    op = (operation.op or "").lower()
    if op not in ("replace", "add"):
      continue
    path = (operation.path or "").lower()
    raw: Any = None
    if path == "active":
      raw = operation.value
    elif operation.path is None and isinstance(operation.value, dict):
      for key, val in operation.value.items():
        if key.lower() == "active":
          raw = val
          break
    if raw is None:
      continue
    if isinstance(raw, bool):
      return raw
    if isinstance(raw, str):
      lowered = raw.strip().lower()
      if lowered in ("true", "false"):
        return lowered == "true"
  return None


def _audit_active_change(user: User, org_id: str, event: SecurityEventType) -> None:
  SecurityAuditLogger.log_security_event(
    event_type=event,
    user_id=str(user.id),
    details={"action": event.value, "org_id": org_id, "external_id": user.external_id},
    risk_level="medium",
  )


def _set_active(user: User, active: bool, org_id: str, session: Session) -> None:
  transition = bool(user.is_active) != active

  if active:
    if not transition:
      return
    user.activate(session)
    _audit_active_change(user, org_id, SecurityEventType.SCIM_USER_REACTIVATED)
    return

  # Deactivation runs even when the DB flag is already false: the flag flip
  # is only part of the kill switch — an IdP retry after a 503 below is
  # retrying the session-cache invalidation and API-key revocation, not the
  # column write, so a short-circuit here would make the retry a no-op.
  result = user.deactivate(session)
  if transition:
    _audit_active_change(user, org_id, SecurityEventType.SCIM_USER_DEACTIVATED)
  if not result.fully_applied:
    # The account is inactive in the DB, but revocation side effects did not
    # all take. Fail the request so the IdP retries — deactivate is
    # idempotent and re-asserts them — instead of reporting an offboarding
    # complete that isn't.
    raise _scim_error(
      status.HTTP_503_SERVICE_UNAVAILABLE,
      "Deactivation incompletely applied; retry",
    )


@router.get("/Users", include_in_schema=False)
async def list_users(
  request: Request,
  filter: str | None = Query(default=None),
  startIndex: int = Query(default=1, ge=1),
  count: int = Query(default=100, ge=0, le=200),
) -> ScimListResponse:
  org_id = request.state.scim_org_id
  session = next(get_db_session())
  try:
    base = (
      session.query(User)
      .join(OrgUser, User.id == OrgUser.user_id)
      .filter(OrgUser.org_id == org_id)
    )

    if filter:
      match = _USERNAME_EQ.match(filter)
      if not match:
        # Unsupported filter → empty result, never an error.
        return ScimListResponse(
          total_results=0, start_index=startIndex, items_per_page=0
        )
      base = base.filter(User.email == match.group("value").lower())

    total = base.count()
    rows = base.order_by(User.created_at).offset(startIndex - 1).limit(count).all()
    resources = [_to_resource(u) for u in rows]
    return ScimListResponse(
      total_results=total,
      start_index=startIndex,
      items_per_page=len(resources),
      resources=resources,
    )
  finally:
    session.close()


@router.post("/Users", status_code=status.HTTP_201_CREATED, include_in_schema=False)
async def create_user(
  request: Request, body: ScimUserCreateRequest
) -> ScimUserResource:
  org_id = request.state.scim_org_id
  session = next(get_db_session())
  try:
    org = Org.get_by_id(org_id, session)
    if org is None:
      raise _scim_error(status.HTTP_404_NOT_FOUND, "Provisioning org not found")

    email = body.user_name.strip().lower()
    display_name = (
      (body.name.formatted if body.name else None)
      or " ".join(
        p
        for p in [
          body.name.given_name if body.name else None,
          body.name.family_name if body.name else None,
        ]
        if p
      )
      or email
    )

    try:
      provisioned = provision_user(
        session,
        email=email,
        name=display_name,
        password_hash=None,
        email_verified=True,  # the IdP asserted the mailbox
        target_org=org,
        target_org_role=_default_role(),
        external_id=body.external_id,
      )
    except EmailAlreadyRegisteredError:
      raise _scim_error(
        status.HTTP_409_CONFLICT,
        "A user with this userName already exists",
        scim_type="uniqueness",
      )

    user = provisioned.user
    if not body.active:
      user.deactivate(session)

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.SCIM_USER_PROVISIONED,
      user_id=str(user.id),
      details={
        "action": "scim_user_provisioned",
        "org_id": org_id,
        "external_id": user.external_id,
      },
      risk_level="low",
    )
    return _to_resource(user)
  finally:
    session.close()


@router.get("/Users/{user_id}", include_in_schema=False)
async def get_user(request: Request, user_id: str) -> ScimUserResource:
  session = next(get_db_session())
  try:
    user = _org_user_or_404(request.state.scim_org_id, user_id, session)
    return _to_resource(user)
  finally:
    session.close()


@router.put("/Users/{user_id}", include_in_schema=False)
async def replace_user(
  request: Request, user_id: str, body: ScimUserCreateRequest
) -> ScimUserResource:
  org_id = request.state.scim_org_id
  session = next(get_db_session())
  try:
    user = _org_user_or_404(org_id, user_id, session)

    new_name = None
    if body.name:
      new_name = body.name.formatted or " ".join(
        p for p in [body.name.given_name, body.name.family_name] if p
      )
    if new_name and new_name != user.name:
      user.update(session, name=new_name)

    _set_active(user, bool(body.active), org_id, session)

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.SCIM_USER_UPDATED,
      user_id=str(user.id),
      details={"action": "scim_user_updated", "org_id": org_id},
      risk_level="low",
    )
    session.refresh(user)
    return _to_resource(user)
  finally:
    session.close()


@router.patch("/Users/{user_id}", include_in_schema=False)
async def patch_user(
  request: Request, user_id: str, body: ScimPatchRequest
) -> ScimUserResource:
  org_id = request.state.scim_org_id
  session = next(get_db_session())
  try:
    user = _org_user_or_404(org_id, user_id, session)

    desired_active = _coerce_active(body)
    if desired_active is None:
      # Nothing recognized means nothing applied — say so. A 200 here would
      # tell the IdP an update (possibly a deactivation in a shape we don't
      # parse) succeeded when the account is unchanged.
      raise _scim_error(
        status.HTTP_400_BAD_REQUEST,
        "PATCH contained no supported operation (only 'active' is supported)",
        scim_type="invalidValue",
      )
    _set_active(user, desired_active, org_id, session)

    session.refresh(user)
    return _to_resource(user)
  finally:
    session.close()


@router.delete(
  "/Users/{user_id}", status_code=status.HTTP_204_NO_CONTENT, include_in_schema=False
)
async def delete_user(request: Request, user_id: str) -> None:
  """SCIM DELETE maps to deactivate, idempotently — never destructive.

  Hard deletion stays the admin-CLI operation with its blocker planner.
  """
  org_id = request.state.scim_org_id
  session = next(get_db_session())
  try:
    user = _org_user_or_404(org_id, user_id, session)
    _set_active(user, False, org_id, session)
  finally:
    session.close()
