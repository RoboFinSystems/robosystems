"""OIDC login kernel: connection config, flow state, token validation, user resolution.

The IdP authenticates; the platform mints. These functions cover everything
between the two — building the authorize redirect, holding the in-flight flow
state, exchanging and validating the ID token, and resolving the asserted
identity to a local user. Resolution is link-only: login never creates a
user, and the one-time email-match fallback is gated on SCIM provenance
(``users.external_id``), never on the email string alone.

Contract: session-in, dataclass-out, domain exceptions — the router
translates to redirects. Connection config is env-backed behind
``get_oidc_connection()``; a per-org DB lookup slots in behind the same
resolver if shared-SaaS enterprise SSO is ever wanted.
"""

import hashlib
import json
import secrets
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlencode

import httpx
import jwt as pyjwt
from authlib.integrations.httpx_client import AsyncOAuth2Client
from authlib.oauth2.rfc7636 import create_s256_code_challenge
from sqlalchemy.orm import Session

from robosystems.config import env
from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client
from robosystems.logger import logger
from robosystems.models.core import OrgUser, User, UserIdentity


class OIDCError(Exception):
  """Base class for OIDC login failures."""


class OIDCNotConfiguredError(OIDCError):
  """The deployment has no usable OIDC connection."""


class OIDCUnavailableError(OIDCError):
  """A dependency (state store, discovery document) is unreachable."""


class OIDCTokenInvalidError(OIDCError):
  """Code exchange or ID-token validation failed."""


class OIDCUserNotProvisionedError(OIDCError):
  """The IdP authenticated a subject with no linked or linkable local user."""


class OIDCUserInactiveError(OIDCError):
  """The resolved local user is deactivated."""


OIDC_SCOPES = "openid profile email"

_HTTP_TIMEOUT_SECONDS = 10.0
_METADATA_TTL_SECONDS = 3600.0
_JWKS_TTL_SECONDS = 3600.0

_metadata_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_jwks_cache: dict[str, tuple[float, dict[str, Any]]] = {}


@dataclass(frozen=True)
class OIDCConnection:
  """The deployment's single IdP connection."""

  issuer: str
  client_id: str
  client_secret: str
  redirect_uri: str


def get_oidc_connection() -> OIDCConnection:
  """Resolve the configured OIDC connection.

  The issuer is normalized without a trailing slash so ``iss`` comparisons
  are insensitive to how the operator typed the URL.
  """
  issuer = (env.SSO_OIDC_ISSUER or "").rstrip("/")
  client_id = env.SSO_OIDC_CLIENT_ID or ""
  client_secret = env.SSO_OIDC_CLIENT_SECRET or ""
  if not (issuer and client_id and client_secret):
    raise OIDCNotConfiguredError("OIDC connection is not configured")
  return OIDCConnection(
    issuer=issuer,
    client_id=client_id,
    client_secret=client_secret,
    redirect_uri=f"{env.ROBOSYSTEMS_API_URL}/v1/auth/oidc/callback",
  )


async def fetch_server_metadata(issuer: str) -> dict[str, Any]:
  """Fetch (and cache) the issuer's OIDC discovery document.

  The document's own ``issuer`` must match the configured one — a mismatch
  means misconfiguration or a hostile authorization server, and everything
  downstream (ID-token ``iss`` validation) keys off it.
  """
  cached = _metadata_cache.get(issuer)
  if cached and (time.monotonic() - cached[0]) < _METADATA_TTL_SECONDS:
    return cached[1]

  url = f"{issuer}/.well-known/openid-configuration"
  try:
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT_SECONDS) as client:
      response = await client.get(url)
      response.raise_for_status()
      metadata = response.json()
  except Exception as exc:
    logger.error(f"OIDC discovery fetch failed for {issuer}: {exc}")
    raise OIDCUnavailableError("Identity provider discovery failed") from exc

  if str(metadata.get("issuer", "")).rstrip("/") != issuer:
    raise OIDCTokenInvalidError("Discovery document issuer mismatch")
  for field in ("authorization_endpoint", "token_endpoint", "jwks_uri"):
    if not metadata.get(field):
      raise OIDCTokenInvalidError(f"Discovery document missing {field}")

  _metadata_cache[issuer] = (time.monotonic(), metadata)
  return metadata


# Public: the router mirrors this as the browser-binding cookie's max-age.
OIDC_STATE_TTL_SECONDS = 600
_STATE_TTL_SECONDS = OIDC_STATE_TTL_SECONDS
_STATE_KEY_PREFIX = "oidc:state:"


class OIDCState:
  """Single-use, 10-minute state for an in-flight OIDC login.

  Same shape as ``operations/providers/oauth_handler.OAuthState`` and for the
  same reasons: Valkey because the callback lands on an arbitrary task,
  SHA-256 keys so a store read can't replay a flow, ``GETDEL`` so redemption
  is atomically single-use, and fail-closed validation. Distinct class
  because the payload differs — a login flow has no user yet, so the state
  carries the ``nonce`` + PKCE verifier + validated ``return_to`` instead of
  a connection/user pair.
  """

  @staticmethod
  def _key(state: str) -> str:
    return f"{_STATE_KEY_PREFIX}{hashlib.sha256(state.encode()).hexdigest()}"

  @classmethod
  def create(
    cls,
    *,
    nonce: str,
    code_verifier: str,
    return_to: str | None,
    browser_state: str,
  ) -> str:
    """Mint a state token and persist what the callback needs to resume.

    ``browser_state`` is the flow's browser-binding secret: it is mirrored
    into an HttpOnly cookie by the login endpoint and re-checked at the
    callback, so the browser that completes the flow must be the one that
    started it (login-CSRF / forced-authentication defense).
    """
    state = secrets.token_urlsafe(32)
    payload = {
      "nonce": nonce,
      "code_verifier": code_verifier,
      "return_to": return_to,
      "browser_state": browser_state,
      "created_at": datetime.now(UTC).isoformat(),
    }
    try:
      client = create_redis_client(ValkeyDatabase.AUTH)
      client.setex(cls._key(state), _STATE_TTL_SECONDS, json.dumps(payload))
    except Exception as exc:
      logger.error(f"Failed to persist OIDC state: {exc}")
      raise OIDCUnavailableError("Unable to start the sign-in flow") from exc
    return state

  @classmethod
  def validate(cls, state: str) -> dict[str, Any] | None:
    """Consume a state token, returning its payload, or None if invalid.

    Fails closed: an unreachable or unparseable store reads as an invalid
    state rather than waving the callback through.
    """
    try:
      client = create_redis_client(ValkeyDatabase.AUTH)
      raw = client.getdel(cls._key(state))
    except Exception as exc:
      logger.error(f"Failed to read OIDC state: {exc}")
      return None
    if not raw:
      return None
    try:
      payload = json.loads(raw)
      if not (
        payload.get("nonce")
        and payload.get("code_verifier")
        and payload.get("browser_state")
      ):
        raise ValueError("missing nonce, code_verifier, or browser_state")
    except (ValueError, TypeError) as exc:
      logger.error(f"Discarding malformed OIDC state: {exc}")
      return None
    return payload


def parse_return_to(raw: str | None, available_apps: list[str]) -> str | None:
  """Validate a ``return_to`` value: ``app`` or ``app:/relative/path``.

  Returns the canonical string, or None for anything not provably safe — an
  invalid value degrades to the default landing rather than failing login.
  """
  if not raw:
    return None
  app, _, path = raw.partition(":")
  if app not in available_apps:
    return None
  if not path:
    return app
  if not _is_safe_relative_path(path):
    return None
  return f"{app}:{path}"


def _is_safe_relative_path(path: str) -> bool:
  # Mirrors routers/auth/utils.is_safe_relative_path (kept local to avoid an
  # operations→routers import): a single-slash-rooted path with no
  # backslash/whitespace tricks, so it can never be scheme-relative.
  return (
    path.startswith("/")
    and not path.startswith("//")
    and "\\" not in path
    and "\t" not in path
    and "\n" not in path
    and "\r" not in path
  )


def build_authorize_redirect(
  connection: OIDCConnection,
  metadata: dict[str, Any],
  *,
  return_to: str | None,
  browser_state: str,
) -> str:
  """Mint flow state and build the IdP authorize URL (code flow + PKCE S256)."""
  nonce = secrets.token_urlsafe(16)
  code_verifier = secrets.token_urlsafe(48)
  state = OIDCState.create(
    nonce=nonce,
    code_verifier=code_verifier,
    return_to=return_to,
    browser_state=browser_state,
  )
  params = {
    "response_type": "code",
    "client_id": connection.client_id,
    "redirect_uri": connection.redirect_uri,
    "scope": OIDC_SCOPES,
    "state": state,
    "nonce": nonce,
    "code_challenge": create_s256_code_challenge(code_verifier),
    "code_challenge_method": "S256",
  }
  return f"{metadata['authorization_endpoint']}?{urlencode(params)}"


async def exchange_code(
  connection: OIDCConnection,
  metadata: dict[str, Any],
  *,
  code: str,
  code_verifier: str,
) -> str:
  """Exchange the authorization code, returning the raw ID token."""
  try:
    async with AsyncOAuth2Client(
      client_id=connection.client_id,
      client_secret=connection.client_secret,
      redirect_uri=connection.redirect_uri,
      timeout=_HTTP_TIMEOUT_SECONDS,
    ) as client:
      token = await client.fetch_token(
        metadata["token_endpoint"],
        grant_type="authorization_code",
        code=code,
        code_verifier=code_verifier,
      )
  except Exception as exc:
    logger.warning(f"OIDC code exchange failed: {exc}")
    raise OIDCTokenInvalidError("Authorization code exchange failed") from exc

  id_token = token.get("id_token")
  if not id_token:
    raise OIDCTokenInvalidError("Token response carried no id_token")
  return id_token


async def _fetch_jwks(jwks_uri: str, force: bool = False) -> dict[str, Any]:
  cached = _jwks_cache.get(jwks_uri)
  if not force and cached and (time.monotonic() - cached[0]) < _JWKS_TTL_SECONDS:
    return cached[1]
  try:
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT_SECONDS) as client:
      response = await client.get(jwks_uri)
      response.raise_for_status()
      jwks = response.json()
  except Exception as exc:
    logger.error(f"OIDC JWKS fetch failed for {jwks_uri}: {exc}")
    raise OIDCUnavailableError("Identity provider keys unavailable") from exc
  _jwks_cache[jwks_uri] = (time.monotonic(), jwks)
  return jwks


async def _signing_key_for(jwks_uri: str, id_token: str) -> pyjwt.PyJWK:
  try:
    header = pyjwt.get_unverified_header(id_token)
  except pyjwt.PyJWTError as exc:
    raise OIDCTokenInvalidError("Malformed id_token header") from exc
  kid = header.get("kid")

  key = _key_by_kid(await _fetch_jwks(jwks_uri), kid)
  if key is None:
    # Unknown kid usually means the IdP rotated keys — refetch once.
    key = _key_by_kid(await _fetch_jwks(jwks_uri, force=True), kid)
  if key is None:
    raise OIDCTokenInvalidError("No matching signing key for id_token")
  return key


def _key_by_kid(jwks: dict[str, Any], kid: str | None) -> pyjwt.PyJWK | None:
  for entry in jwks.get("keys", []):
    if kid is not None and entry.get("kid") != kid:
      continue
    try:
      return pyjwt.PyJWK.from_dict(entry)
    except pyjwt.PyJWTError:
      continue
  return None


async def validate_id_token(
  id_token: str,
  *,
  metadata: dict[str, Any],
  connection: OIDCConnection,
  nonce: str,
) -> dict[str, Any]:
  """Verify the ID token (signature, iss, aud, exp, nonce) and return claims."""
  key = await _signing_key_for(metadata["jwks_uri"], id_token)
  try:
    claims = pyjwt.decode(
      id_token,
      key=key.key,
      algorithms=["RS256"],
      audience=connection.client_id,
      issuer=metadata["issuer"],
      leeway=120,
      options={"require": ["exp", "iat", "iss", "aud", "sub"]},
    )
  except pyjwt.PyJWTError as exc:
    raise OIDCTokenInvalidError(f"id_token rejected: {exc}") from exc

  # str() coercion, not just `or ""`: a non-string nonce claim (valid JSON,
  # invalid per spec) would make compare_digest raise TypeError, which would
  # escape the callback's OIDCTokenInvalidError handler and 500 instead of
  # redirecting. Coercing keeps every rejection on the same path.
  token_nonce = str(claims.get("nonce") or "")
  if not secrets.compare_digest(token_nonce, nonce):
    raise OIDCTokenInvalidError("id_token nonce mismatch")

  aud = claims.get("aud")
  if isinstance(aud, list) and len(aud) > 1:
    if claims.get("azp") != connection.client_id:
      raise OIDCTokenInvalidError("id_token azp mismatch")

  return claims


@dataclass(frozen=True)
class OIDCResolution:
  """Outcome of resolving an IdP-asserted identity to a local user."""

  user: User
  # True when this login wrote the user_identities link (first login of a
  # SCIM-provisioned account) rather than finding an existing one.
  linked: bool


def resolve_oidc_user(
  session: Session,
  *,
  issuer: str,
  subject: str,
  email: str | None,
  email_verified: bool | None = None,
  binding_value: str | None = None,
) -> OIDCResolution:
  """Resolve ``(issuer, sub)`` to a local user — link-only, never JIT.

  Primary lookup is the ``user_identities`` link. On a miss, exactly one
  fallback: an active user whose email matches AND whose SCIM ``external_id``
  equals ``binding_value`` (the ID-token claim named by
  ``SSO_OIDC_BINDING_CLAIM`` — ``sub`` by default; Okta sends the same user id
  in both channels) AND who has no identity for this issuer yet — then the
  link is written and is the lookup forever after. Equality, not mere
  presence, is load-bearing twice over: presence alone let anyone controlling
  a matching mailbox in the customer's IdP bind onto a locally-created
  account, and it accepted an empty-string ``external_id`` as provenance.
  A missing/empty ``binding_value`` never links.

  When ``ENTERPRISE_ORG_ID`` pins the deployment's org, membership is
  required on *every* resolution, not only at link time: an identity linked
  before the org was pinned, or a user since removed from the pinned org,
  stops resolving. (The fallback additionally checks membership before
  writing a link, so a refused resolution never leaves one behind.)

  The email-match fallback additionally refuses a claim whose ``email`` the
  IdP marked *unverified* (``email_verified: false``). It does not *require*
  the claim to be present-and-true — Entra omits ``email_verified`` entirely
  — but an explicit false is exactly the case OIDC Core §5.7 warns against
  using as an identifier, so binding on it is refused.

  The ``is_active`` check runs even for a valid link because IdP assignment
  and SCIM assignment can drift apart (Okta runs them as two apps): a
  SCIM-deactivated user may still reach the callback with a valid ID token.
  """
  linked = False
  identity = UserIdentity.get_by_issuer_subject(issuer, subject, session)

  if identity is not None:
    user = User.get_by_id(str(identity.user_id), session)
    if user is None:
      raise OIDCUserNotProvisionedError("Identity link points at no user")
  else:
    user = User.get_by_email(email, session) if email else None
    if user is None:
      raise OIDCUserNotProvisionedError("No provisioned user for this identity")
    eligible = (
      bool(user.is_active)
      and bool(binding_value)
      and user.external_id == binding_value
      and email_verified is not False
      and UserIdentity.get_by_user_and_issuer(str(user.id), issuer, session) is None
    )
    if eligible and env.ENTERPRISE_ORG_ID:
      eligible = (
        OrgUser.get_by_org_and_user(env.ENTERPRISE_ORG_ID, str(user.id), session)
        is not None
      )
    if not eligible:
      raise OIDCUserNotProvisionedError("No provisioned user for this identity")
    identity = UserIdentity.create(
      user_id=str(user.id),
      issuer=issuer,
      subject=subject,
      email_at_link=email or "",
      session=session,
    )
    linked = True

  if env.ENTERPRISE_ORG_ID and (
    OrgUser.get_by_org_and_user(env.ENTERPRISE_ORG_ID, str(user.id), session) is None
  ):
    raise OIDCUserNotProvisionedError("No provisioned user for this identity")

  if not user.is_active:
    raise OIDCUserInactiveError("User is deactivated")

  identity.touch_login(session)
  return OIDCResolution(user=user, linked=linked)
