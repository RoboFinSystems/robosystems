"""Client resolution, redirect-URI matching, registration validation.

Redirect matching is exact (scheme, host, path, query) with one carve-out
the MCP spec and RFC 8252 require: an ``http`` loopback redirect
(``localhost`` / ``127.0.0.1`` / ``[::1]``) matches regardless of port,
because a native client (Claude Code, an IDE) binds an ephemeral port per
run. Everything else — including ``http`` to any non-loopback host — is
refused at registration, and a registered URI is checked again at use.

A redirect URI is also required to be canonical enough that every parser
reads the same authority from it: RFC 3986 ASCII only, no userinfo, a
numeric port. Python's ``urlsplit`` treats a backslash as an ordinary host
character and takes the host from after an ``@``; the WHATWG parser a
browser runs ends the authority at the backslash. Without this rule the
host we validate and show on the consent page is not the host the browser
delivers the authorization code to.
"""

import ipaddress
from dataclasses import dataclass
from datetime import timedelta
from urllib.parse import urlsplit

from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.core.user.oauth_client import (
  AUTH_METHOD_NONE,
  SUPPORTED_AUTH_METHODS,
  OAuthClient,
)

# RFC 7591 hardening caps (spec §6). Tuned after Phase 0 shows how often
# the IDE-family clients re-register.
DCR_MAX_REDIRECT_URIS = 10
DCR_MAX_CLIENT_NAME_LENGTH = 100
DCR_MAX_URI_LENGTH = 2048
DCR_PER_IP_DAILY_CAP = 50
DCR_PER_IP_WINDOW = timedelta(hours=24)

_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})

# RFC 3986 unreserved + reserved + percent. Anything outside — a backslash,
# whitespace, control characters, non-ASCII — makes parsers disagree.
_URI_ALLOWED_CHARS = frozenset(
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
  "-._~:/?#[]@!$&'()*+,;=%"
)


class ClientError(Exception):
  """An OAuth error attributable to the client identity or its metadata.

  ``error`` is the RFC 6749 / RFC 7591 error code; ``description`` is safe
  to return to the caller (never echoes attacker-controlled values).
  """

  def __init__(self, error: str, description: str):
    super().__init__(description)
    self.error = error
    self.description = description


@dataclass(frozen=True)
class RegistrationMetadata:
  client_name: str
  redirect_uris: list[str]
  token_endpoint_auth_method: str
  client_uri: str | None
  logo_uri: str | None
  scope: str | None


def _is_loopback_host(host: str) -> bool:
  bare = host.strip("[]").lower()
  if bare in _LOOPBACK_HOSTS:
    return True
  try:
    return ipaddress.ip_address(bare).is_loopback
  except ValueError:
    return False


def is_loopback_redirect(uri: str) -> bool:
  parts = urlsplit(uri)
  return parts.scheme == "http" and _is_loopback_host(parts.hostname or "")


def redirect_uri_matches(registered: str, presented: str) -> bool:
  """Exact match, except that a registered loopback redirect ignores the
  presented port (RFC 8252 §7.3)."""
  if registered == presented:
    return True
  if not is_loopback_redirect(registered) or not is_loopback_redirect(presented):
    return False
  reg = urlsplit(registered)
  pre = urlsplit(presented)
  return (
    reg.scheme == pre.scheme
    and (reg.hostname or "").lower() == (pre.hostname or "").lower()
    and reg.path == pre.path
    and reg.query == pre.query
  )


def validate_redirect_uri(uri: object) -> str | None:
  """Reason a redirect URI is unregistrable, or ``None`` when acceptable.

  HTTPS to any host, or HTTP to a loopback host. Native-app custom schemes
  (``cursor://``, ``vscode://``) are accepted as absolute URIs with a host
  or path — they cannot be hijacked by our redirect since the OS resolves
  them — but ``javascript:``/``data:`` and friends are not.
  """
  if not isinstance(uri, str) or not uri or len(uri) > DCR_MAX_URI_LENGTH:
    return "redirect_uris entries must be non-empty strings"
  if any(ch not in _URI_ALLOWED_CHARS for ch in uri):
    return "redirect_uris entries must be ASCII URIs without whitespace or backslashes"
  parts = urlsplit(uri)
  if parts.fragment:
    return "redirect_uris entries must not contain a fragment"
  if "@" in parts.netloc:
    return "redirect_uris entries must not contain userinfo"
  try:
    _ = parts.port
  except ValueError:
    return "redirect_uris entries must have a numeric port"
  scheme = parts.scheme.lower()
  if not scheme:
    return "redirect_uris entries must be absolute URIs"
  if scheme == "https":
    return None if parts.hostname else "https redirect_uris require a host"
  if scheme == "http":
    if _is_loopback_host(parts.hostname or ""):
      return None
    return "http redirect_uris are only permitted for loopback hosts"
  if scheme in {"javascript", "data", "file", "blob", "vbscript", "about"}:
    return f"{scheme}: redirect_uris are not permitted"
  # Custom native-app scheme (RFC 8252 §7.1): must still be a real URI.
  if not (parts.netloc or parts.path):
    return "custom-scheme redirect_uris must include a host or path"
  return None


def validate_registration_metadata(payload: object) -> RegistrationMetadata:
  """Validate an RFC 7591 request body. Raises ``ClientError`` with the
  RFC 7591 error codes (``invalid_redirect_uri`` / ``invalid_client_metadata``)."""
  if not isinstance(payload, dict):
    raise ClientError("invalid_client_metadata", "Request body must be a JSON object")

  redirect_uris = payload.get("redirect_uris")
  if not isinstance(redirect_uris, list) or not redirect_uris:
    raise ClientError("invalid_redirect_uri", "redirect_uris is required")
  if len(redirect_uris) > DCR_MAX_REDIRECT_URIS:
    raise ClientError(
      "invalid_redirect_uri", f"At most {DCR_MAX_REDIRECT_URIS} redirect_uris"
    )
  for uri in redirect_uris:
    reason = validate_redirect_uri(uri)
    if reason:
      raise ClientError("invalid_redirect_uri", reason)
  deduped = list(dict.fromkeys(redirect_uris))

  client_name = payload.get("client_name")
  if client_name is None:
    client_name = urlsplit(deduped[0]).hostname or "MCP client"
  if not isinstance(client_name, str) or not client_name.strip():
    raise ClientError("invalid_client_metadata", "client_name must be a string")
  client_name = client_name.strip()[:DCR_MAX_CLIENT_NAME_LENGTH]

  auth_method = payload.get("token_endpoint_auth_method", AUTH_METHOD_NONE)
  if auth_method not in SUPPORTED_AUTH_METHODS:
    raise ClientError(
      "invalid_client_metadata",
      "token_endpoint_auth_method must be one of none, client_secret_post, "
      "client_secret_basic",
    )

  # Clients list every grant they can speak (VS Code's document includes
  # device_code); we only require that the one we issue is among them.
  grant_types = payload.get("grant_types")
  if grant_types is not None:
    if not isinstance(grant_types, list) or "authorization_code" not in grant_types:
      raise ClientError(
        "invalid_client_metadata", "grant_types must include authorization_code"
      )
  response_types = payload.get("response_types")
  if response_types is not None and (
    not isinstance(response_types, list) or any(r != "code" for r in response_types)
  ):
    raise ClientError("invalid_client_metadata", "response_types may only be code")

  def _optional_https(field: str) -> str | None:
    value = payload.get(field)
    if value is None:
      return None
    if (
      not isinstance(value, str)
      or len(value) > DCR_MAX_URI_LENGTH
      or urlsplit(value).scheme.lower() != "https"
    ):
      raise ClientError("invalid_client_metadata", f"{field} must be an https URL")
    return value

  scope = payload.get("scope")
  if scope is not None and not isinstance(scope, str):
    raise ClientError("invalid_client_metadata", "scope must be a string")

  return RegistrationMetadata(
    client_name=client_name,
    redirect_uris=deduped,
    token_endpoint_auth_method=auth_method,
    client_uri=_optional_https("client_uri"),
    logo_uri=_optional_https("logo_uri"),
    scope=scope[:200] if scope else None,
  )


def register_dynamic_client(
  payload: object, *, registration_ip: str | None, session: Session
) -> tuple[OAuthClient, str | None]:
  """Validate and persist an RFC 7591 registration, enforcing the per-IP
  daily cap on top of the endpoint's burst limit."""
  metadata = validate_registration_metadata(payload)
  if registration_ip:
    recent = OAuthClient.count_recent_dynamic_registrations(
      registration_ip, DCR_PER_IP_WINDOW, session
    )
    if recent >= DCR_PER_IP_DAILY_CAP:
      raise ClientError(
        "invalid_client_metadata", "Registration limit reached; try again later"
      )
  return OAuthClient.register_dynamic(
    client_name=metadata.client_name,
    redirect_uris=metadata.redirect_uris,
    token_endpoint_auth_method=metadata.token_endpoint_auth_method,
    client_uri=metadata.client_uri,
    logo_uri=metadata.logo_uri,
    scope=metadata.scope,
    registration_ip=registration_ip,
    session=session,
  )


def resolve_client(
  client_id: str | None, session: Session, *, allow_stale_mirror: bool = False
) -> OAuthClient:
  """The registered, usable client for a presented ``client_id``.

  An HTTPS ``client_id`` is a Client ID Metadata Document: its document is
  fetched (or served from cache), validated with the same rules as a
  dynamic registration, and mirrored into an ``oauth_clients`` row so the
  rest of the flow — redirect matching, grants, the consent page — sees
  one client model. Raises ``invalid_client`` for unknown, deactivated, and
  expired-unused registrations alike — indistinguishable to the caller.

  ``allow_stale_mirror`` is for the token and revocation endpoints, where
  the client's identity only has to match the grant being refreshed: when
  the document cannot be fetched or no longer validates, the mirrored row
  (the last document that did) stands in, so an outage at the metadata
  host does not take every refresh down with it. The authorization
  endpoint never takes the fallback — a redirect URI must come from a
  live document.
  """
  if not client_id or not isinstance(client_id, str) or len(client_id) > 512:
    raise ClientError("invalid_client", "Unknown client")

  from .cimd import get_client_metadata, is_cimd_client_id, trusted_cimd_host

  if is_cimd_client_id(client_id):
    existing = OAuthClient.get_by_client_id(client_id, session)
    if existing is not None and not existing.is_active:
      # Operator-deactivated: the document is not consulted again.
      raise ClientError("invalid_client", "Unknown client")
    try:
      document = dict(get_client_metadata(client_id))
      # CIMD clients authenticate with PKCE alone (``none``); a document that
      # names an auth method we do not speak (private_key_jwt) still gets the
      # public-client treatment rather than a rejection.
      document["token_endpoint_auth_method"] = AUTH_METHOD_NONE
      metadata = validate_registration_metadata(document)
    except ClientError as exc:
      if allow_stale_mirror and existing is not None and existing.is_usable:
        logger.warning(
          "CIMD document unavailable; serving the mirrored registration",
          extra={"oauth_client_id": existing.id, "error": exc.error},
        )
        return existing
      raise
    return OAuthClient.upsert_cimd(
      client_id=client_id,
      client_name=metadata.client_name,
      redirect_uris=metadata.redirect_uris,
      client_uri=metadata.client_uri,
      logo_uri=metadata.logo_uri,
      scope=metadata.scope,
      is_trusted=trusted_cimd_host(client_id),
      session=session,
    )

  client = OAuthClient.get_by_client_id(client_id, session)
  if client is None or not client.is_usable:
    raise ClientError("invalid_client", "Unknown client")
  return client


def authenticate_client(client: OAuthClient, presented_secret: str | None) -> None:
  """Token-endpoint client authentication.

  Public clients (``none``) carry no secret and must not present one that
  verifies against nothing; confidential clients must present theirs.
  Raises ``invalid_client`` on failure.
  """
  if client.is_confidential:
    if not client.verify_secret(presented_secret):
      raise ClientError("invalid_client", "Client authentication failed")
    return
  # A public client sending a secret is a misconfiguration, not an attack;
  # ignore it rather than fail the flow.


def pick_redirect_uri(client: OAuthClient, presented: str | None) -> str:
  """Resolve the redirect for an authorization request: the presented one
  when it matches a registration, else the sole registered one when the
  client omitted it, else ``invalid_request``.

  Registered URIs are re-validated here, so a row written under an earlier,
  looser rule is refused at use and not only at registration."""
  stored: list[str] = list(client.redirect_uris or [])
  registered = [uri for uri in stored if validate_redirect_uri(uri) is None]
  if presented:
    if not isinstance(presented, str) or len(presented) > DCR_MAX_URI_LENGTH:
      raise ClientError("invalid_request", "redirect_uri is not registered")
    for uri in registered:
      if redirect_uri_matches(uri, presented):
        return presented
    raise ClientError("invalid_request", "redirect_uri is not registered")
  if len(registered) == 1 and not is_loopback_redirect(registered[0]):
    return registered[0]
  raise ClientError("invalid_request", "redirect_uri is required")
