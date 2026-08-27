"""Client ID Metadata Documents (CIMD).

A client identifies itself with an HTTPS URL whose document describes it —
no registration round-trip, and the same ``client_id`` for every server.
Claude, ChatGPT and VS Code all prefer this over dynamic registration
when the authorization server advertises it.

The document fetch is the platform's only server-side request to an
attacker-chosen URL, so it is fenced: HTTPS only, a real hostname (not an
address literal), every resolved address public, no redirects followed,
a small size cap, a short timeout, and the document's ``client_id`` must
equal the URL exactly. Resolution happens before the request and again
inside the HTTP client (there is no pin between the two — a DNS-rebinding
window remains), so the guard is a filter on what can be asked for, not a
proof of where the bytes came from; the fetch reads nothing but a JSON
document and never follows a redirect, which keeps that window's blast
radius to one GET.

Documents are cached in Valkey per the response's ``Cache-Control`` (clamped),
so a client that reconnects reuses the validated document without a fetch.
"""

import hashlib
import ipaddress
import json
import re
import socket
from typing import Any
from urllib.parse import urlsplit

import httpx

from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client
from robosystems.logger import logger

from .clients import ClientError

CIMD_MAX_BYTES = 64 * 1024
CIMD_TIMEOUT_SECONDS = 5.0
CIMD_CACHE_MIN_SECONDS = 60
CIMD_CACHE_MAX_SECONDS = 24 * 3600
CIMD_CACHE_DEFAULT_SECONDS = 3600
CIMD_MAX_URL_LENGTH = 512

# Hosts whose documents render on the consent page without the unknown-client
# warning. The fetch, validation and redirect rules are identical for every
# host — trust changes only what the user is told.
CIMD_TRUSTED_HOSTS = frozenset(
  {
    "claude.ai",
    "claude.com",
    "chatgpt.com",
    "vscode.dev",
    "insiders.vscode.dev",
  }
)

_CACHE_KEY_PREFIX = "oauth:cimd:"
_MAX_AGE_RE = re.compile(r"max-age=(\d+)")


def is_cimd_client_id(client_id: object) -> bool:
  """Whether a presented ``client_id`` is a metadata-document URL.

  HTTPS, a hostname, a path beyond ``/``, no userinfo, query or fragment —
  the spec's shape, and enough to rule out every opaque id we mint.
  """
  if not isinstance(client_id, str) or len(client_id) > CIMD_MAX_URL_LENGTH:
    return False
  parts = urlsplit(client_id)
  return bool(
    parts.scheme == "https"
    and parts.hostname
    and "@" not in parts.netloc
    and parts.path
    and parts.path != "/"
    and not parts.query
    and not parts.fragment
  )


def trusted_cimd_host(client_id: str) -> bool:
  host = (urlsplit(client_id).hostname or "").lower()
  return host in CIMD_TRUSTED_HOSTS


def _assert_public_host(host: str) -> None:
  """Refuse address literals and any name that resolves to a non-public
  address (loopback, private, link-local, multicast, reserved, unspecified)."""
  try:
    ipaddress.ip_address(host.strip("[]"))
    raise ClientError("invalid_client", "client_id must use a hostname")
  except ValueError:
    pass
  try:
    infos = socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)
  except socket.gaierror as exc:
    raise ClientError("invalid_client", "client_id host does not resolve") from exc
  if not infos:
    raise ClientError("invalid_client", "client_id host does not resolve")
  for info in infos:
    address = ipaddress.ip_address(info[4][0])
    if not address.is_global:
      raise ClientError("invalid_client", "client_id host is not publicly routable")


def _cache_ttl(cache_control: str | None) -> int:
  if cache_control:
    if "no-store" in cache_control or "no-cache" in cache_control:
      return CIMD_CACHE_MIN_SECONDS
    match = _MAX_AGE_RE.search(cache_control)
    if match:
      return max(
        CIMD_CACHE_MIN_SECONDS, min(int(match.group(1)), CIMD_CACHE_MAX_SECONDS)
      )
  return CIMD_CACHE_DEFAULT_SECONDS


def fetch_client_metadata(
  client_id: str, *, transport: httpx.BaseTransport | None = None
) -> tuple[dict[str, Any], int]:
  """Fetch and validate a metadata document. Returns ``(document, cache_ttl)``.

  ``transport`` exists for tests; production uses httpx's default.
  """
  if not is_cimd_client_id(client_id):
    raise ClientError("invalid_client", "client_id is not a metadata document URL")
  host = urlsplit(client_id).hostname or ""
  _assert_public_host(host)

  try:
    with httpx.Client(
      follow_redirects=False,
      timeout=CIMD_TIMEOUT_SECONDS,
      transport=transport,
      headers={"Accept": "application/json", "User-Agent": "robosystems-oauth/1"},
    ) as client:
      with client.stream("GET", client_id) as response:
        if response.status_code != 200:
          raise ClientError(
            "invalid_client", "client metadata document is not available"
          )
        declared = response.headers.get("content-length")
        if declared and declared.isdigit() and int(declared) > CIMD_MAX_BYTES:
          raise ClientError("invalid_client", "client metadata document is too large")
        body = bytearray()
        for chunk in response.iter_bytes():
          body.extend(chunk)
          if len(body) > CIMD_MAX_BYTES:
            raise ClientError("invalid_client", "client metadata document is too large")
        cache_control = response.headers.get("cache-control")
  except ClientError:
    raise
  except httpx.HTTPError as exc:
    logger.warning(f"CIMD fetch failed for {client_id}: {exc}")
    raise ClientError(
      "invalid_client", "client metadata document is not available"
    ) from exc

  try:
    document = json.loads(bytes(body))
  except ValueError as exc:
    raise ClientError("invalid_client", "client metadata document is not JSON") from exc
  if not isinstance(document, dict):
    raise ClientError("invalid_client", "client metadata document must be an object")
  if document.get("client_id") != client_id:
    raise ClientError(
      "invalid_client", "client metadata document client_id does not match its URL"
    )
  return document, _cache_ttl(cache_control)


def _cache_key(client_id: str) -> str:
  return f"{_CACHE_KEY_PREFIX}{hashlib.sha256(client_id.encode()).hexdigest()}"


def get_client_metadata(
  client_id: str, *, transport: httpx.BaseTransport | None = None
) -> dict[str, Any]:
  """The validated document for a CIMD ``client_id``, from cache when fresh."""
  key = _cache_key(client_id)
  try:
    cached = create_redis_client(ValkeyDatabase.AUTH).get(key)
    if cached:
      document = json.loads(cached)
      if isinstance(document, dict) and document.get("client_id") == client_id:
        return document
  except Exception as exc:
    logger.warning(f"CIMD cache read failed: {exc}")

  document, ttl = fetch_client_metadata(client_id, transport=transport)
  try:
    create_redis_client(ValkeyDatabase.AUTH).setex(key, ttl, json.dumps(document))
  except Exception as exc:
    logger.warning(f"CIMD cache write failed: {exc}")
  return document
