"""Protected resources, their canonical URLs, and the discovery documents.

Two resources exist: the graph-agnostic ``/v1/mcp`` (OAuth-only; the grant
carries the graph) and ``/v1/graphs/{graph_id}/mcp`` (the URL carries the
graph). A token is bound to exactly one canonical resource URL — the
audience — and is refused at the other.

Canonical form is ``{issuer}{path}`` with no query, fragment, or trailing
slash. Clients present the URL as the user typed it (RFC 8707 ``resource``),
so parsing is lenient about a trailing slash and strict about everything
else.
"""

import re
from dataclasses import dataclass
from urllib.parse import urlsplit

from robosystems.config import env
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN

AGNOSTIC_MCP_PATH = "/v1/mcp"
_GRAPH_MCP_PATH = re.compile(
  rf"^/v1/graphs/({GRAPH_OR_SUBGRAPH_ID_PATTERN.strip('^$')})/mcp$"
)

SCOPE_MCP = "mcp"
SCOPE_OFFLINE_ACCESS = "offline_access"
SUPPORTED_SCOPES: tuple[str, ...] = (SCOPE_MCP, SCOPE_OFFLINE_ACCESS)

PRM_PATH_PREFIX = "/.well-known/oauth-protected-resource"
AS_METADATA_PATH = "/.well-known/oauth-authorization-server"

AUTHORIZATION_ENDPOINT_PATH = "/v1/oauth/authorize"
TOKEN_ENDPOINT_PATH = "/v1/oauth/token"
REGISTRATION_ENDPOINT_PATH = "/v1/oauth/register"
REVOCATION_ENDPOINT_PATH = "/v1/oauth/revoke"


@dataclass(frozen=True)
class ResourceTarget:
  """A resolved protected resource: its canonical URL and, for the per-graph
  route, the graph the URL names (``None`` on the agnostic route)."""

  resource: str
  graph_id: str | None

  @property
  def is_agnostic(self) -> bool:
    return self.graph_id is None

  @property
  def path(self) -> str:
    return self.resource[len(issuer()) :]


def issuer() -> str:
  """The authorization server's issuer identifier: the API origin."""
  return (env.ROBOSYSTEMS_API_URL or "").strip().rstrip("/")


def agnostic_target() -> ResourceTarget:
  return ResourceTarget(resource=f"{issuer()}{AGNOSTIC_MCP_PATH}", graph_id=None)


def graph_target(graph_id: str) -> ResourceTarget:
  return ResourceTarget(
    resource=f"{issuer()}/v1/graphs/{graph_id}/mcp", graph_id=graph_id
  )


def resolve_resource(value: str | None) -> ResourceTarget | None:
  """Map a presented ``resource`` value onto one of our two targets.

  ``None`` (a client that predates RFC 8707 in MCP) resolves to the agnostic
  route — the only resource a token can be minted for without a graph in
  the URL. Anything that is not exactly one of our MCP URLs is ``None``:
  a different origin, a query string, a fragment, an unknown path.
  """
  if value is None or value == "":
    return agnostic_target()
  if not isinstance(value, str) or len(value) > 512:
    return None

  parts = urlsplit(value.strip())
  if parts.query or parts.fragment or not parts.scheme or not parts.netloc:
    return None
  origin = f"{parts.scheme.lower()}://{parts.netloc.lower()}"
  if origin != issuer().lower():
    return None

  path = parts.path.rstrip("/") or "/"
  if path == AGNOSTIC_MCP_PATH:
    return agnostic_target()
  match = _GRAPH_MCP_PATH.match(path)
  if match:
    return graph_target(match.group(1))
  return None


def normalize_scope(requested: str | None) -> str | None:
  """Validate a space-separated scope request against the supported set.

  Absent → ``mcp``. Unknown values → ``None`` (the caller answers
  ``invalid_scope``). ``offline_access`` is accepted and echoed; a refresh
  token is issued either way, because every hosted client relies on it.
  """
  if requested is None or not requested.strip():
    return SCOPE_MCP
  parts = [p for p in requested.split(" ") if p]
  if any(p not in SUPPORTED_SCOPES for p in parts):
    return None
  ordered = [s for s in SUPPORTED_SCOPES if s in parts]
  if SCOPE_MCP not in ordered:
    ordered.insert(0, SCOPE_MCP)
  return " ".join(ordered)


def prm_url(target: ResourceTarget) -> str:
  """Where the resource's RFC 9728 document lives: the well-known prefix
  followed by the resource path (the path-suffix form of the RFC)."""
  return f"{issuer()}{PRM_PATH_PREFIX}{target.path}"


def protected_resource_metadata(target: ResourceTarget) -> dict:
  return {
    "resource": target.resource,
    "authorization_servers": [issuer()],
    "scopes_supported": list(SUPPORTED_SCOPES),
    "bearer_methods_supported": ["header"],
    "resource_name": "RoboSystems MCP"
    if target.is_agnostic
    else f"RoboSystems MCP — {target.graph_id}",
    "resource_documentation": f"{env.ROBOSYSTEMS_URL.rstrip('/')}/open-source",
  }


def authorization_server_metadata() -> dict:
  """RFC 8414 document. The two flags Claude keys on to choose CIMD over
  DCR are ``client_id_metadata_document_supported`` and ``"none"`` in
  ``token_endpoint_auth_methods_supported``; OpenAI additionally requires
  ``code_challenge_methods_supported`` and honors RFC 9207 ``iss``."""
  base = issuer()
  return {
    "issuer": base,
    "authorization_endpoint": f"{base}{AUTHORIZATION_ENDPOINT_PATH}",
    "token_endpoint": f"{base}{TOKEN_ENDPOINT_PATH}",
    "registration_endpoint": f"{base}{REGISTRATION_ENDPOINT_PATH}",
    "revocation_endpoint": f"{base}{REVOCATION_ENDPOINT_PATH}",
    "scopes_supported": list(SUPPORTED_SCOPES),
    "response_types_supported": ["code"],
    "response_modes_supported": ["query"],
    "grant_types_supported": ["authorization_code", "refresh_token"],
    "token_endpoint_auth_methods_supported": [
      "none",
      "client_secret_post",
      "client_secret_basic",
    ],
    "revocation_endpoint_auth_methods_supported": [
      "none",
      "client_secret_post",
      "client_secret_basic",
    ],
    "code_challenge_methods_supported": ["S256"],
    "authorization_response_iss_parameter_supported": True,
    "client_id_metadata_document_supported": False,
    "service_documentation": f"{env.ROBOSYSTEMS_URL.rstrip('/')}/open-source",
  }


def bearer_challenge(
  target: ResourceTarget,
  *,
  error: str | None = None,
  error_description: str | None = None,
) -> str:
  """The ``WWW-Authenticate`` value for a 401/403 on an MCP route.

  Always names the resource's metadata document — that is how a client
  discovers the authorization server (Claude only honors it on a 401) —
  and the scope to request.
  """
  params = [f'resource_metadata="{prm_url(target)}"', f'scope="{SCOPE_MCP}"']
  if error:
    params.insert(0, f'error="{error}"')
    if error_description:
      safe = error_description.replace('"', "'")
      params.insert(1, f'error_description="{safe}"')
  return "Bearer " + ", ".join(params)
