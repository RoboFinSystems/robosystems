"""Request/response models for the MCP OAuth 2.1 authorization server.

The token, registration, and revocation endpoints speak RFC-shaped JSON /
form bodies and are documented inline in ``routers/oauth/server.py``; the
models here are the consent leg the login home drives.
"""

from pydantic import BaseModel, Field


class PendingAuthorizationResponse(BaseModel):
  """What the consent page renders. The redirect hostname is shown so the
  user can see where approval sends them (MCP spec requirement); loopback
  redirects carry an extra warning."""

  request_id: str
  client_name: str = Field(..., description="Registered client name")
  client_uri: str | None = Field(None, description="Client homepage, when registered")
  logo_uri: str | None = Field(None, description="Client logo, when registered")
  is_trusted: bool = Field(
    ...,
    description="Operator-registered or allowlisted client — no unknown-client warning",
  )
  redirect_host: str = Field(
    ..., description="Hostname (or scheme) approval redirects to"
  )
  is_loopback_redirect: bool = Field(
    ..., description="Redirect targets the local machine (native client)"
  )
  resource: str = Field(..., description="Canonical MCP URL the token will be bound to")
  graph_id: str | None = Field(
    None,
    description="Graph fixed by the resource URL; null on the graph-agnostic route, "
    "where the user picks one",
  )
  scope: str = Field(..., description="Scope that will be granted")


class ConsentDecisionRequest(BaseModel):
  approved: bool = Field(..., description="True to grant, False to deny")
  graph_id: str | None = Field(
    None,
    description="The graph to grant (required on the graph-agnostic route; must "
    "match the URL's graph on a per-graph route if given)",
    max_length=64,
  )


class ConsentDecisionResponse(BaseModel):
  redirect_to: str = Field(
    ..., description="The client's callback URL; the browser must navigate here"
  )
