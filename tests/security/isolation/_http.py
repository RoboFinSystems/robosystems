"""Black-box HTTP client for the tenant-isolation harness.

Everything here talks to a *deployed* API over HTTP with a credential — no
imports of application internals, no database access. A `Principal` is a
labelled credential (an API key plus the identity it belongs to); the
`Client` issues requests as a given principal against a fixed base URL.

The harness deliberately does not use the typed SDK for the attack matrix:
it must send arbitrary paths and bodies as an arbitrary principal, including
requests the SDK would refuse to construct.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import httpx


@dataclass
class Principal:
  """A labelled credential and the identity it authenticates as."""

  label: str
  api_key: str
  user_id: str
  email: str = ""
  # graphs this principal legitimately owns/controls, by role label
  graph_id: str = ""
  org_id: str = ""
  jwt: str = field(default="", repr=False)

  def headers(self) -> dict[str, str]:
    return {"X-API-Key": self.api_key, "Content-Type": "application/json"}


class Client:
  """Issues HTTP requests against a fixed base URL.

  Stateless with respect to auth — every call names the principal (or a raw
  key / no key) it should run as, so one client drives the whole matrix.
  """

  def __init__(self, base_url: str, *, timeout: float = 60.0) -> None:
    self.base_url = base_url.rstrip("/")
    self._c = httpx.Client(base_url=self.base_url, timeout=timeout)

  def close(self) -> None:
    self._c.close()

  def request(
    self,
    method: str,
    path: str,
    *,
    principal: Principal | None = None,
    api_key: str | None = None,
    jwt: str | None = None,
    json: dict | None = None,
    params: dict | None = None,
  ) -> httpx.Response:
    headers: dict[str, str] = {"Content-Type": "application/json"}
    if principal is not None:
      headers["X-API-Key"] = principal.api_key
    elif api_key is not None:
      headers["X-API-Key"] = api_key
    if jwt is not None:
      headers["Authorization"] = f"Bearer {jwt}"
    return self._c.request(method, path, headers=headers, json=json, params=params)

  def get(self, path: str, **kw) -> httpx.Response:
    return self.request("GET", path, **kw)

  def post(self, path: str, **kw) -> httpx.Response:
    return self.request("POST", path, **kw)
