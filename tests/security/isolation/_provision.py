"""Fixture provisioning for the isolation harness — over the API only.

Builds the test tenants the matrix runs against: two independent tenants
(A and B) for the horizontal / cross-tenant axis, and — within tenant A's
org — a set of principals at different roles for the vertical axis.

Every tenant is a real account with a real graph, created through the
public API exactly as a customer would. Nothing here reaches into the
database; the DB is a side channel used only by the runbook for teardown
verification, never by the harness.

The provisioning sequence per user is the documented one:
    register  -> POST /v1/auth/register   (auto-creates an org)
    login     -> POST /v1/auth/login      (returns a JWT)
    api key   -> POST /v1/user/api-keys    (returns the key string)
and per graph:
    create    -> POST /v1/graphs           (202 OperationEnvelope)
    poll      -> GET  /v1/operations/{id}/status  until completed
"""

from __future__ import annotations

import secrets
import string
import time
from dataclasses import dataclass, field

from ._http import Client, Principal

_PW_CLASSES = (
  string.ascii_uppercase,
  string.ascii_lowercase,
  string.digits,
  "!@#$%^&*",
)


def _make_password() -> str:
  """A random password that structurally passes the platform's policy.

  Rotating character classes guarantees no run of three repeated characters
  (`(.)\\1{2,}`) and no same-class sequence (`123456` / `abcdef`), which is
  what a plain hex token trips on intermittently.
  """
  return "".join(secrets.choice(_PW_CLASSES[i % len(_PW_CLASSES)]) for i in range(20))


def _unique(prefix: str) -> str:
  return f"{prefix}_{int(time.time())}_{secrets.token_hex(3)}"


def _find_graph_id(obj) -> str | None:
  """Recursively locate a graph_id in a free-form operation result."""
  if isinstance(obj, dict):
    for k, v in obj.items():
      if k in ("graph_id", "graphId") and isinstance(v, str) and v:
        return v
      found = _find_graph_id(v)
      if found:
        return found
  elif isinstance(obj, list):
    for item in obj:
      found = _find_graph_id(item)
      if found:
        return found
  return None


def register_principal(client: Client, label: str) -> Principal:
  """Register a user, log in, mint an API key. Returns a Principal."""
  email = f"{_unique('iso_' + label)}@example.com"
  name = f"iso-harness {label}"
  password = _make_password()

  r = client.post(
    "/v1/auth/register", json={"name": name, "email": email, "password": password}
  )
  if r.status_code not in (200, 201):
    raise RuntimeError(f"register({label}) -> {r.status_code}: {r.text[:300]}")
  body = r.json()
  user_id = (body.get("user") or {}).get("id", "")
  org_id = (body.get("org") or {}).get("id", "") if body.get("org") else ""

  # Prefer the token from register; fall back to an explicit login.
  jwt = body.get("token") or ""
  if not jwt:
    lr = client.post("/v1/auth/login", json={"email": email, "password": password})
    if lr.status_code != 200:
      raise RuntimeError(f"login({label}) -> {lr.status_code}: {lr.text[:300]}")
    jwt = lr.json().get("token") or ""
  if not jwt:
    raise RuntimeError(
      f"no JWT for {label} (MFA enforced locally? register/login returned "
      f"mfa_token instead of token)"
    )

  kr = client.post(
    "/v1/user/api-keys", jwt=jwt, json={"name": f"iso-harness {label} key"}
  )
  if kr.status_code not in (200, 201):
    raise RuntimeError(f"create_api_key({label}) -> {kr.status_code}: {kr.text[:300]}")
  key = kr.json().get("key") or ""
  if not key:
    raise RuntimeError(f"no api key string returned for {label}: {kr.text[:200]}")

  return Principal(
    label=label, api_key=key, user_id=user_id, email=email, org_id=org_id, jwt=jwt
  )


def create_entity_graph(
  client: Client,
  owner: Principal,
  *,
  schema_extensions: list[str] | None = None,
  timeout: float = 180.0,
  poll: float = 2.0,
) -> str:
  """Create an entity graph for `owner` and return its graph_id.

  `schema_extensions=["roboledger"]` provisions a roboledger tenant so the
  GraphQL and `/extensions/roboledger/operations/*` surfaces are testable.
  """
  name = _unique("isograph" + owner.label)
  body = {
    "metadata": {
      "graph_name": name,
      "description": "tenant-isolation harness test graph",
      "schema_extensions": schema_extensions or [],
      "tags": ["iso-harness", "test"],
    },
    "initial_entity": {
      "name": f"IsoHarness {owner.label}",
      "uri": f"https://iso-harness.test/{owner.label}/{name}",
    },
    "create_entity": True,
  }
  r = client.post("/v1/graphs", principal=owner, json=body)
  if r.status_code not in (200, 201, 202):
    raise RuntimeError(
      f"create_graph({owner.label}) -> {r.status_code}: {r.text[:300]}"
    )
  env = r.json()

  gid = _find_graph_id(env.get("result")) or _find_graph_id(env)
  op_id = env.get("operationId") or env.get("operation_id")
  if gid:
    return gid
  if not op_id:
    raise RuntimeError(f"no operationId/graph_id in create response: {env}")

  deadline = time.time() + timeout
  while time.time() < deadline:
    time.sleep(poll)
    sr = client.get(f"/v1/operations/{op_id}/status", principal=owner)
    if sr.status_code != 200:
      continue
    st = sr.json()
    status = str(st.get("status", "")).lower()
    if status in ("completed", "succeeded", "success"):
      gid = _find_graph_id(st)
      if gid:
        return gid
      raise RuntimeError(f"operation completed but no graph_id: {st}")
    if status in ("failed", "error", "cancelled"):
      raise RuntimeError(f"graph creation failed: {st}")
  raise TimeoutError(f"graph creation for {owner.label} timed out after {timeout}s")


@dataclass
class TenantFixture:
  """The provisioned world the matrix runs against."""

  tenant_a: Principal  # owner of graph_a
  tenant_b: Principal  # owner of graph_b (the cross-tenant victim/target)
  graph_a: str
  graph_b: str
  # vertical-axis principals inside tenant A's org (populated in phase 2)
  roles: dict[str, Principal] = field(default_factory=dict)

  def all_principals(self) -> list[Principal]:
    return [self.tenant_a, self.tenant_b, *self.roles.values()]


def provision(
  client: Client, *, schema_extensions: list[str] | None = None
) -> TenantFixture:
  """Provision two independent tenants, each with one entity graph.

  Defaults to roboledger graphs so the extensions command surface and the
  GraphQL data fields are exercised; pass schema_extensions=[] for plain
  entity graphs.
  """
  if schema_extensions is None:
    schema_extensions = ["roboledger", "roboinvestor"]
  a = register_principal(client, "A")
  b = register_principal(client, "B")
  graph_a = create_entity_graph(client, a, schema_extensions=schema_extensions)
  graph_b = create_entity_graph(client, b, schema_extensions=schema_extensions)
  a.graph_id = graph_a
  b.graph_id = graph_b
  return TenantFixture(tenant_a=a, tenant_b=b, graph_a=graph_a, graph_b=graph_b)
