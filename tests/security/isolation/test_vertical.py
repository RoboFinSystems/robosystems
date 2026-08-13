"""Vertical axis — privilege escalation within one org.

Where the horizontal axis proves tenant A cannot touch tenant B at all, the
vertical axis proves that *inside* one org a lower role cannot do a higher
role's work: a graph VIEWER cannot write, a MEMBER cannot administer, and the
derived-privilege rule (org OWNER/ADMIN → implicit graph ADMIN) grants exactly
what it should. Grounded in the verified role model: `GraphRole viewer<member<
admin` and `GraphUser.get_effective_role`.

── Provisioning ─────────────────────────────────────────────────────────────
This axis needs a second principal inside tenant A's org. The only way to add
one is an email invitation, and the raw invite token is neither in the API
response nor recoverable from the DB (stored as `sha256(token)`). So the run
depends on two flags being on in the (non-prod) target:
  - ORG_MEMBER_INVITATIONS_ENABLED — the invitation feature, and
  - AUTH_INVITE_TOKEN_IN_RESPONSE — the test-support seam that returns the raw
    token in the create response (structurally forced off in production).
With both on, `provision_role_matrix` completes invite → register → role-grant
and these tests run; otherwise `tenants.roles` is empty and they skip.
"""

from __future__ import annotations

import pytest

from ._http import Client

pytestmark = [
  pytest.mark.isolation,
  pytest.mark.security,
  pytest.mark.integration,
]

_CYPHER = {"query": "MATCH (n) RETURN count(n) AS c", "parameters": {}}


def _role(tenants, name: str):
  roles = tenants.roles or {}
  if name not in roles:
    pytest.skip(
      "role matrix not provisioned — set ORG_MEMBER_INVITATIONS_ENABLED and "
      "AUTH_INVITE_TOKEN_IN_RESPONSE in the (non-prod) target"
    )
  return roles[name]


def test_viewer_can_read(tenants, client: Client) -> None:
  """Positive control: a graph VIEWER can read (so a write denial is meaningful)."""
  viewer = _role(tenants, "viewer")
  r = client.post(
    f"/v1/graphs/{tenants.graph_a}/query/cypher", principal=viewer, json=_CYPHER
  )
  assert r.status_code // 100 == 2, f"viewer denied read on own graph: {r.status_code}"


def test_viewer_cannot_write(tenants, client: Client) -> None:
  viewer = _role(tenants, "viewer")
  w = client.post(
    f"/v1/graphs/{tenants.graph_a}/operations/update-graph-metadata",
    principal=viewer,
    json={"description": "viewer write attempt"},
  )
  assert w.status_code in (401, 403), f"VIEWER WROTE (escalation): {w.status_code}"


def test_viewer_cannot_write_cypher(tenants, client: Client) -> None:
  viewer = _role(tenants, "viewer")
  w = client.post(
    f"/v1/graphs/{tenants.graph_a}/query/cypher",
    principal=viewer,
    json={"query": "CREATE (n:IsoHarnessProbe {m: 1}) RETURN n", "parameters": {}},
  )
  assert w.status_code in (401, 403), (
    f"VIEWER WROTE CYPHER (escalation): {w.status_code}"
  )


def test_member_can_read(tenants, client: Client) -> None:
  member = _role(tenants, "member")
  r = client.post(
    f"/v1/graphs/{tenants.graph_a}/query/cypher", principal=member, json=_CYPHER
  )
  assert r.status_code // 100 == 2, f"member denied read on own graph: {r.status_code}"


def test_member_cannot_administer(tenants, client: Client) -> None:
  member = _role(tenants, "member")
  a = client.post(
    f"/v1/graphs/{tenants.graph_a}/members",
    principal=member,
    json={"user_id": member.user_id, "role": "admin"},
  )
  assert a.status_code in (401, 403), (
    f"MEMBER ADMINISTERED (escalation): {a.status_code}"
  )


def test_org_admin_has_implicit_graph_admin(tenants, client: Client) -> None:
  """Derived-privilege positive: org ADMIN with no explicit grant can administer."""
  admin = _role(tenants, "admin")
  m = client.get(f"/v1/graphs/{tenants.graph_a}/members", principal=admin)
  assert m.status_code // 100 == 2, (
    f"org admin denied on org graph — derived privilege broken: {m.status_code}"
  )
