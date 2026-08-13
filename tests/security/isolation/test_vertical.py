"""Vertical axis — privilege escalation within one org.

Where the horizontal axis proves tenant A cannot touch tenant B at all, the
vertical axis proves that *inside* one org a lower role cannot do a higher
role's work: a graph VIEWER cannot write, a MEMBER cannot administer, and the
derived-privilege rule (org OWNER/ADMIN → implicit graph ADMIN) grants exactly
what it should and no more. Grounded in the verified role model:
`GraphRole viewer<member<admin` and `GraphUser.get_effective_role`.

── Provisioning blocker (finding for spec OQ1) ──────────────────────────────
This axis needs a *second* principal inside tenant A's org. The only way to
add one via the platform is an email invitation
(`POST /v1/orgs/{org_id}/invitations`, gated by ORG_MEMBER_INVITATIONS_ENABLED),
and the raw invite token is **neither returned by the API nor recoverable from
the database** — the DB stores only `sha256(token)` (`org_invitation.py`
`token_hash`), and the raw token is emailed. So automated provisioning of a
multi-user org cannot be done black-box, and cannot use the DB side channel
either. It requires one of:
  (a) intercepting the invitation email in the target env, or
  (b) a test-support seam that returns the raw token in a non-prod build.
Until one exists this axis is provisioned-skipped. The intended matrix below
is the design, ready to activate once (a) or (b) lands.
"""

from __future__ import annotations

import pytest

from ._http import Client

pytestmark = [
  pytest.mark.isolation,
  pytest.mark.security,
  pytest.mark.integration,
]


def _invitations_enabled(client: Client, org_owner) -> bool:
  """Probe whether org invitations are turned on (else 501)."""
  if not org_owner.org_id:
    return False
  r = client.post(
    f"/v1/orgs/{org_owner.org_id}/invitations",
    principal=org_owner,
    json={"email": "iso-harness-probe@example.com", "role": "member"},
  )
  # 501 => flag off; 201 => created (but no raw token in the response, see below)
  return r.status_code != 501


def _provision_role_matrix(client: Client, tenants):
  """Provision viewer/member/admin principals inside tenant A's org.

  Returns a dict of role -> Principal, or raises Skipped with the reason.
  Currently always skips: the invite token cannot be obtained programmatically
  (see the module docstring). The steps are written out so the seam is obvious.
  """
  owner = tenants.tenant_a
  if not _invitations_enabled(client, owner):
    pytest.skip(
      "ORG_MEMBER_INVITATIONS_ENABLED is off — enable it to provision a "
      "multi-user org for the vertical axis"
    )
  pytest.skip(
    "vertical axis not auto-provisionable: the org invite token is emailed and "
    "stored only as a sha256 hash, so it is retrievable from neither the API "
    "nor the DB. Needs email interception or a test-support seam (spec OQ1). "
    "Intended matrix: invite member/admin users -> register with invite_token "
    "-> grant explicit GraphRole -> assert viewer!write, member!admin, and "
    "org-admin=implicit-graph-admin (positive)."
  )


def test_viewer_cannot_write(tenants, client: Client) -> None:
  roles = _provision_role_matrix(client, tenants)
  viewer = roles["viewer"]
  graph = tenants.graph_a
  # viewer can read (positive control)
  r = client.post(
    f"/v1/graphs/{graph}/query/cypher",
    principal=viewer,
    json={"query": "MATCH (n) RETURN count(n) AS c", "parameters": {}},
  )
  assert r.status_code // 100 == 2, f"viewer denied read on own graph: {r.status_code}"
  # viewer cannot write
  w = client.post(
    f"/v1/graphs/{graph}/operations/update-graph-metadata",
    principal=viewer,
    json={"description": "viewer write attempt"},
  )
  assert w.status_code in (401, 403), f"VIEWER WROTE (escalation): {w.status_code}"


def test_member_cannot_administer(tenants, client: Client) -> None:
  roles = _provision_role_matrix(client, tenants)
  member = roles["member"]
  graph = tenants.graph_a
  # member cannot add another member (admin-only)
  a = client.post(
    f"/v1/graphs/{graph}/members",
    principal=member,
    json={"user_id": member.user_id, "role": "admin"},
  )
  assert a.status_code in (401, 403), (
    f"MEMBER ADMINISTERED (escalation): {a.status_code}"
  )


def test_org_admin_has_implicit_graph_admin(tenants, client: Client) -> None:
  """Derived-privilege positive: org ADMIN with no explicit grant can administer."""
  roles = _provision_role_matrix(client, tenants)
  admin = roles["admin"]
  graph = tenants.graph_a
  m = client.get(f"/v1/graphs/{graph}/members", principal=admin)
  assert m.status_code // 100 == 2, (
    f"org admin denied on org graph — derived privilege broken: {m.status_code}"
  )
