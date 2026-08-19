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

import json

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


# ── Connections — a write surface end to end ────────────────────────────────
# Registering a source seeds sync, the fiscal calendar and the mapping
# operator; completing OAuth stores credentials and starts a full-rebuild
# sync; a sync rewrites captured events. All of it authenticates on graph
# membership, so each entry point must run the write-role gate itself.
# Bogus connection ids are deliberate: the gate must answer 403 *before* the
# lookup — a 404 here would mean a viewer got as far as resolving the row.


def test_viewer_cannot_create_connection(tenants, client: Client) -> None:
  viewer = _role(tenants, "viewer")
  w = client.post(
    f"/v1/graphs/{tenants.graph_a}/connections",
    principal=viewer,
    json={"provider": "sec", "sec_config": {"cik": "0000320193"}},
  )
  assert w.status_code in (401, 403), (
    f"VIEWER CREATED A CONNECTION (escalation): {w.status_code}"
  )


def test_viewer_cannot_sync_connection(tenants, client: Client) -> None:
  viewer = _role(tenants, "viewer")
  w = client.post(
    f"/v1/graphs/{tenants.graph_a}/connections/conn_isoprobe/sync",
    principal=viewer,
    json={"full_rebuild": True},
  )
  assert w.status_code in (401, 403), (
    f"VIEWER REACHED SYNC (escalation or gate after lookup): {w.status_code}"
  )


def test_viewer_cannot_init_oauth(tenants, client: Client) -> None:
  viewer = _role(tenants, "viewer")
  w = client.post(
    f"/v1/graphs/{tenants.graph_a}/connections/oauth/init",
    principal=viewer,
    json={
      "connection_id": "conn_isoprobe",
      "redirect_uri": "http://localhost/callback",
    },
  )
  assert w.status_code in (401, 403), (
    f"VIEWER REACHED OAUTH INIT (escalation or gate after lookup): {w.status_code}"
  )


# ── Cypher classifier verbs ─────────────────────────────────────────────────
# Statements that are neither reads nor CREATE/MERGE/SET/DELETE writes but
# still mutate or steer the database: transaction control (a BEGIN left open
# on a pooled connection poisons the next borrower), CHECKPOINT (admin), and
# COMMENT ON (schema DDL). A viewer's read-only role must refuse all of them
# through the same classifier that refuses a CREATE.

_CLASSIFIER_VERBS = [
  "BEGIN TRANSACTION",
  "BEGIN TRANSACTION READ ONLY",
  "COMMIT",
  "ROLLBACK",
  "CHECKPOINT",
  "COMMENT ON TABLE Entity IS 'iso-harness'",
  "MATCH (n) RETURN count(n) AS c; CHECKPOINT",
]


@pytest.mark.parametrize("statement", _CLASSIFIER_VERBS)
def test_viewer_cannot_use_classifier_verbs(
  tenants, client: Client, statement: str
) -> None:
  viewer = _role(tenants, "viewer")
  w = client.post(
    f"/v1/graphs/{tenants.graph_a}/query/cypher",
    principal=viewer,
    json={"query": statement, "parameters": {}},
  )
  assert w.status_code in (401, 403), (
    f"VIEWER RAN {statement.split()[0]} (classifier gap): {w.status_code} "
    f"{w.text[:200]}"
  )


# ── MCP tool-level admin gates ──────────────────────────────────────────────
# The REST lifecycle ops require admin; the MCP tools for the same actions
# must too. A member passes the transport's write classification (member ≥
# write), so the denial has to come from the tool itself. Tool errors travel
# as a 200 with the error inside the result text, so the probe reads the body.


def _mcp_tool_result(response) -> dict:
  """Decode the tool's own JSON result out of an MCP call-tool response."""
  if response.status_code // 100 != 2:
    return {"_http_status": response.status_code}
  try:
    body = response.json()
    result = body.get("result", body)
    text = result.get("text") if isinstance(result, dict) else None
    return json.loads(text) if isinstance(text, str) else {"_raw": result}
  except (ValueError, AttributeError):
    return {"_raw": response.text[:200]}


def test_member_cannot_create_subgraph_via_mcp(tenants, client: Client) -> None:
  member = _role(tenants, "member")
  r = client.post(
    f"/v1/graphs/{tenants.graph_a}/mcp/call-tool",
    principal=member,
    json={"name": "create-subgraph", "arguments": {"name": "isomcp"}},
  )
  if r.status_code in (401, 403):
    return  # denied at the transport — fine
  result = _mcp_tool_result(r)
  if result.get("error") == "subgraph_creation_disabled":
    pytest.skip("SUBGRAPH_CREATION_ENABLED is off in the target; gate not observable")
  assert result.get("error") == "insufficient_permissions", (
    f"MEMBER CREATED A SUBGRAPH OVER MCP (escalation vs REST admin gate): {result}"
  )


def test_member_cannot_create_backup_via_mcp(tenants, client: Client) -> None:
  member = _role(tenants, "member")
  r = client.post(
    f"/v1/graphs/{tenants.graph_a}/mcp/call-tool",
    principal=member,
    json={"name": "create-backup", "arguments": {}},
  )
  if r.status_code in (401, 403):
    return
  result = _mcp_tool_result(r)
  if result.get("error") == "backup_disabled":
    pytest.skip("BACKUP_CREATION_ENABLED is off in the target; gate not observable")
  assert result.get("error") == "insufficient_permissions", (
    f"MEMBER CREATED A BACKUP OVER MCP (escalation vs REST admin gate): {result}"
  )


# ── Pre-authentication ──────────────────────────────────────────────────────
# An anonymous or invalid-key request against a graph must be a 401/403 and
# nothing else: not a 404 (existence oracle), not a 429 keyed to the graph
# (which would mean the graph's rate-limit bucket is charged before the caller
# proved they may use it — a drain of the tenant's quota by an outsider).


def test_anonymous_request_is_refused_without_touching_the_graph(
  tenants, client: Client
) -> None:
  path = f"/v1/graphs/{tenants.graph_a}/query/cypher"
  for _ in range(5):
    r = client.post(path, json=_CYPHER)  # no credentials at all
    assert r.status_code in (401, 403), f"anonymous → {r.status_code}"
    r = client.post(path, api_key="rfs_isoharness_invalid_key", json=_CYPHER)
    assert r.status_code in (401, 403), f"invalid key → {r.status_code}"
  # The owner is unaffected by the anonymous burst.
  ok = client.post(path, principal=tenants.tenant_a, json=_CYPHER)
  assert ok.status_code // 100 == 2, (
    f"owner degraded after anonymous burst: {ok.status_code}"
  )
