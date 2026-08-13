"""Horizontal axis — cross-tenant isolation (R-11).

A principal in one tenant attempts to read or write another tenant's graph,
across REST, GraphQL, and MCP. Run in both directions so an asymmetric bug
(e.g. the first-created graph being special) cannot hide. A LEAK fails the
suite; a failed positive control (INVALID) fails it too — a green run that
never proved the owner's own access proves nothing.
"""

from __future__ import annotations

import pytest

from . import matrix
from ._http import Client, Principal
from ._oracle import (
  Verdict,
  classify_graphql,
  classify_read,
  classify_write,
)
from ._report import Finding, Report

pytestmark = [
  pytest.mark.isolation,
  pytest.mark.security,
  pytest.mark.integration,
]


def _directions(tenants):
  """(name, attacker, victim, victim_graph) for both cross-tenant directions."""
  return [
    ("A→B", tenants.tenant_a, tenants.tenant_b, tenants.graph_b),
    ("B→A", tenants.tenant_b, tenants.tenant_a, tenants.graph_a),
  ]


def _assert_clean(found: list[Finding]) -> None:
  leaks = [f for f in found if f.verdict == Verdict.LEAK]
  invalid = [f for f in found if f.verdict == Verdict.INVALID]
  assert not invalid, (
    "positive control(s) failed — harness misconfigured:\n"
    + "\n".join(f"  {f.method} {f.path}: {f.note}" for f in invalid)
  )
  assert not leaks, "CROSS-TENANT LEAK(S):\n" + "\n".join(
    f"  [{f.attacker}→{f.target_graph}] {f.surface} {f.method} {f.path} "
    f"→ {f.status}: {f.note}"
    for f in leaks
  )


def test_rest_reads_are_isolated(tenants, client: Client, report: Report) -> None:
  found: list[Finding] = []
  for _name, attacker, victim, graph in _directions(tenants):
    for tmpl in matrix.REST_READS:
      path = tmpl.format(graph_id=graph)
      v = client.get(path, principal=victim)
      a = client.get(path, principal=attacker)
      verdict, note = classify_read(v, a)
      f = Finding(
        "horizontal",
        "rest-read",
        "GET",
        path,
        attacker.label,
        graph,
        verdict,
        a.status_code,
        note,
      )
      report.add(f)
      found.append(f)
  _assert_clean(found)


def test_cypher_read_is_isolated(tenants, client: Client, report: Report) -> None:
  """The strongest data probe: a cross-tenant Cypher read of node counts."""
  found: list[Finding] = []
  for _name, attacker, victim, graph in _directions(tenants):
    path = matrix.CYPHER_READ_PATH.format(graph_id=graph)
    v = client.post(path, principal=victim, json=matrix.CYPHER_READ_BODY)
    a = client.post(path, principal=attacker, json=matrix.CYPHER_READ_BODY)
    verdict, note = classify_read(v, a)
    f = Finding(
      "horizontal",
      "cypher",
      "POST",
      path,
      attacker.label,
      graph,
      verdict,
      a.status_code,
      note,
    )
    report.add(f)
    found.append(f)
  _assert_clean(found)


def test_rest_writes_are_isolated(tenants, client: Client, report: Report) -> None:
  found: list[Finding] = []
  for _name, attacker, _victim, graph in _directions(tenants):
    for spec in matrix.REST_WRITES:
      path = spec["path"].format(graph_id=graph)
      body = dict(spec["body"])
      if body.get("user_id") == "{attacker_user_id}":
        body["user_id"] = attacker.user_id
      a = client.post(path, principal=attacker, json=body)
      verdict, note = classify_write(a)
      f = Finding(
        "horizontal",
        "rest-write",
        "POST",
        path,
        attacker.label,
        graph,
        verdict,
        a.status_code,
        f"{spec['label']}: {note}",
      )
      report.add(f)
      found.append(f)
  _assert_clean(found)


def test_core_ops_are_isolated(tenants, client: Client, report: Report) -> None:
  """Destructive/sensitive core graph ops — a non-owner must be turned away."""
  found: list[Finding] = []
  for _name, attacker, _victim, graph in _directions(tenants):
    for op in matrix.CORE_OP_WRITES:
      path = matrix.CORE_OP_PATH.format(graph_id=graph, op=op)
      a = client.post(path, principal=attacker, json={})
      verdict, note = classify_write(a)
      f = Finding(
        "horizontal",
        "core-op",
        "POST",
        path,
        attacker.label,
        graph,
        verdict,
        a.status_code,
        f"{op}: {note}",
      )
      report.add(f)
      found.append(f)
  _assert_clean(found)


def test_mcp_is_isolated(tenants, client: Client, report: Report) -> None:
  found: list[Finding] = []
  for _name, attacker, victim, graph in _directions(tenants):
    path = matrix.MCP_PATH.format(graph_id=graph)
    # read tool — data probe
    v = client.post(path, principal=victim, json=matrix.MCP_READ_BODY)
    a = client.post(path, principal=attacker, json=matrix.MCP_READ_BODY)
    verdict, note = classify_read(v, a)
    fr = Finding(
      "horizontal",
      "mcp",
      "POST",
      path,
      attacker.label,
      graph,
      verdict,
      a.status_code,
      f"read-graph-cypher: {note}",
    )
    report.add(fr)
    found.append(fr)
    # write tool — any acceptance is a leak
    aw = client.post(path, principal=attacker, json=matrix.MCP_WRITE_BODY)
    wv, wnote = classify_write(aw)
    fw = Finding(
      "horizontal",
      "mcp",
      "POST",
      path,
      attacker.label,
      graph,
      wv,
      aw.status_code,
      f"write-graph-cypher: {wnote}",
    )
    report.add(fw)
    found.append(fw)
  _assert_clean(found)


def _graphql_available(client: Client, victim: Principal, graph: str) -> bool:
  path = matrix.GRAPHQL_PATH.format(graph_id=graph)
  r = client.post(path, principal=victim, json=matrix.GRAPHQL_HELLO_BODY)
  return r.status_code != 404


def test_graphql_is_isolated(tenants, client: Client, report: Report) -> None:
  if not _graphql_available(client, tenants.tenant_b, tenants.graph_b):
    pytest.skip(
      "GraphQL endpoint not mounted (EXTENSIONS_GRAPHQL_ENABLED + a domain flag "
      "off) — enable to cover this surface"
    )
  probes = [
    (matrix.GRAPHQL_HELLO_BODY, False),  # access-control probe
    (matrix.GRAPHQL_DATA_BODY, True),  # data-leak probe (entity)
  ]
  found: list[Finding] = []
  for _name, attacker, victim, graph in _directions(tenants):
    path = matrix.GRAPHQL_PATH.format(graph_id=graph)
    for body, is_data in probes:
      v = client.post(path, principal=victim, json=body)
      a = client.post(path, principal=attacker, json=body)
      verdict, note = classify_graphql(v, a, data_field=is_data)
      f = Finding(
        "horizontal",
        "graphql",
        "POST",
        path,
        attacker.label,
        graph,
        verdict,
        a.status_code,
        f"{'entity' if is_data else 'hello'}: {note}",
      )
      report.add(f)
      found.append(f)
  _assert_clean(found)


def test_extensions_ops_are_isolated(tenants, client: Client, report: Report) -> None:
  """The roboledger + roboinvestor command/view surfaces — non-owner turned away."""
  first = matrix.EXTENSIONS_OPS[0]
  probe_path = matrix.EXTENSIONS_OP_PATH.format(
    domain=first["domain"], graph_id=tenants.graph_b, op=first["op"]
  )
  if client.post(probe_path, principal=tenants.tenant_b, json={}).status_code == 404:
    pytest.skip(
      "extensions operations not mounted (ROBOLEDGER/ROBOINVESTOR off, or graphs "
      "not provisioned with the extension)"
    )
  found: list[Finding] = []
  for _name, attacker, _victim, graph in _directions(tenants):
    for spec in matrix.EXTENSIONS_OPS:
      path = matrix.EXTENSIONS_OP_PATH.format(
        domain=spec["domain"], graph_id=graph, op=spec["op"]
      )
      a = client.post(path, principal=attacker, json={})
      verdict, note = classify_write(a)
      f = Finding(
        "horizontal",
        "extensions-op",
        "POST",
        path,
        attacker.label,
        graph,
        verdict,
        a.status_code,
        f"{spec['domain']}/{spec['op']}: {note}",
      )
      report.add(f)
      found.append(f)
  _assert_clean(found)
