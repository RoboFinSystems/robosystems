"""Scoping axis — isolation units finer than the user↔graph pair.

Two controls the base matrix doesn't reach:

- **Subgraph isolation.** The invariant keys on `graph_id` *including subgraphs*
  (`{parent}_{name}`). A subgraph is its own isolation unit routed through a
  distinct access path (`is_shared_repository_or_subgraph`). A non-owner must be
  denied on the subgraph id, not just the parent.
- **Graph-scoped API keys.** A key minted with a `graph_id` is scoped: allowed on
  that graph (and its subgraphs) and *denied everywhere else* — even on another
  graph the same user owns. That last case is the sharp test: the denial must
  come from the key's scope, not from user access, so we fire it at `graph_a2`,
  which the key's owner *does* own.
"""

from __future__ import annotations

import pytest

from ._http import Client
from ._oracle import Verdict, classify_read
from ._report import Finding, Report

pytestmark = [
  pytest.mark.isolation,
  pytest.mark.security,
  pytest.mark.integration,
]

_CYPHER = {"query": "MATCH (n) RETURN count(n) AS c", "parameters": {}}


def _assert_clean(found: list[Finding]) -> None:
  leaks = [f for f in found if f.verdict == Verdict.LEAK]
  invalid = [f for f in found if f.verdict == Verdict.INVALID]
  assert not invalid, (
    "positive control(s) failed — harness misconfigured:\n"
    + "\n".join(f"  {f.method} {f.path}: {f.note}" for f in invalid)
  )
  assert not leaks, "SCOPE LEAK(S):\n" + "\n".join(
    f"  {f.surface} {f.method} {f.path} as {f.attacker} → {f.status}: {f.note}"
    for f in leaks
  )


def test_subgraph_isolation(tenants, client: Client, report: Report) -> None:
  """Tenant B must be denied on tenant A's subgraph id."""
  sg = tenants.subgraph_a
  if not sg:
    pytest.skip("no subgraph provisioned (tier without subgraph support, or op failed)")
  found: list[Finding] = []
  # /members is intentionally rejected on subgraphs, so it's not a valid probe;
  # /info and /schema are, and the Cypher probe below reads actual data.
  reads = [
    f"/v1/graphs/{sg}/info",
    f"/v1/graphs/{sg}/schema",
  ]
  for path in reads:
    # owner of the parent can see the subgraph (positive); tenant B cannot
    v = client.get(path, principal=tenants.tenant_a)
    a = client.get(path, principal=tenants.tenant_b)
    verdict, note = classify_read(v, a)
    f = Finding(
      "scope", "subgraph-read", "GET", path, "B", sg, verdict, a.status_code, note
    )
    report.add(f)
    found.append(f)
  # cypher on the subgraph — the data probe
  cpath = f"/v1/graphs/{sg}/query/cypher"
  v = client.post(cpath, principal=tenants.tenant_a, json=_CYPHER)
  a = client.post(cpath, principal=tenants.tenant_b, json=_CYPHER)
  verdict, note = classify_read(v, a)
  f = Finding(
    "scope", "subgraph-cypher", "POST", cpath, "B", sg, verdict, a.status_code, note
  )
  report.add(f)
  found.append(f)
  _assert_clean(found)


def test_graph_scoped_key_enforcement(tenants, client: Client, report: Report) -> None:
  """A graph_a-scoped key must be denied on graph_a2, which its owner also owns."""
  scoped = tenants.scoped_key_a
  if scoped is None or not tenants.graph_a2:
    pytest.skip("scoped key or second graph not provisioned")

  # Sanity: the scoped key works within its scope (graph_a) — so a denial on
  # graph_a2 is scope enforcement, not a dead key.
  in_scope = client.get(f"/v1/graphs/{tenants.graph_a}/info", principal=scoped)
  assert in_scope.status_code // 100 == 2, (
    f"scoped key denied on its own graph ({in_scope.status_code}) — can't attribute "
    f"the graph_a2 denial to scope"
  )

  found: list[Finding] = []
  for path in (
    f"/v1/graphs/{tenants.graph_a2}/info",
    f"/v1/graphs/{tenants.graph_a2}/schema",
    f"/v1/graphs/{tenants.graph_a2}/query/cypher",
  ):
    is_cypher = path.endswith("/query/cypher")
    if is_cypher:
      v = client.post(path, principal=tenants.tenant_a, json=_CYPHER)  # account-wide
      a = client.post(path, principal=scoped, json=_CYPHER)  # scoped to graph_a
    else:
      v = client.get(path, principal=tenants.tenant_a)
      a = client.get(path, principal=scoped)
    verdict, note = classify_read(v, a)
    f = Finding(
      "scope",
      "scoped-key",
      "POST" if is_cypher else "GET",
      path,
      "A-scoped",
      tenants.graph_a2,
      verdict,
      a.status_code,
      note,
    )
    report.add(f)
    found.append(f)
  _assert_clean(found)
