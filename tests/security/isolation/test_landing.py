"""Landing axis — an authorized write lands where it should, and nowhere else.

The horizontal and vertical axes prove denial: a non-owner is turned away.
They cannot see an *authorized* request doing the wrong thing — the class
the tenant-schema fall-through belonged to, where an owner's write was
accepted and landed in a schema every tenant shares. This axis proves the
landing: the owner writes through the extensions command surface, reads the
row back through GraphQL on the same graph, and the same owner's *other*
graph does not see it. That last leg is the sharp one — same principal, same
authorization, different tenant schema — so it isolates the schema binding
from the access control.
"""

from __future__ import annotations

import secrets

import pytest

from ._http import Client
from ._oracle import Verdict
from ._report import Finding, Report

pytestmark = [
  pytest.mark.isolation,
  pytest.mark.security,
  pytest.mark.integration,
]

_AGENTS_QUERY = {"query": "{ agents(isActive: true) { name } }"}


def _agent_names(response) -> list[str]:
  if response.status_code // 100 != 2:
    return []
  data = (response.json() or {}).get("data") or {}
  return [a.get("name", "") for a in (data.get("agents") or [])]


def test_owner_write_lands_only_in_its_own_graph(
  tenants, client: Client, report: Report
) -> None:
  nonce = f"IsoHarness Landing {secrets.token_hex(4)}"
  op = f"/extensions/roboledger/{tenants.graph_a}/operations/create-agent"
  w = client.post(
    op,
    principal=tenants.tenant_a,
    json={"agent_type": "vendor", "name": nonce},
  )
  if w.status_code in (403, 404) and "roboledger" in w.text.lower():
    pytest.skip(f"roboledger not provisioned on the target graph: {w.text[:120]}")
  assert w.status_code // 100 == 2, (
    f"positive control failed: owner write refused ({w.status_code}) {w.text[:200]}"
  )

  # Read it back on the graph it was written to.
  own = client.post(
    f"/extensions/{tenants.graph_a}/graphql",
    principal=tenants.tenant_a,
    json=_AGENTS_QUERY,
  )
  own_names = _agent_names(own)
  assert nonce in own_names, (
    f"write accepted but not readable on its own graph ({own.status_code}): "
    f"{own.text[:200]}"
  )

  # The same owner's other graph — same authorization, different tenant
  # schema — must not see it. If graph_a2 could not be provisioned, fall back
  # to tenant B's graph as B (weaker: also covered by the horizontal axis).
  if tenants.graph_a2:
    other_graph, other_principal, label = tenants.graph_a2, tenants.tenant_a, "A→A2"
  else:
    other_graph, other_principal, label = tenants.graph_b, tenants.tenant_b, "B"
  other = client.post(
    f"/extensions/{other_graph}/graphql",
    principal=other_principal,
    json=_AGENTS_QUERY,
  )
  other_names = _agent_names(other)
  leaked = nonce in other_names
  report.add(
    Finding(
      "landing",
      "graphql-readback",
      "POST",
      f"/extensions/{other_graph}/graphql",
      label,
      other_graph,
      Verdict.LEAK if leaked else Verdict.PASS,
      other.status_code,
      "row written to graph_a visible from another graph"
      if leaked
      else "row confined to the graph it was written to",
    )
  )
  assert not leaked, (
    f"LANDING LEAK: a row written to {tenants.graph_a} is visible from "
    f"{other_graph} — the write did not land in the tenant's own schema"
  )
