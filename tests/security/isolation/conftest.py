"""Fixtures + skip gate for the tenant-isolation harness.

The whole suite is opt-in: it only runs when TARGET_API_URL points at a live
deployment. In a normal `just test` / CI run it is collected and then skips,
exactly like the DB-backed integration tests skip when their resource is
absent — so it is inert by default and never gates unrelated work.

Run it explicitly:

    TARGET_API_URL=http://localhost:8000 uv run pytest tests/security/isolation -m isolation -s

A JSON report is written to .local/isolation-report.json (override with
ISOLATION_REPORT). Nothing here touches the database — provisioning and the
matrix are pure HTTP; DB-side teardown verification is the runbook's tunnel
step, not the harness's.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from ._http import Client
from ._provision import provision
from ._report import Report

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_REPORT = _REPO_ROOT / ".local" / "isolation-report.json"


@pytest.fixture(scope="session")
def target_url() -> str:
  url = os.environ.get("TARGET_API_URL")
  if not url:
    pytest.skip(
      "TARGET_API_URL not set — the isolation harness is opt-in; point it at a "
      "deployment (e.g. TARGET_API_URL=http://localhost:8000)"
    )
  return url.rstrip("/")


@pytest.fixture(scope="session")
def client(target_url: str) -> Client:
  c = Client(target_url)
  yield c
  c.close()


@pytest.fixture(scope="session")
def report():
  r = Report()
  yield r
  print(r.summary())
  path = os.environ.get("ISOLATION_REPORT") or str(_DEFAULT_REPORT)
  try:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    r.write_json(path)
    print(f"[isolation] report → {path}")
  except OSError as exc:  # pragma: no cover - best effort
    print(f"[isolation] could not write report to {path}: {exc}")


@pytest.fixture(scope="session")
def tenants(client: Client):
  """Provision two independent tenants (each with an entity graph)."""
  fx = provision(client)
  yield fx
  # Best-effort teardown: delete both graphs as their owners. Full disposal
  # verification (that the rows and OpenSearch content are actually gone) is
  # the runbook's DB-tunnel step, deliberately not automated here.
  for owner, gid in ((fx.tenant_a, fx.graph_a), (fx.tenant_b, fx.graph_b)):
    try:
      client.post(
        f"/v1/graphs/{gid}/operations/delete-graph",
        principal=owner,
        json={"confirm": gid, "at_period_end": False},
      )
    except Exception:
      pass
