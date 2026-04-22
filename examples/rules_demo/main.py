#!/usr/bin/env python3
"""Rules Engine Demo — Phase delta.3

Demonstrates the evaluate-rules operation against the library-seeded
FAC Balance Sheet structure. Shows:

1. Seeds 3 facts (Assets, Liabilities, Equity) directly into the
   tenant schema — bypassing create-report since FAC structures are
   library-seeded and don't have a report-creation path yet.
2. Calls POST /extensions/roboledger/{graph_id}/operations/evaluate-rules.
3. Prints results in a human-readable summary.
4. Breaks the identity (Assets = 1001 vs L+E = 1000), re-runs, shows
   the failing result with residual.

Usage:
    uv run python -m examples.rules_demo.main

Requires:
    Docker stack running (just start)
    Demo credentials present (.local/config.json from just demo-user)
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime, UTC
from pathlib import Path

BASE_URL = "http://localhost:8000"
CREDENTIALS_FILE = Path(".local/config.json")
DEMO_NAME = "rules_demo"

# FAC BS2 rule: $Assets = ($Liabilities + $Equity)
# Seeded variables use these qnames
FAC_ASSETS_QNAME = "fac:Assets"
FAC_LIABILITIES_QNAME = "fac:Liabilities"
FAC_EQUITY_QNAME = "fac:Equity"

PERIOD_END = date.today()
PERIOD_START = date(PERIOD_END.year, 1, 1)


# ---------------------------------------------------------------------------
# Credential helpers
# ---------------------------------------------------------------------------


def _load_credentials() -> dict:
  if not CREDENTIALS_FILE.exists():
    print("  ERROR: No credentials file. Run `just demo-user` first.")
    sys.exit(1)
  return json.loads(CREDENTIALS_FILE.read_text())


def _get_or_create_graph(creds: dict) -> str:
  """Reuse the demo graph or create a new one."""
  project_root = Path(__file__).resolve().parents[2]
  if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

  from examples.credentials.utils import (
    CredentialContext,
    ensure_user_credentials,
    get_graph_id,
    save_graph_id,
  )
  from robosystems_client.api.graphs.create_graph import (
    sync_detailed as api_create_graph,
  )
  from robosystems_client.client import AuthenticatedClient
  from robosystems_client.models import CreateGraphRequest, GraphMetadata

  context = CredentialContext(
    base_url=BASE_URL,
    credentials_path=CREDENTIALS_FILE,
    force=False,
    default_name_prefix="Rules Demo",
    default_email_prefix="rules_demo",
    api_key_prefix="Rules Demo Key",
    display_title="Rules Engine Demo Setup",
  )
  credentials = ensure_user_credentials(context)
  api_key = credentials["api_key"]

  existing = get_graph_id(CREDENTIALS_FILE, DEMO_NAME)
  if existing:
    print(f"  Reusing existing graph: {existing}")
    return existing

  client = AuthenticatedClient(
    base_url=BASE_URL,
    token=api_key,
    prefix="",
    auth_header_name="X-API-Key",
  )
  request = CreateGraphRequest(
    metadata=GraphMetadata(
      graph_name="Rules Engine Demo",
      description="Demonstrates the Phase delta.3 evaluate-rules operation",
      schema_extensions=["roboledger"],
    ),
    initial_entity={
      "name": "Demo Company",
      "entity_type": "corporation",
    },
    tags=["demo", "rules"],
  )
  response = api_create_graph(client=client, body=request)
  if not response.parsed:
    print(f"  ERROR: Failed to create graph: {response.status_code}")
    sys.exit(1)

  graph_id = response.parsed.result.graph_id  # type: ignore[union-attr]
  save_graph_id(CREDENTIALS_FILE, DEMO_NAME, graph_id)
  print(f"  Created graph: {graph_id}")
  return graph_id


# ---------------------------------------------------------------------------
# DB helpers — fact seeding bypasses create-report
# ---------------------------------------------------------------------------


def _resolve_bs_structure_id(graph_id: str) -> str:
  """Find the tenant-schema copy of the FAC Balance Sheet structure."""
  from robosystems.db.extensions import extensions_session
  from robosystems.models.extensions import Structure
  from sqlalchemy import select

  with extensions_session(graph_id) as session:
    row = session.execute(
      select(Structure.id).where(Structure.structure_type == "balance_sheet").limit(1)
    ).scalar()
    if row is None:
      print("  ERROR: No balance_sheet structure found. Has the taxonomy been seeded?")
      sys.exit(1)
    return row


def _resolve_element_ids(
  graph_id: str,
) -> tuple[str | None, str | None, str | None]:
  """Resolve fac:Assets, fac:Liabilities, fac:Equity element ids."""
  from robosystems.db.extensions import extensions_session
  from robosystems.models.extensions import Element
  from sqlalchemy import select

  with extensions_session(graph_id) as session:
    def _eid(qname: str) -> str | None:
      return session.execute(
        select(Element.id).where(Element.qname == qname).limit(1)
      ).scalar()

    return _eid(FAC_ASSETS_QNAME), _eid(FAC_LIABILITIES_QNAME), _eid(FAC_EQUITY_QNAME)


def _seed_facts(
  graph_id: str,
  structure_id: str,
  assets: float,
  liabilities: float,
  equity: float,
) -> None:
  """Insert in-scope facts for the three BS concepts.

  Deletes any existing in-scope facts for this structure first so the
  demo is idempotent on re-runs.
  """
  from robosystems.db.extensions import extensions_session
  from robosystems.models.extensions import Element
  from robosystems.models.extensions.roboledger import Fact
  from robosystems.utils.ulid import generate_prefixed_ulid
  from sqlalchemy import select, delete

  elem_assets_id, elem_liab_id, elem_equity_id = _resolve_element_ids(graph_id)
  if not all([elem_assets_id, elem_liab_id, elem_equity_id]):
    print("  ERROR: Could not resolve one or more FAC element ids.")
    print(f"    fac:Assets     → {elem_assets_id}")
    print(f"    fac:Liabilities → {elem_liab_id}")
    print(f"    fac:Equity     → {elem_equity_id}")
    sys.exit(1)

  # Use a stable fake report_id (check constraint requires report_id or fact_set_id)
  fake_report_id = "demo_rules_report"

  with extensions_session(graph_id) as session:
    # Clear existing in-scope facts for this structure
    session.execute(
      delete(Fact).where(
        Fact.structure_id == structure_id,
        Fact.fact_scope == "in_scope",
      )
    )

    facts_to_seed = [
      (elem_assets_id, assets),
      (elem_liab_id, liabilities),
      (elem_equity_id, equity),
    ]
    for elem_id, value in facts_to_seed:
      session.add(
        Fact(
          id=generate_prefixed_ulid("fact"),
          report_id=fake_report_id,
          element_id=elem_id,
          value=value,
          period_start=PERIOD_START,
          period_end=PERIOD_END,
          period_type="duration",
          unit="USD",
          entity_id="demo",
          structure_id=structure_id,
          fact_scope="in_scope",
        )
      )
    session.commit()


# ---------------------------------------------------------------------------
# evaluate-rules via HTTP
# ---------------------------------------------------------------------------


def _evaluate_rules(
  api_key: str, graph_id: str, structure_id: str, period_end: date
) -> dict:
  import urllib.request

  payload = json.dumps(
    {"structure_id": structure_id, "period_end": period_end.isoformat()}
  ).encode()
  req = urllib.request.Request(
    f"{BASE_URL}/extensions/roboledger/{graph_id}/operations/evaluate-rules",
    data=payload,
    headers={
      "Content-Type": "application/json",
      "X-API-Key": api_key,
    },
    method="POST",
  )
  with urllib.request.urlopen(req) as resp:
    return json.loads(resp.read())


# ---------------------------------------------------------------------------
# Pretty printing
# ---------------------------------------------------------------------------


def _print_results(label: str, response: dict) -> None:
  result_data = response.get("result", {})
  results = result_data.get("results", [])
  summary = result_data.get("summary", {})

  print(f"\n{label}")
  print(f"  {'Rule ID':<40} {'Pattern':<16} {'Status'}")
  print(f"  {'-'*40} {'-'*16} {'-'*10}")
  for r in results:
    icon = "✓" if r["status"] == "pass" else ("✗" if r["status"] == "fail" else "·")
    rule_id = r.get("ruleId") or r.get("rule_id", "?")
    status = r["status"]
    msg = f"  ({r['message']})" if r.get("message") else ""
    print(f"  {icon} {rule_id:<38} {status:<16}{msg}")

  total = sum(summary.values())
  print(
    f"\n  Summary: {summary.get('pass',0)} pass, "
    f"{summary.get('fail',0)} fail, "
    f"{summary.get('skipped',0)} skipped, "
    f"{summary.get('error',0)} error  ({total} total)"
  )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
  print("\n=== Phase delta.3: Rule Engine Demo ===\n")

  creds = _load_credentials()
  api_key = creds.get("api_key", "")
  if not api_key:
    print("  ERROR: api_key missing from .local/config.json")
    sys.exit(1)

  print("Step 1: Resolving graph...")
  graph_id = _get_or_create_graph(creds)

  print("Step 2: Resolving Balance Sheet structure...")
  structure_id = _resolve_bs_structure_id(graph_id)
  print(f"  structure_id = {structure_id}")

  # ── Run 1: identity holds ──────────────────────────────────────────────
  assets, liabilities, equity = 1000.0, 600.0, 400.0
  print(f"\nStep 3: Seeding facts (assets={assets}, liabilities={liabilities}, equity={equity})...")
  _seed_facts(graph_id, structure_id, assets, liabilities, equity)
  print("  Facts seeded.")

  print("\nStep 4: Running evaluate-rules...")
  response1 = _evaluate_rules(api_key, graph_id, structure_id, PERIOD_END)
  _print_results("=== Run 1: identity holds ($1,000 = $600 + $400) ===", response1)

  # ── Run 2: break the identity ──────────────────────────────────────────
  assets_broken = 1001.0
  print(f"\nStep 5: Breaking the identity (assets={assets_broken}, liabilities={liabilities}, equity={equity})...")
  _seed_facts(graph_id, structure_id, assets_broken, liabilities, equity)
  print("  Facts updated.")

  print("\nStep 6: Re-running evaluate-rules...")
  response2 = _evaluate_rules(api_key, graph_id, structure_id, PERIOD_END)
  _print_results("=== Run 2: identity broken ($1,001 != $600 + $400) ===", response2)

  print("\n=== Demo complete. ===\n")


if __name__ == "__main__":
  main()
