#!/usr/bin/env python3
"""Rules Engine Demo — Phase delta.3

Demonstrates the evaluate-rules operation against the library-seeded
FAC Balance Sheet structure:

1. Creates (or reuses) a demo graph with roboledger enabled.
2. Seeds 3 facts (Assets, Liabilities, Equity) directly into the
   tenant schema.
3. Calls POST /extensions/roboledger/{graph_id}/operations/evaluate-rules.
4. Prints results in a human-readable summary.
5. Breaks the identity (Assets = 1001 vs L+E = 1000), re-runs, shows
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
import time
from datetime import date
from pathlib import Path

BASE_URL = "http://localhost:8000"
CREDENTIALS_FILE = Path(".local/config.json")
DEMO_NAME = "rules_demo"

FAC_ASSETS_QNAME = "fac:Assets"
FAC_LIABILITIES_QNAME = "fac:Liabilities"
FAC_EQUITY_QNAME = "fac:Equity"

PERIOD_END = date.today()
PERIOD_START = date(PERIOD_END.year, 1, 1)


# ---------------------------------------------------------------------------
# Graph setup
# ---------------------------------------------------------------------------


def _get_or_create_graph() -> tuple[str, str]:
  """Return (graph_id, api_key), creating a graph if needed."""
  from examples.credentials.utils import (
    CredentialContext,
    ensure_user_credentials,
    get_graph_id,
    save_graph_id,
  )
  from robosystems_client.api.graphs.create_graph import sync_detailed as api_create_graph
  from robosystems_client.api.operations.get_operation_status import (
    sync_detailed as api_get_operation_status,
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
    return existing, api_key

  client = AuthenticatedClient(
    base_url=BASE_URL, token=api_key, prefix="", auth_header_name="X-API-Key"
  )
  request = CreateGraphRequest(
    metadata=GraphMetadata(
      graph_name="Rules Engine Demo",
      description="Phase delta.3 evaluate-rules demo",
      schema_extensions=["roboledger"],
    ),
    initial_entity={"name": "Demo Company", "entity_type": "corporation", "uri": "https://demo.robosystems.ai"},
    tags=["demo", "rules"],
  )
  response = api_create_graph(client=client, body=request)
  if not response.parsed:
    print(f"  ERROR: Failed to create graph: {response.status_code}")
    sys.exit(1)

  parsed = response.parsed
  graph_id = getattr(parsed, "graph_id", None)
  operation_id = getattr(parsed, "operation_id", None)
  if isinstance(parsed, dict):
    graph_id = parsed.get("graph_id")
    operation_id = parsed.get("operation_id")

  if not graph_id and operation_id:
    print(f"  Queued (operation: {operation_id}), waiting...")
    for _ in range(30):
      time.sleep(2)
      status_resp = api_get_operation_status(operation_id=operation_id, client=client)
      if not status_resp.parsed:
        continue
      status_data = status_resp.parsed
      if isinstance(status_data, dict):
        status = status_data.get("status")
        result = status_data.get("result", {})
      elif hasattr(status_data, "additional_properties"):
        props = status_data.additional_properties
        status = props.get("status")
        result = props.get("result", {})
      else:
        status = getattr(status_data, "status", None)
        result = getattr(status_data, "result", {})
      if isinstance(result, dict):
        graph_id = graph_id or result.get("graph_id")
      if status == "completed":
        break
      if status == "failed":
        print(f"  Graph creation failed: {result}")
        sys.exit(1)

  if not graph_id:
    print("  Timed out waiting for graph creation")
    sys.exit(1)

  save_graph_id(CREDENTIALS_FILE, DEMO_NAME, graph_id, time.strftime("%Y-%m-%d %H:%M:%S"))
  print(f"  Created graph: {graph_id}")
  return graph_id, api_key


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _resolve_bs_structure_id(graph_id: str) -> str:
  """Find the FAC BS2 calculation structure (the one the bs-identity rule targets)."""
  from sqlalchemy import text

  from robosystems.db.extensions import extensions_session

  with extensions_session(graph_id) as session:
    # Find the structure targeted by fac-rule-bs-identity — it's the FAC BS2
    # calculation structure (structure_type='custom'), not the balance_sheet
    # presentation structure.
    row = session.execute(
      text(
        "SELECT target_structure_id FROM rules "
        "WHERE rule_expression LIKE '%Assets%Liabilities%Equity%' "
        "AND rule_pattern = 'EqualTo' "
        "AND target_structure_id IS NOT NULL "
        "LIMIT 1"
      )
    ).scalar()
    if row is None:
      print("  ERROR: Could not find FAC BS identity rule — has the taxonomy been seeded?")
      sys.exit(1)
    return row


def _resolve_element_id(graph_id: str, qname: str) -> str:
  from sqlalchemy import select

  from robosystems.db.extensions import extensions_session
  from robosystems.models.extensions import Element

  with extensions_session(graph_id) as session:
    row = session.execute(
      select(Element.id).where(Element.qname == qname).limit(1)
    ).scalar()
    if row is None:
      print(f"  ERROR: Element not found for qname {qname!r}")
      sys.exit(1)
    return row


def _seed_facts(
  graph_id: str,
  structure_id: str,
  assets: float,
  liabilities: float,
  equity: float,
) -> None:
  from sqlalchemy import delete

  from robosystems.db.extensions import extensions_session
  from robosystems.models.extensions.roboledger import Fact
  from robosystems.utils.ulid import generate_prefixed_ulid

  elem_assets = _resolve_element_id(graph_id, FAC_ASSETS_QNAME)
  elem_liab = _resolve_element_id(graph_id, FAC_LIABILITIES_QNAME)
  elem_equity = _resolve_element_id(graph_id, FAC_EQUITY_QNAME)

  with extensions_session(graph_id) as session:
    session.execute(
      delete(Fact).where(
        Fact.structure_id == structure_id,
        Fact.fact_scope == "in_scope",
      )
    )
    for elem_id, value in [
      (elem_assets, assets),
      (elem_liab, liabilities),
      (elem_equity, equity),
    ]:
      session.add(
        Fact(
          id=generate_prefixed_ulid("fact"),
          report_id="rules_demo",
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
# evaluate-rules
# ---------------------------------------------------------------------------


def _evaluate_rules(api_key: str, graph_id: str, structure_id: str) -> dict:
  import urllib.request

  payload = json.dumps(
    {"structure_id": structure_id, "period_end": PERIOD_END.isoformat()}
  ).encode()
  req = urllib.request.Request(
    f"{BASE_URL}/extensions/roboledger/{graph_id}/operations/evaluate-rules",
    data=payload,
    headers={"Content-Type": "application/json", "X-API-Key": api_key},
    method="POST",
  )
  with urllib.request.urlopen(req) as resp:
    return json.loads(resp.read())


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def _print_results(label: str, response: dict) -> None:
  result_data = response.get("result", {})
  results = result_data.get("results", [])
  summary = result_data.get("summary", {})

  print(f"\n{label}")
  print(f"  {'Rule':<42} {'Pattern':<14} Status")
  print(f"  {'-'*42} {'-'*14} {'-'*10}")
  for r in results:
    icon = "✓" if r["status"] == "pass" else ("✗" if r["status"] == "fail" else "·")
    rule_id = (r.get("ruleId") or r.get("rule_id", "?"))[-42:]
    msg = f"  ({r['message']})" if r.get("message") else ""
    print(f"  {icon} {rule_id:<41} {r['status']:<14}{msg}")

  print(
    f"\n  Summary: {summary.get('pass', 0)} pass  "
    f"{summary.get('fail', 0)} fail  "
    f"{summary.get('skipped', 0)} skipped  "
    f"{summary.get('error', 0)} error"
  )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
  print("\n=== Phase delta.3: Rule Engine Demo ===\n")

  print("Step 1: Resolving graph...")
  graph_id, api_key = _get_or_create_graph()

  print("\nStep 2: Resolving Balance Sheet structure...")
  structure_id = _resolve_bs_structure_id(graph_id)
  print(f"  structure_id = {structure_id}")

  # Run 1 — identity holds
  print("\nStep 3: Seeding facts  assets=$1,000  liabilities=$600  equity=$400")
  _seed_facts(graph_id, structure_id, 1000.0, 600.0, 400.0)

  print("Step 4: Running evaluate-rules...")
  _print_results(
    "=== Run 1: identity holds ($1,000 = $600 + $400) ===",
    _evaluate_rules(api_key, graph_id, structure_id),
  )

  # Run 2 — break the identity
  print("\nStep 5: Breaking the identity  assets=$1,001  (L+E still $1,000)")
  _seed_facts(graph_id, structure_id, 1001.0, 600.0, 400.0)

  print("Step 6: Re-running evaluate-rules...")
  _print_results(
    "=== Run 2: identity broken ($1,001 != $600 + $400) ===",
    _evaluate_rules(api_key, graph_id, structure_id),
  )

  print("\n=== Demo complete. ===\n")


if __name__ == "__main__":
  main()
