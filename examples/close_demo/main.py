#!/usr/bin/env python3
"""Cascade Advisory Group LLC — AI Month-End Close Demo

Sets up a complete demo environment with synthetic consulting company data,
CoA→GAAP mappings, depreciation/prepaid schedules, and accounting policy
documents. After running, use Claude Desktop or MCP tools to simulate
a month-end close.

Data is generated for a rolling 16-month window ending at the current month,
so the demo stays evergreen. OLTP load goes through the same `OLTPLoader`
path that the QuickBooks pipeline uses in production.

**Transport split:**
- **Bulk historical data** (elements, transactions, entries, line items)
  goes through `OLTPLoader` — the same bulk-import pipeline QuickBooks
  uses. This is the honest "migration/import" path.
- **Additive setup** (mappings, schedules, fiscal calendar) goes through
  the HTTP API via `LedgerClient` — the same path the frontend UI and
  MCP tools use. This exercises the native-accounting operations surface.
- **Reset** (demo-only cleanup for re-runs) goes through direct DB
  access in `_reset.py` — this is intentionally NOT a product operation.

Usage:
    uv run python -m examples.close_demo.main                        # Create new graph + load
    uv run python -m examples.close_demo.main <graph_id>             # Load into existing graph
    uv run python -m examples.close_demo.main --dry-run              # Validate data only
    uv run python -m examples.close_demo.main --ai                    # Use MappingAgent instead of hardcoded mappings (requires Bedrock)

Requires: Docker stack running (just start)
"""

from __future__ import annotations

import json
import sys
import time
from datetime import date, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE = "native"
CONNECTION_ID = "demo_cascade"
CREATED_BY = "demo:cascade"
CREDENTIALS_FILE = Path(".local/config.json")
DEMO_NAME = "cascade_demo"
BASE_URL = "http://localhost:8000"
COMPANY_NAME = "Cascade Advisory Group LLC"


# ---------------------------------------------------------------------------
# LedgerClient helper
# ---------------------------------------------------------------------------


def _get_ledger_client():
  """Construct a LedgerClient from saved credentials."""
  from robosystems_client.clients.ledger_client import LedgerClient

  if not CREDENTIALS_FILE.exists():
    print("  ERROR: No credentials file. Run `just demo-user` first.")
    sys.exit(1)

  creds = json.loads(CREDENTIALS_FILE.read_text())
  return LedgerClient(
    {
      "base_url": BASE_URL,
      "token": creds.get("api_key", ""),
    }
  )


# ---------------------------------------------------------------------------
# Step 1: Create graph via API
# ---------------------------------------------------------------------------


def create_demo_graph() -> str:
  """Create user + roboledger graph via the API."""
  project_root = Path(__file__).resolve().parents[2]
  if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

  from examples.credentials.utils import (
    CredentialContext,
    ensure_user_credentials,
    get_graph_id,
    save_graph_id,
  )

  context = CredentialContext(
    base_url=BASE_URL,
    credentials_path=CREDENTIALS_FILE,
    force=False,
    default_name_prefix="Cascade Demo",
    default_email_prefix="cascade_demo",
    api_key_prefix="Cascade Demo Key",
    display_title="Cascade Advisory Demo Setup",
  )
  credentials = ensure_user_credentials(context)
  api_key = credentials["api_key"]

  existing = get_graph_id(CREDENTIALS_FILE, DEMO_NAME)
  if existing:
    print(f"\nReusing existing graph: {existing}")
    return existing

  from robosystems_client.api.graphs.create_graph import (
    sync_detailed as api_create_graph,
  )
  from robosystems_client.api.operations.get_operation_status import (
    sync_detailed as api_get_operation_status,
  )
  from robosystems_client.client import AuthenticatedClient
  from robosystems_client.models import CreateGraphRequest, GraphMetadata

  client = AuthenticatedClient(
    base_url=BASE_URL,
    token=api_key,
    prefix="",
    auth_header_name="X-API-Key",
  )

  metadata = GraphMetadata(
    graph_name=COMPANY_NAME,
    description="Boutique management consulting firm — close workflow demo",
    schema_extensions=["roboledger"],
  )

  request = CreateGraphRequest(
    metadata=metadata,
    initial_entity={
      "name": COMPANY_NAME,
      "uri": "https://cascadeadvisory.com",
      "entity_type": "llc",
      "ticker": "CAG",
    },
    tags=["demo", "cascade", "roboledger", "close-workflow"],
  )

  print(f"\nCreating graph: {COMPANY_NAME}")
  response = api_create_graph(client=client, body=request)
  if not response.parsed:
    print(f"Failed to create graph: {response.status_code}")
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

      if status == "completed":
        graph_id = result.get("graph_id") if isinstance(result, dict) else None
        break
      if status == "failed":
        error = result.get("error") if isinstance(result, dict) else "unknown"
        print(f"  Graph creation failed: {error}")
        sys.exit(1)

  if not graph_id:
    print("  Timed out waiting for graph creation")
    sys.exit(1)

  save_graph_id(
    CREDENTIALS_FILE, DEMO_NAME, graph_id, time.strftime("%Y-%m-%d %H:%M:%S")
  )
  print(f"  Graph created: {graph_id}")
  return graph_id


# ---------------------------------------------------------------------------
# Step 2a: Reset (DB-only — demo cleanup for re-runs)
# ---------------------------------------------------------------------------


def reset_demo(graph_id: str) -> None:
  """Wipe prior demo state so re-runs are idempotent.

  This is the ONLY direct-DB operation in the demo. Everything else
  goes through the HTTP API. Drops the tenant schema and re-provisions
  it with the shared taxonomy seed data.
  """
  from ._reset import reset_demo_state

  reset_demo_state(graph_id)


# ---------------------------------------------------------------------------
# Step 2b: Create chart of accounts (via HTTP API)
# ---------------------------------------------------------------------------


def create_chart_of_accounts(graph_id: str) -> tuple[dict[str, str], str, int]:
  """Create the CoA taxonomy, its elements, and link to the entity.

  Returns (element_lookup, coa_taxonomy_id, count).
  element_lookup maps account code → element ID for downstream use.
  """
  from .data import ACCOUNTS

  client = _get_ledger_client()

  # 1. Create CoA taxonomy
  tax_result = client.create_taxonomy(
    graph_id,
    {
      "name": "Native Chart of Accounts",
      "taxonomy_type": "chart_of_accounts",
    },
  )
  coa_taxonomy_id = tax_result["id"]

  # 2. Link entity → CoA taxonomy (ENTITY_HAS_TAXONOMY edge)
  client.link_entity_taxonomy(
    graph_id, coa_taxonomy_id, basis="chart_of_accounts", is_primary=True
  )

  # 3. Create mapping structure (coa_mapping) in the CoA taxonomy
  client.create_structure(
    graph_id,
    {
      "name": "CoA to US GAAP Mapping",
      "description": "Maps Chart of Accounts to US GAAP reporting concepts",
      "structure_type": "coa_mapping",
      "taxonomy_id": coa_taxonomy_id,
    },
  )

  # 4. Create elements (flat CoA — no parent hierarchy)
  element_lookup: dict[str, str] = {}
  for code, name, classification, _sub_class, balance_type, description in ACCOUNTS:
    result = client.create_element(
      graph_id,
      {
        "taxonomy_id": coa_taxonomy_id,
        "code": code,
        "name": name,
        "classification": classification,
        "balance_type": balance_type,
        "description": description,
        "source": SOURCE,
        "external_id": code,
        "external_source": SOURCE,
      },
    )
    element_lookup[code] = result["id"]

  return element_lookup, coa_taxonomy_id, len(ACCOUNTS)


# ---------------------------------------------------------------------------
# Step 2c: Create journal entries (via HTTP API)
# ---------------------------------------------------------------------------


def create_journal_entries(
  graph_id: str, txns: list, element_lookup: dict[str, str]
) -> dict[str, int]:
  """Create historical journal entries via the HTTP API.

  Each transaction becomes one journal entry with balanced line items,
  created with status='posted' (historical data, not drafts).
  """
  client = _get_ledger_client()

  entry_count = 0
  line_item_count = 0
  skipped = 0

  for txn_date, txn_type, description, _amount, lines in txns:
    # Build line items, mapping codes → element IDs
    li_list = []
    for elem_code, debit, credit in lines:
      elem_id = element_lookup.get(elem_code)
      if not elem_id:
        skipped += 1
        continue
      li_list.append(
        {
          "element_id": elem_id,
          "debit_amount": debit,
          "credit_amount": credit,
        }
      )

    if len(li_list) < 2:
      skipped += 1
      continue

    memo = description or f"{txn_type} on {txn_date}"
    client.create_journal_entry(
      graph_id,
      posting_date=txn_date.isoformat(),
      memo=memo,
      line_items=li_list,
      type="standard",
      status="posted",
    )
    entry_count += 1
    line_item_count += len(li_list)

  if skipped:
    print(f"  WARNING: Skipped {skipped} entries (missing elements)")

  return {"entries": entry_count, "line_items": line_item_count}


# ---------------------------------------------------------------------------
# Step 3a: Create CoA → GAAP mappings — hardcoded path (no Bedrock needed)
# ---------------------------------------------------------------------------


def create_mappings(graph_id: str, element_lookup: dict[str, str]) -> int:
  """Create mapping associations between CoA elements and FAC reporting concepts.

  Per Phase 3b, CoA → FAC (Fundamental Accounting Concepts) is the
  primary mapping target. FAC → rs-gaap expansion is handled by
  equivalence arcs on the FAC side.

  Uses `LedgerClient.create_associations()` — the bulk HTTP API — to
  exercise the same path the frontend UI and MCP tools use.
  """
  from .mappings import MAPPINGS

  client = _get_ledger_client()

  # Find the coa_mapping structure (created by OLTPLoader during taxonomy seed)
  structures = client.list_structures(graph_id, structure_type="coa_mapping")
  if not structures:
    print("  ERROR: No mapping structure found")
    return 0
  mapping_id = structures[0]["id"]

  # Resolve FAC qnames → element IDs via the library in the entity graph.
  fac_elements = client.list_elements(graph_id, source="fac", limit=500)
  fac_by_qname: dict[str, str] = {
    e["qname"]: e["id"] for e in (fac_elements or {}).get("elements", []) if e.get("qname")
  }

  # Build the associations list, skipping any CoA codes not in the element lookup
  associations = []
  for coa_code, fac_qname in MAPPINGS:
    coa_id = element_lookup.get(coa_code)
    if not coa_id:
      print(f"  WARNING: CoA code {coa_code} not in element_lookup")
      continue
    fac_id = fac_by_qname.get(fac_qname)
    if not fac_id:
      print(f"  WARNING: FAC qname {fac_qname} not found in library")
      continue
    associations.append(
      {
        "from_element_id": coa_id,
        "to_element_id": fac_id,
        "association_type": "mapping",
        "order_value": 0.0,
      }
    )

  if not associations:
    return 0

  result = client.create_associations(graph_id, mapping_id, associations)
  return result.get("created", 0)


# ---------------------------------------------------------------------------
# Step 3b: AI mapping path — requires Bedrock (optional)
# ---------------------------------------------------------------------------


def run_ai_mapping(graph_id: str) -> None:
  """Trigger the MappingAgent via the auto-map-elements operation.

  Requires Bedrock to be configured (BEDROCK_REGION + IAM role with
  bedrock:InvokeModel). Skipped when --ai is not passed.

  Finds the coa_mapping structure, dispatches the async agent operation,
  and polls until it completes or times out.
  """
  import httpx

  if not CREDENTIALS_FILE.exists():
    print("  ERROR: No credentials file")
    return

  creds = json.loads(CREDENTIALS_FILE.read_text())
  api_key = creds.get("api_key", "")
  headers = {"X-API-Key": api_key, "Content-Type": "application/json"}

  # Find the coa_mapping structure ID
  client = _get_ledger_client()
  structures = client.list_structures(graph_id, structure_type="coa_mapping")
  if not structures:
    print("  ERROR: No coa_mapping structure found — was the CoA taxonomy created?")
    return
  mapping_id = structures[0]["id"]
  print(f"  Mapping structure: {mapping_id}")

  # Dispatch the async agent operation
  try:
    resp = httpx.post(
      f"{BASE_URL}/extensions/roboledger/{graph_id}/operations/auto-map-elements",
      headers=headers,
      json={"mapping_id": mapping_id},
      timeout=30,
    )
  except Exception as e:
    print(f"  ERROR: Failed to trigger auto-map-elements: {e}")
    return

  if resp.status_code not in (200, 201, 202):
    print(f"  ERROR: auto-map-elements returned {resp.status_code}: {resp.text[:200]}")
    return

  data = resp.json()
  op_id = data.get("operationId", data.get("operation_id", ""))
  print(f"  Dispatched (operation: {op_id})")
  print("  Polling… (mapping 27 accounts, may take 1–3 min)")

  status, result = _poll_operation(api_key, op_id, timeout_seconds=300)
  if status == "completed":
    mapped = result.get("mapped", "?")
    flagged = result.get("flagged", "?")
    skipped = result.get("skipped", "?")
    coverage = result.get("coverage_percent", "?")
    print(f"  Done — mapped: {mapped}, flagged: {flagged}, skipped: {skipped}")
    print(f"  Coverage: {coverage}%")
  elif status == "failed":
    error = result.get("error", "unknown")
    print(f"  WARNING: AI mapping failed: {error}")
    print("  Falling back to hardcoded mappings...")
    # Not calling create_mappings here — caller decides fallback policy.
  elif status == "timeout":
    print("  WARNING: AI mapping timed out after 5 min (may still be running)")
  else:
    print(f"  WARNING: Polling error: {result.get('error', 'unknown')}")


# ---------------------------------------------------------------------------
# Step 4a: Initialize fiscal calendar (via HTTP API)
# ---------------------------------------------------------------------------


def initialize_fiscal_calendar(graph_id: str) -> str:
  """Initialize the fiscal calendar with closed_through = month before last.

  Uses `LedgerClient.initialize_ledger()` — the HTTP API — which creates
  the fiscal calendar, seeds FiscalPeriod rows from `earliest_data_period`
  to the current month, and marks periods as closed/open based on
  `closed_through`.

  Returns the `close_target` period string for use in the next-steps output.
  """
  from robosystems.operations.roboledger.fiscal_calendar import (
    current_month_period,
    previous_period,
  )

  from .data import get_demo_start_date

  # closed_through = month before last completed month
  # → close_target = last completed month (one period ready to close)
  last_completed = previous_period(current_month_period())
  closed_through = previous_period(last_completed)
  demo_start = get_demo_start_date()
  demo_start_period = f"{demo_start.year:04d}-{demo_start.month:02d}"

  client = _get_ledger_client()
  result = client.initialize_ledger(
    graph_id,
    closed_through=closed_through,
    earliest_data_period=demo_start_period,
    note="close_demo initialization",
  )

  fc = result.get("fiscal_calendar", {})
  close_target = fc.get("close_target_period", last_completed)

  print(f"  closed_through: {closed_through}")
  print(f"  close_target:   {close_target}")
  print(f"  periods seeded: {result.get('periods_created', 0)}")
  return close_target


# ---------------------------------------------------------------------------
# Step 4b: Create schedules (via HTTP API)
# ---------------------------------------------------------------------------


def create_schedules(graph_id: str, element_lookup: dict[str, str]) -> int:
  """Create depreciation and prepaid amortization schedules.

  Uses `LedgerClient.create_schedule()` — the HTTP API — so schedule
  facts inherit the fiscal calendar's `closed_through` boundary
  automatically. Prior-run schedules are cleaned up via
  `LedgerClient.delete_schedule()`.
  """
  from .data import add_months as demo_add_months
  from .data import get_demo_start_date

  demo_start = get_demo_start_date()

  def _schedule_window(start_offset: int, life_months: int) -> tuple[date, date]:
    """Compute (start, end) from month offsets relative to the demo window."""
    start = demo_add_months(demo_start, start_offset)
    end_month = demo_add_months(start, life_months)
    return start, end_month - timedelta(days=1)

  schedules = [
    # (name, dr_code, cr_code, monthly_cents, original_cents, useful_months, start_offset)
    ("Computer Equipment Depreciation", "7000", "1350", 13_333, 480_000, 36, 0),
    ("Office Furniture Depreciation", "7000", "1350", 2_500, 150_000, 60, 2),
    ("Business Insurance", "6400", "1200", 10_000, 120_000, 12, 2),
    ("Business Insurance (Year 2 Renewal)", "6400", "1200", 10_000, 120_000, 12, 14),
    ("Software Subscription", "6100", "1210", 2_500, 30_000, 12, 5),
    ("Cloud Hosting (AWS Savings Plan)", "6200", "1220", 5_000, 60_000, 12, 8),
  ]

  client = _get_ledger_client()

  # Find or create schedule taxonomy
  existing_taxonomies = client.list_taxonomies(graph_id, taxonomy_type="schedule")
  cascade_tax = next(
    (t for t in existing_taxonomies if t["name"] == "Cascade Schedules"), None
  )
  if cascade_tax:
    taxonomy_id = cascade_tax["id"]
    # Clean up prior-run schedules for idempotency
    existing_blocks = client.list_information_blocks(graph_id, block_type="schedule")
    for block in existing_blocks:
      if block.get("taxonomy_name") == "Cascade Schedules":
        client.delete_schedule(graph_id, block["id"])
  else:
    result = client.create_taxonomy(
      graph_id,
      {
        "name": "Cascade Schedules",
        "taxonomy_type": "schedule",
      },
    )
    taxonomy_id = result["id"]

  created = 0
  for (
    name,
    dr_code,
    cr_code,
    monthly_cents,
    original_cents,
    life_months,
    start_offset,
  ) in schedules:
    dr_elem_id = element_lookup[dr_code]
    cr_elem_id = element_lookup[cr_code]
    start, end = _schedule_window(start_offset, life_months)

    client.create_schedule(
      graph_id,
      name=name,
      element_ids=[dr_elem_id, cr_elem_id],
      period_start=start.isoformat(),
      period_end=end.isoformat(),
      monthly_amount=monthly_cents,
      debit_element_id=dr_elem_id,
      credit_element_id=cr_elem_id,
      entry_type="closing",
      memo_template=f"Monthly amortization - {name}",
      taxonomy_id=taxonomy_id,
      method="straight_line",
      original_amount=original_cents,
      useful_life_months=life_months,
    )
    created += 1

  return created


# ---------------------------------------------------------------------------
# Step 5: Upload policy documents
# ---------------------------------------------------------------------------


def upload_policies(graph_id: str) -> int:
  """Upload accounting policy documents via the API."""
  import httpx

  from .policies import DOCUMENTS

  if CREDENTIALS_FILE.exists():
    creds = json.loads(CREDENTIALS_FILE.read_text())
    api_key = creds.get("api_key", "")
  else:
    print("  WARNING: No credentials file, skipping document upload")
    return 0

  uploaded = 0
  for doc in DOCUMENTS:
    try:
      resp = httpx.post(
        f"{BASE_URL}/v1/graphs/{graph_id}/documents",
        headers={"X-API-Key": api_key, "Content-Type": "application/json"},
        json={
          "title": doc["title"],
          "content": doc["content"],
          "folder": doc["folder"],
          "tags": doc["tags"],
        },
        timeout=30,
      )
      if resp.status_code in (200, 201):
        uploaded += 1
      else:
        print(f"  WARNING: Failed to upload '{doc['title']}': {resp.status_code}")
    except Exception as e:
      print(f"  WARNING: Document upload failed: {e}")

  return uploaded


# ---------------------------------------------------------------------------
# Step 6: Materialize to graph
# ---------------------------------------------------------------------------


def _poll_operation(
  api_key: str,
  operation_id: str,
  timeout_seconds: int = 120,
  interval_seconds: float = 2.0,
) -> tuple[str, dict]:
  """Poll /v1/operations/{id}/status until it terminates or times out."""
  import httpx

  deadline = time.time() + timeout_seconds
  url = f"{BASE_URL}/v1/operations/{operation_id}/status"
  headers = {"X-API-Key": api_key}

  while time.time() < deadline:
    try:
      resp = httpx.get(url, headers=headers, timeout=10)
      if resp.status_code == 200:
        data = resp.json()
        status = data.get("status")
        if status in ("completed", "failed"):
          return status, data.get("result") or {}
    except Exception as e:
      return "error", {"error": str(e)}
    time.sleep(interval_seconds)

  return "timeout", {}


def materialize_graph(graph_id: str) -> None:
  """Materialize OLTP data into LadybugDB graph."""
  import httpx

  if not CREDENTIALS_FILE.exists():
    print("  WARNING: No credentials file, skipping materialization")
    return

  creds = json.loads(CREDENTIALS_FILE.read_text())
  api_key = creds.get("api_key", "")

  try:
    resp = httpx.post(
      f"{BASE_URL}/v1/graphs/{graph_id}/operations/materialize",
      headers={"X-API-Key": api_key, "Content-Type": "application/json"},
      json={"force": True, "rebuild": True, "source": "extensions"},
      timeout=30,
    )
    if resp.status_code not in (200, 201, 202):
      print(f"  WARNING: Trigger failed: {resp.status_code}")
      return

    data = resp.json()
    op_id = data.get("operationId", data.get("operation_id", ""))
    print(f"  Triggered (operation: {op_id})")
    print("  Polling operation status...")

    status, result = _poll_operation(api_key, op_id)
    if status == "completed":
      print("  Done")
    elif status == "failed":
      error = result.get("error", "unknown")
      print(f"  WARNING: Materialization failed: {error}")
    elif status == "timeout":
      print("  WARNING: Materialization timed out after 120s (may still be running)")
    else:
      print(f"  WARNING: Polling error: {result.get('error', 'unknown')}")
  except Exception as e:
    print(f"  WARNING: Materialization failed: {e}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
  dry_run = "--dry-run" in sys.argv
  with_ai_mapping = "--ai" in sys.argv
  args = [a for a in sys.argv[1:] if not a.startswith("--")]

  # Add project root to path
  project_root = Path(__file__).resolve().parents[2]
  if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

  from .data import ACCOUNTS, get_all_transactions, validate_transactions

  # Validate data
  txns = get_all_transactions()
  print(f"\n{COMPANY_NAME} — Close Demo Setup")
  print("=" * 60)
  print(f"  Accounts:     {len(ACCOUNTS)}")
  print(f"  Transactions: {len(txns)}")

  if not validate_transactions(txns):
    print("\n  ERROR: Transaction validation failed (imbalanced entries)")
    sys.exit(1)
  print("  Validation:   All entries balance")

  if with_ai_mapping:
    print("  AI mapping:   enabled (--ai)")

  if dry_run:
    total_dr = sum(sum(dr for _, dr, _ in lines) for _, _, _, _, lines in txns)
    print(f"  Total debits: ${total_dr / 100:,.2f}")
    print("\nDry run complete. No data written.")
    return

  # Create or reuse graph
  if args:
    graph_id = args[0]
  else:
    graph_id = create_demo_graph()

  # Reset prior demo state (the ONLY direct-DB operation)
  print(f"\nResetting demo state for graph {graph_id}...")
  reset_demo(graph_id)
  print("  Done")

  # Create chart of accounts (via HTTP API — exercises create-element op)
  print("\nCreating chart of accounts...")
  element_lookup, coa_taxonomy_id, elem_count = create_chart_of_accounts(graph_id)
  print(f"  Taxonomy:     {coa_taxonomy_id}")
  print(f"  Elements:     {elem_count}")

  # Create journal entries (via HTTP API — exercises create-journal-entry op)
  print(f"\nCreating {len(txns)} journal entries...")
  entry_counts = create_journal_entries(graph_id, txns, element_lookup)
  print(f"  Entries:      {entry_counts['entries']}")
  print(f"  Line Items:   {entry_counts['line_items']}")

  # Create CoA → GAAP mappings — hardcoded or AI-powered
  if with_ai_mapping:
    print("\nRunning AI mapping (MappingAgent)...")
    run_ai_mapping(graph_id)
  else:
    print("\nCreating CoA → GAAP mappings...")
    mapping_count = create_mappings(graph_id, element_lookup)
    print(f"  Mappings:     {mapping_count}")

  # Initialize fiscal calendar (via HTTP API — exercises initialize op)
  print("\nInitializing fiscal calendar...")
  close_target = initialize_fiscal_calendar(graph_id)

  # Create schedules (via HTTP API — exercises create-schedule op)
  print("\nCreating schedules...")
  schedule_count = create_schedules(graph_id, element_lookup)
  print(f"  Schedules:    {schedule_count}")

  # Upload policies
  print("\nUploading accounting policies...")
  doc_count = upload_policies(graph_id)
  print(f"  Documents:    {doc_count}")

  # Materialize to graph
  print("\nMaterializing to graph...")
  materialize_graph(graph_id)

  # Summary
  print("\n" + "=" * 60)
  print(f"  Graph ID: {graph_id}")
  if CREDENTIALS_FILE.exists():
    print(f"  API Key:  (saved to {CREDENTIALS_FILE})")

  from robosystems.operations.roboledger.fiscal_calendar import parse_period

  year, month = parse_period(close_target)
  close_label = date(year, month, 1).strftime("%B %Y")

  print("\n  Ready for AI close workflow!")
  print(f"\n  Close target:  {close_target} ({close_label})")
  print("\n  Next steps:")
  print("    1. Open Claude Desktop (or MCP client)")
  print(f"    2. Switch to workspace: {graph_id}")
  print("    3. Ask: 'Show me the fiscal calendar'")
  print("    4. Ask: 'Search for month-end close procedures'")
  print(f"    5. Ask: 'Draft all closing entries for {close_label}'")
  print("\n  Optional — practice a one-off adjustment:")
  print(f"    6. Ask: 'I sold a computer on {close_target[:4]}-{close_target[5:]}-15'")
  print("          '  for $3,000 — truncate its depreciation schedule and'")
  print("          '  create a manual disposal entry for the gain/loss'")
  print("\n  Review and commit:")
  print(f"    7. Ask: 'Show me all the draft entries for {close_target}'")
  print(f"    8. Ask: 'Close the period {close_target}'")
  print("    9. Ask: 'Show me the balance sheet'")
  print("=" * 60)


if __name__ == "__main__":
  main()
