#!/usr/bin/env python3
"""Cascade Advisory Group LLC — RoboLedger End-to-End Demo

Sets up a complete demo environment with synthetic consulting company data,
CoA→GAAP mappings, depreciation/prepaid schedules, accounting policy
documents, and a filed FY 2025 annual report. After running, use Claude
Desktop or MCP tools to simulate the close workflow on the queued period.

Data is generated for a rolling 16-month window ending at the current month,
so the demo stays evergreen.

**Transport rule**: all content goes through the HTTP API via `LedgerClient`
— the same surface frontend UI and MCP tools use. This emulates "data
arriving from outside the system" the way a real customer's integration
would. The ONLY exception is `_reset.py`, which uses direct DB access for
demo cleanup between runs — that path is intentionally NOT a product
operation.

Usage:
    uv run python -m examples.roboledger_demo.main                        # Create new graph + load
    uv run python -m examples.roboledger_demo.main <graph_id>             # Load into existing graph
    uv run python -m examples.roboledger_demo.main --dry-run              # Validate data only
    uv run python -m examples.roboledger_demo.main --ai                    # Use MappingAgent instead of hardcoded mappings (requires Bedrock)

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


def _client_config() -> dict[str, str]:
  """Build the standard SDK client config from saved credentials."""
  if not CREDENTIALS_FILE.exists():
    print("  ERROR: No credentials file. Run `just demo-user` first.")
    sys.exit(1)
  creds = json.loads(CREDENTIALS_FILE.read_text())
  return {"base_url": BASE_URL, "token": creds.get("api_key", "")}


def _get_ledger_client():
  """Construct a LedgerClient from saved credentials."""
  from robosystems_client.clients.ledger_client import LedgerClient

  return LedgerClient(_client_config())


def _get_document_client():
  """Construct a DocumentClient from saved credentials."""
  from robosystems_client.clients.document_client import DocumentClient

  return DocumentClient(_client_config())


def _get_graph_client():
  """Construct a GraphClient from saved credentials."""
  from robosystems_client.clients.graph_client import GraphClient

  return GraphClient(_client_config())


def _get_operation_client():
  """Construct an OperationClient from saved credentials."""
  from robosystems_client.clients.operation_client import OperationClient

  return OperationClient(_client_config())


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

  Uses the Taxonomy Block envelope — one POST creates the taxonomy, the
  mapping structure, and every CoA element atomically.
  """
  from .data import ACCOUNTS

  client = _get_ledger_client()

  elements_payload = [
    {
      "qname": f"coa:{code}",
      "name": name,
      "trait": classification,
      "balance_type": balance_type,
      "description": description,
      "code": code,
      "sub_classification": sub_class,
    }
    for code, name, classification, sub_class, balance_type, description in ACCOUNTS
  ]

  envelope = client.create_taxonomy_block(
    graph_id,
    {
      "name": "Native Chart of Accounts",
      "taxonomy_type": "chart_of_accounts",
      "description": "Tenant chart of accounts for Cascade Advisory Group LLC.",
      "elements": elements_payload,
      "structures": [
        {
          "name": "CoA to US GAAP Mapping",
          "description": "Maps Chart of Accounts to US GAAP reporting concepts",
          "structure_type": "coa_mapping",
        },
      ],
      "associations": [],
      "rules": [],
    },
  )
  coa_taxonomy_id = envelope.id

  element_lookup: dict[str, str] = {
    (e.qname or "").split(":", 1)[-1]: e.id
    for e in (envelope.elements or [])
    if e.qname
  }

  return element_lookup, coa_taxonomy_id, len(ACCOUNTS)


# ---------------------------------------------------------------------------
# Step 2b: Create counterparty agents (via HTTP API)
# ---------------------------------------------------------------------------


def create_agents(graph_id: str) -> dict[str, str]:
  """Create counterparty agents (customers, vendors, employees).

  Each agent is posted via `create-agent` — the same surface a real
  customer's integration uses. Returns a name → Agent.id lookup that
  later steps use to populate `event_block.agent_id` on invoice / bill /
  payment events, exercising the REA counterparty linkage that QB
  pipelines surface by default.
  """
  from .agents import AGENTS

  client = _get_ledger_client()
  lookup: dict[str, str] = {}
  skipped = 0

  for body in AGENTS:
    # No idempotency_key: reset_demo wipes the agents table per-run, but
    # the Valkey idempotency cache outlives the DB reset and would replay
    # a prior run's response (returning stale agent_ids that point at
    # since-deleted rows). The DB-level uniqueness on
    # (source, external_id) is sufficient to dedupe inside a single run.
    try:
      resp = client.create_agent(graph_id, body)
    except Exception as e:
      print(f"  WARNING: create-agent failed for {body['name']} — {e}")
      skipped += 1
      continue
    lookup[body["name"]] = resp.id

  if skipped:
    print(f"  WARNING: Skipped {skipped} agents")

  return lookup


# ---------------------------------------------------------------------------
# Step 2c: Create business events (via HTTP API)
# ---------------------------------------------------------------------------

# Account codes used by the typed-event branching logic — defined as
# constants so the cash / AP semantics are visible at the call site
# rather than buried inside string comparisons.
_CASH_CODE = "1000"
_AP_CODE = "2000"


def _build_line_items(
  lines: list[tuple[str, int, int]],
  element_lookup: dict[str, str],
) -> list[dict] | None:
  """Resolve `(code, debit, credit)` tuples to the event metadata shape.

  Returns None if any element can't be resolved (caller decides whether
  to skip the event or fall back to a generic journal entry).
  """
  out: list[dict] = []
  for elem_code, debit, credit in lines:
    elem_id = element_lookup.get(elem_code)
    if not elem_id:
      return None
    out.append(
      {"element_id": elem_id, "debit_amount": debit, "credit_amount": credit}
    )
  return out


def _can_split_bill_payment(lines: list[tuple[str, int, int]]) -> bool:
  """A bill_payment can be split into bill_received + bill_paid when its
  shape is a vendor purchase: expense/asset DR + cash CR. Liability-side
  debits (e.g. payroll tax deposits crediting cash against an existing
  payable) are settlements, not vendor bills — keep those as plain
  journal entries so we don't misroute the AP intermediate."""
  has_cash_credit = any(
    code == _CASH_CODE and credit > 0 for code, _, credit in lines
  )
  has_liability_debit = any(
    code.startswith("2") and debit > 0 for code, debit, _ in lines
  )
  return has_cash_credit and not has_liability_debit


def create_business_events(
  graph_id: str,
  txns: list,
  element_lookup: dict[str, str],
  agent_lookup: dict[str, str],
) -> dict[str, int]:
  """Create typed business events from synthetic transactions.

  Branches per txn_type so the demo exercises the rich event vocabulary
  rather than collapsing everything to `journal_entry_recorded`:

  - `invoice` → `invoice_issued` (event_category=sales) with `agent_id`
    set to the customer; event_id indexed for the matching payment.
  - `payment` → `payment_received` (event_category=sales) with
    `agent_id` and `discharges_event_id` linking back to the invoice
    (REA duality: payment discharges the AR obligation).
  - `bill_payment` for a known vendor with expense/asset debits → splits
    into a `bill_received` (DR expense/asset, CR AP) followed by a
    `bill_paid` (DR AP, CR cash) with `discharges_event_id` linking back
    to the bill. The AP balance carries between the two events,
    mirroring real-world AR/AP timing.
  - `journal_entry`, `bill_payment` against unknown counterparties, and
    bill_payments structurally identified as liability settlements
    (e.g. payroll tax deposits) → `journal_entry_recorded` with
    `event_category=adjustment` and optional `agent_id`.

  All event handlers dispatch through the journal_entry_recorded Python
  handler (per `python_handlers/registry.py`) — the event_type drives
  inbox classification and the duality chain, not GL posting shape.
  """
  client = _get_ledger_client()

  # Index of invoice events keyed by (customer_name, amount) so the
  # subsequent payment can populate `discharges_event_id`. LIFO match
  # via dict deletion handles the case where a single customer has
  # multiple identical-amount invoices in flight.
  invoice_index: dict[tuple[str, int], str] = {}

  counts: dict[str, int] = {
    "invoice_issued": 0,
    "payment_received": 0,
    "bill_received": 0,
    "bill_paid": 0,
    "journal_entry_recorded": 0,
  }
  line_item_count = 0
  skipped = 0

  cash_id = element_lookup.get(_CASH_CODE)
  ap_id = element_lookup.get(_AP_CODE)

  for txn_date, txn_type, description, agent_name, lines in txns:
    li_list = _build_line_items(lines, element_lookup)
    if li_list is None or len(li_list) < 2:
      skipped += 1
      continue

    agent_id = agent_lookup.get(agent_name) if agent_name else None
    memo = description or f"{txn_type} on {txn_date}"
    total_amount = sum(dr for _, dr, _ in lines)
    occurred_at = f"{txn_date.isoformat()}T00:00:00+00:00"

    base_metadata = {
      "posting_date": txn_date.isoformat(),
      "memo": memo,
      "line_items": li_list,
      "type": "standard",
      "status": "posted",
    }
    base_body: dict = {
      "event_class": "economic",
      "source": "manual",
      "occurred_at": occurred_at,
      "apply_handlers": True,
      "amount": total_amount,
      "currency": "USD",
      "description": memo,
    }

    try:
      if txn_type == "invoice":
        body = {
          **base_body,
          "event_type": "invoice_issued",
          "event_category": "sales",
          "agent_id": agent_id,
          "metadata": base_metadata,
        }
        resp = client.create_event_block(graph_id, body)
        invoice_index[(agent_name or "", total_amount)] = resp.id
        counts["invoice_issued"] += 1
        line_item_count += len(li_list)

      elif txn_type == "payment":
        invoice_evt_id = invoice_index.pop((agent_name or "", total_amount), None)
        body = {
          **base_body,
          "event_type": "payment_received",
          "event_category": "sales",
          "agent_id": agent_id,
          "metadata": base_metadata,
        }
        if invoice_evt_id:
          body["discharges_event_id"] = invoice_evt_id
        client.create_event_block(graph_id, body)
        counts["payment_received"] += 1
        line_item_count += len(li_list)

      elif (
        txn_type == "bill_payment"
        and agent_id is not None
        and ap_id is not None
        and cash_id is not None
        and _can_split_bill_payment(lines)
      ):
        # bill_received: rewrite the cash credit to an AP credit
        br_lines = [
          {"element_id": ap_id, "debit_amount": 0, "credit_amount": credit}
          if li["element_id"] == cash_id and li["credit_amount"] > 0
          else li
          for li, (_, _, credit) in zip(li_list, lines, strict=True)
        ]
        br_body = {
          **base_body,
          "event_type": "bill_received",
          "event_category": "purchase",
          "agent_id": agent_id,
          "metadata": {**base_metadata, "line_items": br_lines, "memo": f"Bill: {memo}"},
        }
        br_resp = client.create_event_block(graph_id, br_body)
        counts["bill_received"] += 1
        line_item_count += len(br_lines)

        # bill_paid: DR AP, CR cash — straight discharge
        bp_lines = [
          {"element_id": ap_id, "debit_amount": total_amount, "credit_amount": 0},
          {"element_id": cash_id, "debit_amount": 0, "credit_amount": total_amount},
        ]
        bp_body = {
          **base_body,
          "event_type": "bill_paid",
          "event_category": "purchase",
          "agent_id": agent_id,
          "discharges_event_id": br_resp.id,
          "metadata": {**base_metadata, "line_items": bp_lines, "memo": f"Payment of: {memo}"},
        }
        client.create_event_block(graph_id, bp_body)
        counts["bill_paid"] += 1
        line_item_count += len(bp_lines)

      else:
        # journal_entry, unknown-vendor bill_payment, or liability settlement
        body = {
          **base_body,
          "event_type": "journal_entry_recorded",
          "event_category": "adjustment",
          "metadata": base_metadata,
        }
        if agent_id:
          body["agent_id"] = agent_id
        client.create_event_block(graph_id, body)
        counts["journal_entry_recorded"] += 1
        line_item_count += len(li_list)

    except Exception as e:
      print(f"  WARNING: create-event-block failed for {txn_date} {txn_type} — {e}")
      skipped += 1
      continue

  if skipped:
    print(f"  WARNING: Skipped {skipped} events (missing elements or errors)")

  return {
    "entries": sum(counts.values()),
    "line_items": line_item_count,
    "events_by_type": counts,
  }


# ---------------------------------------------------------------------------
# Step 3a: Create CoA → GAAP mappings — hardcoded path (no Bedrock needed)
# ---------------------------------------------------------------------------


def create_mappings(graph_id: str, element_lookup: dict[str, str]) -> int:
  """Create mapping associations between CoA elements and rs-gaap reporting concepts.

  CoA → rs-gaap is the canonical mapping target (§3.2 Reporting Style).
  The Default Style's Networks resolve rs-gaap concepts up through the
  calc-linkbase to FAC subtotals at render time; FAC subtotals are
  derived, not mapped.

  Uses `LedgerClient.create_associations()` — the bulk HTTP API — to
  exercise the same path the frontend UI and MCP tools use.
  """
  from .mappings import MAPPINGS

  client = _get_ledger_client()

  # Find the coa_mapping structure (created during create_chart_of_accounts step)
  structures = client.list_structures(graph_id, structure_type="coa_mapping")
  if not structures:
    print("  ERROR: No mapping structure found")
    return 0
  mapping_id = structures[0]["id"]

  # Resolve rs-gaap qnames → element IDs via the library in the entity graph.
  # rs-gaap has ~2000 elements; list_elements caps at 1000 per page, so paginate.
  rs_gaap_by_qname: dict[str, str] = {}
  offset = 0
  while True:
    page = client.list_elements(graph_id, source="rs-gaap", limit=1000, offset=offset)
    items = (page or {}).get("elements", [])
    if not items:
      break
    for e in items:
      if e.get("qname"):
        rs_gaap_by_qname[e["qname"]] = e["id"]
    if len(items) < 1000:
      break
    offset += 1000

  # Walk MAPPINGS and post each association one-by-one through
  # create-mapping-association which takes a single pair. The demo's
  # mapping set is ~27 rows so per-call latency is fine.
  created = 0
  for coa_code, rs_gaap_qname in MAPPINGS:
    coa_id = element_lookup.get(coa_code)
    if not coa_id:
      print(f"  WARNING: CoA code {coa_code} not in element_lookup")
      continue
    rs_gaap_id = rs_gaap_by_qname.get(rs_gaap_qname)
    if not rs_gaap_id:
      print(f"  WARNING: rs-gaap qname {rs_gaap_qname} not found in library")
      continue
    client.create_mapping_association(
      graph_id,
      mapping_id=mapping_id,
      from_element_id=coa_id,
      to_element_id=rs_gaap_id,
    )
    created += 1

  return created


# ---------------------------------------------------------------------------
# Step 3b: AI mapping path — requires Bedrock (optional)
# ---------------------------------------------------------------------------


def run_ai_mapping(graph_id: str) -> None:
  """Trigger the MappingAgent via the auto-map-elements operation.

  Requires Bedrock to be configured (BEDROCK_REGION + IAM role with
  bedrock:InvokeModel). Skipped when --ai is not passed.

  Finds the coa_mapping structure, dispatches the async agent operation
  via ``LedgerClient.auto_map_elements()``, then waits via
  ``OperationClient.monitor_operation()`` (SSE with polling fallback).
  """
  from robosystems_client.clients.operation_client import (
    MonitorOptions,
    OperationStatus,
  )

  ledger = _get_ledger_client()
  structures = ledger.list_structures(graph_id, structure_type="coa_mapping")
  if not structures:
    print("  ERROR: No coa_mapping structure found — was the CoA taxonomy created?")
    return
  mapping_id = structures[0]["id"]
  print(f"  Mapping structure: {mapping_id}")

  try:
    ack = ledger.auto_map_elements(graph_id, mapping_id)
  except Exception as e:
    print(f"  ERROR: Failed to trigger auto-map-elements: {e}")
    return

  op_id = ack.get("operation_id", "")
  print(f"  Dispatched (operation: {op_id})")
  print("  Monitoring… (mapping 27 accounts, may take 1–3 min)")

  ops = _get_operation_client()
  try:
    result = ops.monitor_operation(op_id, MonitorOptions(timeout=300))
  except TimeoutError:
    print("  WARNING: AI mapping timed out after 5 min (may still be running)")
    return

  payload = result.result if isinstance(result.result, dict) else {}

  if result.status == OperationStatus.COMPLETED:
    print(
      f"  Done — mapped: {payload.get('mapped', '?')}, "
      f"flagged: {payload.get('flagged', '?')}, "
      f"skipped: {payload.get('skipped', '?')}"
    )
    print(f"  Coverage: {payload.get('coverage_percent', '?')}%")
  elif result.status == OperationStatus.FAILED:
    print(f"  WARNING: AI mapping failed: {result.error or 'unknown'}")
    print("  Falling back to hardcoded mappings...")
  else:
    print(f"  WARNING: Unexpected operation status: {result.status}")


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
    note="roboledger_demo initialization",
  )

  fc = result.fiscal_calendar
  close_target = fc.close_target or last_completed

  print(f"  closed_through: {closed_through}")
  print(f"  close_target:   {close_target}")
  print(f"  periods seeded: {result.periods_created}")
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
    result = client.create_taxonomy_block(
      graph_id,
      {
        "name": "Cascade Schedules",
        "taxonomy_type": "schedule",
        "description": "Container for the demo's recurring closing schedules.",
      },
    )
    taxonomy_id = result.id

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
  """Upload accounting policy documents via ``DocumentClient.upload()``."""
  from .policies import DOCUMENTS

  client = _get_document_client()
  uploaded = 0
  for doc in DOCUMENTS:
    try:
      client.upload(
        graph_id,
        title=doc["title"],
        content=doc["content"],
        folder=doc["folder"],
        tags=doc["tags"],
      )
      uploaded += 1
    except Exception as e:
      print(f"  WARNING: Failed to upload '{doc['title']}': {e}")

  return uploaded


# ---------------------------------------------------------------------------
# Step 6: Materialize to graph
# ---------------------------------------------------------------------------


def materialize_graph(graph_id: str) -> None:
  """Materialize OLTP data into LadybugDB graph.

  Routes through ``GraphClient.materialize()`` — the SDK handles SSE
  monitoring (with polling fallback) and surfaces a typed result.
  """
  from robosystems_client.clients.graph_client import MaterializationOptions

  client = _get_graph_client()
  try:
    result = client.materialize(
      graph_id,
      MaterializationOptions(
        force=True,
        rebuild=True,
        on_progress=lambda msg: print(f"  {msg}"),
      ),
    )
  except Exception as e:
    print(f"  WARNING: Materialization failed: {e}")
    return

  if result.success:
    print("  Done")
  else:
    print(f"  WARNING: Materialization failed: {result.error or result.message}")


# ---------------------------------------------------------------------------
# Step 7: Generate + file FY 2025 annual report (Plan C capstone)
# ---------------------------------------------------------------------------


def generate_fy2025_report(graph_id: str) -> str | None:
  """Create a published, filed FY 2025 annual report.

  Exercises the Report Block lifecycle end-to-end: ``create-report`` →
  ``get-report-package`` → ``file-report``. The result is a frozen,
  filed snapshot of the prior year visible at ``/reports/{id}`` in the
  package viewer, alongside the queued-for-close current period.

  Returns the report_id (or None on failure) so the caller can print
  the viewer URL.
  """
  client = _get_ledger_client()

  # Find the coa_mapping structure created during the taxonomy seed
  structures = client.list_structures(graph_id, structure_type="coa_mapping")
  if not structures:
    print("  ERROR: No coa_mapping structure — was the CoA created?")
    return None
  mapping_id = structures[0]["id"]

  # Find the FAC presentation taxonomy. This is where the proper
  # income_statement / cash_flow_statement structures live (with
  # associations to FAC elements). The bare ``fac v1`` reporting_standard
  # only has a default placeholder structure with no associations, which
  # is why the report would otherwise have zero rendered statements.
  taxonomies = client.list_taxonomies(graph_id, taxonomy_type="mapping")
  fac_pres = next(
    (t for t in taxonomies if t.get("name", "").startswith("fac-presentation")),
    None,
  )
  if not fac_pres:
    print("  ERROR: No fac-presentation taxonomy seeded on this graph")
    return None
  taxonomy_id = fac_pres["id"]

  report = client.create_report(
    graph_id,
    name="FY 2025 Annual Report",
    mapping_id=mapping_id,
    taxonomy_id=taxonomy_id,
    period_start=date(2025, 1, 1),
    period_end=date(2025, 12, 31),
    period_type="annual",
    comparative=False,
  )
  report_id = report.id
  print(f"  Generated:    {report_id}")

  # Pull the package to confirm it rehydrates
  package = client.get_report_package(graph_id, report_id)
  if package:
    items = package.get("items", []) or []
    # `block_type` lives nested at item.block.block_type — the rehydrated
    # InformationBlockEnvelope. Earlier this read item.block_type directly
    # and silently rendered "?" for every item.
    block_names = [
      (i.get("block") or {}).get("name") or (i.get("block") or {}).get("block_type") or "?"
      for i in items
    ]
    print(f"  Package:      {len(items)} block(s) — {', '.join(block_names)}")

  # File it — flips filing_status draft → filed
  try:
    client.file_report(graph_id, report_id)
    print("  Filed:        ✓")
  except Exception as e:
    print(f"  WARNING: file_report failed: {e}")

  return report_id


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
  print(f"\n{COMPANY_NAME} — RoboLedger Demo Setup")
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

  # Create counterparty agents (via HTTP API — exercises create-agent op)
  print("\nCreating counterparty agents...")
  agent_lookup = create_agents(graph_id)
  print(f"  Agents:       {len(agent_lookup)}")

  # Create business events (via HTTP API — exercises typed event vocabulary:
  # invoice_issued, payment_received, bill_received, bill_paid,
  # journal_entry_recorded — with REA duality chains)
  print(f"\nCreating {len(txns)} business events...")
  entry_counts = create_business_events(graph_id, txns, element_lookup, agent_lookup)
  print(f"  Events:       {entry_counts['entries']}")
  print(f"  Line Items:   {entry_counts['line_items']}")
  for event_type, count in entry_counts["events_by_type"].items():
    if count > 0:
      print(f"    {event_type}: {count}")

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

  # Validate Information Block system
  print("\nValidating Information Blocks...")
  api_key = (
    json.loads(CREDENTIALS_FILE.read_text()).get("api_key", "")
    if CREDENTIALS_FILE.exists()
    else ""
  )
  if api_key:
    from .validate import run_validation

    run_validation(graph_id, api_key)
  else:
    print("  (skipped — no credentials)")

  # Upload policies
  print("\nUploading accounting policies...")
  doc_count = upload_policies(graph_id)
  print(f"  Documents:    {doc_count}")

  # Materialize to graph
  print("\nMaterializing to graph...")
  materialize_graph(graph_id)

  # Generate + file FY 2025 annual report (Plan C capstone)
  print("\nGenerating FY 2025 annual report...")
  fy2025_report_id = generate_fy2025_report(graph_id)

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
  if fy2025_report_id:
    print(f"  FY 2025:       filed ({fy2025_report_id})")
    print(f"  Viewer URL:    /reports/{fy2025_report_id}?graph={graph_id}")
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
