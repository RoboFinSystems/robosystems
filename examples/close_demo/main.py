#!/usr/bin/env python3
"""Cascade Advisory Group LLC — AI Month-End Close Demo

Sets up a complete demo environment with synthetic consulting company data,
CoA→GAAP mappings, depreciation/prepaid schedules, and accounting policy
documents. After running, use Claude Desktop or MCP tools to simulate
a month-end close.

Data is generated for a rolling 16-month window ending at the current month,
so the demo stays evergreen. OLTP load goes through the same `OLTPLoader`
path that the QuickBooks pipeline uses in production.

Usage:
    uv run python -m examples.close_demo.main              # Create new graph + load
    uv run python -m examples.close_demo.main <graph_id>   # Load into existing graph
    uv run python -m examples.close_demo.main --dry-run    # Validate data only

Requires: Docker stack running (just start)
"""

from __future__ import annotations

import json
import sys
import time
from datetime import UTC, datetime
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
# Step 2: Load OLTP data
# ---------------------------------------------------------------------------


def load_oltp_data(graph_id: str, txns: list) -> tuple[dict[str, str], dict[str, int]]:
  """Load accounts and transactions into the extensions OLTP database.

  Writes synthetic data to a DuckDB file (same shape as QB's dbt output)
  then calls OLTPLoader — the same load path QuickBooks uses in production.

  Idempotent: prior demo state (closing entries, fiscal calendar, schedule
  facts, associations) is wiped before reload. Each `just demo-close` lands
  in an identical "one period ready to close" state regardless of prior runs.
  """
  from robosystems.db.extensions import extensions_session
  from robosystems.models.extensions import Element, Entry, LineItem
  from robosystems.models.extensions.roboledger import Association, Fact, Structure
  from robosystems.models.extensions.roboledger.fiscal_calendar import (
    FiscalCalendar,
    FiscalCalendarEvent,
  )
  from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
  from robosystems.operations.extensions.loader import OLTPLoader

  from .data import ACCOUNTS
  from .oltp_writer import default_duckdb_path, write_demo_duckdb

  # Pre-cleanup: wipe all demo-generated state so the re-run lands in a
  # clean "fresh demo" state regardless of what was left behind. We do this
  # in one transaction in FK-safe order.
  with extensions_session(graph_id) as session:
    # 1. Close workflow artifacts (schedule-derived + manual drafts + posts)
    close_provenances = ("schedule_derived", "ai_generated", "manual_entry")
    stale_entry_ids = (
      session.query(Entry.id).filter(Entry.provenance.in_(close_provenances)).subquery()
    )
    session.query(LineItem).filter(LineItem.entry_id.in_(stale_entry_ids)).delete(
      synchronize_session=False
    )
    session.query(Entry).filter(Entry.provenance.in_(close_provenances)).delete(
      synchronize_session=False
    )

    # 2. Schedule structures → facts + associations (referencing demo elements)
    elem_ids_sub = (
      session.query(Element.id).filter(Element.external_source == SOURCE).subquery()
    )
    session.query(Association).filter(
      Association.from_element_id.in_(elem_ids_sub)
    ).delete(synchronize_session=False)
    schedule_struct_ids = (
      session.query(Structure.id)
      .filter(Structure.structure_type == "schedule")
      .subquery()
    )
    session.query(Fact).filter(Fact.structure_id.in_(schedule_struct_ids)).delete(
      synchronize_session=False
    )

    # 3. Fiscal calendar state — wiped so initialize_fiscal_calendar() starts fresh
    session.query(FiscalCalendarEvent).delete(synchronize_session=False)
    session.query(FiscalCalendar).delete(synchronize_session=False)
    session.query(FiscalPeriod).delete(synchronize_session=False)

    session.flush()

  duckdb_path = default_duckdb_path()
  write_demo_duckdb(duckdb_path, ACCOUNTS, txns, source=SOURCE)

  result = OLTPLoader().load(
    graph_id=graph_id,
    source=SOURCE,
    connection_id=CONNECTION_ID,
    duckdb_path=duckdb_path,
    created_by=CREATED_BY,
  )

  if result.errors:
    for err in result.errors:
      print(f"  WARNING: {err}")

  # Rebuild the code → oltp_id lookup for downstream steps (mappings, schedules)
  element_lookup: dict[str, str] = {}
  with extensions_session(graph_id) as session:
    for elem in session.query(Element).filter(Element.external_source == SOURCE).all():
      element_lookup[elem.code] = elem.id

  counts = {
    "elements": result.elements,
    "transactions": result.transactions,
    "entries": result.entries,
    "line_items": result.line_items,
  }
  return element_lookup, counts


# ---------------------------------------------------------------------------
# Step 3: Create CoA → GAAP mappings
# ---------------------------------------------------------------------------


def create_mappings(graph_id: str, element_lookup: dict[str, str]) -> int:
  """Create mapping associations between CoA elements and GAAP reporting concepts."""
  from robosystems.db.extensions import extensions_session
  from robosystems.models.extensions.roboledger import Association, Structure
  from robosystems.utils.ulid import generate_prefixed_ulid

  from .mappings import MAPPINGS

  now = datetime.now(UTC)

  with extensions_session(graph_id) as session:
    mapping_struct = (
      session.query(Structure).filter(Structure.structure_type == "coa_mapping").first()
    )
    if not mapping_struct:
      print("  ERROR: No mapping structure found")
      return 0

    created = 0
    for coa_code, gaap_id in MAPPINGS:
      coa_id = element_lookup.get(coa_code)
      if not coa_id:
        print(f"  WARNING: CoA code {coa_code} not in element_lookup")
        continue

      session.add(
        Association(
          id=generate_prefixed_ulid("assoc"),
          structure_id=mapping_struct.id,
          from_element_id=coa_id,
          to_element_id=gaap_id,
          association_type="mapping",
          order_value=0.0,
          created_at=now,
          updated_at=now,
        )
      )
      created += 1

    session.flush()

  return created


# ---------------------------------------------------------------------------
# Step 4a: Initialize fiscal calendar
# ---------------------------------------------------------------------------


def initialize_fiscal_calendar(graph_id: str) -> str:
  """Initialize the fiscal calendar with closed_through = month before last.

  This sets up the demo so that the user has exactly one period ready to
  close (the most recent completed month). Assumes `load_oltp_data()` has
  already wiped any prior fiscal state so this is always a fresh init.

  Returns the `close_target` period string for use in the next-steps output.
  """
  from robosystems.db.extensions import extensions_session
  from robosystems.operations.fiscal_calendar import (
    FiscalCalendarService,
    add_months,
    current_month_period,
    previous_period,
  )

  svc = FiscalCalendarService()

  # closed_through = month before last completed month
  # → close_target = last completed month (one period ready to close)
  last_completed = previous_period(current_month_period())
  closed_through = previous_period(last_completed)

  with extensions_session(graph_id) as session:
    # Fresh initialization — walls off prior data as historical.
    # load_oltp_data() wipes any prior fiscal state so this never collides.
    calendar = svc.initialize(
      session,
      graph_id,
      closed_through=closed_through,
      actor_id=CREATED_BY,
      actor_type="user",
      note="close_demo initialization",
    )

    # Seed FiscalPeriod rows spanning the demo's data window
    from .data import DEMO_MONTHS, get_demo_start_date

    demo_start = get_demo_start_date()
    demo_start_period = f"{demo_start.year:04d}-{demo_start.month:02d}"
    demo_end_period = add_months(demo_start_period, DEMO_MONTHS - 1)

    periods_created = svc.ensure_fiscal_periods(
      session,
      graph_id,
      start_period=demo_start_period,
      end_period=demo_end_period,
      closed_through=closed_through,
    )

    session.commit()

    print(f"  closed_through: {closed_through}")
    print(f"  close_target:   {calendar.close_target_period}")
    print(f"  periods seeded: {periods_created}")
    return calendar.close_target_period or last_completed


# ---------------------------------------------------------------------------
# Step 4b: Create schedules
# ---------------------------------------------------------------------------


def create_schedules(graph_id: str, element_lookup: dict[str, str]) -> int:
  """Create depreciation and prepaid amortization schedules via ScheduleService.

  Routes through the service so schedule facts inherit the fiscal calendar's
  `closed_through` boundary — facts in periods ≤ closed_through are flagged
  `historical` (skipped by the close workflow); later facts are `in_scope`.

  Prior-run schedules in the "Cascade Schedules" taxonomy are cleaned up
  before re-seeding so the demo is idempotent across runs.
  """
  from datetime import date, timedelta

  from robosystems.db.extensions import extensions_session
  from robosystems.models.extensions.roboledger import (
    Association,
    Fact,
    Structure,
    Taxonomy,
  )
  from robosystems.operations.fiscal_calendar import (
    FiscalCalendarService,
    parse_period,
  )
  from robosystems.operations.schedules import ScheduleService
  from robosystems.operations.schedules.service import (
    EntryTemplate,
    ScheduleMetadata,
  )
  from robosystems.utils.ulid import generate_prefixed_ulid

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
    # Depreciation — staggered purchase dates
    ("Computer Equipment Depreciation", "7000", "1350", 13_333, 480_000, 36, 0),
    ("Office Furniture Depreciation", "7000", "1350", 2_500, 150_000, 60, 2),
    # Prepaids — staggered renewal dates throughout the year
    ("Business Insurance", "6400", "1200", 10_000, 120_000, 12, 2),
    ("Business Insurance (Year 2 Renewal)", "6400", "1200", 10_000, 120_000, 12, 14),
    ("Software Subscription", "6100", "1210", 2_500, 30_000, 12, 5),
    ("Cloud Hosting (AWS Savings Plan)", "6200", "1220", 5_000, 60_000, 12, 8),
  ]

  schedule_svc = ScheduleService()
  calendar_svc = FiscalCalendarService()
  created = 0

  with extensions_session(graph_id) as session:
    # Pull closed_through from the fiscal calendar (set during Step 4a).
    # Facts in periods ≤ closed_through become 'historical' automatically.
    calendar = calendar_svc.get(session, graph_id)
    closed_through_date: date | None = None
    if calendar and calendar.closed_through_period:
      year, month = parse_period(calendar.closed_through_period)
      # End-of-month so that any fact with period_end ≤ this is historical
      from calendar import monthrange

      closed_through_date = date(year, month, monthrange(year, month)[1])

    # Find or create schedule taxonomy
    schedule_tax = (
      session.query(Taxonomy)
      .filter(
        Taxonomy.taxonomy_type == "schedule", Taxonomy.name == "Cascade Schedules"
      )
      .first()
    )
    if not schedule_tax:
      now = datetime.now(UTC)
      schedule_tax = Taxonomy(
        id=generate_prefixed_ulid("tax"),
        name="Cascade Schedules",
        taxonomy_type="schedule",
        is_active=True,
        created_by=CREATED_BY,
        created_at=now,
        updated_at=now,
      )
      session.add(schedule_tax)
      session.flush()

    # Clean up any schedules from a prior demo run so re-running is idempotent
    existing = (
      session.query(Structure).filter(Structure.taxonomy_id == schedule_tax.id).all()
    )
    for struct in existing:
      session.query(Fact).filter(Fact.structure_id == struct.id).delete(
        synchronize_session=False
      )
      session.query(Association).filter(Association.structure_id == struct.id).delete(
        synchronize_session=False
      )
      session.delete(struct)
    if existing:
      session.flush()

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

      schedule_svc.create_schedule(
        session,
        name=name,
        taxonomy_id=schedule_tax.id,
        element_ids=[dr_elem_id, cr_elem_id],
        period_start=start,
        period_end=end,
        monthly_amount=monthly_cents,
        entry_template=EntryTemplate(
          debit_element_id=dr_elem_id,
          credit_element_id=cr_elem_id,
          entry_type="closing",
          memo_template="Monthly amortization - {structure_name}",
        ),
        schedule_metadata=ScheduleMetadata(
          method="straight_line",
          original_amount=original_cents,
          useful_life_months=life_months,
        ),
        created_by=CREATED_BY,
        closed_through=closed_through_date,
      )
      created += 1

    session.commit()

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
  """Poll /v1/operations/{id}/status until it terminates or times out.

  Returns (status, result_dict). status is one of: "completed", "failed",
  "timeout", or "error".
  """
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
  """Materialize OLTP data into LadybugDB graph.

  Uses source=extensions to trigger the OLTP → DuckDB → LadybugDB pipeline
  (not the S3-based staged pipeline).
  """
  import httpx

  if not CREDENTIALS_FILE.exists():
    print("  WARNING: No credentials file, skipping materialization")
    return

  creds = json.loads(CREDENTIALS_FILE.read_text())
  api_key = creds.get("api_key", "")

  try:
    resp = httpx.post(
      f"{BASE_URL}/v1/graphs/{graph_id}/materialize",
      headers={"X-API-Key": api_key, "Content-Type": "application/json"},
      json={"force": True, "rebuild": True, "source": "extensions"},
      timeout=30,
    )
    if resp.status_code not in (200, 201, 202):
      print(f"  WARNING: Trigger failed: {resp.status_code}")
      return

    op_id = resp.json().get("operation_id", "")
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

  if dry_run:
    # Print summary
    total_dr = sum(sum(dr for _, dr, _ in lines) for _, _, _, _, lines in txns)
    print(f"  Total debits: ${total_dr / 100:,.2f}")
    print("\nDry run complete. No data written.")
    return

  # Create or reuse graph
  if args:
    graph_id = args[0]
  else:
    graph_id = create_demo_graph()

  # Load OLTP data
  print(f"\nLoading OLTP data for graph {graph_id}...")
  element_lookup, counts = load_oltp_data(graph_id, txns)
  print(f"  Elements:     {counts['elements']}")
  print(f"  Transactions: {counts['transactions']}")
  print(f"  Entries:      {counts['entries']}")
  print(f"  Line Items:   {counts['line_items']}")

  # Create mappings
  print("\nCreating CoA → GAAP mappings...")
  mapping_count = create_mappings(graph_id, element_lookup)
  print(f"  Mappings:     {mapping_count}")

  # Initialize fiscal calendar (must happen before create_schedules so that
  # schedule facts can be scoped against closed_through)
  print("\nInitializing fiscal calendar...")
  close_target = initialize_fiscal_calendar(graph_id)

  # Create schedules (scoped against the calendar's closed_through)
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
    # Don't print the API key — even a prefix is sensitive. Point the user
    # at the credentials file so they can pick it up locally if needed.
    print(f"  API Key:  (saved to {CREDENTIALS_FILE})")
  # Human-readable label for the close target period
  from datetime import date

  from robosystems.operations.fiscal_calendar import parse_period

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
