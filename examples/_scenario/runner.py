#!/usr/bin/env python3
"""Shared end-to-end runner for the showcase scenario demos.

Given an :class:`~examples._scenario.drivers.Scenario` plus its counterparty
agents, CoA→rs-gaap mappings, and policy documents, ``run_demo`` provisions a
RoboLedger graph and loads the whole company: chart of accounts, agents, the
typed REA event stream, mappings, fiscal calendar, schedules, policies,
materialization, and a filed annual report (with JSON-LD + XBRL bundles).

Each episode (e.g. ``coffee_roaster_demo``, ``saas_startup_demo``) is a thin
``main.py`` that calls ``run_demo`` — the company *is* the scenario, so a new
episode is a new ``data.py``/``agents.py``/``mappings.py``/``policies.py``, not
a fork of this runner.

**Transport rule**: all content goes through the HTTP API via `LedgerClient`
— the same surface frontend UI and MCP tools use. This emulates "data arriving
from outside the system" the way a real customer's integration would. The ONLY
exception is ``reset.py``, which uses direct DB access for demo cleanup between
runs — that path is intentionally NOT a product operation.

That rule is what lets an episode load into a deployed environment as well as
the local stack: set ``DEMO_API_URL`` and the whole run is ordinary API traffic
against an account that already holds a provisioned graph. Credentials and the
demo-slot map are kept in a per-target file so a remote run cannot capture the
local slots, and the reset is skipped entirely off-local (see ``reset_demo``).

Requires: the local Docker stack (just start), or ``DEMO_API_URL`` pointing at
a deployed API.
"""

from __future__ import annotations

import json
import os
import sys
import time
from collections import Counter
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlparse

from .drivers import Scenario

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE = "native"

# The API the demo loads into. Defaults to the local stack; set DEMO_API_URL to
# aim an episode at a deployed environment (for demo tenants a prospect will
# actually be shown). Everything the runner does is a public API call, so the
# only thing that changes with the target is the base URL — except the reset,
# which is not a product operation. See ``reset_demo``.
BASE_URL = os.environ.get("DEMO_API_URL", "http://localhost:8000").rstrip("/")

_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "0.0.0.0", "host.docker.internal"})


def _is_local_target() -> bool:
  return (urlparse(BASE_URL).hostname or "") in _LOCAL_HOSTS


def _credentials_file() -> Path:
  """Credentials + demo-slot map, kept separate per target.

  The slot map is keyed on ``scenario.slug`` and ``create_demo_graph`` returns
  early on a hit, so sharing one file across targets would make a later local
  run silently reuse a remote graph id — and point the reset at it.
  """
  if _is_local_target():
    return Path(".local/config.json")
  host = (urlparse(BASE_URL).hostname or "remote").replace(".", "-")
  return Path(f".local/config.{host}.json")


CREDENTIALS_FILE = _credentials_file()


# ---------------------------------------------------------------------------
# LedgerClient helper
# ---------------------------------------------------------------------------


def _client_config() -> dict[str, str]:
  """Build the standard SDK client config from saved credentials."""
  if not CREDENTIALS_FILE.exists():
    if _is_local_target():
      print("  ERROR: No credentials file. Run `just demo-user` first.")
    else:
      print(f"  ERROR: No credentials for {BASE_URL}.")
      print(f"  Write the account's API key to {CREDENTIALS_FILE} as")
      print('  {"api_key": "..."} — demo-user only registers against local.')
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


def create_demo_graph(scenario: Scenario, entity_type: str) -> str:
  """Create user + roboledger graph via the API, seeded from the scenario.

  ``entity_type`` sets the entity's legal form on the create request, which
  the backend uses to prefill the graph's Reporting Style (partnership →
  PART, llc → LLC, else corporate).

  Local targets only. Registering a user and creating a graph on a deployed
  environment are billed, gated product actions with their own flows — a demo
  runner reaching for them would either fail confusingly or quietly commit an
  org's payment method. Provision the graph the way a customer does, then pass
  its id.
  """
  if not _is_local_target():
    print(f"\n  ERROR: {BASE_URL} is not a local target, so this runner will")
    print("  not register a user or create a graph. Provision the graph the")
    print("  way a customer would (checkout, or POST /v1/graphs with a payment")
    print("  method on file) and pass its id as the last argument.")
    sys.exit(1)

  project_root = Path(__file__).resolve().parents[2]
  if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

  from examples._common.config import get_graph_id, save_graph_id
  from examples.credentials.utils import (
    CredentialContext,
    ensure_user_credentials,
  )

  short_name = scenario.company_name.split(",")[0]
  context = CredentialContext(
    base_url=BASE_URL,
    credentials_path=CREDENTIALS_FILE,
    force=False,
    default_name_prefix=f"{short_name} Demo",
    default_email_prefix=scenario.slug,
    api_key_prefix=f"{short_name} Demo Key",
    display_title=f"{scenario.company_name} Demo Setup",
  )
  credentials = ensure_user_credentials(context)
  api_key = credentials["api_key"]

  demo_slot = (
    scenario.slug if entity_type == "corporation" else f"{scenario.slug}_{entity_type}"
  )
  existing = get_graph_id(CREDENTIALS_FILE, demo_slot)
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
    graph_name=scenario.company_name,
    description=scenario.description,
    schema_extensions=["roboledger"],
  )
  request = CreateGraphRequest(
    metadata=metadata,
    initial_entity={
      "name": scenario.company_name,
      "uri": scenario.uri,
      "entity_type": entity_type,
      "ticker": scenario.ticker,
    },
    tags=["demo", scenario.slug, "roboledger", entity_type],
  )
  print(f"\nCreating graph: {scenario.company_name}  (entity_type={entity_type})")
  response = api_create_graph(client=client, body=request)
  if response.status_code >= 400:
    body = response.content.decode() if response.content else "(no body)"
    print(f"Failed to create graph: HTTP {response.status_code}\n  {body}")
    sys.exit(1)
  if not response.parsed:
    print(f"Failed to create graph: empty response (HTTP {response.status_code})")
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
    CREDENTIALS_FILE, demo_slot, graph_id, time.strftime("%Y-%m-%d %H:%M:%S")
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

  **Local targets only.** It issues raw DELETEs against whatever database
  ``EXTENSIONS_DATABASE_URL`` currently points at, which is not necessarily
  the same system as ``DEMO_API_URL`` — an SSM tunnel makes a remote database
  look local. Rather than try to prove those two agree, the reset is simply
  not run against a remote target: a graph provisioned there is empty anyway,
  so there is nothing to reset on the intended path.
  """
  if not _is_local_target():
    print(f"  SKIPPED — reset is local-only, and the target is {BASE_URL}")
    print("  Expecting a freshly provisioned graph; re-running this episode")
    print("  against a graph that already holds demo data will duplicate it.")
    return

  from .reset import reset_demo_state

  reset_demo_state(graph_id)


# ---------------------------------------------------------------------------
# Step 2b: Create chart of accounts (via HTTP API)
# ---------------------------------------------------------------------------


def create_chart_of_accounts(
  graph_id: str, scenario: Scenario
) -> tuple[dict[str, str], str, int]:
  """Create the CoA taxonomy, its elements, and link to the entity.

  Returns (element_lookup, coa_taxonomy_id, count).
  element_lookup maps account code → element ID for downstream use.

  Uses the Taxonomy Block envelope — one POST creates the taxonomy, the
  mapping structure, and every CoA element atomically.
  """
  accounts = scenario.accounts

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
    for code, name, classification, sub_class, balance_type, description in accounts
  ]

  envelope = client.create_taxonomy_block(
    graph_id,
    {
      "name": "Native Chart of Accounts",
      "taxonomy_type": "chart_of_accounts",
      "description": f"Tenant chart of accounts for {scenario.company_name}.",
      "elements": elements_payload,
      "structures": [
        {
          "name": "CoA to US GAAP Mapping",
          "description": "Maps Chart of Accounts to US GAAP reporting concepts",
          "block_type": "coa_mapping",
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

  return element_lookup, coa_taxonomy_id, len(accounts)


# ---------------------------------------------------------------------------
# Step 2b: Create counterparty agents (via HTTP API)
# ---------------------------------------------------------------------------


def create_agents(graph_id: str, agents: list[dict]) -> dict[str, str]:
  """Create counterparty agents (customers, vendors, employees).

  Each agent is posted via `create-agent` — the same surface a real
  customer's integration uses. Returns a name → Agent.id lookup that
  later steps use to populate `event_block.agent_id` on invoice / bill /
  payment events, exercising the REA counterparty linkage that QB
  pipelines surface by default.
  """
  client = _get_ledger_client()
  lookup: dict[str, str] = {}
  skipped = 0

  for body in agents:
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
    out.append({"element_id": elem_id, "debit_amount": debit, "credit_amount": credit})
  return out


def _can_split_bill_payment(lines: list[tuple[str, int, int]]) -> bool:
  """A bill_payment can be split into bill_received + bill_paid when its
  shape is a vendor purchase: expense/asset DR + cash CR. Liability-side
  debits (e.g. payroll tax deposits crediting cash against an existing
  payable) are settlements, not vendor bills — keep those as plain
  journal entries so we don't misroute the AP intermediate."""
  has_cash_credit = any(code == _CASH_CODE and credit > 0 for code, _, credit in lines)
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

  # Index of open invoice events keyed by (customer_name, amount) so the
  # subsequent payment can populate `discharges_event_id`. A *stack* per key
  # (not a single id) is essential: a customer can have several identical-amount
  # invoices outstanding at once (e.g. a wholesale account billed the same
  # amount for several months while its payments lag), and each payment must
  # discharge a distinct one. A plain dict would overwrite, leaving the earlier
  # same-amount invoices forever un-discharged even after they're paid.
  invoice_index: dict[tuple[str, int], list[str]] = {}

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
        invoice_index.setdefault((agent_name or "", total_amount), []).append(resp.id)
        counts["invoice_issued"] += 1
        line_item_count += len(li_list)

      elif txn_type == "payment":
        _open = invoice_index.get((agent_name or "", total_amount))
        invoice_evt_id = _open.pop() if _open else None
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
          "metadata": {
            **base_metadata,
            "line_items": br_lines,
            "memo": f"Bill: {memo}",
          },
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
          "metadata": {
            **base_metadata,
            "line_items": bp_lines,
            "memo": f"Payment of: {memo}",
          },
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


def create_mappings(
  graph_id: str,
  element_lookup: dict[str, str],
  mappings_for,
  entity_type: str = "corporation",
) -> int:
  """Create mapping associations between CoA elements and rs-gaap reporting concepts.

  CoA → rs-gaap is the canonical mapping target. The Default Style's
  Networks resolve rs-gaap concepts up through the calc-linkbase to FAC
  subtotals at render time; FAC subtotals are derived, not mapped.

  ``mappings_for`` is the episode's ``mappings.mappings_for`` callable
  (entity_type → list[(coa_code, rs_gaap_qname)]).

  Uses `LedgerClient.create_associations()` — the bulk HTTP API — to
  exercise the same path the frontend UI and MCP tools use.
  """
  mappings = mappings_for(entity_type)

  client = _get_ledger_client()

  # Find the coa_mapping structure (created during create_chart_of_accounts step)
  structures = client.list_structures(graph_id, block_type="coa_mapping")
  if not structures:
    print("  ERROR: No mapping structure found")
    return 0
  mapping_id = structures[0].id

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

  # Walk the (form-aware) mappings and post each association one-by-one
  # through create-mapping-association which takes a single pair. The
  # demo's mapping set is ~27 rows so per-call latency is fine.
  created = 0
  for coa_code, rs_gaap_qname in mappings:
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
# Step 3c: Author disclosure notes (TaxonomyBlock reporting_extension)
# ---------------------------------------------------------------------------


def create_disclosure_notes(
  graph_id: str,
  notes: list[dict],
  element_lookup: dict[str, str],
) -> tuple[int, int]:
  """Author per-tenant disclosure note structures and multi-map their members.

  Each note becomes a ``reporting_extension`` TaxonomyBlock: member
  concepts parented under a library total, one ``regulatory_disclosure``
  structure with ``concept_arrangement='roll_up'``, and presentation +
  calculation arcs whose total endpoint IS the library concept. The
  member CoA accounts are then multi-mapped (member + the library total
  the episode's ``mappings.py`` already maps), so the face-statement
  line and the note foot over the same facts. Rendering is fact-driven:
  ``create-report`` picks the note because its concepts receive facts.

  Returns (notes_created, member_mappings_created).
  """
  client = _get_ledger_client()

  taxonomies = client.list_taxonomies(graph_id, taxonomy_type="reporting_standard")
  rs_gaap = next(
    (t for t in taxonomies if (t.get("standard") or "").startswith("rs-gaap")), None
  )
  if rs_gaap is None:
    print("  ERROR: rs-gaap reporting standard not found in graph")
    return 0, 0

  structures = client.list_structures(graph_id, block_type="coa_mapping")
  mapping_id = structures[0].id if structures else None
  if mapping_id is None:
    print("  ERROR: no coa_mapping structure — member multi-mapping skipped")

  notes_created = 0
  member_mappings = 0
  for note in notes:
    struct_name = note["name"]
    total_qname = note["total_qname"]
    members = note["members"]
    payload = {
      "name": note.get("taxonomy_name", f"{struct_name} Extension"),
      "taxonomy_type": "reporting_extension",
      "parent_taxonomy_id": rs_gaap.id,
      "description": note.get("description"),
      "elements": [
        {
          "qname": m["qname"],
          "name": m["name"],
          "parent_ref": total_qname,
          # Balance-sheet notes (inventory) default to debit/instant;
          # flow notes (revenue disaggregation) pass credit/duration.
          "balance_type": m.get("balance_type", "debit"),
          "period_type": m.get("period_type", "instant"),
          "is_monetary": True,
        }
        for m in members
      ],
      "structures": [
        {
          "name": struct_name,
          "description": note.get("description"),
          "block_type": "regulatory_disclosure",
          "concept_arrangement": "roll_up",
          "role_uri": note.get("role_uri"),
          # note_order pins the note's position in the rendered report
          # (the bundle sorts disclosures by it; see _disclosure_sort_key).
          "metadata": (
            {"note_order": note["note_order"]} if note.get("note_order") else {}
          ),
        }
      ],
      "associations": [
        *(
          {
            "structure_ref": struct_name,
            "from_ref": total_qname,
            "to_ref": m["qname"],
            "association_type": "presentation",
            "order_value": float(i + 1),
          }
          for i, m in enumerate(members)
        ),
        *(
          {
            "structure_ref": struct_name,
            "from_ref": total_qname,
            "to_ref": m["qname"],
            "association_type": "calculation",
            "order_value": float(i + 1),
            "weight": 1.0,
          }
          for i, m in enumerate(members)
        ),
      ],
      "rules": [],
    }
    try:
      envelope = client.create_taxonomy_block(graph_id, payload)
    except Exception as e:
      print(f"  ERROR: create-taxonomy-block failed: {e}")
      continue
    notes_created += 1

    member_ids_by_qname = {e.qname: e.id for e in (envelope.elements or [])}
    if mapping_id is None:
      continue
    for m in members:
      coa_id = element_lookup.get(m["coa_code"])
      member_id = member_ids_by_qname.get(m["qname"])
      if not coa_id or not member_id:
        print(f"  WARNING: could not multi-map {m['coa_code']} → {m['qname']}")
        continue
      client.create_mapping_association(
        graph_id,
        mapping_id=mapping_id,
        from_element_id=coa_id,
        to_element_id=member_id,
      )
      member_mappings += 1

  return notes_created, member_mappings


def update_disclosure_notes(graph_id: str, notes: list[dict]) -> int:
  """Grow authored notes via ``update-taxonomy-block`` — the update-path probe.

  For each note carrying an ``update_member``, adds that member element
  plus presentation + calculation arcs to the existing note structure
  through the update operation, then asserts the note's auto RollUp
  footing rule was re-derived to include it (the rule is derived state —
  the update path must refresh it, not leave the creation-time freeze).

  Returns the number of members added.
  """
  client = _get_ledger_client()

  updates = [n for n in notes if n.get("update_member")]
  if not updates:
    return 0

  taxonomies = client.list_taxonomies(graph_id, taxonomy_type="reporting_extension")
  by_name = {t.get("name"): t for t in taxonomies}
  structures = client.list_structures(graph_id, block_type="regulatory_disclosure")
  structure_by_name = {s.get("name"): s for s in structures}

  added = 0
  for note in updates:
    member = note["update_member"]
    taxonomy = by_name.get(note.get("taxonomy_name", f"{note['name']} Extension"))
    structure = structure_by_name.get(note["name"])
    if taxonomy is None or structure is None:
      print(f"  ERROR: could not locate taxonomy/structure for {note['name']!r}")
      continue

    order = float(len(note["members"]) + 1)
    payload = {
      "taxonomy_id": taxonomy.id,
      "elements_to_add": [
        {
          "qname": member["qname"],
          "name": member["name"],
          "parent_ref": note["total_qname"],
          "balance_type": member.get("balance_type", "debit"),
          "period_type": member.get("period_type", "instant"),
          "is_monetary": True,
        }
      ],
      "associations_to_add": [
        {
          "structure_ref": note["name"],
          "from_ref": note["total_qname"],
          "to_ref": member["qname"],
          "association_type": "presentation",
          "order_value": order,
        },
        {
          "structure_ref": note["name"],
          "from_ref": note["total_qname"],
          "to_ref": member["qname"],
          "association_type": "calculation",
          "order_value": order,
          "weight": 1.0,
        },
      ],
    }
    try:
      client.update_taxonomy_block(graph_id, payload)
    except Exception as e:
      print(f"  ERROR: update-taxonomy-block failed: {e}")
      continue

    block = client.get_information_block(graph_id, structure.id)
    rollups = [
      r for r in (block.rules if block else []) if r.rule_pattern == "RollUp"
    ]
    refreshed = any(
      member["qname"]
      in {v.get("variable_qname") for v in (r.get("rule_variables") or [])}
      for r in rollups
    )
    child_count = max(
      (len(r.get("rule_variables") or []) - 1 for r in rollups), default=0
    )
    if not refreshed:
      print(
        f"  ERROR: footing rule for {note['name']!r} was NOT refreshed "
        f"with {member['qname']} — update-path auto-rule refresh failed"
      )
      continue
    print(
      f"  {note['name']}: +{member['qname']} "
      f"(footing rule refreshed, {child_count} children)"
    )
    added += 1

  return added


def create_text_block_notes(
  graph_id: str,
  text_blocks: list[dict],
  doc_ids: dict[str, str],
  scenario: Scenario,
) -> int:
  """Author text-block disclosure notes and bind policy documents to them.

  Each note becomes a ``reporting_extension`` TaxonomyBlock: an abstract
  heading plus one non-monetary text-block concept per bound document,
  arced abstract→concept, on a ``regulatory_disclosure`` structure with
  ``concept_arrangement='text_block'``. Then ``bind-text-block`` attaches
  each uploaded policy document to its concept as a ``Nonnumeric`` fact
  over the scenario's report period — the Document→fact bridge. Reports
  generated afterward snapshot the standing bindings, so the narrative
  rides the report package, the JSON-LD bundle, and the graph.

  Returns the number of documents bound.
  """
  client = _get_ledger_client()

  if scenario.report_period is None:
    print("  (skipped — scenario has no report_period)")
    return 0
  period_start, period_end = scenario.report_period

  taxonomies = client.list_taxonomies(graph_id, taxonomy_type="reporting_standard")
  rs_gaap = next(
    (t for t in taxonomies if (t.get("standard") or "").startswith("rs-gaap")), None
  )
  if rs_gaap is None:
    print("  ERROR: rs-gaap reporting standard not found in graph")
    return 0

  bound = 0
  for note in text_blocks:
    struct_name = note["name"]
    abstract_qname = note["abstract_qname"]
    concepts = note["concepts"]
    payload = {
      "name": note.get("taxonomy_name", f"{struct_name} Extension"),
      "taxonomy_type": "reporting_extension",
      "parent_taxonomy_id": rs_gaap.id,
      "description": note.get("description"),
      "elements": [
        {
          "qname": abstract_qname,
          "name": note.get("abstract_name", struct_name),
          "element_type": "abstract",
          "period_type": "duration",
        },
        *(
          {
            "qname": c["qname"],
            "name": c["name"],
            "parent_ref": abstract_qname,
            "period_type": "duration",
            "is_monetary": False,
          }
          for c in concepts
        ),
      ],
      "structures": [
        {
          "name": struct_name,
          "description": note.get("description"),
          "block_type": "regulatory_disclosure",
          "concept_arrangement": "text_block",
          "role_uri": note.get("role_uri"),
          "metadata": (
            {"note_order": note["note_order"]} if note.get("note_order") else {}
          ),
        }
      ],
      "associations": [
        {
          "structure_ref": struct_name,
          "from_ref": abstract_qname,
          "to_ref": c["qname"],
          "association_type": "presentation",
          "order_value": float(i + 1),
        }
        for i, c in enumerate(concepts)
      ],
      "rules": [],
    }
    try:
      envelope = client.create_taxonomy_block(graph_id, payload)
    except Exception as e:
      print(f"  ERROR: create-taxonomy-block failed: {e}")
      continue
    structure_id = next((s.id for s in (envelope.structures or []) if s.id), None)
    if structure_id is None:
      print(f"  ERROR: no structure id returned for '{struct_name}'")
      continue

    for c in concepts:
      doc_id = doc_ids.get(c["document_title"])
      if doc_id is None:
        print(f"  WARNING: no uploaded document titled '{c['document_title']}'")
        continue
      bind_payload = {
        "document_id": doc_id,
        "structure_id": structure_id,
        "element_qname": c["qname"],
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
      }
      try:
        client.bind_text_block(graph_id, bind_payload)
      except Exception as e:
        print(f"  ERROR: bind-text-block failed: {e}")
        continue
      bound += 1

  return bound


def verify_disclosure_notes(graph_id: str) -> None:
  """Print the rendered disclosure notes after the report ran.

  Fact-driven proof: the notes only exist in the render output because
  the report's facts reached their concepts. Prints each note's rows
  (with the footed total) and its verification summary.
  """
  client = _get_ledger_client()
  blocks = client.list_information_blocks(graph_id, block_type="regulatory_disclosure")
  if not blocks:
    print("  WARNING: no disclosure notes rendered")
    return
  for block in blocks:
    # parse_information_blocks snake_cases every key.
    name = block.get("name") or block.get("display_name") or block.get("id")
    facts = block.get("facts") or []
    summary = block.get("verification_summary") or {}
    rendering = (block.get("view") or {}).get("rendering") or {}
    rows = rendering.get("rows") or []
    print(f"  {name}: {len(facts)} facts, {len(rows)} rendered rows")
    for row in rows:
      text_value = row.get("text_value")
      if text_value:
        # Text-block row — show the narrative head, not a numeric column.
        head = " ".join(str(text_value).split())[:100]
        print(f"      {row.get('element_name')}: “{head}…”")
        continue
      values = row.get("values") or []
      latest = values[-1] if values else None
      marker = "Σ" if row.get("is_subtotal") else " "
      # Report fact values are natural-signed dollars (not cents).
      amount = f"${latest:,.2f}" if isinstance(latest, (int, float)) else "—"
      print(f"    {marker} {row.get('element_name')}: {amount}")
    passed = summary.get("passed", 0)
    total = summary.get("total", 0)
    if total:
      print(f"    Verification: {passed}/{total} rules passed")


def _metric_period_windows(
  scenario, period_windows: list[tuple[date, date]] | None
) -> list[tuple[date | None, date]]:
  """The (period_start, period_end) windows a metric compute should run.

  Monthly windows when the monthly-report loop provided them (the
  standing series at the operating cadence); otherwise a two-window
  probe — prior comparative end + report end, mirroring the report's
  ``_compute_prior_period``.
  """
  if period_windows:
    return list(period_windows)
  if scenario.report_period is None:
    return []
  period_start, period_end = scenario.report_period
  prior_end = period_start - timedelta(days=1)
  return [(None, prior_end), (None, period_end)]


def _post_compute_metrics(
  graph_id: str,
  structure_id: str,
  window: tuple[date | None, date],
  api_key: str,
) -> dict | None:
  """One ``compute-metrics`` call; returns the result dict or None on error.

  ``period_start`` rides along when the window carries one — load-bearing
  for the final monthly window, whose ``period_end`` coincides with the
  annual report's: the start bound keeps FY-duration facts out of the
  monthly column.
  """
  import httpx

  period_start, period_end = window
  payload: dict = {
    "structure_id": structure_id,
    "period_end": period_end.isoformat(),
  }
  if period_start is not None:
    payload["period_start"] = period_start.isoformat()
  resp = httpx.post(
    f"{BASE_URL}/extensions/roboledger/{graph_id}/operations/compute-metrics",
    json=payload,
    headers={"X-API-Key": api_key, "Content-Type": "application/json"},
    timeout=60.0,
  )
  if resp.status_code >= 400:
    print(
      f"  ERROR: compute-metrics {period_end} -> "
      f"HTTP {resp.status_code}: {resp.text[:200]}"
    )
    return None
  return (resp.json() or {}).get("result") or {}


def verify_metrics(
  graph_id: str,
  scenario,
  period_windows: list[tuple[date, date]] | None = None,
) -> None:
  """Compute + render the Key Financial Metrics standing time series.

  With ``period_windows`` (the CLOSED windows from
  :func:`close_monthly_history` — the caller slices off the open
  close-target month) the library-seeded rs-metric catalog computes at
  every month-end — the monthly series the FP&A cadence doctrine calls
  for, bound to the close-stamped canonical statement sets. Detailed
  values print for the final window only; every window prints its counts.
  InterestCoverage skips with a reason for a debt-free scenario,
  exercising the soft-fail path on purpose.
  """
  client = _get_ledger_client()
  blocks = client.list_information_blocks(graph_id, block_type="metric")
  if not blocks:
    print("  ERROR: no metric block found — was the rs-metric package seeded?")
    return
  # Pick the library-seeded catalog by name — tenant-authored metric
  # blocks (create_custom_metrics) share the block_type.
  block = next(
    (b for b in blocks if b.get("name") == "Key Financial Metrics"), blocks[0]
  )
  structure_id = block.get("id")
  print(f"  Metric block: {block.get('name')} ({structure_id})")

  windows = _metric_period_windows(scenario, period_windows)
  if not windows:
    print("  (skipped — scenario has no report_period)")
    return

  api_key = _client_config()["token"]
  for window in windows:
    result = _post_compute_metrics(graph_id, structure_id, window, api_key)
    if result is None:
      continue
    computed = result.get("computed") or []
    skipped = result.get("skipped") or []
    print(f"  {window[1]}: {len(computed)} computed, {len(skipped)} skipped")
    if window is windows[-1]:
      for m in computed:
        value = m.get("value")
        if not isinstance(value, (int, float)):
          continue
        shown = f"${value:,.2f}" if m.get("unit") == "USD" else f"{value:,.2f}"
        print(f"    {m.get('name')}: {shown}")
      for s in skipped:
        print(f"    (skipped) {s.get('element_qname')}: {s.get('reason')}")

  # Re-read the block — the standing sets are now the rendered series.
  blocks = client.list_information_blocks(graph_id, block_type="metric")
  block = next(
    (b for b in blocks if b.get("name") == "Key Financial Metrics"),
    blocks[0] if blocks else None,
  )
  rendering = ((block.get("view") or {}).get("rendering") or {}) if block else {}
  periods = rendering.get("periods") or []
  rows = rendering.get("rows") or []
  print(f"  Time series: {len(periods)} period(s), {len(rows)} metric row(s)")
  current_ratio = next(
    (r for r in rows if r.get("element_qname") == "rs-metric:CurrentRatio"), None
  )
  if current_ratio is None:
    print("  WARNING: Current Ratio row missing from the rendering")
    return
  values = current_ratio.get("values") or []
  print(f"    Current Ratio series: {values}")
  numeric = [v for v in values if isinstance(v, (int, float))]
  if len(periods) >= len(windows) and len(numeric) >= len(windows):
    print(f"  Metric series verified ({len(periods)} periods, all numeric)")
  elif len(numeric) < len(windows):
    print(
      f"  WARNING: Current Ratio has {len(numeric)} numeric value(s) "
      f"across {len(windows)} computed window(s)"
    )


# ---------------------------------------------------------------------------
# Step 3e: Tenant-authored custom metrics (TaxonomyBlock reporting_extension)
# ---------------------------------------------------------------------------


def create_custom_metrics(graph_id: str, catalogs: list[dict]) -> list[dict]:
  """Author tenant metric catalogs via ``create-taxonomy-block``.

  Each catalog becomes a ``reporting_extension`` under rs-gaap carrying a
  ``block_type='metric'`` structure: an abstract catalog root, one concept
  per metric, presentation arcs enumerating them, and one ``Derive`` rule
  per metric whose ``$Variable`` bindings reference rs-gaap anchors by
  qname. This is the open-edges half of the metric surface — the library's
  ``Key Financial Metrics`` (rs-metric) is the curated-core half; judgment
  metrics (working capital views, margin definitions) live here,
  tenant-local, computed by the same structure-agnostic ``compute-metrics``.

  Returns ``[{"structure_id", "name"}]`` for the series probe after the
  report is filed (operands bind to persisted report facts).
  """
  client = _get_ledger_client()

  taxonomies = client.list_taxonomies(graph_id, taxonomy_type="reporting_standard")
  rs_gaap = next(
    (t for t in taxonomies if (t.get("standard") or "").startswith("rs-gaap")), None
  )
  if rs_gaap is None:
    print("  ERROR: rs-gaap reporting standard not found in graph")
    return []

  created: list[dict] = []
  for catalog in catalogs:
    struct_name = catalog["name"]
    abstract_qname = catalog["abstract_qname"]
    metrics = catalog["metrics"]
    payload = {
      "name": catalog.get("taxonomy_name", f"{struct_name} Extension"),
      "taxonomy_type": "reporting_extension",
      "parent_taxonomy_id": rs_gaap.id,
      "description": catalog.get("description"),
      "elements": [
        {
          "qname": abstract_qname,
          "name": struct_name,
          "element_type": "abstract",
          "period_type": "duration",
          "is_monetary": False,
        },
        *(
          {
            "qname": m["qname"],
            "name": m["name"],
            "element_type": "concept",
            "period_type": m.get("period_type", "instant"),
            "is_monetary": m.get("is_monetary", False),
            "balance_type": m.get("balance_type", "debit"),
          }
          for m in metrics
        ),
      ],
      "structures": [
        {
          "name": struct_name,
          "description": catalog.get("description"),
          "block_type": "metric",
          "concept_arrangement": "arithmetic",
        }
      ],
      "associations": [
        {
          "structure_ref": struct_name,
          "from_ref": abstract_qname,
          "to_ref": m["qname"],
          "association_type": "presentation",
          "order_value": float(i + 1),
        }
        for i, m in enumerate(metrics)
      ],
      "rules": [
        {
          "name": f"derive-{m['qname'].split(':', 1)[-1]}",
          "description": m.get("description"),
          "rule_category": "ReportingSystemSpecificRule",
          "rule_pattern": "Derive",
          "expression": m["expression"],
          "message": m.get("description"),
          "severity": "info",
          "target_element_qname": m["qname"],
          "variables": m["variables"],
        }
        for m in metrics
      ],
    }
    # Direct POST rather than the SDK: the generated client's rule_pattern
    # enum doesn't carry 'Derive', and the demo shouldn't depend on an SDK
    # release to author Derive rules.
    import httpx

    resp = httpx.post(
      f"{BASE_URL}/extensions/roboledger/{graph_id}/operations/create-taxonomy-block",
      json=payload,
      headers={
        "X-API-Key": _client_config()["token"],
        "Content-Type": "application/json",
      },
      timeout=60.0,
    )
    if resp.status_code >= 400:
      print(
        f"  ERROR: create-taxonomy-block (metric catalog) -> "
        f"HTTP {resp.status_code}: {resp.text[:200]}"
      )
      continue
    envelope_body = resp.json() or {}
    if envelope_body.get("error"):
      print(f"  ERROR: create-taxonomy-block: {envelope_body['error']}")
      continue

    structures = client.list_structures(graph_id, block_type="metric")
    structure = next((s for s in structures if s.get("name") == struct_name), None)
    if structure is None:
      print(f"  ERROR: metric structure {struct_name!r} not found after create")
      continue
    created.append({"structure_id": structure.id, "name": struct_name})
    print(f"  {struct_name}: {len(metrics)} metric(s) authored")

  return created


def compute_custom_metrics(
  graph_id: str,
  scenario,
  catalogs: list[dict],
  period_windows: list[tuple[date, date]] | None = None,
) -> None:
  """Compute each tenant metric catalog across the metric windows.

  Same windows as :func:`verify_metrics` (monthly when the monthly-report
  loop ran), against the tenant-authored structures — proving
  compute-metrics is structure-agnostic (library catalog and tenant
  catalog run the same op). Values print for the final window only.
  """
  windows = _metric_period_windows(scenario, period_windows)
  if not windows:
    print("  (skipped — scenario has no report_period)")
    return

  api_key = _client_config()["token"]
  for catalog in catalogs:
    print(f"  {catalog['name']}:")
    computed_windows = 0
    for window in windows:
      result = _post_compute_metrics(graph_id, catalog["structure_id"], window, api_key)
      if result is None:
        continue
      if result.get("computed"):
        computed_windows += 1
      if window is windows[-1]:
        for m in result.get("computed") or []:
          value = m.get("value")
          if not isinstance(value, (int, float)):
            continue
          shown = f"${value:,.2f}" if m.get("unit") == "USD" else f"{value:,.4f}"
          print(f"    {window[1]}  {m.get('name')}: {shown}")
        for s in result.get("skipped") or []:
          print(
            f"    {window[1]}  (skipped) {s.get('element_qname')}: {s.get('reason')}"
          )
    print(f"    Series: {computed_windows}/{len(windows)} window(s) computed")


# ---------------------------------------------------------------------------
# Step 3f: Forecast scenario — author levers, walk the driver cascade
# ---------------------------------------------------------------------------


def _graphql_rendering(
  graph_id: str, block_id: str, headers: dict, scenario_id: str | None = None
) -> dict:
  """One block's ``view.rendering`` via GraphQL (scenario-aware read)."""
  import httpx

  scenario_arg = f', scenarioId: "{scenario_id}"' if scenario_id else ""
  query = (
    f'{{ informationBlock(id: "{block_id}"{scenario_arg}) '
    "{ view { rendering { rows { elementQname values } "
    "periods { end label } } } } }"
  )
  resp = httpx.post(
    f"{BASE_URL}/extensions/{graph_id}/graphql",
    json={"query": query},
    headers=headers,
    timeout=60.0,
  )
  if resp.status_code >= 400:
    print(f"  ERROR: graphql read -> HTTP {resp.status_code}: {resp.text[:160]}")
    return {}
  block = ((resp.json() or {}).get("data") or {}).get("informationBlock") or {}
  return (block.get("view") or {}).get("rendering") or {}


def _rendering_value(rendering: dict, qname: str) -> float | None:
  """Last-column value of one row in a rendering grid."""
  for row in rendering.get("rows") or []:
    if row.get("elementQname") == qname:
      values = [v for v in (row.get("values") or []) if isinstance(v, (int, float))]
      return float(values[-1]) if values else None
  return None


def _statement_block_id(graph_id: str, block_type: str) -> str | None:
  """The tenant's reporting structure for a statement family (the one
  whose envelope carries facts — data-driven, never by name)."""
  client = _get_ledger_client()
  blocks = client.list_information_blocks(graph_id, block_type=block_type)
  for block in blocks:
    if block.get("facts"):
      return block.get("id")
  return blocks[0].get("id") if blocks else None


def run_forecast_scenario(
  graph_id: str,
  scenario,
  forecast_levers: dict[str, float],
) -> str | None:
  """Author + compute the operating-budget scenario.

  The full forecast-engine loop through the public surface: the forecast
  block is authored via the generic ``create-information-block`` op
  (lever values on rs-driver catalog concepts), ``compute-forecast``
  walks the driver cascade 12 months past the close boundary, and the
  scenario's metric series extends through ``compute-metrics`` with the
  scenario filter. Verifications are e2e claims, not spot prints:

  - month-over-month revenue growth equals the asserted lever exactly
    (compounding through [t-1] binding),
  - AR / (Revenues / 30) equals the asserted DSO (days-based balance),
  - the ACTUALS metric envelope is byte-for-byte unchanged in period
    count (the scenario never leaks into default reads — the hijack
    check, live),
  - the scenario metric envelope extends by the horizon with
    "(forecast)"-labeled columns.
  """
  import httpx

  api_key = _client_config()["token"]
  headers = {"X-API-Key": api_key, "Content-Type": "application/json"}
  horizon = 12

  # 1. Author the scenario — lever VALUES on rs-driver concepts; the
  #    mechanics (what each lever drives) are the library's Derive rules.
  #    (Provisioning idempotency is owned by ``reset_demo``, which wipes
  #    prior demo state — including any earlier forecast block.)
  levers = [
    {"qname": qname, "value": value} for qname, value in forecast_levers.items()
  ]
  resp = httpx.post(
    f"{BASE_URL}/extensions/roboledger/{graph_id}/operations/create-information-block",
    json={
      "block_type": "forecast",
      "payload": {
        "name": f"FY{(scenario.report_period[1].year + 1) % 100} Operating Budget"
        if scenario.report_period
        else "Operating Budget",
        "scenario_kind": "budget",
        "horizon_months": horizon,
        "levers": levers,
      },
    },
    headers=headers,
    timeout=60.0,
  )
  if resp.status_code >= 400:
    print(f"  ERROR: create forecast -> HTTP {resp.status_code}: {resp.text[:200]}")
    return None
  envelope = (resp.json() or {}).get("result") or {}
  scenario_id = envelope.get("id")
  mechanics = ((envelope.get("artifact") or {}).get("mechanics")) or {}
  print(f"  Scenario:     {envelope.get('name')} ({scenario_id})")
  print(f"  Base period:  {mechanics.get('base_period')} (+{horizon} months)")
  for qname, value in forecast_levers.items():
    print(f"    {qname.split(':')[-1]}: {value}")

  # 2. Walk the cascade — first to horizon-1 (the compounding /
  #    continuity probes' baseline), then the full horizon (rerun
  #    replaces cleanly).
  is_block_id = _statement_block_id(graph_id, "income_statement")
  bs_block_id = _statement_block_id(graph_id, "balance_sheet")
  cf_block_id = _statement_block_id(graph_id, "cash_flow_statement")
  rev_prev = cash_prev = re_prev = None
  if is_block_id:
    probe = httpx.post(
      f"{BASE_URL}/extensions/roboledger/{graph_id}/operations/compute-forecast",
      json={"structure_id": scenario_id, "months": horizon - 1},
      headers=headers,
      timeout=120.0,
    )
    if probe.status_code < 400:
      rendering = _graphql_rendering(graph_id, is_block_id, headers, scenario_id)
      rev_prev = _rendering_value(rendering, "rs-gaap:Revenues")
      if bs_block_id:
        probe_bs = _graphql_rendering(graph_id, bs_block_id, headers, scenario_id)
        cash_prev = _rendering_value(
          probe_bs, "rs-gaap:CashAndCashEquivalentsAtCarryingValue"
        )
        re_prev = _rendering_value(
          probe_bs, "rs-gaap:RetainedEarningsAccumulatedDeficit"
        )

  resp = httpx.post(
    f"{BASE_URL}/extensions/roboledger/{graph_id}/operations/compute-forecast",
    json={"structure_id": scenario_id},
    headers=headers,
    timeout=120.0,
  )
  if resp.status_code >= 400:
    print(f"  ERROR: compute-forecast -> HTTP {resp.status_code}: {resp.text[:200]}")
    return scenario_id
  result = (resp.json() or {}).get("result") or {}
  months_computed = result.get("months_computed") or []
  skipped = result.get("skipped") or []
  print(f"  Cascade:      {len(months_computed)}/{horizon} months computed")
  for skip in skipped[:3]:
    print(f"    (skipped) {skip.get('element_qname')}: {skip.get('reason')}")

  # 3. Compounding — the last month's revenue over the prior month's
  #    must equal (1 + growth) exactly; [t-1] binding e2e.
  growth = forecast_levers.get("rs-driver:RevenueGrowthRate")
  rendering = (
    _graphql_rendering(graph_id, is_block_id, headers, scenario_id)
    if is_block_id
    else {}
  )
  rev_last = _rendering_value(rendering, "rs-gaap:Revenues")
  if growth and rev_prev and rev_last:
    ratio = rev_last / rev_prev
    if abs(ratio - (1 + growth)) < 1e-6:
      print(f"  Compounding:  {ratio:.4f} == 1 + {growth} (month {horizon})")
    else:
      print(f"  WARNING: growth ratio {ratio:.6f} != {1 + growth}")
  labels = [p.get("label") for p in rendering.get("periods") or []]
  if labels and all(label and "(forecast)" in label for label in labels):
    print(f"  Labels:       {labels[-1]}")

  # 4. Working capital — AR against the same month's revenue must
  #    reproduce the asserted DSO (days-based balance mechanics).
  dso = forecast_levers.get("rs-driver:DaysSalesOutstanding")
  bs_rendering = (
    _graphql_rendering(graph_id, bs_block_id, headers, scenario_id)
    if bs_block_id
    else {}
  )
  if dso and rev_last:
    ar = _rendering_value(bs_rendering, "rs-gaap:ReceivablesNetCurrent")
    if ar is not None:
      implied = ar / (rev_last / 30)
      if abs(implied - dso) < 1e-6:
        print(f"  DSO:          AR / (Revenues/30) == {implied:.1f} days")
      else:
        print(f"  WARNING: implied DSO {implied:.4f} != asserted {dso}")

  # 4b. Articulation — the three-statement claims, live:
  #     A = L + E by construction; RE rolls by exactly NI; cash moves by
  #     exactly the CF net change; every month's rule corpus passes.
  assets = _rendering_value(bs_rendering, "rs-gaap:Assets")
  le_total = _rendering_value(bs_rendering, "rs-gaap:LiabilitiesAndStockholdersEquity")
  if assets is not None and le_total is not None:
    if abs(assets - le_total) < 0.01:
      print(f"  A = L + E:    {assets:,.2f} == {le_total:,.2f}")
    else:
      print(f"  WARNING: balance sheet out of balance ({assets} != {le_total})")
  cash_last = _rendering_value(
    bs_rendering, "rs-gaap:CashAndCashEquivalentsAtCarryingValue"
  )
  re_last = _rendering_value(bs_rendering, "rs-gaap:RetainedEarningsAccumulatedDeficit")
  ni_last = _rendering_value(rendering, "rs-gaap:NetIncomeLoss")
  if re_prev is not None and re_last is not None and ni_last is not None:
    if abs((re_last - re_prev) - ni_last) < 0.01:
      print(f"  RE roll:      ΔRE == NI ({ni_last:,.2f})")
    else:
      print(f"  WARNING: ΔRE {re_last - re_prev:,.2f} != NI {ni_last:,.2f}")
  if cf_block_id and cash_prev is not None and cash_last is not None:
    cf_rendering = _graphql_rendering(graph_id, cf_block_id, headers, scenario_id)
    net_change = _rendering_value(
      cf_rendering, "rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease"
    )
    if net_change is not None:
      if abs((cash_last - cash_prev) - net_change) < 0.01:
        print(f"  Cash roll:    ΔCash == CF net change ({net_change:,.2f})")
      else:
        print(
          f"  WARNING: ΔCash {cash_last - cash_prev:,.2f} != "
          f"CF net change {net_change:,.2f}"
        )
  verified = [m for m in months_computed if m.get("verification_passed")]
  failed = [m for m in months_computed if m.get("verification_passed") is False]
  if failed:
    print(f"  WARNING: {len(failed)} months failed verification")
    for month in failed[:2]:
      for failure in (month.get("verification_failures") or [])[:2]:
        print(f"    {month.get('period')}: {failure}")
  elif verified:
    print(f"  Verification: {len(verified)}/{len(months_computed)} months pass")
  cf_months = [m for m in months_computed if m.get("cash_flow_fact_set_id")]
  print(f"  Statements:   IS + BS + CF ({len(cf_months)}/{len(months_computed)} CF)")
  for note in result.get("diagnostics") or []:
    print(f"    (note) {note}")

  # 5. Extend the metric series into the scenario's forward months, then
  #    prove the seam: actuals envelope unchanged, scenario envelope
  #    longer by the horizon with labeled columns.
  client = _get_ledger_client()
  blocks = client.list_information_blocks(graph_id, block_type="metric")
  metric_block = next(
    (b for b in blocks if b.get("name") == "Key Financial Metrics"),
    blocks[0] if blocks else None,
  )
  if metric_block is None:
    return scenario_id
  metric_id = metric_block.get("id")
  actual_rendering = _graphql_rendering(graph_id, metric_id, headers)
  actual_periods = len(actual_rendering.get("periods") or [])

  computed_months = 0
  for month in months_computed:
    payload = {
      "structure_id": metric_id,
      "period_end": month["period_end"],
      "period_start": month["period_start"],
      "scenario_id": scenario_id,
    }
    resp = httpx.post(
      f"{BASE_URL}/extensions/roboledger/{graph_id}/operations/compute-metrics",
      json=payload,
      headers=headers,
      timeout=60.0,
    )
    if resp.status_code < 400 and (
      ((resp.json() or {}).get("result") or {}).get("computed")
    ):
      computed_months += 1
  print(f"  Metrics:      {computed_months}/{len(months_computed)} scenario months")

  after_actual = _graphql_rendering(graph_id, metric_id, headers)
  after_scenario = _graphql_rendering(graph_id, metric_id, headers, scenario_id)
  actual_after = len(after_actual.get("periods") or [])
  scenario_periods = after_scenario.get("periods") or []
  forecast_cols = [
    p for p in scenario_periods if p.get("label") and "(forecast)" in p["label"]
  ]
  if actual_after == actual_periods:
    print(f"  Actuals:      unchanged ({actual_after} periods — no scenario leak)")
  else:
    print(
      f"  WARNING: actuals envelope changed "
      f"({actual_periods} -> {actual_after} periods)"
    )
  print(
    f"  Series:       {len(scenario_periods)} periods with scenario "
    f"({len(forecast_cols)} forecast columns)"
  )
  return scenario_id


# ---------------------------------------------------------------------------
# Step 4b: Seed semantic memories (MCP `remember` via call-tool)
# ---------------------------------------------------------------------------


def seed_memories(graph_id: str, memories: list[dict]) -> int:
  """Seed the graph's semantic memory through the MCP tool surface.

  Memory writes are deliberately MCP-only (``remember``; REST is
  read/governance — list/get/recall), so seeding goes through
  ``POST /v1/graphs/{g}/mcp/call-tool`` — still the public HTTP API, and
  it exercises the same tool an AI Operator uses during the close.
  Degrades gracefully when semantic memory is disabled on the deployment.
  """
  import httpx

  api_key = _client_config()["token"]
  seeded = 0
  for memory in memories:
    resp = httpx.post(
      f"{BASE_URL}/v1/graphs/{graph_id}/mcp/call-tool",
      json={
        "name": "remember",
        "arguments": {
          "text": memory["text"],
          "memory_type": memory.get("memory_type", "fact"),
          "tags": memory.get("tags", []),
        },
      },
      headers={"X-API-Key": api_key, "Content-Type": "application/json"},
      timeout=60.0,
    )
    if resp.status_code >= 400:
      print(f"  WARNING: remember -> HTTP {resp.status_code}: {resp.text[:160]}")
      continue
    body = resp.json() or {}
    payload = body.get("result") or body
    if isinstance(payload, dict) and payload.get("error"):
      print(f"  (skipped) {payload.get('message', payload['error'])}")
      return seeded
    seeded += 1
  return seeded


# ---------------------------------------------------------------------------
# Step 3b: AI mapping path — requires Bedrock (optional)
# ---------------------------------------------------------------------------


def run_ai_mapping(graph_id: str) -> None:
  """Trigger the MappingOperator via the auto-map-elements operation.

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
  structures = ledger.list_structures(graph_id, block_type="coa_mapping")
  if not structures:
    print("  ERROR: No coa_mapping structure found — was the CoA taxonomy created?")
    return
  mapping_id = structures[0].id
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


def initialize_fiscal_calendar(graph_id: str, scenario: Scenario) -> str:
  """Initialize a never-closed fiscal calendar and set the close target.

  Uses `LedgerClient.initialize_ledger()` — the HTTP API — with
  ``closed_through=None`` ("fresh business, never closed"): every seeded
  FiscalPeriod starts open, and :func:`close_monthly_history` then closes
  each historical month for real — closing is the act that stamps a
  month's canonical statement FactSets, so the monthly series comes from
  genuine closes rather than a pre-set boundary plus draft reports.

  The close target must be set explicitly (initialize leaves it None
  when closed_through is None) — without it the catch-up sequence,
  auto-advance, and the Closing Book UI degrade.

  Returns the `close_target` period string for use in the next-steps output.
  """
  from robosystems.operations.roboledger.fiscal_calendar import (
    current_month_period,
    previous_period,
  )

  from .engine import get_demo_start_date

  # close_target = last completed month; every month before it will be
  # closed by close_monthly_history, leaving exactly one period queued.
  last_completed = previous_period(current_month_period())
  demo_start = get_demo_start_date(scenario.months)
  demo_start_period = f"{demo_start.year:04d}-{demo_start.month:02d}"

  client = _get_ledger_client()
  result = client.initialize_ledger(
    graph_id,
    closed_through=None,
    earliest_data_period=demo_start_period,
    note=f"{scenario.slug} initialization",
  )
  client.set_close_target(
    graph_id, last_completed, note=f"{scenario.slug} initialization"
  )

  print(f"  close_target:   {last_completed}")
  print(f"  periods seeded: {result.periods_created} (all open)")
  return last_completed


# ---------------------------------------------------------------------------
# Step 4b: Create schedules (via HTTP API)
# ---------------------------------------------------------------------------


def create_schedules(
  graph_id: str, element_lookup: dict[str, str], scenario: Scenario
) -> int:
  """Create depreciation and prepaid amortization schedules.

  Uses `LedgerClient.create_schedule()` — the HTTP API — so schedule
  facts inherit the fiscal calendar's `closed_through` boundary
  automatically. Prior-run schedules are cleaned up via
  `LedgerClient.delete_schedule()`.
  """
  from .engine import add_months as demo_add_months
  from .engine import get_demo_start_date

  demo_start = get_demo_start_date(scenario.months)
  taxonomy_name = f"{scenario.company_name.split(',')[0].split()[0]} Schedules"

  def _schedule_window(start_offset: int, life_months: int) -> tuple[date, date]:
    """Compute (start, end) from month offsets relative to the demo window."""
    start = demo_add_months(demo_start, start_offset)
    end_month = demo_add_months(start, life_months)
    return start, end_month - timedelta(days=1)

  # Derive schedules from the scenario's capex (depreciation) and prepaids
  # (amortization), so the schedule blocks the AI close acts on always match
  # the posted entries the engine emitted.
  # (name, dr_code, cr_code, monthly_cents, original_cents, useful_months, start_offset)
  schedules: list[tuple[str, str, str, int, int, int, int]] = []
  for cx in scenario.capex:
    schedules.append(
      (
        f"{cx.name} Depreciation",
        cx.dep_expense_account,
        cx.accum_dep_account,
        cx.cost // cx.life_months,
        cx.cost,
        cx.life_months,
        0 if cx.in_opening else cx.purchase_offset,
      )
    )
  for pp in scenario.prepaids:
    schedules.append(
      (
        f"{pp.name} Amortization",
        pp.expense_account,
        pp.prepaid_account,
        pp.cost // pp.life_months,
        pp.cost,
        pp.life_months,
        pp.purchase_offset,
      )
    )

  client = _get_ledger_client()

  # Find or create schedule taxonomy
  existing_taxonomies = client.list_taxonomies(graph_id, taxonomy_type="schedule")
  schedule_tax = next(
    (t for t in existing_taxonomies if t.name == taxonomy_name), None
  )
  if schedule_tax:
    taxonomy_id = schedule_tax.id
    # Clean up prior-run schedules for idempotency
    existing_blocks = client.list_information_blocks(graph_id, block_type="schedule")
    for block in existing_blocks:
      if block.taxonomy_name == taxonomy_name:
        client.delete_schedule(graph_id, block.id)
  else:
    result = client.create_taxonomy_block(
      graph_id,
      {
        "name": taxonomy_name,
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


def upload_policies(graph_id: str, documents: list[dict]) -> dict[str, str]:
  """Upload accounting policy documents via ``DocumentClient.upload()``.

  Returns ``{title: document_id}`` so downstream steps (the text-block
  binds) can reference the uploaded documents by title.
  """
  client = _get_document_client()
  doc_ids: dict[str, str] = {}
  for doc in documents:
    try:
      envelope = client.upload(
        graph_id,
        title=doc["title"],
        content=doc["content"],
        folder=doc["folder"],
        tags=doc["tags"],
      )
      # The upload result carries TWO ids: `id` is the PostgreSQL document
      # id (what bind-text-block and get-document take); `document_id` is
      # the OpenSearch-side key (`udoc_`-prefixed). We want the PG id.
      result = getattr(envelope, "result", None)
      doc_id = getattr(result, "id", None)
      if doc_id is None and isinstance(result, dict):
        doc_id = result.get("id")
      if doc_id:
        doc_ids[doc["title"]] = str(doc_id)
    except Exception as e:
      print(f"  WARNING: Failed to upload '{doc['title']}': {e}")

  return doc_ids


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
# Step 7: Generate + file FY 2025 annual report
# ---------------------------------------------------------------------------


def close_monthly_history(graph_id: str, scenario: Scenario) -> list[tuple[date, date]]:
  """Close every completed historical month, in sequence.

  Closing IS the act that persists a month's statements: each close
  pivots the posted ledger and stamps the month's canonical statement
  FactSets (``report_id NULL``), which is the monthly series the metric
  engine, the forecast, and the Plan grid all bind. No draft Report rows
  are minted — reports are a deliberate publication act (the annual
  report stays the filed artifact).

  The FINAL window — the close target — stays OPEN deliberately: closing
  it live is the AI-close demo beat, and its statements stamp then.

  Ordering matters: this must run BEFORE ``create_schedules`` — the
  closeable gate counts pending ``schedule_entry_due`` obligations
  globally, so pre-existing schedules would block every historical
  close. Schedules created after this loop derive their scope from the
  advanced ``closed_through`` and queue only the close-target month's
  obligation (the demo beat intact).

  Fail-loud: a close error aborts provisioning — a hole in the closed
  history would silently hole the statement series.

  Returns the full month-window list (including the open close-target
  month) so downstream consumers share the exact period grid.
  """
  from .engine import add_months, get_demo_start_date, month_windows

  client = _get_ledger_client()
  if scenario.report_period is None:
    print("  (skipped — scenario has no report_period)")
    return []

  # Recover the rolling window's start from the annual period: the annual
  # window ends at the close target, offset ``months - 2`` from the start.
  annual_end = scenario.report_period[1]
  start = add_months(date(annual_end.year, annual_end.month, 1), -(scenario.months - 2))
  windows = month_windows(start, scenario.months)

  # The two window derivations (period seeding from earliest_data_period,
  # close windows from the annual period) must agree on the first month,
  # or the very first close hits the SEQUENCE blocker.
  demo_start = get_demo_start_date(scenario.months)
  if (windows[0][0].year, windows[0][0].month) != (demo_start.year, demo_start.month):
    raise RuntimeError(
      f"Window derivations disagree: first close window starts {windows[0][0]} "
      f"but the calendar was seeded from {demo_start} — the sequence gate "
      f"would block every close."
    )

  stamped = 0
  for _period_start, period_end in windows[:-1]:
    period = f"{period_end.year:04d}-{period_end.month:02d}"
    result = client.close_period(
      graph_id, period, note="historical close (provisioning)"
    )
    # The stamping fields aren't on the generated response model, so read
    # them off the untyped overflow.
    props = getattr(result, "additional_properties", None) or {}
    if props.get("statements_stamped"):
      stamped += 1
  closed = len(windows) - 1
  print(f"  Closed:       {closed}/{closed} historical months")
  print(f"  Stamped:      {stamped} months of canonical statement sets")
  return windows


def generate_annual_report(
  graph_id: str, scenario: Scenario, output_dir: Path
) -> str | None:
  """Create a published, filed annual report for the scenario's report period.

  Exercises the Report Block lifecycle end-to-end: ``create-report`` →
  ``get-report-package`` → ``file-report``. The result is a frozen,
  filed snapshot of the prior year visible at ``/reports/{id}`` in the
  package viewer, alongside the queued-for-close current period.

  Returns the report_id (or None on failure) so the caller can print
  the viewer URL.
  """
  client = _get_ledger_client()
  if scenario.report_period is None:
    print("  (skipped — scenario has no report_period)")
    return None
  period_start, period_end = scenario.report_period

  # Find the coa_mapping structure created during the taxonomy seed
  structures = client.list_structures(graph_id, block_type="coa_mapping")
  if not structures:
    print("  ERROR: No coa_mapping structure — was the CoA created?")
    return None
  mapping_id = structures[0].id

  # Render against rs-gaap — the canonical reporting vocabulary. The
  # CoA was mapped CoA→rs-gaap above, and the Default Reporting Style's
  # Networks resolve rs-gaap concepts into the BS / IS / CF / Equity
  # structures, so we pass the standard name directly: create_report
  # resolves it to the rs-gaap reporting_standard taxonomy (also the
  # request default).
  taxonomy_id = "rs-gaap"

  report = client.create_report(
    graph_id,
    name=f"FY {period_end.year} Annual Report",
    mapping_id=mapping_id,
    taxonomy_id=taxonomy_id,
    period_start=period_start,
    period_end=period_end,
    period_type="annual",
    # CF derivation needs a prior period to delta against; SE roll-forward
    # likewise needs a prior to compute period-over-period equity changes.
    comparative=True,
  )
  report_id = report.id
  print(f"  Generated:    {report_id}")

  # Pull the package to confirm it rehydrates
  package = client.get_report_package(graph_id, report_id)
  if package:
    items = package.get("items", []) or []
    # `block_type` lives nested at item.block.block_type on the
    # rehydrated InformationBlockEnvelope, not on the item itself.
    block_names = [
      (i.get("block") or {}).get("name")
      or (i.get("block") or {}).get("block_type")
      or "?"
      for i in items
    ]
    print(f"  Package:      {len(items)} block(s) — {', '.join(block_names)}")

    # Fact provenance — every FactSet records how its facts were
    # constructed (the auditability spine). Surfaced via the SDK on
    # block.fact_set.provenance; report blocks pivot from the posted ledger.
    origins = [
      (((i.get("block") or {}).get("fact_set") or {}).get("provenance") or {}).get(
        "origin"
      )
      for i in items
    ]
    if origins and all(origins):
      summary = ", ".join(f"{n}×{o}" for o, n in sorted(Counter(origins).items()))
      print(f"  Provenance:   {summary}")
    else:
      print("  WARNING: some FactSets lack a provenance descriptor")

  # File it — flips filing_status draft → filed
  try:
    client.file_report(graph_id, report_id)
    print("  Filed:        ✓")
  except Exception as e:
    print(f"  WARNING: file_report failed: {e}")

  # Download both bundle flavors via the SDK so the demo finishes with a
  # tangible artifact on disk that the customer can open immediately.
  # JSON-LD is the canonical projection; XBRL 2.1 is the filing-grade
  # equivalent. Same Report, same fact set, two serializations.
  _download_bundles(client, graph_id, report_id, output_dir, scenario)

  return report_id


def _download_bundles(
  client, graph_id: str, report_id: str, out_dir: Path, scenario: Scenario
) -> None:
  """Render the aligned report-artifact set for the scenario's report — the
  same shared pipeline every RoboLedger demo uses (``_common/artifacts.py``)."""
  from examples._common.artifacts import render_report_artifacts

  base = scenario.slug.replace("_", "-")
  label = scenario.company_name.split(",")[0]
  render_report_artifacts(client, graph_id, report_id, out_dir, f"{base}-demo", label)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run_demo(
  *,
  scenario: Scenario,
  agents: list[dict],
  mappings_for,
  documents: list[dict],
  output_dir: Path,
  reveal_prompts: list[str],
  disclosures: list[dict] | None = None,
  text_blocks: list[dict] | None = None,
  custom_metrics: list[dict] | None = None,
  memories: list[dict] | None = None,
  forecast_levers: dict[str, float] | None = None,
  argv: list[str] | None = None,
) -> None:
  """Provision a graph and load the full company for one showcase episode.

  ``scenario`` is the company; ``agents`` / ``mappings_for`` / ``documents``
  are the episode's counterparties, CoA→rs-gaap mapping callable, and policy
  docs; ``output_dir`` is where the report bundles land; ``reveal_prompts``
  are the episode-specific Beat-4 analysis questions printed at the end.
  ``disclosures`` (optional) are per-tenant authored disclosure notes (see
  ``create_disclosure_notes``) — authored after mapping, verified after the
  report renders them fact-driven. ``custom_metrics`` (optional) are
  tenant-authored metric catalogs (see ``create_custom_metrics``) —
  authored with the disclosures, computed after the report files.
  ``memories`` (optional) seed the graph's semantic memory through the
  MCP ``remember`` tool (see ``seed_memories``).
  ``forecast_levers`` (optional) authors + computes an operating-budget
  forecast scenario after the metric series lands (see
  ``run_forecast_scenario``) — a ``{rs-driver qname: value}`` map.
  """
  argv = sys.argv if argv is None else argv
  dry_run = "--dry-run" in argv
  with_ai_mapping = "--ai" in argv
  entity_type = scenario.entity_type
  args = [a for a in argv[1:] if not a.startswith("--")]

  # Add project root to path
  project_root = Path(__file__).resolve().parents[2]
  if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

  from .engine import emit, validate

  # Validate data
  txns = emit(scenario)
  print(f"\n{scenario.company_name} — RoboLedger Showcase Demo Setup")
  print("=" * 60)
  print(f"  Accounts:     {len(scenario.accounts)}")
  print(f"  Transactions: {len(txns)}")

  if not validate(txns):
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
    graph_id = create_demo_graph(scenario, entity_type)

  # Reset prior demo state (the ONLY direct-DB operation)
  print(f"\nResetting demo state for graph {graph_id}...")
  reset_demo(graph_id)
  print("  Done")

  # Create chart of accounts (via HTTP API — exercises create-element op)
  print("\nCreating chart of accounts...")
  element_lookup, coa_taxonomy_id, elem_count = create_chart_of_accounts(
    graph_id, scenario
  )
  print(f"  Taxonomy:     {coa_taxonomy_id}")
  print(f"  Elements:     {elem_count}")

  # Create counterparty agents (via HTTP API — exercises create-agent op)
  print("\nCreating counterparty agents...")
  agent_lookup = create_agents(graph_id, agents)
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

  # Create CoA → rs-gaap mappings — hardcoded or AI-powered
  if with_ai_mapping:
    print("\nRunning AI mapping (MappingOperator)...")
    run_ai_mapping(graph_id)
  else:
    print("\nCreating CoA → rs-gaap mappings...")
    mapping_count = create_mappings(
      graph_id, element_lookup, mappings_for, entity_type=entity_type
    )
    print(f"  Mappings:     {mapping_count}")

  # Author disclosure notes (TaxonomyBlock reporting_extension) + member
  # multi-mapping — after CoA mapping so the note's members and the face
  # statements share leaves.
  if disclosures:
    print("\nAuthoring disclosure notes...")
    note_count, member_map_count = create_disclosure_notes(
      graph_id, disclosures, element_lookup
    )
    print(f"  Notes:        {note_count}")
    print(f"  Member maps:  {member_map_count}")

    # Grow a note through update-taxonomy-block — proves the update path
    # re-derives the auto footing rule (not just creation-time emission).
    updated_members = update_disclosure_notes(graph_id, disclosures)
    if updated_members:
      print(f"  Updated:      +{updated_members} member (via update-taxonomy-block)")

  # Author tenant metric catalogs (same reporting_extension surface);
  # the series computes after the report files (operands = report facts).
  authored_metric_catalogs: list[dict] = []
  if custom_metrics:
    print("\nAuthoring custom metrics...")
    authored_metric_catalogs = create_custom_metrics(graph_id, custom_metrics)

  # Initialize fiscal calendar (via HTTP API — exercises initialize op)
  print("\nInitializing fiscal calendar...")
  close_target = initialize_fiscal_calendar(graph_id, scenario)

  # Close the completed history — closing stamps each month's canonical
  # statement FactSets (the monthly series the metrics, forecast, and
  # Plan grid bind). Runs BEFORE schedule creation: the closeable gate
  # counts pending schedule obligations globally, and schedules created
  # after the loop scope themselves to the advanced closed_through.
  print("\nClosing monthly history...")
  period_windows = close_monthly_history(graph_id, scenario)

  # Create schedules (derived from the scenario's capex + prepaids)
  print("\nCreating schedules...")
  schedule_count = create_schedules(graph_id, element_lookup, scenario)
  print(f"  Schedules:    {schedule_count}")

  # Upload policies
  print("\nUploading accounting policies...")
  doc_ids = upload_policies(graph_id, documents)
  print(f"  Documents:    {len(doc_ids)}")

  # Seed semantic memories — the institutional knowledge an AI Operator
  # recalls during the close (via the same MCP `remember` tool it uses).
  if memories:
    print("\nSeeding semantic memories...")
    seeded = seed_memories(graph_id, memories)
    print(f"  Memories:     {seeded}")

  # Bind policy documents as text-block disclosure facts — after upload
  # (needs document ids) and before materialize, so the standing
  # Nonnumeric facts reach the graph in this run.
  if text_blocks:
    print("\nBinding text-block notes...")
    bound_count = create_text_block_notes(graph_id, text_blocks, doc_ids, scenario)
    print(f"  Bound:        {bound_count}")

  # Materialize to graph
  print("\nMaterializing to graph...")
  materialize_graph(graph_id)

  # Generate + file the annual report — the deliberate publication act
  # (the monthly series comes from the close loop above, not reports).
  print("\nGenerating annual report...")
  report_id = generate_annual_report(graph_id, scenario, output_dir)

  # Fact-driven disclosure proof — the notes render because the report's
  # facts reached their concepts (no style composition involved).
  if disclosures and report_id:
    print("\nRendered disclosure notes:")
    verify_disclosure_notes(graph_id)

  # Metrics probe — compute the seeded Key Financial Metrics catalog
  # at every CLOSED monthly window (the final window is the still-open
  # close target with no stamped facts yet — its column arrives when the
  # AI close stamps it), then render the standing time series.
  if report_id:
    print("\nComputing key financial metrics...")
    verify_metrics(graph_id, scenario, period_windows[:-1])

  # Tenant metric catalogs compute through the same structure-agnostic op.
  if report_id and authored_metric_catalogs:
    print("\nComputing custom metrics...")
    compute_custom_metrics(
      graph_id, scenario, authored_metric_catalogs, period_windows[:-1]
    )

  # Forecast scenario — authored levers, driver cascade 12 months past
  # the close boundary, scenario metric series.
  forecast_scenario_id = None
  if report_id and forecast_levers:
    print("\nRunning forecast scenario...")
    forecast_scenario_id = run_forecast_scenario(graph_id, scenario, forecast_levers)

  # Summary
  print("\n" + "=" * 60)
  print(f"  Graph ID: {graph_id}")
  if CREDENTIALS_FILE.exists():
    print(f"  API Key:  (saved to {CREDENTIALS_FILE})")

  from robosystems.operations.roboledger.fiscal_calendar import parse_period

  year, month = parse_period(close_target)
  close_label = date(year, month, 1).strftime("%B %Y")

  print("\n  Ready for the AI close + the unscripted analysis!")
  print(f"\n  Close target:  {close_target} ({close_label})")
  if report_id:
    print(f"  Annual report: filed ({report_id})")
    print(f"  Viewer URL:    /reports/{report_id}?graph={graph_id}")
  if forecast_scenario_id:
    print(f"  Forecast:      /explorer?scenario={forecast_scenario_id}")
  print("\n  Run the close (Claude Desktop / MCP):")
  print(f"    1. Switch to workspace: {graph_id}")
  print("    2. Ask: 'Search for month-end close procedures'")
  print(f"    3. Ask: 'Draft all closing entries for {close_label}'")
  print(f"    4. Ask: 'Close the period {close_target}'")
  print("\n  The reveal — turn Claude loose, no script:")
  for i, prompt in enumerate(reveal_prompts, start=5):
    print(f"    {i}. Ask: '{prompt}'")
  print("=" * 60)
