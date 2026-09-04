#!/usr/bin/env python3
"""Meridian Ventures Fund I — RoboInvestor end-to-end demo.

The first demo that exercises the RoboInvestor surface, and the first that
crosses a graph boundary. It builds a venture fund's private-markets
portfolio, then has one of its portfolio companies publish a report *into*
the fund's graph — which is the capability nothing else on the market has:

    Portfolio → Position → Security → Entity → Report → Fact

Getting to that traversal takes two tenants. The **issuer** is Cadence
Labs, Inc., a seed-funded B2B SaaS company built by the existing showcase
scenario (``examples/saas_startup_demo``) with a published, filed annual
report. The **investor** is a fund graph provisioned here. Both belong to
the same user and org in this run — the boundary being crossed is the
*graph* boundary, which is where the report-sharing authorization actually
lives.

What each phase proves:

1. **Pre-association.** Securities are registered naming the issuer's
   graph before any link exists. ``entity_id`` stays null and ``holdings``
   reports the issuer as unlinked — a first-class state, not an error.
2. **The Portfolio Block is the write surface.** Portfolio and positions
   validate whole and write atomically; the update applies add / update /
   dispose deltas in one call; deleting a portfolio with active positions
   is refused without an explicit confirmation.
3. **Every write marks its graph stale.** Checked through the graph health
   endpoint after the investor writes and before any materialization —
   the regression guard for the defect that kept this entire domain out of
   LadybugDB.
4. **The handshake links.** The issuer shares its filed report to a publish
   list containing the fund's graph. The share creates a linked ``Entity``
   in the fund's graph and resolves *every* security that pre-associated
   to the issuer.
5. **The traversal executes.** After materialization, one Cypher query
   walks from the fund's portfolio to the issuer's reported facts.
6. **Revocation withdraws the copy** while leaving the linked entity — the
   relationship survives one report being pulled.

Every write goes through the HTTP API via the SDK facades, the same
surface the frontends and MCP tools use. The one exception is ``_reset.py``,
which uses direct DB access so re-runs start clean; that path is
deliberately not a product operation and runs against local targets only.

Set ``DEMO_API_URL`` to point the run at a deployed environment; credentials
then come from a per-target ``.local/config.<host>.json``, both graphs must be
provisioned up front (pass the investor id positionally and ``--issuer``),
and the reset is skipped entirely.

Prerequisites:
    just start        # Docker stack (API, PostgreSQL, Valkey, LadybugDB)
    just demo-user    # writes credentials to .local/config.json

Usage:
    just demo-roboinvestor                    # provision both graphs, run everything
    just demo-roboinvestor <graph_id>         # reuse an existing investor graph
    just demo-roboinvestor --issuer <id>      # use a specific issuer graph
    just demo-roboinvestor --reload-issuer    # rebuild the issuer's ledger first
    just demo-roboinvestor --skip-share       # portfolio surface only, no handshake
    just demo-roboinvestor --dry-run          # preview the portfolio, write nothing

The run ends with a hard validation pass; it exits non-zero if any
invariant fails, so it doubles as a pre-release gate.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from . import data as portfolio_data

# The API this demo loads into. Defaults to the local stack; set DEMO_API_URL to
# point at a deployed environment, in which case every step is ordinary API
# traffic against an account that already holds provisioned graphs. This
# mirrors examples/_scenario/runner.py — the off-local guards below are the
# reason a remote run cannot reach the direct-DB reset (F10).
_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "0.0.0.0", "host.docker.internal"})
BASE_URL = os.environ.get("DEMO_API_URL", "http://localhost:8000").rstrip("/")


def _is_local_target() -> bool:
  return (urlparse(BASE_URL).hostname or "") in _LOCAL_HOSTS


def _credentials_file() -> Path:
  """Credentials + demo-slot map, kept in a separate file per target.

  Local runs use ``.local/config.json``; a remote target uses
  ``.local/config.<host>.json`` so a later local run cannot silently reuse a
  remote graph id from the slot map (and vice versa). Same rationale — and the
  same file — as ``_scenario.runner``, so an issuer graph loaded remotely by
  the saas_startup scenario is found under its slot here.
  """
  if _is_local_target():
    return Path(".local/config.json")
  host = (urlparse(BASE_URL).hostname or "remote").replace(".", "-")
  return Path(f".local/config.{host}.json")


CREDENTIALS_FILE = _credentials_file()

INVESTOR_SLOT = "roboinvestor_demo"
ISSUER_SLOT = "saas_startup"

PUBLISH_LIST_NAME = "Investor Reporting — Meridian"
PUBLISH_LIST_DESCRIPTION = (
  "Quarterly and annual reporting distributed to institutional investors."
)


# ---------------------------------------------------------------------------
# Clients
# ---------------------------------------------------------------------------


def _client_config() -> dict[str, str]:
  if not CREDENTIALS_FILE.exists():
    print("  ERROR: No credentials file. Run `just demo-user` first.")
    sys.exit(1)
  creds = json.loads(CREDENTIALS_FILE.read_text())
  return {"base_url": BASE_URL, "token": creds.get("api_key", "")}


def _investor_client():
  from robosystems_client.clients.investor_client import InvestorClient

  return InvestorClient(_client_config())


def _ledger_client():
  from robosystems_client.clients.ledger_client import LedgerClient

  return LedgerClient(_client_config())


def _graph_client():
  from robosystems_client.clients.graph_client import GraphClient

  return GraphClient(_client_config())


def _query_client():
  from robosystems_client.clients.query_client import QueryClient

  return QueryClient(_client_config())


def _authenticated_client():
  from robosystems_client.client import AuthenticatedClient

  return AuthenticatedClient(
    base_url=BASE_URL,
    token=_client_config()["token"],
    prefix="",
    auth_header_name="X-API-Key",
  )


# ---------------------------------------------------------------------------
# Step 1: the issuer graph — a company that keeps its books here
# ---------------------------------------------------------------------------


def ensure_issuer_graph(*, reload: bool, explicit: str | None) -> str:
  """Return a RoboLedger graph holding a published, filed annual report.

  Reuses the cached ``saas_startup`` graph when it already has one —
  rebuilding a whole ledger to re-read one report would dominate the run.
  Otherwise runs the showcase scenario inline, which provisions the graph
  and files the report as its final step.
  """
  from examples._common.config import cached_graph_id, load_credentials

  if explicit:
    print(f"  Using issuer graph: {explicit}")
    return explicit

  cfg = load_credentials(CREDENTIALS_FILE) or {}
  cached = cached_graph_id(cfg, ISSUER_SLOT)

  if cached and not reload:
    report_id = _published_report_id(cached)
    if report_id:
      print(f"  Reusing issuer graph: {cached}")
      print(f"  Published report:     {report_id}")
      return cached
    print(f"  Issuer graph {cached} has no published report — rebuilding its ledger.")

  print("\n  Building the issuer's ledger (Cadence Labs, Inc.)...")
  print("  This is the full RoboLedger showcase scenario and takes a few minutes.")
  print("  " + "-" * 66)

  from examples.saas_startup_demo.main import main as run_saas_startup

  # A bare argv creates or reuses the scenario's own graph slot; any
  # positional arg would be read as a graph id to load into.
  run_saas_startup(argv=["roboinvestor_demo"])

  print("  " + "-" * 66)
  cfg = load_credentials(CREDENTIALS_FILE) or {}
  graph_id = cached_graph_id(cfg, ISSUER_SLOT)
  if not graph_id:
    print("  ERROR: the issuer scenario did not record a graph id.")
    sys.exit(1)
  return graph_id


def _published_report_id(graph_id: str) -> str | None:
  """The newest published report on a graph, or None."""
  try:
    reports = _ledger_client().list_reports(graph_id)
  except Exception:
    return None
  published = [
    r for r in reports or [] if getattr(r, "generation_status", None) == "published"
  ]
  if not published:
    return None
  return published[-1].id


# ---------------------------------------------------------------------------
# Step 2: the investor graph
# ---------------------------------------------------------------------------


def create_investor_graph() -> str:
  """Create (or reuse) the fund's graph.

  Provisioned with **both** extensions. ``roboinvestor`` is the obvious
  one; ``roboledger`` is required because ``add-publish-list-members``
  rejects any target graph that doesn't declare it. An investor keeps no
  books, so that predicate really wants to be "target can receive
  reports" — until it is, a fund that wants to receive reporting has to
  carry the ledger extension it will never write to.
  """
  project_root = Path(__file__).resolve().parents[2]
  if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

  from examples._common.config import get_graph_id, now_timestamp, save_graph_id

  existing = get_graph_id(CREDENTIALS_FILE, INVESTOR_SLOT)
  if existing:
    print(f"  Reusing investor graph: {existing}")
    return existing

  if not _is_local_target():
    print(f"\n  ERROR: {BASE_URL} is not a local target, so this demo will")
    print("  not register a user or create a graph. Provision the fund graph")
    print("  the way a customer would (POST /v1/graphs with the roboinvestor +")
    print("  roboledger extensions) and pass its id as the positional argument.")
    sys.exit(1)

  from examples.credentials.utils import CredentialContext, ensure_user_credentials

  context = CredentialContext(
    base_url=BASE_URL,
    credentials_path=CREDENTIALS_FILE,
    force=False,
    default_name_prefix="Meridian Demo",
    default_email_prefix="roboinvestor_demo",
    api_key_prefix="Meridian Demo Key",
    display_title="Meridian Ventures Demo Setup",
  )
  credentials = ensure_user_credentials(context)
  api_key = credentials["api_key"]

  from robosystems_client.api.graphs.create_graph import (
    sync_detailed as api_create_graph,
  )
  from robosystems_client.client import AuthenticatedClient
  from robosystems_client.models import CreateGraphRequest, GraphMetadata

  from examples._common.sdk import operation_status

  client = AuthenticatedClient(
    base_url=BASE_URL, token=api_key, prefix="", auth_header_name="X-API-Key"
  )

  metadata = GraphMetadata(
    graph_name=portfolio_data.FUND_NAME,
    description="Early-stage venture fund — private-markets portfolio",
    schema_extensions=["roboinvestor", "roboledger"],
  )
  request = CreateGraphRequest(
    metadata=metadata,
    initial_entity={
      "name": portfolio_data.FUND_NAME,
      "uri": portfolio_data.FUND_URI,
      "entity_type": "partnership",
      "ticker": portfolio_data.FUND_TICKER,
    },
    tags=["demo", "meridian", "roboinvestor", "venture"],
  )
  print(f"\n  Creating investor graph: {portfolio_data.FUND_NAME}")
  response = api_create_graph(client=client, body=request)
  if response.status_code >= 400:
    body = response.content.decode() if response.content else "(no body)"
    print(f"  Failed to create graph: HTTP {response.status_code}\n  {body}")
    sys.exit(1)
  if not response.parsed:
    print(f"  Failed to create graph: empty response ({response.status_code})")
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
      status_data = operation_status(client, operation_id)
      if not status_data:
        continue
      status = status_data.get("status")
      result = status_data.get("result", {})

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

  save_graph_id(CREDENTIALS_FILE, INVESTOR_SLOT, graph_id, now_timestamp())
  print(f"  Graph created: {graph_id}")
  return graph_id


# ---------------------------------------------------------------------------
# Step 3: securities (Master Data CRUD)
# ---------------------------------------------------------------------------


def register_securities(investor_graph: str, issuer_graph: str) -> dict[str, str]:
  """Register every security, pre-associating the issuer's two instruments.

  Securities are Master Data: positions reference them, and the portfolio
  block never mints them. The two Cadence instruments are created with
  ``source_graph_id`` and no ``entity_id`` — the investor declaring an
  issuer relationship that doesn't exist yet.
  """
  client = _investor_client()
  ids: dict[str, str] = {}

  for spec in portfolio_data.SECURITIES:
    body: dict[str, Any] = {
      "name": spec.name,
      "security_type": spec.security_type,
      "terms": spec.terms,
    }
    if spec.security_subtype:
      body["security_subtype"] = spec.security_subtype
    if spec.authorized_shares is not None:
      body["authorized_shares"] = spec.authorized_shares
    if spec.outstanding_shares is not None:
      body["outstanding_shares"] = spec.outstanding_shares
    if spec.links_to_issuer:
      body["source_graph_id"] = issuer_graph

    result = client.create_security(investor_graph, body)
    security_id = _field(result, "id")
    ids[spec.key] = security_id

    linked = "pre-associated" if spec.links_to_issuer else "unlinked"
    print(f"    {spec.name[:52]:<52} {linked}")

  return ids


def show_pre_association(investor_graph: str, security_ids: dict[str, str]) -> None:
  """Print the pre-association state — declared intent, no link yet."""
  client = _investor_client()
  for key in portfolio_data.issuer_linked_keys():
    security = client.get_security(investor_graph, security_ids[key])
    entity_id = _field(security, "entity_id", None)
    source = _field(security, "source_graph_id", None)
    print(f"    {_field(security, 'name')[:52]:<52}")
    print(f"      source_graph_id: {source}")
    print(f"      entity_id:       {entity_id or '(none — awaiting handshake)'}")


# ---------------------------------------------------------------------------
# Step 4: the Portfolio Block
# ---------------------------------------------------------------------------


def create_portfolio(
  investor_graph: str, security_ids: dict[str, str], fund_entity_id: str | None
) -> str:
  """Create the portfolio and its opening positions in one atomic envelope."""
  client = _investor_client()
  valuation = portfolio_data.valuation_date()

  portfolio: dict[str, Any] = {
    "name": portfolio_data.PORTFOLIO_NAME,
    "description": "Direct private-company positions held by Fund I.",
    "strategy": portfolio_data.PORTFOLIO_STRATEGY,
    "inception_date": portfolio_data.inception_date().isoformat(),
    "base_currency": "USD",
  }
  if fund_entity_id:
    portfolio["entity_id"] = fund_entity_id

  positions = [
    _position_body(spec, security_ids, valuation)
    for spec in portfolio_data.INITIAL_POSITIONS
  ]

  block = client.create_portfolio_block(
    investor_graph, {"portfolio": portfolio, "positions": positions}
  )
  portfolio_id = _field(block, "id")
  _print_block(block)
  return portfolio_id


def apply_position_deltas(
  investor_graph: str, portfolio_id: str, security_ids: dict[str, str]
) -> None:
  """Apply add / update / dispose deltas in a single atomic call."""
  client = _investor_client()
  valuation = portfolio_data.valuation_date()

  block = client.get_portfolio_block(investor_graph, portfolio_id)
  position_by_security = {
    _field(_field(p, "security"), "id"): _field(p, "id")
    for p in _field(block, "positions", []) or []
  }

  cadence_position = position_by_security.get(security_ids["cadence_series_a"])
  aldergrove_position = position_by_security.get(security_ids["aldergrove_seed"])
  if not cadence_position or not aldergrove_position:
    print("  ERROR: opening positions missing — cannot apply deltas.")
    sys.exit(1)

  updates: dict[str, Any] = {
    "positions": {
      "add": [
        _position_body(spec, security_ids, valuation)
        for spec in portfolio_data.ADDED_POSITIONS
      ],
      "update": [
        {
          "id": cadence_position,
          "current_value": portfolio_data.CADENCE_REMARK_VALUE,
          "valuation_date": valuation.isoformat(),
          "valuation_source": portfolio_data.CADENCE_REMARK_SOURCE,
        }
      ],
      "dispose": [
        {
          "id": aldergrove_position,
          "disposition_reason": portfolio_data.DISPOSED_REASON,
        }
      ],
    }
  }

  print("    add:      2 positions (bridge warrant, LLC units)")
  print(
    f"    update:   Cadence Series A re-marked to "
    f"${portfolio_data.CADENCE_REMARK_VALUE / 100:,.2f}"
  )
  print(f"    dispose:  Alder Grove Bio — {portfolio_data.DISPOSED_REASON}")

  block = client.update_portfolio_block(investor_graph, portfolio_id, updates)
  _print_block(block)


def prove_cascade_guard(investor_graph: str, portfolio_id: str) -> bool:
  """Deleting a portfolio holding active positions must be refused.

  Returns True when the guard fired. The safety belt is the reason the
  demo can hold a live portfolio through the rest of the run.
  """
  client = _investor_client()
  try:
    client.delete_portfolio_block(
      investor_graph, portfolio_id, confirm_active_positions=False
    )
  except Exception as exc:
    message = str(exc)
    if "409" in message:
      print("    Refused (409) — confirm_active_positions required. Guard holds.")
      return True
    print(f"    WARNING: refused, but not with a 409: {message[:160]}")
    return False

  print("    WARNING: the cascade delete SUCCEEDED without confirmation.")
  return False


def _position_body(
  spec: portfolio_data.PositionSpec,
  security_ids: dict[str, str],
  valuation: date,
) -> dict[str, Any]:
  body: dict[str, Any] = {
    "security_id": security_ids[spec.security_key],
    "quantity": spec.quantity,
    "quantity_type": spec.quantity_type,
    "cost_basis": spec.cost_basis,
    "currency": "USD",
    "acquisition_date": portfolio_data.acquisition_date(spec).isoformat(),
  }
  if spec.current_value is not None:
    body["current_value"] = spec.current_value
    body["valuation_date"] = valuation.isoformat()
    body["valuation_source"] = spec.valuation_source
  if spec.notes:
    body["notes"] = spec.notes
  return body


def _print_block(block: Any) -> None:
  cost = _field(block, "total_cost_basis_dollars", 0.0) or 0.0
  value = _field(block, "total_current_value_dollars", None)
  active = _field(block, "active_position_count", 0)
  owner = _field(block, "owner", None)
  print(f"    Active positions: {active}")
  print(f"    Cost basis:       ${cost:,.2f}")
  print(
    f"    Current value:    {f'${value:,.2f}' if value is not None else '(unmarked)'}"
  )
  if owner:
    print(f"    Owner:            {_field(owner, 'name')}")


# ---------------------------------------------------------------------------
# Step 5: staleness — the regression guard
# ---------------------------------------------------------------------------


def read_staleness(graph_id: str) -> tuple[bool, str | None]:
  """Read the graph's staleness flag through the health endpoint.

  Every extensions write is supposed to mark its graph stale; the
  materialization sensor triggers on nothing else. RoboInvestor shipped
  with zero of its six operations doing so, which kept portfolios,
  securities and positions out of LadybugDB entirely. This is the check
  that would have caught it.
  """
  from robosystems_client.api.graph_health.get_database_health import (
    sync_detailed as api_health,
  )

  response = api_health(graph_id=graph_id, client=_authenticated_client())
  if response.status_code >= 400 or not response.parsed:
    print(f"    WARNING: health check returned HTTP {response.status_code}")
    return (False, None)

  parsed = response.parsed
  payload = getattr(parsed, "additional_properties", None) or {}
  if isinstance(parsed, dict):
    payload = parsed

  is_stale = _field(parsed, "is_stale", payload.get("is_stale"))
  reason = _field(parsed, "stale_reason", payload.get("stale_reason"))
  return (bool(is_stale), reason)


# ---------------------------------------------------------------------------
# Step 6: the cross-graph handshake
# ---------------------------------------------------------------------------


def share_report(issuer_graph: str, investor_graph: str, report_id: str) -> str | None:
  """Publish the issuer's filed report into the fund's graph.

  Creates a publish list on the issuer, adds the fund's graph as a member,
  and shares. Each share is an independent copy: the report row, a
  cross-graph fact set, and every fact are written into the recipient's
  own schema. Returns the publish list id.
  """
  client = _ledger_client()

  publish_list = client.create_publish_list(
    issuer_graph, name=PUBLISH_LIST_NAME, description=PUBLISH_LIST_DESCRIPTION
  )
  list_id = _field(publish_list, "id")
  print(f"    Publish list:  {list_id}")

  client.add_publish_list_members(issuer_graph, list_id, [investor_graph])
  print(f"    Member added:  {investor_graph}")

  response = client.share_report(issuer_graph, report_id, list_id)
  for item in _field(response, "results", []) or []:
    status = _field(item, "status")
    target = _field(item, "target_graph_id")
    if status == "shared":
      print(f"    Shared:        {target} ({_field(item, 'fact_count', 0)} facts)")
    else:
      print(f"    FAILED:        {target} — {_field(item, 'error', status)}")
  return list_id


def show_handshake_result(
  investor_graph: str, issuer_graph: str, security_ids: dict[str, str]
) -> None:
  """Print what the share created on the investor's side."""
  ledger = _ledger_client()
  investor = _investor_client()

  linked = [
    e
    for e in (ledger.list_entities(investor_graph, source="linked") or [])
    if _field(e, "source") == "linked"
  ]
  for entity in linked:
    print(f"    Linked entity:  {_field(entity, 'name')} ({_field(entity, 'id')})")

  for key in portfolio_data.issuer_linked_keys():
    security = investor.get_security(investor_graph, security_ids[key])
    print(
      f"    {_field(security, 'name')[:46]:<46} "
      f"entity_id → {_field(security, 'entity_id', None) or '(still null)'}"
    )

  shared = [
    r
    for r in (ledger.list_reports(investor_graph) or [])
    if _field(r, "source_graph_id", None) == issuer_graph
  ]
  for report in shared:
    print(f"    Shared report:  {_field(report, 'name')} ({_field(report, 'id')})")


# ---------------------------------------------------------------------------
# Step 7: materialize + traverse
# ---------------------------------------------------------------------------


def materialize(graph_id: str) -> bool:
  """Rebuild the graph from OLTP. Returns False on anything but a clean run.

  A *partial* materialization is a failure, not a warning. Blue/green
  abandons the whole WIP database when any table errors, so one bad table
  leaves the previously-active graph untouched — on a first run that means
  an empty graph, and every downstream read silently returns nothing. The
  demo treats it as fatal so the cause is named where it happens.
  """
  from robosystems_client.clients.graph_client import MaterializationOptions

  try:
    result = _graph_client().materialize(
      graph_id,
      MaterializationOptions(
        force=True, rebuild=True, on_progress=lambda msg: print(f"    {msg}")
      ),
    )
  except Exception as exc:
    print(f"    FAIL  materialization raised: {exc}")
    return False

  if not result.success:
    print(f"    FAIL  materialization failed: {result.error or result.message}")
    return False
  if result.total_rows == 0:
    print("    FAIL  materialization reported success but wrote 0 rows.")
    print("          A partial run abandons the WIP graph — check the worker log:")
    print("          just logs-grep worker 'Failed to materialize'")
    return False

  print(f"    Materialized {result.total_rows:,} rows")
  return True


TRAVERSAL_CYPHER = """
MATCH (p:Portfolio)-[:PORTFOLIO_HAS_POSITION]->(pos:Position)
      -[:POSITION_IN_SECURITY]->(s:Security)
MATCH (issuer:Entity)-[:ENTITY_ISSUES_SECURITY]->(s)
MATCH (issuer)-[:ENTITY_HAS_REPORT]->(r:Report)-[:REPORT_HAS_FACT]->(f:Fact)
      -[:FACT_HAS_ELEMENT]->(e:Element)
WHERE e.qname IN $qnames
RETURN p.name AS portfolio,
       s.name AS security,
       issuer.name AS issuer,
       r.name AS report,
       e.qname AS concept,
       f.numeric_value AS value
ORDER BY security, concept
"""

TRAVERSAL_CONCEPTS = [
  "rs-gaap:Revenues",
  "rs-gaap:Assets",
  "rs-gaap:NetIncomeLoss",
  "rs-gaap:CashAndCashEquivalentsAtCarryingValue",
]


def run_traversal(investor_graph: str) -> list[dict[str, Any]]:
  """The differentiated query: a private holding joined to its issuer's facts."""
  try:
    result = _query_client().query(
      investor_graph, TRAVERSAL_CYPHER, {"qnames": TRAVERSAL_CONCEPTS}
    )
  except Exception as exc:
    # An unmaterialized graph has no node tables at all, so this surfaces
    # as a Cypher binder error rather than an empty result. Report and
    # keep going — validation names the invariant that broke.
    print(f"    FAIL  traversal query failed: {str(exc)[:200]}")
    return []
  rows = result.data or []
  if not rows:
    print("    (no rows — the traversal did not resolve)")
    return []

  for row in rows:
    value = row.get("value")
    rendered = f"${value:,.0f}" if isinstance(value, (int, float)) else "—"
    print(
      f"    {str(row.get('security'))[:38]:<38} "
      f"{str(row.get('concept')):<48} {rendered:>18}"
    )
  return rows


def show_graphql_reads(investor_graph: str, portfolio_id: str) -> None:
  """Every RoboInvestor GraphQL read, against the finished graph."""
  client = _investor_client()

  portfolios = client.list_portfolios(investor_graph)
  print(f"    portfolios:     {len(_field(portfolios, 'portfolios', []) or [])}")

  securities = client.list_securities(investor_graph, is_active=True)
  print(f"    securities:     {len(_field(securities, 'securities', []) or [])} active")

  positions = client.list_positions(investor_graph, portfolio_id=portfolio_id)
  print(f"    positions:      {len(_field(positions, 'positions', []) or [])}")

  block = client.get_portfolio_block(investor_graph, portfolio_id)
  print(f"    portfolioBlock: {_field(block, 'active_position_count', 0)} active")

  holdings = client.get_holdings(investor_graph, portfolio_id)
  for holding in _field(holdings, "holdings", []) or []:
    name = _field(holding, "entity_name")
    cost = _field(holding, "total_cost_basis_dollars", 0.0) or 0.0
    count = len(_field(holding, "securities", []) or [])
    source = _field(holding, "source_graph_id", None)
    marker = "  (platform-native issuer)" if source else ""
    print(f"    holdings:       {name[:34]:<34} {count} sec  ${cost:>14,.2f}{marker}")


# ---------------------------------------------------------------------------
# Step 8: revocation
# ---------------------------------------------------------------------------


def revoke_and_verify(issuer_graph: str, investor_graph: str, report_id: str) -> bool:
  """Withdraw the shared copy and assert what survives it.

  The recipient loses the report. They keep the linked ``Entity`` and the
  securities pointing at it — an investor's declared holding is a
  relationship, not an artifact of one filing, and breaking it over a
  single withdrawal would be wrong.
  """
  ledger = _ledger_client()
  try:
    response = ledger.revoke_report_share(issuer_graph, report_id, investor_graph)
  except Exception as exc:
    print(f"    FAIL  revoke raised: {exc}")
    return False

  print(f"    Copy deleted:   {_field(response, 'copy_deleted', None)}")

  remaining = [
    r
    for r in (ledger.list_reports(investor_graph) or [])
    if _field(r, "source_graph_id", None) == issuer_graph
  ]
  linked = [
    e
    for e in (ledger.list_entities(investor_graph, source="linked") or [])
    if _field(e, "source_graph_id", None) == issuer_graph
  ]

  ok = True
  if remaining:
    print(f"    FAIL  {len(remaining)} shared report(s) still in the fund's schema")
    ok = False
  else:
    print("    PASS  shared report withdrawn from the fund's schema")

  if linked:
    print("    PASS  linked entity survives the withdrawal")
  else:
    print("    FAIL  linked entity was removed with the report")
    ok = False
  return ok


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_MISSING = object()


def _field(obj: Any, name: str, default: Any = _MISSING) -> Any:
  """Read a field off an SDK model, a dict, or an attrs envelope result.

  The SDK returns generated attrs classes in production and plain dicts
  under some code paths; ``UNSET`` sentinels stand in for absent optional
  fields. One accessor keeps the demo readable across all three.
  """
  if obj is None:
    return None if default is _MISSING else default

  if isinstance(obj, dict):
    value = obj.get(name, _MISSING)
  else:
    value = getattr(obj, name, _MISSING)

  if value is _MISSING or (value is not None and "Unset" in type(value).__name__):
    if default is _MISSING:
      raise KeyError(f"{type(obj).__name__} has no field {name!r}")
    return default
  return value


def _heading(step: str, title: str) -> None:
  print(f"\n{step}  {title}")
  print("  " + "-" * 68)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
  argv = sys.argv if argv is None else argv
  dry_run = "--dry-run" in argv
  reload_issuer = "--reload-issuer" in argv
  skip_share = "--skip-share" in argv
  revoke = "--revoke" in argv
  issuer_override = _flag_value(argv, "--issuer")
  positional = [a for a in argv[1:] if not a.startswith("--")]
  # `--issuer <id>` consumes its own value; don't also read it as the
  # investor graph id.
  if issuer_override and issuer_override in positional:
    positional.remove(issuer_override)

  project_root = Path(__file__).resolve().parents[2]
  if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

  print(f"\n{portfolio_data.FUND_NAME} — RoboInvestor End-to-End Demo")
  print("=" * 70)

  if dry_run:
    portfolio_data._preview()
    print("Dry run complete. No data written.")
    return

  _heading("[1/9]", "Issuer graph — a portfolio company that keeps its books here")
  issuer_graph = ensure_issuer_graph(reload=reload_issuer, explicit=issuer_override)
  report_id = _published_report_id(issuer_graph)
  if not report_id and not skip_share:
    print("  ERROR: the issuer graph has no published report to share.")
    print("  Re-run with --reload-issuer to rebuild its ledger.")
    sys.exit(1)

  _heading("[2/9]", "Investor graph — the fund")
  investor_graph = positional[0] if positional else create_investor_graph()

  # Local targets only (F10): the reset issues raw DELETEs against whatever
  # EXTENSIONS_DATABASE_URL points at, which is *not* necessarily the same
  # system as DEMO_API_URL — an SSM tunnel makes a remote RDS look local
  # (on an offset port, localhost:15432, so the default port is never prod by
  # accident, but a URL can still name it). The import lives inside the
  # local-only branch so the route is
  # unreachable off-local, not merely skipped; and because the API host proves
  # nothing about the database, the reset itself re-checks on the DB side
  # (``examples/_common/local_db.py``: local host + not-RDS) before deleting.
  # A remote run gets freshly provisioned graphs, so there is nothing to
  # reset — re-running against the same remote graphs will duplicate demo
  # state, not replace it.
  if _is_local_target():
    print("\n  Resetting prior demo state (the only direct-DB step)...")
    from ._reset import reset_investor_state, reset_issuer_share_state

    reset_investor_state(investor_graph)
    reset_issuer_share_state(issuer_graph)
    print("  Done")
  else:
    print(f"\n  Reset SKIPPED — reset is local-only, and the target is {BASE_URL}")
    print("  (raw-DB reset never runs against a remote target; provide freshly")
    print("  provisioned graphs — re-running over demo data will duplicate it).")

  fund_entity = _ledger_client().get_entity(investor_graph)
  fund_entity_id = _field(fund_entity, "id", None) if fund_entity else None

  _heading("[3/9]", "Securities — Master Data, with two pre-associations")
  security_ids = register_securities(investor_graph, issuer_graph)

  print("\n  Pre-association state (declared intent, no link yet):")
  show_pre_association(investor_graph, security_ids)

  _heading("[4/9]", "Portfolio Block — create (atomic envelope)")
  portfolio_id = create_portfolio(investor_graph, security_ids, fund_entity_id)

  _heading("[5/9]", "Portfolio Block — update (add / update / dispose, one call)")
  apply_position_deltas(investor_graph, portfolio_id, security_ids)

  print("\n  Cascade-delete guard:")
  guard_held = prove_cascade_guard(investor_graph, portfolio_id)

  _heading("[6/9]", "Staleness — every write marks its graph")
  is_stale, stale_reason = read_staleness(investor_graph)
  print(f"    is_stale:     {is_stale}")
  print(f"    stale_reason: {stale_reason}")
  if not is_stale:
    print("    ^ REGRESSION: RoboInvestor writes are not marking the graph stale.")

  shared = False
  if skip_share:
    _heading("[7/9]", "Report share — SKIPPED (--skip-share)")
  else:
    _heading("[7/9]", "Cross-graph handshake — the issuer publishes into the fund")
    assert report_id is not None
    share_report(issuer_graph, investor_graph, report_id)
    print("\n  What the share created in the fund's graph:")
    show_handshake_result(investor_graph, issuer_graph, security_ids)
    shared = True

  _heading("[8/9]", "Materialize + traverse")
  print("  Materializing the fund's graph...")
  materialized = materialize(investor_graph)

  rows: list[dict[str, Any]] = []
  if shared:
    print("\n  Portfolio → Position → Security → Entity → Report → Fact")
    rows = run_traversal(investor_graph)

  print("\n  GraphQL reads:")
  show_graphql_reads(investor_graph, portfolio_id)

  _heading("[9/9]", "Validation")
  from .validate import validate_run

  ok = validate_run(
    investor_graph=investor_graph,
    issuer_graph=issuer_graph,
    portfolio_id=portfolio_id,
    security_ids=security_ids,
    report_id=report_id,
    shared=shared,
    materialized=materialized,
    guard_held=guard_held,
    was_stale=is_stale,
    stale_reason=stale_reason,
    traversal_rows=rows,
  )

  # Revocation runs last, after the assertions, because it dismantles the
  # state they check. The point it makes is what *doesn't* go away.
  if shared and revoke:
    _heading("[+]", "Revocation — the sender withdraws the copy")
    assert report_id is not None
    ok = revoke_and_verify(issuer_graph, investor_graph, report_id) and ok

  print("\n" + "=" * 70)
  print(f"  Investor graph: {investor_graph}")
  print(f"  Issuer graph:   {issuer_graph}")
  print(f"  Portfolio:      {portfolio_id}")
  if shared and report_id:
    print(f"  Shared report:  {report_id}")
  print("=" * 70)

  if not ok:
    sys.exit(1)


def _flag_value(argv: list[str], flag: str) -> str | None:
  """Read ``--flag value`` or ``--flag=value`` out of argv."""
  for i, arg in enumerate(argv):
    if arg == flag and i + 1 < len(argv):
      return argv[i + 1]
    if arg.startswith(f"{flag}="):
      return arg.split("=", 1)[1]
  return None


if __name__ == "__main__":
  main()
