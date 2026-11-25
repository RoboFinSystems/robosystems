"""Hard assertions over a finished RoboInvestor demo run.

Everything here is checked through the product surface — GraphQL reads,
the graph health endpoint, and Cypher against the materialized graph — so
a failure means a customer would see it too.

Two invariants carry most of the weight:

- **Every operation marks its graph stale.** An operation that writes OLTP
  rows without flagging staleness leaves the graph silently behind, and only
  a pure-investor graph exercises the path.
- **The pre-association is a first-class state.** ``source_graph_id`` is
  meaningful before ``entity_id`` exists, and anything reading issuer
  attribution must prefer the security's own column — reading it off the
  linked entity returns null for exactly the securities that declared
  intent but have not yet been linked.

``validate_run`` prints one line per check and returns False if any
failed, so the caller can exit non-zero.
"""

from __future__ import annotations

from typing import Any

from . import data as portfolio_data

# Stale reasons the six RoboInvestor operations emit. A stale graph
# carrying some other domain's reason would mean the investor writes
# still aren't marking — masked by a ledger write on a dual-schema graph,
# which is exactly how the original defect stayed hidden.
INVESTOR_STALE_REASONS = {
  "portfolio_block_created",
  "portfolio_block_updated",
  "portfolio_block_deleted",
  "security_created",
  "security_updated",
  "security_deleted",
}

EXPECTED_ACTIVE_POSITIONS = (
  len(portfolio_data.INITIAL_POSITIONS) + len(portfolio_data.ADDED_POSITIONS) - 1
)

_DISPOSED_KEY = "aldergrove_seed"


def _expected_cost_basis_cents() -> int:
  opening = sum(p.cost_basis for p in portfolio_data.INITIAL_POSITIONS)
  added = sum(p.cost_basis for p in portfolio_data.ADDED_POSITIONS)
  disposed = sum(
    p.cost_basis
    for p in portfolio_data.INITIAL_POSITIONS
    if p.security_key == _DISPOSED_KEY
  )
  return opening + added - disposed


class Checks:
  """Collects pass/fail results and prints them as it goes."""

  def __init__(self) -> None:
    self.failures: list[str] = []
    self.passed = 0

  def check(self, label: str, condition: bool, detail: str = "") -> bool:
    if condition:
      self.passed += 1
      print(f"    PASS  {label}")
    else:
      self.failures.append(label)
      suffix = f" — {detail}" if detail else ""
      print(f"    FAIL  {label}{suffix}")
    return condition

  def equal(self, label: str, actual: Any, expected: Any) -> bool:
    return self.check(
      label, actual == expected, f"expected {expected!r}, got {actual!r}"
    )


def _first_row(
  query: Any, graph_id: str, cypher: str, checks: Checks
) -> dict[str, Any]:
  """Run a Cypher check, turning a query failure into a recorded failure.

  A gate that dies mid-run prints no summary, so an unrelated hiccup —
  a memory-starved graph node, a restarting container — costs the whole
  report. Returning an empty row makes each dependent check fail on its
  own terms instead.
  """
  try:
    result = query.query(graph_id, cypher)
  except Exception as exc:
    checks.check("graph query executed", False, str(exc)[:160])
    return {}
  return (result.data or [{}])[0]


def _check_copy_renders(
  checks: Checks,
  ledger: Any,
  investor_graph: str,
  issuer_graph: str,
  copy: Any,
  report_id: str | None,
) -> None:
  """The shared copy renders every block the source report does.

  Delivery and traversal can both pass over a copy whose package is empty,
  so this reads the package the app renders from.
  """
  from .main import _field

  package = ledger.get_report_package(investor_graph, _field(copy, "id"))
  items = package.items if package else []
  rendered = [
    i for i in items if i.block.view.rendering and i.block.view.rendering.rows
  ]
  source = ledger.get_report_package(issuer_graph, report_id) if report_id else None
  expected = len(source.items) if source else None
  checks.check(
    "shared copy renders every block of the source report",
    bool(items) and len(rendered) == len(items) and len(items) == expected,
    f"{len(rendered)} of {len(items)} items render; the source has {expected}",
  )


def _validate_received(
  checks: Checks,
  *,
  investor_graph: str,
  security_ids: dict[str, str],
  securities: list[Any],
  materialized: bool,
  traversal_rows: list[dict[str, Any]],
  package: dict[str, Any],
  as_file: dict[str, Any],
) -> None:
  """Reports that arrived from outside the platform, and the route check."""
  from .main import _field, _ledger_client, _query_client

  ledger = _ledger_client()
  issuer_id = package.get("issuer_id")
  if not checks.check(
    "off-platform issuer created", bool(issuer_id), "create-issuer returned no id"
  ):
    return
  holding = next(
    (
      s
      for s in securities
      if _field(s, "id", None) == security_ids[portfolio_data.OFF_PLATFORM_SECURITY_KEY]
    ),
    None,
  )
  checks.equal(
    "the holding is attached to its issuer",
    _field(holding, "entity_id", None) if holding else None,
    issuer_id,
  )
  checks.check(
    "a package that does not foot is refused with the numbers",
    package.get("refused_status") == 422
    and "sum to" in package.get("refused_detail", ""),
    f"HTTP {package.get('refused_status')}: {package.get('refused_detail', '')[:120]}",
  )

  expected_facts = len(portfolio_data.monthly_package()["facts"])
  dry = package.get("dry_run") or {}
  checks.check(
    "the dry run checks the package and writes nothing",
    dry.get("dry_run") is True
    and dry.get("report_id") is None
    and dry.get("fact_count") == expected_facts,
    f"got {dry}",
  )
  result = package.get("result") or {}
  report_id = result.get("report_id")
  if not checks.equal("package ingested", result.get("fact_count"), expected_facts):
    return
  checks.equal("its three sections loaded", result.get("section_count"), 3)
  checks.check(
    "every total footed and the balance sheet balanced",
    result.get("calculations_checked") == 4 and result.get("balance_checked") is True,
    f"got {result}",
  )
  checks.equal(
    "every value landed on a statement or a table",
    result.get("unplaced_concepts"),
    [],
  )
  checks.equal(
    "its statements are arranged as the library arranges them",
    result.get("conforms_to_library"),
    True,
  )
  checks.equal(
    "only the fund's own KPIs were added to its vocabulary",
    sorted(
      q
      for q in result.get("concepts_added") or []
      if q.startswith("kpi:") and not q.endswith("Head")
    ),
    sorted(portfolio_data.PACKAGE_KPIS),
  )

  listed = {_field(r, "id", None) for r in (ledger.list_reports(investor_graph) or [])}
  checks.check(
    "the ingested report is listed for the fund", report_id in listed, f"{listed}"
  )
  stored = ledger.download_report_bundle(investor_graph, report_id).content.decode()
  checks.check(
    "its stored holon downloads, naming the company and a source cell",
    portfolio_data.OFF_PLATFORM_ISSUER["name"] in stored
    and "halyard-2026-09.xlsx#sheet=Balance%20Sheet&cell=C18" in stored,
    f"{len(stored)} bytes",
  )

  if not materialized:
    return
  issuer_name = portfolio_data.OFF_PLATFORM_ISSUER["name"]
  reached = {
    r.get("concept"): r.get("value")
    for r in traversal_rows
    if r.get("issuer") == issuer_name
  }
  checks.equal(
    "the traversal reaches the off-platform company's facts",
    reached.get("rs-gaap:Assets"),
    4_180_000,
  )
  query = _query_client()
  kpi = _first_row(
    query,
    investor_graph,
    f"""
    MATCH (e:Entity {{identifier: '{issuer_id}'}})-[:ENTITY_HAS_REPORT]->(:Report)
          -[:REPORT_HAS_FACT]->(f:Fact)-[:FACT_HAS_ELEMENT]->(el:Element)
    WHERE el.qname = 'kpi:Headcount'
    RETURN f.numeric_value AS value
    """,
    checks,
  )
  checks.equal("a KPI of the fund's own is in the graph by name", kpi.get("value"), 31)

  # The same report by both routes: the copy the share wrote, and the file.
  shared_id = as_file.get("shared_report_id")
  file_id = (as_file.get("result") or {}).get("report_id")
  if not checks.check(
    "the shared report's holon ingested as a file",
    bool(shared_id and file_id),
    f"got {as_file}",
  ):
    return
  by_route: dict[str, dict[str, Any]] = {}
  for label, report in (("shared", shared_id), ("file", file_id)):
    by_route[label] = _first_row(
      query,
      investor_graph,
      f"""
      MATCH (r:Report {{identifier: '{report}'}})-[:REPORT_HAS_FACT]->(f:Fact)
      RETURN count(f) AS facts, sum(f.numeric_value) AS total
      """,
      checks,
    )
  checks.check(
    "the file lands the same facts and values as the share",
    by_route["shared"].get("facts") == by_route["file"].get("facts")
    and by_route["shared"].get("facts", 0) > 0
    and round(by_route["shared"].get("total") or 0)
    == round(by_route["file"].get("total") or 0),
    f"shared {by_route['shared']} vs file {by_route['file']}",
  )


def validate_run(
  *,
  investor_graph: str,
  issuer_graph: str,
  portfolio_id: str,
  security_ids: dict[str, str],
  report_id: str | None,
  shared: bool,
  materialized: bool,
  guard_held: bool,
  was_stale: bool,
  stale_reason: str | None,
  traversal_rows: list[dict[str, Any]],
  package: dict[str, Any] | None = None,
  as_file: dict[str, Any] | None = None,
) -> bool:
  """Assert every invariant the run is supposed to establish."""
  from .main import _field, _investor_client, _ledger_client, _query_client

  checks = Checks()
  investor = _investor_client()
  ledger = _ledger_client()

  # ── Securities (Master Data) ─────────────────────────────────────────
  securities_page = investor.list_securities(investor_graph, is_active=True)
  securities = _field(securities_page, "securities", []) or []
  checks.equal(
    "every security registered and active",
    len(securities),
    len(portfolio_data.SECURITIES),
  )

  issuer_keys = portfolio_data.issuer_linked_keys()
  pre_associated = [
    s for s in securities if _field(s, "source_graph_id", None) == issuer_graph
  ]
  checks.equal(
    "securities pre-associated to the issuer graph",
    len(pre_associated),
    len(issuer_keys),
  )

  # ── Portfolio Block ──────────────────────────────────────────────────
  block = investor.get_portfolio_block(investor_graph, portfolio_id)
  checks.check("portfolio block reads back", block is not None)

  if block is not None:
    checks.equal(
      "active position count after deltas",
      _field(block, "active_position_count", 0),
      EXPECTED_ACTIVE_POSITIONS,
    )
    expected_dollars = _expected_cost_basis_cents() / 100
    checks.equal(
      "total cost basis (dollars, from integer cents)",
      round(_field(block, "total_cost_basis_dollars", 0.0) or 0.0, 2),
      round(expected_dollars, 2),
    )
    checks.check(
      "portfolio owner resolves to the fund entity",
      _field(block, "owner", None) is not None,
      "Portfolio.entity_id was accepted on write but did not surface",
    )

  # The disposed position must be gone from the active projection and
  # must not have been hard-deleted — dispose is a soft state change.
  active_positions = _field(
    investor.list_positions(investor_graph, portfolio_id=portfolio_id, status="active"),
    "positions",
    [],
  )
  disposed_positions = _field(
    investor.list_positions(
      investor_graph, portfolio_id=portfolio_id, status="disposed"
    ),
    "positions",
    [],
  )
  checks.equal(
    "active positions (read projection)",
    len(active_positions or []),
    EXPECTED_ACTIVE_POSITIONS,
  )
  checks.equal(
    "disposed position retained, not deleted", len(disposed_positions or []), 1
  )

  # Money is integer cents in storage; dollars are a boundary concern.
  cents_ok = all(
    _field(p, "cost_basis", 0)
    == round((_field(p, "cost_basis_dollars", 0.0) or 0.0) * 100)
    for p in (active_positions or [])
  )
  checks.check("cost basis cents ↔ dollars agree on every position", cents_ok)

  # The re-mark applied through update-portfolio-block.
  cadence_positions = [
    p
    for p in (active_positions or [])
    if _field(p, "security_id", None) == security_ids["cadence_series_a"]
  ]
  if checks.check("re-marked position found", len(cadence_positions) == 1):
    marked = cadence_positions[0]
    checks.equal(
      "409A re-mark applied (cents)",
      _field(marked, "current_value", None),
      portfolio_data.CADENCE_REMARK_VALUE,
    )
    checks.equal(
      "valuation source recorded",
      _field(marked, "valuation_source", None),
      portfolio_data.CADENCE_REMARK_SOURCE,
    )

  checks.check(
    "cascade-delete guard refuses active positions without confirmation",
    guard_held,
  )

  # ── Staleness (the regression guard) ─────────────────────────────────
  checks.check(
    "investor writes marked the graph stale",
    was_stale,
    "materialization is sensor-driven on this flag alone",
  )
  checks.check(
    "stale reason came from a RoboInvestor operation",
    stale_reason in INVESTOR_STALE_REASONS,
    f"got {stale_reason!r}; a ledger reason means the investor ops still aren't marking",
  )

  # ── The cross-graph handshake ────────────────────────────────────────
  if shared:
    linked = [
      e
      for e in (ledger.list_entities(investor_graph, source="linked") or [])
      if _field(e, "source_graph_id", None) == issuer_graph
    ]
    linked_ok = checks.equal("linked entity created for the issuer", len(linked), 1)

    if linked_ok:
      linked_entity_id = _field(linked[0], "id")
      resolved = [
        s
        for s in securities
        if _field(s, "source_graph_id", None) == issuer_graph
        and _field(s, "entity_id", None) == linked_entity_id
      ]
      checks.equal(
        "every pre-associated security resolved to the linked entity",
        len(resolved),
        len(issuer_keys),
      )

    shared_reports = [
      r
      for r in (ledger.list_reports(investor_graph) or [])
      if _field(r, "source_graph_id", None) == issuer_graph
    ]
    if checks.equal(
      "shared report copied into the fund's schema", len(shared_reports), 1
    ):
      copy = shared_reports[0]
      checks.equal(
        "shared copy is published",
        _field(copy, "generation_status", None),
        "published",
      )
      checks.equal(
        "shared copy back-references the source report",
        _field(copy, "source_report_id", None),
        report_id,
      )
      _check_copy_renders(checks, ledger, investor_graph, issuer_graph, copy, report_id)

    # Holdings roll up by issuer; the platform-native one must carry its
    # source graph rather than the "unlinked" sentinel.
    holdings = _field(
      investor.get_holdings(investor_graph, portfolio_id), "holdings", []
    )
    issuer_holdings = [
      h for h in (holdings or []) if _field(h, "source_graph_id", None) == issuer_graph
    ]
    if checks.equal("holdings roll up the issuer", len(issuer_holdings), 1):
      checks.equal(
        "both issuer securities roll into one holding",
        len(_field(issuer_holdings[0], "securities", []) or []),
        len(issuer_keys),
      )

  # ── Materialization + the traversal ──────────────────────────────────
  # Everything below reads LadybugDB, so a failed materialization would
  # turn one root cause into a dozen misleading failures. Report the root
  # cause once and skip the derived checks.
  if not checks.check(
    "graph materialized cleanly",
    materialized,
    "blue/green abandons the whole WIP on any table error — the graph is unchanged",
  ):
    print("    SKIP  graph-shape and traversal checks (nothing was materialized)")
  else:
    query = _query_client()

    row = _first_row(
      query,
      investor_graph,
      """
      MATCH (p:Portfolio) WITH count(p) AS portfolios
      MATCH (s:Security)  WITH portfolios, count(s) AS securities
      MATCH (pos:Position) RETURN portfolios, securities, count(pos) AS positions
      """,
      checks,
    )
    checks.equal("Portfolio nodes materialized", row.get("portfolios"), 1)
    checks.equal(
      "Security nodes materialized",
      row.get("securities"),
      len(portfolio_data.SECURITIES),
    )
    checks.equal(
      "only active positions materialize",
      row.get("positions"),
      EXPECTED_ACTIVE_POSITIONS,
    )

    edge_row = _first_row(
      query,
      investor_graph,
      """
      MATCH (:Portfolio)-[r:PORTFOLIO_HAS_POSITION]->(:Position)
      WITH count(r) AS has_position
      MATCH (:Position)-[r2:POSITION_IN_SECURITY]->(:Security)
      WITH has_position, count(r2) AS in_security
      MATCH (:Entity)-[r3:ENTITY_HAS_PORTFOLIO]->(:Portfolio)
      RETURN has_position, in_security, count(r3) AS owns_portfolio
      """,
      checks,
    )
    checks.equal(
      "PORTFOLIO_HAS_POSITION edges",
      edge_row.get("has_position"),
      EXPECTED_ACTIVE_POSITIONS,
    )
    checks.equal(
      "POSITION_IN_SECURITY edges",
      edge_row.get("in_security"),
      EXPECTED_ACTIVE_POSITIONS,
    )
    checks.equal("ENTITY_HAS_PORTFOLIO edge", edge_row.get("owns_portfolio"), 1)

  if shared and materialized:
    checks.check(
      "Portfolio → Position → Security → Entity → Report → Fact resolves",
      len(traversal_rows) > 0,
      "the differentiated capability did not execute",
    )
    issuers = {r.get("issuer") for r in traversal_rows}
    expected_issuers = 2 if (package or {}).get("result") else 1
    checks.check(
      "traversal attributes facts to each issuer",
      len(issuers) == expected_issuers and all(issuers),
      f"issuers seen: {issuers}",
    )

  if package is not None and shared:
    _validate_received(
      checks,
      investor_graph=investor_graph,
      security_ids=security_ids,
      securities=securities,
      materialized=materialized,
      traversal_rows=traversal_rows,
      package=package,
      as_file=as_file or {},
    )

  # ── Summary ──────────────────────────────────────────────────────────
  total = checks.passed + len(checks.failures)
  print(f"\n  {checks.passed}/{total} checks passed")
  if checks.failures:
    print("  Failed:")
    for failure in checks.failures:
      print(f"    - {failure}")
    return False
  return True
