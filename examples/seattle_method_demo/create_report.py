#!/usr/bin/env python3
"""Materialize the four-statement rs-gaap Report and render it to markdown.

The other half of the cross-taxonomy demonstration. ``reconcile.py`` checks
the same postings in their *source* vocabulary (mini) against Charlie
Hoffman's published facts; this script renders them in the *canonical* one
(rs-gaap), producing a balance sheet, income statement, cash flow statement
and statement of changes in equity from the identical 14 journal entries.

Three steps:

1. ``POST /extensions/roboledger/{graph_id}/operations/create-report``
   materializes a Report with four attached Information Blocks and their
   FactSets.
2. One ``statement(reportId, blockType)`` GraphQL query per block type
   fetches the rendered rows, each with its depth and subtotal flag.
3. Markdown rendering, with anchor totals and a table per statement.

Prerequisites: ``just demo-seattle-method`` has run against the graph, so the
CoA, mappings and journal entries are in place.

Run it (step 8 of the orchestrator; --graph defaults to the cached slot):
    just demo-seattle-method --step create-report
    just demo-seattle-method --step create-report --graph <graph_id>

Writes ``output/seattle-method-case-1-four-statements.md``.
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path

import requests


REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = REPO_ROOT / "examples" / "seattle_method_demo" / "output"
OUTPUT_FILE = OUTPUT_DIR / "seattle-method-case-1-four-statements.md"
DEFAULT_PERIOD_START = "2024-01-01"
DEFAULT_PERIOD_END = "2024-03-31"

STATEMENT_ORDER: list[tuple[str, str]] = [
  ("balance_sheet", "Balance Sheet"),
  ("income_statement", "Income Statement"),
  ("cash_flow_statement", "Cash Flow Statement"),
  ("equity_statement", "Statement of Changes in Equity"),
]


# ── Client helpers ────────────────────────────────────────────────────────


def _load_config() -> dict:
  cfg_path = REPO_ROOT / ".local" / "config.json"
  if not cfg_path.exists():
    raise SystemExit(
      "Missing .local/config.json — run `just demo-user` first."
    )
  return json.loads(cfg_path.read_text())


def _resolve_graph_id(cfg: dict, cli_graph: str | None) -> str:
  if cli_graph:
    return cli_graph
  cached = cfg.get("graphs", {}).get("seattle_method_test")
  if not cached:
    raise SystemExit(
      "No seattle_method_test graph cached in .local/config.json. "
      "Pass a graph_id or run `just demo-seattle-method` first."
    )
  return cached["graph_id"]


def _post(base_url: str, api_key: str, path: str, body: dict) -> dict:
  resp = requests.post(
    f"{base_url}{path}",
    headers={
      "X-API-Key": api_key,
      "Content-Type": "application/json",
      # uuid4 rather than a timestamp, so back-to-back runs within the same
      # second don't collide on the idempotency-key cache.
      "Idempotency-Key": f"sm-case-1-{uuid.uuid4()}",
    },
    json=body,
    timeout=60,
  )
  resp.raise_for_status()
  return resp.json()


def _graphql(base_url: str, api_key: str, graph_id: str, query: str) -> dict:
  resp = requests.post(
    f"{base_url}/extensions/{graph_id}/graphql",
    headers={"X-API-Key": api_key, "Content-Type": "application/json"},
    json={"query": query},
    timeout=60,
  )
  resp.raise_for_status()
  payload = resp.json()
  if payload.get("errors"):
    raise SystemExit(f"GraphQL errors: {payload['errors']}")
  return payload["data"]


# ── Workflow steps ────────────────────────────────────────────────────────


def find_mapping_id(base_url: str, api_key: str, graph_id: str) -> str:
  data = _graphql(
    base_url,
    api_key,
    graph_id,
    "{ mappings { structures { id name blockType } } }",
  )
  structures = (data.get("mappings") or {}).get("structures") or []
  if not structures:
    raise SystemExit(
      "No coa_mapping structure on graph. Was load_taxonomy.py run?"
    )
  for s in structures:
    if "mini" in (s.get("name") or "").lower():
      return s["id"]
  return structures[0]["id"]


def create_report(
  base_url: str, api_key: str, graph_id: str, mapping_id: str
) -> dict:
  print(f"Creating Report on graph {graph_id} …")
  envelope = _post(
    base_url,
    api_key,
    f"/extensions/roboledger/{graph_id}/operations/create-report",
    {
      "name": "Seattle Method Test Case 1 — Q1 2024",
      "taxonomy_id": "rs-gaap",
      "mapping_id": mapping_id,
      "period_start": DEFAULT_PERIOD_START,
      "period_end": DEFAULT_PERIOD_END,
      "period_type": "quarterly",
      "comparative": True,
    },
  )
  if envelope.get("status") != "completed":
    raise SystemExit(f"create-report did not complete: {envelope}")
  result = envelope["result"]
  print(
    f"  Report {result['id']} ({result['generation_status']}) "
    f"— {result.get('entity_name', '?')}"
  )
  return result


def fetch_statement(
  base_url: str, api_key: str, graph_id: str, report_id: str, block_type: str
) -> dict:
  query = (
    "{ statement(reportId: \""
    + report_id
    + "\", blockType: \""
    + block_type
    + "\") { structureName blockType unmappedCount "
    + "periods { start end label } "
    + "rows { elementQname elementName trait values isSubtotal depth } } }"
  )
  data = _graphql(base_url, api_key, graph_id, query)
  stmt = data.get("statement")
  if stmt is None:
    raise SystemExit(f"No statement returned for block_type={block_type}")
  return stmt


# ── Markdown rendering ────────────────────────────────────────────────────


def fmt_money(v: float | None) -> str:
  if v is None:
    return "—"
  if v < 0:
    return f"$({abs(v):,.2f})"
  return f"${v:,.2f}"


def fmt_row(row: dict, n_periods: int) -> str:
  indent = "  " * max(0, row["depth"])
  name = row.get("elementName") or row["elementQname"]
  bold = row["isSubtotal"]
  label = f"{indent}{'**' if bold else ''}{name}{'**' if bold else ''}"
  values = " | ".join(fmt_money(v) for v in row["values"])
  return f"| `{row['elementQname']}` | {label} | {values} |"


def render_statement(stmt: dict, display_name: str) -> str:
  periods = stmt.get("periods") or []
  period_headers = " | ".join(
    f"{p['label']} ({p['start']} → {p['end']})" for p in periods
  )
  header_pipes = " | ".join(["---:"] * len(periods))
  rows = stmt.get("rows") or []
  lines = [
    f"### {display_name}",
    "",
    f"- **Structure**: `{stmt['structureName']}`",
    f"- **Block type**: `{stmt['blockType']}`",
    f"- **Row count**: {len(rows)}",
    f"- **Unmapped elements**: {stmt['unmappedCount']}",
    "",
    f"| QName | Concept | {period_headers} |",
    f"|---|---|{header_pipes}|",
  ]
  for row in rows:
    lines.append(fmt_row(row, len(periods)))
  return "\n".join(lines)


def find_value(rows: list[dict], qname: str) -> float | None:
  for row in rows:
    if row["elementQname"] == qname:
      return row["values"][0] if row["values"] else None
  return None


def _period_label(period: dict | None) -> str:
  """Format a period dict as ``YYYY-MM-DD → YYYY-MM-DD`` for the header.

  Returns ``"—"`` when the period is missing, as in a single-period render
  with no comparative.
  """
  if not period:
    return "—"
  return f"{period['start']} → {period['end']}"


def render_report(
  graph_id: str, report: dict, statements: dict[str, dict]
) -> str:
  bs_stmt = statements["balance_sheet"]
  bs = bs_stmt["rows"]
  is_rows = statements["income_statement"]["rows"]
  cf_rows = statements["cash_flow_statement"]["rows"]
  se_rows = statements["equity_statement"]["rows"]

  # Derive period labels from the actual response so the markdown stays
  # honest if DEFAULT_PERIOD_* (or the prior-period computation) shifts.
  periods = bs_stmt.get("periods") or []
  current_period = periods[0] if periods else None
  prior_period = periods[1] if len(periods) > 1 else None

  assets = find_value(bs, "rs-gaap:Assets")
  l_and_e = find_value(bs, "rs-gaap:LiabilitiesAndStockholdersEquity")
  retained_earnings = find_value(bs, "rs-gaap:RetainedEarningsAccumulatedDeficit")
  net_income = find_value(is_rows, "rs-gaap:NetIncomeLoss") or find_value(
    is_rows, "rs-gaap:IncomeLossFromContinuingOperations"
  )
  net_cash_change = find_value(
    cf_rows, "rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease"
  )
  ending_equity = find_value(se_rows, "rs-gaap:StockholdersEquity")
  balances = (
    assets is not None and l_and_e is not None and abs(assets - l_and_e) < 0.01
  )

  body = f"""# Seattle Method Cross-Taxonomy — Test Case 1: Four-Statement Report

**Graph**: `{graph_id}`
**Report**: `{report["id"]}` ({report["generation_status"]})
**Entity**: {report.get("entity_name") or "—"}
**Period**: {_period_label(current_period)} (Current)
**Comparative**: {_period_label(prior_period)} (Prior — zero opening balances)
**Dataset**: Charlie Hoffman's lemonade-stand 14-JE Q1 2024 fixture
**Source vocabulary**: `mini` (Seattle Method MINI base taxonomy)
**Render vocabulary**: `rs-gaap` (RoboSystems canonical reporting taxonomy)

This report is the **rs-gaap projection** of the same 14 JEs that
produced the all-green `mini` reconciliation in
[`seattle-method-case-1.md`](seattle-method-case-1.md), now materialized
through the Report architecture: one Report row, four FactSets, four
Information Blocks (BS / IS / CF / SE), each rendered through the
rs-gaap presentation network associated with the graph's Reporting Style.

Generated by:

1. `POST /extensions/roboledger/{{graph_id}}/operations/create-report`
   — materializes a Report with all 4 attached IBs and their FactSets.
2. `statement(reportId, blockType)` GraphQL queries (one per statement)
   to fetch the rendered hierarchical view with depth + is_subtotal.

---

## Anchor totals

| Anchor | Value |
|---|---:|
| Total Assets (rs-gaap:Assets) | {fmt_money(assets)} |
| Total Liabilities & Equity (rs-gaap:LiabilitiesAndStockholdersEquity) | {fmt_money(l_and_e)} |
| **Balanced?** | **{"✓ YES (Δ = $0.00)" if balances else "✗ NO (Δ = " + fmt_money((assets or 0) - (l_and_e or 0)) + ")"}** |
| Net Income (rs-gaap:NetIncomeLoss) | {fmt_money(net_income)} |
| Retained Earnings (rs-gaap:RetainedEarningsAccumulatedDeficit, auto-derived) | {fmt_money(retained_earnings)} |
| Net Cash Change (rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease) | {fmt_money(net_cash_change)} |
| Ending Stockholders' Equity (rs-gaap:StockholdersEquity) | {fmt_money(ending_equity)} |

### Retained Earnings is auto-derived (QuickBooks pattern)

The 14-JE fixture posts **no period-end closing entry**. The BS still
balances because the rs-gaap renderer auto-derives
`rs-gaap:RetainedEarningsAccumulatedDeficit` from cumulative Net Income
(revenue − expense) at the report's `period_end`. No closing JE is
required.

This matches how QuickBooks, Xero, and other modern ledgers handle RE
— it stays a *derived* concept, computed-on-the-fly, rather than a
balance written to a posted account by a period-end close. The
benefit: any backdated edit immediately re-flows through cumulative
NI on the next BS render, with no risk of a stale closing-entry
silently desyncing the historical view.

The architectural piece that powers this lives in
`operations/roboledger/reports/fact_grid.py`:

- `_append_empty_equity_facts` — materializes
  `rs-gaap:RetainedEarningsAccumulatedDeficit` at $0 for every period,
  even when no source CoA element maps to RE (the mini case)
- `_close_to_retained_earnings` — rolls (revenue − expense −
  equity-flow-reducers) into the RE fact
- `_close_prior_periods_to_retained_earnings` — adds cumulative
  prior-period NI for accurate multi-period BS

---

## Four Statements

"""

  for block_type, display in STATEMENT_ORDER:
    body += render_statement(statements[block_type], display)
    body += "\n\n---\n\n"

  body += f"""## Provenance

- **Report ID**: `{report["id"]}`
- **Graph**: `{graph_id}`
- **Taxonomy**: `rs-gaap` (resolved at create-report time)
- **Mapping**: `{report.get("mapping_id")}`
- **Created**: {report.get("created_at")}
- **Last generated**: {report.get("last_generated")}

### How to reproduce

```bash
# 1. Provision a fresh demo graph + load taxonomy + ingest 14 JEs +
#    seed mappings + author rollforwards
just demo-seattle-method

# 2. Materialize the Report + render this markdown
uv run python -m examples.seattle_method_demo.create_report
```

## Companion artifacts

- [`seattle-method-case-1.md`](seattle-method-case-1.md) —
  source-vocabulary (`mini`) reconciliation, 17/17 exact match against
  Charlie Hoffman's published `luca.pacioli.ai` facts.
- [`../README.md`](../README.md) — demo orchestrator and methodology
  (reconciliation classification).
"""

  return body


# ── Entrypoint ────────────────────────────────────────────────────────────


def main() -> None:
  parser = argparse.ArgumentParser(
    description="Materialize the Seattle Method four-statement Report."
  )
  parser.add_argument(
    "graph_id",
    nargs="?",
    default=None,
    help="Target graph id. Defaults to the cached seattle_method_test entry.",
  )
  args = parser.parse_args()

  cfg = _load_config()
  base_url = cfg.get("base_url", "http://localhost:8000")
  api_key = cfg["api_key"]
  graph_id = _resolve_graph_id(cfg, args.graph_id)

  mapping_id = find_mapping_id(base_url, api_key, graph_id)
  print(f"  Mapping structure: {mapping_id}")

  report = create_report(base_url, api_key, graph_id, mapping_id)

  print("Fetching rendered statements …")
  statements: dict[str, dict] = {}
  for block_type, display in STATEMENT_ORDER:
    stmt = fetch_statement(base_url, api_key, graph_id, report["id"], block_type)
    statements[block_type] = stmt
    print(f"  {display}: {len(stmt.get('rows') or [])} rows")

  body = render_report(graph_id, report, statements)
  OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
  OUTPUT_FILE.write_text(body)
  print(f"\nWrote {OUTPUT_FILE.relative_to(REPO_ROOT)} ({len(body):,} chars)")


if __name__ == "__main__":
  try:
    main()
  except KeyboardInterrupt:
    sys.exit(130)
