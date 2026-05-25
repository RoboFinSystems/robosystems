#!/usr/bin/env python3
"""Materialize the rs-gaap four-statement Report for World Online.

Sibling of ``reconcile.py`` (the source-vocabulary ``mini`` pivot
check). This produces the canonical ``rs-gaap`` projection of the same
22,288 GL lines, materialized through the Report architecture:

1. ``POST /extensions/roboledger/{graph_id}/operations/create-report``
   materializes a Report with 4 Information Blocks (BS, IS, CF, SE).
2. ``statement(reportId, blockType)`` GraphQL queries fetch each rendered
   statement.

Reuses the rendering + GraphQL helpers from
``examples.seattle_method_demo.create_report`` (the building blocks are
generic); only the period, naming, and output path differ. Writes
``output/world-online-four-statements.md``.

Period: the World Online GL runs from an opening balance dated
12/31/2023 through activity in 2028. ``SummaryOfTransactions.csv``
aggregates the entire dataset, so this report is the cumulative position
— balance sheet as of ``PERIOD_END`` with income/cash-flow/equity
movement over ``PERIOD_START..PERIOD_END``. (Charlie's reference report
``index2.html`` doesn't expose its exact period publicly; if it turns
out to be a single fiscal year, narrow these constants.)

Usage:
    uv run python -m examples.seattle_method_world_online.create_report <graph_id>
    uv run python -m examples.seattle_method_world_online.create_report  # cached slot
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path

import requests

from examples.seattle_method_demo.create_report import (
  STATEMENT_ORDER,
  fetch_statement,
  find_mapping_id,
  find_value,
  fmt_money,
  render_statement,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = REPO_ROOT / "examples" / "seattle_method_world_online" / "output"
OUTPUT_FILE = OUTPUT_DIR / "world-online-four-statements.md"

PERIOD_START = "2024-01-01"
PERIOD_END = "2028-12-31"
GRAPH_SLOT = "world_online_test"


def _load_config() -> dict:
  cfg_path = REPO_ROOT / ".local" / "config.json"
  if not cfg_path.exists():
    raise SystemExit("Missing .local/config.json — run `just demo-user` first.")
  return json.loads(cfg_path.read_text())


def _resolve_graph_id(cfg: dict, cli_graph: str | None) -> str:
  if cli_graph:
    return cli_graph
  cached = cfg.get("graphs", {}).get(GRAPH_SLOT)
  if not cached:
    raise SystemExit(
      f"No {GRAPH_SLOT} graph cached in .local/config.json. "
      "Pass a graph_id or run `just demo-world-online` first."
    )
  return cached["graph_id"]


def _post(base_url: str, api_key: str, path: str, body: dict) -> dict:
  resp = requests.post(
    f"{base_url}{path}",
    headers={
      "X-API-Key": api_key,
      "Content-Type": "application/json",
      "Idempotency-Key": f"world-online-{uuid.uuid4()}",
    },
    json=body,
    timeout=180,
  )
  resp.raise_for_status()
  return resp.json()


def create_report(base_url: str, api_key: str, graph_id: str, mapping_id: str) -> dict:
  print(f"Creating Report on graph {graph_id} …")
  envelope = _post(
    base_url,
    api_key,
    f"/extensions/roboledger/{graph_id}/operations/create-report",
    {
      "name": "The World Online — MINI 2026 (cumulative)",
      "taxonomy_id": "rs-gaap",
      "mapping_id": mapping_id,
      "period_start": PERIOD_START,
      "period_end": PERIOD_END,
      "period_type": "annual",
      # Comparative is required for the cash-flow statement: the indirect
      # CF's operating/investing/financing flows are derived from
      # period-over-period balance-sheet deltas (fact_grid Tier 1), so
      # without a prior period only NetIncomeLoss + D&A survive. The
      # 12/31/2023 opening balances fall into the prior period, so the CF
      # correctly reports only the 2024-2028 flows (the opening itself is
      # a position, not a flow). (Driving the CF directly from the tagged
      # flow concepts — Tier 2, the rollforward IBs — is forward work.)
      "comparative": True,
    },
  )
  if envelope.get("status") != "completed":
    raise SystemExit(f"create-report did not complete: {envelope}")
  result = envelope["result"]
  print(f"  Report {result['id']} ({result['generation_status']})")
  return result


def render_report(graph_id: str, report: dict, statements: dict[str, dict]) -> str:
  bs = statements["balance_sheet"]["rows"]
  is_rows = statements["income_statement"]["rows"]
  cf_rows = statements["cash_flow_statement"]["rows"]
  se_rows = statements["equity_statement"]["rows"]

  assets = find_value(bs, "rs-gaap:Assets")
  l_and_e = find_value(bs, "rs-gaap:LiabilitiesAndStockholdersEquity")
  net_income = find_value(is_rows, "rs-gaap:NetIncomeLoss")
  retained_earnings = find_value(bs, "rs-gaap:RetainedEarningsAccumulatedDeficit")
  net_cash_change = find_value(
    cf_rows, "rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease"
  )
  ending_equity = find_value(se_rows, "rs-gaap:StockholdersEquity")
  balanced = assets is not None and l_and_e is not None and abs(assets - l_and_e) < 0.01

  body = f"""# The World Online — Four-Statement Report (rs-gaap projection)

**Graph**: `{graph_id}`
**Report**: `{report["id"]}` ({report["generation_status"]})
**Period**: {PERIOD_START} → {PERIOD_END} (cumulative; opening balance 12/31/2023)
**Dataset**: Charlie Hoffman's *The World Online* — 22,288 GL lines, 3,389 entries
**Source vocabulary**: `mini` (Seattle Method MINI 2026)
**Render vocabulary**: `rs-gaap` (RoboSystems canonical reporting taxonomy)

This is the **rs-gaap projection** of the same general ledger that
produced the `mini` reconciliation in
[`world-online-reconciliation.md`](world-online-reconciliation.md),
materialized through the Report architecture (one Report, four FactSets,
four Information Blocks). The opening balances ingest as ordinary
transactions tagged `mini:OpeningBalance`; the balance sheet therefore
reflects the cumulative position without any separately-injected opening
number.

## Anchor totals

| Anchor | Value |
|---|---:|
| Total Assets (`rs-gaap:Assets`) | {fmt_money(assets)} |
| Total Liabilities & Equity (`rs-gaap:LiabilitiesAndStockholdersEquity`) | {fmt_money(l_and_e)} |
| **Balanced?** | **{"✓ YES (Δ = $0.00)" if balanced else "✗ NO (Δ = " + fmt_money((assets or 0) - (l_and_e or 0)) + ")"}** |
| Net Income (`rs-gaap:NetIncomeLoss`) | {fmt_money(net_income)} |
| Retained Earnings (`rs-gaap:RetainedEarningsAccumulatedDeficit`, auto-derived) | {fmt_money(retained_earnings)} |
| Net Cash Change (`rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease`) | {fmt_money(net_cash_change)} |
| Ending Stockholders' Equity (`rs-gaap:StockholdersEquity`) | {fmt_money(ending_equity)} |

---

## Four Statements

"""
  for block_type, display in STATEMENT_ORDER:
    body += render_statement(statements[block_type], display)
    body += "\n\n---\n\n"

  body += f"""## Companion artifacts

- [`world-online-reconciliation.md`](world-online-reconciliation.md) —
  source-vocabulary (`mini`) cell-by-cell reconciliation against Charlie's
  `SummaryOfTransactions.csv` pivot (also the business-event summary).
- [`../README.md`](../README.md) — demo orchestrator + scope notes.

### Reproduce

```bash
just demo-world-online                         # full pipeline
just demo-world-online-create-report {graph_id}  # this report only
```
"""
  return body


def main() -> None:
  parser = argparse.ArgumentParser(
    description="Materialize the World Online four-statement rs-gaap Report."
  )
  parser.add_argument("graph_id", nargs="?", default=None, help="Target graph id.")
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
