#!/usr/bin/env python3
"""Render a trial balance for the World Online graph.

Charlie's objective list includes "a trial balance" (item 9); MINI 2026
ships a TrialBalance support network. This produces it from the ingested
graph via the existing ``trialBalance`` GraphQL query (the same query
Test Case 1's reconcile consumes) — one row per CoA account with Σdebits,
Σcredits and net balance, plus the footing totals.

A trial balance's defining property is that **total debits == total
credits**; for the World Online GL the per-account net balances also sum
to $0.00 (a balanced ledger). Because LineItems reference the collapsed
mini line-item concept (not the 239 raw GL accounts), the trial balance
is at the mini-concept grain — the same grain as the reconciliation and
the statements.

Period: cumulative — ``TB_START`` precedes the 12/31/2023 opening so the
opening balances are included, through ``TB_END``.

Usage:
    uv run python -m examples.seattle_method_world_online.trial_balance <graph_id>
    uv run python -m examples.seattle_method_world_online.trial_balance  # cached slot
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = REPO_ROOT / "examples" / "seattle_method_world_online" / "output"
OUTPUT_FILE = OUTPUT_DIR / "world-online-trial-balance.md"
GRAPH_SLOT = "world_online_test"
TB_START = "2023-01-01"
TB_END = "2028-12-31"

# Display ordering: balance-sheet accounts first (asset → liability →
# equity), then income-statement accounts (revenue → expense). Unknown
# traits sort last. Within a trait, sort by qname.
_TRAIT_ORDER = {
  "asset": 0,
  "contraAsset": 1,
  "liability": 2,
  "contraLiability": 3,
  "equity": 4,
  "temporaryEquity": 5,
  "revenue": 6,
  "expense": 7,
  "gain": 8,
  "loss": 9,
}


def _get_ledger_client():
  from robosystems_client.clients.ledger_client import LedgerClient

  cfg_path = REPO_ROOT / ".local" / "config.json"
  if not cfg_path.exists():
    raise SystemExit("Missing .local/config.json — run `just demo-user` first.")
  cfg = json.loads(cfg_path.read_text())
  return LedgerClient(
    {
      "base_url": cfg.get("base_url", "http://localhost:8000"),
      "token": cfg["api_key"],
    }
  )


def _resolve_graph_id(cli_graph: str | None) -> str:
  if cli_graph:
    return cli_graph
  cfg = json.loads((REPO_ROOT / ".local" / "config.json").read_text())
  slot = (cfg.get("graphs") or {}).get(GRAPH_SLOT)
  if not slot:
    raise SystemExit(
      f"No {GRAPH_SLOT} graph cached in .local/config.json. "
      "Pass a graph_id or run `just demo-world-online` first."
    )
  return slot["graph_id"]


def _fmt(v: float | None) -> str:
  v = v or 0.0
  if abs(v) < 0.005:
    return "—"
  return f"$({abs(v):,.2f})" if v < 0 else f"${v:,.2f}"


def render(graph_id: str, rows: list) -> str:
  # Keep only mini CoA accounts with activity (the graph also carries
  # the rs-gaap library; filter it out client-side, as reconcile does).
  mini_rows = [
    r
    for r in rows
    if r.account_code.startswith("mini:") and (r.total_debits or r.total_credits)
  ]
  mini_rows.sort(
    key=lambda r: (
      _TRAIT_ORDER.get(r.trait or "", 99),
      r.account_code,
    )
  )

  total_dr = sum(r.total_debits for r in mini_rows)
  total_cr = sum(r.total_credits for r in mini_rows)
  total_net = sum(r.net_balance for r in mini_rows)
  balances = abs(total_dr - total_cr) < 0.01

  lines = [
    "# The World Online — Trial Balance",
    "",
    f"**Graph**: `{graph_id}`  ",
    f"**Period**: {TB_START} → {TB_END} (cumulative; includes the 12/31/2023 opening)  ",
    "**Source vocabulary**: `mini` (Seattle Method MINI 2026)  ",
    "**Grain**: mini line-item concept (the 239 raw GL accounts collapse to "
    "their mini concept at ingest)  ",
    "",
    "Produced from the ingested graph via the `trialBalance` GraphQL query. "
    "A trial balance balances when **total debits = total credits**; the net "
    "column is debit-positive and sums to $0.00 for a balanced ledger.",
    "",
    "| Account | Trait | Debits | Credits | Net balance (Dr +) |",
    "|---|---|---:|---:|---:|",
  ]
  for r in mini_rows:
    name = r.account_name or r.account_code
    lines.append(
      f"| {name} (`{r.account_code}`) | {r.trait or '—'} | "
      f"{_fmt(r.total_debits)} | "
      f"{_fmt(r.total_credits)} | "
      f"{_fmt(r.net_balance)} |"
    )
  lines += [
    f"| **Total** | | **{_fmt(total_dr)}** | **{_fmt(total_cr)}** | "
    f"**{_fmt(total_net)}** |",
    "",
    f"**Balanced?** {'✓ YES — total debits = total credits' if balances else '✗ NO'} "
    f"(Δ {_fmt(total_dr - total_cr)})",
    "",
    "## Companion artifacts",
    "",
    "- [`world-online-reconciliation.md`](world-online-reconciliation.md) — "
    "business-event summary + reconciliation vs `SummaryOfTransactions.csv`.",
    "- [`world-online-four-statements.md`](world-online-four-statements.md) — "
    "rs-gaap 4-statement Report.",
    "",
    "### Reproduce",
    "",
    "```bash",
    f"just demo-world-online-trial-balance {graph_id}",
    "```",
  ]
  return "\n".join(lines)


def main() -> None:
  parser = argparse.ArgumentParser(description="Render the World Online trial balance.")
  parser.add_argument("graph_id", nargs="?", default=None, help="Target graph id.")
  args = parser.parse_args()

  graph_id = _resolve_graph_id(args.graph_id)
  client = _get_ledger_client()
  data = client.get_trial_balance(graph_id, start_date=TB_START, end_date=TB_END)
  rows = data.rows if data else []
  if not rows:
    raise SystemExit(f"trialBalance returned no rows for graph {graph_id}.")

  body = render(graph_id, rows)
  OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
  OUTPUT_FILE.write_text(body)
  print(f"Wrote {OUTPUT_FILE.relative_to(REPO_ROOT)} ({len(body):,} chars)")


if __name__ == "__main__":
  main()
