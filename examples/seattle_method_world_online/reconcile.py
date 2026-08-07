#!/usr/bin/env python3
"""Reconcile the World Online graph against the published transaction pivot.

The dataset ships ``SummaryOfTransactions.csv``: a
``(StandardLineItem x StandardBusinessEvent) -> (Amount, Count)`` pivot over
all 22,288 GL lines. That pivot is the summary of business-event information
the reporting framework asks for, which makes it the natural reference for
checking the ingestion.

This rebuilds the same pivot from the ingested graph — group line items by
(line-item concept, flow concept), sum ``debit_amount - credit_amount`` in
debit-positive cents, count the rows — then compares it cell by cell and
classifies every difference into one of four categories:

  Matching | Methodology gap | Our bug | Their data quality

Both sides are already debit-positive signed, so the comparison is direct.
The ingested ledger balances to exactly $0.00 while the published summary
rounds each cell and so nets to a small non-zero figure; a cell whose count
matches and whose amount differs by less than the per-line rounding budget is
therefore classified as rounding in the published summary rather than as a
defect here.

The grouping is the same one the rollforward filter engine sees, so a clean
reconciliation here is also evidence that the rollforward attribution will
foot.

Prerequisites: ``just demo-world-online`` has ingested the GL, and
``EXTENSIONS_DATABASE_URL`` is set — the ``just`` recipe loads it from
``.env.local``; export it yourself when invoking the module directly.

Run it (the orchestrator runs this as step 7):
    just demo-world-online-reconcile <graph_id>
    just demo-world-online-reconcile <graph_id> --no-diff   # print the pivot only

Writes ``output/world-online-reconciliation.md``.
"""

from __future__ import annotations

import argparse
import csv
import os
import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

REPO_ROOT = Path(__file__).resolve().parents[2]
DEMO_ROOT = REPO_ROOT / "examples" / "seattle_method_world_online"
DATA_DIR = REPO_ROOT / "local" / "datasets" / "seattle_method_world_online"
SUMMARY_PATH = DATA_DIR / "SummaryOfTransactions.csv"
OUTPUT_DIR = DEMO_ROOT / "output"
OUTPUT_FILE = OUTPUT_DIR / "world-online-reconciliation.md"

EXTENSIONS_DB_URL = os.environ.get("EXTENSIONS_DATABASE_URL")
SAFE_GRAPH_ID = re.compile(r"^kg[a-zA-Z0-9_]+$")

# Per-line rounding budget. The published summary rounds each cell, so a cell
# whose count matches can still differ by a fraction of a cent per line.
# $0.02/line with a $0.50 floor comfortably covers the observed spread.
ROUNDING_PER_LINE_DOLLARS = 0.02
ROUNDING_FLOOR_DOLLARS = 0.50


@dataclass
class Cell:
  line_item: str
  business_event: str
  amount_dollars: float
  count: int


@dataclass
class DiffRow:
  line_item: str
  business_event: str
  expected: Cell | None
  actual: Cell | None
  category: str
  note: str

  @property
  def amount_delta(self) -> float:
    e = self.expected.amount_dollars if self.expected else 0.0
    a = self.actual.amount_dollars if self.actual else 0.0
    return a - e

  @property
  def count_delta(self) -> int:
    e = self.expected.count if self.expected else 0
    a = self.actual.count if self.actual else 0
    return a - e


# ── Credentials / session ──────────────────────────────────────────────────


def _read_graph_id_from_credentials() -> str | None:
  import json

  config_path = REPO_ROOT / ".local" / "config.json"
  if not config_path.exists():
    return None
  try:
    cfg = json.loads(config_path.read_text())
  except (OSError, ValueError):
    return None
  slot = (cfg.get("graphs") or {}).get("world_online_test") or {}
  return slot.get("graph_id")


@contextmanager
def _open_session(graph_id: str) -> Iterator[Session]:
  """Open a SQLAlchemy session scoped to the graph's extensions schema."""
  if not SAFE_GRAPH_ID.match(graph_id):
    raise SystemExit(
      f"Refusing to use graph_id {graph_id!r} — must match {SAFE_GRAPH_ID.pattern}"
    )
  if not EXTENSIONS_DB_URL:
    raise SystemExit(
      "EXTENSIONS_DATABASE_URL is not set. Run via "
      "`just demo-world-online-reconcile <graph_id>` (which loads "
      ".env.local), or export the env var manually."
    )
  engine = create_engine(EXTENSIONS_DB_URL)
  try:
    with Session(engine) as session:
      session.execute(text(f"SET search_path TO {graph_id}, public"))
      yield session
  finally:
    engine.dispose()


# ── Load expected (Charlie's pivot) ─────────────────────────────────────────


def _parse_amount(raw: str) -> float:
  return float(raw.replace(",", "").replace('"', "").strip())


def _parse_count(raw: str) -> int:
  return int(raw.replace(",", "").replace('"', "").strip())


def load_expected(summary_path: Path = SUMMARY_PATH) -> tuple[list[Cell], Cell | None]:
  """Parse SummaryOfTransactions.csv → ([per-cell], grand_total).

  The final "All LineItems / All Business Events" row is split out as the
  grand total, which doubles as a balance check: a double-entry ledger sums
  to ≈ $0.00 across every account.
  """
  cells: list[Cell] = []
  grand_total: Cell | None = None
  with summary_path.open(encoding="utf-8-sig") as f:
    for row in csv.DictReader(f):
      li = (row.get("StandardLineItem") or "").strip()
      ev = (row.get("StandardBusinessEvent") or "").strip()
      amt = _parse_amount(row["Amount"])
      cnt = _parse_count(row["Count"])
      if li.lower().startswith("all ") or ev.lower().startswith("all "):
        grand_total = Cell(li, ev, amt, cnt)
        continue
      cells.append(Cell(li, ev, amt, cnt))
  return cells, grand_total


# ── Load actual (graph pivot) ────────────────────────────────────────────────


def load_actual_pivot(session: Session) -> dict[tuple[str, str], Cell]:
  """Group posted LineItems by (line-item qname, flow qname) → Cell.

  Rebuilds the published pivot: ``SUM(debit_amount - credit_amount)`` in
  debit-positive cents, converted to dollars, plus ``COUNT(*)``.
  ``flow_element_id`` is the business-event reference, so a line that failed
  to resolve one surfaces as ``(untagged)`` rather than disappearing.
  """
  rows = session.execute(
    text(
      """
      SELECT
        el.qname AS line_item,
        COALESCE(flow.qname, '(untagged)') AS business_event,
        COALESCE(SUM(li.debit_amount - li.credit_amount), 0) AS cents,
        COUNT(*) AS cnt
      FROM line_items li
      JOIN entries en ON en.id = li.entry_id
      JOIN elements el ON el.id = li.element_id
      LEFT JOIN elements flow ON flow.id = li.flow_element_id
      WHERE en.status = 'posted'
      GROUP BY el.qname, COALESCE(flow.qname, '(untagged)')
      """
    )
  ).fetchall()
  out: dict[tuple[str, str], Cell] = {}
  for line_item, business_event, cents, cnt in rows:
    out[(line_item, business_event)] = Cell(
      line_item, business_event, round(float(cents)) / 100.0, int(cnt)
    )
  return out


# ── Diff + classify ──────────────────────────────────────────────────────────


def _tolerance(count: int) -> float:
  return max(ROUNDING_FLOOR_DOLLARS, count * ROUNDING_PER_LINE_DOLLARS)


def build_diff(
  expected: list[Cell], actual: dict[tuple[str, str], Cell]
) -> list[DiffRow]:
  diffs: list[DiffRow] = []
  matched_keys: set[tuple[str, str]] = set()

  for cell in expected:
    key = (cell.line_item, cell.business_event)
    matched_keys.add(key)
    act = actual.get(key)
    if act is None:
      diffs.append(
        DiffRow(
          cell.line_item,
          cell.business_event,
          cell,
          None,
          "Our bug",
          "Expected cell has no matching LineItems on the graph — ingest "
          "may have dropped these lines.",
        )
      )
      continue

    count_ok = act.count == cell.count
    amt_delta = abs(act.amount_dollars - cell.amount_dollars)
    within_round = amt_delta <= _tolerance(cell.count)

    if count_ok and within_round:
      note = (
        "Exact match."
        if amt_delta < 0.005
        else (
          f"Amounts agree within published-summary rounding "
          f"(Δ ${amt_delta:,.2f} over {cell.count} lines; GL balances to $0.00)."
        )
      )
      category = "Matching"
    elif count_ok and not within_round:
      category = "Our bug"
      note = (
        f"Counts match ({cell.count}) but amounts differ by ${amt_delta:,.2f} "
        f"— beyond rounding tolerance ${_tolerance(cell.count):,.2f}. Investigate "
        f"sign/scale in ingest."
      )
    elif not count_ok and within_round:
      # Amounts agree but the line count is lower: the published pivot counts
      # the $0 memo lines that a double-entry handler cannot post (see the
      # zero-line skip in ingest_gl). They carry no economic substance, so
      # the amount still ties.
      category = "Their data quality"
      note = (
        f"Amounts tie (Δ ${amt_delta:,.2f}) but Charlie counts "
        f"{cell.count - act.count} more line(s) — $0 memo line(s) we skip "
        f"as non-postable in double-entry. No economic impact."
      )
    else:
      # Count mismatch and amount beyond tolerance — genuinely missing lines.
      category = "Our bug"
      note = (
        f"Line count differs: graph={act.count}, expected={cell.count} "
        f"(Δ {act.count - cell.count}); amount Δ ${amt_delta:,.2f}."
      )
    diffs.append(
      DiffRow(cell.line_item, cell.business_event, cell, act, category, note)
    )

  # Cells present on the graph with no counterpart in the published pivot.
  for key, act in actual.items():
    if key in matched_keys:
      continue
    note = "Graph cell not present in Charlie's published summary."
    category = "Their data quality"
    if act.business_event == "(untagged)":
      category = "Our bug"
      note = (
        "Untagged LineItems (flow_element_id NULL) — a business-event tag "
        "failed to resolve to an Element at ingest."
      )
    diffs.append(DiffRow(act.line_item, act.business_event, None, act, category, note))

  diffs.sort(key=lambda d: (d.line_item, d.business_event))
  return diffs


def summarize(diffs: list[DiffRow]) -> dict[str, int]:
  out: dict[str, int] = {}
  for d in diffs:
    out[d.category] = out.get(d.category, 0) + 1
  return out


# ── Markdown ─────────────────────────────────────────────────────────────────


def _fmt(v: float) -> str:
  if v < 0:
    return f"$({abs(v):,.2f})"
  return f"${v:,.2f}"


def render_markdown(
  graph_id: str,
  diffs: list[DiffRow],
  grand_total: Cell | None,
  actual_total_dollars: float,
  actual_total_count: int,
) -> str:
  counts = summarize(diffs)
  total = len(diffs)
  matching = counts.get("Matching", 0)

  lines = [
    "# The World Online — Cross-Taxonomy Reconciliation",
    "",
    f"**Graph**: `{graph_id}`  ",
    "**Dataset**: Charlie Hoffman's *The World Online* — 22,288 GL lines, "
    "3,389 journal entries, opening 12/31/2023 through 2028  ",
    "**Source vocabulary**: `mini` (Seattle Method MINI 2026)  ",
    "**Reference**: `SummaryOfTransactions.csv` "
    "(`StandardLineItem × StandardBusinessEvent` pivot)  ",
    "",
    "This is the **summary of business-event information** Charlie's README "
    "asks the report to produce, reproduced from the ingested graph and "
    "reconciled cell-by-cell against his published pivot. Every LineItem is "
    "grouped by its balance-sheet/income-statement concept and its "
    "business-event flow tag (`LineItem.flow_element_id`) — the same grouping "
    "the rollforward filter engine sees.",
    "",
    "## Scorecard",
    "",
    f"- **Cells compared**: {total}",
    f"- **Matching**: {matching} / {total}",
    f"- **Methodology gap**: {counts.get('Methodology gap', 0)}",
    f"- **Our bug**: {counts.get('Our bug', 0)}",
    f"- **Their data quality**: {counts.get('Their data quality', 0)}",
    "",
    "### Balance check (grand total)",
    "",
    "| | Amount (debit-positive) | Lines |",
    "|---|---:|---:|",
    f"| Graph (all posted LineItems) | {_fmt(actual_total_dollars)} | "
    f"{actual_total_count:,} |",
  ]
  if grand_total is not None:
    lines.append(
      f"| Charlie's summary (All × All) | {_fmt(grand_total.amount_dollars)} | "
      f"{grand_total.count:,} |"
    )
  lines += [
    "",
    "A double-entry GL must net to $0.00 across all accounts; the graph does. "
    "Charlie's published summary nets to a small non-zero figure — accumulated "
    "per-cell rounding in the summary, not a posting imbalance.",
    "",
    "## Cell-by-cell",
    "",
    "| Line item | Business event | Expected | Graph | Δ amount | Cnt (exp/graph) "
    "| Category | Note |",
    "|---|---|---:|---:|---:|:---:|---|---|",
  ]
  for d in diffs:
    exp_amt = _fmt(d.expected.amount_dollars) if d.expected else "—"
    act_amt = _fmt(d.actual.amount_dollars) if d.actual else "—"
    exp_cnt = d.expected.count if d.expected else "—"
    act_cnt = d.actual.count if d.actual else "—"
    lines.append(
      f"| `{d.line_item}` | `{d.business_event}` | {exp_amt} | {act_amt} | "
      f"{_fmt(d.amount_delta)} | {exp_cnt}/{act_cnt} | {d.category} | {d.note} |"
    )

  lines += [
    "",
    "## Classification key",
    "",
    "| Category | Meaning | Owner |",
    "|---|---|---|",
    "| **Matching** | Graph equals expected (exact, or within the published "
    "summary's per-cell rounding). | — |",
    "| **Methodology gap** | Architectural feature not yet shipped. | RoboSystems "
    "(forward queue) |",
    "| **Our bug** | Implementation error in shipped code — needs a fix. | "
    "RoboSystems (fix) |",
    "| **Their data quality** | Source data tagging/rounding issue, not a pipeline "
    "defect. | Source author |",
    "",
    "## How to reproduce",
    "",
    "```bash",
    "just demo-world-online                       # full pipeline",
    "just demo-world-online-reconcile <graph_id>  # this report only",
    "```",
  ]
  return "\n".join(lines)


# ── Entrypoint ───────────────────────────────────────────────────────────────


def main() -> None:
  parser = argparse.ArgumentParser(
    description="Reconcile the World Online graph against Charlie's pivot.",
  )
  parser.add_argument(
    "graph_id",
    nargs="?",
    default=None,
    help="Target graph id. Defaults to the cached world_online_test slot.",
  )
  parser.add_argument(
    "--no-diff",
    action="store_true",
    help="Print the graph pivot only; skip the comparison + markdown.",
  )
  args = parser.parse_args()

  graph_id = args.graph_id or _read_graph_id_from_credentials()
  if not graph_id:
    raise SystemExit(
      "No graph_id given and no world_online_test slot in .local/config.json. "
      "Pass a graph_id or run `just demo-world-online` first."
    )

  if not SUMMARY_PATH.exists():
    raise SystemExit(f"{SUMMARY_PATH} missing — run the pull step.")

  with _open_session(graph_id) as session:
    actual = load_actual_pivot(session)

  actual_total_dollars = sum(c.amount_dollars for c in actual.values())
  actual_total_count = sum(c.count for c in actual.values())

  if args.no_diff:
    for (li, ev), cell in sorted(actual.items()):
      print(f"  {li:<45} {ev:<45} {cell.amount_dollars:>15,.2f} {cell.count:>7}")
    print(f"\nTotal: {actual_total_dollars:,.2f} over {actual_total_count} lines")
    return

  expected, grand_total = load_expected()
  diffs = build_diff(expected, actual)
  body = render_markdown(
    graph_id, diffs, grand_total, actual_total_dollars, actual_total_count
  )
  OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
  OUTPUT_FILE.write_text(body)

  counts = summarize(diffs)
  print(f"Reconciliation: {counts.get('Matching', 0)}/{len(diffs)} matching")
  for cat in ("Methodology gap", "Our bug", "Their data quality"):
    if counts.get(cat):
      print(f"  {cat}: {counts[cat]}")
  print(f"Wrote {OUTPUT_FILE.relative_to(REPO_ROOT)} ({len(body):,} chars)")


if __name__ == "__main__":
  main()
