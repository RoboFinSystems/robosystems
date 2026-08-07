#!/usr/bin/env python3
"""Statement-level reconciliation — World Online vs the reference report.

Where ``reconcile.py`` validates the *GL ingestion* cell by cell against the
published transaction pivot, this checks the *rendered statements*: it diffs
the four-statement Report's seven anchor totals against Charlie Hoffman's
published XBRL reference instance at
``xbrlsite.azurewebsites.net/2026/reporting-framework/mini/ref-num/instance.xml``,
the same artifact his rendered report is built from.

The matcher bridges two differences between the two sides:
  * **Vocabulary** — the reference instance is in ``mini:`` concepts while the
    Report renders ``rs-gaap:``. An explicit correspondence map pairs the
    anchors, which is the mini→rs-gaap projection the whole demo exercises.
  * **Period labels** — his reference carries different period labels from
    ours, but the amounts tie, so anchors are matched by *period position*
    (latest instant = current, the one before = prior) rather than by date.

Our side is read from the exported JSON-LD bundle's ``rs:Fact`` nodes, so the
published artifact is what gets reconciled — not an internal query behind it.

Prerequisites: ``just demo-world-online`` has run through the download-bundles
step, which writes ``output/world-online.jsonld``.

Run it (the orchestrator runs this as step 11):
    just demo-world-online-statement-reconcile
    just demo-world-online-statement-reconcile --no-fetch   # use the cached reference

Writes ``output/world-online-statement-reconciliation.md``.
"""

from __future__ import annotations

import argparse
import json
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEMO_ROOT = REPO_ROOT / "examples" / "seattle_method_world_online"
BUNDLE_PATH = DEMO_ROOT / "output" / "world-online.jsonld"
OUTPUT_FILE = DEMO_ROOT / "output" / "world-online-statement-reconciliation.md"
DATA_DIR = REPO_ROOT / "local" / "datasets" / "seattle_method_world_online"
REF_INSTANCE_CACHE = DATA_DIR / "ref-num-instance.xml"
REF_INSTANCE_URL = "https://xbrlsite.azurewebsites.net/2026/reporting-framework/mini/ref-num/instance.xml"

XBRLI_NS = "http://www.xbrl.org/2003/instance"

# Anchor correspondence: (mini local name, rs-gaap qname, label). The seven
# headline totals that tie the rendered statements to the reference report.
ANCHORS: list[tuple[str, str, str]] = [
  ("Assets", "rs-gaap:Assets", "Total Assets"),
  (
    "LiabilitiesAndEquity",
    "rs-gaap:LiabilitiesAndStockholdersEquity",
    "Total Liabilities & Equity",
  ),
  ("NetIncomeLoss", "rs-gaap:NetIncomeLoss", "Net Income (Loss)"),
  ("Receivables", "rs-gaap:ReceivablesNetCurrent", "Receivables"),
  (
    "PropertyPlantAndEquipment",
    "rs-gaap:PropertyPlantAndEquipmentNet",
    "Property, Plant & Equipment",
  ),
  ("LongtermDebt", "rs-gaap:LongTermDebtAndCapitalLeaseObligations", "Long-term Debt"),
  (
    "CashAndCashEquivalents",
    "rs-gaap:CashAndCashEquivalentsAtCarryingValue",
    "Cash & Equivalents",
  ),
]

ROUNDING_TOLERANCE = 0.50  # the published summary carries per-cell rounding


@dataclass
class Row:
  label: str
  expected_current: float | None
  actual_current: float | None
  expected_prior: float | None
  actual_prior: float | None

  def delta_current(self) -> float:
    e = self.expected_current or 0.0
    a = self.actual_current or 0.0
    return a - e


# ── Expected (Charlie's instance.xml) ───────────────────────────────────────


def _fetch_reference(no_fetch: bool) -> Path:
  if REF_INSTANCE_CACHE.exists() and no_fetch:
    return REF_INSTANCE_CACHE
  DATA_DIR.mkdir(parents=True, exist_ok=True)
  try:
    with urllib.request.urlopen(REF_INSTANCE_URL, timeout=30) as resp:
      REF_INSTANCE_CACHE.write_bytes(resp.read())
  except Exception as exc:
    if REF_INSTANCE_CACHE.exists():
      print(f"  ⚠️  fetch failed ({exc}); using cached {REF_INSTANCE_CACHE.name}")
    else:
      raise SystemExit(f"Could not fetch reference instance: {exc}") from exc
  return REF_INSTANCE_CACHE


def _load_expected(path: Path) -> dict[str, tuple[float | None, float | None]]:
  """{mini_local: (current, prior)} — current = latest-dated context."""
  # A trusted artifact fetched over HTTPS from the publisher, not user
  # content, so the stdlib XML parser is appropriate. Switch to defusedxml if
  # the input source ever widens.
  root = ET.parse(path).getroot()  # noqa: S314
  ctx_date: dict[str, str] = {}
  for c in root.findall(f"{{{XBRLI_NS}}}context"):
    cid = c.get("id") or ""
    period = c.find(f"{{{XBRLI_NS}}}period")
    if period is None:
      continue
    inst = period.find(f"{{{XBRLI_NS}}}instant")
    end = period.find(f"{{{XBRLI_NS}}}endDate")
    ctx_date[cid] = (inst if inst is not None else end).text or ""  # type: ignore[union-attr]

  wanted = {mini for mini, _, _ in ANCHORS}
  by_concept: dict[str, list[tuple[str, float]]] = {}
  for el in root:
    local = el.tag.split("}")[-1]
    cref = el.get("contextRef")
    if local in wanted and cref and el.text:
      by_concept.setdefault(local, []).append((ctx_date.get(cref, ""), float(el.text)))

  out: dict[str, tuple[float | None, float | None]] = {}
  for local, vals in by_concept.items():
    vals.sort(key=lambda x: x[0])  # ascending date
    current = vals[-1][1] if vals else None
    prior = vals[-2][1] if len(vals) > 1 else None
    out[local] = (current, prior)
  return out


# ── Actual (our exported bundle) ────────────────────────────────────────────


def _load_actual(path: Path) -> dict[str, tuple[float | None, float | None]]:
  """{rs-gaap qname: (current, prior)} from the bundle's rs:Fact nodes."""
  doc = json.loads(path.read_text())
  # JSON-LD compaction can emit a bare root object with no @graph envelope
  # when the graph has a single named root, so fall back to one node.
  graph = doc.get("@graph", [doc])
  period_date: dict[str, str] = {}
  for n in graph:
    if n.get("@type") == "rs:Period":
      period_date[n["@id"]] = str(n.get("instant") or n.get("endDate") or "")

  wanted = {rs for _, rs, _ in ANCHORS}
  by_concept: dict[str, list[tuple[str, float]]] = {}
  for n in graph:
    if n.get("@type") != "rs:Fact":
      continue
    el = str(n.get("element", ""))
    if el in wanted and n.get("numericValue") is not None:
      d = period_date.get(str(n.get("period")), "")
      by_concept.setdefault(el, []).append((d, float(n["numericValue"])))

  out: dict[str, tuple[float | None, float | None]] = {}
  for qname, vals in by_concept.items():
    vals.sort(key=lambda x: x[0])
    out[qname] = (
      vals[-1][1] if vals else None,
      vals[-2][1] if len(vals) > 1 else None,
    )
  return out


# ── Diff + markdown ─────────────────────────────────────────────────────────


def _fmt(v: float | None) -> str:
  if v is None:
    return "—"
  return f"$({abs(v):,.2f})" if v < 0 else f"${v:,.2f}"


def build_rows(
  expected: dict[str, tuple[float | None, float | None]],
  actual: dict[str, tuple[float | None, float | None]],
) -> list[Row]:
  rows: list[Row] = []
  for mini, rs, label in ANCHORS:
    ec, ep = expected.get(mini, (None, None))
    ac, ap = actual.get(rs, (None, None))
    rows.append(Row(label, ec, ac, ep, ap))
  return rows


def render(graph_bundle: Path, rows: list[Row]) -> str:
  matching = sum(
    1
    for r in rows
    if r.expected_current is not None
    and r.actual_current is not None
    and abs(r.delta_current()) <= ROUNDING_TOLERANCE
  )
  lines = [
    "# The World Online — Statement-Level Reconciliation",
    "",
    (
      "Diffs our rendered four-statement Report's **anchor totals** against "
      + "Charlie Hoffman's **published XBRL reference instance** "
      + "(`mini/ref-num/instance.xml`, the source of his `index2.html`). Complements "
      + "the GL-pivot `reconcile.py` (which validates ingestion against "
      + "`SummaryOfTransactions.csv`) at the rendered-statement level."
    ),
    "",
    (
      "Our values are read from the **v2 graph-native bundle** "
      + f"(`{graph_bundle.name}` — `rs:Fact` nodes). Charlie's instance is labelled "
      + "FY2022 (EUR); ours spans 2023→2028 — amounts are matched by **period "
      + "position** (latest = current), since they tie regardless of the label."
    ),
    "",
    "## Scorecard",
    "",
    f"- **Anchors compared**: {len(rows)}",
    f"- **Matching (current period, within ${ROUNDING_TOLERANCE:.2f})**: {matching} / {len(rows)}",
    "",
    "| Anchor | Charlie (current) | Ours (current) | Δ | Charlie (prior) | Ours (prior) | |",
    "|---|---:|---:|---:|---:|---:|:---:|",
  ]
  for r in rows:
    tie = (
      "✓"
      if r.expected_current is not None
      and r.actual_current is not None
      and abs(r.delta_current()) <= ROUNDING_TOLERANCE
      else "✗"
    )
    lines.append(
      f"| {r.label} | {_fmt(r.expected_current)} | {_fmt(r.actual_current)} | "
      f"{_fmt(r.delta_current())} | {_fmt(r.expected_prior)} | {_fmt(r.actual_prior)} | {tie} |"
    )
  lines += [
    "",
    (
      "A ✓ means the rendered statement ties to Charlie's published reference for "
      + "that anchor (within the published summary's per-cell rounding). The negative "
      + "ending cash is **in Charlie's reference report too** — it is a property of the "
      + "source dataset's tagging (financing cash legs tagged operating), not an "
      + 'ingestion error; see `README.md` §"Cash flow statement".'
    ),
    "",
    "## How to reproduce",
    "",
    "```bash",
    "just demo-world-online                                 # full pipeline",
    "just demo-world-online-statement-reconcile             # this report only",
    "```",
  ]
  return "\n".join(lines) + "\n"


def main() -> None:
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument(
    "--no-fetch",
    action="store_true",
    help="use the cached reference instance instead of re-fetching.",
  )
  args = parser.parse_args()

  if not BUNDLE_PATH.exists():
    raise SystemExit(
      f"{BUNDLE_PATH} missing — run `just demo-world-online` first "
      "(the download-bundles step writes it)."
    )

  ref = _fetch_reference(args.no_fetch)
  expected = _load_expected(ref)
  actual = _load_actual(BUNDLE_PATH)
  rows = build_rows(expected, actual)
  body = render(BUNDLE_PATH, rows)
  OUTPUT_FILE.write_text(body)

  matching = sum(
    1
    for r in rows
    if r.expected_current is not None
    and r.actual_current is not None
    and abs(r.delta_current()) <= ROUNDING_TOLERANCE
  )
  print(f"Statement reconciliation: {matching}/{len(rows)} anchors tie")
  for r in rows:
    print(f"  {r.label:32s} Δ {r.delta_current():+,.2f}")
  print(f"Wrote {OUTPUT_FILE.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
  main()
