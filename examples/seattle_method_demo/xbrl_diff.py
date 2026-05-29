#!/usr/bin/env python3
"""XBRL emit-side reconciliation — Step 9 of the Seattle Method demo.

Companion to :mod:`reconcile`. Where ``reconcile.py`` validates the
*ingest path* (Charlie's GL → our DB matches what Charlie published),
this script validates the *emit path*: ingest the same GL → publish a
Report → emit it as XBRL → diff vs Charlie's published ``instance.xml``
at fact level.

Together the two close the round-trip loop end-to-end:

    Charlie's data ──ingest──► our DB ──reconcile──► matches Charlie's facts
                                  │
                                  └──emit──► our XBRL ──xbrl-diff──► matches Charlie's XBRL

Charlie's reference uses the ``mini:`` taxonomy; our published Report
uses ``rs-gaap:``. Concept-by-concept identity holds across the two
namespaces for the common BS/IS concepts (CashAndCashEquivalents,
NetIncomeLoss, StockholdersEquity, etc.), so we diff on the
**concept local-name** (the part after the colon) rather than the full
qname. Any mismatches surface explicitly in the output.

Pulls the latest filed Report for the target graph, fetches its XBRL
zip via the published Python SDK
(``LedgerClient.download_report_bundle``), unzips the instance.xml in
memory, and writes a markdown diff to
``output/seattle-method-case-1-xbrl-diff.md``.

Usage:
    uv run python -m examples.seattle_method_demo.xbrl_diff <graph_id>
    uv run python -m examples.seattle_method_demo.xbrl_diff  # uses cached
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from robosystems_client.clients import LedgerClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

REPO_ROOT = Path(__file__).resolve().parents[2]
DEMO_ROOT = REPO_ROOT / "examples" / "seattle_method_demo"
DEFAULT_REPORT_PATH = DEMO_ROOT / "output" / "seattle-method-case-1-xbrl-diff.md"
EXPECTED_INSTANCE_PATH = (
  REPO_ROOT / "local" / "datasets" / "seattle_method" / "report" / "instance.xml"
)
EXTENSIONS_DB_URL = os.environ.get("EXTENSIONS_DATABASE_URL")

# XBRL 2003 instance namespace — used for context / period / entity
# elements in both our emit and Charlie's reference (both forms are
# the standard XBRL XML serialization, no trailing # — that variant is
# the RDF representation our JSON-LD encoder uses internally, not what
# our XBRL emitter outputs). Concept elements carry their own taxonomy
# namespace (rs-gaap vs mini); we project both down to local names for
# diff identity.
NS_XBRLI = "{http://www.xbrl.org/2003/instance}"

# Our published taxonomy concept namespace (for printing the side-by-side
# qnames in the diff table; the diff itself runs on local-name only).
NS_RS_GAAP = "{https://robosystems.ai/taxonomy/rs-gaap/v1/}"
NS_MINI = "{http://www.xbrlsite.com/mini}"


# ── Dataclasses ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class FactKey:
  """The diff key: concept local-name + period kind.

  Period date is intentionally NOT part of the key — our seattle_method
  demo emits a Q1 2024 Report (period_end 2024-03-31) while Charlie's
  reference reports FY 2024 (period_end 2024-12-31). All 14 JEs are in
  Q1, so the balances and durations match in value — only the period
  labels differ. Keying on (concept, period_kind) gives us a useful
  diff; the actual period_end appears in the display columns so the
  human can verify alignment.
  """

  local_name: str
  period_kind: str  # "instant" or "duration"


@dataclass(frozen=True)
class FactValue:
  """The diff value: raw qname + value in cents + period_end (display).

  ``period_end`` is informational — used in the markdown table so the
  reader can spot when our and Charlie's periods don't line up.
  """

  qname: str
  value_cents: int
  period_end: date


# ── Config + report lookup ─────────────────────────────────────────────


def _load_config() -> dict:
  cfg_path = REPO_ROOT / ".local" / "config.json"
  if not cfg_path.exists():
    raise SystemExit("Missing .local/config.json — run `just demo-user` first.")
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


def _latest_report_id(graph_id: str) -> str:
  """Look up the most-recent Report id for this graph by querying the
  extensions DB directly. Same boundary ``reconcile.py`` crosses for
  similar DB-side lookups; revisited if/when a GraphQL "latest report
  for graph" surface lands."""
  if not EXTENSIONS_DB_URL:
    raise SystemExit(
      "EXTENSIONS_DATABASE_URL not set. The justfile sets it from "
      ".env.local; if invoking outside the justfile, export it yourself."
    )
  engine = create_engine(EXTENSIONS_DB_URL)
  SessionLocal = sessionmaker(bind=engine, autoflush=False)
  with SessionLocal() as session:
    row = (
      session.execute(
        text(
          f"SET search_path TO {graph_id}; "
          "SELECT id FROM reports "
          "WHERE bundle_url IS NOT NULL "
          "ORDER BY created_at DESC LIMIT 1"
        )
      )
      .mappings()
      .first()
    )
  if row is None:
    raise SystemExit(
      f"No filed Report with a stamped bundle found for graph {graph_id}. "
      "Run `just demo-seattle-method-create-report` first."
    )
  return str(row["id"])


# ── Instance.xml parsing (reused for both our + Charlie's) ─────────────


def _parse_instance_facts(instance_bytes: bytes) -> dict[FactKey, FactValue]:
  """Parse an XBRL 2.1 instance.xml into a {FactKey: FactValue} dict.

  Two passes:
  1. Walk ``<context>`` elements → ``{context_id: (kind, period_end)}``.
  2. Walk direct children of ``<xbrl>`` that carry ``contextRef`` +
     have numeric text → emit one ``FactKey → FactValue`` entry per
     concept-period pair.

  Diff key is ``(local_name, period_kind, period_end)`` so cross-
  taxonomy concept identity (``mini:Foo`` vs ``rs-gaap:Foo``) holds
  on local-name. Values are stored as cents to avoid float drift.

  Both inputs are trusted artifacts — our emit is what the bundle
  producer just generated; Charlie's reference is what
  ``pull_expected_report.sh`` curl'd from xbrlsite.com. Neither is
  user-supplied content, so the stdlib XML parser is appropriate
  (same precedent as ``reconcile.py``). Swap to ``defusedxml`` if
  the input source ever widens.
  """
  root = ET.fromstring(instance_bytes)  # noqa: S314
  contexts: dict[str, tuple[str, date]] = {}
  for ctx in root.findall(f"{NS_XBRLI}context"):
    ctx_id = ctx.get("id")
    if not ctx_id:
      continue
    period = ctx.find(f"{NS_XBRLI}period")
    if period is None:
      continue
    instant = period.find(f"{NS_XBRLI}instant")
    end = period.find(f"{NS_XBRLI}endDate")
    if instant is not None and instant.text:
      contexts[ctx_id] = ("instant", date.fromisoformat(instant.text.strip()))
    elif end is not None and end.text:
      contexts[ctx_id] = ("duration", date.fromisoformat(end.text.strip()))

  out: dict[FactKey, FactValue] = {}
  for elem in root:
    ctx_ref = elem.get("contextRef")
    if not ctx_ref or ctx_ref not in contexts:
      continue
    raw = (elem.text or "").strip()
    if not raw:
      continue
    try:
      dollars = float(raw)
    except ValueError:
      continue
    # Clark-notation tag → (namespace, local) → strip namespace.
    tag = elem.tag
    if tag.startswith("{") and "}" in tag:
      ns_uri, local = tag[1:].split("}", 1)
      qname = f"{_short_prefix(ns_uri)}:{local}"
    else:
      qname = tag
      local = tag
    kind, period_end = contexts[ctx_ref]
    key = FactKey(local, kind)
    # When multiple contexts of the same period_kind tag the same
    # concept (e.g. comparative BS with prior-period instants), keep
    # the latest by period_end so the diff against Charlie's reference
    # picks the most-recent value he reports.
    existing = out.get(key)
    if existing is None or period_end > existing.period_end:
      out[key] = FactValue(qname, round(dollars * 100), period_end)
  return out


def _short_prefix(ns_uri: str) -> str:
  """Human-readable short prefix for a concept namespace. Used only for
  the side-by-side qname column in the markdown table — diff identity
  runs on local-name."""
  if ns_uri == "http://www.xbrlsite.com/mini":
    return "mini"
  if ns_uri.rstrip("/").endswith("/rs-gaap/v1"):
    return "rs-gaap"
  return ns_uri.rsplit("/", 1)[-1]


# ── Diff classification ────────────────────────────────────────────────


@dataclass
class DiffRow:
  """One row of the side-by-side diff."""

  key: FactKey
  ours: FactValue | None
  charlies: FactValue | None
  status: str  # 'match', 'value_mismatch', 'ours_only', 'charlies_only'


def _classify(
  ours: dict[FactKey, FactValue], charlies: dict[FactKey, FactValue]
) -> list[DiffRow]:
  rows: list[DiffRow] = []
  all_keys = sorted(
    set(ours) | set(charlies),
    key=lambda k: (k.period_kind, k.local_name),
  )
  for key in all_keys:
    o = ours.get(key)
    c = charlies.get(key)
    if o is not None and c is not None:
      status = "match" if o.value_cents == c.value_cents else "value_mismatch"
    elif o is not None:
      status = "ours_only"
    else:
      status = "charlies_only"
    rows.append(DiffRow(key=key, ours=o, charlies=c, status=status))
  return rows


# ── Markdown output ────────────────────────────────────────────────────


def _format_cents(c: int) -> str:
  return f"{c / 100:,.2f}"


def _render_markdown(
  graph_id: str,
  report_id: str,
  ours_count: int,
  charlies_count: int,
  rows: list[DiffRow],
) -> str:
  """Build the side-by-side markdown diff."""
  matches = [r for r in rows if r.status == "match"]
  mismatches = [r for r in rows if r.status == "value_mismatch"]
  ours_only = [r for r in rows if r.status == "ours_only"]
  charlies_only = [r for r in rows if r.status == "charlies_only"]

  shared_keys = len(matches) + len(mismatches)
  pct = 100 * len(matches) / shared_keys if shared_keys else 0

  lines: list[str] = [
    "# Seattle Method — XBRL emit-side reconciliation (Test Case 1)",
    "",
    f"Graph: `{graph_id}`",
    f"Report: `{report_id}`",
    "",
    "## Summary",
    "",
    f"- **Our emit**: {ours_count} facts (rs-gaap)",
    f"- **Charlie's reference**: {charlies_count} facts (mini)",
    f"- **Shared (concept × period)**: {shared_keys}",
    f"- **Exact value match**: {len(matches)} / {shared_keys} ({pct:.1f}%)",
    f"- **Value mismatch**: {len(mismatches)}",
    f"- **Our emit only**: {len(ours_only)}",
    f"- **Charlie's reference only**: {len(charlies_only)}",
    "",
    "## Notes",
    "",
    "Charlie's reference uses the `mini:` taxonomy; ours uses `rs-gaap:`. "
    "Diff identity is on concept **local-name** so cross-taxonomy parity "
    "holds for the canonical BS/IS concepts (the seeded mini→rs-gaap "
    "derivation mappings handle the namespace projection upstream in "
    "`reconcile.py` step 7). The columns below show the full qname from "
    "each side so the namespace difference is visible.",
    "",
    "Values are dollars (the underlying XBRL `decimals='INF'` integer amounts ÷ 100).",
    "",
  ]

  def _table(title: str, subset: list[DiffRow]) -> list[str]:
    out = [f"## {title} ({len(subset)})", ""]
    if not subset:
      out.append("_None._\n")
      return out
    out += [
      "| Kind | Concept | Ours (rs-gaap) | Ours period | Charlie's (mini) | Charlie's period | Δ |",
      "|---|---|---:|:--|---:|:--|---:|",
    ]
    for r in subset:
      ours_str = _format_cents(r.ours.value_cents) if r.ours else "—"
      ours_period = r.ours.period_end.isoformat() if r.ours else "—"
      charlies_str = _format_cents(r.charlies.value_cents) if r.charlies else "—"
      charlies_period = r.charlies.period_end.isoformat() if r.charlies else "—"
      delta = ""
      if r.ours and r.charlies:
        diff = r.ours.value_cents - r.charlies.value_cents
        delta = "0" if diff == 0 else f"{diff / 100:+,.2f}"
      out.append(
        f"| {r.key.period_kind} | `{r.key.local_name}` | "
        f"{ours_str} | {ours_period} | "
        f"{charlies_str} | {charlies_period} | {delta} |"
      )
    out.append("")
    return out

  lines += _table("Exact matches", matches)
  lines += _table("Value mismatches", mismatches)
  lines += _table("Our emit only", ours_only)
  lines += _table("Charlie's reference only", charlies_only)
  return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────


def main() -> None:
  parser = argparse.ArgumentParser(
    description=(
      "Diff our XBRL emit against Charlie's published instance.xml for "
      "Seattle Method Test Case 1."
    )
  )
  parser.add_argument(
    "graph_id",
    nargs="?",
    default=None,
    help="Target graph id. Defaults to the cached seattle_method_test entry.",
  )
  parser.add_argument(
    "--report-id",
    default=None,
    help="Report id to diff. Defaults to the most recent filed Report on the graph.",
  )
  parser.add_argument(
    "--out",
    type=Path,
    default=DEFAULT_REPORT_PATH,
    help=f"Output markdown path (default: {DEFAULT_REPORT_PATH.relative_to(REPO_ROOT)}).",
  )
  args = parser.parse_args()

  cfg = _load_config()
  base_url = cfg.get("base_url", "http://localhost:8000")
  api_key = cfg["api_key"]
  graph_id = _resolve_graph_id(cfg, args.graph_id)
  report_id = args.report_id or _latest_report_id(graph_id)

  print(f"Diffing XBRL emit for graph={graph_id}, report={report_id}")
  print(f"Charlie's reference: {EXPECTED_INSTANCE_PATH.relative_to(REPO_ROOT)}")

  # Pull our XBRL via the published SDK.
  client = LedgerClient(
    {"base_url": base_url, "token": api_key, "headers": {}, "timeout": 60}
  )
  ours_zip = client.download_report_bundle(graph_id, report_id, format="xbrl-2.1")
  print(f"  Downloaded our XBRL: {len(ours_zip.content):,} bytes")

  with zipfile.ZipFile(io.BytesIO(ours_zip.content)) as zf:
    our_instance_bytes = zf.read("instance.xml")
  ours = _parse_instance_facts(our_instance_bytes)
  print(f"  Our facts: {len(ours)}")

  if not EXPECTED_INSTANCE_PATH.exists():
    raise SystemExit(
      f"Charlie's instance.xml missing at {EXPECTED_INSTANCE_PATH}. "
      "Run pull_expected_report.sh first."
    )
  charlies = _parse_instance_facts(EXPECTED_INSTANCE_PATH.read_bytes())
  print(f"  Charlie's facts: {len(charlies)}")

  rows = _classify(ours, charlies)
  markdown = _render_markdown(graph_id, report_id, len(ours), len(charlies), rows)
  args.out.parent.mkdir(parents=True, exist_ok=True)
  args.out.write_text(markdown)
  print(f"\nWrote {args.out.relative_to(REPO_ROOT)}")

  matches = sum(1 for r in rows if r.status == "match")
  shared = sum(1 for r in rows if r.status in {"match", "value_mismatch"})
  if shared:
    print(f"\nExact-match rate: {matches}/{shared} ({100 * matches / shared:.1f}%)")


if __name__ == "__main__":
  try:
    main()
  except KeyboardInterrupt:
    sys.exit(130)
