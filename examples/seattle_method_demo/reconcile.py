#!/usr/bin/env python3
"""Reconciliation harness — Step 7 of the Seattle Method demo.

Reads concept totals + rollforward IBs via the **GraphQL facade**
(`LedgerClient.get_trial_balance`, `LedgerClient.list_information_blocks`),
parses the rollforward IB's typed mechanics, invokes the Phase 2
MVP filter engine for each rollforward against a direct DB session,
computes the four anchor totals (Total Assets, Total Liabilities &
Equity, Net Income, Net Cash Change), and writes a markdown report
to ``examples/seattle_method_demo/output/seattle-method-case-1.md``.

**Architecture note** — this is hand-written GraphQL consumption.
Forward work in [`python-client-graphql.md`](../../local/docs/specs/python-client-graphql.md)
replaces the hand-written Python GraphQL query strings + ``dict[str, Any]``
returns with codegen-generated Pydantic models via ``ariadne-codegen``.
The trigger condition the spec calls out ("a new GraphQL surface that
the Python client genuinely needs to consume — e.g. rollforward
RollforwardMechanics") has now fired. Phase 1 of that work
(~1 day) replaces every ``client.list_information_blocks(...)
→ dict`` hop here with a typed Pydantic response.

The filter engine itself still runs against a direct SQLAlchemy
session — that's the Phase 2 MVP boundary documented in
[`information-block.md`](../../local/docs/specs/information-block.md) §4.5.
Phase 3 wires filter evaluation into ``build_envelope`` so the
attributed facts arrive populated in the IB envelope; this script
becomes purely API-driven at that point.

Usage:
    uv run python -m examples.seattle_method_demo.reconcile <graph_id>
    uv run python -m examples.seattle_method_demo.reconcile <graph_id> --no-diff
"""

from __future__ import annotations

import argparse
import csv
import os
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from robosystems.models.api.information_block import RollforwardMechanics
from robosystems.operations.roboledger.reports.rollforward_filters import (
  AttributedFact,
  evaluate_attribution_filters,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DEMO_ROOT = REPO_ROOT / "examples" / "seattle_method_demo"
DEFAULT_REPORT_PATH = DEMO_ROOT / "output" / "seattle-method-case-1.md"
EXPECTED_FACTS_PATH = DEMO_ROOT / "fixtures" / "expected_facts_mini.csv"
# Filter engine needs a direct SQLAlchemy session (Phase 2 MVP boundary
# per ``information-block.md`` §4.5; Phase 3 wires it into
# ``build_envelope`` so we can drop this entirely). Reads
# ``EXTENSIONS_DATABASE_URL`` from the process env — the
# ``just demo-seattle-method-reconcile`` recipe loads ``.env.local``
# which sets this. If you invoke ``reconcile.py`` outside the justfile,
# export the var yourself.
EXTENSIONS_DB_URL = os.environ.get("EXTENSIONS_DATABASE_URL")
SAFE_GRAPH_ID = re.compile(r"^kg[a-zA-Z0-9_]+$")


@dataclass
class ConceptTotal:
  """Sum of (debit - credit) on one concept across a period."""

  qname: str
  label: str
  balance_type: str  # 'debit' | 'credit'
  period_type: str  # 'instant' | 'duration'
  trait: str | None
  debit_positive_cents: int


@dataclass
class RollforwardResult:
  """One rollforward IB's filter engine output for a period."""

  bs_qname: str
  bs_label: str
  delta_cents: int  # ΔBS over the period (debit-positive)
  attributed: list[AttributedFact]
  residual_cents: int


@dataclass
class ExpectedFact:
  """One concept value from Charlie's published luca.pacioli.ai export."""

  concept: str  # e.g. "mini:CashAndCashEquivalents"
  period_label: str  # raw "12/31/24" or "2024-01-01 | 2024-12-31"
  value_cents: int  # dollars from CSV × 100
  fact_id: str


@dataclass
class ReconciliationLine:
  """One line of the automated diff against Charlie's published facts."""

  concept: str
  our_cents: int
  expected_cents: int

  @property
  def delta_cents(self) -> int:
    return self.our_cents - self.expected_cents

  @property
  def status(self) -> str:
    if self.delta_cents == 0:
      return "match"
    return "delta"


@dataclass
class ReconciliationReport:
  graph_id: str
  period_start: date
  period_end: date
  concept_totals: list[ConceptTotal]
  rollforward_results: list[RollforwardResult]
  diff_lines: list[ReconciliationLine] = field(default_factory=list)
  diff_summary: dict[str, int] = field(default_factory=dict)


# ── DB queries ───────────────────────────────────────────────────────────


def _read_graph_id_from_credentials() -> str | None:
  """Read the ``seattle_method_test`` graph slot from ``.local/config.json``.

  Returns ``None`` if the file or slot doesn't exist — caller surfaces
  a friendly "run the orchestrator first" message. The slot is saved
  by ``main.step_provision_graph`` via
  ``examples.credentials.utils.save_graph_id``.
  """
  import json

  config_path = Path(".local/config.json")
  if not config_path.exists():
    return None
  try:
    cfg = json.loads(config_path.read_text())
  except (OSError, json.JSONDecodeError):
    return None
  slot = (cfg.get("graphs") or {}).get("seattle_method_test") or {}
  return slot.get("graph_id")


def _get_ledger_client():
  """Construct a LedgerClient from saved credentials.

  Mirrors the helper used across the demo scripts. The GraphQL
  facade methods (``get_trial_balance``, ``list_information_blocks``)
  hang off this client.
  """
  import json

  from robosystems_client.clients.ledger_client import LedgerClient

  config_path = Path(".local/config.json")
  if not config_path.exists():
    raise SystemExit("Missing .local/config.json — run `just demo-user` first.")
  with config_path.open() as f:
    cfg = json.load(f)
  return LedgerClient(
    {
      "base_url": cfg.get("base_url", "http://localhost:8000"),
      "token": cfg["api_key"],
    }
  )


def _open_session(graph_id: str) -> Session:
  """Open a SQLAlchemy session scoped to the graph's schema.

  ``search_path`` is set so all subsequent queries against
  ``elements`` / ``line_items`` / ``entries`` / ``structures`` land
  in the right tenant schema. graph_id is validated against a strict
  regex before interpolation (SET search_path doesn't accept bind
  parameters).
  """
  if not SAFE_GRAPH_ID.match(graph_id):
    raise SystemExit(
      f"Refusing to use graph_id {graph_id!r} — must match {SAFE_GRAPH_ID.pattern}"
    )
  if not EXTENSIONS_DB_URL:
    raise SystemExit(
      "EXTENSIONS_DATABASE_URL is not set. Run via "
      "`just demo-seattle-method-reconcile <graph_id>` (which loads "
      ".env.local), or export the env var manually."
    )
  engine = create_engine(EXTENSIONS_DB_URL)
  session_factory = sessionmaker(bind=engine)
  session = session_factory()
  session.execute(text(f"SET search_path TO {graph_id}, public"))
  return session


def _load_concept_totals_via_graphql(
  client, graph_id: str, period_start: date, period_end: date
) -> list[ConceptTotal]:
  """Load per-concept period totals via the GraphQL trialBalance query.

  Replaces the prior direct-DB query against ``line_items``. The
  ``trialBalance`` resolver does the same aggregation server-side
  (Σ DR, Σ CR, net per account) — which means this script consumes
  the same query surface a frontend or MCP agent would.

  Returns ``ConceptTotal`` shapes scoped to ``mini:`` qnames only;
  rs-gaap and other taxonomies on the graph are filtered out
  client-side (the GraphQL query returns every CoA account).

  The trialBalance response shape per row:
  ``{account_id, account_code (=qname), account_name, trait,
  account_type, total_debits, total_credits, net_balance}``.
  Values come back as floats in dollars; we multiply by 100 for
  internal cents-precision.

  TODO: replace with codegen-generated typed response model when
  ``python-client-graphql.md`` Phase 1 lands.
  """
  data = client.get_trial_balance(
    graph_id,
    start_date=period_start.isoformat(),
    end_date=period_end.isoformat(),
  )
  rows = (data or {}).get("rows", []) or []

  totals: list[ConceptTotal] = []
  for r in rows:
    # ``account_code`` carries the mini qname (set at load time —
    # see ``load_taxonomy.build_element_payloads``).
    qname = r.get("account_code") or ""
    if not qname.startswith("mini:"):
      continue
    dollars_dr = float(r.get("total_debits") or 0)
    dollars_cr = float(r.get("total_credits") or 0)
    # Filter on "had activity" not "non-zero net" — Charlie's
    # Receivables has $8K DR + $8K CR netting to $0, but it's
    # legitimate activity that the diff should compare.
    if dollars_dr == 0 and dollars_cr == 0:
      continue
    debit_positive_cents = int(round((dollars_dr - dollars_cr) * 100))
    # trialBalance doesn't return period_type or balance_type
    # directly — they live on the Element row. Derive period_type
    # from trait (instant for asset/liability/equity, duration for
    # revenue/expense). For balance_type, infer from trait + sign.
    trait = r.get("trait")
    period_type = _period_type_from_trait(trait)
    balance_type = _balance_type_from_trait(trait)
    totals.append(
      ConceptTotal(
        qname=qname,
        label=r.get("account_name") or qname,
        balance_type=balance_type,
        period_type=period_type,
        trait=trait,
        debit_positive_cents=debit_positive_cents,
      )
    )
  totals.sort(key=lambda c: c.qname)
  return totals


def _period_type_from_trait(trait: str | None) -> str:
  """Instant for BS concepts (asset/liability/equity), duration for IS."""
  if trait in ("asset", "liability", "equity", "contraAsset", "contraLiability"):
    return "instant"
  return "duration"


def _balance_type_from_trait(trait: str | None) -> str:
  """Debit for asset/expense/loss; credit for liability/equity/revenue/income/gain."""
  if trait in ("asset", "contraLiability", "contraEquity", "expense", "loss"):
    return "debit"
  return "credit"


def _load_expected_facts(csv_path: Path) -> list[ExpectedFact]:
  """Parse Charlie's luca.pacioli.ai export → list of ExpectedFact.

  The CSV ships with a UTF-8 BOM and uses pipe-separated multi-value
  columns for the entity identifier and the calendar period aspect.
  We only care about ``Concept``, ``FactValue``, ``CalendarPeriodAspect``,
  and ``FactID`` — the rest is irrelevant for the anchor-total diff.

  Values come in whole dollars in the export (rounding=INF means
  no decimal places); we multiply by 100 for cents-internal storage
  so comparison against our LineItem.amount_cents (already in cents)
  is direct.
  """
  out: list[ExpectedFact] = []
  with csv_path.open(encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    for row in reader:
      raw_value = (row.get("FactValue") or "").strip()
      if not raw_value:
        continue
      try:
        dollars = float(raw_value)
      except ValueError:
        # Non-numeric facts (text blocks, etc.) — skip for diff.
        continue
      out.append(
        ExpectedFact(
          concept=row["Concept"],
          period_label=row["CalendarPeriodAspect"],
          value_cents=int(round(dollars * 100)),
          fact_id=row["FactID"],
        )
      )
  return out


def _build_diff(
  ours: list[ConceptTotal],
  expected: list[ExpectedFact],
  period_label_for_instants: str,
  period_label_for_durations: str,
) -> list[ReconciliationLine]:
  """Diff our concept totals against Charlie's expected facts.

  Charlie publishes both instant (e.g. ``12/31/24`` BS positions)
  and duration (e.g. ``2024-01-01 | 2024-12-31`` IS / CF flows)
  facts. We pick the matching period label per concept based on
  ``period_type`` so we compare apples-to-apples.

  Our debit-positive convention means we need to sign-flip credit-
  balance instant concepts (liabilities, equity) and credit-balance
  duration concepts (revenues) to match Charlie's presentation
  values, which are always positive when the concept is naturally
  growing.
  """
  expected_by_concept_period: dict[tuple[str, str], int] = {}
  for f in expected:
    expected_by_concept_period.setdefault((f.concept, f.period_label), f.value_cents)

  diff_lines: list[ReconciliationLine] = []
  for c in ours:
    period_label = (
      period_label_for_instants
      if c.period_type == "instant"
      else period_label_for_durations
    )
    key = (c.qname, period_label)
    if key not in expected_by_concept_period:
      continue

    # Sign convention: Charlie's published values are
    # presentation-positive (assets positive when grown, liabilities
    # positive when grown). Ours are debit-positive (credit-balance
    # concepts come in negative). Sign-flip credit-balance concepts
    # so the diff is meaningful.
    our_signed = (
      c.debit_positive_cents
      if c.balance_type == "debit"
      else -c.debit_positive_cents
    )
    diff_lines.append(
      ReconciliationLine(
        concept=c.qname,
        our_cents=our_signed,
        expected_cents=expected_by_concept_period[key],
      )
    )
  return diff_lines


def _summarize_diff(lines: list[ReconciliationLine]) -> dict[str, int]:
  return {
    "lines_compared": len(lines),
    "exact_match": sum(1 for ln in lines if ln.status == "match"),
    "delta": sum(1 for ln in lines if ln.status == "delta"),
    "total_abs_delta_cents": sum(abs(ln.delta_cents) for ln in lines),
  }


def _derive_trait_from_concept(
  qname: str, balance_type: str | None, period_type: str | None
) -> str | None:
  """Mirror ``load_taxonomy._derive_trait`` — instant+debit → asset;
  instant+credit → liability (or equity by name match);
  duration+debit → expense; duration+credit → revenue."""
  if not balance_type or not period_type:
    return None
  if period_type == "instant":
    name_lower = qname.lower()
    equity_markers = (
      "paidincapital",
      "retainedearnings",
      "commonstock",
      "preferredstock",
      "treasurystock",
    )
    if any(m in name_lower for m in equity_markers):
      return "equity"
    return "asset" if balance_type == "debit" else "liability"
  return "revenue" if balance_type == "credit" else "expense"


def _load_rollforward_ibs_via_graphql(
  client, graph_id: str
) -> list[tuple[str, RollforwardMechanics]]:
  """Return [(structure_id, RollforwardMechanics), …] via GraphQL.

  Uses ``LedgerClient.list_information_blocks(block_type='rollforward')``
  — the same query the frontend / MCP / any external client would
  hit. The envelope's ``artifact.mechanics`` field carries the
  typed ``RollforwardMechanics`` JSON; we re-validate it through
  Pydantic to recover the typed shape (the hand-written GraphQL
  parser returns ``dict[str, Any]``, the documented
  ``python-client-graphql.md`` Phase 1 work replaces that with
  typed Pydantic on the response).

  Note: the IB envelope's ``facts`` field is empty in Phase 2 MVP
  (see ``information_block/rollforward.py:build_envelope``). The
  filter engine still runs separately to produce attributed facts —
  Phase 3 wires that into the envelope so this script can drop the
  direct-session step entirely.

  TODO: replace with codegen-generated typed response model when
  ``python-client-graphql.md`` Phase 1 lands.
  """
  blocks = client.list_information_blocks(graph_id, block_type="rollforward")
  out: list[tuple[str, RollforwardMechanics]] = []
  for block in blocks or []:
    artifact = block.get("artifact") or {}
    raw_mechanics = artifact.get("mechanics")
    if not raw_mechanics:
      continue
    mechanics = RollforwardMechanics.model_validate(raw_mechanics)
    out.append((block.get("id", ""), mechanics))
  return out


def _evaluate_all_rollforwards(
  session: Session,
  rollforwards: list[tuple[str, RollforwardMechanics]],
  period_start: date,
  period_end: date,
  bs_label_by_qname: dict[str, str],
) -> list[RollforwardResult]:
  """Run the filter engine for every rollforward IB; collect results."""
  results: list[RollforwardResult] = []
  for _structure_id, mech in rollforwards:
    facts = evaluate_attribution_filters(session, mech, period_start, period_end)
    # Compute the BS delta from the facts (matching + residual sum =
    # ΔBS by construction in residual_as_default mode).
    delta = sum(f.value_cents for f in facts)
    residual = sum(f.value_cents for f in facts if f.is_residual)
    results.append(
      RollforwardResult(
        bs_qname=mech.bs_source_qname,
        bs_label=bs_label_by_qname.get(mech.bs_source_qname, mech.bs_source_qname),
        delta_cents=delta,
        attributed=[f for f in facts if not f.is_residual],
        residual_cents=residual,
      )
    )
  return results


# ── Anchor totals ────────────────────────────────────────────────────────


def _anchor_totals(concepts: list[ConceptTotal]) -> dict[str, int]:
  """Compute the four anchor totals from concept-level period sums.

  - **Total Assets** = sum of debit-positive instant + asset concepts
    (period activity, not ending balance — for Q1 2024 with no
    opening balances this equals ending positions).
  - **Total Liabilities & Equity** = sum of credit-positive instant
    liability/equity concepts **plus implicit Retained Earnings**.
    The accounting equation requires ``Assets = Liabilities +
    Equity``, where Equity = PaidInCapital + RetainedEarnings, and
    RetainedEarnings = opening RE + Net Income − Dividends. Charlie's
    lemonade-stand fixture starts from zero balances and posts no
    closing entry to RE during the period, so RE-ending = Net Income.
    We add Net Income to L&E to close the accounting equation
    (otherwise L&E < Assets by exactly the period's net income).
  - **Net Income** = revenues less expenses (duration concepts).
  - **Net Cash Change** = delta on ``mini:CashAndCashEquivalents``.

  All in debit-positive cents internally; sign-flipped for
  presentation where convention requires it.
  """
  totals = {
    "Total Assets": 0,
    "Total Liabilities & Equity": 0,
    "Net Income": 0,
    "Net Cash Change": 0,
  }

  for c in concepts:
    if c.trait == "asset" and c.period_type == "instant":
      totals["Total Assets"] += c.debit_positive_cents
    elif c.trait in ("liability", "equity") and c.period_type == "instant":
      # Credit-balance — flip sign so the total is presented as
      # positive when liabilities/equity grew over the period.
      totals["Total Liabilities & Equity"] += -c.debit_positive_cents
    elif c.period_type == "duration":
      if c.trait == "revenue":
        # Revenues are credit-balance; positive net = inflow.
        totals["Net Income"] += -c.debit_positive_cents
      elif c.trait == "expense":
        # Expenses reduce income.
        totals["Net Income"] += -c.debit_positive_cents

    if c.qname == "mini:CashAndCashEquivalents":
      totals["Net Cash Change"] = c.debit_positive_cents

  # Implicit Retained Earnings: Net Income flows into RE at period-end.
  # Without it, the accounting equation doesn't close for a startup's
  # first-period reconciliation (no opening RE to inherit).
  totals["Total Liabilities & Equity"] += totals["Net Income"]

  return totals


# ── Markdown rendering ──────────────────────────────────────────────────


def _fmt_cents(cents: int) -> str:
  """Format integer cents as ``$X,XXX.XX`` (negative → parentheses)."""
  dollars = cents / 100.0
  if dollars < 0:
    return f"$({abs(dollars):,.2f})"
  return f"${dollars:,.2f}"


def render_markdown(report: ReconciliationReport) -> str:
  """Build the markdown reconciliation report."""
  out: list[str] = []
  out.append("# Seattle Method Cross-Taxonomy — Test Case 1 Reconciliation")
  out.append("")
  out.append(f"**Graph**: `{report.graph_id}`")
  out.append(f"**Period**: {report.period_start} → {report.period_end}")
  out.append(f"**Dataset**: Charlie Hoffman's lemonade-stand 14-JE Q1 2024 fixture")
  out.append(f"**Expected output reference**: "
             "[luca.pacioli.ai/luca/view/0f24fd35…](https://luca.pacioli.ai/luca/view/0f24fd35e961e167a727b663c75a4c5ec9fb7eb86730d6292f46e6e180fc2018980cd52e/index)")
  out.append("")
  out.append("---")
  out.append("")

  # Auto-diff against Charlie's published facts
  if report.diff_lines:
    summary = report.diff_summary
    out.append("## Automated Diff vs. Charlie's Published Facts")
    out.append("")
    out.append(
      f"Compared **{summary['lines_compared']}** concept(s) against "
      f"Charlie's luca.pacioli.ai export "
      f"(`fixtures/expected_facts_mini.csv`). "
      f"**{summary['exact_match']} exact match** • "
      f"**{summary['delta']} delta**. "
      f"Total absolute delta: **{_fmt_cents(summary['total_abs_delta_cents'])}**."
    )
    out.append("")
    out.append("| Concept | Our value | Charlie's value | Δ | |")
    out.append("|---|---:|---:|---:|---|")
    for ln in report.diff_lines:
      mark = "✓" if ln.status == "match" else "⚠️"
      out.append(
        f"| `{ln.concept}` | {_fmt_cents(ln.our_cents)} | "
        f"{_fmt_cents(ln.expected_cents)} | "
        f"{_fmt_cents(ln.delta_cents)} | {mark} |"
      )
    out.append("")

  # Anchor totals
  out.append("## Four Anchor Totals")
  out.append("")
  out.append("Methodology spec §4.6 exit criterion: these four lines must "
             "match Charlie's PoC for the test to pass. All amounts are "
             "debit-positive cents internally; presentation flips signs "
             "per accounting convention.")
  out.append("")
  totals = _anchor_totals(report.concept_totals)
  out.append("| Anchor | Our value |")
  out.append("|---|---:|")
  for label, cents in totals.items():
    out.append(f"| {label} | {_fmt_cents(cents)} |")
  out.append("")

  # Concept-level period totals
  out.append("## Concept-Level Period Totals")
  out.append("")
  out.append("Every mini concept with non-zero activity in the period. "
             "``Δ debit-positive`` is the period flow (Σ DR − Σ CR). For "
             "instant/asset concepts this equals the period-ending "
             "balance (Charlie's data starts from zero). For duration "
             "concepts this is the period income/expense.")
  out.append("")
  out.append("| QName | Label | Trait | Period | Δ debit-positive |")
  out.append("|---|---|---|---|---:|")
  for c in report.concept_totals:
    if c.debit_positive_cents == 0:
      continue
    out.append(
      f"| `{c.qname}` | {c.label} | {c.trait or '—'} | "
      f"{c.period_type} | {_fmt_cents(c.debit_positive_cents)} |"
    )
  out.append("")

  # Rollforward attribution detail
  out.append("## Rollforward Attribution (Phase 2 MVP Filter Engine)")
  out.append("")
  out.append("Each rollforward IB decomposes its BS source's period "
             "delta across declared TDC filters. Where ``Σ filters == "
             "Δ BS``, the rollforward is balanced (residual = 0). A "
             "non-zero residual indicates either an unattributed flow "
             "or a phantom TDC in the source data (logged at author time).")
  out.append("")
  for rf in report.rollforward_results:
    out.append(f"### {rf.bs_label} ({rf.bs_qname})")
    out.append("")
    out.append(f"**Δ BS** (debit-positive): {_fmt_cents(rf.delta_cents)}")
    if rf.residual_cents:
      out.append(f"**Residual**: {_fmt_cents(rf.residual_cents)}")
    out.append("")
    if not rf.attributed:
      out.append("_No attributed facts (no matching activity in period)._")
      out.append("")
      continue
    out.append("| Flow concept | Value | Matched lines | Event ids |")
    out.append("|---|---:|---:|---|")
    for f in rf.attributed:
      events = ", ".join(f.event_ids[:3])
      if len(f.event_ids) > 3:
        events += f", … (+{len(f.event_ids) - 3})"
      out.append(
        f"| `{f.target_qname}` | {_fmt_cents(f.value_cents)} | "
        f"{f.matched_line_count} | {events or '—'} |"
      )
    out.append("")

  # Findings
  out.append("## Findings — Classification per Methodology §3.2")
  out.append("")
  out.append("**Their data quality** (source CSV inconsistencies):")
  out.append("")
  out.append("- **JE-205** — Description \"Payment for contractor\" but "
             "TDC on the AP line is `mini:PurchasesInventoryForSaleOnAccount`. "
             "Contractor services aren't inventory; vocabulary misuse.")
  out.append("- **JE-209** — TDC `mini:PaymentOfInterest` was a typo for "
             "`mini:PaymentInterest` (the canonical mini.xsd concept name). "
             "Fixed at source in `fixtures/transactions.csv` prior to "
             "ingest. Note that `mini:DecreaseFromPaymentOfInterest` (the "
             "AccruedExpenses-side TDC) keeps the \"Of\" — Charlie's "
             "naming is internally inconsistent.")
  out.append("- **JE-226** — Income tax accrual ($400) but TDC is "
             "`mini:InterestAccrued` instead of `IncomeTaxAccrued`. "
             "Copy-paste-style bug from the JE-210 interest pattern.")
  out.append("")
  out.append("**Methodology gap** (architecturally aligned, semantically distinct):")
  out.append("")
  out.append("- **JE-225** — \"Write off of PPE\" with `Amount = 0` on "
             "both lines. Boundary test case. Our GL handler rejects "
             "nil-amount entries (`must have non-zero D or C`); Charlie's "
             "system likely creates `$0` facts. Reconciliation delta is "
             "`$0` either way; the four anchor totals are unaffected.")
  out.append("- **rs-gaap library subset** — Two flow concepts in "
             "`mappings.py` don't exist in our currently-loaded rs-gaap "
             "library: `rs-gaap:InterestPaidNet` (mapped through to the "
             "closest available `rs-gaap:InterestExpense`) and "
             "`rs-gaap:StockIssuedDuringPeriodValueNewIssues` (mapped "
             "through to `rs-gaap:ProceedsFromIssuanceOfCommonStock`). "
             "Approximation; future library expansion closes the gap.")
  out.append("")
  out.append("**Our bug**: none identified.")
  out.append("")
  out.append("**Matching**: see Anchor Totals table above + line-by-line "
             "concept totals. Compare manually against Charlie's PoC "
             "rendering at the expected-output URL — automated HTML diff "
             "is a forward-queue enhancement (methodology §3.1 step 5 "
             "stretch goal).")
  out.append("")

  # Footer
  out.append("---")
  out.append("")
  out.append("*Reconciliation produced by "
             "`examples/seattle_method_demo/reconcile.py` against the "
             "Phase 2 MVP rollforward filter engine. See "
             "`examples/seattle_method_demo/README.md` for the full "
             "methodology and `local/docs/specs/cross-taxonomy-projection.md` "
             "for the architectural pattern this test validates.*")

  return "\n".join(out) + "\n"


# ── Main ─────────────────────────────────────────────────────────────────


def main() -> None:
  parser = argparse.ArgumentParser(
    description="Reconcile the Seattle Method Test Case 1 demo against "
    "Charlie Hoffman's expected output. Produces a markdown report.",
  )
  parser.add_argument(
    "graph_id",
    nargs="?",
    default=None,
    help=(
      "The graph id to reconcile. If omitted, reads the "
      "``seattle_method_test`` slot from ``.local/config.json`` "
      "(populated by ``just demo-seattle-method``)."
    ),
  )
  parser.add_argument(
    "--period-start",
    type=date.fromisoformat,
    default=date(2024, 1, 1),
    help="Period start (default 2024-01-01).",
  )
  parser.add_argument(
    "--period-end",
    type=date.fromisoformat,
    default=date(2024, 1, 31),
    help="Period end (default 2024-01-31).",
  )
  # The diff-period labels are deliberately separate from
  # --period-start/--period-end because Charlie's PoC reports BS
  # positions at his calendar-year-end ("12/31/24") and IS/CF flows
  # across the full calendar year ("2024-01-01 | 2024-12-31") even
  # though the underlying JEs are only in January. Other test cases
  # (us-gaap, IFRS) will publish facts with different period labels;
  # they need their own values here. Defaults match Charlie's mini
  # CSV export format for Test Case 1.
  parser.add_argument(
    "--diff-label-instant",
    default="12/31/24",
    help=(
      "Period label expected on Charlie's instant facts (BS positions). "
      "Default matches his mini CSV export for Test Case 1; override for "
      "other test cases."
    ),
  )
  parser.add_argument(
    "--diff-label-duration",
    default="2024-01-01 | 2024-12-31",
    help=(
      "Period label expected on Charlie's duration facts (IS / CF flows). "
      "Default matches his mini CSV export for Test Case 1; override for "
      "other test cases."
    ),
  )
  parser.add_argument(
    "--out",
    type=Path,
    default=DEFAULT_REPORT_PATH,
    help="Output markdown report path.",
  )
  parser.add_argument(
    "--expected-facts",
    type=Path,
    default=EXPECTED_FACTS_PATH,
    help="Charlie's luca.pacioli.ai mini facts export (CSV). Skip with --no-diff.",
  )
  parser.add_argument(
    "--no-diff",
    action="store_true",
    help="Skip the automated diff against Charlie's expected facts.",
  )
  args = parser.parse_args()

  # Fall back to the orchestrator's saved graph slot if not passed
  # explicitly — mirrors how ``just demo-roboledger`` reads from
  # ``.local/config.json`` without requiring the caller to remember
  # the graph id.
  graph_id = args.graph_id or _read_graph_id_from_credentials()
  if not graph_id:
    raise SystemExit(
      "No graph_id provided and no ``seattle_method_test`` slot found "
      "in ``.local/config.json``. Run ``just demo-seattle-method`` first "
      "(to provision a graph + save the slot), then re-run reconcile."
    )

  print(f"Reconciling graph {graph_id} for {args.period_start}..{args.period_end}")

  client = _get_ledger_client()

  # Read paths go through GraphQL (the same surface a frontend or
  # MCP agent would hit). The filter engine is still invoked
  # against a direct session — Phase 2 MVP boundary; Phase 3 wires
  # the engine into ``build_envelope`` so the IB envelope returns
  # populated facts and this script becomes pure-API.
  concepts = _load_concept_totals_via_graphql(
    client, graph_id, args.period_start, args.period_end
  )
  print(f"  Loaded {len(concepts)} concept(s) with period activity (via GraphQL trialBalance)")
  bs_label_by_qname = {c.qname: c.label for c in concepts}

  rollforwards = _load_rollforward_ibs_via_graphql(client, graph_id)
  print(
    f"  Loaded {len(rollforwards)} rollforward IB(s) (via GraphQL informationBlocks)"
  )

  # Filter engine still runs against a direct session — Phase 2 MVP.
  session = _open_session(graph_id)
  try:
    results = _evaluate_all_rollforwards(
      session, rollforwards, args.period_start, args.period_end, bs_label_by_qname
    )
    for r in results:
      print(
        f"    {r.bs_qname:<40} Δ={_fmt_cents(r.delta_cents)} "
        f"attributed={len(r.attributed)} residual={_fmt_cents(r.residual_cents)}"
      )
  finally:
    session.close()

  report = ReconciliationReport(
    graph_id=graph_id,
    period_start=args.period_start,
    period_end=args.period_end,
    concept_totals=concepts,
    rollforward_results=results,
  )

  if not args.no_diff and args.expected_facts.exists():
    expected = _load_expected_facts(args.expected_facts)
    print(f"\nLoaded {len(expected)} expected fact(s) from {args.expected_facts.name}")
    # Match our instant concepts to Charlie's instant-period label;
    # duration concepts to his duration-period label. Defaults match
    # Charlie's mini CSV export for Test Case 1; CLI override the
    # labels for other test cases.
    report.diff_lines = _build_diff(
      ours=concepts,
      expected=expected,
      period_label_for_instants=args.diff_label_instant,
      period_label_for_durations=args.diff_label_duration,
    )
    # Also compare our computed anchor totals to Charlie's published
    # aggregates. These are concepts Charlie publishes as facts but
    # our pipeline computes as scalars (no direct LineItem activity).
    totals = _anchor_totals(concepts)
    expected_by_key = {(f.concept, f.period_label): f.value_cents for f in expected}
    anchor_pairs = [
      ("mini:Assets", args.diff_label_instant, totals["Total Assets"]),
      (
        "mini:LiabilitiesAndEquity",
        args.diff_label_instant,
        totals["Total Liabilities & Equity"],
      ),
      ("mini:NetIncomeLoss", args.diff_label_duration, totals["Net Income"]),
      ("mini:NetCashFlow", args.diff_label_duration, totals["Net Cash Change"]),
    ]
    for concept, period_label, our_value in anchor_pairs:
      key = (concept, period_label)
      if key in expected_by_key:
        report.diff_lines.append(
          ReconciliationLine(
            concept=concept,
            our_cents=our_value,
            expected_cents=expected_by_key[key],
          )
        )
    report.diff_summary = _summarize_diff(report.diff_lines)
    print(
      f"  Diff: {report.diff_summary['exact_match']}/"
      f"{report.diff_summary['lines_compared']} exact match • "
      f"{report.diff_summary['delta']} delta • "
      f"|Δ|={_fmt_cents(report.diff_summary['total_abs_delta_cents'])}"
    )
  elif args.no_diff:
    print("\nSkipping diff (--no-diff)")
  else:
    print(f"\nExpected facts CSV not found at {args.expected_facts}; skipping diff")

  args.out.parent.mkdir(parents=True, exist_ok=True)
  args.out.write_text(render_markdown(report))
  print(f"\n✓ Report written → {args.out}")

  totals = _anchor_totals(concepts)
  print("\nFour anchor totals:")
  for label, cents in totals.items():
    print(f"  {label:<30} {_fmt_cents(cents)}")


if __name__ == "__main__":
  main()
