"""Report fact generation and structure rendering.

1. generate_report_facts() — mapped trial balance -> structure-agnostic
   ReportFacts (one per element x period), persisted for materialization.
2. render_structure_view() — applies a structure's hierarchy to those facts,
   computing subtotals and ordering for display.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.api.extensions import cents_to_dollars
from robosystems.operations.roboledger.entry_status import (
  landed_entry_bindparam,
)
from robosystems.operations.roboledger.reports.calc_dag import (
  load_rs_gaap_calculations,
  resolve_calc_dag,
  topo_sort_calculations,
)

# ── Data classes ──────────────────────────────────────────────────────────


@dataclass
class ReportFact:
  """A discrete financial data point — structure-agnostic."""

  element_id: str
  element_qname: str
  element_name: str
  classification: str
  balance_type: str
  value: float  # natural-sign dollars
  period_start: date
  period_end: date
  period_type: str  # "duration" or "instant"
  # Detail fact already absorbed into a synthesized parent (e.g. PP&E Gross
  # + Accumulated Depreciation -> PPE Net). Kept for the CF derivation but
  # excluded from rendering so it doesn't double-count in an ancestor.
  audit_only: bool = False
  # The portion of ``value`` the RE/NI close may count. An account mapped to
  # N concepts yields N facts; only its primary target carries the close
  # contribution (others 0) so each posting counts once in net income.
  # ``None`` means "same as value".
  close_value: float | None = None


@dataclass
class PeriodSpec:
  """A reporting period column specification."""

  start: date
  end: date
  label: str


@dataclass
class ReportFacts:
  """All facts generated for a report."""

  facts: list[ReportFact]
  periods: list[PeriodSpec]
  unmapped_count: int
  taxonomy_id: str
  mapping_id: str


@dataclass
class FactRow:
  """One line in a rendered financial statement."""

  element_id: str
  element_qname: str
  element_name: str
  classification: str
  balance_type: str
  values: list[float | None]  # one per period column
  is_subtotal: bool = False
  is_abstract: bool = False
  depth: int = 0


@dataclass
class FactGrid:
  """Rendered financial statement — facts viewed through a structure."""

  structure_id: str
  structure_name: str
  block_type: str
  periods: list[PeriodSpec]
  rows: list[FactRow] = field(default_factory=list)
  unmapped_count: int = 0


# ── Stage 1: Generate facts (structure-agnostic) ─────────────────────────


def _arc_type_for_taxonomy(session: Session, taxonomy_id: str) -> str:
  """CoA->target arc type to walk: always ``mapping`` (CoA -> rs-gaap leaf).

  A hook for per-taxonomy dispatch should a tenant taxonomy need
  ``equivalence``-direct rendering.
  """
  return "mapping"


def generate_report_facts(
  session: Session,
  taxonomy_id: str,
  mapping_id: str,
  periods: list[PeriodSpec],
  close_target_qname: str = "rs-gaap:RetainedEarningsAccumulatedDeficit",
) -> ReportFacts:
  """Generate structure-agnostic facts for all mapped elements across periods.

  ``close_target_qname`` is the equity concept cumulative earnings close to —
  the Reporting Style's earnings home (``PartnersCapital`` / ``MembersEquity``
  for PART/LLC) — so earnings land on the form's capital line and the BS foots.
  """
  arc_type = _arc_type_for_taxonomy(session, taxonomy_id)
  facts: list[ReportFact] = []

  for period in periods:
    balances = _read_mapped_balances(
      session, mapping_id, period.start, period.end, arc_type=arc_type
    )
    for balance in balances.values():
      facts.append(
        ReportFact(
          element_id=balance.element_id,
          element_qname=balance.qname,
          element_name=balance.name,
          classification=balance.classification,
          balance_type=balance.balance_type,
          value=_natural_sign(balance.net_balance, balance.balance_type),
          period_start=period.start,
          period_end=period.end,
          period_type=_infer_period_type(balance.classification),
          close_value=(
            _natural_sign(balance.close_net_balance, balance.balance_type)
            if balance.close_net_balance is not None
            else None
          ),
        )
      )

    # Seed zero facts for mapped equity targets with no postings yet, so
    # the close finds the RE-shaped target instead of falling back to APIC.
    _append_empty_equity_facts(
      session,
      mapping_id,
      facts,
      period.start,
      period.end,
      arc_type=arc_type,
      close_target_qname=close_target_qname,
    )

    # No-op where real closing entries already zeroed rev/exp.
    _close_to_retained_earnings(
      facts, period.start, period.end, close_target_qname=close_target_qname
    )

    # Runs from inception: real closing entries already zeroed what they
    # closed, so cumulative rev - exp is only the still-unclosed portion.
    _close_prior_periods_to_retained_earnings(
      session,
      mapping_id,
      facts,
      period.start,
      period.end,
      arc_type=arc_type,
      close_target_qname=close_target_qname,
    )

  # NetIncomeLoss as its own fact: the IS bottom line and the first child
  # of the CF Operating rollup.
  _emit_net_income_facts(session, facts, periods)

  # PPE Net = Gross - AccumulatedDepreciation for gross + contra mappings;
  # skipped when a direct PPE Net fact exists.
  _synthesize_ppe_net_facts(session, facts, periods)

  # Investing/financing CF facts from per-line flow concepts. Must run
  # before _derive_cash_flow_facts so its "direct fact wins" guard skips
  # the ΔBS derivation for these leaves.
  _emit_flow_facts(session, facts, periods, mapping_id, arc_type)

  # Balances the day before each period starts, where that is not the previous
  # column's end (year-over-year, YTD, rolling). The cash flow measures change
  # from a period's opening, not from whichever column precedes it. Kept out of
  # ``facts`` so they are never rendered.
  opening_facts = _load_opening_facts(session, mapping_id, periods, arc_type)

  # Operating CF from BS deltas (indirect method); needs every period's BS.
  _derive_cash_flow_facts(session, facts, periods, opening_facts)

  # Book ΔCash - Investing - Financing - (NI + DDA + ΔWC) on an operating
  # leaf so the CF foots to actual cash. Must precede the subtotal roll-up.
  _reconcile_operating_to_cash(session, facts, periods, opening_facts)

  # Calc-DAG subtotals as facts, so rules scoped to a subtotal can bind.
  # Runs after every leaf and derived fact is in place.
  _emit_subtotal_facts(session, facts, periods)

  # Warning-only: a mismatch flags incomplete investing/financing attribution.
  _check_cash_flow_tie_out(facts, periods, opening_facts)

  unmapped_count = _count_unmapped(session, mapping_id, arc_type=arc_type)

  return ReportFacts(
    facts=facts,
    periods=periods,
    unmapped_count=unmapped_count,
    taxonomy_id=taxonomy_id,
    mapping_id=mapping_id,
  )


# ── Stage 2: Render structure view ───────────────────────────────────────


def render_structure_view(
  session: Session,
  facts: list[ReportFact],
  block_type: str,
  periods: list[PeriodSpec],
  reporting_style_id: str,
) -> FactGrid:
  """Apply the Reporting Style's structure for ``block_type`` to ``facts``.

  Facts outside the hierarchy roll up via ``general-special`` arcs to their
  nearest in-structure ancestor; facts with none are dropped from the view
  (still persisted for audit).
  """
  (
    structure_id,
    structure_name,
    concept_arrangement,
    hierarchy,
  ) = _load_reporting_structure(session, block_type, reporting_style_id)

  if not hierarchy:
    return FactGrid(
      structure_id=structure_id or "",
      structure_name=structure_name or "",
      block_type=block_type,
      periods=periods,
    )

  in_structure = _collect_hierarchy_element_ids(hierarchy)
  rolled_up = _roll_up_facts_to_structure(session, facts, in_structure)

  period_balances = [_facts_to_balance_dict(rolled_up, p.start, p.end) for p in periods]

  # ``arithmetic`` Disclosures compose calcs across taxonomies: every calc
  # whose subtotal is in the element set. Others use the structure's own.
  if concept_arrangement == "arithmetic":
    calculations = _load_calculations(session, element_ids=in_structure)
  else:
    calculations = _load_calculations(session, structure_id=structure_id)

  # Facts are already natural-signed.
  rows = _build_rows(hierarchy, period_balances, calculations, pre_signed=True)

  return FactGrid(
    structure_id=structure_id,
    structure_name=structure_name,
    block_type=block_type,
    periods=periods,
    rows=rows,
  )


# ── Ancestor rollup ────────────────────────────────────────────────────────


def _collect_hierarchy_element_ids(hierarchy: list[_HierarchyNode]) -> set[str]:
  ids: set[str] = set()

  def _walk(node: _HierarchyNode) -> None:
    ids.add(node.element_id)
    for child in node.children:
      _walk(child)

  for root in hierarchy:
    _walk(root)
  return ids


def _resolve_renderable_ancestor(
  session: Session,
  element_id: str,
  in_structure: set[str],
  cache: dict[str, str | None],
) -> str | None:
  """Nearest ancestor of ``element_id`` in ``in_structure``, or ``None``.

  Walks ``equivalence``, ``mapping`` and ``general-special`` arcs together
  breadth-first, so the nearest ancestor wins when several paths exist.
  """
  if element_id in cache:
    return cache[element_id]
  if element_id in in_structure:
    cache[element_id] = element_id
    return element_id

  visited: set[str] = {element_id}
  frontier: list[str] = [element_id]

  while frontier:
    # Arc directions differ: ``general-special`` points parent -> child;
    # ``mapping`` / ``equivalence`` point child (CoA) -> anchor.
    parent_rows = session.execute(
      text(
        """
        SELECT DISTINCT a.from_element_id AS parent_id
        FROM associations a
        WHERE a.association_type = 'general-special'
          AND a.to_element_id = ANY(:children)
        UNION
        SELECT DISTINCT a.to_element_id AS parent_id
        FROM associations a
        WHERE a.association_type IN ('mapping', 'equivalence')
          AND a.from_element_id = ANY(:children)
        """
      ),
      {"children": frontier},
    ).fetchall()

    next_frontier: list[str] = []
    for r in parent_rows:
      pid = r.parent_id
      if pid in visited:
        continue
      visited.add(pid)
      if pid in in_structure:
        cache[element_id] = pid
        return pid
      next_frontier.append(pid)
    frontier = next_frontier

  cache[element_id] = None
  return None


def _roll_up_facts_to_structure(
  session: Session,
  facts: list[ReportFact],
  in_structure: set[str],
) -> list[ReportFact]:
  """Repoint out-of-structure facts at their nearest in-structure ancestor.

  Facts with no ancestor are dropped. No aggregation here —
  ``_facts_to_balance_dict`` sums facts sharing an element_id.
  """
  if not facts:
    return facts

  # An in-structure element with its own fact for a period is authoritative
  # (as in ``_build_rows``); detail rolling up onto it would double-count
  # (e.g. APIC + RE onto StockholdersEquity in the equity roll-forward).
  # Keyed per period.
  direct_keys = {
    (f.element_id, f.period_start, f.period_end)
    for f in facts
    if not f.audit_only and f.element_id in in_structure
  }

  cache: dict[str, str | None] = {}
  rolled: list[ReportFact] = []
  for fact in facts:
    if fact.audit_only:
      continue
    if fact.element_id in in_structure:
      rolled.append(fact)
      continue
    ancestor = _resolve_renderable_ancestor(
      session, fact.element_id, in_structure, cache
    )
    if ancestor is None:
      continue
    if (ancestor, fact.period_start, fact.period_end) in direct_keys:
      continue
    rolled.append(
      ReportFact(
        element_id=ancestor,
        element_qname=fact.element_qname,
        element_name=fact.element_name,
        classification=fact.classification,
        balance_type=fact.balance_type,
        value=fact.value,
        period_start=fact.period_start,
        period_end=fact.period_end,
        period_type=fact.period_type,
      )
    )
  return rolled


# ── Internal helpers ───────────────────────────────────────────────────────


@dataclass
class _Balance:
  """Aggregated balance for a reporting element."""

  element_id: str
  qname: str
  name: str
  classification: str
  balance_type: str
  total_debits: float
  total_credits: float
  net_balance: float
  # The portion of ``net_balance`` from sources for which this target is
  # the close-primary mapping (see ``ReportFact.close_value``).
  close_net_balance: float | None = None


@dataclass
class _HierarchyNode:
  """Node in the reporting structure tree."""

  element_id: str
  qname: str
  name: str
  classification: str
  balance_type: str
  is_abstract: bool
  depth: int
  children: list[_HierarchyNode] = field(default_factory=list)


def _facts_to_balance_dict(
  facts: list[ReportFact],
  period_start: date,
  period_end: date,
) -> dict[str, _Balance]:
  """One period's facts as balances, summed per element_id.

  Values are already natural-signed; ``balance_type="debit"`` makes
  ``_natural_sign`` pass them through unchanged.
  """
  balances: dict[str, _Balance] = {}
  for fact in facts:
    if fact.period_start != period_start or fact.period_end != period_end:
      continue
    existing = balances.get(fact.element_id)
    if existing is None:
      balances[fact.element_id] = _Balance(
        element_id=fact.element_id,
        qname=fact.element_qname,
        name=fact.element_name,
        classification=fact.classification,
        balance_type="debit",  # natural sign already applied
        total_debits=0.0,
        total_credits=0.0,
        net_balance=fact.value,  # already natural-sign
      )
    else:
      existing.net_balance += fact.value
  return balances


# Equity-reducing flows (dividends, distributions, buybacks): own lines on
# SE / CF, but cumulatively they net out of RE on the BS. They carry no
# 'equity' classification trait, so they're detected by qname.
_EQUITY_FLOW_REDUCER_QNAMES: frozenset[str] = frozenset(
  {
    "rs-gaap:PaymentsOfDividends",
    "rs-gaap:PaymentsOfDividendsCommonStock",
    "rs-gaap:PaymentsOfDividendsPreferredStockAndPreferenceStock",
    "rs-gaap:PaymentsForRepurchaseOfCommonStock",
    "rs-gaap:PaymentsForRepurchaseOfEquity",
    "rs-gaap:PaymentsForRepurchaseOfPreferredStockAndPreferenceStock",
    "rs-gaap:DistributionsMade",
    "rs-gaap:DistributionsMadeToLimitedLiabilityCompanyLlcMember",
  }
)


def _is_equity_flow_reducer(qname: str | None) -> bool:
  return qname in _EQUITY_FLOW_REDUCER_QNAMES


def _infer_classification(qname: str | None, balance_type: str | None) -> str | None:
  """Classification from qname + balance_type for elements lacking SFAC 6 traits.

  Without it the close can't compute net income and the BS doesn't balance.
  Only used when the trait join returned NULL.
  """
  if not qname:
    return None
  qn = qname.lower()
  bt = (balance_type or "").lower()

  if bt == "credit" and any(t in qn for t in ("revenue", "sales")):
    return "revenue"
  if bt == "debit" and any(
    t in qn for t in ("expense", "cost", "loss", "depreciation", "amortization")
  ):
    return "expense"
  # Equity before liability.
  if bt == "credit" and any(
    t in qn for t in ("equity", "capital", "retainedearnings", "stockholder")
  ):
    return "equity"
  if bt == "debit" and "asset" in qn:
    return "asset"
  if bt == "credit" and "liabilit" in qn:
    return "liability"

  # qname-only fallback for rollup elements whose balance_type doesn't match
  # what they aggregate (e.g. fac:LiabilitiesRollUp is ``debit``).
  if "liabilit" in qn and ("equity" in qn or "stockholder" in qn or "capital" in qn):
    return None  # combined L+E rollup — not a pure classification
  if any(t in qn for t in ("equity", "capital", "retainedearnings", "stockholder")):
    return "equity"
  if "liabilit" in qn:
    return "liability"
  if "asset" in qn:
    return "asset"
  if any(t in qn for t in ("revenue", "sales")):
    return "revenue"
  if any(t in qn for t in ("expense", "cost", "loss")):
    return "expense"
  return None


def _read_mapped_balances(
  session: Session,
  mapping_id: str,
  period_start: date,
  period_end: date,
  arc_type: str = "mapping",
) -> dict[str, _Balance]:
  """Mapped trial balance per reporting target (same join as /trial-balance/mapped).

  Windowing keys off the target's ``period_type``, not its SFAC 6 trait
  (which can be NULL or contra-*): instant concepts load cumulatively
  through ``period_end``; duration concepts load only the period's activity.

  A source mapped to several targets contributes its full postings to each
  target's ``net_balance``, but its close contribution
  (``close_net_balance``) to exactly one primary target — trait-classified
  over inferred, qname tiebreak — so the RE/NI close counts each posting once.
  """
  result = session.execute(
    text("""
      SELECT
        source_elem.id AS source_id,
        target.id AS reporting_element_id,
        target.qname,
        target.name AS reporting_name,
        tcls.identifier AS classification,
        target.balance_type,
        COALESCE(SUM(li.debit_amount), 0) AS total_debits,
        COALESCE(SUM(li.credit_amount), 0) AS total_credits
      FROM elements source_elem
      JOIN line_items li ON li.element_id = source_elem.id
      JOIN entries e ON e.id = li.entry_id
      JOIN associations mapping
        ON mapping.from_element_id = source_elem.id
        AND mapping.association_type = :arc_type
        AND mapping.structure_id = :mapping_id
      JOIN elements target ON target.id = mapping.to_element_id
      LEFT JOIN (
        SELECT et.element_id, t.identifier
        FROM element_traits et
        JOIN traits t ON t.id = et.trait_id
        WHERE et.is_primary = TRUE
          AND t.category = 'elementsOfFinancialStatements'
      ) tcls ON tcls.element_id = target.id
      WHERE e.status IN :landed_entry_statuses
        AND target.element_type = 'concept'
        AND target.is_abstract = false
        AND (e.posting_date <= :end_date OR :end_date IS NULL)
        AND (
          target.period_type = 'instant'
          OR e.posting_date >= :start_date
          OR :start_date IS NULL
        )
      GROUP BY source_elem.id, target.id, target.qname, target.name,
               tcls.identifier, target.balance_type
      ORDER BY target.qname
    """).bindparams(landed_entry_bindparam()),
    {
      "mapping_id": mapping_id,
      "arc_type": arc_type,
      "start_date": period_start,
      "end_date": period_end,
    },
  )

  # Pass 1 — group the per-(source, target) rows both ways.
  rows = list(result)
  by_source: dict[str, list] = {}
  for row in rows:
    by_source.setdefault(row.source_id, []).append(row)

  # Pass 2 — per source, pick the close-primary target.
  primary_pairs: set[tuple[str, str]] = set()
  for source_id, source_rows in by_source.items():
    ranked = sorted(source_rows, key=lambda r: (r.classification is None, r.qname))
    primary_pairs.add((source_id, ranked[0].reporting_element_id))

  # Pass 3 — full balance from every source, close balance from primaries.
  balances: dict[str, _Balance] = {}
  for row in rows:
    debits = cents_to_dollars(row.total_debits)
    credits = cents_to_dollars(row.total_credits)
    classification = row.classification or _infer_classification(
      row.qname, row.balance_type
    )
    balance = balances.get(row.reporting_element_id)
    if balance is None:
      balance = _Balance(
        element_id=row.reporting_element_id,
        qname=row.qname,
        name=row.reporting_name,
        classification=classification,
        balance_type=row.balance_type,
        total_debits=0.0,
        total_credits=0.0,
        net_balance=0.0,
        close_net_balance=0.0,
      )
      balances[row.reporting_element_id] = balance
    balance.total_debits += debits
    balance.total_credits += credits
    balance.net_balance += debits - credits
    if (row.source_id, row.reporting_element_id) in primary_pairs:
      assert balance.close_net_balance is not None
      balance.close_net_balance += debits - credits

  return balances


def _append_empty_equity_facts(
  session: Session,
  mapping_id: str,
  facts: list[ReportFact],
  period_start: date,
  period_end: date,
  arc_type: str = "mapping",
  close_target_qname: str = "rs-gaap:RetainedEarningsAccumulatedDeficit",
) -> None:
  """Append zero-balance facts for mapped equity targets without postings.

  The close needs ``close_target_qname`` (the Style's earnings home) present
  as a fact to route net income to it.
  """
  result = session.execute(
    text("""
      SELECT DISTINCT target.id, target.qname, target.name, target.balance_type,
             tcls.identifier AS classification
      FROM associations mapping
      JOIN elements target ON target.id = mapping.to_element_id
      LEFT JOIN (
        SELECT et.element_id, t.identifier
        FROM element_traits et
        JOIN traits t ON t.id = et.trait_id
        WHERE et.is_primary = TRUE
          AND t.category = 'elementsOfFinancialStatements'
      ) tcls ON tcls.element_id = target.id
      WHERE mapping.structure_id = :mapping_id
        AND mapping.association_type = :arc_type
        AND target.element_type = 'concept'
        AND target.is_abstract = false
    """),
    {"mapping_id": mapping_id, "arc_type": arc_type},
  )

  existing_ids = {
    f.element_id
    for f in facts
    if f.period_start == period_start and f.period_end == period_end
  }

  for row in result:
    if row.id in existing_ids:
      continue
    classification = row.classification or _infer_classification(
      row.qname, row.balance_type
    )
    if classification != "equity":
      continue
    facts.append(
      ReportFact(
        element_id=row.id,
        element_qname=row.qname,
        element_name=row.name,
        classification="equity",
        balance_type=row.balance_type,
        value=0.0,
        period_start=period_start,
        period_end=period_end,
        period_type="instant",
      )
    )

  # Materialize the close target even when nothing maps to it: earnings are
  # derived at render time (the QuickBooks / Xero pattern), and without it
  # net income would land on a fact no network renders.
  re_row = session.execute(
    text(
      """
      SELECT id, qname, name, balance_type
      FROM elements
      WHERE qname = :close_target
      LIMIT 1
      """
    ),
    {"close_target": close_target_qname},
  ).fetchone()
  if re_row is not None and re_row.id not in existing_ids:
    already_present = any(
      f.element_id == re_row.id
      and f.period_start == period_start
      and f.period_end == period_end
      for f in facts
    )
    if not already_present:
      facts.append(
        ReportFact(
          element_id=re_row.id,
          element_qname=re_row.qname,
          element_name=re_row.name or "Retained Earnings (Accumulated Deficit)",
          classification="equity",
          balance_type=re_row.balance_type or "credit",
          value=0.0,
          period_start=period_start,
          period_end=period_end,
          period_type="instant",
        )
      )


def _emit_net_income_facts(
  session: Session,
  facts: list[ReportFact],
  periods: list[PeriodSpec],
) -> None:
  """Append one ``rs-gaap:NetIncomeLoss`` fact per non-zero period.

  Sums close-eligible values (:func:`_close_value`) so multi-mapped
  accounts count once. A directly-mapped NetIncomeLoss fact wins.
  """
  ni_row = session.execute(
    text("SELECT id, balance_type FROM elements WHERE qname='rs-gaap:NetIncomeLoss'")
  ).fetchone()
  if ni_row is None:
    return
  ni_id, ni_balance_type = ni_row[0], ni_row[1] or "credit"

  for period in periods:
    already_present = any(
      f.element_id == ni_id
      and f.period_start == period.start
      and f.period_end == period.end
      for f in facts
    )
    if already_present:
      continue
    revenue = 0.0
    expense = 0.0
    for f in facts:
      if f.period_start != period.start or f.period_end != period.end:
        continue
      if f.classification == "revenue":
        revenue += _close_value(f)
      elif f.classification == "expense":
        expense += _close_value(f)
    net_income = revenue - expense
    if net_income == 0.0:
      continue
    facts.append(
      ReportFact(
        element_id=ni_id,
        element_qname="rs-gaap:NetIncomeLoss",
        element_name="Net Income (Loss)",
        classification=None,
        balance_type=ni_balance_type,
        value=net_income,
        period_start=period.start,
        period_end=period.end,
        period_type="duration",
      )
    )


def _emit_subtotal_facts(
  session: Session,
  facts: list[ReportFact],
  periods: list[PeriodSpec],
) -> None:
  """Append one fact per non-zero rs-gaap calculation subtotal, per period.

  Persisted so verification rules scoped to a subtotal can bind. Uses the
  same resolution as ``_build_rows`` (direct fact wins, else Σ child·weight),
  so the rendered number is identical. Must run after every leaf and
  derived fact is in place.
  """
  calculations = load_rs_gaap_calculations(session)
  if not calculations:
    return

  target_ids = list(calculations.keys())
  meta = {
    m.id: m
    for m in session.execute(
      text(
        "SELECT id, qname, name, balance_type, period_type "
        "FROM elements WHERE id = ANY(:ids)"
      ),
      {"ids": target_ids},
    ).fetchall()
  }

  order = topo_sort_calculations(calculations)

  for period in periods:
    balances: dict[str, float] = {}
    present: set[str] = set()
    for f in facts:
      if f.period_start == period.start and f.period_end == period.end:
        balances[f.element_id] = balances.get(f.element_id, 0.0) + f.value
        present.add(f.element_id)

    computed = resolve_calc_dag(balances, present, calculations, order)

    for elem_id in target_ids:
      if elem_id in present:
        continue
      value = computed.get(elem_id, 0.0)
      if value == 0.0:
        continue
      m = meta.get(elem_id)
      if m is None:
        continue
      facts.append(
        ReportFact(
          element_id=elem_id,
          element_qname=m.qname,
          element_name=m.name or m.qname,
          classification=None,  # subtotals aren't leaf revenue/expense
          balance_type=m.balance_type or "credit",
          value=value,
          period_start=period.start,
          period_end=period.end,
          period_type=m.period_type or "instant",
        )
      )


def _mark_ppe_details_audit_only(
  facts: list[ReportFact],
  period: PeriodSpec,
  gross_id: str | None,
  ad_id: str | None,
) -> None:
  """Mark ``period``'s PP&E Gross + AD facts audit-only; PPE Net carries them."""
  detail_ids = {i for i in (gross_id, ad_id) if i is not None}
  for f in facts:
    if (
      f.element_id in detail_ids
      and f.period_start == period.start
      and f.period_end == period.end
    ):
      f.audit_only = True


def _synthesize_ppe_net_facts(
  session: Session,
  facts: list[ReportFact],
  periods: list[PeriodSpec],
) -> None:
  """Append PPE Net = Gross - Accumulated Depreciation per period.

  For gross + contra mappings, which let CF Investing read ΔGross as
  purchases. Skipped where a direct PPE Net fact exists.
  """
  row = session.execute(
    text(
      "SELECT id, balance_type FROM elements "
      "WHERE qname = 'rs-gaap:PropertyPlantAndEquipmentNet'"
    )
  ).fetchone()
  if row is None:
    return
  net_id, net_balance_type = row[0], row[1] or "debit"

  src_rows = session.execute(
    text(
      "SELECT qname, id FROM elements WHERE qname IN ("
      "'rs-gaap:PropertyPlantAndEquipmentGross', "
      "'rs-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment')"
    )
  ).fetchall()
  src_ids = dict(src_rows)
  gross_id = src_ids.get("rs-gaap:PropertyPlantAndEquipmentGross")
  ad_id = src_ids.get(
    "rs-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment"
  )
  if gross_id is None and ad_id is None:
    return  # Neither source mapped — nothing to synthesize.

  for period in periods:
    already_present = any(
      f.element_id == net_id
      and f.period_start == period.start
      and f.period_end == period.end
      for f in facts
    )
    if already_present:
      # All-in-Net mapping: the BS is right, but CF capex (from ΔGross) is 0.
      if gross_id is not None and not any(
        f.element_id == gross_id
        and f.period_start == period.start
        and f.period_end == period.end
        for f in facts
      ):
        logger.warning(
          "_synthesize_ppe_net_facts: direct PropertyPlantAndEquipmentNet "
          "fact exists for period %s..%s but no PropertyPlantAndEquipmentGross "
          "fact — CF Investing's PaymentsToAcquirePropertyPlantAndEquipment "
          "will be 0. Migrate the mapping: route fixed-asset accounts to "
          "rs-gaap:PropertyPlantAndEquipmentGross and the accumulated-"
          "depreciation contra-account to "
          "rs-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment.",
          period.start,
          period.end,
        )
      _mark_ppe_details_audit_only(facts, period, gross_id, ad_id)
      continue
    gross_value = 0.0
    ad_value = 0.0
    for f in facts:
      if f.period_start != period.start or f.period_end != period.end:
        continue
      if gross_id is not None and f.element_id == gross_id:
        gross_value += f.value
      elif ad_id is not None and f.element_id == ad_id:
        ad_value += f.value
    if gross_value == 0.0 and ad_value == 0.0:
      continue
    if gross_value == 0.0:
      logger.warning(
        "_synthesize_ppe_net_facts: AD fact present without Gross fact "
        "for period %s..%s — refusing to synthesize negative PPE Net. "
        "Verify the CoA maps a fixed-asset account to "
        "rs-gaap:PropertyPlantAndEquipmentGross.",
        period.start,
        period.end,
      )
      continue
    facts.append(
      ReportFact(
        element_id=net_id,
        element_qname="rs-gaap:PropertyPlantAndEquipmentNet",
        element_name="Property, Plant and Equipment, Net",
        classification="asset",
        balance_type=net_balance_type,
        value=gross_value - ad_value,
        period_start=period.start,
        period_end=period.end,
        period_type="instant",
      )
    )
    # Still read by the CF derivation (capex = ΔGross), which ignores the flag.
    _mark_ppe_details_audit_only(facts, period, gross_id, ad_id)


# Cash concepts for cash-flow attribution: a line on one of these is a cash
# line, and its debit - credit is that entry's cash flow. Curated by qname
# because trait coverage on cash concepts is incomplete.
_CASH_ANCHOR_QNAMES: frozenset[str] = frozenset(
  {
    "rs-gaap:CashCashEquivalentsAndShortTermInvestments",
    "rs-gaap:CashAndCashEquivalentsAtCarryingValue",
    "rs-gaap:CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
    "rs-gaap:Cash",
    "rs-gaap:CashAndCashEquivalents",
  }
)

# The reconciling leaf is in both the CF calc DAG and the Indirect
# presentation, so the adjustment foots the subtotal and renders as a line.
_CF_NET_CHANGE_QNAME = "rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease"
_CF_RECONCILING_LEAF_QNAME = "rs-gaap:IncreaseDecreaseInOtherOperatingCapitalNet"
_CF_OPERATING_SUBTOTAL_QNAME = "rs-gaap:NetCashProvidedByUsedInOperatingActivities"

# A reconciling plug above this fraction of operating cash signals an
# un-itemized non-cash item or a misclassified flow. Shared with guard_rails.
_CF_PLUG_WARN_RATIO = 0.25


def _emit_flow_facts(
  session: Session,
  facts: list[ReportFact],
  periods: list[PeriodSpec],
  mapping_id: str,
  arc_type: str,
) -> None:
  """Emit investing/financing CF facts from per-line flows, per period.

  Pass 1 (explicit): the cash line carries ``flow_element_id``; its
  ``debit - credit`` is the flow. Pass 2 (fallback, the path for QuickBooks
  data): each non-cash line of an untagged cash-moving entry routes to its
  element's default flow via a ``derivation`` arc whose weight sign is the
  cash direction. The passes agree in sign because a balanced entry's cash
  side negates its non-cash side. Operating flows are excluded — indirect
  operating is NI + addbacks + ΔWC. Replaces any existing fact for the leaf.
  """
  cash_qnames = list(_CASH_ANCHOR_QNAMES)

  explicit_sql = text("""
        SELECT
          COALESCE(fmap.to_element_id, li.flow_element_id) AS flow_id,
          rf.qname AS flow_qname,
          rf.name AS flow_name,
          rf.balance_type AS flow_balance_type,
          COALESCE(SUM(li.debit_amount - li.credit_amount), 0) AS value_cents
        FROM line_items li
        JOIN entries en ON en.id = li.entry_id
        JOIN elements acct ON acct.id = li.element_id
        LEFT JOIN associations amap
          ON amap.from_element_id = li.element_id
          AND amap.association_type = :arc_type
          AND amap.structure_id = :mapping_id
        LEFT JOIN elements racct ON racct.id = amap.to_element_id
        LEFT JOIN associations fmap
          ON fmap.from_element_id = li.flow_element_id
          AND fmap.association_type = :arc_type
          AND fmap.structure_id = :mapping_id
        JOIN elements rf ON rf.id = COALESCE(fmap.to_element_id, li.flow_element_id)
        JOIN element_traits et ON et.element_id = rf.id
        JOIN traits t ON t.id = et.trait_id
          AND t.category = 'activityType'
          AND t.identifier IN ('investingActivity', 'financingActivity')
        WHERE li.flow_element_id IS NOT NULL
          AND en.status IN :landed_entry_statuses
          AND en.posting_date BETWEEN :start AND :end
          AND COALESCE(racct.qname, acct.qname) = ANY(:cash_qnames)
        GROUP BY flow_id, rf.qname, rf.name, rf.balance_type
      """).bindparams(landed_entry_bindparam())

  # Gated on the entry moving cash (so accruals don't leak in) and carrying
  # no explicit tag anywhere (Pass 1 owns those entries).
  fallback_sql = text("""
        SELECT
          rf.id AS flow_id,
          rf.qname AS flow_qname,
          rf.name AS flow_name,
          rf.balance_type AS flow_balance_type,
          COALESCE(SUM(li.credit_amount - li.debit_amount), 0) AS value_cents
        FROM line_items li
        JOIN entries en ON en.id = li.entry_id
        JOIN elements acct ON acct.id = li.element_id
        LEFT JOIN associations amap
          ON amap.from_element_id = li.element_id
          AND amap.association_type = :arc_type
          AND amap.structure_id = :mapping_id
        LEFT JOIN elements racct ON racct.id = amap.to_element_id
        JOIN associations darc
          ON darc.to_element_id = COALESCE(amap.to_element_id, li.element_id)
          AND darc.association_type = 'derivation'
          AND SIGN(darc.weight) = SIGN(li.credit_amount - li.debit_amount)
        JOIN elements rf ON rf.id = darc.from_element_id
        JOIN element_traits et ON et.element_id = rf.id
        JOIN traits t ON t.id = et.trait_id
          AND t.category = 'activityType'
          AND t.identifier IN ('investingActivity', 'financingActivity')
        WHERE li.flow_element_id IS NULL
          AND en.status IN :landed_entry_statuses
          AND en.posting_date BETWEEN :start AND :end
          AND COALESCE(racct.qname, acct.qname) <> ALL(:cash_qnames)
          AND EXISTS (
            SELECT 1 FROM line_items cl
            JOIN elements ca ON ca.id = cl.element_id
            LEFT JOIN associations cmap
              ON cmap.from_element_id = cl.element_id
              AND cmap.association_type = :arc_type
              AND cmap.structure_id = :mapping_id
            LEFT JOIN elements cra ON cra.id = cmap.to_element_id
            WHERE cl.entry_id = en.id
              AND COALESCE(cra.qname, ca.qname) = ANY(:cash_qnames)
          )
          AND NOT EXISTS (
            SELECT 1 FROM line_items x
            WHERE x.entry_id = en.id AND x.flow_element_id IS NOT NULL
          )
        GROUP BY rf.id, rf.qname, rf.name, rf.balance_type
      """).bindparams(landed_entry_bindparam())

  for period in periods:
    params = {
      "arc_type": arc_type,
      "mapping_id": mapping_id,
      "start": period.start,
      "end": period.end,
      "cash_qnames": cash_qnames,
    }
    # flow_id -> [qname, name, balance_type, summed_value]
    combined: dict[str, list] = {}
    for sql in (explicit_sql, fallback_sql):
      for row in session.execute(sql, params).fetchall():
        agg = combined.get(row.flow_id)
        if agg is None:
          agg = [row.flow_qname, row.flow_name, row.flow_balance_type or "debit", 0.0]
          combined[row.flow_id] = agg
        agg[3] += cents_to_dollars(row.value_cents)

    for flow_id, (qname, name, balance_type, value) in combined.items():
      if value == 0:
        continue
      facts[:] = [
        f
        for f in facts
        if not (
          f.element_id == flow_id
          and f.period_start == period.start
          and f.period_end == period.end
        )
      ]
      facts.append(
        ReportFact(
          element_id=flow_id,
          element_qname=qname,
          element_name=name,
          classification=None,
          balance_type=balance_type,
          value=value,
          period_start=period.start,
          period_end=period.end,
          period_type="duration",
        )
      )


def _opening_date(period: PeriodSpec) -> date:
  """The balance-sheet date a period's change is measured from."""
  return period.start - timedelta(days=1)


def _load_opening_facts(
  session: Session,
  mapping_id: str,
  periods: list[PeriodSpec],
  arc_type: str,
) -> list[ReportFact]:
  """Instant balances at each later period's opening, where that isn't
  already another column's end."""
  ordered = sorted(periods, key=lambda p: p.end)
  loaded = {p.end for p in ordered}
  opening_facts: list[ReportFact] = []
  for current in ordered[1:]:
    opening = _opening_date(current)
    if opening in loaded:
      continue
    loaded.add(opening)
    balances = _read_mapped_balances(
      session, mapping_id, opening, opening, arc_type=arc_type
    )
    for balance in balances.values():
      opening_facts.append(
        ReportFact(
          element_id=balance.element_id,
          element_qname=balance.qname,
          element_name=balance.name,
          classification=balance.classification,
          balance_type=balance.balance_type,
          value=_natural_sign(balance.net_balance, balance.balance_type),
          period_start=opening,
          period_end=opening,
          period_type=_infer_period_type(balance.classification),
        )
      )
  return opening_facts


def _derive_cash_flow_facts(
  session: Session,
  facts: list[ReportFact],
  periods: list[PeriodSpec],
  opening_facts: list[ReportFact] | None = None,
) -> None:
  """Append operating CF facts from BS deltas (indirect method).

  Each ``derivation`` arc says "this CF leaf is the change in this BS
  element" with a signed weight (asset up = -1, liability up = +1). For each
  period after the first, ``Σ weight * (BS_end - BS_opening)``, the opening
  being the day before the period starts. Zero values are skipped.
  """
  if len(periods) < 2:
    logger.debug(
      "_derive_cash_flow_facts: skipped — indirect method needs ≥2 periods (got %d)",
      len(periods),
    )
    return

  # Periods arrive in presentation order (often newest-first).
  ordered = sorted(periods, key=lambda p: p.end)

  # Unscoped on purpose: derivation arcs are library-only (tenants can't
  # insert them). Tenant-authored derivations would need a scope here.
  # Investing/financing arcs are excluded: net-delta can't present gross
  # flows; _emit_flow_facts uses them as default-flow lookups instead.
  rows = session.execute(
    text("""
      SELECT a.from_element_id, a.to_element_id, a.weight
      FROM associations a
      WHERE a.association_type = 'derivation'
        AND NOT EXISTS (
          SELECT 1 FROM element_traits et
          JOIN traits t ON t.id = et.trait_id
          WHERE et.element_id = a.from_element_id
            AND t.category = 'activityType'
            AND t.identifier IN ('investingActivity', 'financingActivity')
        )
    """)
  ).fetchall()
  if not rows:
    return

  derivations: dict[str, list[tuple[str, float]]] = {}
  for cf_id, source_id, weight in rows:
    derivations.setdefault(cf_id, []).append((source_id, float(weight or 1.0)))

  cf_leaf_ids = list(derivations.keys())
  if not cf_leaf_ids:
    return
  meta_rows = session.execute(
    text("""
      SELECT id, qname, name, balance_type
      FROM elements
      WHERE id = ANY(:ids)
    """),
    {"ids": cf_leaf_ids},
  ).fetchall()
  cf_meta: dict[str, tuple[str, str, str]] = {
    row[0]: (row[1], row[2], row[3] or "debit") for row in meta_rows
  }

  # Sum on collision, as _facts_to_balance_dict does.
  fact_index: dict[tuple[str, date], float] = {}
  for f in facts:
    key = (f.element_id, f.period_end)
    fact_index[key] = fact_index.get(key, 0.0) + f.value
  opening_index: dict[tuple[str, date], float] = dict(fact_index)
  for f in opening_facts or ():
    key = (f.element_id, f.period_end)
    opening_index[key] = opening_index.get(key, 0.0) + f.value

  for current in ordered[1:]:
    opening = _opening_date(current)
    for cf_leaf_id, sources in derivations.items():
      # A direct fact wins (e.g. DDA mapped from Depreciation Expense).
      if (cf_leaf_id, current.end) in fact_index:
        continue
      cf_value = 0.0
      for source_id, weight in sources:
        current_v = fact_index.get((source_id, current.end), 0.0)
        opening_v = opening_index.get((source_id, opening), 0.0)
        cf_value += weight * (current_v - opening_v)
      if cf_value == 0.0:
        continue
      meta = cf_meta.get(cf_leaf_id)
      if meta is None:
        continue
      qname, name, balance_type = meta
      facts.append(
        ReportFact(
          element_id=cf_leaf_id,
          element_qname=qname,
          element_name=name,
          classification=None,
          balance_type=balance_type,
          value=cf_value,
          period_start=current.start,
          period_end=current.end,
          period_type="duration",
        )
      )


def _reconcile_operating_to_cash(
  session: Session,
  facts: list[ReportFact],
  periods: list[PeriodSpec],
  opening_facts: list[ReportFact] | None = None,
) -> None:
  """Foot the indirect CF to the actual cash-balance movement.

  The indirect operating section reverses only depreciation among non-cash
  items, so gains/losses in net income leave the CF off from ΔCash. True
  operating cash is ``ΔCash - Investing - Financing``; the gap is booked as
  one adjustment on ``IncreaseDecreaseInOtherOperatingCapitalNet``.

  Tradeoff: this foots the CF by construction, silencing
  ``_check_cash_flow_tie_out``; a large plug relative to operating cash is
  logged as a warning instead. Skips periods with no cash-anchor balance.
  """
  if len(periods) < 2:
    return

  id_rows = session.execute(
    text("SELECT id, qname, name, balance_type FROM elements WHERE qname = ANY(:q)"),
    {
      "q": [
        _CF_NET_CHANGE_QNAME,
        _CF_RECONCILING_LEAF_QNAME,
        _CF_OPERATING_SUBTOTAL_QNAME,
      ]
    },
  ).fetchall()
  by_qname = {r.qname: r for r in id_rows}
  net_change = by_qname.get(_CF_NET_CHANGE_QNAME)
  recon = by_qname.get(_CF_RECONCILING_LEAF_QNAME)
  operating = by_qname.get(_CF_OPERATING_SUBTOTAL_QNAME)
  operating_id = operating.id if operating is not None else None
  if net_change is None or recon is None:
    return
  net_change_id = net_change.id
  recon_leaf_id = recon.id

  calculations = load_rs_gaap_calculations(session)
  if not calculations:
    return
  order = topo_sort_calculations(calculations)

  cash_by_date = _cash_by_date(facts, opening_facts)

  ordered = sorted(periods, key=lambda p: p.end)
  for current in ordered[1:]:
    cash_end = cash_by_date.get(current.end)
    if cash_end is None:
      continue
    cash_delta = cash_end - cash_by_date.get(_opening_date(current), 0.0)

    balances: dict[str, float] = {}
    present: set[str] = set()
    for f in facts:
      if f.period_start == current.start and f.period_end == current.end:
        balances[f.element_id] = balances.get(f.element_id, 0.0) + f.value
        present.add(f.element_id)
    computed = resolve_calc_dag(balances, present, calculations, order)
    derived_net_change = computed.get(net_change_id, 0.0)

    residual = cash_delta - derived_net_change
    if abs(residual) <= 0.005:
      continue
    # Basis is post-plug operating cash, floored by ΔCash so a near-zero
    # operating section can't divide by zero.
    derived_operating = computed.get(operating_id, 0.0) if operating_id else 0.0
    true_operating = derived_operating + residual
    denom = max(abs(true_operating), abs(cash_delta))
    large = denom > 0.005 and abs(residual) > _CF_PLUG_WARN_RATIO * denom
    suffix = (
      " — LARGE relative to operating cash (%.0f%%); review for an un-itemized "
      "non-cash item (gain/loss on disposal, …) or a flow misclassification."
      % (100 * abs(residual) / denom)
      if large
      else "; itemizing its components is a future enrichment."
    )
    (logger.warning if large else logger.info)(
      "CF reconciled to cash for period ending %s: non-cash operating "
      "adjustment %.2f (derived net change %.2f, actual cash movement %.2f). "
      "Booked to %s%s",
      current.end,
      residual,
      derived_net_change,
      cash_delta,
      _CF_RECONCILING_LEAF_QNAME,
      suffix,
    )
    facts.append(
      ReportFact(
        element_id=recon_leaf_id,
        element_qname=_CF_RECONCILING_LEAF_QNAME,
        element_name=recon.name or _CF_RECONCILING_LEAF_QNAME,
        classification=None,
        balance_type=recon.balance_type or "debit",
        value=residual,
        period_start=current.start,
        period_end=current.end,
        period_type="duration",
      )
    )


def _cash_by_date(
  facts: list[ReportFact], opening_facts: list[ReportFact] | None
) -> dict[date, float]:
  cash_by_date: dict[date, float] = {}
  for f in [*facts, *(opening_facts or ())]:
    if f.period_type == "instant" and f.element_qname in _CASH_ANCHOR_QNAMES:
      cash_by_date[f.period_end] = cash_by_date.get(f.period_end, 0.0) + f.value
  return cash_by_date


def _check_cash_flow_tie_out(
  facts: list[ReportFact],
  periods: list[PeriodSpec],
  opening_facts: list[ReportFact] | None = None,
) -> None:
  """Warn when the CF net change in cash differs from the BS cash movement.

  A mismatch means investing/financing attribution is incomplete.
  """
  if len(periods) < 2:
    return
  cash_by_date = _cash_by_date(facts, opening_facts)
  net_change_by_end: dict[date, float] = {}
  for f in facts:
    if f.element_qname == "rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease":
      net_change_by_end[f.period_end] = (
        net_change_by_end.get(f.period_end, 0.0) + f.value
      )

  ordered = sorted(periods, key=lambda p: p.end)
  for current in ordered[1:]:
    net_change = net_change_by_end.get(current.end)
    if net_change is None:
      continue
    cash_end = cash_by_date.get(current.end)
    if cash_end is None:
      # "Couldn't check" must not look like "checked and tied".
      logger.warning(
        "CF tie-out could not run for period ending %s: net change in cash "
        "(%.2f) is present but no instant cash-balance fact was found (cash "
        "concept missing from _CASH_ANCHOR_QNAMES, or balance sheet not "
        "generated).",
        current.end,
        net_change,
      )
      continue
    # No opening balance means $0 at inception — flag, don't skip.
    cash_start = cash_by_date.get(_opening_date(current), 0.0)
    delta_cash = cash_end - cash_start
    residual = net_change - delta_cash
    if abs(residual) > 0.01:
      logger.warning(
        "CF tie-out mismatch for period ending %s: net change in cash (%.2f) "
        "≠ ΔCash from balance sheet (%.2f), residual %.2f. Investing/financing "
        "attribution is likely incomplete (flow concept off the render "
        "structure, or a coarse CoA mapping the gross default arcs can't "
        "resolve).",
        current.end,
        net_change,
        delta_cash,
        residual,
      )


def _count_unmapped(
  session: Session, mapping_id: str, arc_type: str = "mapping"
) -> int:
  """Count CoA elements that have no association of the given arc-type."""
  from robosystems.models.extensions.roboledger import COA_SOURCES

  result = session.execute(
    text("""
      SELECT COUNT(*) AS cnt
      FROM elements e
      WHERE e.source = ANY(:sources)
        AND (e.taxonomy_id IS NULL OR e.taxonomy_id IN (
          SELECT id FROM taxonomies WHERE taxonomy_type = 'chart_of_accounts'
        ))
        AND e.is_active = true
        AND NOT EXISTS (
          SELECT 1 FROM associations ea
          WHERE ea.from_element_id = e.id
            AND ea.association_type = :arc_type
            AND ea.structure_id = :mapping_id
        )
    """),
    {
      "sources": list(COA_SOURCES),
      "mapping_id": mapping_id,
      "arc_type": arc_type,
    },
  )
  row = result.fetchone()
  return row.cnt if row else 0


def _whole_month_span(period_start: date, period_end: date) -> int | None:
  """Months spanned when the range is exactly N whole calendar months, else None."""
  from robosystems.operations.roboledger.fiscal_calendar.periods import (
    last_day_of_month,
  )

  if period_start.day != 1:
    return None
  if period_end.day != last_day_of_month(period_end.year, period_end.month):
    return None
  months = (
    (period_end.year - period_start.year) * 12
    + (period_end.month - period_start.month)
    + 1
  )
  return months if months >= 1 else None


def _compute_prior_period(period_start: date, period_end: date) -> tuple[date, date]:
  """The comparative prior period ending the day before period_start.

  N whole calendar months -> the N calendar months before (equal-day
  arithmetic would miss stored monthly FactSets). Otherwise equal length.
  """
  from robosystems.operations.roboledger.fiscal_calendar.periods import (
    add_months,
    parse_period,
    period_name,
  )

  prior_end = period_start - timedelta(days=1)

  months = _whole_month_span(period_start, period_end)
  if months is not None:
    prior_year, prior_month = parse_period(
      add_months(period_name(period_start.year, period_start.month), -months)
    )
    return date(prior_year, prior_month, 1), prior_end

  duration = (period_end - period_start).days + 1
  return prior_end - timedelta(days=duration - 1), prior_end


# Used only when the close finds no target fact (unmapped graphs).
_ANON_RE_ELEMENT_ID = "elem_rsgaap_retained_earnings_anon"

# Labels for the anonymous fallback row, so PART/LLC graphs don't show
# "Retained Earnings".
_CLOSE_TARGET_LABELS = {
  "rs-gaap:RetainedEarningsAccumulatedDeficit": "Retained Earnings (Accumulated Deficit)",
  "rs-gaap:PartnersCapital": "Partners' Capital",
  "rs-gaap:MembersEquity": "Members' Equity",
}


def _close_target_label(qname: str) -> str:
  return _CLOSE_TARGET_LABELS.get(qname, qname)


def _find_close_target(
  facts: list[ReportFact],
  period_start: date,
  period_end: date,
  close_target_qname: str = "rs-gaap:RetainedEarningsAccumulatedDeficit",
) -> ReportFact | None:
  """The equity fact that receives the closing entry, or ``None``.

  Exact qname match, except that the corporate default also accepts any
  ``*RetainedEarnings*`` / ``*RetainedDeficit*`` qname (seeded taxonomies
  name RE differently). Form-specific targets never widen.
  """
  re_default = close_target_qname == "rs-gaap:RetainedEarningsAccumulatedDeficit"
  for fact in facts:
    if fact.period_start != period_start or fact.period_end != period_end:
      continue
    if fact.classification != "equity":
      continue
    if fact.element_qname == close_target_qname:
      return fact
    if re_default:
      qname_lower = (fact.element_qname or "").lower()
      if "retainedearnings" in qname_lower or "retaineddeficit" in qname_lower:
        return fact

  return None


def _cumulative_closeable_sums(
  session: Session,
  mapping_id: str,
  period_end: date,
  arc_type: str = "mapping",
) -> tuple[float, float, float]:
  """Cumulative (revenue, expense, equity_reductions) from inception to period_end.

  Counted once per source account, not once per mapped target: each source
  takes one classification — reducer targets first, then trait-classified
  P&L targets ahead of inferred ones.
  """
  result = session.execute(
    text("""
      SELECT
        source_elem.id AS source_id,
        target.qname,
        target.balance_type,
        tcls.identifier AS classification,
        COALESCE(SUM(li.debit_amount), 0) AS total_debits,
        COALESCE(SUM(li.credit_amount), 0) AS total_credits
      FROM elements source_elem
      JOIN line_items li ON li.element_id = source_elem.id
      JOIN entries e ON e.id = li.entry_id
      JOIN associations mapping
        ON mapping.from_element_id = source_elem.id
        AND mapping.association_type = :arc_type
        AND mapping.structure_id = :mapping_id
      JOIN elements target ON target.id = mapping.to_element_id
      LEFT JOIN (
        SELECT et.element_id, t.identifier
        FROM element_traits et
        JOIN traits t ON t.id = et.trait_id
        WHERE et.is_primary = TRUE
          AND t.category = 'elementsOfFinancialStatements'
      ) tcls ON tcls.element_id = target.id
      WHERE e.status IN :landed_entry_statuses
        AND target.element_type = 'concept'
        AND target.is_abstract = false
        AND e.posting_date <= :end_date
      GROUP BY source_elem.id, target.qname, target.balance_type, tcls.identifier
    """).bindparams(landed_entry_bindparam()),
    {
      "mapping_id": mapping_id,
      "arc_type": arc_type,
      "end_date": period_end,
    },
  )

  by_source: dict[str, list] = {}
  for row in result:
    by_source.setdefault(row.source_id, []).append(row)

  total_revenue = 0.0
  total_expenses = 0.0
  total_equity_reductions = 0.0
  for rows in by_source.values():
    reducer = next((r for r in rows if _is_equity_flow_reducer(r.qname)), None)
    if reducer is not None:
      net = cents_to_dollars(reducer.total_debits - reducer.total_credits)
      total_equity_reductions += _natural_sign(net, reducer.balance_type)
      continue
    classified = []
    for r in rows:
      cls = r.classification or _infer_classification(r.qname, r.balance_type)
      if cls in ("revenue", "expense"):
        classified.append((r.classification is None, r.qname, cls, r))
    if not classified:
      continue
    classified.sort()
    _, _, cls, row = classified[0]
    net = cents_to_dollars(row.total_debits - row.total_credits)
    natural = _natural_sign(net, row.balance_type)
    if cls == "revenue":
      total_revenue += natural
    else:
      total_expenses += natural

  return total_revenue, total_expenses, total_equity_reductions


def _close_value(fact: ReportFact) -> float:
  """The close-eligible portion of a fact's value (see ``ReportFact.close_value``)."""
  return fact.value if fact.close_value is None else fact.close_value


def _close_to_retained_earnings(
  facts: list[ReportFact],
  period_start: date,
  period_end: date,
  close_target_qname: str = "rs-gaap:RetainedEarningsAccumulatedDeficit",
) -> None:
  """Add the period's revenue - expense + equity reducers to the close target.

  Uses close-eligible values so multi-mapped accounts count once. With no
  target fact (unmapped graphs), appends an anonymous row.
  """
  total_revenue = 0.0
  total_expenses = 0.0
  # Reducer values are already natural-signed negative; add without a flip.
  total_equity_reductions = 0.0

  for fact in facts:
    if fact.period_start != period_start or fact.period_end != period_end:
      continue
    if fact.classification == "revenue":
      total_revenue += _close_value(fact)
    elif fact.classification == "expense":
      total_expenses += _close_value(fact)
    elif _is_equity_flow_reducer(fact.element_qname):
      total_equity_reductions += _close_value(fact)

  net_income = total_revenue - total_expenses + total_equity_reductions
  if net_income == 0.0:
    return

  target = _find_close_target(
    facts, period_start, period_end, close_target_qname=close_target_qname
  )
  if target is not None:
    target.value += net_income
    return

  logger.warning(
    "close_to_retained_earnings: no %s fact in scope for period "
    "%s..%s; appending anonymous fallback row. CoA is missing a mapping "
    "to the Style's earnings-home equity concept.",
    close_target_qname,
    period_start,
    period_end,
  )
  facts.append(
    ReportFact(
      element_id=_ANON_RE_ELEMENT_ID,
      element_qname=close_target_qname,
      element_name=_close_target_label(close_target_qname),
      classification="equity",
      balance_type="credit",
      value=net_income,
      period_start=period_start,
      period_end=period_end,
      period_type="instant",
    )
  )


def _close_prior_periods_to_retained_earnings(
  session: Session,
  mapping_id: str,
  facts: list[ReportFact],
  period_start: date,
  period_end: date,
  arc_type: str = "mapping",
  close_target_qname: str = "rs-gaap:RetainedEarningsAccumulatedDeficit",
) -> None:
  """Add un-closed net income from before the current period to the close target.

  Cumulative net income from inception through ``period_end``, minus the
  current period's (already closed). Inception is always safe: a real
  closing entry zeroes the rev/exp accounts it closes, so the cumulative
  sum is only the still-unclosed portion — no double count.
  """
  cumulative_revenue, cumulative_expenses, cumulative_equity_reductions = (
    _cumulative_closeable_sums(session, mapping_id, period_end, arc_type)
  )
  cumulative_net_income = (
    cumulative_revenue - cumulative_expenses + cumulative_equity_reductions
  )

  current_revenue = 0.0
  current_expenses = 0.0
  current_equity_reductions = 0.0
  for fact in facts:
    if fact.period_start != period_start or fact.period_end != period_end:
      continue
    if fact.classification == "revenue":
      current_revenue += _close_value(fact)
    elif fact.classification == "expense":
      current_expenses += _close_value(fact)
    elif _is_equity_flow_reducer(fact.element_qname):
      current_equity_reductions += _close_value(fact)

  current_net_income = current_revenue - current_expenses + current_equity_reductions
  prior_periods_net_income = cumulative_net_income - current_net_income

  if prior_periods_net_income == 0.0:
    return

  target = _find_close_target(
    facts, period_start, period_end, close_target_qname=close_target_qname
  )
  if target is not None:
    target.value += prior_periods_net_income
  else:
    logger.warning(
      "close_prior_periods_to_retained_earnings: no %s fact in "
      "scope for period %s..%s; appending anonymous fallback row. "
      "CoA is missing a mapping to the Style's earnings-home equity concept.",
      close_target_qname,
      period_start,
      period_end,
    )
    facts.append(
      ReportFact(
        element_id=_ANON_RE_ELEMENT_ID,
        element_qname=close_target_qname,
        element_name="Retained Earnings (Accumulated Deficit)",
        classification="equity",
        balance_type="credit",
        value=prior_periods_net_income,
        period_start=period_start,
        period_end=period_end,
        period_type="instant",
      )
    )


def _infer_period_type(classification: str) -> str:
  if classification in ("asset", "liability", "equity"):
    return "instant"
  return "duration"


def _load_reporting_structure(
  session: Session,
  report_type: str,
  reporting_style_id: str,
) -> tuple[str, str, str | None, list[_HierarchyNode]]:
  """(structure_id, structure_name, concept_arrangement, root_nodes) for a report type.

  The Network is the one the Reporting Style pins for the statement type.
  When the Style composes none, returns empty values ("nothing to render").
  """
  # Lazy: the picker's import chain pulls commands code into the reads layer.
  from robosystems.operations.roboledger.reports.network_picker import (
    NoNetworkForStatementTypeError,
    get_render_network,
  )

  try:
    network = get_render_network(session, reporting_style_id, report_type)
  except NoNetworkForStatementTypeError:
    return "", "", None, []

  structure_id = network.structure_id
  structure_name = network.name
  concept_arrangement = network.concept_arrangement

  assoc_result = session.execute(
    text("""
      SELECT
        ea.from_element_id AS parent_id,
        ea.to_element_id AS child_id,
        e.id AS element_id,
        e.qname,
        e.name,
        cls.identifier AS classification,
        e.balance_type,
        e.is_abstract,
        e.depth,
        ea.order_value
      FROM associations ea
      JOIN elements e ON e.id = ea.to_element_id
      LEFT JOIN (
        SELECT et.element_id, t.identifier
        FROM element_traits et
        JOIN traits t ON t.id = et.trait_id
        WHERE et.is_primary = TRUE
          AND t.category = 'elementsOfFinancialStatements'
      ) cls ON cls.element_id = e.id
      WHERE ea.structure_id = :structure_id
        AND ea.association_type = 'presentation'
        AND ea.from_element_id != :structure_id
      ORDER BY ea.order_value
    """),
    {"structure_id": structure_id},
  )

  children_map: dict[str, list[dict[str, Any]]] = {}
  all_child_ids: set[str] = set()
  element_info: dict[str, dict[str, Any]] = {}

  for row in assoc_result:
    child_data = {
      "element_id": row.element_id,
      "qname": row.qname,
      "name": row.name,
      "classification": row.classification,
      "balance_type": row.balance_type or "debit",
      "is_abstract": row.is_abstract,
      "depth": row.depth or 0,
      "order": row.order_value,
    }
    children_map.setdefault(row.parent_id, []).append(child_data)
    all_child_ids.add(row.child_id)
    element_info[row.element_id] = child_data

  # Roots: parents that are never a child.
  root_parent_ids: set[str] = set(children_map.keys()) - all_child_ids

  if root_parent_ids:
    placeholders = ", ".join(f":p{i}" for i in range(len(root_parent_ids)))
    params = {f"p{i}": pid for i, pid in enumerate(root_parent_ids)}
    root_result = session.execute(
      text(f"""
        SELECT e.id, e.qname, e.name, cls.identifier AS classification,
               e.balance_type, e.is_abstract, e.depth
        FROM elements e
        LEFT JOIN (
          SELECT et.element_id, t.identifier
          FROM element_traits et
          JOIN traits t ON t.id = et.trait_id
          WHERE et.is_primary = TRUE
            AND t.category = 'elementsOfFinancialStatements'
        ) cls ON cls.element_id = e.id
        WHERE e.id IN ({placeholders})
      """),
      params,
    )
    for row in root_result:
      element_info[row.id] = {
        "element_id": row.id,
        "qname": row.qname,
        "name": row.name,
        "classification": row.classification,
        "balance_type": row.balance_type or "debit",
        "is_abstract": row.is_abstract,
        "depth": row.depth or 0,
      }

  # The presentation hierarchy is a DAG; render each element once, under
  # the first parent that reaches it, or shared subtrees double-count.
  emitted: set[str] = set()

  def _build_tree(element_id: str, depth: int) -> _HierarchyNode | None:
    if element_id in emitted:
      return None
    emitted.add(element_id)
    info = element_info.get(element_id, {})
    node = _HierarchyNode(
      element_id=element_id,
      qname=info.get("qname", ""),
      name=info.get("name", ""),
      classification=info.get("classification", ""),
      balance_type=info.get("balance_type", "debit"),
      is_abstract=info.get("is_abstract", False),
      depth=depth,
    )
    for child_data in children_map.get(element_id, []):
      child_node = _build_tree(child_data["element_id"], depth + 1)
      if child_node is not None:
        node.children.append(child_node)
    return node

  root_order = _load_root_order(session, structure_id)
  roots: list[_HierarchyNode] = []
  for root_id in sorted(
    root_parent_ids,
    key=lambda rid: _root_sort_key(rid, root_order, element_info),
  ):
    root_node = _build_tree(root_id, 0)
    if root_node is not None:
      roots.append(root_node)

  return structure_id, structure_name, concept_arrangement, roots


def _root_sort_key(
  root_id: str,
  root_order: dict[str, float],
  element_info: dict[str, dict[str, Any]],
) -> tuple[float, int, str]:
  """Explicit root order, then debit-balance roots first (Assets before
  L+E), then qname — roots come from a set, so the tiebreak keeps it stable."""
  explicit = root_order.get(root_id, float("inf"))
  info = element_info.get(root_id, {})
  bt_priority = 0 if info.get("balance_type") == "debit" else 1
  return (explicit, bt_priority, info.get("qname") or "")


def _load_root_order(
  session: Session,
  structure_id: str,
) -> dict[str, float]:
  """Root ordering, stored as presentation arcs from the structure_id itself."""
  result = session.execute(
    text("""
      SELECT to_element_id, order_value
      FROM associations
      WHERE structure_id = :structure_id
        AND from_element_id = :structure_id
        AND association_type = 'presentation'
      ORDER BY order_value
    """),
    {"structure_id": structure_id},
  )
  return {row.to_element_id: row.order_value for row in result}


def _load_calculations(
  session: Session,
  structure_id: str | None = None,
  element_ids: set[str] | None = None,
) -> dict[str, list[tuple[str, float]]]:
  """Calculation arcs as subtotal element_id -> [(summand element_id, weight)].

  With ``structure_id``: calcs authored inside that structure. With
  ``element_ids``: calcs from any structure whose subtotal is in the set,
  composed into one DAG.
  """
  if structure_id is not None:
    result = session.execute(
      text("""
        SELECT from_element_id, to_element_id, weight
        FROM associations
        WHERE structure_id = :structure_id
          AND association_type = 'calculation'
        ORDER BY order_value
      """),
      {"structure_id": structure_id},
    )
  elif element_ids:
    placeholders = ", ".join(f":e{i}" for i in range(len(element_ids)))
    params = {f"e{i}": eid for i, eid in enumerate(element_ids)}
    result = session.execute(
      text(f"""
        SELECT structure_id, from_element_id, to_element_id, weight
        FROM associations
        WHERE association_type = 'calculation'
          AND from_element_id IN ({placeholders})
        ORDER BY structure_id, order_value
      """),
      params,
    )
  else:
    return {}

  calculations: dict[str, list[tuple[str, float]]] = {}

  if structure_id is not None:
    for row in result:
      weight = row.weight if row.weight is not None else 1.0
      calculations.setdefault(row.from_element_id, []).append(
        (row.to_element_id, weight)
      )
    return calculations

  # Several calc structures may target the same element as alternative
  # arrangements (merging them double-counts). Keep only arrangements whose
  # summands are all in the hierarchy, then pick one per target.
  assert element_ids is not None  # narrowed by the elif above
  by_struct_target: dict[tuple[str, str], list[tuple[str, float]]] = {}
  for row in result:
    weight = row.weight if row.weight is not None else 1.0
    by_struct_target.setdefault((row.structure_id, row.from_element_id), []).append(
      (row.to_element_id, weight)
    )

  candidates_per_target: dict[str, list[tuple[str, list[tuple[str, float]]]]] = {}
  for (sid, target), sources in by_struct_target.items():
    if not all(src_id in element_ids for src_id, _ in sources):
      continue
    candidates_per_target.setdefault(target, []).append((sid, sources))

  for target, candidates in candidates_per_target.items():
    # Most summands wins: the smallest variant is usually an identity check
    # (Assets = LiabilitiesAndEquity), meant for validation, not computation.
    candidates.sort(key=lambda c: (-len(c[1]), c[0]))
    calculations[target] = candidates[0][1]
  return calculations


def _balance_value(
  balances: dict[str, _Balance],
  element_id: str,
  pre_signed: bool,
  balance_type: str,
) -> float | None:
  """The element's balance, or ``None`` when absent (distinct from a present 0.0,
  which callers prefer over a derived rollup)."""
  balance = balances.get(element_id)
  if balance is None:
    return None
  if pre_signed:
    return balance.net_balance
  return _natural_sign(balance.net_balance, balance_type)


# ASC 205-20 redundant-subtotal suppression (see _build_rows); IS-only concepts.
_CONTINUING_OPS_QNAME = "rs-gaap:IncomeLossFromContinuingOperations"
_DISCONTINUED_OPS_QNAME = "rs-gaap:IncomeLossFromDiscontinuedOperationsNetOfTax"


def _build_rows(
  hierarchy: list[_HierarchyNode],
  period_balances: list[dict[str, _Balance]],
  calculations: dict[str, list[tuple[str, float]]],
  pre_signed: bool = False,
) -> list[FactRow]:
  """Build FactRows (one value per period) in presentation order.

  Pass 1 collects leaf balances and parent rollups, then calc subtotals are
  resolved; pass 2 emits rows post-order.
  """
  n_periods = len(period_balances)

  # Keyed on presence, not non-zero, so a legitimate 0 wins over a rollup.
  computed_per_period: list[dict[str, float]] = [{} for _ in range(n_periods)]
  present_per_period: list[set[str]] = [set() for _ in range(n_periods)]

  def _collect(node: _HierarchyNode) -> tuple[list[float], list[bool]]:
    """(values, presence) per period; a direct fact, even 0, beats a child rollup."""
    if node.children:
      child_totals = [0.0] * n_periods
      child_present = [False] * n_periods
      for child in node.children:
        child_vals, child_pres = _collect(child)
        for i in range(n_periods):
          child_totals[i] += child_vals[i]
          child_present[i] = child_present[i] or child_pres[i]
      vals: list[float] = []
      present: list[bool] = []
      for i in range(n_periods):
        direct = _balance_value(
          period_balances[i], node.element_id, pre_signed, node.balance_type
        )
        if direct is not None:
          chosen, is_present = direct, True
        elif child_present[i]:
          chosen, is_present = child_totals[i], True
        else:
          chosen, is_present = 0.0, False
        computed_per_period[i][node.element_id] = chosen
        if is_present:
          present_per_period[i].add(node.element_id)
        vals.append(chosen)
        present.append(is_present)
      return vals, present
    else:
      vals = []
      present = []
      for i in range(n_periods):
        v = _balance_value(
          period_balances[i], node.element_id, pre_signed, node.balance_type
        )
        if v is not None:
          computed_per_period[i][node.element_id] = v
          present_per_period[i].add(node.element_id)
          vals.append(v)
          present.append(True)
        else:
          computed_per_period[i][node.element_id] = 0.0
          vals.append(0.0)
          present.append(False)
      return vals, present

  for root in hierarchy:
    _collect(root)

  # Calcs chain, so resolve in dependency order.
  for elem_id in topo_sort_calculations(calculations):
    sources = calculations[elem_id]
    for i in range(n_periods):
      # A pass-1 value is authoritative; the calc is only the fallback.
      if elem_id in present_per_period[i]:
        continue
      computed = sum(
        computed_per_period[i].get(src_id, 0.0) * weight for src_id, weight in sources
      )
      computed_per_period[i][elem_id] = computed
      if any(src_id in present_per_period[i] for src_id, _ in sources):
        present_per_period[i].add(elem_id)

  # Pass 2: post-order, so each subtotal follows its details.
  rows: list[FactRow] = []

  calc_targets = set(calculations.keys())

  def _emit(node: _HierarchyNode) -> None:
    for child in node.children or []:
      _emit(child)
    # An abstract over several subtotals heads a calc cascade whose children
    # overlap; summing them double-counts, so it renders value-less.
    subtotal_children = sum(
      1
      for child in (node.children or [])
      if child.children or child.element_id in calc_targets
    )
    if node.is_abstract and subtotal_children > 1:
      vals: list[float | None] = [None] * n_periods
    else:
      vals = [
        _unsigned_zero(computed_per_period[i].get(node.element_id, 0.0))
        for i in range(n_periods)
      ]
    # Calc targets count too: their summands may be siblings, not children.
    is_subtotal = bool(node.children) or node.element_id in calc_targets
    rows.append(
      FactRow(
        element_id=node.element_id,
        element_qname=node.qname,
        element_name=node.name,
        classification=node.classification,
        balance_type=node.balance_type,
        values=vals,
        is_subtotal=is_subtotal,
        is_abstract=node.is_abstract,
        depth=node.depth,
      )
    )

  for root in hierarchy:
    _emit(root)

  # The full presentation tree is hundreds of concepts; keep only populated rows.
  rows = [r for r in rows if any(v not in (None, 0, 0.0) for v in r.values)]

  # ASC 205-20: without discontinued ops, the continuing-operations subtotal
  # duplicates net income.
  if not any(r.element_qname == _DISCONTINUED_OPS_QNAME for r in rows):
    rows = [r for r in rows if r.element_qname != _CONTINUING_OPS_QNAME]

  return rows


def _natural_sign(net_balance: float, balance_type: str) -> float:
  """Debits - credits to natural sign: credit-normal balances flip."""
  if balance_type == "credit":
    # A zero balance has no sign: ``-(0.0)`` is ``-0.0``, which compares
    # equal to zero but formats as "-$0.00" on a statement.
    return -net_balance if net_balance else 0.0
  return net_balance


def _unsigned_zero(value: float | None) -> float | None:
  """Normalize ``-0.0`` so no upstream sign flip renders "-$0.00"."""
  return 0.0 if value == 0.0 else value
