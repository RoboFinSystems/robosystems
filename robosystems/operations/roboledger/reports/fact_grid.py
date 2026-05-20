"""Report fact generation and structure rendering.

Two-phase design:
1. generate_report_facts() — reads mapped trial balance, produces structure-agnostic
   ReportFact objects (one per element x period). These get written to the
   facts OLTP table for graph materialization.
2. render_structure_view() — applies a structure's hierarchy to pre-generated facts,
   computing subtotals and ordering for display. Same facts, different structure =
   different view.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.api.extensions import cents_to_dollars

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
  """Pick which CoA→target arc-type to walk for fact generation.

  Returns ``mapping`` unconditionally under the rs-gaap-anchored
  architecture (roadmap §3.15). Each CoA element carries both ``mapping``
  (CoA → rs-gaap leaf) and ``equivalence`` (cross-taxonomy bridge) arcs;
  the rs-gaap reporting layer follows ``mapping``. Hook kept as a
  function for per-taxonomy dispatch when custom tenant taxonomies need
  ``equivalence``-direct rendering.
  """
  return "mapping"


def generate_report_facts(
  session: Session,
  taxonomy_id: str,
  mapping_id: str,
  periods: list[PeriodSpec],
) -> ReportFacts:
  """Generate facts for all mapped elements across N periods.

  Returns structure-agnostic facts — the raw data points that can be
  slotted into any structure's hierarchy for rendering.

  Args:
      session: Extensions database session (search_path set to tenant schema).
      taxonomy_id: Taxonomy identifier (e.g., "tax_usgaap_reporting").
      mapping_id: Structure ID for the CoA→GAAP mapping.
      periods: Ordered list of period specifications.

  Returns:
      ReportFacts with all generated facts and metadata.
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
        )
      )

    # Materialize zero-balance facts for mapped equity targets that
    # didn't appear in `balances` because their source CoA element has
    # no GL postings yet. Without this, `_find_close_target` can't see
    # a RetainedEarnings-shaped target — even though the mapping points
    # at one — and falls back to dumping net income onto whatever other
    # equity fact exists (typically APIC), which produces wildly wrong
    # equity values. Pre-seeding the empty equity facts gives the close
    # logic a stable target.
    _append_empty_equity_facts(
      session, mapping_id, facts, period.start, period.end, arc_type=arc_type
    )

    # Close the current period's temporary accounts (revenue/expense)
    # into retained earnings. For periods where real closing entries
    # have already zeroed the rev/exp accounts, this is a no-op (sum=0).
    _close_to_retained_earnings(facts, period.start, period.end)

    # For balance sheet accuracy: add cumulative prior-period net income
    # to RE. This always runs from inception — real closing entries
    # (from QB year-end closes, roboledger close_period, etc.) already
    # zero out the rev/exp accounts they close, so cumulative rev - exp
    # returns only the still-unclosed portion. Adding that to whatever
    # RE balance the ledger already carries is always correct.
    _close_prior_periods_to_retained_earnings(
      session, mapping_id, facts, period.start, period.end, arc_type=arc_type
    )

  # Emit rs-gaap:NetIncomeLoss facts for each period. The close logic
  # only rolls net income into RE; for the IS bottom-line row AND for the
  # CF Operating calc rollup (NetIncomeLoss is its first child), we need
  # NetIncomeLoss as its own fact. Persistence fan-out (per
  # commands/reports.py::_persist_report_facts) then stamps the same fact
  # into each owning structure's FactSet — IS sees it as the bottom line,
  # CF sees it as a calc input.
  _emit_net_income_facts(session, facts, periods)

  # Synthesize rs-gaap:PropertyPlantAndEquipmentNet = Gross - AccumulatedDepreciation
  # for each period. Tenants who map PP&E with a gross + contra-asset split
  # (the standard accounting setup that lets CF Investing read ΔGross as
  # purchases instead of conflating it with depreciation) won't have a
  # direct PPE Net fact — synthesize it so BS still renders the net carrying
  # value. Skipped when a direct PPE Net fact already exists.
  _synthesize_ppe_net_facts(session, facts, periods)

  # Derive Cash Flow facts from period-over-period BS deltas (indirect
  # method). Each derivation arc encodes "this CF leaf is the change in
  # this BS source element" with a sign weight for the
  # asset-up=cash-use / liability-up=cash-source convention. Runs after
  # the per-period loop because each derivation reads both the current
  # and prior period's BS values.
  _derive_cash_flow_facts(session, facts, periods)

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
  """Apply a structure's hierarchy to raw facts to produce a rendered view.

  This is the "lens" — same facts, different structure = different view.

  Facts whose ``element_id`` isn't in the structure's hierarchy are
  resolved upward via type-subtype ``general-special`` arcs to the
  nearest in-structure ancestor (see ``_resolve_renderable_ancestor``)
  and aggregated there. Facts with no in-structure ancestor are dropped
  from the rendered view — they're persisted in ``facts`` for audit
  but invisible to the standard report.

  Args:
      session: Extensions database session.
      facts: Pre-generated ReportFact objects (from generate_report_facts).
      block_type: Structure type to render (income_statement, balance_sheet, etc.).
      periods: Ordered list of period specifications for columns.
      reporting_style_id: The graph's Reporting Style id
          (``Graph.reporting_style_id``). Resolves which Network this
          statement type renders against via the §3.2 picker.

  Returns:
      FactGrid with rows ordered per the structure's hierarchy.
  """
  # Load the Network the Style composes for this statement type
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

  # Resolve out-of-structure facts to their nearest in-structure ancestor.
  in_structure = _collect_hierarchy_element_ids(hierarchy)
  rolled_up = _roll_up_facts_to_structure(session, facts, in_structure)

  # Build balance dicts per period (keyed by element_id, summing across
  # facts that resolved to the same ancestor).
  period_balances = [_facts_to_balance_dict(rolled_up, p.start, p.end) for p in periods]

  # Load calculation arcs. For ``arithmetic`` Disclosures we compose
  # calcs across taxonomies (fac-calculations + rs-gaap-calculations +
  # any others) — load all calcs whose subtotal target appears in the
  # Disclosure's element set. Other CAPs use the legacy single-structure
  # behavior so existing rendering paths don't change.
  if concept_arrangement == "arithmetic":
    calculations = _load_calculations(session, element_ids=in_structure)
  else:
    calculations = _load_calculations(session, structure_id=structure_id)

  # Walk hierarchy depth-first
  # Facts from the facts table are already natural-signed, so skip sign conversion
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
  """Walk a hierarchy tree and return every element_id reachable.

  Used by ``render_structure_view`` to identify in-structure elements
  so out-of-structure facts can be resolved to a renderable ancestor.
  """
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
  """Walk anchor arcs upward from ``element_id`` until reaching an
  element in ``in_structure``. Returns the ancestor's element_id, or
  ``None`` if no ancestor is reachable.

  Three arc types are followed (these are the recognized
  CoA-to-reporting bridges):

  - ``equivalence`` — explicit owl-style "this concept IS that one";
    the auto-mapper writes one of these from each tenant CoA element
    to its rs-gaap leaf equivalent
  - ``mapping`` — broader category placement; auto-mapper writes one
    of these from each CoA element to its FAC anchor (e.g.,
    fac:Revenues, fac:OperatingExpenses)
  - ``general-special`` — class-subtype hierarchy (type-subtype,
    rs-gaap-hierarchy); used to roll a specialized rs-gaap concept up
    to its in-Disclosure ancestor

  All three are walked together via BFS-by-depth so the nearest
  ancestor wins when multiple paths exist (semantic 'a' from Option C
  planning — deterministic, mirrors XBRL renderer convention). The
  cache memoizes per call so multiple facts on the same out-of-
  structure element only walk once.
  """
  if element_id in cache:
    return cache[element_id]
  if element_id in in_structure:
    cache[element_id] = element_id
    return element_id

  visited: set[str] = {element_id}
  frontier: list[str] = [element_id]

  while frontier:
    # Two arc-direction conventions both walk "upward" from a tenant
    # CoA element / specific concept toward an in-Disclosure anchor:
    #
    # - ``general-special``: from = general (parent), to = specific
    #   (child). Walk: where to = child, return from = parent.
    # - ``mapping`` / ``equivalence``: from = specific (CoA), to =
    #   anchor (FAC concept / rs-gaap leaf). Walk: where from = child,
    #   return to = parent. The auto-mapper writes both kinds with this
    #   direction.
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
  """For each fact whose element isn't in the rendered structure,
  resolve to the nearest in-structure ancestor and emit a rewritten
  ReportFact pointing at that ancestor.

  Facts with no in-structure ancestor are dropped (they're audit-only
  data — invisible to the standard report). Facts whose element is
  already in-structure pass through unchanged.

  ``_facts_to_balance_dict`` sums multiple facts on the same
  element_id, so this function doesn't aggregate — it just rewrites
  the element pointers.
  """
  if not facts:
    return facts

  cache: dict[str, str | None] = {}
  rolled: list[ReportFact] = []
  for fact in facts:
    if fact.element_id in in_structure:
      rolled.append(fact)
      continue
    ancestor = _resolve_renderable_ancestor(
      session, fact.element_id, in_structure, cache
    )
    if ancestor is None:
      continue
    # Reuse fact metadata; only the element_id pointer changes.
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
  """Convert ReportFact list to balance dict for a specific period.

  Facts already have natural-sign values, so we store them directly
  as net_balance. The _build_rows walker reads current_value from
  _natural_sign(balance.net_balance, node.balance_type), so we set
  balance_type to "debit" to pass through the value unchanged (since
  natural sign was already applied during fact generation).

  Multiple facts on the same ``element_id`` for the same period sum —
  needed for the ancestor-rollup path where many out-of-structure
  facts can resolve to a single in-structure ancestor.
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


# rs-gaap concepts that represent equity-reducing cash flows. These appear
# on the Statement of Equity and Cash Flow Statement as separate line items,
# but their cumulative effect also has to net out of Retained Earnings on
# the Balance Sheet. They're flow concepts (period_type='duration') that
# rs-gaap models with balance_type='credit' (XBRL outflow-as-negative
# convention), so they don't pick up the 'equity' EFS classification trait
# automatically. Detection by qname is more reliable than trait inference.
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
  """True if a fact's concept represents an equity-reducing cash flow.

  Dividends paid, distributions to members, treasury stock buybacks —
  these reduce retained earnings on the balance sheet even though they
  render on the SE / CF as their own line items. See the constant
  above for the curated list of recognized rs-gaap concepts.
  """
  return qname in _EQUITY_FLOW_REDUCER_QNAMES


def _infer_classification(qname: str | None, balance_type: str | None) -> str | None:
  """Best-effort classification fallback for elements lacking FASB traits.

  Reference taxonomies (FAC, rs-gaap, type-subtype) and freshly-loaded
  custom taxonomies often have no ``element_traits`` rows pointing at
  ``traits.category='elementsOfFinancialStatements'``. Without
  classification, ``_close_to_retained_earnings`` can't compute Net
  Income (revenue/expense facts never match) and the BS doesn't balance.

  This heuristic restores classification from the qname + balance_type
  pair using conventional naming. It returns one of
  ``asset/liability/equity/revenue/expense`` or ``None`` when nothing
  matches confidently. Real ``element_traits`` always win — this only
  fires when the SQL join returned NULL.
  """
  if not qname:
    return None
  qn = qname.lower()
  bt = (balance_type or "").lower()

  # Revenue: credit balance + revenue/sales/income token (excluding
  # liability-shaped "income tax payable" — keyed on balance_type).
  if bt == "credit" and any(t in qn for t in ("revenue", "sales")):
    return "revenue"
  # Expense: debit balance + expense/cost/loss/depreciation token.
  if bt == "debit" and any(
    t in qn for t in ("expense", "cost", "loss", "depreciation", "amortization")
  ):
    return "expense"
  # Equity must be tested before liability — "stockholdersequity" contains
  # "equity" and is a credit, but a liability check would also match
  # "stockholders" tokens in some pathological qnames.
  if bt == "credit" and any(
    t in qn for t in ("equity", "capital", "retainedearnings", "stockholder")
  ):
    return "equity"
  if bt == "debit" and "asset" in qn:
    return "asset"
  if bt == "credit" and "liabilit" in qn:
    return "liability"

  # Weak fallback: abstract / rollup container elements often have a
  # ``balance_type`` that doesn't match the classification of what they
  # aggregate (e.g. FAC's ``fac:LiabilitiesRollUp`` has balance_type
  # ``debit`` even though it rolls up credit-balance liabilities). For
  # these qname-only is the only signal. Order matters — check equity
  # before liability so combined "LiabilitiesAndEquity" rollups don't
  # misclassify as equity (the validator skips combined rollups
  # explicitly via the qname check).
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
  """Read mapped trial balance — same join as the /trial-balance/mapped endpoint.

  ``arc_type`` selects which CoA→target arc-type to follow:
  ``'mapping'`` (CoA→FAC, default for fac-presentation reports) or
  ``'equivalence'`` (CoA→rs-gaap, for rs-gaap-presentation reports).

  ``classification`` is resolved via ``element_traits`` →
  ``classifications`` with ``category='elementsOfFinancialStatements'``
  (the FASB SFAC 6 trait axis). Balance-sheet classifications (asset /
  liability / equity) are stock concepts and must be loaded
  cumulatively; IS / SCF items are flows and constrain by
  ``posting_date >= :start_date``.

  When the trait join returns ``NULL`` (reference taxonomies whose
  elements aren't wired to FASB traits), :func:`_infer_classification`
  fills in best-effort classification from qname + balance_type. Real
  trait data always wins; the fallback only fires for null rows.

  Belt-and-suspenders ``element_type='concept'`` filter on the target
  ensures facts never land on abstracts, hypercubes, axes, or members
  even if a future bad mapping arc points there.
  """
  result = session.execute(
    text("""
      SELECT
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
      WHERE e.status = 'posted'
        AND target.element_type = 'concept'
        AND target.is_abstract = false
        AND (e.posting_date <= :end_date OR :end_date IS NULL)
        AND (
          tcls.identifier IN ('asset', 'liability', 'equity')
          OR e.posting_date >= :start_date
          OR :start_date IS NULL
        )
      GROUP BY target.id, target.qname, target.name,
               tcls.identifier, target.balance_type
      ORDER BY target.qname
    """),
    {
      "mapping_id": mapping_id,
      "arc_type": arc_type,
      "start_date": period_start,
      "end_date": period_end,
    },
  )

  balances: dict[str, _Balance] = {}
  for row in result:
    debits = cents_to_dollars(row.total_debits)
    credits = cents_to_dollars(row.total_credits)
    classification = row.classification or _infer_classification(
      row.qname, row.balance_type
    )
    balances[row.reporting_element_id] = _Balance(
      element_id=row.reporting_element_id,
      qname=row.qname,
      name=row.reporting_name,
      classification=classification,
      balance_type=row.balance_type,
      total_debits=debits,
      total_credits=credits,
      net_balance=debits - credits,
    )

  return balances


def _append_empty_equity_facts(
  session: Session,
  mapping_id: str,
  facts: list[ReportFact],
  period_start: date,
  period_end: date,
  arc_type: str = "mapping",
) -> None:
  """Append zero-balance facts for mapped equity targets without postings.

  Equity targets are stock concepts (period_type='instant') — they should
  appear on the BS even when their source CoA element has no current
  postings. The close-to-RE flow specifically needs the RE-shaped target
  to exist as a fact so `_find_close_target` can route net income to
  it. Without this, RE silently disappears and net income lands on
  whatever other equity fact is present (typically APIC).
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

  # Always materialize rs-gaap:RetainedEarningsAccumulatedDeficit at $0
  # even when no source CoA element maps to it. This is the QuickBooks /
  # Xero pattern: RE is a *derived* concept, computed from cumulative
  # (revenue - expense - dividends) at render time, not a posted GL
  # balance from period-end closing journal entries. Without this, mini
  # / FAC / simple CoAs that omit an explicit RE concept can't carry
  # net income onto the BS - `_close_to_retained_earnings` falls back
  # to an anonymous fact whose element_id isn't in any presentation
  # network and silently disappears from the rendered statement.
  re_row = session.execute(
    text(
      """
      SELECT id, qname, name, balance_type
      FROM elements
      WHERE qname = 'rs-gaap:RetainedEarningsAccumulatedDeficit'
      LIMIT 1
      """
    )
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
  """Synthesize one ``rs-gaap:NetIncomeLoss`` fact per period.

  ``_close_to_retained_earnings`` rolls (revenue - expense) into RE
  but never emits NetIncomeLoss as its own fact. Two consumers need it
  as a standalone fact: the Income Statement (where it's the bottom-line
  row) and the Cash Flow Operating calc rollup (where it's the first
  calc child of NetCashProvidedByUsedInOperatingActivities). Emit it
  here once per period; the persistence fan-out then stamps the same
  fact into every structure that references the element.

  Skips zero net income — the renderer treats absent facts as 0 anyway.
  Mutates the facts list in place.
  """
  ni_row = session.execute(
    text("SELECT id, balance_type FROM elements WHERE qname='rs-gaap:NetIncomeLoss'")
  ).fetchone()
  if ni_row is None:
    return
  ni_id, ni_balance_type = ni_row[0], ni_row[1] or "credit"

  for period in periods:
    # Skip if a NetIncomeLoss fact already exists for this period — a
    # tenant might map a CoA element directly to rs-gaap:NetIncomeLoss
    # (rare but legal), in which case the direct fact wins.
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
        revenue += f.value
      elif f.classification == "expense":
        expense += f.value
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


def _synthesize_ppe_net_facts(
  session: Session,
  facts: list[ReportFact],
  periods: list[PeriodSpec],
) -> None:
  """Synthesize ``rs-gaap:PropertyPlantAndEquipmentNet`` per period as
  ``PropertyPlantAndEquipmentGross - AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment``.

  When a tenant maps PP&E with a gross + contra-asset split (so 1300/1310-type
  fixed-asset accounts → Gross, 1350-type accumulated-depreciation account →
  AD), there's no direct PPE Net fact for the BS to render. Computing it as
  Gross - AD here keeps the BS correct AND lets the CF Investing derivation
  source from Gross directly (ΔGross = purchases, cleanly isolated from
  depreciation activity which flows through DDA on the Operating side).

  Skipped when a direct PPE Net fact already exists for the period — tenants
  using the simpler "all-in PPE Net" mapping (1300 + 1350 both → PPE Net)
  still work because their direct fact wins.

  Mutates the facts list in place.
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

  # Resolve source element ids once.
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
      # Direct PPE Net fact exists — likely the legacy "all-in-Net"
      # mapping (1300/1310/1350 all → PropertyPlantAndEquipmentNet). The
      # BS renders correctly via the direct fact, BUT the CF Investing
      # derivation now sources from PropertyPlantAndEquipmentGross (per
      # rs-gaap-calculations) and will produce 0 for PaymentsToAcquirePPE
      # — silently omitting capital expenditures from CF Investing.
      # Warn so operators can migrate to the split mapping.
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
      # AD present but no Gross — would synthesize Net = -AD, which is
      # arithmetically wrong (no asset to depreciate against). Indicates
      # a misconfigured mapping; warn and skip.
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


def _derive_cash_flow_facts(
  session: Session,
  facts: list[ReportFact],
  periods: list[PeriodSpec],
) -> None:
  """Synthesize CF facts from period-over-period BS deltas (indirect method).

  Each ``association_type='derivation'`` arc declares "this CF leaf is
  the change in this BS source element" with a signed weight:

  - ``IncreaseDecreaseInAccountsReceivable derivationOf ReceivablesNetCurrent (w=-1)``
    (asset up = cash use)
  - ``IncreaseDecreaseInAccountsPayableAndAccruedLiabilities derivationOf
    AccountsPayableAndAccruedLiabilitiesCurrent (w=+1)``
    (liability up = cash source)

  For each period after the first, compute
  ``cf_value = sum(weight * (BS_current - BS_prior))`` across all arcs
  that target each CF leaf, and append a synthetic
  ``ReportFact(period_type='duration')`` covering that period.

  Zero-value derivations are skipped — keeps the rendered CF clean for
  tenants whose BS hasn't moved on a given line. The renderer's calc
  DAG (``rs-gaap:NetCashProvidedByUsedInOperatingActivities = Σ
  derivation outputs + NetIncome + DDA``) does the upward roll-up.

  Mutates the facts list in place.
  """
  if len(periods) < 2:
    # Indirect-method CF derivation needs a prior period to delta against.
    # Caller should pass comparative=True (or supply explicit periods) when
    # they want CF rows; logging at debug so an empty CF block isn't a
    # mystery in logs.
    logger.debug(
      "_derive_cash_flow_facts: skipped — indirect method needs ≥2 periods (got %d)",
      len(periods),
    )
    return

  # Period lists arrive in presentation order (typically newest-first:
  # [Current, Prior]). Sort chronologically so periods[i] / periods[i-1]
  # means "current / prior" in real time, not list-order. Without this
  # the deltas come out negated AND the synthesized CF facts get tagged
  # with the wrong period.
  ordered = sorted(periods, key=lambda p: p.end)

  # Load every derivation arc — intentionally global (no structure_id /
  # taxonomy_id filter). Derivation arcs are library-seeded into a
  # dedicated structure per (cf_leaf, source) pair and the library
  # immutability trigger blocks tenants from inserting their own. If
  # tenant-authored derivations land later (§3.16 Phase 4), this query
  # will need a scope (Reporting Style or taxonomy_id).
  rows = session.execute(
    text("""
      SELECT from_element_id, to_element_id, weight
      FROM associations
      WHERE association_type = 'derivation'
    """)
  ).fetchall()
  if not rows:
    return

  derivations: dict[str, list[tuple[str, float]]] = {}
  for cf_id, source_id, weight in rows:
    derivations.setdefault(cf_id, []).append((source_id, float(weight or 1.0)))

  # Element metadata for the CF leaves we'll synthesize
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

  # Index existing facts by (element_id, period_end) for delta lookup.
  # Sum on collision — multiple facts on the same (element, period) is
  # already tolerated by `_facts_to_balance_dict` (it sums net_balance);
  # do the same here so no future caller silently overwrites an upstream
  # baseline and drops half the CF delta.
  fact_index: dict[tuple[str, date], float] = {}
  for f in facts:
    key = (f.element_id, f.period_end)
    fact_index[key] = fact_index.get(key, 0.0) + f.value

  for i in range(1, len(ordered)):
    current = ordered[i]
    prior = ordered[i - 1]
    for cf_leaf_id, sources in derivations.items():
      # Skip if a direct fact already exists for this CF leaf at the
      # current period — direct fact wins, derivation is the fallback.
      # Avoids double-counting when a tenant maps both source paths
      # (e.g. Depreciation Expense → DDA fact directly, AND Accumulated
      # Depreciation → DDA via ΔBS derivation).
      if (cf_leaf_id, current.end) in fact_index:
        continue
      cf_value = 0.0
      for source_id, weight in sources:
        current_v = fact_index.get((source_id, current.end), 0.0)
        prior_v = fact_index.get((source_id, prior.end), 0.0)
        cf_value += weight * (current_v - prior_v)
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
          classification=None,  # CF leaves don't fit asset/liab/eq/rev/exp axes
          balance_type=balance_type,
          value=cf_value,
          period_start=current.start,
          period_end=current.end,
          period_type="duration",
        )
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


def _compute_prior_period(period_start: date, period_end: date) -> tuple[date, date]:
  """Compute the prior period of equal length ending the day before period_start."""
  duration = (period_end - period_start).days + 1
  prior_end = period_start - timedelta(days=1)
  prior_start = prior_end - timedelta(days=duration - 1)
  return prior_start, prior_end


# Anonymous element id used when no rs-gaap RE fact is present in the
# mapping graph and the close logic has to append a fresh row. Reachable
# only on legacy / unmapped graphs; properly-configured tenants always
# have rs-gaap:RetainedEarningsAccumulatedDeficit materialized via
# _append_empty_equity_facts before the close runs.
_ANON_RE_ELEMENT_ID = "elem_rsgaap_retained_earnings_anon"


def _find_close_target(
  facts: list[ReportFact],
  period_start: date,
  period_end: date,
) -> ReportFact | None:
  """Find the rs-gaap RetainedEarnings fact to receive the closing entry.

  Looks for an equity-classified fact whose qname matches
  ``*RetainedEarnings*`` or ``*RetainedDeficit*`` — under the
  rs-gaap-anchored architecture the canonical target is
  ``rs-gaap:RetainedEarningsAccumulatedDeficit``, materialized at $0
  by :func:`_append_empty_equity_facts` when the source CoA element has
  no postings. Returns ``None`` if no such fact exists; caller appends
  a fresh row (defensive fallback for unmapped graphs).
  """
  for fact in facts:
    if fact.period_start != period_start or fact.period_end != period_end:
      continue
    if fact.classification != "equity":
      continue
    qname_lower = (fact.element_qname or "").lower()
    if "retainedearnings" in qname_lower or "retaineddeficit" in qname_lower:
      return fact

  return None


def _close_to_retained_earnings(
  facts: list[ReportFact],
  period_start: date,
  period_end: date,
) -> None:
  """Close the current period's revenue/expense into retained earnings.

  Computes net income = sum(revenue facts) - sum(expense facts) for the
  given period and adds it to the rs-gaap:RetainedEarningsAccumulatedDeficit
  fact materialized by :func:`_append_empty_equity_facts`. On
  unmapped graphs where no RE fact exists, appends a fresh anonymous row
  with the canonical rs-gaap qname so downstream rendering still has a
  bottom line.

  Mutates the facts list in place.
  """
  total_revenue = 0.0
  total_expenses = 0.0
  # Equity-flow concepts (dividends paid, treasury stock buybacks, etc.)
  # affect retained earnings on the BS but don't appear with classification
  # 'equity' in element_traits — they're flow-on-equity concepts that
  # rs-gaap models with balance_type='credit' (per XBRL outflow-as-negative
  # convention). Detect them by qname pattern. Their stored fact.value is
  # already natural-signed negative (dividends present as -$2,500), so we
  # add it directly without a sign flip — without this term the BS
  # misbalances by exactly the cumulative dividend / buyback amount.
  total_equity_reductions = 0.0

  for fact in facts:
    if fact.period_start != period_start or fact.period_end != period_end:
      continue
    if fact.classification == "revenue":
      total_revenue += fact.value
    elif fact.classification == "expense":
      total_expenses += fact.value
    elif _is_equity_flow_reducer(fact.element_qname):
      total_equity_reductions += fact.value

  net_income = total_revenue - total_expenses + total_equity_reductions
  if net_income == 0.0:
    return

  target = _find_close_target(facts, period_start, period_end)
  if target is not None:
    target.value += net_income
    return

  # No rs-gaap RE fact in scope — append a fresh anonymous row so the
  # close amount is preserved even when the CoA isn't mapped to an
  # equity target yet. The reachability validator
  # (operations/roboledger/reads/taxonomies.py::check_mapping_reachability)
  # surfaces this gap to operators; this warning makes it visible at
  # render time too.
  logger.warning(
    "close_to_retained_earnings: no rs-gaap RE fact in scope for period "
    "%s..%s; appending anonymous fallback row. CoA is missing a mapping "
    "to an equity RetainedEarnings concept.",
    period_start,
    period_end,
  )
  facts.append(
    ReportFact(
      element_id=_ANON_RE_ELEMENT_ID,
      element_qname="rs-gaap:RetainedEarningsAccumulatedDeficit",
      element_name="Retained Earnings (Accumulated Deficit)",
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
) -> None:
  """Close un-closed cumulative net income into retained earnings.

  Balance sheet accounts are loaded cumulatively, but
  `_close_to_retained_earnings` only closes the current period's
  revenue/expense. This function computes cumulative net income **from
  inception** through `period_end`, subtracts the current period's net
  income (already closed by `_close_to_retained_earnings`), and adds
  the remainder to retained earnings.

  ## Why "from inception" is always correct

  A real closing entry (QB year-end, roboledger `close_period`, etc.)
  zeroes out the revenue/expense accounts it closes:

      DR Revenue 100k
      CR Expense  60k
      CR RE       40k

  After this entry, the revenue and expense accounts have net_balance=0,
  so the `cumulative rev - exp` query returns only the **still-unclosed**
  portion of P&L activity. Adding that to whatever RE the ledger already
  carries (real closed amount + any manual adjustments) always produces
  the right total on the balance sheet. There is no double-count risk.

  Falls through three close-target conventions (see :func:`_find_close_target`)
  so seed.py us-gaap, FAC, rs-gaap, and other equity-element shapes all
  receive the prior-period closing amount.
  """
  # Cumulative net income from inception through period_end.
  # Classification is resolved via element_traits → classifications
  # (FASB elementsOfFinancialStatements trait axis); rows whose target
  # element lacks a primary trait fall back to qname-based inference
  # (see :func:`_infer_classification`) so reference taxonomies (FAC,
  # rs-gaap) without wired traits don't get a phantom $0 cumulative
  # that would undo the current-period close.
  result = session.execute(
    text("""
      SELECT
        tcls.identifier AS classification,
        target.qname,
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
      WHERE e.status = 'posted'
        AND target.element_type = 'concept'
        AND target.is_abstract = false
        AND e.posting_date <= :end_date
      GROUP BY tcls.identifier, target.qname, target.balance_type
    """),
    {
      "mapping_id": mapping_id,
      "arc_type": arc_type,
      "end_date": period_end,
    },
  )

  cumulative_revenue = 0.0
  cumulative_expenses = 0.0
  # Equity-flow concepts (dividends paid, distributions, treasury buybacks)
  # reduce retained earnings on the balance sheet. Detect by qname pattern
  # rather than classification because rs-gaap models these as
  # balance_type='credit' flow concepts that often lack the 'equity' EFS
  # trait. Tracked separately so prior-period closing nets them out of RE.
  cumulative_equity_reductions = 0.0
  for row in result:
    if _is_equity_flow_reducer(row.qname):
      net = cents_to_dollars(row.total_debits - row.total_credits)
      cumulative_equity_reductions += _natural_sign(net, row.balance_type)
      continue
    classification = row.classification or _infer_classification(
      row.qname, row.balance_type
    )
    if classification not in ("revenue", "expense"):
      continue
    net = cents_to_dollars(row.total_debits - row.total_credits)
    natural = _natural_sign(net, row.balance_type)
    if classification == "revenue":
      cumulative_revenue += natural
    else:
      cumulative_expenses += natural

  cumulative_net_income = (
    cumulative_revenue - cumulative_expenses + cumulative_equity_reductions
  )

  # Current period net income was already closed — compute it from facts.
  # The closed amount is now sitting on whichever element
  # ``_find_close_target`` selected (seed.py RE, us-gaap-shaped RE, or
  # a single-line equity element like ``fac:Equity``); we don't need to
  # locate that fact here, only to compute the prior-period delta.
  current_revenue = 0.0
  current_expenses = 0.0
  current_equity_reductions = 0.0
  for fact in facts:
    if fact.period_start != period_start or fact.period_end != period_end:
      continue
    if fact.classification == "revenue":
      current_revenue += fact.value
    elif fact.classification == "expense":
      current_expenses += fact.value
    elif _is_equity_flow_reducer(fact.element_qname):
      current_equity_reductions += fact.value

  current_net_income = current_revenue - current_expenses + current_equity_reductions
  prior_periods_net_income = cumulative_net_income - current_net_income

  if prior_periods_net_income == 0.0:
    return

  target = _find_close_target(facts, period_start, period_end)
  if target is not None:
    target.value += prior_periods_net_income
  else:
    logger.warning(
      "close_prior_periods_to_retained_earnings: no rs-gaap RE fact in "
      "scope for period %s..%s; appending anonymous fallback row. "
      "CoA is missing a mapping to an equity RetainedEarnings concept.",
      period_start,
      period_end,
    )
    facts.append(
      ReportFact(
        element_id=_ANON_RE_ELEMENT_ID,
        element_qname="rs-gaap:RetainedEarningsAccumulatedDeficit",
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
  """Infer period type from element classification.

  Balance sheet items (asset, liability, equity) are instant (point-in-time).
  Income statement / cash flow items (revenue, expense) are duration.
  """
  if classification in ("asset", "liability", "equity"):
    return "instant"
  return "duration"


def _load_reporting_structure(
  session: Session,
  report_type: str,
  reporting_style_id: str,
) -> tuple[str, str, str | None, list[_HierarchyNode]]:
  """Load the reporting structure hierarchy for the given report type.

  Resolves the Network deterministically via the Reporting Style
  composition layer (§3.2 Phase 1) — the renderer never picks among
  same-typed Structures by recency / heuristics anymore. The Style's
  ``reporting_style_networks`` row pins exactly one Network per
  statement_type.

  Returns (structure_id, structure_name, concept_arrangement, root_nodes).
  ``concept_arrangement`` is Charlie's CAP declared on the Disclosure
  (``arithmetic`` / ``roll_up`` / ``roll_forward`` / ``set`` / ...);
  the renderer uses it to pick a compilation strategy. See
  information-block.md §3.2.1 for the canonical 15-value enumeration.

  When the Reporting Style doesn't compose a Network for this statement
  type, returns the empty tuple — callers treat that as "no statement
  to render". This matches the prior behaviour of "no matching structure
  row" so upstream code paths don't need to grow new error branches.
  """
  # Lazy import to avoid the picker's `from .. import` chain pulling
  # commands code into the reads layer at module-import time.
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

  # Load all elements and associations for this structure.
  # Classification is resolved via element_traits → classifications
  # (FASB elementsOfFinancialStatements trait axis).
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

  # Build parent → children map
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

  # Also load the parent elements (SFAC 6 roots) that appear as from_element_id
  # but not as to_element_id — these are the tree roots
  root_parent_ids: set[str] = set(children_map.keys()) - all_child_ids

  # Load root element info (classification via element_traits JOIN)
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

  # Render each element at most once globally per structure walk. The
  # rs-gaap-presentation hierarchy is a DAG (a concept may have
  # multiple parents — e.g. "Cash" rolls up under both Current Assets
  # and the Cash Flow reconciliation). Without global dedup the walk
  # would expand a shared subtree under each parent, producing
  # exponential row counts and double-counting facts at render time.
  # The first parent that reaches a node owns it; subsequent parents
  # treat the node as already-rendered.
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

  # Build trees from roots, ordered by the root-ordering associations
  # seeded as (structure → SFAC6 root) presentation associations.
  # Roots that have an explicit order_value sort by that; others fall
  # back to accounting convention (debit-balance roots first — Assets
  # before L+E on the Balance Sheet) and finally qname for determinism.
  #
  # ``root_parent_ids`` is a set, so without the secondary keys the sort
  # would preserve unstable hash-order: e.g. the rs-gaap BS Classified
  # structure has no root_order rows, and the set happened to emit
  # ``LiabilitiesAndStockholdersEquity`` before ``Assets``, producing a
  # BS rendered Liabilities-first. Single-root statements (IS / CF / SE)
  # are unaffected.
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
  """Sort key for multi-root presentation hierarchies.

  Three-tier precedence:

  1. ``root_order[root_id]`` — explicit ordering seeded as
     ``(structure → root)`` presentation associations on the structure.
     Wins when present (tenants can pin a specific layout per structure).
  2. ``balance_type`` priority — debit-balance roots before credit-balance
     roots. Produces conventional Assets-then-L+E on the Balance Sheet
     and any other multi-root statement that follows accounting
     convention, without requiring explicit root_order rows.
  3. ``qname`` alphabetical — determinism tiebreak so identical-priority
     roots always emit in the same order across runs.

  Single-root statements (IS / CF / SE under most reporting styles)
  never hit this path — there's nothing to sort.
  """
  explicit = root_order.get(root_id, float("inf"))
  info = element_info.get(root_id, {})
  bt_priority = 0 if info.get("balance_type") == "debit" else 1
  return (explicit, bt_priority, info.get("qname") or "")


def _load_root_order(
  session: Session,
  structure_id: str,
) -> dict[str, float]:
  """Load root-level ordering for SFAC6 roots within a structure.

  Root ordering is stored as presentation associations where from_element_id
  equals the structure_id (a convention for root-level ordering).
  """
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
  """Load calculation associations.

  Two modes:

  1. **Single-structure (legacy)** — pass ``structure_id``. Returns calcs
     authored INSIDE that structure (the older convention where presentation +
     calculation arcs lived in the same structure).
  2. **Cross-structure (Stage 2 disclosure rebuild)** — pass ``element_ids``
     (the set of element ids reachable in the Disclosure hierarchy). Returns
     calcs from ANY structure whose subtotal target (``from_element_id``) is
     in the hierarchy. This composes ``fac-calculations`` (FAC's 18 canonical
     equations) + ``rs-gaap-calculations`` (rs-gaap leaf→FAC summations) +
     any other calc structures into a single calc DAG for the renderer.

  Returns a dict mapping subtotal element_id → list of (summand element_id, weight).
  For example, Total Assets might map to [(Current Assets, 1.0), (Non-Current Assets, 1.0)].
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
    # Cross-structure load. Filter to calcs whose subtotal target lives
    # in the Disclosure — that's the set the renderer can actually
    # position. Calcs whose target is outside the hierarchy are
    # irrelevant for this rendering pass.
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

  # Cross-structure path: multiple calc structures may target the same
  # element (e.g. FAC IS2 multistep and IS11 single-step both compute
  # fac:OperatingIncomeLoss; BS2 and BS3 both compute fac:Assets). They
  # are alternative arrangements, not summands — merging them double-
  # counts. Pick exactly one structure per target by requiring every
  # summand to also live in the disclosure's hierarchy. The arrangement
  # whose inputs the disclosure actually carries is the one the
  # disclosure is asking the renderer to apply.
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
    # When multiple calc structures target the same element AND each
    # one's summands are all carried by the disclosure, pick the
    # decomposition with the MOST summands. The fewest-summands variant
    # is almost always an identity check — fac-calculations BS2 says
    # `Assets = LiabilitiesAndEquity`, which is meant for VALIDATION,
    # not COMPUTATION (Assets's value comes from summing real
    # current/noncurrent leaves, not from a tautology pointing at
    # another rollup). The decomposition (BS3: `Assets = Current +
    # Noncurrent`) is the calc that walks down to leaf data. For IS the
    # tie is moot — every variant has the same summand count after the
    # hierarchy filter — so structure_id breaks it deterministically.
    candidates.sort(key=lambda c: (-len(c[1]), c[0]))
    calculations[target] = candidates[0][1]
  return calculations


def _topo_sort_calculations(
  calculations: dict[str, list[tuple[str, float]]],
) -> list[str]:
  """Return calc subtotal targets in topological dependency order.

  When calcs chain (e.g., GrossProfit = Rev - COGS, then OperatingIncome =
  GrossProfit - OpEx, then NetIncome = OperatingIncome - Tax), the renderer
  must compute them in order so each depends on the resolved values of the
  prior ones. Returns target ids in the order they should be computed.
  Targets with no internal dependencies come first.
  """
  targets = set(calculations.keys())
  # Edge: target → target it depends on (when its summand is itself a
  # calc target). Inputs that are leaves (not calc targets) don't create
  # edges.
  deps: dict[str, set[str]] = {t: set() for t in targets}
  for target, sources in calculations.items():
    for src_id, _ in sources:
      if src_id in targets:
        deps[target].add(src_id)

  # Kahn's algorithm: emit nodes with no remaining deps; remove from graph.
  ready = [t for t, d in deps.items() if not d]
  ordered: list[str] = []
  while ready:
    n = ready.pop(0)
    ordered.append(n)
    for other, other_deps in deps.items():
      if n in other_deps:
        other_deps.discard(n)
        if not other_deps and other not in ordered and other not in ready:
          ready.append(other)
  # Any remaining targets indicate a cycle in the calc DAG — emit them
  # last in arbitrary order. The renderer's existing behavior is to use
  # whatever values are present, so a cycle just means one iteration of
  # stale values rather than a crash.
  for t in targets:
    if t not in ordered:
      ordered.append(t)
  return ordered


def _balance_value(
  balances: dict[str, _Balance],
  element_id: str,
  pre_signed: bool,
  balance_type: str,
) -> float:
  """Look up a balance value for an element, applying sign convention if needed."""
  balance = balances.get(element_id)
  if not balance:
    return 0.0
  if pre_signed:
    return balance.net_balance
  return _natural_sign(balance.net_balance, balance_type)


def _build_rows(
  hierarchy: list[_HierarchyNode],
  period_balances: list[dict[str, _Balance]],
  calculations: dict[str, list[tuple[str, float]]],
  pre_signed: bool = False,
) -> list[FactRow]:
  """Walk the hierarchy depth-first, building FactRows with N period columns.

  Two-pass approach:
  1. Walk all nodes to collect leaf balances and abstract rollups per period.
  2. Resolve calculation elements using the fully-populated computed dicts,
     then build the final row list in presentation order.

  Abstract/parent nodes render with the rollup sum of their children (not
  zero). Leaf nodes get values from the balance dict, or from calculation
  associations for computed elements (Total Assets, Net Income, etc.).
  """
  n_periods = len(period_balances)

  # Pass 1: collect all values per period (leaf balances + abstract rollups)
  # computed_per_period[period_idx][element_id] = value
  computed_per_period: list[dict[str, float]] = [{} for _ in range(n_periods)]

  def _collect(node: _HierarchyNode) -> list[float]:
    """Walk a node, return list of values (one per period) for rollup.

    A parent node can receive a value via two distinct reporting paths:
    (a) directly — a fact mapped at the parent's element_id (the auto-
    mapper writes one of these per CoA element to its FAC anchor like
    fac:Revenues), or (b) computed — sum of child values when the
    breakdown is reported at the leaves. Whichever path is populated
    in a given period wins; we never sum a real direct fact under empty
    leaves.
    """
    if node.children:
      child_totals = [0.0] * n_periods
      for child in node.children:
        child_vals = _collect(child)
        for i in range(n_periods):
          child_totals[i] += child_vals[i]
      vals = []
      for i in range(n_periods):
        direct = _balance_value(
          period_balances[i], node.element_id, pre_signed, node.balance_type
        )
        # Prefer the direct fact when present (mapping landed at this
        # FAC anchor); fall back to children sum (rs-gaap leaves
        # populated by equivalence path).
        chosen = direct if direct != 0.0 else child_totals[i]
        computed_per_period[i][node.element_id] = chosen
        vals.append(chosen)
      return vals
    else:
      vals = []
      for i in range(n_periods):
        v = _balance_value(
          period_balances[i], node.element_id, pre_signed, node.balance_type
        )
        computed_per_period[i][node.element_id] = v
        vals.append(v)
      return vals

  for root in hierarchy:
    _collect(root)

  # Resolve calculations (may chain: Gross Profit → Operating Income → Net Income).
  # Topologically sort by dependency so each subtotal is computed AFTER
  # the subtotals it depends on. Without this, a calc whose summand is
  # itself a calc target gets a stale value (or zero) when iterated in
  # dict insertion order.
  #
  # If a direct fact has already populated computed_per_period[i][elem_id]
  # in pass 1 (mapping arc landed at this anchor), prefer it over the
  # calc result — calc is the fallback path for subtotals not directly
  # reported, not an override of authoritative direct facts.
  for elem_id in _topo_sort_calculations(calculations):
    sources = calculations[elem_id]
    for i in range(n_periods):
      direct = computed_per_period[i].get(elem_id, 0.0)
      computed = sum(
        computed_per_period[i].get(src_id, 0.0) * weight for src_id, weight in sources
      )
      computed_per_period[i][elem_id] = direct if direct != 0.0 else computed

  # Pass 2: build rows in financial-statement order — children first,
  # then their parent subtotal (post-order). This matches the convention
  # readers expect: revenues / expenses listed before Gross Profit;
  # current / non-current sections before Total Assets; everything
  # before Net Income (which lands at the bottom of the IS). Pre-order
  # would put rollups at the top with details below, which reads as
  # an outline rather than a financial statement.
  rows: list[FactRow] = []

  calc_targets = set(calculations.keys())

  def _emit(node: _HierarchyNode) -> None:
    for child in node.children or []:
      _emit(child)
    # Both subtotal (has children) and leaf rows read the precomputed
    # value from `computed_per_period`. Pass 1 populated it with the
    # rolled-up sum of all descendants for parent nodes, so subtotal
    # rows get the correct aggregate instead of zeros.
    vals = [computed_per_period[i].get(node.element_id, 0.0) for i in range(n_periods)]
    # A row is a subtotal if it aggregates other rows by either path:
    # (a) it has child summands in the disclosure DAG, or (b) it is a
    # calc-DAG target whose summands live elsewhere in the disclosure
    # (e.g., fac:GrossProfit's summands fac:Revenues / fac:CostOfRevenue
    # are siblings, not children). The UI treats both the same way.
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

  # Drop rows whose every period value is zero / null. The full
  # rs-gaap presentation tree includes every concept the seed knows
  # about; for any given filer only a fraction carry data. Suppressing
  # the empty rows turns ~600-row trees into the dozen or two lines
  # that actually populate the statement, which is what readers expect
  # to see.
  rows = [r for r in rows if any(v not in (None, 0, 0.0) for v in r.values)]

  return rows


def _natural_sign(net_balance: float, balance_type: str) -> float:
  """Convert net balance (debits - credits) to natural sign for display.

  - Debit-normal accounts (assets, expenses): positive when net_balance > 0
  - Credit-normal accounts (liabilities, equity, revenue): positive when net_balance < 0
    (i.e., when credits exceed debits)

  This means Revenue shows as positive, Expenses show as positive,
  and Net Income = Revenue - Expenses works correctly.
  """
  if balance_type == "credit":
    return -net_balance
  return net_balance
