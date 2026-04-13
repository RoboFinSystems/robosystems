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
  depth: int = 0


@dataclass
class FactGrid:
  """Rendered financial statement — facts viewed through a structure."""

  structure_id: str
  structure_name: str
  structure_type: str
  periods: list[PeriodSpec]
  rows: list[FactRow] = field(default_factory=list)
  unmapped_count: int = 0


# ── Phase 1: Generate facts (structure-agnostic) ─────────────────────────


def generate_report_facts(
  session: Session,
  taxonomy_id: str,
  mapping_id: str,
  periods: list[PeriodSpec],
  closed_through: date | None = None,
) -> ReportFacts:
  """Generate facts for all mapped elements across N periods.

  Returns structure-agnostic facts — the raw data points that can be
  slotted into any structure's hierarchy for rendering.

  Args:
      session: Extensions database session (search_path set to tenant schema).
      taxonomy_id: Taxonomy identifier (e.g., "tax_usgaap_reporting").
      mapping_id: Structure ID for the CoA→GAAP mapping.
      periods: Ordered list of period specifications.
      closed_through: Last calendar day that has been real-closed in the
          ledger (derived from FiscalCalendar.closed_through_period). When
          set, the synthetic retained-earnings adjustments are bounded so
          that periods ≤ closed_through are trusted as-is and cumulative
          P&L is only computed for the range (closed_through, period_end].
          This prevents double-counting prior earnings on books that
          already carry a real RE balance.

  Returns:
      ReportFacts with all generated facts and metadata.
  """
  facts: list[ReportFact] = []

  # Safety detection for ledgers without an initialized FiscalCalendar.
  # If the ledger already carries real postings to Retained Earnings (QB
  # opening-balance entry, prior-year close from another system, etc.) we
  # derive an effective closed_through from the latest such posting. This
  # bounds the synthetic prior-period adjustment so it only covers the
  # un-closed window — preventing double-count while still allowing
  # current-period net income to flow into RE for open-period BS accuracy.
  if closed_through is None:
    closed_through = _derive_closed_through_from_ledger(session, mapping_id)

  # If the fiscal calendar declares "closed through X" but the ledger has
  # NO actual RE postings backing that claim, the calendar is aspirational
  # — a user set close state without posting real closing entries. Drop
  # the marker so the synthetic close runs from inception and the balance
  # sheet actually balances. This is the demo case: an evergreen seed
  # with initialize_ledger(closed_through=...) but no historical close
  # entries. Real tenants with posted closes (QB or roboledger) won't hit
  # this branch because _derive_closed_through_from_ledger returns a date.
  if closed_through is not None and not _ledger_has_re_postings(session, mapping_id):
    closed_through = None

  for period in periods:
    balances = _read_mapped_balances(session, mapping_id, period.start, period.end)
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

    # If the period is entirely within the closed range, trust the ledger's
    # existing RE balance — real closing entries have already moved NI into
    # equity and the cumulative query would double-count them.
    if closed_through is not None and period.end <= closed_through:
      continue

    # Close temporary accounts into retained earnings per period
    _close_to_retained_earnings(facts, period.start, period.end)

    # For balance sheet accuracy: close prior-period net income into RE.
    # QB only formally closes to RE at year-end, so cumulative BS accounts
    # are missing prior periods' net income. Compute cumulative IS for the
    # un-closed window (from day after closed_through, or from inception
    # when no marker is set) and add the difference to RE.
    _close_prior_periods_to_retained_earnings(
      session,
      mapping_id,
      facts,
      period.start,
      period.end,
      closed_through=closed_through,
    )

  unmapped_count = _count_unmapped(session, mapping_id)

  return ReportFacts(
    facts=facts,
    periods=periods,
    unmapped_count=unmapped_count,
    taxonomy_id=taxonomy_id,
    mapping_id=mapping_id,
  )


# ── Phase 2: Render structure view ───────────────────────────────────────


def render_structure_view(
  session: Session,
  facts: list[ReportFact],
  structure_type: str,
  periods: list[PeriodSpec],
) -> FactGrid:
  """Apply a structure's hierarchy to raw facts to produce a rendered view.

  This is the "lens" — same facts, different structure = different view.

  Args:
      session: Extensions database session.
      facts: Pre-generated ReportFact objects (from generate_report_facts).
      structure_type: Structure type to render (income_statement, balance_sheet, etc.).
      periods: Ordered list of period specifications for columns.

  Returns:
      FactGrid with rows ordered per the structure's hierarchy.
  """
  # Load structure hierarchy
  structure_id, structure_name, hierarchy = _load_reporting_structure(
    session, structure_type
  )

  if not hierarchy:
    return FactGrid(
      structure_id=structure_id or "",
      structure_name=structure_name or "",
      structure_type=structure_type,
      periods=periods,
    )

  # Build balance dicts per period (keyed by element_id)
  period_balances = [_facts_to_balance_dict(facts, p.start, p.end) for p in periods]

  # Load calculation associations for computed elements (Total Assets, Net Income, etc.)
  calculations = _load_calculations(session, structure_id)

  # Walk hierarchy depth-first
  # Facts from the facts table are already natural-signed, so skip sign conversion
  rows = _build_rows(hierarchy, period_balances, calculations, pre_signed=True)

  return FactGrid(
    structure_id=structure_id,
    structure_name=structure_name,
    structure_type=structure_type,
    periods=periods,
    rows=rows,
  )


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
  """
  balances: dict[str, _Balance] = {}
  for fact in facts:
    if fact.period_start == period_start and fact.period_end == period_end:
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
  return balances


def _read_mapped_balances(
  session: Session,
  mapping_id: str,
  period_start: date,
  period_end: date,
) -> dict[str, _Balance]:
  """Read mapped trial balance — same join as the /trial-balance/mapped endpoint."""
  result = session.execute(
    text("""
      SELECT
        target.id AS reporting_element_id,
        target.qname,
        target.name AS reporting_name,
        target.classification,
        target.balance_type,
        COALESCE(SUM(li.debit_amount), 0) AS total_debits,
        COALESCE(SUM(li.credit_amount), 0) AS total_credits
      FROM elements source_elem
      JOIN line_items li ON li.element_id = source_elem.id
      JOIN entries e ON e.id = li.entry_id
      JOIN associations mapping
        ON mapping.from_element_id = source_elem.id
        AND mapping.association_type = 'mapping'
        AND mapping.structure_id = :mapping_id
      JOIN elements target ON target.id = mapping.to_element_id
      WHERE e.status = 'posted'
        AND (e.posting_date <= :end_date OR :end_date IS NULL)
        AND (
          target.classification IN ('asset', 'liability', 'equity')
          OR e.posting_date >= :start_date
          OR :start_date IS NULL
        )
      GROUP BY target.id, target.qname, target.name,
               target.classification, target.balance_type
      ORDER BY target.qname
    """),
    {
      "mapping_id": mapping_id,
      "start_date": period_start,
      "end_date": period_end,
    },
  )

  balances: dict[str, _Balance] = {}
  for row in result:
    debits = cents_to_dollars(row.total_debits)
    credits = cents_to_dollars(row.total_credits)
    balances[row.reporting_element_id] = _Balance(
      element_id=row.reporting_element_id,
      qname=row.qname,
      name=row.reporting_name,
      classification=row.classification,
      balance_type=row.balance_type,
      total_debits=debits,
      total_credits=credits,
      net_balance=debits - credits,
    )

  return balances


def _count_unmapped(session: Session, mapping_id: str) -> int:
  """Count CoA elements that have no mapping association."""
  from robosystems.models.extensions.roboledger import COA_SOURCES

  # Build a safe SQL IN clause from the constant
  source_list = ", ".join(f"'{s}'" for s in COA_SOURCES)
  result = session.execute(
    text(f"""
      SELECT COUNT(*) AS cnt
      FROM elements e
      WHERE e.source IN ({source_list})
        AND e.is_active = true
        AND NOT EXISTS (
          SELECT 1 FROM associations ea
          WHERE ea.from_element_id = e.id
            AND ea.association_type = 'mapping'
            AND ea.structure_id = :mapping_id
        )
    """),
    {"mapping_id": mapping_id},
  )
  row = result.fetchone()
  return row.cnt if row else 0


def _compute_prior_period(period_start: date, period_end: date) -> tuple[date, date]:
  """Compute the prior period of equal length ending the day before period_start."""
  duration = (period_end - period_start).days + 1
  prior_end = period_start - timedelta(days=1)
  prior_start = prior_end - timedelta(days=duration - 1)
  return prior_start, prior_end


def _close_to_retained_earnings(
  facts: list[ReportFact],
  period_start: date,
  period_end: date,
) -> None:
  """Close temporary accounts (revenue/expense) into retained earnings.

  Computes net income = sum(revenue facts) - sum(expense facts) for the
  given period and adds it to the retained earnings fact. This is the
  standard period-end closing entry that ensures the balance sheet balances.

  Mutates the facts list in place.
  """
  # Deterministic ID from config/taxonomy/seed.py: _elem("retained_earnings", ...)
  RETAINED_EARNINGS_ID = "elem_gaap_retained_earnings"

  total_revenue = 0.0
  total_expenses = 0.0
  retained_earnings_fact: ReportFact | None = None

  for fact in facts:
    if fact.period_start != period_start or fact.period_end != period_end:
      continue
    if fact.classification == "revenue":
      total_revenue += fact.value
    elif fact.classification == "expense":
      total_expenses += fact.value
    if fact.element_id == RETAINED_EARNINGS_ID:
      retained_earnings_fact = fact

  net_income = total_revenue - total_expenses

  if retained_earnings_fact is not None:
    retained_earnings_fact.value += net_income
  elif net_income != 0.0:
    # No retained earnings fact yet — create one
    facts.append(
      ReportFact(
        element_id=RETAINED_EARNINGS_ID,
        element_qname="us-gaap:RetainedEarningsAccumulatedDeficit",
        element_name="Retained Earnings",
        classification="equity",
        balance_type="credit",
        value=net_income,
        period_start=period_start,
        period_end=period_end,
        period_type="instant",
      )
    )


def _ledger_has_re_postings(
  session: Session,
  mapping_id: str,
) -> bool:
  """True if any posted line item flows (via the mapping) into Retained Earnings.

  Used by `generate_report_facts` to detect aspirational-vs-real calendar
  state: if the fiscal calendar says "closed through X" but the ledger has
  no postings hitting RE, the calendar is unbacked and we fall back to
  synthesizing prior-period net income from inception.
  """
  RETAINED_EARNINGS_ID = "elem_gaap_retained_earnings"
  row = session.execute(
    text("""
      SELECT 1
      FROM line_items li
      JOIN entries e ON e.id = li.entry_id
      JOIN associations mapping
        ON mapping.from_element_id = li.element_id
        AND mapping.association_type = 'mapping'
        AND mapping.structure_id = :mapping_id
      WHERE e.status = 'posted'
        AND mapping.to_element_id = :re_id
        AND (li.debit_amount > 0 OR li.credit_amount > 0)
      LIMIT 1
    """),
    {"mapping_id": mapping_id, "re_id": RETAINED_EARNINGS_ID},
  ).fetchone()
  return row is not None


def _derive_closed_through_from_ledger(
  session: Session,
  mapping_id: str,
) -> date | None:
  """Infer an effective closed_through date from real Retained Earnings postings.

  When no FiscalCalendar exists, we still want to avoid double-counting
  prior earnings on books that already carry a real RE balance. The
  latest posting_date of any entry that flows (via the CoA→GAAP mapping)
  into retained_earnings gives a safe lower bound — periods ≤ that date
  are trusted as-is, while later periods still run the synthetic close
  so current-period open balance sheets reflect fresh net income.

  Returns None when the ledger has no real RE postings; callers should
  fall back to the legacy "from inception" behavior in that case.
  """
  RETAINED_EARNINGS_ID = "elem_gaap_retained_earnings"
  row = session.execute(
    text("""
      SELECT MAX(e.posting_date) AS last_re_posting
      FROM line_items li
      JOIN entries e ON e.id = li.entry_id
      JOIN associations mapping
        ON mapping.from_element_id = li.element_id
        AND mapping.association_type = 'mapping'
        AND mapping.structure_id = :mapping_id
      WHERE e.status = 'posted'
        AND mapping.to_element_id = :re_id
        AND (li.debit_amount > 0 OR li.credit_amount > 0)
    """),
    {"mapping_id": mapping_id, "re_id": RETAINED_EARNINGS_ID},
  ).fetchone()
  return row.last_re_posting if row and row.last_re_posting else None


def _close_prior_periods_to_retained_earnings(
  session: Session,
  mapping_id: str,
  facts: list[ReportFact],
  period_start: date,
  period_end: date,
  closed_through: date | None = None,
) -> None:
  """Close un-closed prior-period net income into retained earnings.

  Balance sheet accounts are loaded cumulatively, but the existing
  _close_to_retained_earnings() only closes the current period's
  revenue/expense. This function computes cumulative net income from
  (day after closed_through) through period_end — or from inception
  when no closed_through marker is provided — subtracts the current
  period's net income (already closed), and adds the remainder to RE.

  The `closed_through` lower bound is critical: without it, books that
  already carry a real RE balance (e.g., a QuickBooks opening-balance
  entry plus historical P&L transactions) would double-count all prior
  earnings because the ledger's RE value already reflects them.
  """
  RETAINED_EARNINGS_ID = "elem_gaap_retained_earnings"

  # Compute cumulative net income for the un-closed window.
  # If closed_through is set, lower bound is the day after it; otherwise
  # start from inception (start_date IS NULL).
  lower_bound = closed_through + timedelta(days=1) if closed_through else None
  result = session.execute(
    text("""
      SELECT
        target.classification,
        target.balance_type,
        COALESCE(SUM(li.debit_amount), 0) AS total_debits,
        COALESCE(SUM(li.credit_amount), 0) AS total_credits
      FROM elements source_elem
      JOIN line_items li ON li.element_id = source_elem.id
      JOIN entries e ON e.id = li.entry_id
      JOIN associations mapping
        ON mapping.from_element_id = source_elem.id
        AND mapping.association_type = 'mapping'
        AND mapping.structure_id = :mapping_id
      JOIN elements target ON target.id = mapping.to_element_id
      WHERE e.status = 'posted'
        AND e.posting_date <= :end_date
        AND (:start_date IS NULL OR e.posting_date >= :start_date)
        AND target.classification IN ('revenue', 'expense')
      GROUP BY target.classification, target.balance_type
    """),
    {
      "mapping_id": mapping_id,
      "end_date": period_end,
      "start_date": lower_bound,
    },
  )

  cumulative_revenue = 0.0
  cumulative_expenses = 0.0
  for row in result:
    net = cents_to_dollars(row.total_debits - row.total_credits)
    natural = _natural_sign(net, row.balance_type)
    if row.classification == "revenue":
      cumulative_revenue += natural
    else:
      cumulative_expenses += natural

  cumulative_net_income = cumulative_revenue - cumulative_expenses

  # Current period net income was already closed — compute it from facts
  current_revenue = 0.0
  current_expenses = 0.0
  retained_earnings_fact: ReportFact | None = None
  for fact in facts:
    if fact.period_start != period_start or fact.period_end != period_end:
      continue
    if fact.classification == "revenue":
      current_revenue += fact.value
    elif fact.classification == "expense":
      current_expenses += fact.value
    if fact.element_id == RETAINED_EARNINGS_ID:
      retained_earnings_fact = fact

  current_net_income = current_revenue - current_expenses
  prior_periods_net_income = cumulative_net_income - current_net_income

  if prior_periods_net_income == 0.0:
    return

  if retained_earnings_fact is not None:
    retained_earnings_fact.value += prior_periods_net_income
  else:
    facts.append(
      ReportFact(
        element_id=RETAINED_EARNINGS_ID,
        element_qname="us-gaap:RetainedEarningsAccumulatedDeficit",
        element_name="Retained Earnings",
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
) -> tuple[str, str, list[_HierarchyNode]]:
  """Load the reporting structure hierarchy for the given report type.

  Returns (structure_id, structure_name, root_nodes).
  """
  # Find the structure for this report type
  struct_result = session.execute(
    text("""
      SELECT id, name FROM structures
      WHERE structure_type = :report_type
      LIMIT 1
    """),
    {"report_type": report_type},
  )
  struct_row = struct_result.fetchone()
  if not struct_row:
    return "", "", []

  structure_id = struct_row.id
  structure_name = struct_row.name

  # Load all elements and associations for this structure
  assoc_result = session.execute(
    text("""
      SELECT
        ea.from_element_id AS parent_id,
        ea.to_element_id AS child_id,
        e.id AS element_id,
        e.qname,
        e.name,
        e.classification,
        e.balance_type,
        e.is_abstract,
        e.depth,
        ea.order_value
      FROM associations ea
      JOIN elements e ON e.id = ea.to_element_id
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

  # Load root element info
  if root_parent_ids:
    placeholders = ", ".join(f":p{i}" for i in range(len(root_parent_ids)))
    params = {f"p{i}": pid for i, pid in enumerate(root_parent_ids)}
    root_result = session.execute(
      text(f"""
        SELECT id, qname, name, classification, balance_type, is_abstract, depth
        FROM elements WHERE id IN ({placeholders})
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

  def _build_tree(element_id: str, depth: int) -> _HierarchyNode:
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
      node.children.append(child_node)
    return node

  # Build trees from roots, ordered by the root-ordering associations
  # seeded as (structure → SFAC6 root) presentation associations.
  # Roots that have an explicit order_value sort by that; others sort last.
  root_order = _load_root_order(session, structure_id)

  roots = []
  for root_id in sorted(
    root_parent_ids,
    key=lambda rid: root_order.get(rid, float("inf")),
  ):
    roots.append(_build_tree(root_id, 0))

  return structure_id, structure_name, roots


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
  structure_id: str,
) -> dict[str, list[tuple[str, float]]]:
  """Load calculation associations for a structure.

  Returns a dict mapping computed element ID → list of (source_element_id, weight).
  For example, Total Assets might map to [(Current Assets, 1.0), (Non-Current Assets, 1.0)].
  """
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

  calculations: dict[str, list[tuple[str, float]]] = {}
  for row in result:
    weight = row.weight if row.weight is not None else 1.0
    calculations.setdefault(row.from_element_id, []).append((row.to_element_id, weight))
  return calculations


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
    """Walk a node, return list of values (one per period) for rollup."""
    if node.children:
      child_totals = [0.0] * n_periods
      for child in node.children:
        child_vals = _collect(child)
        for i in range(n_periods):
          child_totals[i] += child_vals[i]
      for i in range(n_periods):
        computed_per_period[i][node.element_id] = child_totals[i]
      return child_totals
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
  # Iterate in definition order; each resolved value is stored so
  # downstream calculations can reference it.
  for elem_id, sources in calculations.items():
    for i in range(n_periods):
      computed_per_period[i][elem_id] = sum(
        computed_per_period[i].get(src_id, 0.0) * weight for src_id, weight in sources
      )

  # Pass 2: build rows in presentation order
  rows: list[FactRow] = []

  def _emit(node: _HierarchyNode) -> None:
    # Both subtotal (has children) and leaf rows read the precomputed
    # value from `computed_per_period`. Pass 1 populated it with the
    # rolled-up sum of all descendants for parent nodes, so subtotal
    # rows get the correct aggregate instead of zeros.
    vals = [computed_per_period[i].get(node.element_id, 0.0) for i in range(n_periods)]
    rows.append(
      FactRow(
        element_id=node.element_id,
        element_qname=node.qname,
        element_name=node.name,
        classification=node.classification,
        balance_type=node.balance_type,
        values=vals,
        is_subtotal=bool(node.children),
        depth=node.depth,
      )
    )
    for child in node.children or []:
      _emit(child)

  for root in hierarchy:
    _emit(root)

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
