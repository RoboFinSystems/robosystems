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
class ReportFacts:
  """All facts generated for a report."""

  facts: list[ReportFact]
  current_period: tuple[date, date]
  prior_period: tuple[date, date] | None
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
  current_value: float
  prior_value: float | None = None
  is_subtotal: bool = False
  depth: int = 0


@dataclass
class FactGrid:
  """Rendered financial statement — facts viewed through a structure."""

  structure_id: str
  structure_name: str
  structure_type: str
  period_start: date
  period_end: date
  comparative_period_start: date | None = None
  comparative_period_end: date | None = None
  rows: list[FactRow] = field(default_factory=list)
  unmapped_count: int = 0


# ── Phase 1: Generate facts (structure-agnostic) ─────────────────────────


def generate_report_facts(
  session: Session,
  taxonomy_id: str,
  mapping_id: str,
  period_start: date,
  period_end: date,
  comparative: bool = True,
) -> ReportFacts:
  """Generate facts for all mapped elements across current and prior periods.

  Returns structure-agnostic facts — the raw data points that can be
  slotted into any structure's hierarchy for rendering.

  Args:
      session: Extensions database session (search_path set to tenant schema).
      taxonomy_id: Taxonomy identifier (e.g., "tax_usgaap_reporting").
      mapping_id: Structure ID for the CoA→GAAP mapping.
      period_start: Start of the reporting period.
      period_end: End of the reporting period.
      comparative: Whether to generate prior period facts too.

  Returns:
      ReportFacts with all generated facts and metadata.
  """
  # Read mapped trial balance for current period
  current_balances = _read_mapped_balances(
    session, mapping_id, period_start, period_end
  )

  # Convert to ReportFact objects
  facts: list[ReportFact] = []
  for balance in current_balances.values():
    facts.append(
      ReportFact(
        element_id=balance.element_id,
        element_qname=balance.qname,
        element_name=balance.name,
        classification=balance.classification,
        balance_type=balance.balance_type,
        value=_natural_sign(balance.net_balance, balance.balance_type),
        period_start=period_start,
        period_end=period_end,
        period_type=_infer_period_type(balance.classification),
      )
    )

  # Close temporary accounts into retained earnings for the current period.
  # Net Income = sum(revenue) - sum(expenses), added to retained earnings
  # so the balance sheet balances (A = L + E).
  _close_to_retained_earnings(facts, period_start, period_end)

  # Generate prior period facts
  prior_start = None
  prior_end = None
  if comparative:
    prior_start, prior_end = _compute_prior_period(period_start, period_end)
    prior_balances = _read_mapped_balances(session, mapping_id, prior_start, prior_end)
    for balance in prior_balances.values():
      facts.append(
        ReportFact(
          element_id=balance.element_id,
          element_qname=balance.qname,
          element_name=balance.name,
          classification=balance.classification,
          balance_type=balance.balance_type,
          value=_natural_sign(balance.net_balance, balance.balance_type),
          period_start=prior_start,
          period_end=prior_end,
          period_type=_infer_period_type(balance.classification),
        )
      )

    _close_to_retained_earnings(facts, prior_start, prior_end)

  unmapped_count = _count_unmapped(session, mapping_id)

  return ReportFacts(
    facts=facts,
    current_period=(period_start, period_end),
    prior_period=(prior_start, prior_end) if comparative and prior_start else None,
    unmapped_count=unmapped_count,
    taxonomy_id=taxonomy_id,
    mapping_id=mapping_id,
  )


# ── Phase 2: Render structure view ───────────────────────────────────────


def render_structure_view(
  session: Session,
  facts: list[ReportFact],
  structure_type: str,
  period_start: date,
  period_end: date,
  comparative_period_start: date | None = None,
  comparative_period_end: date | None = None,
) -> FactGrid:
  """Apply a structure's hierarchy to raw facts to produce a rendered view.

  This is the "lens" — same facts, different structure = different view.

  Args:
      session: Extensions database session.
      facts: Pre-generated ReportFact objects (from generate_report_facts).
      structure_type: Structure type to render (income_statement, balance_sheet, etc.).
      period_start: Current period start.
      period_end: Current period end.
      comparative_period_start: Prior period start (None if no comparative).
      comparative_period_end: Prior period end (None if no comparative).

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
      period_start=period_start,
      period_end=period_end,
      comparative_period_start=comparative_period_start,
      comparative_period_end=comparative_period_end,
    )

  # Build balance dicts from facts (keyed by element_id)
  current_balances = _facts_to_balance_dict(facts, period_start, period_end)
  prior_balances: dict[str, _Balance] = {}
  if comparative_period_start and comparative_period_end:
    prior_balances = _facts_to_balance_dict(
      facts, comparative_period_start, comparative_period_end
    )

  # Walk hierarchy depth-first with rollup
  # Facts from the facts table are already natural-signed, so skip sign conversion
  rows = _build_rows(hierarchy, current_balances, prior_balances, pre_signed=True)

  return FactGrid(
    structure_id=structure_id,
    structure_name=structure_name,
    structure_type=structure_type,
    period_start=period_start,
    period_end=period_end,
    comparative_period_start=comparative_period_start,
    comparative_period_end=comparative_period_end,
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
      JOIN element_associations mapping
        ON mapping.from_element_id = source_elem.id
        AND mapping.association_type = 'mapping'
        AND mapping.structure_id = :mapping_id
      JOIN elements target ON target.id = mapping.to_element_id
      WHERE e.status = 'posted'
        AND (e.posting_date >= :start_date OR :start_date IS NULL)
        AND (e.posting_date <= :end_date OR :end_date IS NULL)
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
          SELECT 1 FROM element_associations ea
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
      FROM element_associations ea
      JOIN elements e ON e.id = ea.to_element_id
      WHERE ea.structure_id = :structure_id
        AND ea.association_type = 'presentation'
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

  # Build trees from roots
  roots = []
  for root_id in sorted(root_parent_ids):
    roots.append(_build_tree(root_id, 0))

  return structure_id, structure_name, roots


def _build_rows(
  hierarchy: list[_HierarchyNode],
  current_balances: dict[str, _Balance],
  prior_balances: dict[str, _Balance],
  pre_signed: bool = False,
) -> list[FactRow]:
  """Walk the hierarchy depth-first, building FactRows with rollup subtotals.

  Returns rows in presentation order (top-to-bottom as they'd appear
  on a financial statement).
  """
  rows: list[FactRow] = []

  def _walk(node: _HierarchyNode) -> tuple[float, float | None]:
    """Walk a node, return (current_total, prior_total) for rollup."""
    has_prior = bool(prior_balances)

    if node.children:
      # Abstract/parent node: sum children
      child_current_total = 0.0
      child_prior_total = 0.0 if has_prior else None

      # First, walk children to get their values
      child_rows_start = len(rows)
      for child in node.children:
        c_val, p_val = _walk(child)
        child_current_total += c_val
        if has_prior and p_val is not None:
          child_prior_total = (child_prior_total or 0.0) + p_val

      # Insert subtotal row before children (section header)
      header_row = FactRow(
        element_id=node.element_id,
        element_qname=node.qname,
        element_name=node.name,
        classification=node.classification,
        balance_type=node.balance_type,
        current_value=child_current_total,
        prior_value=child_prior_total,
        is_subtotal=True,
        depth=node.depth,
      )
      rows.insert(child_rows_start, header_row)

      return child_current_total, child_prior_total
    else:
      # Leaf node: get value from balances
      balance = current_balances.get(node.element_id)
      if balance:
        current_val = (
          balance.net_balance
          if pre_signed
          else _natural_sign(balance.net_balance, node.balance_type)
        )
      else:
        current_val = 0.0

      prior_val = None
      if has_prior:
        prior_balance = prior_balances.get(node.element_id)
        if prior_balance:
          prior_val = (
            prior_balance.net_balance
            if pre_signed
            else _natural_sign(prior_balance.net_balance, node.balance_type)
          )
        else:
          prior_val = 0.0

      rows.append(
        FactRow(
          element_id=node.element_id,
          element_qname=node.qname,
          element_name=node.name,
          classification=node.classification,
          balance_type=node.balance_type,
          current_value=current_val,
          prior_value=prior_val,
          is_subtotal=False,
          depth=node.depth,
        )
      )

      return current_val, prior_val

  for root in hierarchy:
    _walk(root)

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
