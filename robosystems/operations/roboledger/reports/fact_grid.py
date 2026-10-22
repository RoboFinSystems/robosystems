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
from robosystems.operations.roboledger.commands.opening_balances import books_start
from robosystems.operations.roboledger.entity_scope import membership_window
from robosystems.operations.roboledger.entry_status import (
  landed_entry_bindparam,
)
from robosystems.operations.roboledger.line_dimensions import (
  LINE_SLICE_SQL,
  LineSlice,
  slice_params,
)
from robosystems.operations.roboledger.reports.calc_dag import (
  EXTENDS_NETWORK_KEY,
  PPE_ACCUMULATED_DEPRECIATION_QNAME,
  PPE_GROSS_QNAME,
  PPE_NET_QNAME,
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
  # Natural-sign value per mapped source account, for a mapped balance fact;
  # None for derived facts.
  source_values: dict[str, float] | None = None
  # Each entity's contribution per source account, when the fact sums several
  # entities' pivots (a consolidated fact); None means ``source_values`` is
  # the fact's own entity's.
  source_shares: dict[tuple[str, str], float] | None = None
  # How the value was made — "pivot", "rollup" or "derived" — and what the
  # construction knew: a rollup's rule and children, a derivation's
  # computations, formula, source concepts and the amounts that make up the
  # value. None is unknown, and is stamped as nothing.
  origin: str | None = None
  derivation: dict | None = None


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
  # Findings made while generating (a flow no row presents); reported with
  # the rendered statement's validation, never failures.
  warnings: list[str] = field(default_factory=list)


def _mark_derived(
  fact: ReportFact,
  computation: str,
  *,
  amount: float | None = None,
  formula: str | None = None,
  source_concepts: tuple[str, ...] | list[str] = (),
) -> None:
  """Record that ``computation`` produced this fact, or is about to add
  ``amount`` into it: call before the addition, so the value the fact
  already had — pivoted, rolled up or derived — is kept as its own part and
  the parts always sum to the value."""
  derivation = dict(fact.derivation or {})
  computations = list(derivation.get("computations", []))
  if amount is not None:
    amounts = dict(derivation.get("amounts", {}))
    if not amounts and fact.origin is not None:
      amounts[_part_name(fact)] = fact.value
    amounts[computation] = amounts.get(computation, 0.0) + amount
    derivation["amounts"] = amounts
  if computation not in computations:
    computations.append(computation)
  derivation["computations"] = computations
  if formula:
    derivation["formula"] = formula
  if source_concepts:
    derivation["source_concepts"] = sorted(
      set(derivation.get("source_concepts", [])) | set(source_concepts)
    )
  fact.origin = "derived"
  fact.derivation = derivation


def _part_name(fact: ReportFact) -> str:
  """The name a fact's value goes under in ``amounts`` when something is
  added into it: ``pivot``, ``rollup:<rule>``, or what derived it."""
  derivation = fact.derivation or {}
  if fact.origin == "pivot":
    return "pivot"
  if fact.origin == "rollup":
    return f"rollup:{derivation.get('rule', 'calculation')}"
  return "+".join(derivation.get("computations", [])) or "derived"


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
  # The row's place in the Style's full presentation order — post-order over
  # every node, populated or not — so two grids of one Style merge by it even
  # when one of them kept none of the rows in between.
  position: float = 0.0
  # On a roll-forward, ``start`` / ``end`` mark the balance it opens and closes
  # on: one concept on two rows (XBRL's periodStart / periodEnd labels).
  period_role: str | None = None


@dataclass
class FactGrid:
  """Rendered financial statement — facts viewed through a structure."""

  structure_id: str
  structure_name: str
  block_type: str
  periods: list[PeriodSpec]
  rows: list[FactRow] = field(default_factory=list)
  unmapped_count: int = 0
  warnings: list[str] = field(default_factory=list)


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
  entity_id: str | None = None,
  line_slice: LineSlice | None = None,
  acquired_on: date | None = None,
  left_on: date | None = None,
) -> ReportFacts:
  """Generate structure-agnostic facts for all mapped elements across periods.

  ``entity_id`` restricts the pivot to that entity's entries; every caller
  that reports for an entity passes it. ``close_target_qname`` is the equity
  concept cumulative earnings close to — the Reporting Style's earnings home
  (``PartnersCapital`` / ``MembersEquity`` for PART/LLC) — so earnings land on
  the form's capital line and the BS foots.

  A column starting before the books' first day carries only its balances,
  as a live statement leaves it out: that holds for a column that ends
  inside the books too (a calendar year for books opening in July), whose
  flows would be the months since the opening labelled as the whole year.

  ``line_slice`` sums only one management dimension member's lines (or the
  axis's Unallocated). A slice states the income statement: the mapped
  balances, net income and the subtotals, never the cash flow or equity
  derivations, whose cash and equity lines are not tagged.

  ``acquired_on`` is the eve a group read's member opened its books on when
  it was acquired. In the column opening the day after, the cash it held
  then is the group's cash acquired, an investing inflow, and the column
  opens on none of it, so the group's cash flow opens where its previous
  month closed.

  ``left_on`` is the last day a group read's member that left is read on
  (the end of its last month). The column opening the day after is read
  for its cash alone: the column's own facts are not the group's and are
  dropped, its balances at the column's end held at none, and the cash it
  held on ``left_on`` is the group's cash divested, an investing outflow,
  so the group's cash flow opens where its previous month closed and moves
  to a close without the member.
  """
  arc_type = _arc_type_for_taxonomy(session, taxonomy_id)
  facts: list[ReportFact] = []
  # Columns reaching back before the books are computed, since later columns
  # measure change from their balances, but never plugged or reported.
  start = _books_start(session, entity_id)
  covered = [p for p in periods if start is None or p.start >= start]

  for period in periods:
    facts.extend(
      _period_balance_facts(
        session,
        mapping_id,
        period.start,
        period.end,
        arc_type=arc_type,
        close_target_qname=close_target_qname,
        entity_id=entity_id,
        line_slice=line_slice,
      )
    )

  # NetIncomeLoss as its own fact: the IS bottom line and the first child
  # of the CF Operating rollup.
  _emit_net_income_facts(session, facts, periods)

  if line_slice is not None:
    _emit_subtotal_facts(session, facts, periods)
    if start is not None:
      facts = [f for f in facts if _within_the_books(f, start)]
    return ReportFacts(
      facts=facts,
      periods=periods,
      unmapped_count=_count_unmapped(session, mapping_id, arc_type=arc_type),
      taxonomy_id=taxonomy_id,
      mapping_id=mapping_id,
      warnings=[],
    )

  # PPE Net = Gross - AccumulatedDepreciation for gross + contra mappings;
  # skipped when a direct PPE Net fact exists.
  _synthesize_ppe_net_facts(session, facts, periods)

  # Investing/financing CF facts from per-line flow concepts. Must run
  # before _derive_cash_flow_facts so its "direct fact wins" guard skips
  # the ΔBS derivation for these leaves.
  flow_ids = _emit_flow_facts(session, facts, periods, mapping_id, arc_type, entity_id)

  # The equity statement's movement rows, from every equity line, cash or not.
  _emit_equity_movement_facts(
    session, facts, periods, mapping_id, arc_type, entity_id, close_target_qname
  )
  _emit_nci_movement_facts(session, facts, periods, mapping_id, arc_type, entity_id)

  # Balances the day before each period starts, where that is not the previous
  # column's end (year-over-year, YTD, rolling). The cash flow measures change
  # from a period's opening, not from whichever column precedes it. Kept out of
  # ``facts`` so they are never rendered.
  opening_facts = _load_opening_facts(session, mapping_id, periods, arc_type, entity_id)
  if acquired_on is not None:
    _acquired_cash_as_a_flow(session, facts, opening_facts, periods, acquired_on)

  # Operating CF from BS deltas (indirect method); needs every period's BS.
  derived = _derive_cash_flow_facts(session, facts, periods, opening_facts)

  # Entries with no cash line, and payments settled through payables: the
  # non-cash part comes out of operating, and a settled bill's capex or debt
  # reaches its own section. Adjusts the leaves derived above.
  settlement_flows, settlement_warnings = _emit_noncash_and_settlement_facts(
    session, facts, periods, mapping_id, arc_type, entity_id, derived
  )
  warnings = _flows_without_a_row(
    session, facts, flow_ids | settlement_flows, entity_id
  )
  warnings.extend(settlement_warnings)

  # Book ΔCash - Investing - Financing - (NI + DDA + ΔWC) on an operating
  # leaf so the CF foots to actual cash. Must precede the subtotal roll-up.
  _reconcile_operating_to_cash(
    session,
    facts,
    covered,
    opening_facts,
    holds_no_cash=_is_consolidation_entity(session, entity_id),
  )

  if left_on is not None:
    facts = _leaving_member_facts(
      session,
      facts,
      opening_facts,
      periods,
      left_on,
      mapping_id=mapping_id,
      arc_type=arc_type,
      entity_id=entity_id,
      close_target_qname=close_target_qname,
    )

  # Calc-DAG subtotals as facts, so rules scoped to a subtotal can bind.
  # Runs after every leaf and derived fact is in place.
  _emit_subtotal_facts(session, facts, periods)

  # Warning-only: a mismatch flags incomplete investing/financing attribution.
  _check_cash_flow_tie_out(facts, covered, opening_facts)

  # Last, so no step above reads a balance dated outside every column.
  _emit_opening_equity_facts(
    session, facts, periods, mapping_id, arc_type, entity_id, close_target_qname
  )
  _emit_opening_cash_facts(
    session,
    facts,
    periods,
    mapping_id,
    arc_type,
    entity_id,
    acquired_on=acquired_on,
  )

  if start is not None:
    facts = [f for f in facts if _within_the_books(f, start)]

  unmapped_count = _count_unmapped(session, mapping_id, arc_type=arc_type)

  return ReportFacts(
    facts=facts,
    periods=periods,
    unmapped_count=unmapped_count,
    taxonomy_id=taxonomy_id,
    mapping_id=mapping_id,
    warnings=warnings,
  )


def _books_start(session: Session, entity_id: str | None) -> date | None:
  """The first day the entity's books cover, when they start from an opening
  balance."""
  return books_start(session, [entity_id]) if entity_id else None


def columns_before_the_books(
  session: Session,
  statement_type: str,
  periods: list[PeriodSpec],
  entity_ids: list[str],
) -> set[int]:
  """The columns that reach back before the books begin, by index: a flow
  column starting before the first day an opening balance covers, or a
  balance sheet dated before the opening itself. Such a column has no
  activity to show for the months before the books, only a partial figure."""
  return _columns_before(statement_type, periods, books_start(session, entity_ids))


def _columns_before(
  statement_type: str, periods: list[PeriodSpec], start: date | None
) -> set[int]:
  if start is None:
    return set()
  opening = start - timedelta(days=1)
  if statement_type == "balance_sheet":
    return {i for i, p in enumerate(periods) if p.end < opening}
  return {i for i, p in enumerate(periods) if p.start < start}


def group_columns_before_the_books(
  session: Session,
  statement_type: str,
  periods: list[PeriodSpec],
  members: list,
) -> set[int]:
  """``columns_before_the_books`` for a group read: a member's books hold
  back only the part of a column it is summed for, so one that joins later,
  from an opening balance, leaves the earlier comparatives standing and its
  own months stated. ``members`` are ``GroupMember``s."""
  omitted: set[int] = set()
  for member in members:
    start = books_start(session, [member.id])
    if start is None:
      continue
    for i, period in enumerate(periods):
      if statement_type == "balance_sheet":
        struck = membership_window(member, period.end.replace(day=1), period.end)
        window = (period.start, period.end) if struck else None
      else:
        window = membership_window(member, period.start, period.end)
      if window is not None and _columns_before(
        statement_type,
        [PeriodSpec(start=window[0], end=window[1], label=period.label)],
        start,
      ):
        omitted.add(i)
  return omitted


def _within_the_books(fact: ReportFact, start: date) -> bool:
  """A flow from the books' first day on, or a balance from the opening on:
  anything earlier is a partial figure the books cannot support."""
  if fact.period_type == "instant":
    return fact.period_end >= start - timedelta(days=1)
  return fact.period_start >= start


def _flows_without_a_row(
  session: Session,
  facts: list[ReportFact],
  flow_ids: set[str],
  entity_id: str | None,
) -> list[str]:
  """Name each emitted flow the entity's cash flow statement has no row for.

  Such a fact matches no row when rendered and the operating reconciling
  adjustment absorbs its cash, so the statement foots with the flow in the
  wrong section and nothing says so.
  """
  if not flow_ids:
    return []
  from robosystems.operations.roboledger.entity_scope import resolve_entity_id
  from robosystems.operations.roboledger.reports.network_picker import (
    load_entity_reporting_style,
    taggable_flow_qnames,
  )

  try:
    style = load_entity_reporting_style(
      session, entity_id or resolve_entity_id(session)
    )
  except LookupError:
    return []
  rows = taggable_flow_qnames(session, style)
  if not rows:
    return []
  # Signed, per column: an inflow and an outflow of one flow stay distinct,
  # and a comparative grid is not summed into one figure.
  missing: dict[str, dict[date, float]] = {}
  for f in facts:
    if f.element_id in flow_ids and f.element_qname not in rows:
      by_period = missing.setdefault(f.element_qname, {})
      by_period[f.period_end] = by_period.get(f.period_end, 0.0) + f.value
  return [
    f"{qname} ({amounts}) has no row on this company's cash flow statement, "
    "so its cash lands in the operating reconciling adjustment. Present the row "
    "in the reporting style, or tag the lines with a flow the statement shows."
    for qname, by_period in sorted(missing.items())
    for amounts in [
      ", ".join(
        f"{amount:,.2f} for the period ending {end.isoformat()}"
        for end, amount in sorted(by_period.items())
      )
    ]
  ]


def _period_balance_facts(
  session: Session,
  mapping_id: str,
  period_start: date,
  period_end: date,
  *,
  arc_type: str,
  close_target_qname: str,
  entity_id: str | None,
  line_slice: LineSlice | None = None,
) -> list[ReportFact]:
  """One column's mapped balances, with earnings closed to the Style's
  earnings home: instants through ``period_end``, durations over the period.
  A one-day column (``period_start == period_end``) is the balance sheet as of
  that day."""
  facts: list[ReportFact] = []
  balances = _read_mapped_balances(
    session,
    mapping_id,
    period_start,
    period_end,
    arc_type=arc_type,
    entity_id=entity_id,
    line_slice=line_slice,
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
        period_start=period_start,
        period_end=period_end,
        period_type=balance.period_type or _infer_period_type(balance.classification),
        close_value=(
          _natural_sign(balance.close_net_balance, balance.balance_type)
          if balance.close_net_balance is not None
          else None
        ),
        source_values={
          source_id: _natural_sign(net, balance.balance_type)
          for source_id, net in balance.by_source.items()
        },
        origin="pivot",
      )
    )

  # Seed zero facts for mapped equity targets with no postings yet, so
  # the close finds the RE-shaped target instead of falling back to APIC.
  _append_empty_equity_facts(
    session,
    mapping_id,
    facts,
    period_start,
    period_end,
    arc_type=arc_type,
    close_target_qname=close_target_qname,
  )

  # No-op where real closing entries already zeroed rev/exp.
  _close_to_retained_earnings(
    facts, period_start, period_end, close_target_qname=close_target_qname
  )

  # Runs from inception: real closing entries already zeroed what they
  # closed, so cumulative rev - exp is only the still-unclosed portion.
  _close_prior_periods_to_retained_earnings(
    session,
    mapping_id,
    facts,
    period_start,
    period_end,
    arc_type=arc_type,
    close_target_qname=close_target_qname,
    entity_id=entity_id,
  )
  return facts


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
  nearest in-structure ancestor. On a balance sheet or income statement a
  fact with none takes a row of its own under the nearest calculation
  ancestor the statement presents (``_place_strays_by_calculation``);
  elsewhere it is dropped from the view (still persisted for audit).
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

  if block_type == "equity_statement":
    # The parent's capital rolls forward first and the noncontrolling interest
    # after it, whatever the roots' qnames sort to.
    hierarchy = sorted(hierarchy, key=lambda root: root.qname == _NCI_ROOT)

  in_structure = _collect_hierarchy_element_ids(hierarchy)
  strays: list[ReportFact] = []
  rolled_up = _roll_up_facts_to_structure(session, facts, in_structure, dropped=strays)

  period_balances = [_facts_to_balance_dict(rolled_up, p.start, p.end) for p in periods]

  # ``arithmetic`` Disclosures compose calcs across taxonomies: every calc
  # whose subtotal is in the element set. Others use the structure's own.
  if concept_arrangement == "arithmetic":
    calculations = _load_calculations(session, element_ids=in_structure)
  else:
    calculations = _load_calculations(session, structure_id=structure_id)

  # Facts are already natural-signed.
  rows = _build_rows(
    hierarchy,
    period_balances,
    calculations,
    pre_signed=True,
    keep=_ROLLFORWARD_ROOTS_BY_BLOCK.get(block_type, frozenset()),
  )
  if strays and block_type in _PLACED_BY_CALCULATION:
    ancestry = _calculation_ancestry(
      session, {f.element_id for f in strays}, {row.element_id for row in rows}
    )
    rows = _place_strays_by_calculation(rows, strays, periods, ancestry)
  if block_type == "equity_statement":
    rows = _with_opening_balance(rows, facts, periods, hierarchy)
  if block_type == "cash_flow_statement":
    _show_cash_signed(rows, hierarchy, calculations)
    rows = _with_opening_balance(
      rows,
      facts,
      periods,
      hierarchy,
      roots=CASH_ROLLFORWARD_ROOTS,
      suffixes=(", beginning of period", ", end of period"),
    )

  return FactGrid(
    structure_id=structure_id,
    structure_name=structure_name,
    block_type=block_type,
    periods=periods,
    rows=rows,
  )


def _show_cash_signed(
  rows: list[FactRow],
  hierarchy: list[_HierarchyNode],
  calculations: dict[str, list[tuple[str, float]]],
) -> None:
  """Show a cash flow row its section subtracts (a gain, presented from its
  income statement fact) at its cash effect, as a negated label would, so
  every section reads as a plain sum. The fact keeps its own sign."""
  parent_of: dict[str, str] = {}

  def _walk(node: _HierarchyNode) -> None:
    for child in node.children:
      parent_of.setdefault(child.element_id, node.element_id)
      _walk(child)

  for root in hierarchy:
    _walk(root)
  subtracted = {
    child
    for parent, children in calculations.items()
    for child, weight in children
    if weight < 0 and parent_of.get(child) == parent
  }
  for row in rows:
    if row.element_id in subtracted:
      row.values = [None if v is None else 0.0 - v for v in row.values]


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


# The statements whose stray facts take a row by calculation. The cash flow
# statement keeps its own contract: a flow its network has no row for is named
# in ``ReportFacts.warnings`` rather than placed.
_PLACED_BY_CALCULATION: frozenset[str] = frozenset(
  {"balance_sheet", "income_statement"}
)


def _calculation_ancestry(
  session: Session, element_ids: set[str], presented: set[str], max_depth: int = 4
) -> dict[str, list[str]]:
  """``{summand element_id: [subtotal element_ids]}`` from every calculation
  arc that sums one of ``element_ids``, climbing breadth-first through
  unpresented subtotals until each path reaches a presented element or runs
  out. Library arcs live in their own structures (``rs-gaap-calculations``),
  not in the Style's presentation structure, so the walk is unscoped."""
  parents: dict[str, list[str]] = {}
  frontier = set(element_ids)
  seen = set(frontier)
  for _ in range(max_depth):
    if not frontier:
      break
    rows = session.execute(
      text(
        """
        SELECT DISTINCT from_element_id AS target, to_element_id AS source
        FROM associations
        WHERE association_type = 'calculation'
          AND to_element_id = ANY(:sources)
        """
      ),
      {"sources": sorted(frontier)},
    ).fetchall()
    next_frontier: set[str] = set()
    for row in rows:
      target = str(row.target)
      parents.setdefault(str(row.source), []).append(target)
      if target not in presented and target not in seen:
        next_frontier.add(target)
    seen |= next_frontier
    frontier = next_frontier
  return parents


def _place_strays_by_calculation(
  rows: list[FactRow],
  strays: list[ReportFact],
  periods: list[PeriodSpec],
  parents: dict[str, list[str]],
) -> list[FactRow]:
  """Give a fact the Style presents no row for a row of its own, as the last
  child of the nearest calculation ancestor the statement does present.

  The case is a concept of another entity form on this Style: an LLC's
  members' equity on the corporate balance sheet. Its subtotal already holds
  it (the calculation DAG summed it when the subtotal fact was emitted), so
  without a row the subtotal did not foot and the amount showed nowhere. A
  stray with no presented calculation ancestor belongs to another statement
  and stays off this one; a stray whose ancestor is itself a valued stray is
  covered by that ancestor's row. ``parents`` maps a summand to the subtotals
  that sum it (``_calculation_ancestry``); the arcs' weights play no part,
  because the footing check weights a child by its balance type against its
  parent's, the same convention every presented row follows.
  """
  by_element: dict[str, list[ReportFact]] = {}
  for fact in strays:
    by_element.setdefault(fact.element_id, []).append(fact)
  valued: dict[str, tuple[ReportFact, list[float | None]]] = {}
  for element_id, facts in by_element.items():
    values = [
      _unsigned_zero(
        sum(
          f.value for f in facts if (f.period_start, f.period_end) == (p.start, p.end)
        )
      )
      for p in periods
    ]
    if any(v not in (None, 0, 0.0) for v in values):
      valued[element_id] = (facts[0], values)
  if not valued:
    return rows

  # The deepest presented ancestor is the most specific subtotal; among the
  # rows for one element (rare), the first in presentation order.
  presented: dict[str, tuple[int, int]] = {}
  for index, row in enumerate(rows):
    presented.setdefault(row.element_id, (row.depth, index))
  summed = {target for targets in parents.values() for target in targets}

  def _anchor(element_id: str) -> str | None:
    frontier = [element_id]
    visited = {element_id}
    while frontier:
      found: list[str] = []
      next_frontier: list[str] = []
      for current in frontier:
        for parent in parents.get(current, []):
          if parent in visited:
            continue
          visited.add(parent)
          if parent in presented:
            found.append(parent)
          elif parent in valued:
            return None
          else:
            next_frontier.append(parent)
      if found:
        return max(found, key=lambda eid: (presented[eid][0], -presented[eid][1]))
      frontier = next_frontier
    return None

  placed: dict[str, list[FactRow]] = {}
  for element_id, (first, values) in valued.items():
    anchor = _anchor(element_id)
    if anchor is None:
      continue
    placed.setdefault(anchor, []).append(
      FactRow(
        element_id=element_id,
        element_qname=first.element_qname,
        element_name=first.element_name,
        classification=first.classification,
        balance_type=first.balance_type,
        values=values,
        is_subtotal=element_id in summed,
      )
    )
  if not placed:
    return rows

  result: list[FactRow] = []
  for row in rows:
    for stray in placed.pop(row.element_id, ()):
      stray.depth = row.depth + 1
      # Just before its ancestor in the presentation order, where a merge
      # with another entity's grid keeps it.
      stray.position = row.position - 0.5
      result.append(stray)
    result.append(row)
  return result


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
  *,
  dropped: list[ReportFact] | None = None,
) -> list[ReportFact]:
  """Repoint out-of-structure facts at their nearest in-structure ancestor.

  Facts with no ancestor are dropped, and collected into ``dropped`` when
  given, for the caller to present another way. No aggregation here —
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
      if dropped is not None:
        dropped.append(fact)
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
        source_values=fact.source_values,
        source_shares=fact.source_shares,
        origin=fact.origin,
        derivation=fact.derivation,
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
  # The reporting element's XBRL periodType; None falls back to inference.
  period_type: str | None = None
  # debit - credit per source account.
  by_source: dict[str, float] = field(default_factory=dict)


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
  entity_id: str | None = None,
  line_slice: LineSlice | None = None,
) -> dict[str, _Balance]:
  """Mapped trial balance per reporting target (same join as /trial-balance/mapped),
  for one entity's entries when ``entity_id`` is given, and only one
  management dimension member's lines when ``line_slice`` is.

  Windowing keys off the target's ``period_type``, not its SFAC 6 trait
  (which can be NULL or contra-*): instant concepts load cumulatively
  through ``period_end``; duration concepts load only the period's activity.

  A source mapped to several targets contributes its full postings to each
  target's ``net_balance``, but its close contribution
  (``close_net_balance``) to exactly one primary target — trait-classified
  over inferred, qname tiebreak — so the RE/NI close counts each posting once.
  """
  result = session.execute(
    text(
      """
      SELECT
        source_elem.id AS source_id,
        target.id AS reporting_element_id,
        target.qname,
        target.name AS reporting_name,
        tcls.identifier AS classification,
        target.balance_type,
        target.period_type,
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
        AND (:entity_id IS NULL OR e.entity_id = :entity_id)
        AND target.element_type = 'concept'
        AND target.is_abstract = false
        AND (e.posting_date <= :end_date OR :end_date IS NULL)
        AND (
          target.period_type = 'instant'
          OR e.posting_date >= :start_date
          OR :start_date IS NULL
        )
        AND """
      + LINE_SLICE_SQL
      + """
      GROUP BY source_elem.id, target.id, target.qname, target.name,
               tcls.identifier, target.balance_type, target.period_type
      ORDER BY target.qname
    """
    ).bindparams(landed_entry_bindparam()),
    {
      "mapping_id": mapping_id,
      "arc_type": arc_type,
      "start_date": period_start,
      "end_date": period_end,
      "entity_id": entity_id,
      **slice_params(line_slice),
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
        period_type=row.period_type,
        total_debits=0.0,
        total_credits=0.0,
        net_balance=0.0,
        close_net_balance=0.0,
      )
      balances[row.reporting_element_id] = balance
    balance.total_debits += debits
    balance.total_credits += credits
    balance.net_balance += debits - credits
    balance.by_source[row.source_id] = (
      balance.by_source.get(row.source_id, 0.0) + debits - credits
    )
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
        source_values={},
        origin="pivot",
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
          element_name=re_row.name or _close_target_label(close_target_qname),
          classification="equity",
          balance_type=re_row.balance_type or "credit",
          value=0.0,
          period_start=period_start,
          period_end=period_end,
          period_type="instant",
          source_values={},
          origin="pivot",
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
    net_income = 0.0
    children: dict[str, float] = {}
    for f in facts:
      if f.period_start != period.start or f.period_end != period.end:
        continue
      sign = _INCOME_SIGN.get(f.classification or "")
      if sign is not None:
        net_income += sign * _close_value(f)
        children[f.element_qname] = float(sign)
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
        origin="rollup",
        derivation={
          "rule": "net_income",
          "children": [{"qname": q, "weight": w} for q, w in children.items()],
        },
      )
    )


def _emit_subtotal_facts(
  session: Session,
  facts: list[ReportFact],
  periods: list[PeriodSpec],
) -> None:
  """Append one fact per rs-gaap calculation subtotal with any fact beneath
  it, per period — zero included, so a total whose children net to zero
  still states 0 beside them.

  Persisted so verification rules scoped to a subtotal can bind. Uses the
  same resolution as ``_build_rows`` (direct fact wins, else Σ child·weight),
  so the rendered number is identical. Must run after every leaf and
  derived fact is in place.
  """
  calculations = load_rs_gaap_calculations(session)
  if not calculations:
    return

  target_ids = list(calculations.keys())
  # The children too, so each subtotal can name the concepts it summed.
  child_ids = sorted({src for arcs in calculations.values() for src, _ in arcs})
  meta = {
    m.id: m
    for m in session.execute(
      text(
        "SELECT id, qname, name, balance_type, period_type "
        "FROM elements WHERE id = ANY(:ids)"
      ),
      {"ids": sorted({*target_ids, *child_ids})},
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
    # ``order`` lists children before parents, so one pass propagates presence.
    reported = set(present)
    for elem_id in order:
      if any(src in reported for src, _ in calculations.get(elem_id, ())):
        reported.add(elem_id)

    for elem_id in target_ids:
      if elem_id in present or elem_id not in reported:
        continue
      value = computed.get(elem_id, 0.0)
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
          origin="rollup",
          derivation={
            "rule": "calculation",
            "children": [
              {"qname": meta[src].qname, "weight": float(weight)}
              for src, weight in calculations.get(elem_id, ())
              if src in reported and src in meta
            ],
          },
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


def _ppe_details_outside_net(
  facts: list[ReportFact],
  period: PeriodSpec,
  net_fact: ReportFact,
  gross_id: str | None,
  ad_id: str | None,
) -> float:
  """Gross - AD from accounts not already counted in the direct Net fact.

  AD with no Gross beside it still nets: a chart mapping assets at cost to
  Net and their depreciation to AD reports cost - AD, not cost alone.
  """
  net_sources = set(net_fact.source_values or {})
  total = 0.0
  for f in facts:
    if f.period_start != period.start or f.period_end != period.end:
      continue
    if f.element_id not in (gross_id, ad_id) or f.source_values is None:
      continue
    outside = sum(v for s, v in f.source_values.items() if s not in net_sources)
    total += outside if f.element_id == gross_id else -outside
  return total


def _synthesize_ppe_net_facts(
  session: Session,
  facts: list[ReportFact],
  periods: list[PeriodSpec],
) -> None:
  """Append PPE Net = Gross - Accumulated Depreciation per period.

  For gross + contra mappings, which let CF Investing read ΔGross as
  purchases. Where a direct PPE Net fact exists, Gross - AD from accounts
  not also mapped to Net is added to it, so a chart mixing the two keeps
  every account on the balance sheet exactly once.
  """
  row = session.execute(
    text("SELECT id, balance_type FROM elements WHERE qname = :qname"),
    {"qname": PPE_NET_QNAME},
  ).fetchone()
  if row is None:
    return
  net_id, net_balance_type = row[0], row[1] or "debit"

  src_rows = session.execute(
    text("SELECT qname, id FROM elements WHERE qname = ANY(:qnames)"),
    {"qnames": [PPE_GROSS_QNAME, PPE_ACCUMULATED_DEPRECIATION_QNAME]},
  ).fetchall()
  src_ids = dict(src_rows)
  gross_id = src_ids.get(PPE_GROSS_QNAME)
  ad_id = src_ids.get(PPE_ACCUMULATED_DEPRECIATION_QNAME)
  if gross_id is None and ad_id is None:
    return  # Neither source mapped — nothing to synthesize.

  for period in periods:
    net_fact = next(
      (
        f
        for f in facts
        if f.element_id == net_id
        and f.period_start == period.start
        and f.period_end == period.end
      ),
      None,
    )
    if net_fact is not None:
      net_fact.value += _ppe_details_outside_net(
        facts, period, net_fact, gross_id, ad_id
      )
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
        element_qname=PPE_NET_QNAME,
        element_name="Property, Plant and Equipment, Net",
        classification="asset",
        balance_type=net_balance_type,
        value=gross_value - ad_value,
        period_start=period.start,
        period_end=period.end,
        period_type="instant",
        origin="derived",
        derivation={
          "computations": ["ppe_net"],
          "formula": "PropertyPlantAndEquipmentGross - AccumulatedDepreciation",
          "source_concepts": [
            "rs-gaap:PropertyPlantAndEquipmentGross",
            "rs-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment",
          ],
        },
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
    "rs-gaap:Cash",
    "rs-gaap:CashAndCashEquivalents",
    # Part of the cash total the statement reconciles to (ASU 2016-18), not a
    # flow: its movement is in ΔCash, never in a section.
    "rs-gaap:RestrictedCashAndInvestmentsCurrent",
  }
)

# The total the statement opens and closes on (ASU 2016-18): the root of each
# cash flow network, summed by calc arc from the balance sheet's cash rows and
# never mapped to. Its column-end facts come from ``_emit_subtotal_facts``; the
# opening ones from ``_emit_opening_cash_facts``. Not an anchor: a line can't be
# on it, and ``_cash_by_date`` would count it beside the rows it sums.
_CASH_TOTAL_QNAME = (
  "rs-gaap:CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"
)
CASH_ROLLFORWARD_ROOTS: frozenset[str] = frozenset({_CASH_TOTAL_QNAME})

# The reconciling leaf is in both the CF calc DAG and the Indirect
# presentation, so the adjustment foots the subtotal and renders as a line.
_CF_NET_CHANGE_QNAME = "rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease"
_CF_RECONCILING_LEAF_QNAME = "rs-gaap:IncreaseDecreaseInOtherOperatingCapitalNet"
_CF_OPERATING_SUBTOTAL_QNAME = "rs-gaap:NetCashProvidedByUsedInOperatingActivities"

# A reconciling plug above this fraction of operating cash signals an
# un-itemized non-cash item or a misclassified flow. Shared with guard_rails.
_CF_PLUG_WARN_RATIO = 0.25


def _primary_mapping(line: str) -> str:
  """A subquery giving the one target a line's account counts toward, for
  ``LEFT JOIN LATERAL (...) amap ON TRUE``. An account mapped to several
  targets (a tenant's disclosure member beside its statement concept) is
  read through the library concept the statements are built from, then a
  trait-classified one, qname last, so a per-line pass never counts a line
  twice or reads it as the member."""
  return f"""
    SELECT m.to_element_id
    FROM associations m
    JOIN elements mt ON mt.id = m.to_element_id
    LEFT JOIN taxonomies mtx ON mtx.id = mt.taxonomy_id
    LEFT JOIN element_traits pet
      ON pet.element_id = m.to_element_id AND pet.is_primary = TRUE
    LEFT JOIN traits pt
      ON pt.id = pet.trait_id AND pt.category = 'elementsOfFinancialStatements'
    WHERE m.from_element_id = {line}.element_id
      AND m.association_type = :arc_type
      AND m.structure_id = :mapping_id
    ORDER BY (mtx.taxonomy_type IS DISTINCT FROM 'reporting_standard'),
      (pt.id IS NULL), mt.qname
    LIMIT 1
  """


def _is_consolidation_entity(session: Session, entity_id: str | None) -> bool:
  """Whether the entity is the group's consolidation entity, whose entries
  only the consolidated statements read; a whole-graph pivot is not."""
  if entity_id is None:
    return False
  role = session.execute(
    text("SELECT entity_role FROM entities WHERE id = :id"), {"id": entity_id}
  ).scalar()
  return role == "consolidation"


def _emit_flow_facts(
  session: Session,
  facts: list[ReportFact],
  periods: list[PeriodSpec],
  mapping_id: str,
  arc_type: str,
  entity_id: str | None = None,
) -> set[str]:
  """Emit investing/financing CF facts from per-line flows, per period.

  Pass 1 (explicit): the cash line carries ``flow_element_id``; its
  ``debit - credit`` is the flow. Pass 2 (fallback, the path for QuickBooks
  data): each untagged non-cash line of a cash-moving entry whose cash line
  is untagged routes to its element's default flow via a ``derivation`` arc
  whose weight sign is the cash direction. The passes agree in sign because
  a balanced entry's cash side negates its non-cash side. Operating flows
  are excluded — indirect operating is NI + addbacks + ΔWC. Replaces any
  existing fact for the leaf. Returns the ids of the flows emitted.

  The group's consolidation entity moves no cash: its eliminations carry,
  on the lines themselves, the flows the members' lines produced, so there
  every tagged line counts on its own, cash-effect signed (a credit is an
  inflow) — the debit that reverses a member's note nets the proceeds the
  member booked.
  """
  cash_qnames = list(_CASH_ANCHOR_QNAMES)

  explicit_sql = text(f"""
        SELECT
          COALESCE(fmap.to_element_id, li.flow_element_id) AS flow_id,
          rf.qname AS flow_qname,
          rf.name AS flow_name,
          rf.balance_type AS flow_balance_type,
          COALESCE(SUM(
            CASE WHEN COALESCE(racct.qname, acct.qname) = ANY(:cash_qnames)
              THEN li.debit_amount - li.credit_amount
              ELSE li.credit_amount - li.debit_amount
            END
          ), 0) AS value_cents
        FROM line_items li
        JOIN entries en ON en.id = li.entry_id
        JOIN elements acct ON acct.id = li.element_id
        LEFT JOIN LATERAL ({_primary_mapping("li")}) amap ON TRUE
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
          AND en.type <> 'opening'
          AND (:entity_id IS NULL OR en.entity_id = :entity_id)
          AND en.posting_date BETWEEN :start AND :end
          AND (
            COALESCE(racct.qname, acct.qname) = ANY(:cash_qnames)
            -- the consolidation entity: every tagged line, cash or not
            OR :tags_count_without_cash = TRUE
          )
        GROUP BY flow_id, rf.qname, rf.name, rf.balance_type
      """).bindparams(landed_entry_bindparam())

  # Gated on the entry's untagged lines forming a balanced sub-entry that
  # moves cash: accruals don't leak in, a cash line Pass 1 owns is never
  # attributed twice, and an account line shared between a tagged and an
  # untagged cash line (nothing says how much of it the tag covers) stays
  # with the operating plug. A split's tagged parts post with their own cash
  # and account lines, so its untagged parts balance on their own and keep
  # their defaults.
  fallback_sql = text(f"""
        SELECT
          rf.id AS flow_id,
          rf.qname AS flow_qname,
          rf.name AS flow_name,
          rf.balance_type AS flow_balance_type,
          COALESCE(SUM(li.credit_amount - li.debit_amount), 0) AS value_cents
        FROM line_items li
        JOIN entries en ON en.id = li.entry_id
        JOIN elements acct ON acct.id = li.element_id
        LEFT JOIN LATERAL ({_primary_mapping("li")}) amap ON TRUE
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
          AND en.type <> 'opening'
          AND (:entity_id IS NULL OR en.entity_id = :entity_id)
          AND en.posting_date BETWEEN :start AND :end
          AND COALESCE(racct.qname, acct.qname) <> ALL(:cash_qnames)
          AND EXISTS (
            SELECT 1 FROM line_items cl
            JOIN elements ca ON ca.id = cl.element_id
            LEFT JOIN LATERAL ({_primary_mapping("cl")}) cmap ON TRUE
            LEFT JOIN elements cra ON cra.id = cmap.to_element_id
            WHERE cl.entry_id = en.id
              AND cl.flow_element_id IS NULL
              AND COALESCE(cra.qname, ca.qname) = ANY(:cash_qnames)
          )
          AND (
            SELECT COALESCE(SUM(ul.debit_amount - ul.credit_amount), 0)
            FROM line_items ul
            WHERE ul.entry_id = en.id AND ul.flow_element_id IS NULL
          ) = 0
        GROUP BY rf.id, rf.qname, rf.name, rf.balance_type
      """).bindparams(landed_entry_bindparam())

  emitted: set[str] = set()
  tags_count_without_cash = _is_consolidation_entity(session, entity_id)
  for period in periods:
    params = {
      "arc_type": arc_type,
      "mapping_id": mapping_id,
      "start": period.start,
      "end": period.end,
      "cash_qnames": cash_qnames,
      "entity_id": entity_id,
      "tags_count_without_cash": tags_count_without_cash,
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
      emitted.add(flow_id)
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
          origin="pivot",
          derivation={"posting_filter": {"flow_element": qname}},
        )
      )
  return emitted


def _opening_date(period: PeriodSpec) -> date:
  """The balance-sheet date a period's change is measured from."""
  return period.start - timedelta(days=1)


def _one_column_per_date(facts: list[ReportFact]) -> list[ReportFact]:
  """Keep one column's facts for each element and end date.

  Two columns can end on the same day (a 10-Q's three- and nine-month
  columns) and each carries the same balance-sheet facts; reading balances
  "as of a date" across both counts them twice.
  """
  start_by_key: dict[tuple[str, date], date | None] = {}
  for f in facts:
    key = (f.element_id, f.period_end)
    if key not in start_by_key:
      start_by_key[key] = f.period_start
      continue
    kept = start_by_key[key]
    # A fact with no start (a bare instant) sorts first.
    if kept is not None and (f.period_start is None or f.period_start < kept):
      start_by_key[key] = f.period_start
  return [
    f for f in facts if f.period_start == start_by_key[(f.element_id, f.period_end)]
  ]


def _load_opening_facts(
  session: Session,
  mapping_id: str,
  periods: list[PeriodSpec],
  arc_type: str,
  entity_id: str | None = None,
) -> list[ReportFact]:
  """Instant balances at each period's opening, where that isn't already
  another column's end. The earliest column needs one too, or its cash flow
  has nothing to move from."""
  ordered = sorted(periods, key=lambda p: p.end)
  loaded = {p.end for p in ordered}
  opening_facts: list[ReportFact] = []
  for current in ordered:
    opening = _opening_date(current)
    if opening in loaded:
      continue
    loaded.add(opening)
    balances = _read_mapped_balances(
      session, mapping_id, opening, opening, arc_type=arc_type, entity_id=entity_id
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
          period_type=balance.period_type or _infer_period_type(balance.classification),
          source_values={
            source_id: _natural_sign(net, balance.balance_type)
            for source_id, net in balance.by_source.items()
          },
          origin="pivot",
        )
      )
  return opening_facts


def _derive_cash_flow_facts(
  session: Session,
  facts: list[ReportFact],
  periods: list[PeriodSpec],
  opening_facts: list[ReportFact] | None = None,
) -> set[tuple[str, date, date]]:
  """Append operating CF facts from BS deltas (indirect method).

  Each ``derivation`` arc says "this CF leaf is the change in this BS
  element" with a signed weight (asset up = -1, liability up = +1). For each
  period, ``Σ weight * (BS_end - BS_opening)``, the opening being the day
  before the period starts. Zero values are skipped. Returns the
  ``(leaf, start, end)`` keys derived from deltas, zero or not; a leaf a
  direct fact owns is not among them.
  """

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
  derived: set[tuple[str, date, date]] = set()
  if not rows:
    return derived

  derivations: dict[str, list[tuple[str, float]]] = {}
  for cf_id, source_id, weight in rows:
    derivations.setdefault(cf_id, []).append((source_id, float(weight or 1.0)))

  cf_leaf_ids = list(derivations.keys())
  if not cf_leaf_ids:
    return derived
  meta_rows = session.execute(
    text("""
      SELECT id, qname, name, balance_type
      FROM elements
      WHERE id = ANY(:ids)
    """),
    {
      "ids": sorted(
        {*cf_leaf_ids, *(s for arcs in derivations.values() for s, _ in arcs)}
      )
    },
  ).fetchall()
  cf_meta: dict[str, tuple[str, str, str]] = {
    row[0]: (row[1], row[2], row[3] or "debit") for row in meta_rows
  }

  # Balances as of a date, from one column per date; sum on collision within
  # it, as _facts_to_balance_dict does.
  fact_index: dict[tuple[str, date], float] = {}
  for f in _one_column_per_date(facts):
    key = (f.element_id, f.period_end)
    fact_index[key] = fact_index.get(key, 0.0) + f.value
  opening_index: dict[tuple[str, date], float] = dict(fact_index)
  for f in opening_facts or ():
    key = (f.element_id, f.period_end)
    opening_index[key] = opening_index.get(key, 0.0) + f.value
  direct = {(f.element_id, f.period_start, f.period_end) for f in facts}

  for current in ordered:
    opening = _opening_date(current)
    for cf_leaf_id, sources in derivations.items():
      # A direct fact wins (e.g. DDA mapped from Depreciation Expense).
      if (cf_leaf_id, current.start, current.end) in direct:
        continue
      derived.add((cf_leaf_id, current.start, current.end))
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
          origin="derived",
          derivation={
            "computations": ["indirect_cash_flow"],
            "formula": "Σ weight × (balance at period end − balance at opening)",
            "source_concepts": sorted(
              cf_meta[s][0] for s, _ in sources if s in cf_meta
            ),
          },
        )
      )
  return derived


# Income-statement charges the cash flow presents from their own fact as an
# add-back. One booked on a disposal entry (the month's last depreciation,
# beside the write-off) is the period's charge, not part of the disposal.
_CF_PERIOD_CHARGE_QNAMES = frozenset(
  {
    "rs-gaap:DepreciationDepletionAndAmortization",
    "rs-gaap:DepreciationAndAmortization",
    "rs-gaap:Depreciation",
    "rs-gaap:DepreciationNonproduction",
    "rs-gaap:CostOfGoodsAndServicesSoldDepreciationAndAmortization",
    "rs-gaap:OtherDepreciationAndAmortization",
    "rs-gaap:AmortizationOfIntangibleAssets",
  }
)
_CF_OTHER_NONCASH_QNAME = "rs-gaap:OtherNoncashIncomeExpense"
_CF_CAPEX_UNPAID_QNAME = "rs-gaap:CapitalExpendituresIncurredButNotYetPaid"
_CF_NONCASH_ACQUISITION_QNAME = (
  "rs-gaap:NoncashOrPartNoncashAcquisitionValueOfAssetsAcquired1"
)
_CF_PAYABLES_LEAF_QNAME = (
  "rs-gaap:IncreaseDecreaseInAccountsPayableAndAccruedLiabilities"
)
_CF_RECEIVABLES_LEAF_QNAME = "rs-gaap:IncreaseDecreaseInAccountsReceivable"
_CF_INTEREST_PAID_QNAME = "rs-gaap:InterestPaidNet"
_CF_TAXES_PAID_QNAME = "rs-gaap:IncomeTaxesPaidNet"
# The expense concepts whose cash settlement is a supplemental disclosure.
# Taxes payable is curated out of tenants, so taxes paid reads the expense
# lines that an entry with cash, or a settled bill, carries.
_CF_PAID_ROWS: dict[str, str] = {
  "rs-gaap:InterestExpense": _CF_INTEREST_PAID_QNAME,
  "rs-gaap:InterestExpenseDebt": _CF_INTEREST_PAID_QNAME,
  "rs-gaap:IncomeTaxExpenseBenefit": _CF_TAXES_PAID_QNAME,
  "rs-gaap:CurrentIncomeTaxExpenseBenefit": _CF_TAXES_PAID_QNAME,
}


@dataclass
class _CfLine:
  kind: str  # cash | working | pnl | nonoperating
  concept_id: str
  debit: int
  credit: int
  tagged: bool
  qname: str | None = None

  @property
  def net(self) -> int:
    return self.debit - self.credit


@dataclass
class _Bill:
  """What a payable settles for one event, from its entries with no cash line
  that carry investing or financing lines. ``columns`` are the report's
  columns it was posted in; one posted before them has none."""

  posted: date
  payable: int
  capex: int
  columns: list[PeriodSpec]


def _cash_share(lines: list[_CfLine]) -> float:
  """The share of an entry's other side its cash settles: a payment of 100
  against 150 of debits (the rest added to a debt) settles two thirds."""
  cash = sum(ln.net for ln in lines if ln.kind == "cash")
  if cash < 0:
    other = sum(ln.debit for ln in lines if ln.kind != "cash")
  elif cash > 0:
    other = sum(ln.credit for ln in lines if ln.kind != "cash")
  else:
    return 0.0
  return min(1.0, abs(cash) / other) if other else 0.0


def _sign(value: float) -> int:
  return (value > 0) - (value < 0)


def _matched(amount: float, against: float) -> float:
  """The part of ``amount`` that ``against`` balances: opposite signs, the
  smaller magnitude."""
  if not amount or not against or _sign(amount) == _sign(against):
    return 0.0
  return _sign(amount) * min(abs(amount), abs(against))


def _cash_flow_reused_concepts(session: Session, entity_id: str | None) -> set[str]:
  """Income-statement concepts the entity's cash flow presents from their own
  fact (depreciation, a gain on disposal): their full amount is already out
  of operating."""
  from robosystems.operations.roboledger.entity_scope import resolve_entity_id
  from robosystems.operations.roboledger.reports.network_picker import (
    NoNetworkForStatementTypeError,
    get_render_network,
    load_entity_reporting_style,
    network_structure_ids,
  )

  try:
    style = load_entity_reporting_style(
      session, entity_id or resolve_entity_id(session)
    )
    network = get_render_network(session, style, "cash_flow_statement")
  except LookupError, NoNetworkForStatementTypeError:
    return set()
  rows = session.execute(
    text("""
      SELECT DISTINCT e.id
      FROM associations a
      JOIN elements e ON e.id = a.to_element_id
      WHERE a.structure_id = ANY(:sids)
        AND a.association_type = 'presentation'
        AND e.period_type = 'duration'
    """),
    {"sids": network_structure_ids(session, network.structure_id)},
  ).scalars()
  return {str(r) for r in rows}


def _emit_noncash_and_settlement_facts(
  session: Session,
  facts: list[ReportFact],
  periods: list[PeriodSpec],
  mapping_id: str,
  arc_type: str,
  entity_id: str | None,
  derived: set[tuple[str, date, date]],
) -> tuple[set[str], list[str]]:
  """The cash flow's non-cash and settlement corrections, per period.

  An entry with no cash line moves no cash, yet its income-statement lines
  reach net income and its working-capital lines reach their deltas. Where
  such an entry's other side is a non-operating account (an investment, a
  debt, equity, fixed assets):

  - the income-statement part is backed out on ``OtherNoncashIncomeExpense``,
    unless the concept is one the cash flow presents from its own fact;
  - the working-capital part is backed out of that account's delta;
  - an investing asset it acquires is disclosed: through payables, as capital
    expenditures incurred but not yet paid at the period's end; otherwise (on
    the card, in exchange for another asset) as a non-cash acquisition.

  A payment whose event discharges a bill (or a receipt an invoice) is
  looked through to that bill's entry: the share of the payment that settles
  investing or financing lines takes those lines' default flows, and comes
  out of the payable's delta. In a cash sale, a gain on a concept the cash
  flow presents joins the proceeds. A tagged entry's flow is the explicit
  pass's; its tagged working-capital lines come out of their deltas here.

  Cash paid for interest and income taxes is the expense lines an entry
  with a cash line carries, at the share its cash settles, and a settled
  bill's share of them; interest added to a debt with no cash moving is not
  paid. An expense accrued by journal entry and paid by a cash entry that
  links to nothing is not traced to the payment, so it is not disclosed.

  Returns the ids of the flows emitted, and a warning per period in which
  payments to payables carry no link while a bill with investing or
  financing lines is open: what those payments settled stays in operating.
  """
  if not periods:
    return set(), []

  derivation_rows = session.execute(
    text("""
      SELECT a.from_element_id, a.to_element_id, a.weight, fe.qname AS from_qname,
        (
          SELECT t.identifier FROM element_traits et
          JOIN traits t ON t.id = et.trait_id
          WHERE et.element_id = a.from_element_id
            AND t.category = 'activityType'
            AND t.identifier IN ('investingActivity', 'financingActivity')
          LIMIT 1
        ) AS activity
      FROM associations a
      JOIN elements fe ON fe.id = a.from_element_id
      WHERE a.association_type = 'derivation'
    """)
  ).fetchall()
  working_leaf: dict[str, str] = {}
  payable_concepts: set[str] = set()
  receivable_concepts: set[str] = set()
  default_flows: dict[str, list[tuple[str, float, str]]] = {}
  for row in derivation_rows:
    concept = str(row.to_element_id)
    if row.activity:
      default_flows.setdefault(concept, []).append(
        (str(row.from_element_id), float(row.weight or 1.0), row.activity)
      )
    else:
      working_leaf.setdefault(concept, str(row.from_element_id))
      if row.from_qname == _CF_PAYABLES_LEAF_QNAME:
        payable_concepts.add(concept)
      elif row.from_qname == _CF_RECEIVABLES_LEAF_QNAME:
        receivable_concepts.add(concept)

  reused = _cash_flow_reused_concepts(session, entity_id)
  cash_qnames = _CASH_ANCHOR_QNAMES
  holds_no_cash = _is_consolidation_entity(session, entity_id)

  line_sql = text(f"""
    SELECT en.id AS entry_id, en.posting_date, en.triggered_by_event_id,
      ev.discharges_event_id,
      li.debit_amount, li.credit_amount, li.flow_element_id,
      COALESCE(amap.to_element_id, li.element_id) AS concept_id,
      c.qname AS concept_qname, c.period_type AS concept_period_type
    FROM line_items li
    JOIN entries en ON en.id = li.entry_id
    LEFT JOIN LATERAL ({_primary_mapping("li")}) amap ON TRUE
    LEFT JOIN elements c ON c.id = COALESCE(amap.to_element_id, li.element_id)
    LEFT JOIN events ev ON ev.id = en.triggered_by_event_id
    WHERE en.status IN :landed_entry_statuses
      AND en.type <> 'opening'
      AND (:entity_id IS NULL OR en.entity_id = :entity_id)
      AND {{scope}}
  """)
  base = {"arc_type": arc_type, "mapping_id": mapping_id, "entity_id": entity_id}

  def _lines(scope: str, **params) -> list:
    return session.execute(
      text(line_sql.text.format(scope=scope)).bindparams(landed_entry_bindparam()),
      {**base, **params},
    ).fetchall()

  def _classify(row) -> _CfLine | None:
    concept_id = str(row.concept_id)
    if row.concept_qname in cash_qnames:
      kind = "cash"
    elif concept_id in working_leaf:
      kind = "working"
    elif row.concept_period_type == "duration":
      kind = "pnl"
    elif row.concept_period_type == "instant":
      kind = "nonoperating"
    else:
      return None
    return _CfLine(
      kind,
      concept_id,
      int(row.debit_amount or 0),
      int(row.credit_amount or 0),
      row.flow_element_id is not None,
      row.concept_qname,
    )

  def _entries(rows) -> dict[str, list[_CfLine] | None]:
    """Lines per entry; None for an entry with a line nothing classifies."""
    out: dict[str, list[_CfLine] | None] = {}
    for row in rows:
      key = str(row.entry_id)
      line = _classify(row)
      if line is None:
        out[key] = None
      elif out.get(key, []) is not None:
        out.setdefault(key, []).append(line)  # type: ignore[union-attr]
    return out

  window_start = min(p.start for p in periods)
  rows = _lines(
    "en.posting_date BETWEEN :start AND :end",
    start=window_start,
    end=max(p.end for p in periods),
  )
  dated: dict[str, date] = {}
  event_of: dict[str, str] = {}
  discharges: dict[str, str] = {}
  for row in rows:
    key = str(row.entry_id)
    dated[key] = row.posting_date
    if row.triggered_by_event_id:
      event_of[key] = str(row.triggered_by_event_id)
    if row.discharges_event_id:
      discharges[key] = str(row.discharges_event_id)
  entries = _entries(rows)

  settled: dict[str, list[_CfLine]] = {}
  if discharges:
    bill_rows = _lines(
      "en.triggered_by_event_id = ANY(:events)",
      events=sorted(set(discharges.values())),
    )
    bill_event = {str(r.entry_id): str(r.triggered_by_event_id) for r in bill_rows}
    for entry_key, lines in _entries(bill_rows).items():
      if lines is not None:
        settled.setdefault(bill_event[entry_key], []).extend(lines)

  def _default_flow(
    concept_id: str, cash_effect: float, activity: str | None = None
  ) -> str | None:
    for flow_id, weight, flow_activity in default_flows.get(concept_id, ()):
      if _sign(weight) == _sign(cash_effect) and activity in (None, flow_activity):
        return flow_id
    return None

  element_ids = dict(
    session.execute(
      text("SELECT qname, id FROM elements WHERE qname = ANY(:q)"),
      {
        "q": [
          _CF_OTHER_NONCASH_QNAME,
          _CF_CAPEX_UNPAID_QNAME,
          _CF_NONCASH_ACQUISITION_QNAME,
          _CF_INTEREST_PAID_QNAME,
          _CF_TAXES_PAID_QNAME,
        ]
      },
    ).fetchall()
  )
  other_noncash_id = element_ids.get(_CF_OTHER_NONCASH_QNAME)
  acquisition_id = element_ids.get(_CF_NONCASH_ACQUISITION_QNAME)

  emitted_flows: set[str] = set()
  adjustments: dict[tuple[str, date, date], float] = {}
  bills: dict[str, _Bill] = {}
  unlinked: dict[tuple[date, date], list[int]] = {}

  def _disposes(line: _CfLine) -> bool:
    """A gain or loss the cash flow presents on its own row, or the
    depreciation a sale writes off. The period's charge booked on the same
    entry is neither: its expense is the add-back, and its credit to
    accumulated depreciation is no write-off."""
    return (
      line.kind == "pnl"
      and line.concept_id in reused
      and line.qname not in _CF_PERIOD_CHARGE_QNAMES
    ) or (
      line.kind == "working"
      and line.qname == PPE_ACCUMULATED_DEPRECIATION_QNAME
      and line.net > 0
    )

  def _on_account(lines: list[_CfLine]) -> tuple[float, list[_CfLine], float]:
    """The part of an investing or financing price an entry with cash left
    on a receivable or payable: ``(owed, those lines, their sum)``. Only an
    account on the cash's own side counts, so a payment that also clears an
    accrual or an old bill owes nothing."""
    cash = sum(ln.net for ln in lines if ln.kind == "cash")
    if not cash:
      return 0.0, [], 0.0
    disposal = [ln for ln in lines if _disposes(ln)]
    side = sum(ln.net for ln in lines if ln.kind in ("nonoperating", "cash"))
    side += sum(ln.net for ln in disposal)
    owing = [
      ln
      for ln in lines
      if ln.kind == "working"
      and ln not in disposal
      and ln.concept_id in payable_concepts | receivable_concepts
      and _sign(ln.net) == _sign(cash)
    ]
    working = sum(ln.net for ln in owing)
    return _matched(working, side), owing, working

  def _add(element_id: str, period: PeriodSpec, cents: float) -> None:
    key = (element_id, period.start, period.end)
    adjustments[key] = adjustments.get(key, 0.0) + cents

  def _add_paid(
    lines: list[_CfLine], columns: list[PeriodSpec], share: float = 1.0
  ) -> None:
    """Interest and income tax expense settled in cash, at ``share``."""
    for line in lines:
      row = _CF_PAID_ROWS.get(line.qname or "") if line.kind == "pnl" else None
      paid_id = element_ids.get(row) if row else None
      if paid_id is not None and line.net:
        for column in columns:
          _add(str(paid_id), column, line.net * share)

  def _hold_bill(
    event_id: str,
    lines: list[_CfLine],
    posted: date,
    columns: list[PeriodSpec],
    capex_share: float = 1.0,
  ) -> bool:
    """Record ``lines`` as (part of) a bill when a payable settles investing
    or financing lines; an event's entries accumulate."""
    payable = -sum(
      ln.net
      for ln in lines
      if ln.kind == "working" and ln.concept_id in payable_concepts and ln.net < 0
    )
    settles_flows = any(
      ln.kind == "nonoperating" and _default_flow(ln.concept_id, -ln.net)
      for ln in lines
    )
    if not payable or not settles_flows:
      return False
    capex = round(
      capex_share
      * sum(
        ln.net
        for ln in lines
        if ln.kind == "nonoperating"
        and ln.net > 0
        and _default_flow(ln.concept_id, -ln.net, "investingActivity")
      )
    )
    held = bills.get(event_id)
    if held is None:
      bills[event_id] = _Bill(posted, payable, capex, list(columns))
    else:
      held.posted = min(held.posted, posted)
      held.payable += payable
      held.capex += capex
      held.columns += [c for c in columns if c not in held.columns]
    return True

  for entry_key, lines in entries.items():
    if not lines:
      continue
    posted = dated[entry_key]
    columns = [p for p in periods if p.start <= posted <= p.end]
    if not columns:
      continue
    kinds = {line.kind for line in lines}
    untagged = not any(line.tagged for line in lines)

    if "cash" not in kinds:
      # On the consolidation entity a tagged line is its own flow already;
      # offsetting it here as well would count it twice.
      nonoperating = sum(
        ln.net
        for ln in lines
        if ln.kind == "nonoperating" and not (holds_no_cash and ln.tagged)
      )
      if not nonoperating:
        continue
      # A disposal's gain and the depreciation it writes off belong to the
      # asset side, so a receivable for the proceeds is matched against all
      # of it and none of it reads as working capital.
      disposal = [ln for ln in lines if _disposes(ln)]
      side = nonoperating + sum(ln.net for ln in disposal)
      rest = [ln for ln in lines if ln not in disposal]
      pnl = sum(ln.net for ln in rest if ln.kind == "pnl")
      working = sum(ln.net for ln in rest if ln.kind == "working")
      pnl_matched = _matched(pnl, side)
      working_matched = _matched(working, side + pnl_matched)
      for line in rest:
        if line.kind == "pnl" and pnl_matched and line.concept_id not in reused:
          if other_noncash_id is not None:
            share = -line.net * pnl_matched / pnl
            for column in columns:
              _add(str(other_noncash_id), column, share)
        elif line.kind == "working" and working_matched:
          share = -line.net * working_matched / working
          for column in columns:
            _add(working_leaf[line.concept_id], column, -share)
      for line in disposal:
        if line.kind == "working":
          for column in columns:
            _add(working_leaf[line.concept_id], column, line.net)

      if entry_key in event_of and _hold_bill(
        event_of[entry_key], lines, posted, columns
      ):
        continue
      capex = sum(
        ln.net
        for ln in lines
        if ln.kind == "nonoperating"
        and ln.net > 0
        and _default_flow(ln.concept_id, -ln.net, "investingActivity")
      )
      if capex and acquisition_id is not None:
        consideration = any(
          ln.net < 0 and ln.kind in ("working", "nonoperating") for ln in lines
        )
        if consideration:
          for column in columns:
            _add(str(acquisition_id), column, capex)
      continue

    # The supplemental rows read every cash entry, tagged or not: a tag picks
    # a flow, not whether interest was paid.
    _add_paid(lines, columns, _cash_share(lines))

    if not untagged:
      # The explicit pass already took the tagged cash into its flow, so a
      # payable or receivable it settles comes out of that account's delta;
      # otherwise the bill's back-out above leaves the payment counted twice.
      for line in lines:
        if line.tagged and line.kind == "working":
          for column in columns:
            _add(working_leaf[line.concept_id], column, line.net)
      continue

    bill = settled.get(discharges.get(entry_key, ""))
    if bill and "nonoperating" not in kinds:
      paid_lines = [ln for ln in lines if ln.kind == "working"]
      paid = sum(ln.net for ln in paid_lines)
      # Only the balance this payment settles: a disposal's written-off
      # depreciation is a working line of the bill, not part of what is owed.
      settles = {ln.concept_id for ln in paid_lines}
      owed = [ln for ln in bill if ln.kind == "working"]
      # Paid through clearing, the payment's line is on none of the bill's.
      if any(ln.concept_id in settles for ln in owed):
        owed = [ln for ln in owed if ln.concept_id in settles]
      payable = abs(sum(ln.net for ln in owed))
      if payable and paid:
        fraction = min(abs(paid), payable) / payable
        # A bill that took cash when it posted routed that part on its own
        # entry; the payments settle only what it left owing.
        if _on_account(bill)[0]:
          paid_on_posting = abs(sum(ln.net for ln in bill if ln.kind == "cash"))
          fraction *= payable / (payable + paid_on_posting)
        _add_paid(bill, columns, fraction)
        routed = 0.0
        bill_flows: set[str] = set()
        for line in bill:
          if line.kind != "nonoperating":
            continue
          flow_id = _default_flow(line.concept_id, -line.net)
          if flow_id is None:
            continue
          bill_flows.add(flow_id)
          routed += line.net * fraction
          emitted_flows.add(flow_id)
          for column in columns:
            _add(flow_id, column, -line.net * fraction)
        if routed and len(bill_flows) == 1:
          # The cash a disposal brought in, or a retirement paid, is the
          # asset or debt net of the gain and the depreciation written off.
          flow_id = bill_flows.pop()
          extra = sum(ln.net for ln in bill if _disposes(ln)) * fraction
          routed += extra
          for column in columns:
            _add(flow_id, column, -extra)
        if routed:
          for line in paid_lines:
            for column in columns:
              _add(working_leaf[line.concept_id], column, routed * line.net / paid)
      continue

    if entry_key not in discharges and kinds <= {"cash", "working", "pnl"}:
      to_payables = sum(
        ln.net
        for ln in lines
        if ln.kind == "working" and ln.concept_id in payable_concepts and ln.net > 0
      )
      if to_payables:
        for column in columns:
          unlinked.setdefault((column.start, column.end), []).append(to_payables)

    # The depreciation a sale writes off moved no cash: the proceeds are the
    # asset's cost net of it and of the gain, and it is no depreciation of
    # the period.
    disposal = [ln for ln in lines if _disposes(ln)]
    flows = {
      _default_flow(ln.concept_id, -ln.net) for ln in lines if ln.kind == "nonoperating"
    }
    flows.discard(None)
    if len(flows) != 1:
      continue
    flow_id = flows.pop()
    assert flow_id is not None
    # A receivable or payable for the rest of the price is no cash yet and no
    # working capital: it comes off the flow, which its settlement routes.
    owed, on_account, working = _on_account(lines)
    if not disposal and not owed:
      continue
    emitted_flows.add(flow_id)
    for column in columns:
      _add(flow_id, column, -sum(ln.net for ln in disposal) - owed)
      for ln in disposal:
        if ln.kind == "working":
          _add(working_leaf[ln.concept_id], column, ln.net)
      if owed:
        for ln in on_account:
          _add(working_leaf[ln.concept_id], column, ln.net * owed / working)
    priced = sum(ln.net for ln in lines if ln.kind == "nonoperating")
    priced += sum(ln.net for ln in disposal)
    if owed and priced and entry_key in event_of:
      # Its unpaid part is a bill like any other: capex left owing at period
      # end is disclosed, at the share of the price the payable carries.
      _hold_bill(
        event_of[entry_key],
        lines,
        posted,
        columns,
        capex_share=min(1.0, abs(owed) / abs(priced)),
      )

  # An unlinked payment may settle a bill posted before the window, so the
  # warning weighs those still open too. They are no acquisition of any
  # column: the disclosure is the period's own capex.
  if unlinked:
    earlier = _lines(
      "en.posting_date < :start AND en.triggered_by_event_id IS NOT NULL",
      start=window_start,
    )
    earlier_event = {str(r.entry_id): str(r.triggered_by_event_id) for r in earlier}
    earlier_posted = {str(r.entry_id): r.posting_date for r in earlier}
    for entry_key, lines in _entries(earlier).items():
      if lines and not any(ln.kind == "cash" for ln in lines):
        _hold_bill(earlier_event[entry_key], lines, earlier_posted[entry_key], [])

  # What each bill's linked payments settled, and when: only their payable
  # lines count (a payment through a clearing account carries another
  # working line), and one with no cash line settled the bill in kind.
  payments: dict[str, list[tuple[date, int, bool]]] = {}
  if bills:
    settling: dict[str, tuple[str, date, int, bool]] = {}
    for row in _lines("ev.discharges_event_id = ANY(:events)", events=sorted(bills)):
      line = _classify(row)
      if line is None:
        continue
      event_id, posted, paid, cash = settling.get(
        str(row.entry_id), (str(row.discharges_event_id), row.posting_date, 0, False)
      )
      if line.kind == "working" and line.concept_id in payable_concepts:
        paid += line.net
      settling[str(row.entry_id)] = (
        event_id,
        posted,
        paid,
        cash or line.kind == "cash",
      )
    for event_id, posted, paid, cash in settling.values():
      payments.setdefault(event_id, []).append((posted, paid, cash))

  def _unpaid(event_id: str, bill: _Bill, on: date) -> float:
    paid = sum(net for posted, net, _ in payments.get(event_id, ()) if posted <= on)
    return max(0.0, 1.0 - paid / bill.payable)

  capex_unpaid_id = element_ids.get(_CF_CAPEX_UNPAID_QNAME)
  for event_id, bill in bills.items():
    if not bill.capex:
      continue
    if capex_unpaid_id is not None:
      for column in bill.columns:
        _add(
          str(capex_unpaid_id),
          column,
          bill.capex * _unpaid(event_id, bill, column.end),
        )
    if acquisition_id is not None:
      for posted, paid, cash in payments.get(event_id, ()):
        if cash:
          continue
        for column in periods:
          if column.start <= posted <= column.end:
            _add(str(acquisition_id), column, bill.capex * paid / bill.payable)

  warnings: list[str] = []
  for column in periods:
    amounts = unlinked.get((column.start, column.end))
    if not amounts:
      continue
    open_bills = sum(
      bill.payable * _unpaid(event_id, bill, column.end)
      for event_id, bill in bills.items()
      if bill.posted <= column.end
    )
    if open_bills >= 1:
      warnings.append(
        f"[{column.label}] {len(amounts)} payment(s) to payables "
        f"({cents_to_dollars(sum(amounts)):,.2f}) carry no link to the bill "
        f"they settle, while bills with investing or financing lines "
        f"({cents_to_dollars(round(open_bills)):,.2f}) are unpaid; whatever "
        "capex or loan principal those payments settled is in operating cash. "
        "Link each payment to its bill."
      )

  if not adjustments:
    return emitted_flows, warnings

  meta = {
    str(r.id): r
    for r in session.execute(
      text("SELECT id, qname, name, balance_type FROM elements WHERE id = ANY(:ids)"),
      {"ids": sorted({element_id for element_id, _, _ in adjustments})},
    ).fetchall()
  }
  working_leaves = set(working_leaf.values())
  for (element_id, start, end), cents in adjustments.items():
    row = meta.get(element_id)
    if row is None or not round(cents):
      continue
    if element_id in working_leaves and (element_id, start, end) not in derived:
      continue
    value = cents_to_dollars(round(cents))
    for f in facts:
      if f.element_id == element_id and f.period_start == start and f.period_end == end:
        # Marked before the addition: the mark keeps the value as it was.
        _mark_derived(f, "noncash_and_settlement", amount=value)
        f.value += value
        break
    else:
      facts.append(
        ReportFact(
          element_id=element_id,
          element_qname=row.qname,
          element_name=row.name,
          classification=None,
          balance_type=row.balance_type or "debit",
          value=value,
          period_start=start,
          period_end=end,
          period_type="duration",
          origin="derived",
          derivation={"computations": ["noncash_and_settlement"]},
        )
      )
  return emitted_flows, warnings


def _reconcile_operating_to_cash(
  session: Session,
  facts: list[ReportFact],
  periods: list[PeriodSpec],
  opening_facts: list[ReportFact] | None = None,
  *,
  holds_no_cash: bool = False,
) -> None:
  """Foot the indirect CF to the actual cash-balance movement.

  The consolidation entity (``holds_no_cash``) has no cash: its eliminations
  offset flows its members' own cash moved, so its statement nets to nothing,
  and an untagged elimination is booked here rather than moving the group's
  net change in cash.

  The indirect operating section reverses only depreciation among non-cash
  items, so gains/losses in net income leave the CF off from ΔCash. True
  operating cash is ``ΔCash - Investing - Financing``; the gap is booked as
  one adjustment on ``IncreaseDecreaseInOtherOperatingCapitalNet``.

  Tradeoff: this foots the CF by construction, silencing
  ``_check_cash_flow_tie_out``; a large plug relative to operating cash is
  logged as a warning instead. Skips periods with no cash-anchor balance.
  """

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
  for current in ordered:
    cash_end = cash_by_date.get(current.end)
    if cash_end is not None:
      cash_delta = cash_end - cash_by_date.get(_opening_date(current), 0.0)
    elif holds_no_cash:
      cash_delta = 0.0
    else:
      continue

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
        origin="derived",
        derivation={"computations": ["cash_reconciliation"]},
      )
    )


def _cash_by_date(
  facts: list[ReportFact], opening_facts: list[ReportFact] | None
) -> dict[date, float]:
  cash_by_date: dict[date, float] = {}
  for f in [*_one_column_per_date(facts), *(opening_facts or ())]:
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
  cash_by_date = _cash_by_date(facts, opening_facts)
  # Per column: two columns ending the same day carry different net changes.
  net_change_by_column: dict[tuple[date, date], float] = {}
  for f in facts:
    if f.element_qname == "rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease":
      key = (f.period_start, f.period_end)
      net_change_by_column[key] = net_change_by_column.get(key, 0.0) + f.value

  ordered = sorted(periods, key=lambda p: p.end)
  for current in ordered:
    net_change = net_change_by_column.get((current.start, current.end))
    if net_change is None:
      continue
    cash_end = cash_by_date.get(current.end)
    if cash_end is None and abs(net_change) <= 0.01:
      # No cash moved and none is held (a consolidation entity's books):
      # there is nothing to tie.
      continue
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


# ── Equity roll-forward ───────────────────────────────────────────────────

# The equity statement's change rows are their own concepts, not the cash
# flow's: an owner who pays a company liability directly has contributed
# capital and moved no company cash. Each roll-forward roots at the Style's
# capital concept; the corporate one sums its components through the calc DAG.
_CORPORATE_EQUITY_ROOT = "rs-gaap:StockholdersEquity"
_CAPITAL_ACCOUNT_ROOTS: frozenset[str] = frozenset(
  {"rs-gaap:PartnersCapital", "rs-gaap:MembersEquity"}
)
EQUITY_ROLLFORWARD_ROOTS: frozenset[str] = _CAPITAL_ACCOUNT_ROOTS | {
  _CORPORATE_EQUITY_ROOT
}

_CORPORATE_MOVEMENTS: dict[str, str] = {
  "contribution": "rs-gaap:StockIssuedDuringPeriodValueNewIssues",
  "repurchase": "rs-gaap:StockRepurchasedDuringPeriodValue",
  "distribution": "rs-gaap:Dividends",
  "share_based_compensation": (
    "rs-gaap:AdjustmentsToAdditionalPaidInCapitalSharebasedCompensationRequisiteServicePeriodRecognitionValue"
  ),
  "other": "rs-gaap:StockholdersEquityOther",
}
# The outside owners' share of a consolidated subsidiary, rolled forward
# beside the parent's capital on a group's statement: its share of income is
# the income statement's own line; the movements are read off the flow tags of
# the lines that move it.
_NCI_ROOT = "rs-gaap:MinorityInterest"
_NCI_SHARE_OF_INCOME = "rs-gaap:NetIncomeLossAttributableToNoncontrollingInterest"
_NCI_MOVEMENTS: dict[str, str] = {
  "contribution": "rs-gaap:NoncontrollingInterestIncreaseFromSubsidiaryEquityIssuance",
  "distribution": (
    "rs-gaap:MinorityInterestDecreaseFromDistributionsToNoncontrollingInterestHolders"
  ),
  "deconsolidation": "rs-gaap:MinorityInterestDecreaseFromDeconsolidation",
}
_NCI_FLOW_KIND: dict[str, str] = {
  "rs-gaap:ProceedsFromMinorityShareholders": "contribution",
  "rs-gaap:PaymentsToMinorityShareholders": "distribution",
  "rs-gaap:PaymentsOfDividendsMinorityInterest": "distribution",
}
ROLLFORWARD_ROOTS: frozenset[str] = EQUITY_ROLLFORWARD_ROOTS | {_NCI_ROOT}

# The roots a statement keeps at zero and rolls forward, by block type.
_ROLLFORWARD_ROOTS_BY_BLOCK: dict[str, frozenset[str]] = {
  "equity_statement": ROLLFORWARD_ROOTS,
  "cash_flow_statement": CASH_ROLLFORWARD_ROOTS,
}

# A redemption of units has no row of its own on the capital-account forms.
_CAPITAL_ACCOUNT_MOVEMENTS: dict[str, str] = {
  "contribution": "rs-gaap:PartnersCapitalAccountContributions",
  "repurchase": "rs-gaap:PartnersCapitalOther",
  "distribution": "rs-gaap:PartnersCapitalAccountDistributions",
  "share_based_compensation": "rs-gaap:PartnersCapitalAccountUnitBasedCompensation",
  "other": "rs-gaap:PartnersCapitalOther",
}

# The expense an equity award is recognized through. An entry that books it
# against equity is share-based compensation on the roll-forward, and the
# same fact is the cash flow's non-cash add-back.
_SHARE_BASED_COMPENSATION_QNAME = "rs-gaap:ShareBasedCompensation"

# The equity-side reading of a financing flow: a line tagged, or defaulting
# to, one of these is that kind of equity movement.
_FLOW_MOVEMENT_KIND: dict[str, str] = {
  **dict.fromkeys(
    (
      "rs-gaap:ProceedsFromIssuanceOfCommonStock",
      "rs-gaap:ProceedsFromIssuanceOfPreferredStockAndPreferenceStock",
      "rs-gaap:ProceedsFromContributedCapital",
      "rs-gaap:ProceedsFromPartnershipContribution",
      "rs-gaap:ProceedsFromSaleOfTreasuryStock",
    ),
    "contribution",
  ),
  **dict.fromkeys(
    (
      "rs-gaap:PaymentsForRepurchaseOfCommonStock",
      "rs-gaap:PaymentsForRepurchaseOfEquity",
      "rs-gaap:PaymentsForRepurchaseOfPreferredStockAndPreferenceStock",
    ),
    "repurchase",
  ),
  **dict.fromkeys(
    (
      "rs-gaap:PaymentsOfDividends",
      "rs-gaap:PaymentsOfDividendsCommonStock",
      "rs-gaap:PaymentsOfDividendsPreferredStockAndPreferenceStock",
      "rs-gaap:PaymentsOfOrdinaryDividends",
      "rs-gaap:PaymentsOfCapitalDistribution",
      "rs-gaap:DistributionsMade",
      "rs-gaap:DistributionsMadeToLimitedLiabilityCompanyLlcMember",
      "rs-gaap:DistributionMadeToLimitedLiabilityCompanyLLCMemberCashDistributionsPaid",
    ),
    "distribution",
  ),
}

# Classifications whose line, beside an equity line, makes the movement an
# exchange with an owner rather than an entry against income.
_BALANCE_SHEET_CLASSIFICATIONS = frozenset(
  {"asset", "liability", "contraAsset", "contraLiability", "temporaryEquity"}
)


def _equity_rollforward_form(close_target_qname: str) -> tuple[str, dict[str, str]]:
  """The roll-forward's root concept and movement rows for the Style whose
  earnings close to ``close_target_qname``."""
  if close_target_qname in _CAPITAL_ACCOUNT_ROOTS:
    return close_target_qname, _CAPITAL_ACCOUNT_MOVEMENTS
  return _CORPORATE_EQUITY_ROOT, _CORPORATE_MOVEMENTS


def _equity_descendant_weights(
  calculations: dict[str, list[tuple[str, float]]], root_id: str
) -> dict[str, float]:
  """Every concept the root sums, with the weight it enters the root at
  (the product along its calc path); the root itself at 1."""
  weights = {root_id: 1.0}
  pending = [root_id]
  while pending:
    parent = pending.pop()
    for child, weight in calculations.get(parent, ()):
      if child not in weights:
        weights[child] = weights[parent] * weight
        pending.append(child)
  return weights


def _leaves_the_group(line, start: date) -> bool:
  """A consolidation entity's line that belongs to a member's leaving: its
  deconsolidation entry, or the reversal of its investment elimination in
  the first month it is out, which the engine proposes untagged since it
  moves no cash."""
  if line.adjustment_kind == "deconsolidation":
    return True
  return (
    line.adjustment_kind == "elimination_investment"
    and line.adjusts_left_on is not None
    and line.adjusts_left_on < start
  )


def _emit_equity_movement_facts(
  session: Session,
  facts: list[ReportFact],
  periods: list[PeriodSpec],
  mapping_id: str,
  arc_type: str,
  entity_id: str | None,
  close_target_qname: str,
) -> None:
  """Append the equity statement's change rows, per period, from every
  landed line on an account the roll-forward's root sums — cash or not.

  A line's movement is, in order: its flow tag's; the reducer concept its
  account maps to (a distributions account mapped to ``PaymentsOfDividends``);
  share-based compensation, when the entry books that expense; its account's
  default financing flow, when the entry exchanges with an owner (moves cash,
  or settles or brings an asset or liability); otherwise other. So an owner
  paying the company card is a contribution, and equity booked against any
  other expense is other. Values are
  equity-effect signed, increases positive, as the cash flow's are
  cash-effect signed, so opening + net income + the rows is the closing
  balance by construction; the equity validator checks it.
  """
  root_qname, movement_qnames = _equity_rollforward_form(close_target_qname)
  meta = {
    row.qname: row
    for row in session.execute(
      text("SELECT id, qname, name, balance_type FROM elements WHERE qname = ANY(:q)"),
      {"q": [root_qname, close_target_qname, *movement_qnames.values()]},
    ).fetchall()
  }
  root = meta.get(root_qname)
  if root is None:
    return
  weights = _equity_descendant_weights(load_rs_gaap_calculations(session), root.id)
  # Reducers fold into the earnings home, so they move the root only when it
  # sums that home.
  close_target = meta.get(close_target_qname)
  reducer_qnames = (
    sorted(_EQUITY_FLOW_REDUCER_QNAMES)
    if close_target is not None and close_target.id in weights
    else []
  )

  line_sql = text(f"""
    SELECT
      li.entry_id,
      target.id AS target_id,
      target.qname AS target_qname,
      target.balance_type AS target_balance_type,
      li.debit_amount,
      li.credit_amount,
      tag.qname AS tag_qname,
      en.adjustment_kind,
      adj.left_on AS adjusts_left_on,
      (
        SELECT rf.qname
        FROM associations darc
        JOIN elements rf ON rf.id = darc.from_element_id
        WHERE darc.to_element_id = target.id
          AND darc.association_type = 'derivation'
          AND SIGN(darc.weight) = SIGN(li.credit_amount - li.debit_amount)
        ORDER BY rf.qname
        LIMIT 1
      ) AS default_flow_qname
    FROM line_items li
    JOIN entries en ON en.id = li.entry_id
    LEFT JOIN entities adj ON adj.id = en.adjusts_entity_id
    LEFT JOIN LATERAL ({_primary_mapping("li")}) amap ON TRUE
    JOIN elements target ON target.id = amap.to_element_id
    LEFT JOIN associations fmap
      ON fmap.from_element_id = li.flow_element_id
      AND fmap.association_type = :arc_type
      AND fmap.structure_id = :mapping_id
    LEFT JOIN elements tag ON tag.id = COALESCE(fmap.to_element_id, li.flow_element_id)
    WHERE en.status IN :landed_entry_statuses
      AND en.type <> 'opening'
      AND (:entity_id IS NULL OR en.entity_id = :entity_id)
      AND en.posting_date BETWEEN :start AND :end
      AND (target.id = ANY(:equity_ids) OR target.qname = ANY(:reducer_qnames))
  """).bindparams(landed_entry_bindparam())

  # What else each entry touches: cash, or a balance-sheet account other than
  # the root's own equity.
  counterpart_sql = text(f"""
    SELECT
      li.entry_id,
      target.id AS target_id,
      target.qname AS target_qname,
      target.balance_type AS target_balance_type,
      tcls.identifier AS classification
    FROM line_items li
    LEFT JOIN LATERAL ({_primary_mapping("li")}) amap ON TRUE
    JOIN elements target ON target.id = amap.to_element_id
    LEFT JOIN (
      SELECT et.element_id, t.identifier
      FROM element_traits et
      JOIN traits t ON t.id = et.trait_id
      WHERE et.is_primary = TRUE
        AND t.category = 'elementsOfFinancialStatements'
    ) tcls ON tcls.element_id = target.id
    WHERE li.entry_id = ANY(:entry_ids)
  """)

  for period in periods:
    lines = session.execute(
      line_sql,
      {
        "arc_type": arc_type,
        "mapping_id": mapping_id,
        "entity_id": entity_id,
        "start": period.start,
        "end": period.end,
        "equity_ids": list(weights),
        "reducer_qnames": reducer_qnames,
      },
    ).fetchall()
    if not lines:
      continue

    exchanges_with_owner: set[str] = set()
    books_share_based_compensation: set[str] = set()
    for row in session.execute(
      counterpart_sql,
      {
        "arc_type": arc_type,
        "mapping_id": mapping_id,
        "entry_ids": sorted({line.entry_id for line in lines}),
      },
    ):
      if row.target_id in weights or row.target_qname in reducer_qnames:
        continue
      if row.target_qname == _SHARE_BASED_COMPENSATION_QNAME:
        books_share_based_compensation.add(row.entry_id)
        continue
      classification = row.classification or _infer_classification(
        row.target_qname, row.target_balance_type
      )
      if (
        row.target_qname in _CASH_ANCHOR_QNAMES
        or classification in _BALANCE_SHEET_CLASSIFICATIONS
      ):
        exchanges_with_owner.add(row.entry_id)

    totals: dict[str, float] = {}
    for line in lines:
      net = cents_to_dollars(line.debit_amount - line.credit_amount)
      if line.target_qname in reducer_qnames:
        # The sign the close folds it in with.
        effect = _natural_sign(net, line.target_balance_type or "debit")
      else:
        effect = weights[line.target_id] * _natural_sign(
          net, line.target_balance_type or "credit"
        )
      # A member's leaving is no exchange with an owner: the reversal of its
      # elimination and the results put back are other movements, which net
      # against the capital that leaves with its books.
      kind = "other" if _leaves_the_group(line, period.start) else None
      if kind is None:
        kind = _FLOW_MOVEMENT_KIND.get(line.tag_qname or "")
      if kind is None and line.target_qname in reducer_qnames:
        kind = _FLOW_MOVEMENT_KIND.get(line.target_qname)
      if kind is None and line.entry_id in books_share_based_compensation:
        kind = "share_based_compensation"
      if kind is None and line.entry_id in exchanges_with_owner:
        kind = _FLOW_MOVEMENT_KIND.get(line.default_flow_qname or "")
      qname = movement_qnames[kind or "other"]
      totals[qname] = totals.get(qname, 0.0) + effect

    for qname, value in totals.items():
      concept = meta.get(qname)
      if concept is None or abs(value) < 0.005:
        continue
      facts.append(
        ReportFact(
          element_id=concept.id,
          element_qname=qname,
          element_name=concept.name or qname,
          classification=None,
          balance_type=concept.balance_type or "credit",
          value=value,
          period_start=period.start,
          period_end=period.end,
          period_type="duration",
          origin="derived",
          derivation={"computations": ["equity_movement"]},
        )
      )


def _emit_nci_movement_facts(
  session: Session,
  facts: list[ReportFact],
  periods: list[PeriodSpec],
  mapping_id: str,
  arc_type: str,
  entity_id: str | None,
) -> None:
  """Append the noncontrolling interest's movement rows, per period: a line
  on an account the interest sums, tagged with the outside owners' flow, is a
  contribution or a distribution, equity-effect signed; one in a member's
  deconsolidation entry, or in the reversal of its elimination once it has
  left, is the interest derecognized. A line in an entry that books the
  interest's share of income is that income, which the income statement's
  own fact already carries; any other line has no row, so the roll-forward
  does not reach its ending balance and the validator says so."""
  meta = {
    row.qname: row
    for row in session.execute(
      text("SELECT id, qname, name, balance_type FROM elements WHERE qname = ANY(:q)"),
      {"q": [_NCI_ROOT, *_NCI_MOVEMENTS.values()]},
    ).fetchall()
  }
  root = meta.get(_NCI_ROOT)
  if root is None:
    return
  weights = _equity_descendant_weights(load_rs_gaap_calculations(session), root.id)

  line_sql = text(f"""
    SELECT
      target.id AS target_id,
      target.balance_type AS target_balance_type,
      li.debit_amount,
      li.credit_amount,
      tag.qname AS tag_qname,
      en.adjustment_kind,
      adj.left_on AS adjusts_left_on,
      EXISTS (
        SELECT 1
        FROM line_items other
        LEFT JOIN LATERAL ({_primary_mapping("other")}) omap ON TRUE
        JOIN elements oe ON oe.id = omap.to_element_id
        WHERE other.entry_id = li.entry_id AND oe.qname = :share_of_income
      ) AS books_share_of_income
    FROM line_items li
    JOIN entries en ON en.id = li.entry_id
    LEFT JOIN entities adj ON adj.id = en.adjusts_entity_id
    LEFT JOIN LATERAL ({_primary_mapping("li")}) amap ON TRUE
    JOIN elements target ON target.id = amap.to_element_id
    LEFT JOIN associations fmap
      ON fmap.from_element_id = li.flow_element_id
      AND fmap.association_type = :arc_type
      AND fmap.structure_id = :mapping_id
    LEFT JOIN elements tag ON tag.id = COALESCE(fmap.to_element_id, li.flow_element_id)
    WHERE en.status IN :landed_entry_statuses
      AND en.type <> 'opening'
      AND (:entity_id IS NULL OR en.entity_id = :entity_id)
      AND en.posting_date BETWEEN :start AND :end
      AND target.id = ANY(:nci_ids)
  """).bindparams(landed_entry_bindparam())

  for period in periods:
    totals: dict[str, float] = {}
    for line in session.execute(
      line_sql,
      {
        "arc_type": arc_type,
        "mapping_id": mapping_id,
        "entity_id": entity_id,
        "start": period.start,
        "end": period.end,
        "nci_ids": list(weights),
        "share_of_income": _NCI_SHARE_OF_INCOME,
      },
    ):
      if line.books_share_of_income:
        continue
      if _leaves_the_group(line, period.start):
        kind = "deconsolidation"
      else:
        kind = _NCI_FLOW_KIND.get(line.tag_qname or "")
      if kind is None:
        continue
      net = cents_to_dollars(line.debit_amount - line.credit_amount)
      effect = weights[line.target_id] * _natural_sign(
        net, line.target_balance_type or "credit"
      )
      qname = _NCI_MOVEMENTS[kind]
      totals[qname] = totals.get(qname, 0.0) + effect

    for qname, value in totals.items():
      concept = meta.get(qname)
      if concept is None or abs(value) < 0.005:
        continue
      facts.append(
        ReportFact(
          element_id=concept.id,
          element_qname=qname,
          element_name=concept.name or qname,
          classification=None,
          balance_type=concept.balance_type or "credit",
          value=value,
          period_start=period.start,
          period_end=period.end,
          period_type="duration",
          origin="derived",
          derivation={"computations": ["nci_movement"]},
        )
      )


def _with_opening_balance(
  rows: list[FactRow],
  facts: list[ReportFact],
  periods: list[PeriodSpec],
  hierarchy: list[_HierarchyNode],
  *,
  roots: frozenset[str] = ROLLFORWARD_ROOTS,
  suffixes: tuple[str, str] = (", beginning balance", ", ending balance"),
) -> list[FactRow]:
  """Open each roll-forward on its root's balance the day before each column
  starts, so the statement reads beginning balance + changes = ending balance
  — the equity statement's capital, and on a group with outside owners the
  noncontrolling interest beside it; the cash flow's cash total above net
  change. The opening is another column's closing where one ends that day,
  else the instant ``_emit_opening_equity_facts`` / ``_emit_opening_cash_facts``
  stored; none is 0."""
  # A root's subtree takes the positions just ahead of the root's own, so its
  # opening goes ahead of all of them — in every grid of the Style alike.
  subtree_size = {
    root.element_id: _subtree_size(root) - 1
    for root in hierarchy
    if root.qname in roots
  }
  closings = [
    r
    for r in rows
    if r.element_qname in roots and r.depth == 0 and r.element_id in subtree_size
  ]
  if not closings:
    return rows

  openings: list[FactRow] = []
  dropped: set[int] = set()
  for closing in closings:
    # (end, start) -> value. Two columns can end the same day; the earliest
    # start wins, as ``_one_column_per_date`` decides for the cash flow.
    by_context: dict[tuple[date, date | None], float] = {}
    for f in facts:
      if f.element_id != closing.element_id or f.period_type != "instant":
        continue
      if f.audit_only:
        continue
      key = (f.period_end, f.period_start)
      by_context[key] = by_context.get(key, 0.0) + f.value
    balance_on: dict[date, float] = {}
    for end, start in sorted(by_context, key=lambda k: (k[0], k[1] or date.min)):
      balance_on.setdefault(end, by_context[(end, start)])

    opening_values = [
      _unsigned_zero(balance_on.get(_opening_date(p), 0.0)) for p in periods
    ]
    first = closing.position - subtree_size[closing.element_id]
    if (
      not any(opening_values)
      and not any(closing.values)
      and not any(first <= r.position < closing.position for r in rows)
    ):
      # Kept only to place its opening: a roll-forward with nothing in it.
      dropped.add(id(closing))
      continue

    name = closing.element_name
    closing.element_name = f"{name}{suffixes[1]}"
    closing.period_role = "end"
    openings.append(
      FactRow(
        element_id=closing.element_id,
        element_qname=closing.element_qname,
        element_name=f"{name}{suffixes[0]}",
        classification=closing.classification,
        balance_type=closing.balance_type,
        values=opening_values,
        depth=closing.depth,
        period_role="start",
        position=first - 0.5,
      )
    )
  kept = [r for r in rows if id(r) not in dropped]
  return sorted([*openings, *kept], key=lambda r: r.position)


def _subtree_size(node: _HierarchyNode) -> int:
  return 1 + sum(_subtree_size(child) for child in node.children)


def _emit_opening_equity_facts(
  session: Session,
  facts: list[ReportFact],
  periods: list[PeriodSpec],
  mapping_id: str,
  arc_type: str,
  entity_id: str | None,
  close_target_qname: str,
) -> None:
  """Append each roll-forward root's balance the day before each column
  opens, where no other column ends that day — the earliest column's opening,
  or every column's on a YTD or rolling layout. It is the same balance the
  balance sheet would report that day, earnings closed to date, at an instant
  no column renders. The roots are the Style's capital and the
  noncontrolling interest."""
  capital_qname, _ = _equity_rollforward_form(close_target_qname)
  roots = session.execute(
    text("SELECT id, qname, name, balance_type FROM elements WHERE qname = ANY(:q)"),
    {"q": [capital_qname, _NCI_ROOT]},
  ).fetchall()
  if not roots:
    return
  calculations = load_rs_gaap_calculations(session)
  order = topo_sort_calculations(calculations)
  summed = {
    root.id: _equity_descendant_weights(calculations, root.id) for root in roots
  }

  column_ends = {p.end for p in periods}
  seen: set[date] = set()
  for period in sorted(periods, key=lambda p: p.end):
    opening = _opening_date(period)
    if opening in column_ends or opening in seen:
      continue
    seen.add(opening)
    day = _period_balance_facts(
      session,
      mapping_id,
      opening,
      opening,
      arc_type=arc_type,
      close_target_qname=close_target_qname,
      entity_id=entity_id,
    )
    balances: dict[str, float] = {}
    for f in day:
      if f.period_type == "instant":
        balances[f.element_id] = balances.get(f.element_id, 0.0) + f.value
    present = set(balances)
    computed = resolve_calc_dag(balances, present, calculations, order)
    for root in roots:
      if not present & summed[root.id].keys():
        continue
      facts.append(
        ReportFact(
          element_id=root.id,
          element_qname=root.qname,
          element_name=root.name or root.qname,
          classification="equity",
          balance_type=root.balance_type or "credit",
          value=computed.get(root.id, 0.0),
          period_start=opening,
          period_end=opening,
          period_type="instant",
          origin="derived",
          derivation={"computations": ["opening_equity"]},
        )
      )


ACQUIRED_CASH_QNAME = "rs-gaap:PaymentsToAcquireBusinessesNetOfCashAcquired"


def _acquired_cash_as_a_flow(
  session: Session,
  facts: list[ReportFact],
  opening_facts: list[ReportFact],
  periods: list[PeriodSpec],
  acquired_on: date,
) -> None:
  """An acquired member's cash at acquisition, moved from the opening of the
  column it joins in to that column's investing section: the column measures
  its cash flow from none of it, and the cash acquired is a flow, cash-effect
  signed as the flow pass signs one."""
  joining = next((p for p in periods if _opening_date(p) == acquired_on), None)
  if joining is None:
    return
  # On the eve as an opening, or as an earlier column's end that a group read
  # does not sum.
  held = [
    f
    for f in opening_facts
    if f.period_end == acquired_on and f.element_qname in _CASH_ANCHOR_QNAMES
  ] or [
    f
    for f in facts
    if f.period_type == "instant"
    and f.period_end == acquired_on
    and f.element_qname in _CASH_ANCHOR_QNAMES
  ]
  acquired = 0.0
  for fact in held:
    acquired += fact.value
    fact.value = 0.0
    if fact.source_values:
      fact.source_values = dict.fromkeys(fact.source_values, 0.0)
  if not acquired:
    return
  row = session.execute(
    text("SELECT id, qname, name, balance_type FROM elements WHERE qname = :q"),
    {"q": ACQUIRED_CASH_QNAME},
  ).fetchone()
  if row is None:
    return
  facts.append(
    ReportFact(
      element_id=row.id,
      element_qname=row.qname,
      element_name=row.name or row.qname,
      classification=None,
      balance_type=row.balance_type or "credit",
      value=acquired,
      period_start=joining.start,
      period_end=joining.end,
      period_type="duration",
      origin="derived",
      derivation={"computations": ["cash_acquired"]},
    )
  )


DIVESTED_CASH_QNAME = (
  "rs-gaap:ProceedsFromDivestitureOfInterestInSubsidiariesAndAffiliates"
)


def _root_balance_at(
  session: Session,
  mapping_id: str,
  day: date,
  arc_type: str,
  entity_id: str | None,
  close_target_qname: str,
  root_qname: str,
) -> float | None:
  """A roll-forward root's balance on ``day``, earnings closed to date, as
  the opening-equity rows state it; None when the books hold nothing it
  sums."""
  root = session.execute(
    text("SELECT id FROM elements WHERE qname = :q"), {"q": root_qname}
  ).fetchone()
  if root is None:
    return None
  calculations = load_rs_gaap_calculations(session)
  summed = _equity_descendant_weights(calculations, root.id)
  balances: dict[str, float] = {}
  for f in _period_balance_facts(
    session,
    mapping_id,
    day,
    day,
    arc_type=arc_type,
    close_target_qname=close_target_qname,
    entity_id=entity_id,
  ):
    if f.period_type == "instant":
      balances[f.element_id] = balances.get(f.element_id, 0.0) + f.value
  present = set(balances)
  if not present & summed.keys():
    return None
  computed = resolve_calc_dag(
    balances, present, calculations, topo_sort_calculations(calculations)
  )
  return computed.get(root.id, 0.0)


def _leaving_member_facts(
  session: Session,
  facts: list[ReportFact],
  opening_facts: list[ReportFact],
  periods: list[PeriodSpec],
  left_on: date,
  *,
  mapping_id: str,
  arc_type: str,
  entity_id: str | None,
  close_target_qname: str,
) -> list[ReportFact]:
  """A member read for the column after it left the group, to let its cash
  and its capital go. The column's own facts are not the group's and are
  dropped, its balances at the column's end held at none; the cash it held
  on ``left_on`` is the group's cash divested — the divestiture row, net of
  cash divested, cash-effect signed as every flow is — and the capital it
  held then leaves on the equity statement's other-movements row, where the
  consolidation entity's reversal of its elimination and the results put
  back net against it. Returns the facts kept; the subtotals are computed
  over them afterwards."""
  leaving = [p for p in periods if _opening_date(p) == left_on]
  if not leaving:
    return facts
  columns = {(p.start, p.end) for p in leaving}
  ends = {p.end for p in leaving}
  kept: list[ReportFact] = []
  for f in facts:
    if f.period_type == "instant":
      if f.period_end in ends:
        f.value = 0.0
        if f.close_value is not None:
          f.close_value = 0.0
        if f.source_values:
          f.source_values = dict.fromkeys(f.source_values, 0.0)
      kept.append(f)
    elif (f.period_start, f.period_end) not in columns:
      kept.append(f)
  # On the eve as an opening, or as an earlier column's end.
  held = [
    f
    for f in opening_facts
    if f.period_end == left_on and f.element_qname in _CASH_ANCHOR_QNAMES
  ] or [
    f
    for f in kept
    if f.period_type == "instant"
    and f.period_end == left_on
    and f.element_qname in _CASH_ANCHOR_QNAMES
  ]
  divested = sum(f.value for f in held)
  capital_qname, movements = _equity_rollforward_form(close_target_qname)
  capital = _root_balance_at(
    session, mapping_id, left_on, arc_type, entity_id, close_target_qname, capital_qname
  )
  for qname, value, computation in (
    (DIVESTED_CASH_QNAME, -divested, "cash_divested"),
    (movements["other"], -(capital or 0.0), "capital_left_with_member"),
  ):
    if abs(value) < 0.005:
      continue
    row = session.execute(
      text("SELECT id, qname, name, balance_type FROM elements WHERE qname = :q"),
      {"q": qname},
    ).fetchone()
    if row is None:
      continue
    for column in leaving:
      kept.append(
        ReportFact(
          element_id=row.id,
          element_qname=row.qname,
          element_name=row.name or row.qname,
          classification=None,
          balance_type=row.balance_type or "debit",
          value=value,
          period_start=column.start,
          period_end=column.end,
          period_type="duration",
          origin="derived",
          derivation={"computations": [computation]},
        )
      )
  return kept


def _emit_opening_cash_facts(
  session: Session,
  facts: list[ReportFact],
  periods: list[PeriodSpec],
  mapping_id: str,
  arc_type: str,
  entity_id: str | None,
  *,
  acquired_on: date | None = None,
) -> None:
  """Append the cash total's balance the day before each column opens, where
  no other column ends that day, so the cash flow has a beginning-of-period
  row to open on. None on ``acquired_on``, whose cash is cash acquired. The
  same sum the column ends state through ``_emit_subtotal_facts``: the calc
  arcs from the total to the balance sheet's cash rows, over the mapped
  balances of that day."""
  total = session.execute(
    text("SELECT id, qname, name, balance_type FROM elements WHERE qname = :q"),
    {"q": _CASH_TOTAL_QNAME},
  ).fetchone()
  if total is None:
    return
  calculations = load_rs_gaap_calculations(session)
  summands = {src for src, _ in calculations.get(total.id, ())}
  if not summands:
    return
  order = topo_sort_calculations(calculations)

  column_ends = {p.end for p in periods}
  seen: set[date] = set()
  for period in sorted(periods, key=lambda p: p.end):
    opening = _opening_date(period)
    if opening in column_ends or opening in seen or opening == acquired_on:
      continue
    seen.add(opening)
    balances: dict[str, float] = {}
    for balance in _read_mapped_balances(
      session, mapping_id, opening, opening, arc_type=arc_type, entity_id=entity_id
    ).values():
      balances[balance.element_id] = balances.get(balance.element_id, 0.0) + (
        _natural_sign(balance.net_balance, balance.balance_type)
      )
    present = set(balances)
    if not present & summands:
      continue
    computed = resolve_calc_dag(balances, present, calculations, order)
    facts.append(
      ReportFact(
        element_id=total.id,
        element_qname=total.qname,
        element_name=total.name or total.qname,
        classification=None,
        balance_type=total.balance_type or "debit",
        value=computed.get(total.id, 0.0),
        period_start=opening,
        period_end=opening,
        period_type="instant",
        origin="derived",
        derivation={"computations": ["opening_cash"]},
      )
    )


def _count_unmapped(
  session: Session, mapping_id: str, arc_type: str = "mapping"
) -> int:
  """Count the CoA elements of the mapping's entity that have no association
  of the given arc-type in it."""
  from robosystems.models.extensions.roboledger import COA_SOURCES
  from robosystems.operations.roboledger.reads.accounts import (
    OWNED_ACCOUNT_SQL,
    mapping_account_scope,
  )

  result = session.execute(
    text(f"""
      SELECT COUNT(*) AS cnt
      FROM elements e
      WHERE e.source = ANY(:sources)
        AND (e.taxonomy_id IS NULL OR e.taxonomy_id IN (
          SELECT id FROM taxonomies WHERE taxonomy_type = 'chart_of_accounts'
        ))
        AND {OWNED_ACCOUNT_SQL.format(alias="e")}
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
      **mapping_account_scope(session, mapping_id).params(),
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
  entity_id: str | None = None,
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
        AND (:entity_id IS NULL OR e.entity_id = :entity_id)
        AND target.element_type = 'concept'
        AND target.is_abstract = false
        AND e.posting_date <= :end_date
      GROUP BY source_elem.id, target.qname, target.balance_type, tcls.identifier
    """).bindparams(landed_entry_bindparam()),
    {
      "mapping_id": mapping_id,
      "arc_type": arc_type,
      "end_date": period_end,
      "entity_id": entity_id,
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
      if cls in _INCOME_SIGN:
        classified.append((r.classification is None, r.qname, cls, r))
    if not classified:
      continue
    classified.sort()
    _, _, cls, row = classified[0]
    net = cents_to_dollars(row.total_debits - row.total_credits)
    natural = _natural_sign(net, row.balance_type)
    if _INCOME_SIGN[cls] > 0:
      total_revenue += natural
    else:
      total_expenses += natural

  return total_revenue, total_expenses, total_equity_reductions


# Sign each income-statement class carries into net income, applied to its
# natural-signed value (gain and expenseReversal are credit-natural, loss is
# debit-natural). Every sum that feeds net income or the retained-earnings
# close reads this one table.
_INCOME_SIGN: dict[str, int] = {
  "revenue": 1,
  "gain": 1,
  "expenseReversal": 1,
  "expense": -1,
  "loss": -1,
}


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
    sign = _INCOME_SIGN.get(fact.classification or "")
    if sign is not None:
      if sign > 0:
        total_revenue += _close_value(fact)
      else:
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
    # Marked before the addition: the mark keeps the value as it was.
    _mark_derived(target, "close_to_retained_earnings", amount=net_income)
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
      origin="derived",
      derivation={
        "computations": ["close_to_retained_earnings"],
        "amounts": {"close_to_retained_earnings": net_income},
      },
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
  entity_id: str | None = None,
) -> None:
  """Add un-closed net income from before the current period to the close target.

  Cumulative net income from inception through ``period_end``, minus the
  current period's (already closed). Inception is always safe: a real
  closing entry zeroes the rev/exp accounts it closes, so the cumulative
  sum is only the still-unclosed portion — no double count.
  """
  cumulative_revenue, cumulative_expenses, cumulative_equity_reductions = (
    _cumulative_closeable_sums(session, mapping_id, period_end, arc_type, entity_id)
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
    sign = _INCOME_SIGN.get(fact.classification or "")
    if sign is not None:
      if sign > 0:
        current_revenue += _close_value(fact)
      else:
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
    # Marked before the addition: the mark keeps the value as it was.
    _mark_derived(
      target,
      "close_prior_periods_to_retained_earnings",
      amount=prior_periods_net_income,
    )
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
        element_name=_close_target_label(close_target_qname),
        classification="equity",
        balance_type="credit",
        value=prior_periods_net_income,
        period_start=period_start,
        period_end=period_end,
        period_type="instant",
        origin="derived",
        derivation={
          "computations": ["close_prior_periods_to_retained_earnings"],
          "amounts": {
            "close_prior_periods_to_retained_earnings": prior_periods_net_income
          },
        },
      )
    )


_INSTANT_CLASSIFICATIONS = frozenset(
  {
    "asset",
    "liability",
    "equity",
    "contraAsset",
    "contraLiability",
    "contraEquity",
    "temporaryEquity",
  }
)


def _infer_period_type(classification: str | None) -> str:
  """Fallback only; the element's own ``period_type`` wins when it has one."""
  return "instant" if classification in _INSTANT_CLASSIFICATIONS else "duration"


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
    network_structure_ids,
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
      WHERE ea.structure_id = ANY(:structure_ids)
        AND ea.association_type = 'presentation'
        AND ea.from_element_id != :structure_id
      ORDER BY ea.order_value
    """),
    # A tenant extension's rows hang under the network's own concepts.
    {
      "structure_id": structure_id,
      "structure_ids": network_structure_ids(session, structure_id),
    },
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
        SELECT a.structure_id, a.from_element_id, a.to_element_id, a.weight,
               s.metadata ->> '{EXTENDS_NETWORK_KEY}' IS NOT NULL AS is_extension
        FROM associations a
        JOIN structures s ON s.id = a.structure_id
        WHERE a.association_type = 'calculation'
          AND a.from_element_id IN ({placeholders})
          AND (s.metadata ->> '{EXTENDS_NETWORK_KEY}' IS NULL OR s.is_active)
        ORDER BY a.structure_id, a.order_value
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
  # A tenant extension adds summands to the library's arrangement rather than
  # offering one of its own.
  extension_summands: dict[str, list[tuple[str, float]]] = {}
  for row in result:
    weight = row.weight if row.weight is not None else 1.0
    if row.is_extension:
      if row.to_element_id in element_ids:
        extension_summands.setdefault(row.from_element_id, []).append(
          (row.to_element_id, weight)
        )
      continue
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
  for target, summands in extension_summands.items():
    chosen = calculations.setdefault(target, [])
    chosen.extend(arc for arc in summands if arc not in chosen)
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
  keep: frozenset[str] = frozenset(),
) -> list[FactRow]:
  """Build FactRows (one value per period) in presentation order.

  Pass 1 collects leaf balances and parent rollups, then calc subtotals are
  resolved; pass 2 emits rows post-order. A root in ``keep`` stays at zero:
  a roll-forward's ending balance, whose opening is placed after.
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
        position=len(rows),
      )
    )

  for root in hierarchy:
    _emit(root)

  # The full presentation tree is hundreds of concepts; keep only populated rows.
  rows = [
    r
    for r in rows
    if any(v not in (None, 0, 0.0) for v in r.values)
    or (r.depth == 0 and r.element_qname in keep)
  ]

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
