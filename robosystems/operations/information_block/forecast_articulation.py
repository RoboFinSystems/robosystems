"""Forecast articulation — BS roll, schedule projection, derived CF (F-2).

F-1 shipped a footing forecast IS plus rule-driven working-capital
instants; this module articulates the rest of the model per forward
month, all as composition over shipped machinery:

- **Balance-sheet roll**: every base-month BS leaf that isn't rule- or
  schedule-driven carries at its prior value (the workbook's
  forward-rolling BS); Retained Earnings rolls ``RE[m] = RE[m-1] +
  NI[m]`` (the auto-derived-RE doctrine, distributions not yet
  modeled); **cash is the balancing roll** — with every other line
  known, ``cash = LiabilitiesAndStockholdersEquity - Σ other assets``,
  so A = L + E holds *by construction*.
- **Schedule projection**: schedules already emit future ``in_scope``
  facts (duration movements + running-balance instants) in their
  ``factset_type='schedule'`` sets. Duration contributions delta-adjust
  the IS carry (an ending schedule's expense stops instead of carrying
  forever); instant movements roll the BS (accumulated depreciation
  keeps growing; a prepaid keeps drawing down). Contributions route
  through the CoA→rs-gaap mapping's primary target into the elements
  the base sets actually carry — the PP&E gross/contra pair routes onto
  ``PropertyPlantAndEquipmentNet`` exactly the way
  ``fact_grid._synthesize_ppe_net_facts`` nets it for actuals.
- **Derived cash flow**: the actuals doctrine reused — operating CF
  leaves derive from period-over-period BS deltas via the library's
  ``association_type='derivation'`` arcs (a leaf with a direct IS value,
  e.g. DDA, wins over derivation); the residual against the balancing
  ΔCash books to ``IncreaseDecreaseInOtherOperatingCapitalNet`` (the
  ``_reconcile_operating_to_cash`` plug), so the CF foots to ΔCash
  exactly.

Instant movements are computed **per schedule FactSet**, not per
element: two depreciation schedules feeding one anchor must each
contribute their own month-over-month movement — an element-level
aggregate would reverse an ended schedule's entire balance the month
its instants stop.

Everything here is pure given a loaded :class:`ArticulationContext`;
:mod:`.forecast_compute` owns session orchestration and emission.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sqlalchemy import text

from robosystems.operations.roboledger.reports.calc_dag import (
  resolve_calc_dag,
)
from robosystems.operations.roboledger.reports.fact_grid import (
  _CASH_ANCHOR_QNAMES,
  _CF_NET_CHANGE_QNAME,
  _CF_OPERATING_SUBTOTAL_QNAME,
  _CF_RECONCILING_LEAF_QNAME,
)

if TYPE_CHECKING:
  from datetime import date

  from sqlalchemy.orm import Session

# Form-aware earnings homes, most→least common; the roll target is the
# first with a fact in the base BS set (mirrors fact_grid._find_close_target).
_RE_CANDIDATE_QNAMES = (
  "rs-gaap:RetainedEarningsAccumulatedDeficit",
  "rs-gaap:PartnersCapital",
  "rs-gaap:MembersEquity",
)

_NI_QNAME = "rs-gaap:NetIncomeLoss"
_ASSETS_QNAME = "rs-gaap:Assets"
_LE_QNAME = "rs-gaap:LiabilitiesAndStockholdersEquity"
_CF_INVESTING_QNAME = "rs-gaap:NetCashProvidedByUsedInInvestingActivities"
_CF_FINANCING_QNAME = "rs-gaap:NetCashProvidedByUsedInFinancingActivities"

# PP&E routing — the base BS carries the synthesized Net line, not the
# gross/contra pair (fact_grid._synthesize_ppe_net_facts), so schedule
# movements on either side route onto Net with the netting sign.
_PPE_NET_QNAME = "rs-gaap:PropertyPlantAndEquipmentNet"
_PPE_ROUTES: dict[str, float] = {
  "rs-gaap:PropertyPlantAndEquipmentGross": 1.0,
  "rs-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment": -1.0,
}

# The library's original AP derivation arc sourced only the combined
# AccountsPayableAndAccruedLiabilitiesCurrent anchor; tenants mapped to
# the sibling AccountsPayableCurrent (the DPO rule's target) would land
# their AP movement in the reconciling plug instead of the named CF
# line. The library now carries the split-anchor arc too
# (deriv-cf-ap-arc-2), but it reaches an existing tenant only at its
# next resync — this alias bridges that window (the guard below makes
# it a no-op once the arc is present).
_AP_CF_LEAF_QNAME = "rs-gaap:IncreaseDecreaseInAccountsPayableAndAccruedLiabilities"
_AP_ALIAS_SOURCE_QNAME = "rs-gaap:AccountsPayableCurrent"

_RECONCILE_TOLERANCE = 0.005


@dataclass
class ScheduleProjection:
  """Schedule contributions routed into base-set element space.

  ``duration_total``: IS element → month key (``YYYY-MM``) → Σ weighted
  contribution (element-level aggregate is correct for durations — an
  ended schedule's expense should stop).
  ``instant_by_set``: (schedule FactSet id, BS element) → period_end →
  weighted running balance (per-set so movements freeze, never reverse,
  when a schedule ends).
  ``unrouted``: qnames whose contributions had no base-set landing spot
  — surfaced as compute diagnostics, never silently dropped.
  """

  duration_total: dict[str, dict[str, float]] = field(default_factory=dict)
  instant_by_set: dict[tuple[str, str], dict[date, float]] = field(default_factory=dict)
  unrouted: list[str] = field(default_factory=list)


@dataclass
class ArticulationContext:
  """Everything the per-month BS roll + CF derivation needs, loaded once."""

  bs_structure_id: str
  cf_structure_id: str | None
  base_bs_element_ids: list[str]
  bs_prior: dict[str, float]
  cash_element_id: str | None
  re_element_id: str | None
  ni_element_id: str | None
  assets_element_id: str | None
  le_element_id: str | None
  net_change_element_id: str | None
  recon_element_id: str | None
  operating_element_id: str | None
  investing_element_id: str | None
  financing_element_id: str | None
  derivations: dict[str, list[tuple[str, float]]]
  schedules: ScheduleProjection
  diagnostics: list[str] = field(default_factory=list)


def _resolve_qnames(session: Session, qnames: list[str]) -> dict[str, str]:
  rows = session.execute(
    text("SELECT qname, id FROM elements WHERE qname = ANY(:q)"),
    {"q": qnames},
  ).fetchall()
  return {r.qname: r.id for r in rows}


def _load_operating_derivations(
  session: Session,
) -> dict[str, list[tuple[str, float]]]:
  """Operating derivation arcs (CF leaf ← BS source, signed weight).

  Same query + investing/financing exclusion as
  ``fact_grid._derive_cash_flow_facts`` — net-delta derivation is an
  operating-only doctrine (gross investing/financing presentation can't
  come from balance deltas).
  """
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
  derivations: dict[str, list[tuple[str, float]]] = {}
  for cf_id, source_id, weight in rows:
    derivations.setdefault(cf_id, []).append((source_id, float(weight or 1.0)))
  return derivations


def _load_schedule_projection(
  session: Session,
  *,
  mapping_id: str | None,
  entity_id: str,
  window_start: date,
  window_end: date,
  base_is_element_ids: set[str],
  base_bs_element_ids: set[str],
  ppe_net_element_id: str | None,
) -> ScheduleProjection:
  """Load schedule facts in the window, mapped + routed into base space.

  Each schedule CoA element routes through its **primary** mapping
  target (trait-classified targets outrank inferred, qname tiebreak —
  the ``_read_mapped_balances`` close-primary designation), then into
  the element the base set carries: direct when present, the PP&E
  gross/contra pair onto Net, anything else → ``unrouted``.

  Deliberately NOT filtered on ``fact_scope``: the base month sits left
  of ``closed_through``, so its schedule facts are ``historical`` — and
  they are exactly the delta/movement basis. Without them every forward
  month re-adds the base month's expense (DDA compounding by one
  month's depreciation per month — caught live, 2026-07-23) and the
  first month's instant movement re-adds entire running balances.
  """
  projection = ScheduleProjection()
  if mapping_id is None:
    return projection

  rows = session.execute(
    text("""
      SELECT f.fact_set_id, f.value, f.period_end, f.period_type,
             f.element_id AS source_id,
             m.to_element_id AS target_id, target.qname AS target_qname,
             tcls.identifier AS classification
      FROM facts f
      JOIN fact_sets fs ON fs.id = f.fact_set_id
        AND fs.factset_type = 'schedule'
        AND fs.scenario_id IS NULL
        AND fs.entity_id = :entity_id
      JOIN associations m ON m.from_element_id = f.element_id
        AND m.association_type = 'mapping'
        AND m.structure_id = :mapping_id
      JOIN elements target ON target.id = m.to_element_id
        AND target.element_type = 'concept'
        AND target.is_abstract = false
      LEFT JOIN (
        SELECT et.element_id, t.identifier
        FROM element_traits et
        JOIN traits t ON t.id = et.trait_id
        WHERE et.is_primary = TRUE
          AND t.category = 'elementsOfFinancialStatements'
      ) tcls ON tcls.element_id = target.id
      WHERE f.fact_type = 'Numeric'
        AND f.value IS NOT NULL
        AND f.period_end >= :window_start
        AND f.period_end <= :window_end
    """),
    {
      "mapping_id": mapping_id,
      "entity_id": entity_id,
      "window_start": window_start,
      "window_end": window_end,
    },
  ).fetchall()
  if not rows:
    return projection

  # Primary target per SOURCE element (a schedule account mapped to a
  # statement anchor + disclosure concepts contributes once, to the
  # anchor) — the close-primary ranking from _read_mapped_balances.
  by_source: dict[str, list] = {}
  for row in rows:
    by_source.setdefault(row.source_id, []).append(row)
  primary_target: dict[str, tuple[str, str]] = {}
  for source_id, source_rows in by_source.items():
    ranked = sorted(
      source_rows, key=lambda r: (r.classification is None, r.target_qname)
    )
    primary_target[source_id] = (ranked[0].target_id, ranked[0].target_qname)

  unrouted: set[str] = set()
  for row in rows:
    target_id, target_qname = primary_target[row.source_id]
    if (row.target_id, row.target_qname) != (target_id, target_qname):
      continue  # non-primary fan-out arc — contribution counted once
    value = float(row.value)
    if row.period_type == "instant":
      routed_id: str | None = None
      weight = 1.0
      if target_id in base_bs_element_ids:
        routed_id = target_id
      elif target_qname in _PPE_ROUTES and ppe_net_element_id in base_bs_element_ids:
        routed_id = ppe_net_element_id
        weight = _PPE_ROUTES[target_qname]
      if routed_id is None:
        unrouted.add(target_qname)
        continue
      per_set = projection.instant_by_set.setdefault((row.fact_set_id, routed_id), {})
      per_set[row.period_end] = per_set.get(row.period_end, 0.0) + weight * value
    else:
      if target_id not in base_is_element_ids:
        unrouted.add(target_qname)
        continue
      month_key = f"{row.period_end.year:04d}-{row.period_end.month:02d}"
      per_month = projection.duration_total.setdefault(target_id, {})
      per_month[month_key] = per_month.get(month_key, 0.0) + value

  projection.unrouted = sorted(unrouted)
  return projection


def load_articulation_context(
  session: Session,
  *,
  bs_structure_id: str,
  cf_structure_id: str | None,
  mapping_id: str | None,
  entity_id: str,
  base_bs_element_ids: list[str],
  bs_prior: dict[str, float],
  base_is_element_ids: list[str],
  base_start: date,
  horizon_end: date,
) -> ArticulationContext:
  """Resolve the articulation anchors + arcs + schedule projection once."""
  specials = _resolve_qnames(
    session,
    [
      *_CASH_ANCHOR_QNAMES,
      *_RE_CANDIDATE_QNAMES,
      _NI_QNAME,
      _ASSETS_QNAME,
      _LE_QNAME,
      _CF_NET_CHANGE_QNAME,
      _CF_RECONCILING_LEAF_QNAME,
      _CF_OPERATING_SUBTOTAL_QNAME,
      _CF_INVESTING_QNAME,
      _CF_FINANCING_QNAME,
      _PPE_NET_QNAME,
      _AP_CF_LEAF_QNAME,
      _AP_ALIAS_SOURCE_QNAME,
    ],
  )
  base_set = set(base_bs_element_ids)
  diagnostics: list[str] = []

  def _anchor_with_base_fact(
    candidates: tuple[str, ...] | frozenset[str],
  ) -> str | None:
    with_fact = [
      specials[q] for q in candidates if q in specials and specials[q] in base_set
    ]
    if with_fact:
      return with_fact[0]
    resolvable = [specials[q] for q in candidates if q in specials]
    return resolvable[0] if resolvable else None

  cash_element_id = _anchor_with_base_fact(tuple(sorted(_CASH_ANCHOR_QNAMES)))
  if cash_element_id is not None and cash_element_id not in base_set:
    diagnostics.append("no cash anchor in the base balance sheet — cash rolls from 0")
  re_element_id = _anchor_with_base_fact(_RE_CANDIDATE_QNAMES)
  if re_element_id is not None and re_element_id not in base_set:
    diagnostics.append(
      "no earnings-home fact in the base balance sheet — retained earnings rolls from 0"
    )

  derivations = _load_operating_derivations(session)
  ap_leaf_id = specials.get(_AP_CF_LEAF_QNAME)
  ap_alias_id = specials.get(_AP_ALIAS_SOURCE_QNAME)
  if ap_leaf_id is not None and ap_alias_id is not None:
    sources = derivations.setdefault(ap_leaf_id, [])
    if all(src != ap_alias_id for src, _ in sources):
      sources.append((ap_alias_id, 1.0))

  schedules = _load_schedule_projection(
    session,
    mapping_id=mapping_id,
    entity_id=entity_id,
    window_start=base_start,
    window_end=horizon_end,
    base_is_element_ids=set(base_is_element_ids),
    base_bs_element_ids=base_set,
    ppe_net_element_id=specials.get(_PPE_NET_QNAME),
  )
  if schedules.unrouted:
    diagnostics.append(
      "schedule contributions with no base-set landing spot skipped: "
      + ", ".join(schedules.unrouted)
    )

  return ArticulationContext(
    bs_structure_id=bs_structure_id,
    cf_structure_id=cf_structure_id,
    base_bs_element_ids=base_bs_element_ids,
    bs_prior=bs_prior,
    cash_element_id=cash_element_id,
    re_element_id=re_element_id,
    ni_element_id=specials.get(_NI_QNAME),
    assets_element_id=specials.get(_ASSETS_QNAME),
    le_element_id=specials.get(_LE_QNAME),
    net_change_element_id=specials.get(_CF_NET_CHANGE_QNAME),
    recon_element_id=specials.get(_CF_RECONCILING_LEAF_QNAME),
    operating_element_id=specials.get(_CF_OPERATING_SUBTOTAL_QNAME),
    investing_element_id=specials.get(_CF_INVESTING_QNAME),
    financing_element_id=specials.get(_CF_FINANCING_QNAME),
    derivations=derivations,
    schedules=schedules,
    diagnostics=diagnostics,
  )


def schedule_is_delta(
  ctx: ArticulationContext, element_id: str, month: str, reference_month: str
) -> float:
  """IS carry adjustment: this month's schedule expense minus the
  reference month's. Element-level on purpose — an ended schedule's
  expense must STOP, not carry.

  The walk passes the PREVIOUS walk month as the reference (the carried
  value contains that month's schedule contribution, since prior values
  roll), so successive deltas telescope to ``sched[m] - sched[base]``.
  Anchoring every month at the base instead re-subtracts the gap
  cumulatively — the compounding that marched an expense line negative
  on coherent books (caught live, 2026-07-30). Mirrors
  ``schedule_instant_movement``'s ``prev_end`` reference."""
  per_month = ctx.schedules.duration_total.get(element_id)
  if not per_month:
    return 0.0
  return per_month.get(month, 0.0) - per_month.get(reference_month, 0.0)


def schedule_instant_movement(
  ctx: ArticulationContext, element_id: str, month_end: date, prev_end: date
) -> float:
  """BS movement for the month from schedule running balances, summed
  per schedule set. A set with no instant this month contributes 0 —
  its balance froze at the final value (never reversed)."""
  movement = 0.0
  for (_, routed_id), by_date in ctx.schedules.instant_by_set.items():
    if routed_id != element_id:
      continue
    current = by_date.get(month_end)
    if current is None:
      continue
    movement += current - by_date.get(prev_end, 0.0)
  return movement


def roll_balance_sheet(
  ctx: ArticulationContext,
  *,
  month_end: date,
  prev_end: date,
  prior_bs: dict[str, float],
  rule_values: dict[str, float],
  rule_instant_targets: set[str],
  resolved_is: dict[str, float],
  calculations: dict[str, list[tuple[str, float]]],
  calc_order: list[str],
) -> dict[str, float]:
  """One month's full BS leaf values: carry + rules + schedules + RE
  roll + balancing cash. Subtotals stay derived (resolve_calc_dag at
  emission), never carried."""
  calc_targets = set(calculations.keys())
  bs: dict[str, float] = {}

  for element_id in ctx.base_bs_element_ids:
    if element_id in calc_targets:
      continue  # subtotals derive
    if element_id == ctx.cash_element_id or element_id == ctx.re_element_id:
      continue  # balancing / rolled below
    if element_id in rule_instant_targets and element_id in rule_values:
      bs[element_id] = rule_values[element_id]
      continue
    bs[element_id] = prior_bs.get(element_id, 0.0)

  # Rule-driven instants outside the base presentation still roll (the
  # F-1 behavior — e.g. an AP anchor the actuals never carried).
  for element_id in rule_instant_targets:
    if (
      element_id in rule_values
      and element_id not in bs
      and element_id not in calc_targets
    ):
      bs[element_id] = rule_values[element_id]

  for element_id in list(bs):
    movement = schedule_instant_movement(ctx, element_id, month_end, prev_end)
    if movement:
      bs[element_id] += movement

  if ctx.re_element_id is not None:
    net_income = resolved_is.get(ctx.ni_element_id, 0.0) if ctx.ni_element_id else 0.0
    bs[ctx.re_element_id] = prior_bs.get(ctx.re_element_id, 0.0) + net_income

  if ctx.cash_element_id is not None and ctx.le_element_id and ctx.assets_element_id:
    interim = resolve_calc_dag(bs, set(bs), calculations, calc_order)
    le_total = interim.get(ctx.le_element_id, 0.0)
    assets_ex_cash = interim.get(ctx.assets_element_id, 0.0)
    bs[ctx.cash_element_id] = le_total - assets_ex_cash

  return bs


def derive_cash_flow(
  ctx: ArticulationContext,
  *,
  bs: dict[str, float],
  prior_bs: dict[str, float],
  resolved_is: dict[str, float],
  calculations: dict[str, list[tuple[str, float]]],
  calc_order: list[str],
) -> tuple[dict[str, float], float]:
  """One month's CF values (leaves + emission subtotals) and the plug.

  Leaves: a derivation leaf with a direct IS value (DDA) takes it
  (direct-fact-wins); everything else derives ``Σ weight · ΔBS``.
  The residual against the balancing ΔCash books to the reconciling
  leaf — the CF foots to ΔCash exactly, by the actuals plug doctrine.
  """
  cf: dict[str, float] = {}
  for leaf_id, sources in ctx.derivations.items():
    if leaf_id in resolved_is:
      value = resolved_is[leaf_id]
    else:
      value = sum(
        weight * (bs.get(src, 0.0) - prior_bs.get(src, 0.0)) for src, weight in sources
      )
    if value != 0.0:
      cf[leaf_id] = value

  if ctx.ni_element_id is not None and ctx.ni_element_id in resolved_is:
    cf[ctx.ni_element_id] = resolved_is[ctx.ni_element_id]

  plug = 0.0
  if ctx.cash_element_id is not None and ctx.net_change_element_id is not None:
    delta_cash = bs.get(ctx.cash_element_id, 0.0) - prior_bs.get(
      ctx.cash_element_id, 0.0
    )
    computed = resolve_calc_dag(cf, set(cf), calculations, calc_order)
    residual = delta_cash - computed.get(ctx.net_change_element_id, 0.0)
    if abs(residual) > _RECONCILE_TOLERANCE and ctx.recon_element_id is not None:
      cf[ctx.recon_element_id] = cf.get(ctx.recon_element_id, 0.0) + residual
      plug = residual

  # Emission set mirrors the actual monthly CF sets: nonzero leaves plus
  # the Operating + net-change spine (Investing/Financing when nonzero).
  final = resolve_calc_dag(cf, set(cf), calculations, calc_order)
  for subtotal_id in (ctx.operating_element_id, ctx.net_change_element_id):
    if subtotal_id is not None:
      cf[subtotal_id] = final.get(subtotal_id, 0.0)
  for subtotal_id in (ctx.investing_element_id, ctx.financing_element_id):
    if subtotal_id is not None and final.get(subtotal_id, 0.0) != 0.0:
      cf[subtotal_id] = final[subtotal_id]

  return cf, plug


__all__ = [
  "ArticulationContext",
  "ScheduleProjection",
  "derive_cash_flow",
  "load_articulation_context",
  "roll_balance_sheet",
  "schedule_instant_movement",
  "schedule_is_delta",
]
