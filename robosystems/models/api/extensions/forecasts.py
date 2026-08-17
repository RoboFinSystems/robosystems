"""API request models for the ``forecast`` Information Block type.

Mirrors the shape of ``rollforward.py`` — request bodies live here, the
typed ``ForecastMechanics`` envelope lives in
``robosystems.models.api.information_block`` alongside the other block-
type mechanics shapes.

The forecast block is the **authored scenario container**:
scenario identity (name + ``scenario_kind``), horizon, base period, and
the lever assertions — values on ``rs-driver:*`` catalog elements. The
lever *mechanics* (what each lever drives, and how) are library-seeded
Derive rules; the author asserts only the values. Derived forward facts
land in the existing statement/metric block types stamped with the
scenario, produced by the ``compute-forecast`` operation.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

# ``YYYY-MM`` period keys — same convention as the fiscal calendar.
_PERIOD_PATTERN = r"^\d{4}-(0[1-9]|1[0-2])$"


class LeverAssertionRequest(BaseModel):
  """One lever's asserted values for the scenario.

  ``qname`` must resolve to an ``rs-driver:*`` catalog element (the
  create handler rejects anything else). Each lever's value semantics
  are defined by its catalog element's documentation — surfaced as
  ``documentation`` on the elements bundled in the forecast block's
  envelope (``get-information-block``). Percent levers are decimals but
  their *meaning* varies by lever: growth levers are month-over-month
  rates (``RevenueGrowthRate`` 0.03 = +3%/month, compounding), while
  rate-on-base levers are fractions of the same month's base
  (``CostOfRevenueRate`` 0.62 = cost of revenue at 62% of that month's
  revenues — not a growth rate). Days levers (``DaysSalesOutstanding``,
  ``DaysPayableOutstanding``) are day counts.

  ``value`` is a uniform fill across the whole horizon;
  ``values_by_period`` overrides individual months (``"YYYY-MM"``
  keys) — e.g. a margin-compression ramp asserts a different rate each
  month. At least one of the two must be provided. Months covered by
  neither carry no assertion — the lever's rule is inactive for that
  month and its target falls to the engine's carry-forward default.
  """

  qname: str = Field(
    ...,
    description=(
      "QName of the rs-driver lever element (e.g. ``rs-driver:RevenueGrowthRate``)."
    ),
  )
  value: float | None = Field(
    None,
    description="Uniform value asserted for every month of the horizon.",
  )
  values_by_period: dict[str, float] | None = Field(
    None,
    description=(
      "Per-month overrides keyed by ``YYYY-MM``. Wins over ``value`` "
      "for the months it names."
    ),
  )

  @model_validator(mode="after")
  def _require_some_value(self) -> LeverAssertionRequest:
    if self.value is None and not self.values_by_period:
      raise ValueError(
        f"Lever {self.qname!r} needs a uniform `value` and/or "
        "`values_by_period` overrides."
      )
    return self


class LineAssertionRequest(BaseModel):
  """One statement line's directly asserted values for the scenario.

  The manual-override half of the authored surface: where a lever
  asserts a *driver* (growth %, DSO) whose rule derives the line, a
  line assertion asserts the **line itself** — an rs-gaap (or tenant
  extension) statement leaf pinned to typed values for the months it
  names. Assertions win over driver rules and carry-forward for those
  months (a displaced rule lands in ``skipped``, legibly); months the
  assertion doesn't name keep the engine's normal derivation.

  **Leaves only** — subtotals stay calc-DAG-derived, so a manually set
  line still articulates through RollUps, RE, balancing cash, and the
  derived CF, and stays verification-gated (the whole pitch vs a
  spreadsheet cell). The create handler rejects calc-parent qnames.

  Value/period grammar is identical to levers: ``value`` is a uniform
  fill across the horizon, ``values_by_period`` overrides individual
  months. The canonical uses: zero out a base-month one-off so
  carry-forward stops replicating it, or hold a line at a known budget
  number no driver models.
  """

  qname: str = Field(
    ...,
    description=(
      "QName of the statement leaf to assert (e.g. "
      "``rs-gaap:NonoperatingIncomeExpense``). Must be a calc-DAG leaf; "
      "rs-driver concepts belong in ``levers``."
    ),
  )
  value: float | None = Field(
    None,
    description="Uniform value asserted for every month of the horizon.",
  )
  values_by_period: dict[str, float] | None = Field(
    None,
    description=(
      "Per-month overrides keyed by ``YYYY-MM``. Wins over ``value`` "
      "for the months it names."
    ),
  )

  @model_validator(mode="after")
  def _require_some_value(self) -> LineAssertionRequest:
    if self.value is None and not self.values_by_period:
      raise ValueError(
        f"Line assertion {self.qname!r} needs a uniform `value` and/or "
        "`values_by_period` overrides."
      )
    return self


class LineGrowthRequest(BaseModel):
  """One statement line's asserted growth trajectory for the scenario.

  The generic per-line sibling of ``rs-driver:RevenueGrowthRate``: where
  the catalog lever grows *revenue* through its seeded rule, a line
  growth entry grows **any income-statement leaf** at a month-over-month
  rate — ``value`` -0.05 cuts the line 5% per month, compounding from
  the base month's value. This is what expense trajectories ("opex +2%/mo
  with inflation", "cut costs 5%/mo starting October") use; without it
  every unmodeled line just carries flat.

  Semantics per month: ``line[t] = line[t-1] * (1 + rate[t])``. Months
  the entry doesn't name keep the engine's carry-forward (grow-then-hold
  ramps fall out of ``values_by_period`` naturally). **Duration leaves
  only**: balance-sheet lines roll from the IS and the working-capital
  levers — grow the driving IS line instead. A line already driven by an
  active catalog rule (e.g. Revenues with ``RevenueGrowthRate`` set) or
  named by a ``line_assertions`` entry is rejected — one owner per line.
  """

  qname: str = Field(
    ...,
    description=(
      "QName of the income-statement leaf to grow (e.g. "
      "``rs-gaap:ResearchAndDevelopmentExpense``). Must be a calc-DAG "
      "duration leaf."
    ),
  )
  value: float | None = Field(
    None,
    gt=-1,
    description=(
      "Uniform month-over-month growth rate for every month of the "
      "horizon (decimal: 0.02 = +2%/mo, -0.05 = -5%/mo)."
    ),
  )
  values_by_period: dict[str, float] | None = Field(
    None,
    description=(
      "Per-month rate overrides keyed by ``YYYY-MM``. Wins over "
      "``value`` for the months it names; months named by neither "
      "carry the line's prior value (rate 0)."
    ),
  )

  @model_validator(mode="after")
  def _require_some_value(self) -> LineGrowthRequest:
    if self.value is None and not self.values_by_period:
      raise ValueError(
        f"Line growth {self.qname!r} needs a uniform `value` and/or "
        "`values_by_period` overrides."
      )
    if self.values_by_period and any(v <= -1 for v in self.values_by_period.values()):
      raise ValueError(
        f"Line growth {self.qname!r}: rates must be greater than -1 "
        "(-1 would zero the line — use a line assertion for that)."
      )
    return self


class CreateForecastRequest(BaseModel):
  """Create a forecast block — the authored scenario container.

  ``base_period`` defaults to the fiscal calendar's
  ``closed_through_period`` (else the newest actual report month) —
  the walk projects forward from the last closed actuals. The resolved
  value is stored in the mechanics so recompute is deterministic.
  """

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "name": "FY26 Operating Budget",
          "scenario_kind": "budget",
          "horizon_months": 12,
          "levers": [
            {"qname": "rs-driver:RevenueGrowthRate", "value": 0.03},
            {
              "qname": "rs-driver:CostOfRevenueRate",
              "value": 0.62,
              "values_by_period": {
                "2026-06": 0.44,
                "2026-07": 0.47,
                "2026-08": 0.5,
              },
            },
            {"qname": "rs-driver:DaysSalesOutstanding", "value": 45},
            {"qname": "rs-driver:DaysPayableOutstanding", "value": 30},
          ],
        }
      ]
    }
  )

  name: str = Field(..., description="Human-readable scenario name.")
  scenario_kind: Literal["budget", "forecast", "projection"] = Field(
    "forecast",
    description=(
      "What kind of scenario this is — metadata for display/filtering, "
      "not machinery. All kinds compute identically."
    ),
  )
  horizon_months: int = Field(
    12,
    ge=1,
    le=36,
    description="Forward months to project past the base period.",
  )
  base_period: str | None = Field(
    None,
    pattern=_PERIOD_PATTERN,
    description=(
      "Seed month (``YYYY-MM``) the walk projects forward from. "
      "Defaults to the fiscal calendar's closed-through period, else "
      "the newest actual report month. Resolved and stored at create "
      "time, and it never moves afterwards — every lever is keyed to a "
      "month inside ``base_period + 1 … + horizon_months``, so moving it "
      "would mean restating all of them. ``base_anchor`` decides whether "
      "the walk still *seeds* here once months close under it."
    ),
  )
  base_anchor: Literal["seam", "fixed"] = Field(
    "seam",
    description=(
      "Where the walk takes its opening balances as periods close. "
      "``seam`` (default) re-anchors on the newest closed month inside "
      "the horizon, so the scenario survives a close untouched and its "
      "first forward month rolls off real balances. ``fixed`` pins the "
      "walk to ``base_period`` — the deliberate counterfactual, whose "
      "balances are meant to diverge from actuals."
    ),
  )
  levers: list[LeverAssertionRequest] = Field(
    ...,
    min_length=1,
    description="Lever assertions — at least one.",
  )
  line_assertions: list[LineAssertionRequest] = Field(
    default_factory=list,
    description=(
      "Direct statement-line assertions (manual overrides). Each names "
      "a calc-DAG leaf and wins over driver rules and carry-forward "
      "for the months it asserts."
    ),
  )
  line_growth: list[LineGrowthRequest] = Field(
    default_factory=list,
    description=(
      "Per-line growth trajectories. Each names an income-statement "
      "leaf and grows it month-over-month at the asserted rate — the "
      "generic form of the revenue growth lever, for lines the catalog "
      "doesn't drive (opex trajectories, cost-cut ramps)."
    ),
  )
  entity_id: str | None = Field(
    None,
    description=(
      "Entity the scenario belongs to. Defaults to the graph's "
      "earliest-created entity (single-entity convention)."
    ),
  )


class UpdateForecastRequest(BaseModel):
  """Update a forecast block in place.

  Mutable: name, scenario_kind, horizon_months, base_period, levers,
  line_assertions. ``levers`` and ``line_assertions`` are each a
  **full replace** when provided (partial edits would make the asserted
  set ambiguous); replacing one leaves the other as stored. Updating
  does NOT recompute — previously computed scenario months go stale
  until the next ``compute-forecast`` run (the compute-metrics drift
  semantics).
  """

  structure_id: str = Field(..., description="Structure ID of the forecast block.")
  name: str | None = None
  scenario_kind: Literal["budget", "forecast", "projection"] | None = None
  horizon_months: int | None = Field(None, ge=1, le=36)
  base_period: str | None = Field(None, pattern=_PERIOD_PATTERN)
  base_anchor: Literal["seam", "fixed"] | None = Field(
    None,
    description=(
      "Switch the walk between seam-anchored (default) and pinned to "
      "``base_period``. Changes nothing about the authored window, so "
      "unlike ``base_period`` it needs no levers re-supplied."
    ),
  )
  levers: list[LeverAssertionRequest] | None = Field(
    None,
    min_length=1,
    description="Full replacement of the lever set when provided.",
  )
  line_assertions: list[LineAssertionRequest] | None = Field(
    None,
    description=(
      "Full replacement of the line-assertion set when provided. Pass "
      "an empty list to clear every assertion."
    ),
  )
  line_growth: list[LineGrowthRequest] | None = Field(
    None,
    description=(
      "Full replacement of the line-growth set when provided. Pass an "
      "empty list to clear every growth entry."
    ),
  )


class DeleteForecastRequest(BaseModel):
  """Delete a forecast block.

  Removes the scenario's entire parallel universe: the lever FactSet
  AND every computed scenario FactSet (the forward statement/metric
  months keyed by this scenario). Actuals are never touched.
  """

  structure_id: str = Field(..., description="Structure ID of the forecast block.")
