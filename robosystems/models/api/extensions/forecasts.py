"""API request models for the ``forecast`` Information Block type.

Mirrors the shape of ``rollforward.py`` — request bodies live here, the
typed ``ForecastMechanics`` envelope lives in
``robosystems.models.api.information_block`` alongside the other block-
type mechanics shapes.

The forecast block is the **authored scenario container** (FP&A F-1):
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
  create handler rejects anything else). Value conventions follow the
  catalog: percent levers are decimals per month (0.03 = 3%/month),
  days levers are day counts.

  ``value`` is a uniform fill across the whole horizon;
  ``values_by_period`` overrides individual months (``"YYYY-MM"``
  keys). At least one of the two must be provided. Months covered by
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
          "summary": "FY operating budget — growth + margins + working capital",
          "description": (
            "A 12-month budget scenario: 3%/month revenue growth "
            "compounding, 62% cost-of-revenue rate, 45-day DSO, 30-day "
            "DPO. Lever values are decimals/day-counts per the "
            "rs-driver catalog conventions. After creating, run "
            "`compute-forecast` to derive the forward months."
          ),
          "value": {
            "name": "FY26 Operating Budget",
            "scenario_kind": "budget",
            "horizon_months": 12,
            "levers": [
              {"qname": "rs-driver:RevenueGrowthRate", "value": 0.03},
              {"qname": "rs-driver:CostOfRevenueRate", "value": 0.62},
              {"qname": "rs-driver:DaysSalesOutstanding", "value": 45},
              {"qname": "rs-driver:DaysPayableOutstanding", "value": 30},
            ],
          },
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
      "time."
    ),
  )
  levers: list[LeverAssertionRequest] = Field(
    ...,
    min_length=1,
    description="Lever assertions — at least one.",
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

  Mutable: name, scenario_kind, horizon_months, base_period, levers.
  ``levers`` is a **full replace** when provided (partial lever edits
  would make the asserted set ambiguous). Updating does NOT recompute —
  previously computed scenario months go stale until the next
  ``compute-forecast`` run (the compute-metrics drift semantics).
  """

  structure_id: str = Field(..., description="Structure ID of the forecast block.")
  name: str | None = None
  scenario_kind: Literal["budget", "forecast", "projection"] | None = None
  horizon_months: int | None = Field(None, ge=1, le=36)
  base_period: str | None = Field(None, pattern=_PERIOD_PATTERN)
  levers: list[LeverAssertionRequest] | None = Field(
    None,
    min_length=1,
    description="Full replacement of the lever set when provided.",
  )


class DeleteForecastRequest(BaseModel):
  """Delete a forecast block.

  Removes the scenario's entire parallel universe: the lever FactSet
  AND every computed scenario FactSet (the forward statement/metric
  months keyed by this scenario). Actuals are never touched.
  """

  structure_id: str = Field(..., description="Structure ID of the forecast block.")
