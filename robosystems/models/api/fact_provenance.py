"""Typed provenance descriptor for FactSets — the grounding axis of a fact.

Every fact is produced through a known, typed construction path (the
no-raw-fact-writes envelope/registry discipline), so a fact's origin is
always *recoverable*. ``FactProvenance`` makes it *recorded*: a typed,
discriminated descriptor stamped on each FactSet at emission so a number
can be walked back to how it was made — or honestly marked as a
projection that never had a posted event behind it.

The union is discriminated on ``origin`` (the same typed-boundary
discipline as ``ArtifactMechanics``, which discriminates on ``kind``):

* ``pivot``    — facts pivoted from the posted ledger (statements/reports);
                 re-derivable from posted actuals.
* ``schedule`` — forward facts generated from a schedule template
                 (straight-line depreciation, amortization); projected,
                 no posted event yet.
* ``derived``  — computed from other facts via a formula/computation
                 (subtotals, retained-earnings close, metrics). Defined
                 now; wired when Metrics un-parks.
* ``asserted`` — provided/manual/external/cross-graph-share; the value is
                 the assertion, with no ledger lineage in this graph.

Carried at the **FactSet grain** (one descriptor per period-construction);
facts inherit their parent FactSet's provenance. Stamping is mandatory at
emission — see ``operations/roboledger/fact_set.create_fact_set`` (the
blessed construction path) and the ``before_insert`` backstop on the
``FactSet`` model.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field, model_validator


class PivotProvenance(BaseModel):
  """Facts pivoted from the posted ledger (statements / reports)."""

  origin: Literal["pivot"] = "pivot"
  mapping_id: str = Field(
    ...,
    description="CoA→framework mapping the pivot ran against.",
  )
  period: str = Field(
    ...,
    description=(
      "Period key this FactSet pivots — 'YYYY-MM-DD/YYYY-MM-DD' for a "
      "duration envelope, or a single 'YYYY-MM-DD' for an instant-only one."
    ),
  )
  arc_type: str | None = Field(
    None,
    description="Association arc type the pivot followed, when tracked.",
  )
  posting_filter: dict | None = Field(
    None,
    description="Optional posting filter applied to the source ledger.",
  )


class ScheduleProvenance(BaseModel):
  """Forward facts generated from a schedule template (projected)."""

  origin: Literal["schedule"] = "schedule"
  structure_id: str = Field(
    ..., description="Schedule Structure that generated the facts."
  )
  method: str = Field(
    ...,
    description="Amortization method (straight_line / declining_balance / ...).",
  )
  params: dict | None = Field(
    None,
    description="Method params (original_amount, residual_value, useful_life_months).",
  )
  period_index: int | None = Field(
    None,
    description="Index of this period within the schedule's series, when applicable.",
  )


class DerivedProvenance(BaseModel):
  """Facts computed from other facts via a formula/computation.

  Subtotals, the retained-earnings close, and (future) metrics. Defined
  now so the union is complete; wired when Metrics un-parks. At least one
  of ``formula`` / ``computation`` / ``source_fact_ids`` must be present —
  computation-only covers auto-derived facts with no single source row
  (retained earnings, persisted subtotals).
  """

  origin: Literal["derived"] = "derived"
  formula: str | None = Field(None, description="Formula expression, when one exists.")
  computation: str | None = Field(
    None,
    description="Named computation when there is no single-expression formula.",
  )
  source_fact_ids: list[str] = Field(
    default_factory=list,
    description="Contributing fact ids, when materialized.",
  )

  @model_validator(mode="after")
  def _require_some_derivation(self) -> DerivedProvenance:
    if not (self.formula or self.computation or self.source_fact_ids):
      raise ValueError(
        "DerivedProvenance requires one of formula / computation / source_fact_ids"
      )
    return self


class AssertedProvenance(BaseModel):
  """Provided / manual / external / cross-graph-share facts.

  The value is the assertion; there is no posted-ledger lineage in this
  graph (cross-graph share collapses here because the originating ledger
  is not present in the target).
  """

  origin: Literal["asserted"] = "asserted"
  source_system: str = Field(
    ...,
    description="What asserted the value (cross_graph_share, custom_amortization_curve, ...).",
  )
  asserted_by: str | None = Field(None, description="Actor that asserted the value.")
  basis_note: str | None = Field(
    None, description="Free-text basis / source reference."
  )


# New provenance classes add an `origin` literal and extend this union.
# Pydantic dispatches on `origin` via the discriminator tag.
FactProvenance = Annotated[
  PivotProvenance | ScheduleProvenance | DerivedProvenance | AssertedProvenance,
  Field(discriminator="origin"),
]
