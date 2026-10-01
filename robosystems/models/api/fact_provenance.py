"""Typed provenance descriptor for FactSets — the grounding axis of a fact.

Every fact is produced through a known, typed construction path (the
no-raw-fact-writes envelope/registry discipline), so a fact's origin is
always *recoverable*. `FactProvenance` makes it *recorded*: a typed,
discriminated descriptor stamped on each FactSet at emission so a number
can be walked back to how it was made — or honestly marked as a
projection that never had a posted event behind it.

The union is discriminated on `origin` (the same typed-boundary
discipline as `ArtifactMechanics`, which discriminates on `kind`):

* `pivot`    — a set built by pivoting the posted ledger through a
                 mapping (statements/reports); re-derivable from posted
                 actuals. The mapped leaves are the pivot; the set also
                 carries the calc-DAG rollups and derivations built on
                 them (subtotals, net income, PP&E-net, cash flow).
* `schedule` — forward facts generated from a schedule template
                 (straight-line depreciation, amortization); projected,
                 no posted event yet.
* `derived`  — a set computed from other facts via a formula or
                 computation (metrics).
* `asserted` — provided/manual/external/cross-graph-share; the value is
                 the assertion, with no ledger lineage in this graph.
* `document` — text-block fact bound from a platform Document; the
                 document is the editable source of truth and
                 `content_hash` is the drift signal.
* `forecast` — forward facts derived by the forecast engine's driver
                 cascade (scenario slice, `fact_sets.scenario_id` set);
                 deterministic recompute from lever assertions + actual
                 seeds, no posted event behind any forward month.
* `filed`    — as-filed public disclosure filed with a regulator/authority
                 (SEC EDGAR XBRL); the filing itself is the source of record,
                 citable by accession + filing date. No posted-ledger lineage
                 in this graph — distinct from `asserted` (manual/custom).

* `observed` — balances read from a system outside the ledger (a synced
                 accounting system's trial balance, a bank) and set beside
                 the ledger's own for a reconciliation. The reading is the
                 origin; it is neither a user's assertion nor a filing.

Carried at the **FactSet grain** (one descriptor per period-construction);
facts inherit their parent FactSet's provenance. Stamping is mandatory at
emission — see `operations/roboledger/fact_set.create_fact_set` (the
blessed construction path) and the `before_insert` backstop on the
`FactSet` model.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field, model_validator


class PivotProvenance(BaseModel):
  """A set built by pivoting the posted ledger (statements / reports).

  Describes the set's construction, not each fact: only the mapped leaves
  and flow facts aggregate posted line items; the set's subtotals and
  derivations are computed from those leaves.
  """

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
  """Facts computed from other facts via a formula/computation — today only
  `compute-metrics`. Statement subtotals and the retained-earnings close ride
  under `pivot`. At least one of `formula` / `computation` / `source_fact_ids`
  must be present; computation-only covers a result with no single source row.
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


class DocumentProvenance(BaseModel):
  """Text-block fact bound from a platform Document (markdown).

  The document is the editable source of truth; the fact snapshots its
  text (or one section's) at bind time. `content_hash` is the drift
  signal: if the document moved underneath a bound or filed fact,
  re-binding surfaces the mismatch — the same staleness signal a
  backdated ledger edit gives a `pivot` fact. A document-specialized
  sibling of `asserted`: a human or Operator originated the intent,
  with no posted-ledger lineage.
  """

  origin: Literal["document"] = "document"
  document_id: str = Field(
    ..., description="Platform Document the text was bound from."
  )
  section_id: str | None = Field(
    None,
    description=(
      "Slugified heading id of the bound section (markdown_parser "
      "convention); None when the whole document was bound."
    ),
  )
  content_hash: str = Field(
    ...,
    description="Full sha256 hex digest of the bound text at bind time.",
  )
  asserted_by: str | None = Field(None, description="Actor that bound the text.")


class ForecastProvenance(BaseModel):
  """Forward facts derived by the forecast engine (scenario slice).

  `compute-forecast` walks a scenario's driver cascade month-by-month
  from the last closed actuals and emits the results into scenario-keyed
  standing FactSets (`fact_sets.scenario_id` = the owning forecast
  block). A forecast-specialized sibling of `derived`: deterministic
  recompute from lever assertions + actual seed values, with no posted
  event behind any forward month. Distinct from `schedule` (a single
  self-projecting template) — this is the multi-driver cascade.
  """

  origin: Literal["forecast"] = "forecast"
  scenario_structure_id: str = Field(
    ..., description="Forecast Structure (the scenario) that generated the facts."
  )
  base_period: str = Field(
    ...,
    description="Seed month the walk projected forward from ('YYYY-MM').",
  )
  month_index: int = Field(
    ...,
    description="1-based offset of this month past the actual/forecast seam.",
  )
  drivers: list[str] = Field(
    default_factory=list,
    description="Active lever qnames that shaped this month's cascade.",
  )


class FiledProvenance(BaseModel):
  """As-filed public disclosure filed with a regulator/authority.

  SEC EDGAR XBRL filings are the canonical case: the filing itself is the
  source of record, citable by accession + filing date. Distinct from
  `asserted` — the value is not a manual/custom assertion but a public,
  regulator-filed disclosure. There is no posted-ledger lineage in this
  graph; the filing IS the origin. `source` stays generic so other
  regulator feeds can reuse the arm.
  """

  origin: Literal["filed"] = "filed"
  source: str = Field(
    ...,
    description="Filing system of record (e.g. 'sec_edgar').",
  )
  accession: str | None = Field(
    None, description="Filing accession number, when known."
  )
  filing_date: str | None = Field(
    None, description="Date the filing was accepted (YYYY-MM-DD)."
  )
  filer_cik: str | None = Field(None, description="Filer identifier (CIK for SEC).")
  form: str | None = Field(None, description="Filing form type (10-K, 10-Q, ...).")


class ObservedProvenance(BaseModel):
  """Balances read from a system outside the ledger, for a reconciliation.

  The set compares what that system says with what the ledger says at
  `as_of`. Distinct from `asserted` (nobody typed the value in) and from
  `filed` (nothing was filed): the source was read, and `observed_at` says
  when, because a later reading of the same date can differ.
  """

  origin: Literal["observed"] = "observed"
  source: str = Field(..., description="System the balances were read from.")
  method: str = Field(
    ..., description="Reconciliation method the reading served (source_ledger, ...)."
  )
  as_of: str = Field(..., description="Date the balances are stated at (YYYY-MM-DD).")
  observed_at: str = Field(..., description="When the source was read (ISO 8601, UTC).")
  connection_id: str | None = Field(
    None, description="Connection the source was read through, when there is one."
  )
  basis: str | None = Field(
    None, description="Accounting basis the source reported on, when it states one."
  )


# New provenance classes add an `origin` literal and extend this union.
# Pydantic dispatches on `origin` via the discriminator tag.
FactProvenance = Annotated[
  PivotProvenance
  | ScheduleProvenance
  | DerivedProvenance
  | AssertedProvenance
  | DocumentProvenance
  | ForecastProvenance
  | FiledProvenance
  | ObservedProvenance,
  Field(discriminator="origin"),
]
