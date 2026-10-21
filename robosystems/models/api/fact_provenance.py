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
* `rollup`   — a fact that is Σ child·weight over facts in its own set:
                 a statement subtotal through the calculation arcs, or net
                 income through the income signs. Checkable from the set
                 alone.
* `derived`  — a set computed from other facts via a formula or
                 computation (metrics); on a fact, a value a formula or a
                 rule produced — the indirect cash flow from balance-sheet
                 deltas, PP&E net, the earnings close added into equity.
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

Carried at two grains. The **FactSet** carries one descriptor per
period-construction, mandatory at emission — see
`operations/roboledger/fact_set.create_fact_set` (the blessed construction
path) and the `before_insert` backstop on the `FactSet` model. Each **fact**
may carry its own (`facts.provenance`): a statement set pivots its leaves and
computes its subtotals and derivations, and only the fact's own descriptor
says which this one is. A fact without one is *unknown* — never read as
inheriting the set's.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, Field, model_validator


class FactSource(BaseModel):
  """One account's contribution to a pivoted fact, keyed by the entity whose
  pivot summed it: a consolidation entity's line on a parent-chart account
  is the consolidation entity's."""

  entity_id: str = Field(..., description="Entity whose pivot summed the account.")
  account_id: str = Field(..., description="Chart-of-accounts element id.")
  value: float = Field(
    ..., description="Natural-sign dollars this account contributed."
  )


class PivotProvenance(BaseModel):
  """A set built by pivoting the posted ledger (statements / reports).

  On a set, describes its construction, not each fact: only the mapped
  leaves and flow facts aggregate posted line items; the set's subtotals and
  derivations are computed from those leaves. On a fact, says the fact is one
  of those leaves, and ``sources`` names the accounts that foot to it.
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
  stamped_at: datetime | None = Field(
    None,
    description=(
      "When the pivot ran. For a close-stamped set this is the month as "
      "closed on this date: a later mapping change (a reclassification) "
      "moves the live render, never the stamp."
    ),
  )
  posting_filter: dict | None = Field(
    None,
    description="Optional posting filter applied to the source ledger.",
  )
  sources: list[FactSource] | None = Field(
    None,
    description=(
      "On a fact's own descriptor: the accounts the pivot summed into this "
      "value, largest first, footing to it. None on a set's descriptor, and "
      "on a fact pivoted through a posting filter rather than its accounts."
    ),
  )
  content_hash: str | None = Field(
    None,
    description=(
      "On a frozen fact: 'sha256:<hex>' over the ids of the landed lines "
      "under its accounts in its window, so a reconstruction can be checked."
    ),
  )
  line_count: int | None = Field(
    None, description="On a frozen fact: how many landed lines the hash covers."
  )


class RollupChild(BaseModel):
  """One arc of a rollup: the child concept and the weight it was summed at."""

  qname: str = Field(..., description="The child concept's qname.")
  weight: float = Field(..., description="Signed weight the child was summed at.")


class RollupProvenance(BaseModel):
  """A fact that is Σ child·weight over facts in its own set — a statement
  subtotal through the calculation arcs, or net income through the income
  signs. Its inputs are beside it, so a reader with no ledger access can
  re-add them."""

  origin: Literal["rollup"] = "rollup"
  rule: str = Field(
    ...,
    description="What summed the children: 'calculation' (the arcs) or 'net_income'.",
  )
  structure_id: str | None = Field(
    None, description="The calculation structure the arcs came from, when one did."
  )
  children: list[RollupChild] = Field(
    default_factory=list, description="The concepts summed, with their weights."
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
  """Facts computed from other facts via a formula/computation: a set from
  `compute-metrics`, or a fact a formula or a rule produced — the indirect
  cash flow from balance-sheet deltas, PP&E net, a value the earnings close
  added into. At least one of `formula` / `computation` / `source_fact_ids` /
  `source_concepts` must be present; computation-only covers a result with no
  single source row.
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
  source_concepts: list[str] = Field(
    default_factory=list,
    description="Concepts the computation read, by qname, when it read facts.",
  )
  sources: list[FactSource] | None = Field(
    None,
    description=(
      "The accounts of the pivoted part, when the computation added into a "
      "pivoted value (the earnings close into equity)."
    ),
  )
  amounts: dict[str, float] | None = Field(
    None,
    description=(
      "The parts the value is made of, by name: 'pivot' for the pivoted part, "
      "else the computation that produced each."
    ),
  )

  @model_validator(mode="after")
  def _require_some_derivation(self) -> DerivedProvenance:
    if not (
      self.formula or self.computation or self.source_fact_ids or self.source_concepts
    ):
      raise ValueError(
        "DerivedProvenance requires one of formula / computation / "
        "source_fact_ids / source_concepts"
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
  """Balances read from a source outside the ledger, for a reconciliation:
  another system, or the tenant's own schedules.

  The set compares what that source says with what the ledger says at
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
  | RollupProvenance
  | ScheduleProvenance
  | DerivedProvenance
  | AssertedProvenance
  | DocumentProvenance
  | ForecastProvenance
  | FiledProvenance
  | ObservedProvenance,
  Field(discriminator="origin"),
]
