"""API response models for the Information Block envelope.

Wire-facing types for the cross-domain Information Block construct.
Used by the REST ``create-information-block`` operation, the GraphQL ``informationBlock``/
``informationBlocks`` fields, and the MCP read tools.

Adding a block type: register its ``*Mechanics`` model and add it to
the ``ArtifactMechanics`` discriminated union here. The envelope shape
stays invariant — a union-arm edit, not an envelope redesign.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Discriminator, Field, RootModel

from robosystems.models.api.extensions.forecasts import (
  CreateForecastRequest,
  DeleteForecastRequest,
  UpdateForecastRequest,
)
from robosystems.models.api.extensions.rollforward import (
  AttributionFilter,
  CreateRollforwardRequest,
  DeleteRollforwardRequest,
  UpdateRollforwardRequest,
)
from robosystems.models.api.extensions.schedules import (
  CreateScheduleRequest,
  DeleteScheduleRequest,
  EntryTemplateRequest,
  ScheduleMetadataRequest,
  UpdateScheduleRequest,
)

# ── Atom "Lite" projections ────────────────────────────────────────────────


class ElementLite(BaseModel):
  """Element projection for bundling inside an Information Block envelope.

  Narrower than :class:`LibraryElementResponse` — excludes the heavy fields
  (labels, references, classifications) that library browsing needs but
  block consumers don't. Agents + frontends ask for those on demand via
  the full library GraphQL fields when they need them.
  """

  model_config = ConfigDict(from_attributes=True)

  id: str
  qname: str | None = None
  name: str
  code: str | None = None
  element_type: str = Field(
    ..., description="concept | abstract | axis | member | hypercube"
  )
  is_abstract: bool = False
  is_monetary: bool = True
  balance_type: str | None = None
  period_type: str | None = None
  item_type: str | None = Field(
    None,
    description=(
      "Value-domain vocabulary (monetary | ratio | percent | multiple | "
      "days | string | …). None means untyped; fall back to is_monetary."
    ),
  )
  documentation: str | None = Field(
    None,
    description=(
      "The element's documentation-role label — the catalog's "
      "authoritative value semantics (e.g. whether a percent driver is "
      "a growth rate or a rate-on-base fraction). None when the "
      "element carries no documentation label."
    ),
  )


class ClassificationLite(BaseModel):
  """Classification projection — one row per `association_classifications`
  junction entry.

  Association-side only: concept_arrangement, member_arrangement,
  named_disclosure. Element-side FASB metamodel traits (asset, current,
  operating, …) live in `TraitLite` via `element_traits`.

  Carries enough for the envelope caller to render / filter by category +
  identifier without a follow-up lookup. The full `public.classifications`
  vocabulary catalog (name / description / metadata) is available via the
  library GraphQL surface when callers need the details.
  """

  model_config = ConfigDict(from_attributes=True)

  id: str = Field(..., description="Classification vocabulary row id.")
  category: str = Field(
    ...,
    description=(
      "One of the 3 association-level categories in the "
      "`public.classifications` CHECK constraint: 'concept_arrangement', "
      "'member_arrangement', or 'named_disclosure'."
    ),
  )
  identifier: str = Field(
    ...,
    description=(
      "Vocabulary identifier within the category — e.g. 'RollUp', "
      "'whole_part', 'AssetsRollUp'."
    ),
  )
  is_primary: bool = Field(
    default=True,
    description=(
      "Whether this is the canonical classification for the "
      "(association|element, category) pair. Non-primary rows capture "
      "alternates / AI suggestions alongside the chosen primary."
    ),
  )
  confidence: float | None = Field(
    None,
    description=(
      "AI/adapter-supplied confidence (0.0-1.0). Null for deterministic "
      "library-seeded rows."
    ),
  )
  source: str | None = Field(
    None,
    description=(
      "Provenance — 'arcrole_analysis', 'disclosure_mechanics', "
      "'fac-traits', adapter name, etc."
    ),
  )


class ConnectionLite(BaseModel):
  """Connection (= Association) projection.

  Renamed at the API boundary to match Charlie's ontology vocabulary.
  The underlying storage table is still ``associations``.
  """

  model_config = ConfigDict(from_attributes=True)

  id: str
  from_element_id: str
  to_element_id: str
  association_type: str = Field(
    ...,
    description=(
      "presentation | calculation | mapping | equivalence | "
      "general-special | essence-alias"
    ),
  )
  arcrole: str | None = None
  order_value: float | None = None
  weight: float | None = None
  classifications: list[ClassificationLite] = Field(
    default_factory=list,
    description=(
      "Association-level classifications — concept_arrangement, "
      "member_arrangement, named_disclosure rows from the junction. "
      "Empty for library-seeded associations that haven't been "
      "classified yet."
    ),
  )


class FactLite(BaseModel):
  """Fact projection — just the values the envelope caller cares about."""

  model_config = ConfigDict(from_attributes=True)

  id: str
  element_id: str
  # Denormalized from the related Element row so consumers can render
  # fact rows without an additional join through `elements[]`. Populated
  # by `fact_to_lite` when an element lookup is supplied; null on
  # legacy paths that haven't been migrated yet.
  element_name: str | None = None
  element_qname: str | None = None
  value: float | None = Field(
    None, description="Numeric value; null for Nonnumeric (text-block) facts."
  )
  text_value: str | None = Field(
    None, description="Text payload for Nonnumeric facts; null for numeric."
  )
  fact_type: str = Field("Numeric", description="Numeric | Nonnumeric")
  content_type: str | None = Field(
    None, description="MIME type of text_value (e.g. 'text/markdown')."
  )
  period_start: date | None = None
  period_end: date
  period_type: str
  unit: str = "USD"
  fact_scope: str = Field(..., description="historical | in_scope")
  fact_set_id: str | None = None


class FactSetLite(BaseModel):
  """FactSet projection — period-specific instantiation of the Structure.

  The envelope carries one ``FactSetLite`` per block when a FactSet row
  exists for the requested period; legacy writes that pre-date FactSet
  stamping leave ``fact_set`` null until the expand pass starts
  populating those rows.
  """

  model_config = ConfigDict(from_attributes=True)

  id: str
  structure_id: str | None = None
  period_start: date | None = None
  period_end: date
  factset_type: str = Field(
    ...,
    description=(
      "'report' | 'schedule' | 'custom' | 'disclosure' | 'metric'. Enum "
      "closure enforced by the ``public.fact_sets`` CHECK constraint."
    ),
  )
  entity_id: str
  report_id: str | None = Field(
    None,
    description=(
      "Back-pointer to the ``reports`` table while ``report_id`` still "
      "lives on facts. Drops out once the retirement migration lands."
    ),
  )
  scenario_id: str | None = Field(
    None,
    description=(
      "Scenario axis (the forecast engine). NULL = actuals; non-NULL "
      "names the owning forecast block whose parallel universe this "
      "set belongs to."
    ),
  )
  provenance: dict | None = Field(
    None,
    description=(
      "Typed ``FactProvenance`` descriptor (discriminated on ``origin``: "
      "pivot | schedule | derived | asserted) recording how this FactSet's "
      "facts were constructed. Surfaced as JSON, mirroring how mechanics "
      "is exposed. Null for pre-feature historical FactSets."
    ),
  )


class RuleTargetLite(BaseModel):
  """Polymorphic rule target — points at the atom the rule is scoped to."""

  model_config = ConfigDict(from_attributes=True)

  target_kind: str = Field(
    ...,
    description=(
      "Which atom type the rule targets — 'structure' | 'element' | "
      "'association' | 'taxonomy'. Enum closure enforced by the "
      "``public.rules`` CHECK constraint."
    ),
  )
  target_ref_id: str = Field(
    ...,
    description=(
      "UUID of the target atom — structure_id, element_id, "
      "association_id, or taxonomy_id depending on ``target_kind``."
    ),
  )


class RuleVariableLite(BaseModel):
  """`$Variable` → concept qname binding for a rule expression."""

  model_config = ConfigDict(from_attributes=True)

  variable_name: str = Field(
    ..., description="Local name in the rule expression, e.g. 'Assets'."
  )
  variable_qname: str | None = Field(
    None,
    description=(
      "Concept qname the variable resolves to, e.g. 'fac:Assets'. Null for "
      "tenant CoA elements (which key on `code`/`element_id`, not qname) — "
      "in that case the binding is carried by `variable_element_id`."
    ),
  )
  variable_element_id: str | None = Field(
    None,
    description=(
      "Element id the variable binds to directly. Set for schedule SumEquals "
      "rules over CoA-debit elements that have no qname; null otherwise."
    ),
  )


class VerificationResultLite(BaseModel):
  """Persisted outcome of one Rule evaluation.

  One row per ``public.verification_results`` entry the rule engine
  writes. The envelope surfaces them so the block viewer's
  "Verification Results" tab and MCP ``list-verification-failures``
  tool can render + aggregate without a second round-trip.
  """

  model_config = ConfigDict(from_attributes=True)

  id: str
  rule_id: str
  structure_id: str | None = None
  fact_set_id: str | None = None
  status: str = Field(
    ...,
    description=(
      "'pass' | 'fail' | 'error' | 'skipped'. Enum closure enforced by "
      "the ``public.verification_results`` CHECK constraint."
    ),
  )
  message: str | None = None
  period_start: date | None = None
  period_end: date | None = None
  evaluated_at: datetime | None = None


class VerificationCategorySummary(BaseModel):
  """Pass/fail/skip counts for one ``rule_category`` within a block's
  verification results.

  Drives the per-category accordions in the Verification Results panel.
  ``category`` is the rule's ``rule_category``
  (one of the cm:VerificationRule subclasses), resolved by joining each
  result to its Rule.
  """

  model_config = ConfigDict(from_attributes=True)

  category: str
  total: int = 0
  passed: int = 0
  failed: int = 0
  errored: int = 0
  skipped: int = 0


class VerificationSummary(BaseModel):
  """Server-computed aggregate of a block's ``verification_results``.

  Overall counts plus a per-``rule_category`` breakdown, so the viewer
  renders the grouped Verification Results panel
  without a client-side results→rules join. Status closure is
  ``pass | fail | error | skipped`` (the ``public.verification_results``
  CHECK); ``total`` is their sum.
  """

  model_config = ConfigDict(from_attributes=True)

  total: int = 0
  passed: int = 0
  failed: int = 0
  errored: int = 0
  skipped: int = 0
  by_category: list[VerificationCategorySummary] = Field(default_factory=list)


class RuleLite(BaseModel):
  """Rule projection for the Information Block envelope.

  One row per ``public.rules`` entry scoped to this block. The rule
  engine consumes ``rule_expression`` + ``rule_variables`` to evaluate
  against the in-scope fact set; the envelope surfaces the rules so
  the UI can render them as a checklist alongside any persisted
  verification results.
  """

  model_config = ConfigDict(from_attributes=True)

  id: str
  rule_category: str = Field(
    ...,
    description=(
      "One of 8 cm:VerificationRule subclasses — "
      "AutomatedAccountingAndReportingChecks, "
      "FundamentalAccountingConceptRelation, PeerConsistencyRule, "
      "PriorPeriodConsistencyRule, ReportLevelModelStructureRule, "
      "ReportingSystemSpecificRule, ToDoManualTask, "
      "XBRLTechnicalSyntaxRule."
    ),
  )
  rule_pattern: str | None = Field(
    None,
    description=(
      "Arithmetic / logical pattern evaluated over fact values. "
      "One of 11 cm:BusinessRulePattern mechanisms — Adjustment, "
      "CoExists, EqualTo, Exists, GreaterThan, "
      "GreaterThanOrEqualToZero, LessThan, RollForward, RollUp, "
      "SumEquals, Variance. Null when the rule is a structural check "
      "(see rule_check_kind)."
    ),
  )
  rule_check_kind: str | None = Field(
    None,
    description=(
      "Model-structure check kind evaluated over the association graph. "
      "One of 6 kinds — LeafHasClassification, "
      "LibraryOriginImmutability, NoCycles, NoOrphanArcs, "
      "ParentBeforeChild, UniqueQNameInTaxonomy. Null when the rule is "
      "an arithmetic pattern (see rule_pattern). Exactly one of "
      "rule_pattern / rule_check_kind is non-null per rule."
    ),
  )
  rule_expression: str
  rule_target: RuleTargetLite | None = None
  rule_variables: list[RuleVariableLite] = Field(default_factory=list)
  rule_message: str | None = None
  rule_severity: str = Field(
    "error",
    description=(
      "Failure severity — 'info' | 'warning' | 'error'. Enum closure "
      "enforced by the ``public.rules`` CHECK constraint."
    ),
  )
  rule_origin: str = Field(
    "native",
    description=(
      "Provenance — 'forked' (from an upstream artifact, e.g. Seattle "
      "Method) or 'native' (authored in this seed or by a tenant). Enum "
      "closure enforced by the ``public.rules`` CHECK constraint."
    ),
  )


# ── Artifact Mechanics — discriminated union on `kind` ─────────────────────


class ScheduleMechanics(BaseModel):
  """Closing-entry generator mechanics for ``block_type='schedule'``.

  Reads directly from the typed ``structures.artifact_mechanics`` JSONB
  column. ``entry_template`` and ``schedule_metadata`` are typed
  sub-models (reusing the wire-level request shapes so OpenAPI emits one
  canonical type per concept); the envelope builder falls back to
  ``structures.metadata_`` for legacy Schedule rows that the tenant
  backfill hasn't yet migrated to the typed column.
  """

  kind: Literal["closing_entry_generator"] = "closing_entry_generator"
  entry_template: EntryTemplateRequest = Field(
    ...,
    description=(
      "Debit/credit elements + memo template + auto_reverse flag that "
      "drive fact→entry generation for each in-scope period."
    ),
  )
  schedule_metadata: ScheduleMetadataRequest | None = Field(
    None,
    description=(
      "Method (straight_line / declining_balance / units_of_production), "
      "original_amount, residual_value, useful_life_months, optional "
      "asset_element_id for net-book-value cross-reference."
    ),
  )
  periods_with_entries: int = Field(
    default=0,
    description=(
      "Number of in-scope periods that have at least one closing entry "
      "posted. Runtime state derived at envelope-build time from the "
      "Entry table."
    ),
  )


class MetricMechanics(BaseModel):
  """Derivative mechanics for ``block_type='metric'``.

  A metric block composes its facts from one or more source blocks at
  read time — covenant tests, ratios, KPI trend computations. The typed
  arm ships today so the discriminated union covers all three
  construction modes (declarative / compositional / derivative); the
  derivation evaluator that actually computes facts from source-block
  FactSets is not yet implemented.

  ``source_block_ids`` is the ordered list of Structure ids this metric
  derives from; ``derivation_type`` names the kind of computation
  (``ratio``, ``trailing_twelve_month``, ``covenant_test``, …), and
  ``expression`` carries the agent-authored derivation string that the
  evaluator will consume at envelope build time.
  """

  kind: Literal["metric"] = "metric"
  source_block_ids: list[str] = Field(
    default_factory=list,
    description=(
      "Ordered list of Structure ids this metric sources from. Must be "
      "non-empty at evaluation time; empty lists are accepted so library "
      "scaffolding can register metric templates before source linkage "
      "is wired."
    ),
  )
  derivation_type: str | None = Field(
    None,
    description=(
      "Free-form label for the derivation kind — 'ratio', "
      "'trailing_twelve_month', 'covenant_test', etc. The evaluator "
      "dispatches on this tag; the set may be locked with a CHECK "
      "constraint once the derivation catalog stabilizes."
    ),
  )
  expression: str | None = Field(
    None,
    description=(
      "Derivation expression in the metric DSL — evaluated at envelope "
      "read time to produce the derivative fact value. Opaque string "
      "today; the metric-side parser / evaluator is not yet implemented."
    ),
  )
  unit: str = Field(
    "ratio",
    description=(
      "Output unit of the derived value — 'ratio', 'percent', 'USD', "
      "'count', etc. Used by the renderer to format the metric badge."
    ),
  )


class RollforwardMechanics(BaseModel):
  """Filter-based attribution mechanics for ``block_type='rollforward'``.

  Filter-based attribution: each block decomposes one BS source
  element's period delta into a list of flow concepts via declared
  :class:`AttributionFilter` predicates. The renderer evaluates the
  filters against ledger LineItems at envelope-build time, emits one
  attributed fact per filter per period, and arbitrates any residual
  against the default change tag fallback.

  Reads directly from the typed ``structures.artifact_mechanics`` JSONB
  column. ``attribution_filters`` rides as nested JSON; the predicate
  union widens as new predicate shapes are added — currently only
  ``line_item_metadata_field`` is carried.
  """

  kind: Literal["rollforward"] = "rollforward"
  bs_source_element_id: str = Field(
    ...,
    description=(
      "Element id of the balance-sheet source whose period delta this "
      "block decomposes. Resolved from ``bs_source_qname`` at create "
      "time."
    ),
  )
  bs_source_qname: str = Field(
    ...,
    description=(
      "QName of the BS source element (e.g. "
      "``mini:CashAndCashEquivalents``). Round-tripped for caller "
      "convenience; ``bs_source_element_id`` is authoritative."
    ),
  )
  default_change_tag_element_id: str | None = Field(
    None,
    description=(
      "Element id of the default change tag — the fallback flow "
      "concept that receives any residual (Δ BS − Σ filter matches). "
      "Null when no default is declared; behavior on residual then "
      "follows ``validation_mode``."
    ),
  )
  default_change_tag_qname: str | None = Field(
    None,
    description=(
      "QName of the default change tag (e.g. "
      "``rs-gaap:IncreaseDecreaseInCashAndCashEquivalents``). "
      "Round-tripped for caller convenience and operator-readable "
      "envelopes; ``default_change_tag_element_id`` is authoritative. "
      "Null iff ``default_change_tag_element_id`` is null."
    ),
  )
  attribution_filters: list[AttributionFilter] = Field(
    default_factory=list,
    description=(
      "Filter predicates routing LineItems to flow concepts. The "
      "renderer evaluates each filter against the period's LineItems, "
      "aggregates signed amounts, and emits one fact per filter per "
      "period."
    ),
  )
  validation_mode: Literal["strict", "residual_as_default", "warn_only"] = Field(
    "residual_as_default",
    description=(
      "Renderer arbitration policy when Σ filter matches != Δ BS. "
      "``strict`` raises; ``residual_as_default`` emits the residual "
      "as a default-tag fact (the common case); ``warn_only`` logs and "
      "lets the imbalance pass."
    ),
  )


class LeverAssertionLite(BaseModel):
  """One lever's persisted assertion inside ``ForecastMechanics``.

  The create handler expands the wire-level assertion (uniform ``value``
  + per-month overrides) into the explicit ``values_by_period`` map so
  compute never interpolates — every asserted month is stated. The
  values are duplicated as authored facts in the scenario's lever
  FactSet (rules for mechanics, **facts for values** — the facts are
  what ``compute-forecast`` binds); this mechanics copy is the
  operator-legible round-trip shape.
  """

  qname: str = Field(..., description="rs-driver lever element qname.")
  element_id: str = Field(..., description="Resolved tenant element id.")
  item_type: str | None = Field(
    None,
    description="Format family from the catalog (percent | days | ...).",
  )
  values_by_period: dict[str, float] = Field(
    ...,
    description="Expanded per-month assertions keyed by ``YYYY-MM``.",
  )


class LineAssertionLite(BaseModel):
  """One statement line's persisted direct assertion inside
  ``ForecastMechanics``.

  The manual-override sibling of :class:`LeverAssertionLite`: a lever
  asserts a *driver* whose rule derives a line; a line assertion pins
  the **line itself** (a calc-DAG leaf) to typed values for the months
  it names — winning over driver rules and carry-forward for exactly
  those months (a displaced rule surfaces in the compute response's
  ``skipped`` list). Subtotals stay calc-DAG-derived, so a manual line
  still articulates through RollUps, RE, balancing cash, and derived
  CF, and stays verification-gated.

  Same persistence doctrine as levers: values are duplicated as
  authored facts in the scenario's lever FactSet (facts are what
  ``compute-forecast`` binds); this mechanics copy is the
  operator-legible round-trip shape.
  """

  qname: str = Field(..., description="Asserted statement-leaf qname.")
  element_id: str = Field(..., description="Resolved tenant element id.")
  item_type: str | None = Field(
    None,
    description="Format family from the element (monetary | ...).",
  )
  period_type: str = Field(
    "duration",
    description=(
      "The element's period type — duration assertions pin IS lines; "
      "instant assertions pin BS lines through the roll."
    ),
  )
  values_by_period: dict[str, float] = Field(
    ...,
    description="Expanded per-month assertions keyed by ``YYYY-MM``.",
  )


class LineGrowthLite(BaseModel):
  """One statement line's persisted growth trajectory inside
  ``ForecastMechanics``.

  The generic per-line form of the revenue growth lever: grows an
  income-statement leaf month-over-month at the asserted rate
  (``line[t] = line[t-1] * (1 + rate[t])``), compounding from the base
  month's value. Months without a rate keep the engine's carry-forward.
  Duration leaves only; disjoint from ``line_assertions`` and from any
  active catalog rule's target (one owner per line).

  Persistence deviates from levers/assertions deliberately: rates are
  NOT duplicated as facts in the scenario FactSet — a growth rate on a
  monetary statement element would be a unit-lying fact. This mechanics
  copy is the single authored store; ``compute-forecast`` binds rates
  from here.
  """

  qname: str = Field(..., description="Grown statement-leaf qname.")
  element_id: str = Field(..., description="Resolved tenant element id.")
  item_type: str = Field(
    "percent",
    description="Always 'percent' — the grid row renders rates, not values.",
  )
  values_by_period: dict[str, float] = Field(
    ...,
    description="Expanded per-month growth rates keyed by ``YYYY-MM``.",
  )


class ForecastMechanics(BaseModel):
  """Authored scenario container for ``block_type='forecast'`` (FP&A F-1).

  The block IS the scenario: its structure id is the ``scenario_id``
  every derived forward FactSet carries (NULL = actuals). The authored
  surface is exactly this — scenario identity, horizon, base period,
  lever assertions; everything downstream is derived by
  ``compute-forecast`` (levers → driven rs-gaap anchors via the
  rs-driver Derive rules → carry-forward for unmodeled IS lines →
  calc-DAG subtotals), landing in the EXISTING statement/metric block
  types stamped with the scenario. Reads directly from the typed
  ``structures.artifact_mechanics`` JSONB column.
  """

  kind: Literal["forecast"] = "forecast"
  scenario_kind: Literal["budget", "forecast", "projection"] = Field(
    "forecast",
    description="Scenario kind — display/filter metadata, not machinery.",
  )
  horizon_months: int = Field(
    ...,
    ge=1,
    le=36,
    description="Forward months projected past the base period.",
  )
  base_period: str = Field(
    ...,
    description=(
      "Seed month (``YYYY-MM``) the walk projects forward from — "
      "resolved at create time (request → fiscal calendar "
      "closed-through → newest actual report month) and stored so "
      "recompute is deterministic."
    ),
  )
  levers: list[LeverAssertionLite] = Field(
    ...,
    description="Expanded lever assertions (authoring order).",
  )
  line_assertions: list[LineAssertionLite] = Field(
    default_factory=list,
    description=(
      "Direct statement-line assertions (authoring order) — manual "
      "overrides that win over driver rules and carry-forward for the "
      "months they name."
    ),
  )
  line_growth: list[LineGrowthLite] = Field(
    default_factory=list,
    description=(
      "Per-line growth trajectories (authoring order) — each grows an "
      "income-statement leaf month-over-month at the asserted rate, "
      "compounding from the base month."
    ),
  )
  computed_months: int = Field(
    default=0,
    description=(
      "Number of forward months with computed scenario FactSets. "
      "Runtime state filled at envelope-build time — 0 until the first "
      "compute-forecast run."
    ),
  )


class StatementMechanics(BaseModel):
  """Renderer mechanics for the statement family of block types.

  Covers ``balance_sheet``, ``income_statement``, ``cash_flow_statement``,
  and ``equity_statement``. All fields are optional so library-seeded
  rows that haven't been enriched yet still validate against an empty
  tagged body. The existing ``statement(...)`` GraphQL field continues
  to serve rendered output; this mechanics model is the source of truth
  for future renderer configuration.
  """

  kind: Literal["statement_renderer"] = "statement_renderer"
  template_id: str | None = Field(
    None,
    description=(
      "Pinned template id — when set, the renderer uses that template's "
      "layout instead of the block's default. The templates table is "
      "not yet implemented; the column is reserved so tenant writes can "
      "stamp it without another migration round-trip when it lands."
    ),
  )
  rollup_root_element_ids: list[str] = Field(
    default_factory=list,
    description=(
      "Element ids that anchor the statement's roll-up roots (e.g. the "
      "Assets and LiabilitiesAndEquity totals on a Balance Sheet). "
      "Empty on library-seeded rows until tenant adoption."
    ),
  )
  period_comparisons: int = Field(
    default=1,
    ge=1,
    le=4,
    description=(
      "Number of period columns to render in comparative mode: 1 = "
      "single-period, 2 = prior-period comparison, 3-4 = multi-year "
      "trailing view. Defaults to single-period; overridden by the "
      "template when one is attached."
    ),
  )


# New block-type mechanics models add a `kind` literal and extend this
# union. Pydantic dispatches on `kind` via the discriminator tag.
ArtifactMechanics = Annotated[
  ScheduleMechanics
  | StatementMechanics
  | MetricMechanics
  | RollforwardMechanics
  | ForecastMechanics,
  Field(discriminator="kind"),
]


# ── Information Model + Artifact envelope components ──────────────────────


class InformationModelResponse(BaseModel):
  """The block's intrinsic shape — concept + member arrangement patterns."""

  concept_arrangement: str | None = Field(
    None,
    description=(
      "roll_up | roll_forward | variance | adjustment | set | arithmetic | "
      "textblock. Null for block types where the concept arrangement is "
      "implicit in their mechanics."
    ),
  )
  member_arrangement: str | None = Field(
    None,
    description=(
      "is_a | whole_part | nested_whole_part | "
      "two_dimension_aggregation | complex_aggregating_whole_part, "
      "or null if non-hypercube."
    ),
  )


class ArtifactResponse(BaseModel):
  """The block's producible-artifact envelope — topic, template, mechanics."""

  topic: str | None = Field(
    None, description="Structure.description — the block's human-readable topic."
  )
  renderer_note: str | None = Field(
    None, description="e.g. 'in thousands', 'except per share'."
  )
  template: dict[str, Any] | None = Field(
    None,
    description=(
      "Reusable layout (ordering, subtotals, styling) when attached. "
      "First-class templates are not yet implemented; this field is "
      "always null on currently-shipped block types."
    ),
  )
  mechanics: ArtifactMechanics


# ── View projections ───────────────────────────────────────────────────────


class RenderingRowLite(BaseModel):
  """One row of a server-side rendered statement.

  Mirrors :class:`FactRow` from the legacy
  :mod:`robosystems.operations.roboledger.reports.fact_grid` but lives at
  the API boundary so envelope consumers don't depend on the
  fact-grid module. ``values`` is one entry per period column in
  :class:`RenderingLite.periods`.
  """

  model_config = ConfigDict(from_attributes=True)

  element_id: str
  element_qname: str | None = None
  element_name: str
  classification: str | None = Field(
    None,
    description=(
      "FASB elementsOfFinancialStatements trait identifier — 'asset', "
      "'liability', 'equity', 'revenue', 'expense'. Surfaced so the "
      "viewer can color-code or group rows without a follow-up trait "
      "lookup."
    ),
  )
  balance_type: str | None = None
  item_type: str | None = Field(
    None,
    description=(
      "Value-domain format family from the element (monetary | ratio | "
      "percent | multiple | days | …). Drives per-row value formatting; "
      "None falls back to is_monetary on the element."
    ),
  )
  values: list[float | None] = Field(default_factory=list)
  text_value: str | None = Field(
    None,
    description=(
      "Narrative payload for text-block disclosure rows (markdown); "
      "numeric rows carry values instead."
    ),
  )
  is_subtotal: bool = False
  depth: int = 0


class RenderingPeriodLite(BaseModel):
  """One period column in a rendered statement."""

  model_config = ConfigDict(from_attributes=True)

  start: date
  end: date
  label: str | None = None
  forecast: bool | None = Field(
    None,
    description=(
      "True when this column comes from a forecast scenario's FactSet "
      "— the machine-readable seam marker (labels also carry a "
      "'(forecast)' suffix, but consumers should key styling off this "
      "flag, not label parsing). None/absent = an actuals column."
    ),
  )


class ValidationLite(BaseModel):
  """Outcome of guard-rail validation on a rendered statement.

  Distinct from :class:`VerificationResultLite` (which surfaces the
  rule-engine outcomes from ``public.verification_results``). This lite
  type carries the synchronous guard-rail checks computed at
  envelope-build time — accounting equation, totals foot, etc.
  """

  model_config = ConfigDict(from_attributes=True)

  passed: bool = True
  checks: list[str] = Field(default_factory=list)
  failures: list[str] = Field(default_factory=list)
  warnings: list[str] = Field(default_factory=list)


class RenderingLite(BaseModel):
  """Pre-computed rendering projection of an Information Block.

  Computed server-side at envelope-build time for blocks where rendering
  is deterministic (the statement family today; future block types add
  their own rendering builders). The frontend's ``BlockView``
  ``Rendering`` projection consumes this directly — no client-side
  rollup, depth computation, or calculation walk needed.
  """

  model_config = ConfigDict(from_attributes=True)

  rows: list[RenderingRowLite] = Field(default_factory=list)
  periods: list[RenderingPeriodLite] = Field(default_factory=list)
  validation: ValidationLite | None = None
  unmapped_count: int = 0


class ChartSeriesLite(BaseModel):
  """One plottable series in a chart panel.

  Carries structure and identity only — the values live in the sibling
  ``rendering.rows`` (join on ``element_id``), so the chart arm never
  duplicates the value matrix. ``key`` is the stable series identity for
  client state (colors, toggles); today it equals ``element_id``, and
  future axes (the forecast scenario) arrive as new fields on this
  model, never a new arm shape.
  """

  model_config = ConfigDict(from_attributes=True)

  key: str = Field(..., description="Stable series id — element_id today.")
  element_id: str
  label: str = Field(..., description="Display name for legends.")


class ChartPanelLite(BaseModel):
  """One chart panel — series sharing a y-axis format family.

  Mixed-unit catalogs are unplottable on one axis, so the server groups
  rows into panels by ``item_type`` family (NULL falls back to
  ``is_monetary``). The x-axis is always ``rendering.periods``.
  """

  model_config = ConfigDict(from_attributes=True)

  label: str | None = Field(
    None, description="Panel heading — e.g. 'Monetary', 'Ratios'."
  )
  item_type: str | None = Field(
    None,
    description=(
      "Format family shared by the panel's series (monetary | ratio | "
      "percent | multiple | days); None for the untyped fallback panel."
    ),
  )
  kind: str = Field("line", description="Per-panel mark — 'line' or 'bar'.")
  series: list[ChartSeriesLite] = Field(default_factory=list)


class ChartLite(BaseModel):
  """Server-shaped chart projection — panel/series CONFIG, never values.

  The second real server-computed View arm (after ``rendering``). Values
  come from ``rendering.rows`` joined by ``element_id``; the x-axis is
  ``rendering.periods``. Renderers (report-components) turn one panel
  into one chart.
  """

  model_config = ConfigDict(from_attributes=True)

  panels: list[ChartPanelLite] = Field(default_factory=list)


class ViewProjections(BaseModel):
  """Charlie's six ``type-of View`` arms, surfaced at the envelope boundary.

  Each projection is computed server-side at envelope-build time when
  its source data is available. The frontend's ``BlockView`` dispatcher
  routes to the projection component matching the user's selected view
  mode; missing projections (those still in backlog) render as empty
  states without breaking the dispatcher.

  Today: ``rendering`` is computed for the statement family, and
  ``chart`` (the 7th arm — panel/series config over the rendering's
  rows and periods) for metric blocks.
  Other arms (``fact_table``, ``model_structure``, ``verification_results``,
  ``report_elements``, ``business_rules``) come online as their backend
  support lands; ``fact_table`` is trivially derivable from
  ``InformationBlockEnvelope.facts`` and may stay as a frontend-only
  projection.
  """

  model_config = ConfigDict(from_attributes=True)

  rendering: RenderingLite | None = None
  chart: ChartLite | None = None


# ── Envelope root ──────────────────────────────────────────────────────────


class InformationBlockEnvelope(BaseModel):
  """The Information Block exchange format.

  One envelope per block instance. Carries the block's identity + type,
  Information-Model attributes, the Artifact branch (mechanics +
  topic/template), and bundled atoms (elements, connections, facts).
  Rules / dimensions / FactSet / verification_results are present-but-
  empty for blocks where the upstream content (rule engine, FactSet
  expand, dimension catalog) has not yet been implemented.
  """

  id: str
  block_type: str = Field(..., description="Discriminator — 'schedule', …")
  name: str
  display_name: str = Field(
    ..., description="Registry-sourced display label (e.g., 'Schedule')."
  )
  category: str = Field(
    ..., description="Registry-sourced sidebar grouping ('Close', 'Reporting', …)."
  )

  taxonomy_id: str | None = Field(
    None,
    description=(
      "Source taxonomy the Structure was seeded from. Always present for "
      "currently-registered block types (the Structure → Taxonomy FK is "
      "non-null); declared optional to keep the shape forward-compatible "
      "with future synthetic blocks that don't originate from a taxonomy."
    ),
  )
  taxonomy_name: str | None = Field(
    None, description="Display name of the source taxonomy."
  )

  disclosure_id: str | None = Field(
    None,
    description=(
      "Qname of the named Disclosure this block corresponds to "
      "(e.g., 'disclosures:BalanceSheet'), when an inbound "
      "reportedDisclosure-requiresDisclosure arc identifies one. "
      "Null for tenant-authored blocks without a Disclosure mapping."
    ),
  )

  information_model: InformationModelResponse
  artifact: ArtifactResponse

  elements: list[ElementLite] = Field(default_factory=list)
  connections: list[ConnectionLite] = Field(default_factory=list)
  facts: list[FactLite] = Field(default_factory=list)
  rules: list[RuleLite] = Field(default_factory=list)

  # Reserved for later phases — declared so the envelope shape is stable.
  dimensions: list[dict[str, Any]] = Field(default_factory=list)
  fact_set: FactSetLite | None = Field(
    None,
    description=(
      "The period-specific FactSet this envelope instantiates. Null "
      "when the underlying block has no FactSet row yet — typically "
      "library-seeded statement Structures with no tenant-generated "
      "facts, or Schedule rows written before the create-side FactSet "
      "stamping was added."
    ),
  )
  verification_results: list[VerificationResultLite] = Field(default_factory=list)
  verification_summary: VerificationSummary | None = Field(
    None,
    description=(
      "Server-computed aggregate over ``verification_results`` — overall "
      "pass/fail/error/skip counts plus a per-rule_category breakdown for "
      "the grouped Verification Results panel. Null when the block has no "
      "verification results."
    ),
  )

  view: ViewProjections = Field(
    default_factory=ViewProjections,
    description=(
      "Server-computed view projections (Charlie's six type-of View "
      "arms). ``view.rendering`` carries pre-computed rows + periods + "
      "validation for blocks where rendering is deterministic (the "
      "statement family today). Other projections come online as "
      "their backend support lands — see :class:`ViewProjections`."
    ),
  )


# ── Request models ─────────────────────────────────────────────────────────


class _CreateScheduleArm(BaseModel):
  """Create-information-block body for `block_type="schedule"`.

  Carries a typed schedule payload — full schedule shape is exposed
  inline in the OpenAPI schema so SDK callers see every required field.
  """

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "block_type": "schedule",
          "payload": {
            "name": "Office Building Depreciation",
            "element_ids": [
              "<depreciation_expense_element_id>",
              "<accumulated_depreciation_element_id>",
            ],
            "period_start": "2026-01-01",
            "period_end": "2030-12-31",
            "monthly_amount": 83333,
            "entry_template": {
              "debit_element_id": "<depreciation_expense_element_id>",
              "credit_element_id": "<accumulated_depreciation_element_id>",
              "entry_type": "adjusting",
            },
          },
        }
      ]
    }
  )

  block_type: Literal["schedule"] = Field(
    ...,
    description="Discriminator value selecting this arm.",
  )
  payload: CreateScheduleRequest = Field(
    ...,
    description="Schedule creation payload.",
  )


# Block types whose construction handler still raises 501 — surfaced in
# the union so the OpenAPI schema is honest about which discriminator
# values the registry accepts. To promote a value to its own typed arm:
# (1) add a `_Create<Block>Arm` modeled on `_CreateScheduleArm`,
# (2) drop the value from `_LEGACY_BLOCK_TYPES`,
# (3) add the new arm to `_CreateInformationBlockArms`.
# The `test_create_arm_union_covers_registry` drift test enforces that
# every registry entry remains covered.
_LEGACY_BLOCK_TYPES = Literal[
  "balance_sheet",
  "income_statement",
  "cash_flow_statement",
  "equity_statement",
  "comprehensive_income",
  "regulatory_disclosure",
  "metric",
]


class _CreateLegacyArm(BaseModel):
  """Create-information-block body for block types that don't yet have
  a typed construction path at the API boundary.

  Statement-family blocks (balance_sheet, income_statement,
  cash_flow_statement, equity_statement, comprehensive_income) are
  constructed via `create-report`, not this endpoint. Disclosure
  structures (regulatory_disclosure) are vocabulary, authored via
  `create-taxonomy-block`; their facts land when `create-report`
  picks them. Metric blocks are recognized but their evaluator has
  not shipped. Calling this endpoint with one of these block types
  returns HTTP 501 with a hint pointing to the correct construction
  path.
  """

  block_type: _LEGACY_BLOCK_TYPES = Field(
    ...,
    description=(
      "Statement-family or metric block type. The endpoint returns "
      "501 for these values — statements are constructed via "
      "`create-report`; metric construction is pending."
    ),
  )
  payload: dict[str, Any] = Field(
    default_factory=dict,
    description=(
      "Untyped payload — typed-arm validation is skipped because the "
      "dispatch handler raises 501 before the payload is consumed."
    ),
  )


class _CreateRollforwardArm(BaseModel):
  """Create-information-block body for ``block_type="rollforward"``.

  Carries a typed rollforward payload. The block decomposes the period
  change in a BS source element across the declared attribution
  filters.
  """

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "block_type": "rollforward",
          "payload": {
            "name": "Cash and Cash Equivalents Rollforward",
            "bs_source_qname": "mini:CashAndCashEquivalents",
            "default_change_tag_qname": None,
            "attribution_filters": [
              {
                "target_qname": "mini:ProceedsFromInvestmentsByOwner",
                "predicate": {
                  "kind": "line_item_metadata_field",
                  "field": "transaction_description_code",
                  "values": ["mini:ProceedsFromInvestmentsByOwner"],
                },
              }
            ],
          },
        }
      ]
    }
  )

  block_type: Literal["rollforward"] = Field(
    ...,
    description="Discriminator value selecting this arm.",
  )
  payload: CreateRollforwardRequest = Field(
    ...,
    description="Rollforward creation payload.",
  )


class _CreateForecastArm(BaseModel):
  """Create-information-block body for ``block_type="forecast"``.

  Carries a typed forecast payload — the authored scenario container:
  scenario identity, horizon, base period, lever assertions on
  ``rs-driver:*`` catalog elements. Run ``compute-forecast`` after
  creating to derive the forward months.
  """

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "block_type": "forecast",
          "payload": {
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

  block_type: Literal["forecast"] = Field(
    ...,
    description="Discriminator value selecting this arm.",
  )
  payload: CreateForecastRequest = Field(
    ...,
    description="Forecast creation payload.",
  )


_CreateInformationBlockArms = Annotated[
  _CreateScheduleArm | _CreateRollforwardArm | _CreateForecastArm | _CreateLegacyArm,
  Discriminator("block_type"),
]


class CreateInformationBlockRequest(RootModel[_CreateInformationBlockArms]):
  """Create an Information Block. The body is a discriminated union on
  `block_type`: pick the arm matching the block type you want to
  create. The schedule arm carries a fully typed payload; statement
  and metric arms accept an untyped payload but currently return HTTP
  501 (statements are constructed via `create-report`; metric
  construction is pending)."""

  @property
  def block_type(self) -> str:
    """Discriminator value of the resolved arm."""
    return self.root.block_type

  @property
  def payload(
    self,
  ) -> (
    CreateScheduleRequest
    | CreateRollforwardRequest
    | CreateForecastRequest
    | dict[str, Any]
  ):
    """Payload of the resolved arm. Typed for the schedule, rollforward,
    and forecast arms; a raw dict for the legacy arms (the dispatch
    handler validates it at runtime against the registry entry's
    request model)."""
    return self.root.payload


class _UpdateScheduleArm(BaseModel):
  """Update-information-block body for `block_type="schedule"`.

  Carries a typed schedule update payload — full editable shape is
  exposed inline.
  """

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "block_type": "schedule",
          "payload": {
            "structure_id": "struct_depr_office_2026",
            "name": "Office Building Depreciation (Renamed)",
          },
        }
      ]
    }
  )

  block_type: Literal["schedule"] = Field(
    ...,
    description="Discriminator value selecting this arm.",
  )
  payload: UpdateScheduleRequest = Field(
    ...,
    description="Schedule update payload.",
  )


class _UpdateLegacyArm(BaseModel):
  """Update-information-block body for block types that don't yet have
  a typed update path at the API boundary.

  Statement-family blocks are library-seeded and immutable. Metric
  block updates are not yet implemented. Calling this endpoint with
  one of these block types returns HTTP 501.
  """

  block_type: _LEGACY_BLOCK_TYPES = Field(
    ...,
    description=(
      "Statement-family or metric block type. Updates return 501 — "
      "statement Structures are library-seeded; metric updates are "
      "pending."
    ),
  )
  payload: dict[str, Any] = Field(
    default_factory=dict,
    description=(
      "Untyped payload — typed-arm validation is skipped because the "
      "dispatch handler raises 501 before the payload is consumed."
    ),
  )


class _UpdateRollforwardArm(BaseModel):
  """Update-information-block body for ``block_type="rollforward"``.

  Carries a typed rollforward update payload. Mutable fields: name,
  default_change_tag_qname, attribution_filters, validation_mode.
  """

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "block_type": "rollforward",
          "payload": {
            "structure_id": "struct_rf_cash_2026",
            "name": "Cash Rollforward (Renamed)",
          },
        }
      ]
    }
  )

  block_type: Literal["rollforward"] = Field(
    ...,
    description="Discriminator value selecting this arm.",
  )
  payload: UpdateRollforwardRequest = Field(
    ...,
    description="Rollforward update payload.",
  )


class _UpdateForecastArm(BaseModel):
  """Update-information-block body for ``block_type="forecast"``.

  Mutable: name, scenario_kind, horizon_months, base_period, levers
  (full replace). Updating does not recompute — run ``compute-forecast``
  to refresh the scenario's derived months.
  """

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "block_type": "forecast",
          "payload": {
            "structure_id": "struct_fy26_budget",
            "levers": [
              {"qname": "rs-driver:RevenueGrowthRate", "value": 0.04},
              {"qname": "rs-driver:CostOfRevenueRate", "value": 0.62},
              {"qname": "rs-driver:DaysSalesOutstanding", "value": 45},
              {"qname": "rs-driver:DaysPayableOutstanding", "value": 30},
            ],
          },
        }
      ]
    }
  )

  block_type: Literal["forecast"] = Field(
    ...,
    description="Discriminator value selecting this arm.",
  )
  payload: UpdateForecastRequest = Field(
    ...,
    description=(
      "Forecast update payload. Updating does NOT recompute — lever, "
      "horizon, or base-period changes leave the scenario's computed "
      "months stale until the next `compute-forecast` run."
    ),
  )


_UpdateInformationBlockArms = Annotated[
  _UpdateScheduleArm | _UpdateRollforwardArm | _UpdateForecastArm | _UpdateLegacyArm,
  Discriminator("block_type"),
]


class UpdateInformationBlockRequest(RootModel[_UpdateInformationBlockArms]):
  """Update an Information Block. The body is a discriminated union on
  `block_type` mirroring `CreateInformationBlockRequest`. The schedule
  arm carries a fully typed update payload; statement and metric arms
  return HTTP 501 (statements are library-seeded; metric updates are
  pending)."""

  @property
  def block_type(self) -> str:
    return self.root.block_type

  @property
  def payload(
    self,
  ) -> (
    UpdateScheduleRequest
    | UpdateRollforwardRequest
    | UpdateForecastRequest
    | dict[str, Any]
  ):
    return self.root.payload


class _DeleteScheduleArm(BaseModel):
  """Delete-information-block body for `block_type="schedule"`.

  Carries a typed schedule delete payload — just the `structure_id`.
  """

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "block_type": "schedule",
          "payload": {"structure_id": "struct_depr_office_2026"},
        }
      ]
    }
  )

  block_type: Literal["schedule"] = Field(
    ...,
    description="Discriminator value selecting this arm.",
  )
  payload: DeleteScheduleRequest = Field(
    ...,
    description="Schedule delete payload.",
  )


class _DeleteLegacyArm(BaseModel):
  """Delete-information-block body for block types that don't yet have
  a typed delete path at the API boundary.

  Statement-family blocks cannot be deleted per tenant (the underlying
  Report should be archived via the report APIs instead). Metric
  deletion is not yet implemented. Calls return HTTP 501.
  """

  block_type: _LEGACY_BLOCK_TYPES = Field(
    ...,
    description=(
      "Statement-family or metric block type. Deletion returns 501 — "
      "statements are library-seeded (archive the underlying Report "
      "instead); metric deletion is pending."
    ),
  )
  payload: dict[str, Any] = Field(
    default_factory=dict,
    description=(
      "Untyped payload — typed-arm validation is skipped because the "
      "dispatch handler raises 501 before the payload is consumed."
    ),
  )


class _DeleteRollforwardArm(BaseModel):
  """Delete-information-block body for ``block_type="rollforward"``.

  Cascades through any synthetic facts produced by this block's filter
  evaluations. The underlying ledger LineItems are not touched.
  """

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "block_type": "rollforward",
          "payload": {"structure_id": "struct_rf_cash_2026"},
        }
      ]
    }
  )

  block_type: Literal["rollforward"] = Field(
    ...,
    description="Discriminator value selecting this arm.",
  )
  payload: DeleteRollforwardRequest = Field(
    ...,
    description="Rollforward delete payload.",
  )


class _DeleteForecastArm(BaseModel):
  """Delete-information-block body for ``block_type="forecast"``.

  Removes the scenario's entire parallel universe — the lever FactSet
  and every computed scenario FactSet. Actuals are never touched.
  """

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "block_type": "forecast",
          "payload": {"structure_id": "struct_fy26_budget"},
        }
      ]
    }
  )

  block_type: Literal["forecast"] = Field(
    ...,
    description="Discriminator value selecting this arm.",
  )
  payload: DeleteForecastRequest = Field(
    ...,
    description="Forecast delete payload.",
  )


_DeleteInformationBlockArms = Annotated[
  _DeleteScheduleArm | _DeleteRollforwardArm | _DeleteForecastArm | _DeleteLegacyArm,
  Discriminator("block_type"),
]


class DeleteInformationBlockRequest(RootModel[_DeleteInformationBlockArms]):
  """Delete an Information Block. The body is a discriminated union on
  `block_type` mirroring `CreateInformationBlockRequest`. The schedule
  arm carries a fully typed delete payload; statement and metric arms
  return HTTP 501."""

  @property
  def block_type(self) -> str:
    return self.root.block_type

  @property
  def payload(
    self,
  ) -> (
    DeleteScheduleRequest
    | DeleteRollforwardRequest
    | DeleteForecastRequest
    | dict[str, Any]
  ):
    return self.root.payload


class DeleteInformationBlockResponse(BaseModel):
  """Response for ``delete-information-block``.

  The envelope is gone once the block is deleted, so the response is a
  thin confirmation instead — structure_id + block_type + name for
  caller bookkeeping.
  """

  deleted: Literal[True] = True
  structure_id: str
  block_type: str
  name: str


class EvaluateRulesRequest(BaseModel):
  """Request body for the ``evaluate-rules`` operation.

  Runs every rule scoped to ``structure_id`` (plus element/association-
  scoped rules for the structure's atoms), binds ``$Variable`` references
  to facts via qname lookup, and writes one
  :class:`VerificationResult` row per rule.

  Optional ``period_start`` / ``period_end`` narrow the fact-binding
  window; without them the engine uses the most recent ``in_scope`` fact
  for each element regardless of period.
  """

  structure_id: str = Field(
    ...,
    description=(
      "Structure to evaluate rules for. Resolves all rules scoped to "
      "this structure plus rules attached to its elements and "
      "associations."
    ),
  )
  fact_set_id: str | None = Field(
    None,
    description=(
      "Optional FactSet id to stamp on each VerificationResult row. "
      "Allows results to be scoped to a specific period run once write "
      "paths populate the FactSet table on every run."
    ),
  )
  period_start: date | None = Field(
    None,
    description="Lower bound on the fact period window (inclusive).",
  )
  period_end: date | None = Field(
    None,
    description="Upper bound on the fact period window (inclusive).",
  )

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {"structure_id": "str_balance_sheet"},
        {
          "structure_id": "str_income_statement",
          "period_start": "2026-01-01",
          "period_end": "2026-03-31",
        },
        {
          "structure_id": "str_balance_sheet",
          "fact_set_id": "fs_01HVF8T0M2YTAY3BBNRH0V0",
          "period_start": "2026-01-01",
          "period_end": "2026-03-31",
        },
      ]
    }
  )


class EvaluateRulesResponse(BaseModel):
  """Response for the ``evaluate-rules`` operation.

  ``results`` is the full list of :class:`VerificationResultLite` rows
  written by this evaluation run. ``summary`` gives counts keyed by
  status for quick display without iterating the list.
  """

  structure_id: str
  results: list[VerificationResultLite]
  summary: dict[str, int] = Field(
    default_factory=dict,
    description=(
      "Status counts keyed by outcome string: "
      "``{'pass': N, 'fail': N, 'error': N, 'skipped': N}``."
    ),
  )


class ComputedMetricLite(BaseModel):
  """One metric computed by a ``compute-metrics`` run."""

  rule_id: str = Field(..., description="Derive rule that produced the value.")
  element_id: str = Field(..., description="Metric element the fact was written for.")
  element_qname: str | None = Field(
    None, description="Metric element qname (e.g. rs-metric:CurrentRatio)."
  )
  name: str = Field(..., description="Metric display name.")
  value: float = Field(..., description="Computed value.")
  unit: str = Field(
    ..., description="Fact unit — 'USD' for monetary, 'days' for days, else 'pure'."
  )
  period_type: str = Field(..., description="'instant' or 'duration'.")
  item_type: str | None = Field(
    None,
    description=(
      "Format family from the metric element (monetary | ratio | percent "
      "| multiple | days). None means untyped; fall back to unit."
    ),
  )


class SkippedMetricLite(BaseModel):
  """One metric a ``compute-metrics`` run could not compute.

  Soft-fail by design: a missing operand fact (e.g. InterestExpense for a
  debt-free entity) or an undefined ratio (division by zero) skips the
  metric with a reason — it never errors the run.
  """

  rule_id: str = Field(..., description="Derive rule that was skipped.")
  element_qname: str | None = Field(
    None, description="Metric element qname the rule targets."
  )
  reason: str = Field(..., description="Why the metric was skipped.")
  missing: list[str] = Field(
    default_factory=list,
    description="Operand qnames with no bound fact at the period, when applicable.",
  )


class ComputeMetricsRequest(BaseModel):
  """Request body for the ``compute-metrics`` operation.

  Resolves the ``Derive`` rules scoped to the metric block, binds each
  rule's operands to the entity's most recent persisted report facts at
  ``period_end``, evaluates, and upserts the period's standing
  ``factset_type='metric'`` FactSet (re-running a period replaces its
  facts). One standing FactSet per (structure, entity, period_end) — the
  accumulating time series.
  """

  structure_id: str = Field(
    ...,
    description="Metric block structure (block_type='metric') to compute.",
  )
  period_end: date = Field(
    ...,
    description=(
      "Period end to compute at. Operands bind to report facts whose "
      "period_end matches exactly (instant balances as of this date; "
      "durations ending on it)."
    ),
  )
  period_start: date | None = Field(
    None,
    description=(
      "Optional lower bound for duration-operand binding and the "
      "standing FactSet's period_start."
    ),
  )
  entity_id: str | None = Field(
    None,
    description=(
      "Entity to compute for. Defaults to the graph's earliest-created "
      "entity (the primary entity for single-entity graphs)."
    ),
  )
  scenario_id: str | None = Field(
    None,
    description=(
      "Compute on a scenario slice: operands bind that scenario's facts "
      "(actuals as the fallback across the seam) and the standing metric "
      "set is stamped with the scenario. None (the default) computes "
      "actuals only. Pass a forecast block's structure id after "
      "compute-forecast to extend the metric series into its forward "
      "months."
    ),
  )

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {"structure_id": "str_key_financial_metrics", "period_end": "2026-03-31"},
        {
          "structure_id": "str_key_financial_metrics",
          "period_end": "2026-03-31",
          "period_start": "2025-04-01",
        },
      ]
    }
  )


class ComputeMetricsResponse(BaseModel):
  """Response for the ``compute-metrics`` operation."""

  structure_id: str
  entity_id: str
  period_end: date
  fact_set_id: str | None = Field(
    None,
    description=(
      "Standing metric FactSet for the period — None when every metric "
      "was skipped and no prior set existed."
    ),
  )
  computed: list[ComputedMetricLite] = Field(default_factory=list)
  skipped: list[SkippedMetricLite] = Field(default_factory=list)


class MetricObservation(BaseModel):
  """One externally-observed value in an ``assert-metrics`` request."""

  qname: str = Field(
    ...,
    min_length=1,
    description=(
      "Metric element qname (e.g. rsx:GithubStars). Must resolve to a "
      "concept on the structure's presentation catalog."
    ),
  )
  value: float = Field(..., description="Observed value.")


class AssertedMetricLite(BaseModel):
  """One metric written by an ``assert-metrics`` run."""

  element_id: str = Field(..., description="Metric element the fact was written for.")
  element_qname: str = Field(..., description="Metric element qname.")
  name: str = Field(..., description="Metric display name.")
  value: float = Field(..., description="Asserted value.")
  unit: str = Field(
    ..., description="Fact unit — 'USD' for monetary, 'days' for days, else 'pure'."
  )
  period_type: str = Field(..., description="'instant' or 'duration'.")
  item_type: str | None = Field(
    None,
    description=(
      "Format family from the metric element (monetary | ratio | percent "
      "| multiple | days). None means untyped; fall back to unit."
    ),
  )


class AssertMetricsRequest(BaseModel):
  """Request body for the ``assert-metrics`` operation.

  The observation sibling of ``compute-metrics``: writes externally-
  observed values (usage counts, marketing numbers, hand-carried
  figures) into the period's standing ``factset_type='metric'`` FactSet
  with ``AssertedProvenance``. Re-asserting a period replaces its facts
  — one standing FactSet per (structure, entity, period_end), the
  accumulating time series.

  Structures carrying ``Derive`` rules are compute-owned
  (``compute-metrics``) and rejected — asserted and derived metric
  series keep disjoint structures. Asserted series are actuals; there
  is no scenario axis.
  """

  structure_id: str = Field(
    ...,
    description="Metric block structure (block_type='metric') to assert into.",
  )
  period_end: date = Field(
    ...,
    description=(
      "Period end the observations are for — instant concepts (a follower "
      "count at month end) land as of this date; duration concepts (monthly "
      "downloads) end on it."
    ),
  )
  period_start: date | None = Field(
    None,
    description=(
      "Window start for duration concepts and the standing FactSet's "
      "period_start. Instant concepts ignore it."
    ),
  )
  entity_id: str | None = Field(
    None,
    description=(
      "Entity to assert for. Defaults to the graph's earliest-created "
      "entity (the primary entity for single-entity graphs)."
    ),
  )
  source_system: str = Field(
    ...,
    min_length=1,
    description=(
      "Identifier of the asserting system (e.g. 'content-machine') — "
      "recorded as the AssertedProvenance source_system."
    ),
  )
  basis_note: str | None = Field(
    None,
    description="Free-text basis / source reference for the observations.",
  )
  observations: list[MetricObservation] = Field(
    ...,
    min_length=1,
    description="Observed values, one per metric concept — duplicates rejected.",
  )

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "structure_id": "str_growth_metrics",
          "period_start": "2026-07-01",
          "period_end": "2026-07-31",
          "source_system": "content-machine",
          "observations": [
            {"qname": "rsx:GithubStars", "value": 1240},
            {"qname": "rsx:NpmDownloads", "value": 3811},
          ],
        }
      ]
    }
  )


class AssertMetricsResponse(BaseModel):
  """Response for the ``assert-metrics`` operation."""

  structure_id: str
  entity_id: str
  period_end: date
  fact_set_id: str = Field(
    ...,
    description="Standing metric FactSet the observations were written to.",
  )
  asserted: list[AssertedMetricLite] = Field(default_factory=list)
  replaced: bool = Field(
    False,
    description=(
      "True when a prior standing set existed for the period and its "
      "facts were replaced."
    ),
  )


class ForecastMonthLite(BaseModel):
  """One computed forward month in a ``compute-forecast`` response."""

  period: str = Field(..., description="Month key (``YYYY-MM``).")
  period_start: date
  period_end: date
  income_statement_fact_set_id: str | None = Field(
    None,
    description="Scenario IS FactSet upserted for the month.",
  )
  balance_sheet_fact_set_id: str | None = Field(
    None,
    description=(
      "Scenario BS FactSet upserted for the month — the full roll: "
      "carry-forward, rule-driven working capital, schedule movements, "
      "RE roll, balancing cash (A = L + E by construction)."
    ),
  )
  cash_flow_fact_set_id: str | None = Field(
    None,
    description=(
      "Scenario CF FactSet upserted for the month — indirect-method, "
      "derived from BS deltas + NI, reconciled to the balancing ΔCash."
    ),
  )
  computed_count: int = Field(
    0, description="Number of facts emitted for the month across all sets."
  )
  verification_passed: bool | None = Field(
    None,
    description=(
      "Whether every rule evaluated against the month's scenario sets "
      "passed (None = no rules ran for the month)."
    ),
  )
  verification_failures: list[str] = Field(
    default_factory=list,
    description="Failed/errored rule messages for the month (capped).",
  )


class SkippedForecastLite(BaseModel):
  """One rule/month soft-skip in a ``compute-forecast`` response.

  A skipped rule never aborts the walk — its target falls back to the
  carry-forward value for that month (when a prior value exists).
  """

  rule_id: str | None = None
  element_qname: str | None = None
  period: str = Field(..., description="Month key (``YYYY-MM``) of the skip.")
  reason: str
  missing: list[str] = Field(default_factory=list)


class ComputeForecastRequest(BaseModel):
  """Request body for the ``compute-forecast`` operation.

  Walks the scenario's driver cascade month-by-month forward from the
  forecast block's ``base_period``: lever-driven Derive rules in
  dependency order, carry-forward for unmodeled IS lines, calc-DAG
  subtotals — upserting one scenario IS FactSet (+ a working-capital BS
  set) per forward month, all keyed by the forecast block's
  ``scenario_id``. Re-running replaces each month's values (the
  compute-metrics drift semantics). Deterministic and non-AI — no
  credits consumed.
  """

  structure_id: str = Field(
    ...,
    description="Forecast block structure (block_type='forecast') to compute.",
  )
  months: int | None = Field(
    None,
    ge=1,
    description=(
      "Forward months to compute — defaults to the block's full "
      "horizon_months; must not exceed it (lever assertions don't "
      "extend past the horizon)."
    ),
  )
  entity_id: str | None = Field(
    None,
    description=(
      "Entity to compute for. Defaults to the lever FactSet's entity "
      "(the entity the scenario was authored against)."
    ),
  )

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {"structure_id": "struct_fy26_budget"},
        {"structure_id": "struct_fy26_budget", "months": 6},
      ]
    }
  )


class ComputeForecastResponse(BaseModel):
  """Response for the ``compute-forecast`` operation."""

  structure_id: str
  scenario_id: str = Field(
    ...,
    description=(
      "The scenario key every emitted FactSet carries — the forecast "
      "block's own structure id."
    ),
  )
  entity_id: str
  base_period: str = Field(..., description="Seed month the walk projected from.")
  months: int = Field(..., description="Forward months requested.")
  months_computed: list[ForecastMonthLite] = Field(default_factory=list)
  skipped: list[SkippedForecastLite] = Field(default_factory=list)
  diagnostics: list[str] = Field(
    default_factory=list,
    description=(
      "Articulation notes — a missing cash/earnings anchor, schedule "
      "contributions with no base-set landing spot, an absent "
      "cash-flow structure. Informational; the walk still computed."
    ),
  )


__all__ = [
  "ArtifactMechanics",
  "ArtifactResponse",
  "AssertMetricsRequest",
  "AssertMetricsResponse",
  "AssertedMetricLite",
  "ChartLite",
  "ChartPanelLite",
  "ChartSeriesLite",
  "ClassificationLite",
  "ComputeForecastRequest",
  "ComputeForecastResponse",
  "ComputeMetricsRequest",
  "ComputeMetricsResponse",
  "ComputedMetricLite",
  "ConnectionLite",
  "CreateInformationBlockRequest",
  "DeleteInformationBlockRequest",
  "DeleteInformationBlockResponse",
  "ElementLite",
  "EvaluateRulesRequest",
  "EvaluateRulesResponse",
  "FactLite",
  "FactSetLite",
  "ForecastMechanics",
  "ForecastMonthLite",
  "InformationBlockEnvelope",
  "InformationModelResponse",
  "LeverAssertionLite",
  "LineAssertionLite",
  "MetricMechanics",
  "MetricObservation",
  "RenderingLite",
  "RenderingPeriodLite",
  "RenderingRowLite",
  "RuleLite",
  "RuleTargetLite",
  "RuleVariableLite",
  "ScheduleMechanics",
  "SkippedForecastLite",
  "SkippedMetricLite",
  "StatementMechanics",
  "UpdateInformationBlockRequest",
  "ValidationLite",
  "VerificationCategorySummary",
  "VerificationResultLite",
  "VerificationSummary",
  "ViewProjections",
]
