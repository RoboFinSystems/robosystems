"""API response models for the Information Block envelope.

Wire-facing types for the cross-domain Information Block construct
(see ``local/docs/specs/information-block.md``). Used by the REST
``create-information-block`` operation, the GraphQL ``informationBlock``/
``informationBlocks`` fields, and the MCP read tools.

Adding a block type: register its ``*Mechanics`` model and add it to
the ``ArtifactMechanics`` discriminated union here. The envelope shape
stays invariant — a union-arm edit, not an envelope redesign.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Discriminator, Field, RootModel

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
      "'aggregation', 'AssetsRollUp'."
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
      "'us-gaap-metamodel', adapter name, etc."
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
  value: float
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
      "'report' | 'schedule' | 'custom'. Enum closure enforced by the "
      "``public.fact_sets`` CHECK constraint."
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
  variable_qname: str = Field(
    ..., description="Concept qname the variable resolves to, e.g. 'fac:Assets'."
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
  rule_pattern: str = Field(
    ...,
    description=(
      "One of 10 cm:BusinessRulePattern mechanisms — Adjustment, "
      "CoExists, EqualTo, Exists, GreaterThan, "
      "GreaterThanOrEqualToZero, LessThan, RollForward, RollUp, "
      "Variance."
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
  ScheduleMechanics | StatementMechanics | MetricMechanics,
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
    None, description="aggregation | nonaggregation, or null if non-hypercube."
  )


class ArtifactResponse(BaseModel):
  """The block's producible-artifact envelope — topic, template, mechanics."""

  topic: str | None = Field(
    None, description="Structure.description — the block's human-readable topic."
  )
  parenthetical_note: str | None = Field(
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
  values: list[float | None] = Field(default_factory=list)
  is_subtotal: bool = False
  depth: int = 0


class RenderingPeriodLite(BaseModel):
  """One period column in a rendered statement."""

  model_config = ConfigDict(from_attributes=True)

  start: date
  end: date
  label: str | None = None


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


class ViewProjections(BaseModel):
  """Charlie's six ``type-of View`` arms, surfaced at the envelope boundary.

  Each projection is computed server-side at envelope-build time when
  its source data is available. The frontend's ``BlockView`` dispatcher
  routes to the projection component matching the user's selected view
  mode; missing projections (those still in backlog) render as empty
  states without breaking the dispatcher.

  Today: ``rendering`` is computed for the statement family.
  Other arms (``fact_table``, ``model_structure``, ``verification_results``,
  ``report_elements``, ``business_rules``) come online as their backend
  support lands; ``fact_table`` is trivially derivable from
  ``InformationBlockEnvelope.facts`` and may stay as a frontend-only
  projection.
  """

  model_config = ConfigDict(from_attributes=True)

  rendering: RenderingLite | None = None


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
          "summary": "5-year straight-line depreciation",
          "description": (
            "Monthly depreciation schedule for a $50,000 asset "
            "amortized over 60 months. Each in-scope period produces "
            "a draft closing entry via `entry_template`."
          ),
          "value": {
            "block_type": "schedule",
            "payload": {
              "name": "Office Building Depreciation",
              "element_ids": [
                "us-gaap:Depreciation",
                "us-gaap:AccumulatedDepreciation",
              ],
              "period_start": "2026-01-01",
              "period_end": "2030-12-31",
              "monthly_amount": 83333,
              "entry_template": {
                "debit_element_id": "us-gaap:Depreciation",
                "credit_element_id": "us-gaap:AccumulatedDepreciation",
              },
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
  "metric",
]


class _CreateLegacyArm(BaseModel):
  """Create-information-block body for block types that don't yet have
  a typed construction path at the API boundary.

  Statement-family blocks (balance_sheet, income_statement,
  cash_flow_statement, equity_statement) are constructed via
  `create-report`, not this endpoint. Metric blocks are recognized
  but their evaluator has not shipped. Calling this endpoint with one
  of these block types returns HTTP 501 with a hint pointing to the
  correct construction path.
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


_CreateInformationBlockArms = Annotated[
  _CreateScheduleArm | _CreateLegacyArm,
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
  def payload(self) -> CreateScheduleRequest | dict[str, Any]:
    """Payload of the resolved arm. Typed for the schedule arm; a raw
    dict for the legacy arms (the dispatch handler validates it at
    runtime against the registry entry's request model)."""
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
          "summary": "Rename a schedule",
          "value": {
            "block_type": "schedule",
            "payload": {
              "structure_id": "struct_depr_office_2026",
              "name": "Office Building Depreciation (Renamed)",
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


_UpdateInformationBlockArms = Annotated[
  _UpdateScheduleArm | _UpdateLegacyArm,
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
  def payload(self) -> UpdateScheduleRequest | dict[str, Any]:
    return self.root.payload


class _DeleteScheduleArm(BaseModel):
  """Delete-information-block body for `block_type="schedule"`.

  Carries a typed schedule delete payload — just the `structure_id`.
  """

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "summary": "Hard-delete a schedule",
          "description": (
            "Cascades through facts and associations. To end a "
            "schedule early without removing history, fire "
            "`create-event-block(event_type='asset_disposed')` instead."
          ),
          "value": {
            "block_type": "schedule",
            "payload": {"structure_id": "struct_depr_office_2026"},
          },
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


_DeleteInformationBlockArms = Annotated[
  _DeleteScheduleArm | _DeleteLegacyArm,
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
  def payload(self) -> DeleteScheduleRequest | dict[str, Any]:
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

  structure_id: str
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


__all__ = [
  "ArtifactMechanics",
  "ArtifactResponse",
  "ClassificationLite",
  "ConnectionLite",
  "CreateInformationBlockRequest",
  "DeleteInformationBlockRequest",
  "DeleteInformationBlockResponse",
  "ElementLite",
  "EvaluateRulesRequest",
  "EvaluateRulesResponse",
  "FactLite",
  "FactSetLite",
  "InformationBlockEnvelope",
  "InformationModelResponse",
  "MetricMechanics",
  "RenderingLite",
  "RenderingPeriodLite",
  "RenderingRowLite",
  "RuleLite",
  "RuleTargetLite",
  "RuleVariableLite",
  "ScheduleMechanics",
  "StatementMechanics",
  "UpdateInformationBlockRequest",
  "ValidationLite",
  "VerificationResultLite",
  "ViewProjections",
]
