"""API response models for the Information Block envelope.

Wire-facing types for the cross-domain Information Block construct
(see ``local/docs/specs/information-block.md``). Used by the REST
``create-information-block`` operation, the GraphQL ``informationBlock``/
``informationBlocks`` fields, and the MCP read tools.

Phase a ships only the ``schedule`` block type; new block types register
their own ``*Mechanics`` model and add it to the ``ArtifactMechanics``
discriminated union here. The envelope shape itself stays invariant —
adding a block type is a union-arm edit, not an envelope redesign.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from robosystems.models.api.extensions.schedules import (
  EntryTemplateRequest,
  ScheduleMetadataRequest,
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
  or `element_classifications` junction entry.

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
      "One of the categories in the `public.classifications` CHECK "
      "constraint — e.g. 'concept_arrangement', 'member_arrangement', "
      "'named_disclosure' for association-level; 'liquidity', "
      "'activityType', etc. for element-level."
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
  The underlying storage is still ``associations``; Phase g keeps that
  table name stable.
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
      "Association-level classifications (Phase epsilon) — "
      "concept_arrangement, member_arrangement, named_disclosure rows "
      "from the junction. Empty for library-seeded associations that "
      "haven't been classified yet."
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

  Phase ζ data-model landing. The envelope carries one ``FactSetLite``
  per block when a FactSet row exists for the requested period; older
  writes (pre-Phase-ζ ``create_report`` / ``create_schedule`` paths) may
  still leave ``fact_set`` null until the expand pass stamps FactSet
  rows on every write.
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
      "'association'. Enum closure enforced by the ``public.rules`` "
      "CHECK constraint."
    ),
  )
  target_ref_id: str = Field(
    ...,
    description=(
      "UUID of the target atom — structure_id, element_id, or "
      "association_id depending on ``target_kind``."
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
  """Persisted outcome of one Rule evaluation (Phase iota data model).

  One row per ``public.verification_results`` entry the engine (Phase
  δ.3) writes. The envelope surfaces them so the block viewer's
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

  One row per ``public.rules`` entry scoped to this block. The engine
  (Phase δ.3) consumes ``rule_expression`` + ``rule_variables`` to
  evaluate against the in-scope fact set; until then the envelope just
  surfaces the rules so the UI can render them as a checklist.
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

  Phase δ: reads directly from the typed ``structures.artifact_mechanics``
  JSONB column. ``entry_template`` and ``schedule_metadata`` are typed
  sub-models (reusing the wire-level request shapes so OpenAPI emits one
  canonical type per concept); the envelope builder falls back to
  ``structures.metadata_`` for pre-δ Schedule rows that the tenant
  backfill hasn't yet migrated.
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
      "posted. Runtime state — Phase ζ migrates this to the typed FactSet "
      "envelope where it becomes derivable from fact_sets."
    ),
  )


class MetricMechanics(BaseModel):
  """Derivative mechanics for ``block_type='metric'`` (Phase η data model).

  A metric block composes its facts from one or more source blocks at
  read time — covenant tests, ratios, KPI trend computations. Phase η
  ships the typed arm so the discriminated union covers all three
  construction modes (declarative / compositional / derivative); the
  actual derivation evaluator ships in a follow-up.

  ``source_block_ids`` is the ordered list of Structure ids this metric
  derives from; ``derivation_type`` names the kind of computation
  (``ratio``, ``trailing_twelve_month``, ``covenant_test``, …), and
  ``expression`` carries the agent-authored derivation string evaluated
  at envelope build time.
  """

  kind: Literal["metric"] = "metric"
  source_block_ids: list[str] = Field(
    default_factory=list,
    description=(
      "Ordered list of Structure ids this metric sources from. Must be "
      "non-empty at evaluation time; the blitz landing allows empty "
      "lists so library scaffolding can register metric templates "
      "before source linkage is wired."
    ),
  )
  derivation_type: str | None = Field(
    None,
    description=(
      "Free-form label for the derivation kind — 'ratio', "
      "'trailing_twelve_month', 'covenant_test', etc. The evaluator "
      "dispatches on this tag; future phases may lock the set with a "
      "CHECK constraint once the derivation catalog stabilizes."
    ),
  )
  expression: str | None = Field(
    None,
    description=(
      "Derivation expression in the metric DSL — evaluated at envelope "
      "read time to produce the derivative fact value. Opaque string "
      "during the blitz; the rule-engine work (Phase δ.3) lands the "
      "parser / evaluator in a follow-up."
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
  and ``equity_statement``. Phase δ adds typed mechanics fields alongside
  the existing Phase β tagged body; the fields are all optional so
  library-seeded rows that haven't been enriched yet still validate.
  The existing ``statement(...)`` GraphQL field continues to serve
  rendered output; this mechanics model is the source of truth for
  future renderer configuration.
  """

  kind: Literal["statement_renderer"] = "statement_renderer"
  template_id: str | None = Field(
    None,
    description=(
      "Pinned template id — when set, the renderer uses that template's "
      "layout instead of the block's default. The templates table lands "
      "in a later phase; the column lands now so tenant writes can stamp "
      "it without another migration round-trip."
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
    None, description="e.g. 'in thousands', 'except per share'. Phase d."
  )
  template: dict[str, Any] | None = Field(
    None,
    description=(
      "Reusable layout (ordering, subtotals, styling) when attached. "
      "Phase i delivers first-class templates; null in Phase a."
    ),
  )
  mechanics: ArtifactMechanics


# ── Envelope root ──────────────────────────────────────────────────────────


class InformationBlockEnvelope(BaseModel):
  """The Information Block exchange format.

  One envelope per block instance. Carries the block's identity + type,
  Information-Model attributes, the Artifact branch (mechanics +
  topic/template), and bundled atoms (elements, connections, facts).
  Rules / dimensions / FactSet / verificationResults are present-but-
  empty until the corresponding phases land.
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
      "The period-specific FactSet this envelope instantiates (Phase ζ). "
      "Null when the underlying block has no FactSet row yet — typically "
      "library-seeded statement Structures with no tenant-generated "
      "facts, or Schedule rows written before the create-side stamping "
      "(expand pass) lands."
    ),
  )
  verification_results: list[VerificationResultLite] = Field(default_factory=list)


# ── Request models ─────────────────────────────────────────────────────────


class CreateInformationBlockRequest(BaseModel):
  """Generic create request — discriminator + typed-at-dispatch payload.

  ``block_type`` selects the registry entry. ``payload`` is validated
  against ``BlockTypeRegistryEntry.create_request_model`` (e.g.
  :class:`CreateScheduleRequest` for ``block_type='schedule'``) by the
  command dispatcher. Chosen over a Pydantic discriminated union on the
  top-level request so adding a block type is one registry line, not a
  union-arm edit at the request-model layer.
  """

  block_type: str = Field(
    ...,
    description=(
      "Block type discriminator. Must match a registered entry in "
      "robosystems.operations.information_block.registry.REGISTRY."
    ),
  )
  payload: dict[str, Any] = Field(
    default_factory=dict,
    description=(
      "Block-type-specific creation payload. Shape-validated against the "
      "registry entry's `create_request_model` at dispatch time; the "
      "validation error surfaces as a 422 at the API boundary."
    ),
  )


class UpdateInformationBlockRequest(BaseModel):
  """Generic update request — mirrors :class:`CreateInformationBlockRequest`.

  Validated against the registry entry's ``update_request_model``.
  Block types that don't support updates (e.g. the statement family,
  whose Structures are library-seeded) surface ``NotImplementedError``
  from their dispatch handler, which the registrar maps to HTTP 501.
  """

  block_type: str = Field(
    ...,
    description="Block type discriminator. Must match a registered entry.",
  )
  payload: dict[str, Any] = Field(
    default_factory=dict,
    description=(
      "Block-type-specific update payload. Typically carries the "
      "structure_id plus whichever fields are editable for this block "
      "type. Shape-validated against the registry entry's "
      "`update_request_model` at dispatch time."
    ),
  )


class DeleteInformationBlockRequest(BaseModel):
  """Generic delete request — mirrors :class:`CreateInformationBlockRequest`.

  Validated against the registry entry's ``delete_request_model``.
  Block types that don't support deletion raise ``NotImplementedError``.
  """

  block_type: str = Field(
    ...,
    description="Block type discriminator. Must match a registered entry.",
  )
  payload: dict[str, Any] = Field(
    default_factory=dict,
    description=(
      "Block-type-specific delete payload. Typically carries just the "
      "structure_id. Shape-validated against the registry entry's "
      "`delete_request_model` at dispatch time."
    ),
  )


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
  """Request body for the ``evaluate-rules`` operation (Phase delta.3).

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
      "Allows results to be scoped to a specific period run when the "
      "FactSet table is populated (Phase zeta expand pass)."
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
  "RuleLite",
  "RuleTargetLite",
  "RuleVariableLite",
  "ScheduleMechanics",
  "StatementMechanics",
  "UpdateInformationBlockRequest",
  "VerificationResultLite",
]
