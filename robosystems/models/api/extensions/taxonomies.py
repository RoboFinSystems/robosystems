"""Taxonomy, structure, and mapping response models."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from robosystems.models.api.common import PaginationInfo

# ── Taxonomy ──────────────────────────────────────────────────────────────


class TaxonomyResponse(BaseModel):
  """One taxonomy header — identity + lifecycle flags. Atoms
  (elements, structures, associations, rules) are exposed via the
  Taxonomy Block envelope.

  ``taxonomy_type`` discriminates: ``chart_of_accounts``,
  ``reporting_standard``, ``reporting_extension``, ``custom_ontology``,
  ``mapping``, ``schedule``. ``is_locked=True`` means library-origin
  (immutable for tenants); ``is_shared=True`` means visible to multiple
  graphs from a shared registry.
  """

  id: str
  name: str
  description: str | None = None
  taxonomy_type: str
  version: str | None = None
  standard: str | None = None
  namespace_uri: str | None = None
  is_shared: bool
  is_active: bool
  is_locked: bool
  source_taxonomy_id: str | None = None
  target_taxonomy_id: str | None = None


class TaxonomyListResponse(BaseModel):
  """Flat list of taxonomy headers. Used by the catalog/picker UIs."""

  taxonomies: list[TaxonomyResponse]


class CreateTaxonomyRequest(BaseModel):
  name: str
  description: str | None = None
  taxonomy_type: Literal[
    "chart_of_accounts",
    "reporting_standard",
    "reporting_extension",
    "custom_ontology",
    "mapping",
    "schedule",
  ]
  version: str | None = None
  source_taxonomy_id: str | None = None
  target_taxonomy_id: str | None = None


# ── Structure ─────────────────────────────────────────────────────────────


class StructureResponse(BaseModel):
  """One structure header — a renderable section within a taxonomy
  (balance sheet, income statement, schedule, etc.).

  ``block_type`` drives presentation: 'balance_sheet',
  'income_statement', 'cash_flow_statement', 'equity_statement',
  'schedule', 'chart_of_accounts', 'coa_mapping', 'rollforward', etc.
  """

  id: str
  name: str
  description: str | None = None
  block_type: str
  taxonomy_id: str
  is_active: bool


class StructureListResponse(BaseModel):
  """Flat list of structures within a taxonomy."""

  structures: list[StructureResponse]


class CreateStructureRequest(BaseModel):
  name: str
  description: str | None = None
  # NOTE: `cash_flow_statement` and `comprehensive_income` are intentionally
  # omitted from this Literal even though the DB CHECK constraint allows them.
  # The roboledger CF and CI renderers aren't implemented yet, so API
  # requests to *create* those structures are rejected at the Pydantic
  # layer. Any pre-existing rows with those block_types would only
  # round-trip through this model if a write path validates it — the read
  # path uses `str`-typed response models and is unaffected. Confirmed no
  # rows in local/demo as of this commit; staging should be spot-checked
  # before rollout. SEC XBRL cash-flow parsing is a separate pipeline and
  # is unaffected. When the renderers land, add both back here.
  block_type: Literal[
    "chart_of_accounts",
    "income_statement",
    "balance_sheet",
    "equity_statement",
    "coa_mapping",
    "schedule",
    "custom",
  ]
  taxonomy_id: str


# ── Association ───────────────────────────────────────────────────────────


class AssociationResponse(BaseModel):
  """One edge between two elements within a structure (parent/child
  presentation, calculation rollup, mapping, equivalence).

  ``association_type`` discriminates the edge semantics. Mapping edges
  are the user-facing path (CoA → reporting concept); presentation /
  calculation edges express structure layout and roll-ups.
  ``confidence`` is set on AI-suggested mappings (≥0.90 auto-approved,
  0.70-0.89 flagged for review).
  """

  id: str
  structure_id: str
  from_element_id: str
  from_element_name: str | None = None
  from_element_qname: str | None = None
  to_element_id: str
  to_element_name: str | None = None
  to_element_qname: str | None = None
  association_type: str
  order_value: float | None = None
  weight: float | None = None
  confidence: float | None = None
  suggested_by: str | None = None
  approved_by: str | None = None


class CreateAssociationRequest(BaseModel):
  """Create an edge between two elements within a structure.

  Used by all structure types — presentation, calculation, mapping,
  equivalence. The mapping operation (`create-mapping-association`)
  is the user-facing path; raw associations on other structure types
  are not exposed publicly.
  """

  from_element_id: str = Field(
    ..., description="Source element (typically a CoA element)."
  )
  to_element_id: str = Field(
    ...,
    description="Target element (typically a US GAAP reporting concept).",
  )
  association_type: Literal["presentation", "calculation", "mapping", "equivalence"] = (
    Field(
      "mapping",
      description=(
        "Edge semantics. `mapping` rolls up CoA elements into reporting "
        "concepts; `presentation` orders concepts in a structure; "
        "`calculation` carries debit/credit weights; `equivalence` is a "
        "synonym link."
      ),
    )
  )
  order_value: float | None = Field(
    None, description="Display order within the structure (lower = earlier)."
  )
  weight: float | None = Field(
    None,
    description=(
      "Calculation weight (typically +1.0 or -1.0). Used by calculation "
      "linkbases to express signed roll-ups."
    ),
  )
  confidence: float | None = Field(
    None,
    ge=0.0,
    le=1.0,
    description=(
      "Confidence score (0–1). For AI-suggested mappings: ≥0.90 "
      "auto-approved, 0.70–0.89 flagged for review, <0.70 skipped."
    ),
  )
  suggested_by: str | None = Field(
    None,
    description=(
      "Source of the suggestion (e.g., 'mapping_agent', 'user'). "
      "Captured for audit; not used by the runtime."
    ),
  )


class CreateMappingAssociationOperation(CreateAssociationRequest):
  """Create one CoA → reporting-concept mapping edge.

  This is the iterative, AI-assisted craft path. Each call adds a single
  association to the target mapping structure. Use `auto-map-elements`
  to create many at once via the MappingOperator. Reject duplicates: if
  the (from, to, type) tuple already exists, the call returns 409.
  """

  mapping_id: str = Field(..., description="Target mapping structure ID.")

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "mapping_id": "map_01HVF8T0M2YTAY3BBNRH0V0",
          "from_element_id": "elem_coa_revenue_product",
          "to_element_id": "elem_usgaap_Revenues",
          "association_type": "mapping",
          "weight": 1.0,
          "confidence": 0.95,
          "suggested_by": "user",
        },
        {
          "mapping_id": "map_01HVF8T0M2YTAY3BBNRH0V0",
          "from_element_id": "elem_coa_returns_allowances",
          "to_element_id": "elem_usgaap_Revenues",
          "association_type": "mapping",
          "weight": -1.0,
          "confidence": 0.92,
          "suggested_by": "mapping_agent",
        },
      ]
    },
  )


class DeleteMappingAssociationOperation(BaseModel):
  """Delete a single CoA → reporting-concept mapping edge."""

  mapping_id: str = Field(
    ..., description="The mapping structure containing the association."
  )
  association_id: str = Field(..., description="The association edge to delete.")

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "mapping_id": "map_01HVF8T0M2YTAY3BBNRH0V0",
          "association_id": "assoc_01HVF8T0M2YTAY3BBNRH0V0",
        }
      ]
    }
  )


# ── Mapping ───────────────────────────────────────────────────────────────


class MappingDetailResponse(BaseModel):
  """A mapping structure with all its associations."""

  id: str
  name: str
  block_type: str
  taxonomy_id: str
  associations: list[AssociationResponse]
  total_associations: int


class UnreachableMapping(BaseModel):
  """A CoA→rs-gaap mapping whose target doesn't reach a Network root.

  The reporting layer is closed: every mapping target must trace upward
  through the rs-gaap calc DAG to one of the canonical roots
  (rs-gaap:Assets, rs-gaap:LiabilitiesAndStockholdersEquity,
  rs-gaap:NetIncomeLoss, rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease).
  When it doesn't, the fact will land on a dead branch — visible in the
  trial balance but invisible to any rendered statement. Surfacing
  these as defects lets operators fix the mapping before the report is
  filed.
  """

  coa_element_id: str
  coa_qname: str | None = None
  coa_code: str | None = None
  coa_name: str | None = None
  target_element_id: str
  target_qname: str | None = None
  target_name: str | None = None


class MappingCoverageResponse(BaseModel):
  """Coverage stats for a mapping."""

  mapping_id: str
  total_coa_elements: int
  mapped_count: int
  unmapped_count: int
  coverage_percent: float
  high_confidence: int = 0  # >0.90
  medium_confidence: int = 0  # 0.70-0.90
  low_confidence: int = 0  # <0.70
  unreachable_count: int = 0
  unreachable: list[UnreachableMapping] = []


# ── Element (extended for taxonomy context) ───────────────────────────────


class ElementResponse(BaseModel):
  """Element with taxonomy context — extends AccountResponse."""

  id: str
  code: str | None = None
  name: str
  description: str | None = None
  qname: str | None = None
  namespace: str | None = None
  trait: str | None = None
  sub_classification: str | None = None
  balance_type: str
  period_type: str
  is_abstract: bool
  element_type: str
  source: str
  taxonomy_id: str | None = None
  parent_id: str | None = None
  depth: int
  is_active: bool
  external_id: str | None = None
  external_source: str | None = None


class ElementListResponse(BaseModel):
  """Paginated element listing with taxonomy context."""

  elements: list[ElementResponse]
  pagination: PaginationInfo


class SuggestedTarget(BaseModel):
  """A suggested mapping target from the reporting taxonomy."""

  element_id: str
  qname: str
  name: str
  confidence: float | None = None


class UnmappedElementResponse(BaseModel):
  """An element not yet mapped to the reporting taxonomy."""

  id: str
  code: str | None = None
  name: str
  trait: str | None = None
  liquidity: str | None = None
  balance_type: str
  external_source: str | None = None
  suggested_targets: list[SuggestedTarget] = Field(default_factory=list)


# ── Mapped Trial Balance ──────────────────────────────────────────────────


class MappedTrialBalanceRow(BaseModel):
  """One reporting-concept row in the mapped trial balance."""

  reporting_element_id: str
  qname: str
  reporting_name: str
  trait: str | None = None
  balance_type: str | None = None
  total_debits: float
  total_credits: float
  net_balance: float


class MappedTrialBalanceResponse(BaseModel):
  """Trial balance rolled up to reporting concepts via mapping associations."""

  mapping_id: str
  rows: list[MappedTrialBalanceRow] = Field(default_factory=list)


# ── Entity ↔ Taxonomy linkage ─────────────────────────────────────────────


class LinkEntityTaxonomyRequest(BaseModel):
  """Link an entity to a taxonomy (creates the ENTITY_HAS_TAXONOMY edge).

  This is how a graph declares "this entity reports under this taxonomy."
  For ``chart_of_accounts`` taxonomies, this tells the platform which CoA
  the entity uses. For reporting taxonomies, which standard (us-gaap,
  ifrs). Idempotent — re-linking returns the existing edge unchanged.

  CoA blocks auto-link at create time; use this to switch the primary
  CoA, link a reporting extension, or attach a custom ontology
  explicitly.
  """

  taxonomy_id: str = Field(..., description="The taxonomy to link to.")
  basis: Literal["reporting", "chart_of_accounts", "mapping", "schedule"] = Field(
    "chart_of_accounts",
    description=(
      "Linkage role: `chart_of_accounts` (the entity's CoA), `reporting` "
      "(reporting standard like us-gaap), `mapping` (CoA→reporting "
      "rollup), `schedule` (schedule structure)."
    ),
  )
  is_primary: bool = Field(
    True,
    description=(
      "Mark this as the primary linkage for the basis. False allows "
      "secondary attachments (e.g. parallel reporting standards)."
    ),
  )
  adoption_context: str | None = Field(
    "voluntary",
    description=(
      "Why the entity is reporting under this taxonomy "
      "(e.g. 'voluntary', 'regulatory', 'lender_requirement')."
    ),
  )

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {"taxonomy_id": "tax_acme_coa", "basis": "chart_of_accounts"},
        {
          "taxonomy_id": "tax_usgaap_reporting",
          "basis": "reporting",
          "adoption_context": "lender_requirement",
        },
      ]
    }
  )


class EntityTaxonomyResponse(BaseModel):
  """Result of `link-entity-taxonomy` — the ENTITY_HAS_TAXONOMY edge."""

  entity_id: str = Field(..., description="The entity that was linked.")
  taxonomy_id: str = Field(..., description="The taxonomy that was linked.")
  basis: str = Field(..., description="Linkage role (see request).")
  is_primary: bool = Field(..., description="Whether this is the primary linkage.")
  adoption_context: str | None = Field(
    None, description="Adoption context recorded at link time."
  )


# ── Taxonomy update / delete ──────────────────────────────────────────────


class UpdateTaxonomyRequest(BaseModel):
  """Update mutable fields on a taxonomy. `taxonomy_type` is immutable —
  changing it is not the same operation as editing a taxonomy; deactivate
  and create a new one instead. Only provided (non-null) fields are
  applied."""

  taxonomy_id: str
  name: str | None = None
  description: str | None = None
  version: str | None = None


class DeleteTaxonomyRequest(BaseModel):
  """Soft delete — sets `is_active=false`. Historical references remain
  valid."""

  taxonomy_id: str


# ── Structure update / delete ─────────────────────────────────────────────


class UpdateStructureRequest(BaseModel):
  """Update mutable fields on a structure. `block_type` and
  `taxonomy_id` are immutable."""

  structure_id: str
  name: str | None = None
  description: str | None = None


class DeleteStructureRequest(BaseModel):
  """Soft delete — sets `is_active=false`."""

  structure_id: str


# ── Element create / update / delete ──────────────────────────────────────


class CreateElementRequest(BaseModel):
  """Create a new Element within a taxonomy. For chart-of-accounts
  taxonomies this is how native accounts are added."""

  taxonomy_id: str
  code: str | None = None
  name: str
  description: str | None = None
  trait: Literal[
    "asset",
    "contraAsset",
    "liability",
    "contraLiability",
    "equity",
    "contraEquity",
    "temporaryEquity",
    "revenue",
    "expense",
    "expenseReversal",
    "gain",
    "loss",
    "comprehensiveIncome",
    "investmentByOwners",
    "distributionToOwners",
  ]
  balance_type: Literal["debit", "credit"] = "debit"
  period_type: Literal["duration", "instant"] = "duration"
  liquidity: Literal["current", "noncurrent"] | None = None
  """Optional liquidity classification (assets/liabilities only). Narrows
  rs-gaap mapping candidates within the EFS bucket; ignored / harmless for
  equity/revenue/expense accounts."""
  element_type: Literal["concept", "abstract", "axis", "member", "hypercube"] = (
    "concept"
  )
  is_abstract: bool = False
  is_monetary: bool = True
  parent_id: str | None = None
  source: Literal[
    "native",
    "fac",
    "rs-gaap",
    "us-gaap",
    "ifrs",
    "quickbooks",
    "xero",
    "plaid",
    "import",
  ] = "native"
  currency: str = "USD"
  qname: str | None = None
  namespace: str | None = None
  external_id: str | None = None
  external_source: str | None = None


class UpdateElementRequest(BaseModel):
  """Update mutable fields on an element. `taxonomy_id` and `source` are
  immutable. `parent_id` honors `model_dump(exclude_unset=True)` semantics:
  omit the field to leave unchanged, pass `null` to clear the parent
  (make root).

  ``trait`` updates the element's primary FASB elementsOfFinancialStatements
  trait assignment (in ``element_traits``, not a direct column on
  ``elements``). Omit to leave unchanged. Passing a value replaces the
  current primary EFS trait; there is no set-to-null semantics (use the
  UI/admin path for full trait teardown — here we only support correction
  of a misclassified account)."""

  element_id: str
  code: str | None = None
  name: str | None = None
  description: str | None = None
  balance_type: Literal["debit", "credit"] | None = None
  period_type: Literal["duration", "instant"] | None = None
  parent_id: str | None = None
  currency: str | None = None
  trait: (
    Literal[
      "asset",
      "contraAsset",
      "liability",
      "contraLiability",
      "equity",
      "contraEquity",
      "temporaryEquity",
      "revenue",
      "expense",
      "expenseReversal",
      "gain",
      "loss",
      "comprehensiveIncome",
      "investmentByOwners",
      "distributionToOwners",
    ]
    | None
  ) = None


class DeleteElementRequest(BaseModel):
  """Soft delete — sets `is_active=false`. Historical line items
  referencing this element remain valid; new line items cannot use an
  inactive element."""

  element_id: str


class UpdateAssociationRequest(BaseModel):
  """Update mutable fields on an association. `from_element_id`,
  `to_element_id`, and `association_type` are immutable — delete and
  recreate instead."""

  association_id: str
  order_value: float | None = None
  weight: float | None = None
  confidence: float | None = None
  approved_by: str | None = None


class DeleteAssociationRequest(BaseModel):
  """Hard delete — associations are edges and are cheap to recreate."""

  association_id: str
