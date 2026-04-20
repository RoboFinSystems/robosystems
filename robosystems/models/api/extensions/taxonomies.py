"""Taxonomy, structure, and mapping response models."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from robosystems.models.api.common import PaginationInfo

# ── Taxonomy ──────────────────────────────────────────────────────────────


class TaxonomyResponse(BaseModel):
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
  taxonomies: list[TaxonomyResponse]


class CreateTaxonomyRequest(BaseModel):
  name: str
  description: str | None = None
  taxonomy_type: Literal["chart_of_accounts", "reporting", "mapping", "schedule"]
  version: str | None = None
  source_taxonomy_id: str | None = None
  target_taxonomy_id: str | None = None


# ── Structure ─────────────────────────────────────────────────────────────


class StructureResponse(BaseModel):
  id: str
  name: str
  description: str | None = None
  structure_type: str
  taxonomy_id: str
  is_active: bool


class StructureListResponse(BaseModel):
  structures: list[StructureResponse]


class CreateStructureRequest(BaseModel):
  name: str
  description: str | None = None
  # NOTE: `cash_flow_statement` is intentionally omitted from this Literal
  # even though the DB CHECK constraint still allows it. The roboledger CF
  # renderer isn't implemented yet, so API requests to *create* a CF
  # structure are rejected at the Pydantic layer. Any pre-existing rows
  # with structure_type='cash_flow_statement' would only round-trip
  # through this model if a write path validates it — the read path uses
  # `str`-typed response models and is unaffected. Confirmed no rows in
  # local/demo as of this commit; staging should be spot-checked before
  # rollout. SEC XBRL cash-flow parsing is a separate pipeline and is
  # unaffected. When the renderer lands, add `cash_flow_statement` back.
  structure_type: Literal[
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
  from_element_id: str
  to_element_id: str
  association_type: Literal["presentation", "calculation", "mapping"] = "mapping"
  order_value: float | None = None
  weight: float | None = None
  confidence: float | None = None
  suggested_by: str | None = None


class CreateMappingAssociationOperation(CreateAssociationRequest):
  """CQRS-shaped body for `POST /operations/create-mapping-association`.

  Bundles the target mapping structure's `mapping_id` with the association
  payload so REST + MCP share a single body type via the registrar.
  """

  mapping_id: str = Field(..., description="Target mapping structure ID.")


# ── Mapping ───────────────────────────────────────────────────────────────


class MappingDetailResponse(BaseModel):
  """A mapping structure with all its associations."""

  id: str
  name: str
  structure_type: str
  taxonomy_id: str
  associations: list[AssociationResponse]
  total_associations: int


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


# ── Element (extended for taxonomy context) ───────────────────────────────


class ElementResponse(BaseModel):
  """Element with taxonomy context — extends AccountResponse."""

  id: str
  code: str | None = None
  name: str
  description: str | None = None
  qname: str | None = None
  namespace: str | None = None
  classification: str | None = None
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
  classification: str | None = None
  balance_type: str
  external_source: str | None = None
  suggested_targets: list[SuggestedTarget] = Field(default_factory=list)


# ── Mapped Trial Balance ──────────────────────────────────────────────────


class MappedTrialBalanceRow(BaseModel):
  """One reporting-concept row in the mapped trial balance."""

  reporting_element_id: str
  qname: str
  reporting_name: str
  classification: str | None = None
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
  For chart_of_accounts taxonomies, this tells the platform which CoA the
  entity uses. For reporting taxonomies, which standard (us-gaap, ifrs).
  """

  taxonomy_id: str
  basis: Literal["reporting", "chart_of_accounts", "mapping", "schedule"] = (
    "chart_of_accounts"
  )
  is_primary: bool = True
  adoption_context: str | None = "voluntary"


class EntityTaxonomyResponse(BaseModel):
  entity_id: str
  taxonomy_id: str
  basis: str
  is_primary: bool
  adoption_context: str | None = None


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
  """Update mutable fields on a structure. `structure_type` and
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
  classification: Literal[
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
  (make root)."""

  element_id: str
  code: str | None = None
  name: str | None = None
  description: str | None = None
  balance_type: Literal["debit", "credit"] | None = None
  period_type: Literal["duration", "instant"] | None = None
  parent_id: str | None = None
  currency: str | None = None


class DeleteElementRequest(BaseModel):
  """Soft delete — sets `is_active=false`. Historical line items
  referencing this element remain valid; new line items cannot use an
  inactive element."""

  element_id: str


# ── Association bulk create / update / delete ────────────────────────────


class BulkAssociationItem(BaseModel):
  """A single association within a bulk-create payload. The parent
  `structure_id` is set once on the request envelope, not repeated
  per item."""

  from_element_id: str
  to_element_id: str
  association_type: Literal["presentation", "calculation", "mapping"] = "presentation"
  arcrole: str | None = None
  order_value: float | None = 0.0
  weight: float | None = None
  confidence: float | None = None
  suggested_by: str | None = None


class BulkCreateAssociationsRequest(BaseModel):
  """Bulk create associations within a single structure. Atomic — any
  failed row rolls back the whole batch. Handles 50+ presentation arcs,
  25+ calculation arcs, or a full table linkbase in one call."""

  structure_id: str
  associations: list[BulkAssociationItem] = Field(..., min_length=1, max_length=5000)


class BulkCreateAssociationsResponse(BaseModel):
  """Result of a bulk association create. `association_ids` is in the
  same order as the input `associations` list."""

  structure_id: str
  created: int
  association_ids: list[str]


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
