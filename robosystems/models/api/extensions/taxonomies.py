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
  classification: str
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
  classification: str
  balance_type: str
  external_source: str | None = None
  suggested_targets: list[SuggestedTarget] = Field(default_factory=list)
