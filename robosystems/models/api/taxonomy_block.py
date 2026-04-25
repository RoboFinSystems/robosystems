"""API models for the Taxonomy Block envelope.

Wire-facing types for the cross-domain Taxonomy Block construct
(see ``local/docs/specs/taxonomy-block.md``). Taxonomy Blocks curate
ontology atomically — elements + structures + associations + rules in
one transactional envelope — mirroring the Information Block pattern
that curates business-logic artifacts.

Four block types:
  * ``reporting_standard`` — library-origin, read-only
  * ``reporting_extension`` — tenant, extends a library ``reporting_standard``
  * ``chart_of_accounts`` — tenant, CoA curation
  * ``custom_ontology`` — tenant, declarative
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

# ── Request models ─────────────────────────────────────────────────────────


class TaxonomyBlockElementRequest(BaseModel):
  """Element definition inside a Taxonomy Block envelope.

  ``qname`` is the envelope-local identifier — must be unique within the
  envelope's ``elements`` list and is used by association / rule / patch
  payloads as the reference token. ``parent_ref`` may reference another
  envelope-local qname or, for ``reporting_extension`` blocks, a library
  element qname.
  """

  qname: str = Field(
    ...,
    description=(
      "Envelope-local element identifier. Must be unique within the "
      "envelope's ``elements`` list. Used as the reference token for "
      "associations, rules, and update patches."
    ),
  )
  name: str = Field(
    ...,
    description="Human-readable element name (e.g. 'Total Assets').",
  )
  trait: str | None = Field(
    None,
    description=(
      "FASB metamodel trait for the element. Required for "
      "``chart_of_accounts`` blocks; optional for ``custom_ontology``."
    ),
  )
  balance_type: str | None = Field(
    None, description="'debit' | 'credit' | null for non-monetary concepts."
  )
  element_type: str = Field(
    "concept",
    description="'concept' | 'abstract' | 'axis' | 'member' | 'hypercube'.",
  )
  period_type: str | None = Field(
    None,
    description=(
      "'instant' | 'duration' | null (null = derive from classification "
      "during validation)."
    ),
  )
  is_monetary: bool = Field(True, description="True for dollar-denominated concepts.")
  description: str | None = None
  code: str | None = Field(
    None,
    description=(
      "Optional chart-of-accounts code (e.g. '1000', '4100-02'). Only "
      "meaningful for ``chart_of_accounts`` blocks."
    ),
  )
  sub_classification: str | None = None
  parent_ref: str | None = Field(
    None,
    description=(
      "qname of the parent element — either another envelope-local qname "
      "or, for ``reporting_extension`` blocks, a library element qname."
    ),
  )
  metadata: dict[str, Any] = Field(default_factory=dict)


class TaxonomyBlockStructureRequest(BaseModel):
  """Structure definition inside a Taxonomy Block envelope."""

  name: str = Field(
    ..., description="Envelope-local structure name (unique within envelope)."
  )
  structure_type: Literal[
    "chart_of_accounts",
    "custom",
    "balance_sheet",
    "income_statement",
    "cash_flow_statement",
    "equity_statement",
    "coa_mapping",
    "schedule",
    "rollforward",
    "reconciliation",
    "policy",
    "metric",
  ] = Field(
    ...,
    description=(
      "DB ``structures.structure_type`` enum. CoA blocks use "
      "``chart_of_accounts``; reporting extensions use the statement "
      "family or ``custom``; custom ontology uses ``custom``."
    ),
  )
  description: str | None = None
  role_uri: str | None = None
  metadata: dict[str, Any] = Field(default_factory=dict)


class TaxonomyBlockAssociationRequest(BaseModel):
  """Association (arc) between two elements, scoped to a structure."""

  structure_ref: str = Field(
    ...,
    description=(
      "Envelope-local structure name (references a structure declared in "
      "the same envelope)."
    ),
  )
  from_ref: str = Field(..., description="qname of the source element.")
  to_ref: str = Field(..., description="qname of the target element.")
  association_type: Literal[
    "presentation",
    "calculation",
    "mapping",
    "equivalence",
    "general-special",
    "essence-alias",
  ] = Field(
    ...,
    description=(
      "DB ``associations.association_type`` enum. ``presentation`` = "
      "parent-child hierarchy; ``calculation`` = summation arc."
    ),
  )
  order_value: float | None = None
  arcrole: str | None = None
  weight: float | None = Field(
    None,
    description=(
      "Calculation-arc coefficient (+1 / -1 for summation, other values "
      "for weighted rollups). Null for non-calculation arcs."
    ),
  )
  metadata: dict[str, Any] = Field(default_factory=dict)


class TaxonomyBlockRuleRequest(BaseModel):
  """Rule definition inside a Taxonomy Block envelope.

  Exactly one of ``target_structure_ref``, ``target_element_qname``, or
  ``target_taxonomy_self`` must be set (or all null for a global rule).
  The ``model_validator`` enforces this contract at the Pydantic layer.
  """

  name: str = Field(..., description="Rule identifier, unique within envelope.")
  description: str | None = None
  rule_category: Literal[
    "AutomatedAccountingAndReportingChecks",
    "DisclosureMechanicsRule",
    "FundamentalAccountingConceptRelation",
    "PeerConsistencyRule",
    "PriorPeriodConsistencyRule",
    "ReportLevelModelStructureRule",
    "ReportingSystemSpecificRule",
    "ToDoManualTask",
    "XBRLTechnicalSyntaxRule",
  ] = Field(..., description="One of 8 cm:VerificationRule subclasses.")
  rule_pattern: Literal[
    "Adjustment",
    "CoExists",
    "EqualTo",
    "Exists",
    "GreaterThan",
    "GreaterThanOrEqualToZero",
    "LessThan",
    "RollForward",
    "RollUp",
    "SumEquals",
    "Variance",
  ] = Field(..., description="One of 11 cm:BusinessRulePattern mechanisms.")
  expression: str = Field(
    ..., description="XPath-flavored predicate body (the rule expression)."
  )
  variables: list[dict[str, Any]] = Field(
    default_factory=list,
    description=(
      "``$Variable`` → qname bindings. Each entry is "
      "``{'variable_name': str, 'variable_qname': str}``."
    ),
  )
  severity: Literal["info", "warning", "error"] = "error"
  target_structure_ref: str | None = Field(
    None,
    description=(
      "Envelope-local structure name this rule targets (for structure-"
      "scoped rules). Mutually exclusive with the other target_* fields."
    ),
  )
  target_element_qname: str | None = Field(
    None,
    description=(
      "qname of the element this rule targets. Mutually exclusive with "
      "the other target_* fields."
    ),
  )
  target_taxonomy_self: bool = Field(
    False,
    description=(
      "True iff the rule targets the envelope's own taxonomy row "
      "(``target_kind='taxonomy'``). Mutually exclusive with the other "
      "target_* fields."
    ),
  )
  message: str | None = None
  metadata: dict[str, Any] = Field(default_factory=dict)

  @model_validator(mode="after")
  def _validate_target_exclusivity(self) -> TaxonomyBlockRuleRequest:
    target_count = sum(
      [
        self.target_structure_ref is not None,
        self.target_element_qname is not None,
        self.target_taxonomy_self,
      ]
    )
    if target_count > 1:
      raise ValueError(
        "Rule target is polymorphic: at most one of target_structure_ref, "
        "target_element_qname, target_taxonomy_self may be set."
      )
    return self


# Envelope list-size caps. Prevent accidental submission of ~18k-element
# us-gaap-sized payloads via the tenant envelope — library ingestion has
# a separate path that bypasses these caps.
_MAX_ELEMENTS = 5000
_MAX_STRUCTURES = 100
_MAX_ASSOCIATIONS = 20000
_MAX_RULES = 500

WritableTaxonomyType = Literal[
  "reporting_standard",
  "reporting_extension",
  "custom_ontology",
  "chart_of_accounts",
  "schedule",
]


class CreateTaxonomyBlockRequest(BaseModel):
  """Request body for the ``create-taxonomy-block`` operation.

  One envelope per taxonomy instance. ``taxonomy_type`` discriminates
  which block-type handler the command dispatcher routes to.
  ``parent_taxonomy_id`` is required for ``reporting_extension`` (which
  extends a library taxonomy) and ignored otherwise.

  The library path (seeding ``reporting_standard`` rows) does NOT flow
  through this envelope — it uses a dedicated library writer that bypasses
  these caps and tenant scoping.
  """

  name: str = Field(..., description="Taxonomy display name.")
  taxonomy_type: WritableTaxonomyType = Field(
    ...,
    description=(
      "Block-type discriminator. ``chart_of_accounts`` and "
      "``custom_ontology`` construct from scratch; ``reporting_extension`` "
      "extends an existing library ``reporting_standard``."
    ),
  )
  parent_taxonomy_id: str | None = Field(
    None,
    description=(
      "Required when ``taxonomy_type == 'reporting_extension'`` — the id "
      "of the library ``reporting_standard`` being extended."
    ),
  )
  version: str | None = None
  description: str | None = None
  standard: str | None = None
  namespace_uri: str | None = None
  elements: list[TaxonomyBlockElementRequest] = Field(
    default_factory=list, max_length=_MAX_ELEMENTS
  )
  structures: list[TaxonomyBlockStructureRequest] = Field(
    default_factory=list, max_length=_MAX_STRUCTURES
  )
  associations: list[TaxonomyBlockAssociationRequest] = Field(
    default_factory=list, max_length=_MAX_ASSOCIATIONS
  )
  rules: list[TaxonomyBlockRuleRequest] = Field(
    default_factory=list, max_length=_MAX_RULES
  )
  metadata: dict[str, Any] = Field(default_factory=dict)

  @model_validator(mode="after")
  def _require_parent_for_extension(self) -> CreateTaxonomyBlockRequest:
    if self.taxonomy_type == "reporting_extension" and not self.parent_taxonomy_id:
      raise ValueError(
        "parent_taxonomy_id is required when taxonomy_type == 'reporting_extension'."
      )
    return self


class ElementUpdatePatch(BaseModel):
  """Partial-update patch for a single element, keyed by qname."""

  qname: str = Field(..., description="qname identifier of the element to update.")
  name: str | None = None
  description: str | None = None
  trait: str | None = None
  balance_type: str | None = None
  period_type: str | None = None
  is_monetary: bool | None = None
  code: str | None = None
  parent_ref: str | None = None
  metadata: dict[str, Any] | None = None


class StructureUpdatePatch(BaseModel):
  """Partial-update patch for a single structure, keyed by structure_id."""

  structure_id: str = Field(..., description="Structure id to update.")
  name: str | None = None
  description: str | None = None
  role_uri: str | None = None
  metadata: dict[str, Any] | None = None


class UpdateTaxonomyBlockRequest(BaseModel):
  """Request body for the ``update-taxonomy-block`` operation.

  Top-level fields (name / description / version) apply to the taxonomy
  row itself. The delta lists mutate atoms incrementally — the validator
  re-runs every create-time check across the projected post-update
  state before anything commits.
  """

  taxonomy_id: str
  name: str | None = None
  description: str | None = None
  version: str | None = None
  elements_to_add: list[TaxonomyBlockElementRequest] = Field(default_factory=list)
  elements_to_update: list[ElementUpdatePatch] = Field(default_factory=list)
  elements_to_remove: list[str] = Field(
    default_factory=list, description="qnames of elements to remove."
  )
  structures_to_add: list[TaxonomyBlockStructureRequest] = Field(default_factory=list)
  structures_to_update: list[StructureUpdatePatch] = Field(default_factory=list)
  structures_to_remove: list[str] = Field(
    default_factory=list, description="Structure ids to remove."
  )
  associations_to_add: list[TaxonomyBlockAssociationRequest] = Field(
    default_factory=list
  )
  associations_to_remove: list[str] = Field(
    default_factory=list, description="Association ids to remove."
  )
  rules_to_add: list[TaxonomyBlockRuleRequest] = Field(default_factory=list)
  rules_to_remove: list[str] = Field(
    default_factory=list, description="Rule ids to remove."
  )


class DeleteTaxonomyBlockRequest(BaseModel):
  """Request body for the ``delete-taxonomy-block`` operation.

  ``cascade_facts=False`` (default) fails the delete if any Fact rows
  reference elements in this taxonomy. ``cascade_facts=True`` deletes the
  referencing facts alongside the taxonomy; the response reports
  ``facts_deleted``.
  """

  taxonomy_id: str
  cascade_facts: bool = False
  reason: str = Field(
    ...,
    min_length=1,
    description="Human-readable justification (audit log).",
  )


# ── Response models ────────────────────────────────────────────────────────


class TaxonomyBlockElement(BaseModel):
  """Element projection for the Taxonomy Block envelope."""

  model_config = ConfigDict(from_attributes=True)

  id: str
  qname: str | None = None
  name: str
  trait: str | None = None
  balance_type: str | None = None
  period_type: str | None = None
  element_type: str = "concept"
  is_monetary: bool = True
  parent_qname: str | None = None
  depth: int | None = None
  origin: Literal["library", "tenant"] = Field(
    ...,
    description=(
      "Provenance — 'library' if the element's taxonomy is locked "
      "(``is_locked=true``), else 'tenant'."
    ),
  )


class TaxonomyBlockStructure(BaseModel):
  """Structure projection for the Taxonomy Block envelope."""

  model_config = ConfigDict(from_attributes=True)

  id: str
  name: str
  structure_type: str
  description: str | None = None
  role_uri: str | None = None


class TaxonomyBlockAssociation(BaseModel):
  """Association projection for the Taxonomy Block envelope."""

  model_config = ConfigDict(from_attributes=True)

  id: str
  structure_id: str
  from_element_qname: str
  to_element_qname: str
  association_type: str
  order_value: float | None = None
  arcrole: str | None = None
  weight: float | None = None


class TaxonomyBlockRule(BaseModel):
  """Rule projection for the Taxonomy Block envelope."""

  model_config = ConfigDict(from_attributes=True)

  id: str
  name: str
  rule_category: str
  rule_pattern: str
  rule_expression: str
  severity: str = "error"
  origin: str = Field(
    "native",
    description="'forked' | 'native' | 'auto' — matches DB CHECK.",
  )
  target_kind: str | None = None
  target_ref: str | None = Field(
    None,
    description=(
      "Polymorphic display string — structure_id, element qname, "
      "association_id, or taxonomy_id depending on ``target_kind``."
    ),
  )


class TaxonomyBlockEnvelope(BaseModel):
  """The Taxonomy Block exchange format.

  One envelope per taxonomy instance. Carries identity + type,
  registry-sourced display metadata, the parent taxonomy pointer (for
  ``reporting_extension`` blocks), and bundled atoms (elements,
  structures, associations, rules, verification results).
  """

  id: str = Field(..., description="Taxonomy row id.")
  name: str
  taxonomy_type: str = Field(
    ...,
    description=(
      "Block-type discriminator — 'reporting_standard' | "
      "'reporting_extension' | 'chart_of_accounts' | 'custom_ontology'."
    ),
  )
  display_name: str = Field(..., description="Registry-sourced display label.")
  category: str = Field(..., description="Registry-sourced sidebar grouping.")
  parent_taxonomy_id: str | None = None
  parent_taxonomy_name: str | None = None
  version: str | None = None
  standard: str | None = None
  namespace_uri: str | None = None
  is_locked: bool = False

  elements: list[TaxonomyBlockElement] = Field(default_factory=list)
  structures: list[TaxonomyBlockStructure] = Field(default_factory=list)
  associations: list[TaxonomyBlockAssociation] = Field(default_factory=list)
  rules: list[TaxonomyBlockRule] = Field(default_factory=list)
  verification_results: list[dict[str, Any]] = Field(default_factory=list)

  element_count: int = 0
  structure_count: int = 0
  association_count: int = 0


class DeleteTaxonomyBlockResponse(BaseModel):
  """Response for ``delete-taxonomy-block``."""

  deleted: Literal[True] = True
  taxonomy_id: str
  name: str
  facts_deleted: int = 0
  cascade_applied: bool = False


__all__ = [
  "CreateTaxonomyBlockRequest",
  "DeleteTaxonomyBlockRequest",
  "DeleteTaxonomyBlockResponse",
  "ElementUpdatePatch",
  "StructureUpdatePatch",
  "TaxonomyBlockAssociation",
  "TaxonomyBlockAssociationRequest",
  "TaxonomyBlockElement",
  "TaxonomyBlockElementRequest",
  "TaxonomyBlockEnvelope",
  "TaxonomyBlockRule",
  "TaxonomyBlockRuleRequest",
  "TaxonomyBlockStructure",
  "TaxonomyBlockStructureRequest",
  "UpdateTaxonomyBlockRequest",
  "WritableTaxonomyType",
]
