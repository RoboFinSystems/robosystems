"""Pydantic model for taxonomy packages.

`TaxonomyPackage` is the in-memory intermediate between JSON-LD (canonical
on-disk format) and the database library_writer. The loader reads JSON-LD →
TaxonomyPackage; the writer takes TaxonomyPackage → SQL INSERTs. The
extractor (in `robosystems/arelle/`) produces rdflib.Graph which is
serialized to JSON-LD via the serializer — these never construct
TaxonomyPackage directly; it's a DB-side DTO.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class LabelSpec(BaseModel):
  """XBRL label linkbase entry."""

  role: str = Field(
    "standard",
    description=(
      "Label role: standard, verbose, terse, documentation, "
      "periodStart, periodEnd, negated, total, commentaryGuidance, other"
    ),
  )
  language: str = Field("en", description="ISO 639-1 language code")
  text: str = Field(..., description="The label text")


class ReferenceSpec(BaseModel):
  """XBRL reference linkbase entry (FASB ASC citation, SEC reg, etc)."""

  ref_type: str | None = Field(
    None, description="'ASC' | 'SEC' | 'SFAC' | 'IFRS' | 'Other'"
  )
  citation: str = Field(
    ..., description="Free-form citation, e.g. 'FASB ASC 842-10-25-1'"
  )
  uri: str | None = Field(None, description="Dereferenceable URL if available")
  attributes: str | None = Field(
    None,
    description="Raw attributes from the source reference linkbase (Publisher, Number, Paragraph)",
  )


class ElementSpec(BaseModel):
  """A single taxonomy element (XBRL concept).

  Only XBRL-intrinsic attributes live here (balance_type, period_type,
  is_abstract, is_monetary, element_type, substitution_group).
  Classifications live in ClassificationSpec + ClassificationAssignmentSpec.
  """

  qname: str = Field(..., description="Qualified name, e.g. 'sfac6:Assets'")
  namespace: str = Field(..., description="Namespace prefix, e.g. 'sfac6'")
  namespace_uri: str = Field(..., description="Full namespace URI")
  name: str = Field(..., description="Local name within the namespace")

  balance_type: str = Field("debit", description="debit | credit")
  period_type: str = Field("duration", description="instant | duration")
  is_abstract: bool = Field(False, description="True for abstract grouping concepts")
  is_monetary: bool = Field(True, description="True for monetary-valued concepts")
  element_type: str = Field(
    "concept", description="concept | abstract | axis | member | hypercube"
  )
  substitution_group: str | None = Field(
    None, description="XBRL substitution group qname, e.g. 'xbrli:item'"
  )

  # Source / origin
  source: str = Field(..., description="sfac6 | fac | us-gaap | ifrs | …")

  # Hierarchy (resolved at write time) — parent_id in OLTP. Independent
  # of classification: this is the element-tree hierarchy, not the
  # class-subclass classification hierarchy (which lives in associations).
  parent_qname: str | None = Field(None, description="Parent element qname if known")

  # Rich metadata
  labels: list[LabelSpec] = Field(default_factory=list)
  references: list[ReferenceSpec] = Field(default_factory=list)


class ClassificationSpec(BaseModel):
  """A classification vocabulary entry: (category, identifier) pair.

  Each entry seeds one row in the ``classifications`` table. Categories
  are the 24 FASB metamodel trait axes plus flowClassification and the
  association-level categories.
  """

  category: str = Field(
    ...,
    description=(
      "Classification axis, e.g. 'elementsOfFinancialStatements', "
      "'liquidity', 'activityType'."
    ),
  )
  identifier: str = Field(
    ..., description="Member name within the category, e.g. 'asset', 'current'."
  )
  source: str = Field(
    ..., description="Provenance, e.g. 'us-gaap-metamodel', 'sfac6', 'system'."
  )
  name: str | None = Field(None, description="Human-readable display name")
  description: str | None = Field(None)


class ClassificationAssignmentSpec(BaseModel):
  """Element-to-classification assignment — seeds one element_classifications row."""

  element_qname: str = Field(..., description="Element qname being classified")
  category: str = Field(..., description="Classification category")
  identifier: str = Field(..., description="Member identifier within the category")
  source: str = Field(
    "us-gaap-metamodel",
    description=(
      "Provenance of the assignment — which seed/taxonomy declared the "
      "(element → classification) arc. Defaults to us-gaap-metamodel "
      "since that's where nearly all EFS assignments originate; future "
      "seeds from other metamodels (e.g. ifrs-metamodel) should set "
      "this explicitly so row provenance is preserved in "
      "element_classifications.source."
    ),
  )
  is_primary: bool = Field(
    True,
    description=(
      "Canonical row per (element, category). Most elements carry a "
      "single primary assignment per category; multi-valued axes "
      "(multiple trait members in one category) set is_primary=true "
      "on the first and false on alternates."
    ),
  )
  confidence: float | None = Field(None, description="For AI-suggested assignments")


class AssociationSpec(BaseModel):
  """A single arc in the taxonomy (parent-child, equivalence, etc)."""

  from_qname: str = Field(..., description="Source element qname")
  to_qname: str = Field(..., description="Target element qname")
  association_type: str = Field(
    ...,
    description=(
      "presentation | calculation | mapping | equivalence | "
      "general-special | essence-alias"
    ),
  )
  arcrole: str = Field(..., description="Full XBRL arcrole URI")
  role: str | None = Field(None, description="XBRL extended link role URI (structure)")
  order: float | None = Field(None, description="Presentation order")
  weight: float | None = Field(None, description="Calculation weight (+1/-1)")


class StructureSpec(BaseModel):
  """An extended link role as a named structure (Balance Sheet, etc)."""

  name: str = Field(..., description="Human-readable name")
  role_uri: str = Field(..., description="Full XBRL extended link role URI")
  structure_type: str = Field(
    "custom",
    description=(
      "balance_sheet | income_statement | cash_flow_statement | "
      "equity_statement | chart_of_accounts | coa_mapping | schedule | custom"
    ),
  )


class TaxonomyPackage(BaseModel):
  """A fully-loaded taxonomy ready for library persistence."""

  name: str = Field(..., description="Human name, e.g. 'SFAC 6 v1'")
  standard: str = Field(..., description="sfac6 | fac | us-gaap | ifrs")
  version: str = Field(..., description="Version identifier, e.g. 'v1' or '2020'")
  namespace_uri: str = Field(..., description="Primary namespace URI for this package")

  elements: list[ElementSpec] = Field(default_factory=list)
  associations: list[AssociationSpec] = Field(default_factory=list)
  structures: list[StructureSpec] = Field(default_factory=list)
  classifications: list[ClassificationSpec] = Field(default_factory=list)
  classification_assignments: list[ClassificationAssignmentSpec] = Field(
    default_factory=list
  )

  taxonomy_type: str = Field(
    "reporting",
    description=(
      "chart_of_accounts | reporting | mapping | schedule | "
      "classification-vocabulary | classification-assignment — shapes "
      "how the library viewer renders this taxonomy. Concept taxonomies "
      "(sfac6, rs-gaap) are 'reporting'; equivalence + hierarchy arc "
      "packs (fac, rs-gaap-hierarchy) are 'mapping'; the FASB metamodel "
      "seed is 'classification-vocabulary'; rs-gaap-to-metamodel is "
      "'classification-assignment'."
    ),
  )
  is_shared: bool = Field(True, description="Shared across tenants (library-origin)")
  description: str | None = Field(None)
