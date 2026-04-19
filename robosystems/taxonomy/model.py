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
  """A single taxonomy element (XBRL concept)."""

  qname: str = Field(..., description="Qualified name, e.g. 'sfac6:Assets'")
  namespace: str = Field(..., description="Namespace prefix, e.g. 'sfac6'")
  namespace_uri: str = Field(..., description="Full namespace URI")
  name: str = Field(..., description="Local name within the namespace")

  # Classification — three orthogonal axes. All nullable; only SFAC 6
  # primitives fill every one.
  classification: str | None = Field(
    None,
    description=(
      "Economic nature axis: asset | liability | equity | revenue | "
      "expense | gain | loss. Null for structural, metadata, and "
      "computed-ratio rows. Equity flows (contributions, distributions, "
      "comprehensive income) collapse into `equity`; direction is "
      "captured by balance_type and statement_context."
    ),
  )
  statement_context: str | None = Field(
    None,
    description=(
      "Which report the element belongs to: balance_sheet | "
      "income_statement | cash_flow | equity_changes | disclosure | "
      "metadata | analysis."
    ),
  )
  derivation_role: str | None = Field(
    None,
    description=(
      "Structural role in a report: primitive | subtotal | total | "
      "reconciliation | movement | ratio | identifier | structural."
    ),
  )
  balance_type: str = Field("debit", description="debit | credit")
  period_type: str = Field("duration", description="instant | duration")
  is_abstract: bool = Field(False, description="True for abstract grouping concepts")
  is_monetary: bool = Field(True, description="True for monetary-valued concepts")
  element_type: str = Field(
    "concept", description="concept | abstract | axis | member | hypercube"
  )
  substitution_group: str | None = Field(
    None, description="XBRL substitution group qname"
  )

  # Source / origin
  source: str = Field(..., description="sfac6 | fac | us-gaap | ifrs | …")

  # Hierarchy (resolved at write time)
  parent_qname: str | None = Field(None, description="Parent element qname if known")

  # Rich metadata
  labels: list[LabelSpec] = Field(default_factory=list)
  references: list[ReferenceSpec] = Field(default_factory=list)


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

  taxonomy_type: str = Field(
    "reporting",
    description=(
      "chart_of_accounts | reporting | mapping | schedule — shapes how the "
      "library viewer renders this taxonomy. Concept taxonomies (sfac6, "
      "rs-gaap) are 'reporting'; equivalence + classification arc packs "
      "(fac, type-subtype) are 'mapping'."
    ),
  )
  is_shared: bool = Field(True, description="Shared across tenants (library-origin)")
  description: str | None = Field(None)
