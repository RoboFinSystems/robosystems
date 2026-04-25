"""API response models for the taxonomy library.

These are wire-facing types for the library's read surface — returned
by `operations/library/reads/*` and consumed by REST routers, GraphQL
resolvers, and MCP tools. They are deliberately trimmed versions of
the underlying ORM rows: stable contract, no SQLAlchemy leakage.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class LibraryLabelResponse(BaseModel):
  """A label on a library element."""

  role: str = Field(..., description="Label role: standard/documentation/verbose/…")
  language: str = Field("en", description="Language code")
  text: str = Field(..., description="Label text")


class LibraryReferenceResponse(BaseModel):
  """A cross-reference on a library element (FASB ASC, SEC, etc)."""

  ref_type: str | None = Field(
    None, description="'ASC' | 'SEC' | 'SFAC' | 'IFRS' | 'Other'"
  )
  citation: str = Field(..., description="Full citation text")
  uri: str | None = Field(None, description="Dereferenceable URL if available")


class LibraryElementResponse(BaseModel):
  """A library element (concept, abstract, axis, member, or hypercube)."""

  id: str
  qname: str = Field(..., description="Qualified name, e.g. 'fac:Assets'")
  namespace: str | None = Field(None)
  name: str
  trait: str | None = Field(
    None,
    description=(
      "FASB elementsOfFinancialStatements axis: asset | contraAsset | "
      "liability | contraLiability | equity | contraEquity | "
      "temporaryEquity | revenue | expense | expenseReversal | gain | "
      "loss | comprehensiveIncome | investmentByOwners | "
      "distributionToOwners | metric (derived subtotals, not SFAC 6 "
      "primary elements). Null for structural rows."
    ),
  )
  balance_type: str = Field(..., description="debit | credit")
  period_type: str = Field(..., description="instant | duration")
  is_abstract: bool = False
  is_monetary: bool = True
  element_type: str = Field(
    ..., description="concept | abstract | axis | member | hypercube"
  )
  source: str = Field(..., description="fac | us-gaap | rs-gaap | …")
  taxonomy_id: str | None = Field(None)
  parent_id: str | None = Field(None)
  labels: list[LibraryLabelResponse] = Field(default_factory=list)
  references: list[LibraryReferenceResponse] = Field(default_factory=list)


class LibraryTaxonomyResponse(BaseModel):
  """A library taxonomy (fac, us-gaap, rs-gaap, …)."""

  id: str
  name: str
  description: str | None = None
  standard: str | None = Field(None, description="fac | us-gaap | rs-gaap | ifrs")
  version: str | None = None
  namespace_uri: str | None = None
  taxonomy_type: str = Field(
    ..., description="chart_of_accounts | reporting | mapping | schedule"
  )
  is_shared: bool = True
  is_active: bool = True
  is_locked: bool = True
  element_count: int | None = Field(
    None, description="Total elements in this taxonomy (computed on demand)"
  )


class LibraryStructureResponse(BaseModel):
  """A named structure (extended link role) within a library taxonomy."""

  id: str
  name: str
  structure_type: str = Field(
    ...,
    description="balance_sheet | income_statement | cash_flow_statement | custom | …",
  )
  taxonomy_id: str
  role_uri: str | None = Field(None, description="Original XBRL role URI if any")
  is_active: bool = True


class LibraryAssociationResponse(BaseModel):
  """An arc between two library elements (parent-child, equivalence, etc)."""

  id: str
  structure_id: str
  structure_name: str | None = None
  from_element_id: str
  from_element_qname: str | None = None
  from_element_name: str | None = None
  to_element_id: str
  to_element_qname: str | None = None
  to_element_name: str | None = None
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


class LibraryElementTreeNode(BaseModel):
  """Nested element with descendants for tree walks."""

  element: LibraryElementResponse
  children: list[LibraryElementTreeNode] = Field(default_factory=list)


class LibraryEquivalenceResponse(BaseModel):
  """An element and its equivalence peers.

  Answers "what other concepts mean the same thing as this one" — the
  FAC→us-gaap collapse pattern rendered as an API shape.
  """

  element: LibraryElementResponse
  equivalents: list[LibraryElementResponse] = Field(default_factory=list)


class LibraryElementArcResponse(BaseModel):
  """A mapping arc involving a specific element.

  Flat row view: one arc, oriented from the perspective of the element
  being inspected. `peer` is the other end; `direction` says whether
  this element is the source ('outgoing') or the target ('incoming').

  Scoped to arcs whose structure belongs to a `taxonomy_type='mapping'`
  taxonomy — the cross-taxonomy bridges (equivalence, general-special,
  type-subtype). Hierarchical arcs inside a single reporting taxonomy
  are out of scope.
  """

  id: str
  # Kept as `str` rather than `Literal["outgoing","incoming"]`: the
  # Strawberry Pydantic auto-decorator in `graphql/types/library.py`
  # can't map Python Literal to GraphQL scalars. Producers emit only
  # 'outgoing' or 'incoming'; a caller-side validator would duplicate
  # that invariant without GraphQL enforcement.
  direction: str = Field(
    ..., description="'outgoing' (this element is source) | 'incoming' (target)"
  )
  association_type: str
  arcrole: str | None = None
  taxonomy_id: str | None = None
  taxonomy_standard: str | None = None
  taxonomy_name: str | None = None
  structure_id: str | None = None
  structure_name: str | None = None
  peer: LibraryElementResponse


class LibraryElementTraitResponse(BaseModel):
  """One FASB metamodel trait assigned to a library element.

  A single element can carry multiple traits across multiple categories
  (e.g. elementsOfFinancialStatements=expense AND
  operatingNonoperating=operating AND liquidity=current).
  """

  category: str = Field(
    ..., description="Trait axis (e.g. elementsOfFinancialStatements)"
  )
  identifier: str = Field(..., description="Value within the axis (e.g. expense)")
  name: str | None = Field(None, description="Human-readable name")
  is_primary: bool = Field(
    False, description="True for the element's primary EFS trait assignment"
  )
