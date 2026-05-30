"""Strawberry types for the Taxonomy Block GraphQL surface.

Leaf types are hand-written — ``TaxonomyBlockElement.origin`` is a
``Literal["library", "tenant"]`` which Strawberry's pydantic decorator
can't derive, so the leaves use explicit ``@strawberry.type`` + a
``from_pydantic`` classmethod. Top-level :class:`TaxonomyBlock` is also
hand-written because ``verification_results`` is an opaque JSON list.
``from_pydantic`` on the envelope constructs the full tree explicitly.

Verification results carry an opaque JSON shape; clients consume the
raw payload as the rule-evaluation surface stabilizes.
"""

from __future__ import annotations

import strawberry

from robosystems.models.api.taxonomy_block import (
  TaxonomyBlockAssociation as PydanticAssociation,
)
from robosystems.models.api.taxonomy_block import (
  TaxonomyBlockElement as PydanticElement,
)
from robosystems.models.api.taxonomy_block import (
  TaxonomyBlockEnvelope as PydanticTaxonomyBlock,
)
from robosystems.models.api.taxonomy_block import (
  TaxonomyBlockRule as PydanticRule,
)
from robosystems.models.api.taxonomy_block import (
  TaxonomyBlockStructure as PydanticStructure,
)

# ── Leaf types — hand-written for Literal / JSON compatibility ─────────────


@strawberry.type
class TaxonomyBlockElement:
  """An element bundled inside a Taxonomy Block envelope."""

  id: str
  qname: str | None
  name: str
  trait: str | None
  balance_type: str | None
  period_type: str | None
  element_type: str
  is_monetary: bool
  parent_qname: str | None
  depth: int | None
  origin: str

  @classmethod
  def from_pydantic(cls, e: PydanticElement) -> TaxonomyBlockElement:
    return cls(
      id=e.id,
      qname=e.qname,
      name=e.name,
      trait=e.trait,
      balance_type=e.balance_type,
      period_type=e.period_type,
      element_type=e.element_type,
      is_monetary=e.is_monetary,
      parent_qname=e.parent_qname,
      depth=e.depth,
      origin=e.origin,
    )


@strawberry.type
class TaxonomyBlockStructure:
  """A structure bundled inside a Taxonomy Block envelope."""

  id: str
  name: str
  block_type: str
  description: str | None
  role_uri: str | None

  @classmethod
  def from_pydantic(cls, s: PydanticStructure) -> TaxonomyBlockStructure:
    return cls(
      id=s.id,
      name=s.name,
      block_type=s.block_type,
      description=s.description,
      role_uri=s.role_uri,
    )


@strawberry.type
class TaxonomyBlockAssociation:
  """An association (arc) bundled inside a Taxonomy Block envelope."""

  id: str
  structure_id: str
  from_element_qname: str
  to_element_qname: str
  association_type: str
  order_value: float | None
  arcrole: str | None
  weight: float | None

  @classmethod
  def from_pydantic(cls, a: PydanticAssociation) -> TaxonomyBlockAssociation:
    return cls(
      id=a.id,
      structure_id=a.structure_id,
      from_element_qname=a.from_element_qname,
      to_element_qname=a.to_element_qname,
      association_type=a.association_type,
      order_value=a.order_value,
      arcrole=a.arcrole,
      weight=a.weight,
    )


@strawberry.type
class TaxonomyBlockRule:
  """A verification rule bundled inside a Taxonomy Block envelope."""

  id: str
  name: str
  rule_category: str
  rule_pattern: str
  rule_expression: str
  severity: str
  origin: str
  target_kind: str | None
  target_ref: str | None

  @classmethod
  def from_pydantic(cls, r: PydanticRule) -> TaxonomyBlockRule:
    return cls(
      id=r.id,
      name=r.name,
      rule_category=r.rule_category,
      rule_pattern=r.rule_pattern,
      rule_expression=r.rule_expression,
      severity=r.severity,
      origin=r.origin,
      target_kind=r.target_kind,
      target_ref=r.target_ref,
    )


VerificationResultPayload = strawberry.scalars.JSON


# ── Top-level envelope — hand-written for verification_results JSON ────────


@strawberry.type
class TaxonomyBlock:
  """Taxonomy Block envelope — the molecular exchange format for ontology.

  Consumers (agents via MCP, the React TaxonomyViewer, SDK clients)
  receive the same envelope shape regardless of the taxonomy_type.
  """

  id: strawberry.ID
  name: str
  taxonomy_type: str
  display_name: str
  category: str

  parent_taxonomy_id: str | None
  parent_taxonomy_name: str | None
  version: str | None
  standard: str | None
  namespace_uri: str | None
  is_locked: bool

  elements: list[TaxonomyBlockElement]
  structures: list[TaxonomyBlockStructure]
  associations: list[TaxonomyBlockAssociation]
  rules: list[TaxonomyBlockRule]

  # ``verification_results`` is an opaque JSON list; clients receive the
  # raw payload until the rule-evaluation surface stabilizes.
  verification_results: list[VerificationResultPayload]

  element_count: int
  structure_count: int
  association_count: int

  @classmethod
  def from_pydantic(cls, envelope: PydanticTaxonomyBlock) -> TaxonomyBlock:
    return cls(
      id=strawberry.ID(envelope.id),
      name=envelope.name,
      taxonomy_type=envelope.taxonomy_type,
      display_name=envelope.display_name,
      category=envelope.category,
      parent_taxonomy_id=envelope.parent_taxonomy_id,
      parent_taxonomy_name=envelope.parent_taxonomy_name,
      version=envelope.version,
      standard=envelope.standard,
      namespace_uri=envelope.namespace_uri,
      is_locked=envelope.is_locked,
      elements=[TaxonomyBlockElement.from_pydantic(e) for e in envelope.elements],
      structures=[TaxonomyBlockStructure.from_pydantic(s) for s in envelope.structures],
      associations=[
        TaxonomyBlockAssociation.from_pydantic(a) for a in envelope.associations
      ],
      rules=[TaxonomyBlockRule.from_pydantic(r) for r in envelope.rules],
      verification_results=list(envelope.verification_results),
      element_count=envelope.element_count,
      structure_count=envelope.structure_count,
      association_count=envelope.association_count,
    )


__all__ = [
  "TaxonomyBlock",
  "TaxonomyBlockAssociation",
  "TaxonomyBlockElement",
  "TaxonomyBlockRule",
  "TaxonomyBlockStructure",
]
