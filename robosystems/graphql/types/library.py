"""Strawberry types for the taxonomy library GraphQL surface.

Library types mirror the `robosystems.models.api.library` Pydantic
models via `strawberry.experimental.pydantic.type(model=...,
all_fields=True)`, same pattern as the ledger/investor types. When a
Pydantic field is added to a response model the GraphQL field appears
automatically without hand-written plumbing.

**Recursive types** (`LibraryElementTreeNode`) are hand-written because
Strawberry's pydantic decorator cannot resolve self-references.
"""

from __future__ import annotations

import strawberry

# PaginationInfo must be imported before any type that references it via
# the Pydantic decorator (registry ordering); the ledger types do the
# same dance. Unused-import noqa applies.
from robosystems.graphql.types.common import PaginationInfo  # noqa: F401
from robosystems.models.api.library import (
  LibraryAssociationResponse as PydanticLibraryAssociation,
)
from robosystems.models.api.library import (
  LibraryElementArcResponse as PydanticLibraryElementArc,
)
from robosystems.models.api.library import (
  LibraryElementResponse as PydanticLibraryElement,
)
from robosystems.models.api.library import (
  LibraryElementTreeNode as PydanticLibraryElementTreeNode,
)
from robosystems.models.api.library import (
  LibraryEquivalenceResponse as PydanticLibraryEquivalence,
)
from robosystems.models.api.library import (
  LibraryLabelResponse as PydanticLibraryLabel,
)
from robosystems.models.api.library import (
  LibraryReferenceResponse as PydanticLibraryReference,
)
from robosystems.models.api.library import (
  LibraryStructureResponse as PydanticLibraryStructure,
)
from robosystems.models.api.library import (
  LibraryTaxonomyResponse as PydanticLibraryTaxonomy,
)


@strawberry.experimental.pydantic.type(model=PydanticLibraryLabel, all_fields=True)
class LibraryLabel:
  """A label on a library element (XBRL label linkbase entry)."""


@strawberry.experimental.pydantic.type(model=PydanticLibraryReference, all_fields=True)
class LibraryReference:
  """A cross-reference on a library element (FASB ASC, SEC, etc)."""


@strawberry.experimental.pydantic.type(model=PydanticLibraryElement, all_fields=True)
class LibraryElement:
  """A single library concept (from sfac6, fac, us-gaap, rs-gaap, …)."""


@strawberry.experimental.pydantic.type(model=PydanticLibraryTaxonomy, all_fields=True)
class LibraryTaxonomy:
  """A curated library taxonomy (sfac6, fac, us-gaap version, rs-gaap)."""


@strawberry.experimental.pydantic.type(model=PydanticLibraryStructure, all_fields=True)
class LibraryStructure:
  """A named structure within a library taxonomy (extended link role)."""


@strawberry.experimental.pydantic.type(
  model=PydanticLibraryAssociation, all_fields=True
)
class LibraryAssociation:
  """An arc between two library elements (parent-child, equivalence, …)."""


@strawberry.type
class LibraryElementTreeNode:
  """Recursive library element tree node.

  Hand-written because `children: list[LibraryElementTreeNode]` is a
  self-reference — Strawberry's `experimental.pydantic.type` decorator
  cannot resolve that automatically. Use `from_pydantic` to convert a
  `PydanticLibraryElementTreeNode` returned from
  `operations/library/reads/elements.py::get_element_tree`.
  """

  element: LibraryElement
  children: list[LibraryElementTreeNode]

  @classmethod
  def from_pydantic(
    cls, node: PydanticLibraryElementTreeNode
  ) -> LibraryElementTreeNode:
    return cls(
      element=LibraryElement.from_pydantic(node.element),
      children=[cls.from_pydantic(c) for c in node.children],
    )


@strawberry.experimental.pydantic.type(
  model=PydanticLibraryEquivalence, all_fields=True
)
class LibraryEquivalence:
  """An element and all its equivalence peers (FAC ↔ us-gaap collapse)."""


@strawberry.experimental.pydantic.type(model=PydanticLibraryElementArc, all_fields=True)
class LibraryElementArc:
  """A single mapping arc oriented from an element's perspective.

  Returned by `libraryElementArcs(id)` — the flat list of cross-taxonomy
  bridges that the library detail panel renders in place of the old
  per-taxonomy arc browser.
  """
