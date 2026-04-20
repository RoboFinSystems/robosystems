"""Library reads — query the public-schema taxonomy library.

Pure functions that take an open SQLAlchemy `Session` bound to the
`public` schema and return Pydantic response models (or `None`).
Callers own the session lifetime.
"""

from __future__ import annotations

from robosystems.operations.library.reads.elements import (
  get_element,
  get_element_arcs,
  get_element_by_qname,
  get_element_classifications,
  get_element_equivalents,
  get_element_tree,
  list_elements,
  search_elements,
)
from robosystems.operations.library.reads.structures import (
  get_structure,
  list_structures,
)
from robosystems.operations.library.reads.taxonomies import (
  count_taxonomy_arcs,
  get_taxonomy,
  list_taxonomies,
  list_taxonomy_arcs,
)

__all__ = [
  "count_taxonomy_arcs",
  "get_element",
  "get_element_arcs",
  "get_element_by_qname",
  "get_element_classifications",
  "get_element_equivalents",
  "get_element_tree",
  "get_structure",
  "get_taxonomy",
  "list_elements",
  "list_structures",
  "list_taxonomies",
  "list_taxonomy_arcs",
  "search_elements",
]
