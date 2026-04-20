"""Element CRUD commands.

Pure functions that take an open extensions Session and return
Pydantic response models. Callers own session lifetime and translate
domain exceptions into transport errors.

Element hierarchy uses a materialized path:

- Root element: depth=0, path=""
- Direct child: depth=1, path="<root_id>"
- Grandchild: depth=2, path="<root_id>/<parent_id>"

Reparenting cascades path updates to all descendants, preserving the
relative hierarchy beneath the moved element.
"""

from __future__ import annotations

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.taxonomies import (
  CreateElementRequest,
  DeleteElementRequest,
  ElementResponse,
  UpdateElementRequest,
)
from robosystems.models.extensions import Element, Taxonomy
from robosystems.operations.roboledger.commands._guards import (
  LibraryImmutableError,
  assert_not_library_origin,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  TaxonomyNotFoundError,
)
from robosystems.utils.ulid import generate_prefixed_ulid

__all__ = [
  "ElementCycleError",
  "ElementNotFoundError",
  "LibraryImmutableError",
  "TaxonomyNotFoundError",
  "create_element",
  "delete_element",
  "update_element",
]


class ElementNotFoundError(LookupError):
  """Raised when an element is not found by id.

  Distinct from `taxonomies.ElementNotFoundError`, which carries a
  `side` field ("source" | "target") for mapping-association context.
  """

  def __init__(self, element_id: str) -> None:
    super().__init__(f"Element not found: {element_id}")
    self.element_id = element_id


class ElementCycleError(ValueError):
  """Raised when an update would create a cycle in the element hierarchy."""


def _element_to_response(row: Element) -> ElementResponse:
  return ElementResponse(
    id=row.id,
    code=row.code,
    name=row.name,
    description=row.description,
    qname=row.qname,
    namespace=row.namespace,
    classification=row.classification,
    sub_classification=row.sub_classification,
    balance_type=row.balance_type,
    period_type=row.period_type,
    is_abstract=row.is_abstract,
    element_type=row.element_type,
    source=row.source,
    taxonomy_id=row.taxonomy_id,
    parent_id=row.parent_id,
    depth=row.depth,
    is_active=row.is_active,
    external_id=row.external_id,
    external_source=row.external_source,
  )


def _hierarchy_for_parent(session: Session, parent_id: str | None) -> tuple[int, str]:
  """Compute (depth, path) for a child whose parent is `parent_id`.

  Root elements (no parent) return (0, ""). Others return
  (parent.depth + 1, parent.path + "/" + parent.id) (or just parent.id
  if parent is a root).
  """
  if parent_id is None:
    return 0, ""
  parent = session.execute(
    select(Element).where(Element.id == parent_id)
  ).scalar_one_or_none()
  if parent is None:
    raise ElementNotFoundError(parent_id)
  depth = parent.depth + 1
  path = f"{parent.path}/{parent.id}" if parent.path else parent.id
  return depth, path


def _self_path_prefix(element: Element) -> str:
  """The path prefix used by all descendants of `element`.

  An element with depth=0 path="" and id="elem_X" has descendants with
  paths starting with "elem_X". An element with depth=2 path="A/B"
  id="C" has descendants with paths starting with "A/B/C".
  """
  return f"{element.path}/{element.id}" if element.path else element.id


def _would_create_cycle(element: Element, new_parent: Element) -> bool:
  """True if reparenting `element` under `new_parent` would create a cycle.

  Cycle happens iff new_parent is element itself or new_parent is a
  descendant of element (new_parent's path contains element.id).
  """
  if new_parent.id == element.id:
    return True
  self_prefix = _self_path_prefix(element)
  if new_parent.path == self_prefix:
    return True
  if new_parent.path.startswith(f"{self_prefix}/"):
    return True
  return False


def create_element(
  session: Session, body: CreateElementRequest, created_by: str
) -> ElementResponse:
  """Create a new element within a taxonomy.

  Raises `TaxonomyNotFoundError` if `body.taxonomy_id` does not exist,
  or `ElementNotFoundError` if `body.parent_id` is set but the parent
  does not exist.
  """
  taxonomy = session.execute(
    select(Taxonomy).where(Taxonomy.id == body.taxonomy_id)
  ).scalar_one_or_none()
  if taxonomy is None:
    raise TaxonomyNotFoundError(body.taxonomy_id)
  # Tenants can only author inside tenant-origin taxonomies. The taxonomy
  # row is already in hand from the existence check above, so reuse it
  # instead of issuing a second query (also keeps the unit tests' mock
  # execute side_effect chain intact).
  assert_not_library_origin(taxonomy)

  depth, path = _hierarchy_for_parent(session, body.parent_id)

  element = Element(
    id=generate_prefixed_ulid("elem"),
    code=body.code,
    name=body.name,
    description=body.description,
    classification=body.classification,
    sub_classification=body.sub_classification,
    balance_type=body.balance_type,
    period_type=body.period_type,
    element_type=body.element_type,
    is_abstract=body.is_abstract,
    is_monetary=body.is_monetary,
    parent_id=body.parent_id,
    depth=depth,
    path=path,
    taxonomy_id=body.taxonomy_id,
    source=body.source,
    currency=body.currency,
    qname=body.qname,
    namespace=body.namespace,
    external_id=body.external_id,
    external_source=body.external_source,
    is_active=True,
    created_by=created_by,
  )
  session.add(element)
  session.flush()
  return _element_to_response(element)


def update_element(session: Session, body: UpdateElementRequest) -> ElementResponse:
  """Update mutable fields on an element.

  Uses `model_dump(exclude_unset=True)` semantics: omitted fields are
  left unchanged, explicit nulls are applied. For `parent_id`: omit to
  leave unchanged, pass null to detach from parent (make root).

  Reparenting cascades path + depth updates to all descendants so the
  materialized paths stay consistent. Rejects reparenting that would
  create a cycle.

  Raises `ElementNotFoundError` if the element or new parent does not
  exist, or `ElementCycleError` if the update would create a cycle.
  """
  element = session.execute(
    select(Element).where(Element.id == body.element_id)
  ).scalar_one_or_none()
  if element is None:
    raise ElementNotFoundError(body.element_id)
  assert_not_library_origin(element)

  updates = body.model_dump(exclude_unset=True)
  updates.pop("element_id", None)

  # Handle reparent separately so we can cascade path/depth changes.
  reparent = "parent_id" in updates
  new_parent_id: str | None = updates.pop("parent_id", None) if reparent else None

  if reparent:
    if new_parent_id is not None:
      new_parent = session.execute(
        select(Element).where(Element.id == new_parent_id)
      ).scalar_one_or_none()
      if new_parent is None:
        raise ElementNotFoundError(new_parent_id)
      if _would_create_cycle(element, new_parent):
        raise ElementCycleError(
          f"Cannot reparent {element.id} under {new_parent_id}: would create a cycle"
        )

    old_self_prefix = _self_path_prefix(element)
    new_depth, new_path = _hierarchy_for_parent(session, new_parent_id)
    new_self_prefix = f"{new_path}/{element.id}" if new_path else element.id
    depth_delta = new_depth - element.depth

    # Cascade to descendants: anything whose path starts with the old
    # self prefix (or equals it) gets rewritten. This is O(descendants)
    # which is fine for interactive authoring flows.
    if old_self_prefix != new_self_prefix or depth_delta != 0:
      descendants = (
        session.execute(
          select(Element).where(
            or_(
              Element.path == old_self_prefix,
              Element.path.like(f"{old_self_prefix}/%"),
            )
          )
        )
        .scalars()
        .all()
      )
      for desc in descendants:
        desc.depth = desc.depth + depth_delta
        if desc.path == old_self_prefix:
          desc.path = new_self_prefix
        else:
          # desc.path is old_self_prefix + "/..."
          suffix = desc.path[len(old_self_prefix) :]
          desc.path = new_self_prefix + suffix

    element.parent_id = new_parent_id
    element.depth = new_depth
    element.path = new_path

  for key, value in updates.items():
    setattr(element, key, value)

  session.flush()
  return _element_to_response(element)


def delete_element(session: Session, body: DeleteElementRequest) -> ElementResponse:
  """Soft delete — sets `is_active=false`.

  Historical line items referencing the element remain valid. New line
  items cannot use an inactive element (enforced by caller / validation
  layer, not the model itself).

  Raises `ElementNotFoundError` if the element does not exist.
  """
  element = session.execute(
    select(Element).where(Element.id == body.element_id)
  ).scalar_one_or_none()
  if element is None:
    raise ElementNotFoundError(body.element_id)
  assert_not_library_origin(element)

  element.is_active = False
  session.flush()
  return _element_to_response(element)
