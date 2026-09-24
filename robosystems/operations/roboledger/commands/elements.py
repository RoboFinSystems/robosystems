"""Element CRUD primitives.

Hierarchy is a materialized path: a root has depth 0 and path ``""``, a child
``"<root_id>"``, a grandchild ``"<root_id>/<parent_id>"``. Reparenting rewrites
every descendant's path.
"""

from __future__ import annotations

import re

from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from robosystems.db.integrity import violates
from robosystems.models.api.extensions.taxonomies import (
  CreateElementRequest,
  DeleteElementRequest,
  ElementResponse,
  UpdateElementRequest,
)
from robosystems.models.extensions import (
  Element,
  ElementTrait,
  Taxonomy,
  Trait,
)
from robosystems.operations.library.reads import efs_trait_by_element
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
  "ElementQNameConflictError",
  "LibraryImmutableError",
  "TaxonomyNotFoundError",
  "create_element",
  "delete_element",
  "update_element",
]


class ElementNotFoundError(LookupError):
  """Distinct from `taxonomies.ElementNotFoundError`, which carries a mapping
  ``side``."""

  def __init__(self, element_id: str) -> None:
    super().__init__(f"Element not found: {element_id}")
    self.element_id = element_id


class ElementCycleError(ValueError):
  """An update would create a cycle in the element hierarchy."""


class ElementQNameConflictError(ValueError):
  """Two names collapse to the same derived qname ("Owner's Equity" and
  "Owners Equity"); retry with an explicit ``qname``."""

  def __init__(self, qname: str) -> None:
    super().__init__(f"Element qname already in use: {qname}")
    self.qname = qname


def _element_to_response(row: Element, trait: str | None = None) -> ElementResponse:
  return ElementResponse(
    id=row.id,
    code=row.code,
    name=row.name,
    description=row.description,
    qname=row.qname,
    namespace=row.namespace,
    trait=trait,
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
  """The path prefix every descendant of ``element`` carries."""
  return f"{element.path}/{element.id}" if element.path else element.id


# Balance-sheet (stock) classifications are instants; everything else is a flow.
_INSTANT_CLASSIFICATIONS = frozenset(
  {
    "asset",
    "contraAsset",
    "liability",
    "contraLiability",
    "equity",
    "contraEquity",
    "temporaryEquity",
  }
)


def _derive_period_type(classification: str | None, explicit: str) -> str:
  """'instant' for stock classifications, overriding ``explicit``; otherwise
  ``explicit``."""
  if classification in _INSTANT_CLASSIFICATIONS:
    return "instant"
  return explicit


def _derive_qname(name: str, source: str, namespace: str | None) -> str:
  """'Accounts Receivable' → 'native:AccountsReceivable'; punctuation is
  stripped so the local name is a valid NCName."""
  prefix = namespace or source
  local = re.sub(r"[^\w\s]", "", name)
  local = "".join(word.capitalize() for word in local.split())
  return f"{prefix}:{local}"


def _would_create_cycle(element: Element, new_parent: Element) -> bool:
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
  """Create an element within a tenant-origin taxonomy.

  Not mounted on any surface; tenant element writes go through the taxonomy
  block operations.

  Raises `TaxonomyNotFoundError`, `ElementNotFoundError` (parent), or
  `ElementQNameConflictError`.
  """
  taxonomy = session.execute(
    select(Taxonomy).where(Taxonomy.id == body.taxonomy_id)
  ).scalar_one_or_none()
  if taxonomy is None:
    raise TaxonomyNotFoundError(body.taxonomy_id)
  assert_not_library_origin(taxonomy)

  depth, path = _hierarchy_for_parent(session, body.parent_id)

  resolved_period_type = _derive_period_type(body.trait, body.period_type)
  resolved_qname = body.qname or _derive_qname(body.name, body.source, body.namespace)

  element = Element(
    id=generate_prefixed_ulid("elem"),
    code=body.code,
    name=body.name,
    description=body.description,
    balance_type=body.balance_type,
    period_type=resolved_period_type,
    element_type=body.element_type,
    is_abstract=body.is_abstract,
    is_monetary=body.is_monetary,
    parent_id=body.parent_id,
    depth=depth,
    path=path,
    taxonomy_id=body.taxonomy_id,
    source=body.source,
    currency=body.currency,
    qname=resolved_qname,
    namespace=body.namespace,
    external_id=body.external_id,
    external_source=body.external_source,
    is_active=True,
    created_by=created_by,
  )
  try:
    # Under a savepoint: a qname collision must not roll back the caller's
    # whole transaction — only this insert.
    with session.begin_nested():
      session.add(element)
      session.flush()
  except IntegrityError as exc:
    if violates(exc, "idx_elements_qname"):
      raise ElementQNameConflictError(resolved_qname) from exc
    raise

  _assign_efs_classification(session, element.id, body.trait)

  # Liquidity narrows rs-gaap mapping candidates within the EFS bucket.
  if body.liquidity:
    _assign_trait(session, element.id, "liquidity", body.liquidity)

  return _element_to_response(element, body.trait)


def _assign_trait(
  session: Session, element_id: str, category: str, identifier: str
) -> None:
  """Link an element to the library trait ``(category, identifier)``; a no-op
  when no such trait exists."""
  trait_row = session.execute(
    select(Trait).where(
      Trait.category == category,
      Trait.identifier == identifier,
    )
  ).scalar_one_or_none()
  if trait_row is None:
    return
  session.add(
    ElementTrait(
      element_id=element_id,
      trait_id=trait_row.id,
      is_primary=True,
    )
  )
  session.flush()


def _assign_efs_classification(
  session: Session, element_id: str, efs_identifier: str
) -> None:
  _assign_trait(session, element_id, "elementsOfFinancialStatements", efs_identifier)


def update_element(session: Session, body: UpdateElementRequest) -> ElementResponse:
  """Update an element: omitted fields are unchanged, explicit nulls apply
  (``parent_id=None`` makes it a root). Reparenting rewrites descendants.

  Not mounted on any surface.

  Raises `ElementNotFoundError` (element or new parent) or `ElementCycleError`.
  """
  element = session.execute(
    select(Element).where(Element.id == body.element_id)
  ).scalar_one_or_none()
  if element is None:
    raise ElementNotFoundError(body.element_id)
  assert_not_library_origin(element)

  updates = body.model_dump(exclude_unset=True)
  updates.pop("element_id", None)
  # trait is an element_traits row, not a column; reconciled below.
  new_classification: str | None = updates.pop("trait", None)

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
          suffix = desc.path[len(old_self_prefix) :]
          desc.path = new_self_prefix + suffix

    element.parent_id = new_parent_id
    element.depth = new_depth
    element.path = new_path

  for key, value in updates.items():
    setattr(element, key, value)

  session.flush()
  if new_classification is not None:
    _reassign_efs_classification(session, element.id, new_classification)
  efs = efs_trait_by_element(session, [element.id]).get(element.id)
  return _element_to_response(element, efs)


def _reassign_efs_classification(
  session: Session, element_id: str, efs_identifier: str
) -> None:
  """Make ``efs_identifier`` the element's primary EFS trait. The old primary
  is demoted, not deleted, as an audit trail. No-op if the trait is absent."""
  target = session.execute(
    select(Trait).where(
      Trait.category == "elementsOfFinancialStatements",
      Trait.identifier == efs_identifier,
    )
  ).scalar_one_or_none()
  if target is None:
    return

  existing = (
    session.execute(
      select(ElementTrait)
      .join(
        Trait,
        Trait.id == ElementTrait.trait_id,
      )
      .where(
        ElementTrait.element_id == element_id,
        Trait.category == "elementsOfFinancialStatements",
      )
    )
    .scalars()
    .all()
  )
  found_target = False
  for row in existing:
    if row.trait_id == target.id:
      row.is_primary = True
      found_target = True
    else:
      row.is_primary = False

  if not found_target:
    session.add(
      ElementTrait(
        element_id=element_id,
        trait_id=target.id,
        is_primary=True,
      )
    )
  session.flush()


def delete_element(session: Session, body: DeleteElementRequest) -> ElementResponse:
  """Soft delete (``is_active=false``); historical line items stay valid. Not
  mounted on any surface.

  Raises `ElementNotFoundError`.
  """
  element = session.execute(
    select(Element).where(Element.id == body.element_id)
  ).scalar_one_or_none()
  if element is None:
    raise ElementNotFoundError(body.element_id)
  assert_not_library_origin(element)

  element.is_active = False
  session.flush()
  efs = efs_trait_by_element(session, [element.id]).get(element.id)
  return _element_to_response(element, efs)
