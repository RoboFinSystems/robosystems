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

import re

from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

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
  """Raised when an element is not found by id.

  Distinct from `taxonomies.ElementNotFoundError`, which carries a
  `side` field ("source" | "target") for mapping-association context.
  """

  def __init__(self, element_id: str) -> None:
    super().__init__(f"Element not found: {element_id}")
    self.element_id = element_id


class ElementCycleError(ValueError):
  """Raised when an update would create a cycle in the element hierarchy."""


class ElementQNameConflictError(ValueError):
  """Raised when a new element's qname collides with an existing element.

  Happens when two human-readable names CamelCase-collapse to the same
  qname (e.g., "Owner's Equity" and "Owners Equity" both derive
  `native:OwnersEquity`). The caller should retry with an explicit
  `qname` on the create request.
  """

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


# Balance-sheet classifications are stock concepts (point-in-time → instant).
# Everything else (revenue, expense, gain, loss, investmentByOwners, etc.) is
# a flow / duration concept.
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
  """Return 'instant' for balance-sheet stock concepts, `explicit` otherwise.

  Stock classifications (asset/liability/equity and their contras, plus
  temporaryEquity) are always forced to 'instant' — the caller-supplied
  `explicit` value is ignored for these. Every other classification passes
  `explicit` through unchanged. The request model defaults `explicit` to
  'duration', so non-stock concepts get 'duration' when the caller omits
  `period_type`.

  There is no caller override for stock classifications; the accounting
  rule is enforced unconditionally. If a balance-sheet element genuinely
  needs `period_type='duration'`, it must be written directly rather than
  through this helper.
  """
  if classification in _INSTANT_CLASSIFICATIONS:
    return "instant"
  return explicit


def _derive_qname(name: str, source: str, namespace: str | None) -> str:
  """Derive a namespace-prefixed qname from a human-readable account name.

  Converts 'Accounts Receivable' → 'native:AccountsReceivable'.
  Strips non-alphanumeric characters (apostrophes, ampersands, slashes,
  hyphens) before converting to CamelCase so names like "Owner's Equity"
  and "Salaries & Wages" produce valid XML NCNames.
  """
  prefix = namespace or source
  # Strip non-word chars, title-case each word, join
  local = re.sub(r"[^\w\s]", "", name)
  local = "".join(word.capitalize() for word in local.split())
  return f"{prefix}:{local}"


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

  Internal primitive, not mounted on any surface. Tenant element writes go
  through the taxonomy block surface (``create-taxonomy-block`` /
  ``update-taxonomy-block``), which has its own element path; this stays as
  the single-element primitive for scripts and future callers.

  Raises `TaxonomyNotFoundError` if `body.taxonomy_id` does not exist,
  `ElementNotFoundError` if `body.parent_id` is set but the parent does not
  exist, or `ElementQNameConflictError` when the derived qname is taken.
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
    # Partial unique index `idx_elements_qname` fires when two CamelCased
    # account names collapse to the same qname. Translate to a domain
    # exception so the API layer can return 409 instead of 500.
    if "idx_elements_qname" in str(exc.orig):
      raise ElementQNameConflictError(resolved_qname) from exc
    raise

  # Assign the primary FASB elementsOfFinancialStatements trait so that
  # downstream report renderers (which JOIN through element_traits)
  # see the native account's economic nature.
  _assign_efs_classification(session, element.id, body.trait)

  # Optional liquidity (current / noncurrent) — narrows rs-gaap mapping
  # candidates within the EFS bucket. Only meaningful for assets and
  # liabilities; `_assign_trait` no-ops if the identifier has no trait row.
  if body.liquidity:
    _assign_trait(session, element.id, "liquidity", body.liquidity)

  return _element_to_response(element, body.trait)


def _assign_trait(
  session: Session, element_id: str, category: str, identifier: str
) -> None:
  """Link a native element to the matching library trait by (category, identifier).

  No-op if the Trait row is missing (e.g., in tests that don't seed the
  library, or a liquidity identifier passed for a non-asset/liability).
  """
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
  """Link a native element to the matching FASB EFS trait."""
  _assign_trait(session, element_id, "elementsOfFinancialStatements", efs_identifier)


def update_element(session: Session, body: UpdateElementRequest) -> ElementResponse:
  """Update mutable fields on an element.

  Internal helper. Tenant writes go through the taxonomy block surface
  (``create-taxonomy-block`` / ``update-taxonomy-block``); this is
  invoked indirectly by those operations.

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
  # trait isn't a column on elements — it's an element_traits
  # row. Pop it here and reconcile the junction after the scalar updates land.
  new_classification: str | None = updates.pop("trait", None)

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
  if new_classification is not None:
    _reassign_efs_classification(session, element.id, new_classification)
  efs = efs_trait_by_element(session, [element.id]).get(element.id)
  return _element_to_response(element, efs)


def _reassign_efs_classification(
  session: Session, element_id: str, efs_identifier: str
) -> None:
  """Replace the primary EFS trait assignment for an element.

  Correction path for misclassified native accounts. Demotes any existing
  primary EFS row (same junction kept as an audit trail but flipped to
  is_primary=False) and inserts — or re-promotes — the chosen identifier.
  No-op if the target Trait row isn't in the library (shouldn't
  happen in prod; guarded so tests that skip the library seed still pass).
  """
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
  """Soft delete — sets `is_active=false`.

  Internal helper. Tenant writes go through the taxonomy block surface
  (``delete-taxonomy-block``); this is invoked indirectly by that
  operation.

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
  efs = efs_trait_by_element(session, [element.id]).get(element.id)
  return _element_to_response(element, efs)
