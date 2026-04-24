"""Shared apply-delta helpers for update-taxonomy-block handlers.

Each handler (CoA, reporting_extension, custom_ontology) gets a tiny
wrapper that calls these helpers in order. They're grouped here to
keep the three handlers free of duplicated delta-application code.

Library parent resolution for ``reporting_extension`` uses a
caller-supplied ``parent_taxonomy_id`` and the scoped library qname
lookup from ``reporting_extension._library_qname_lookup`` — passed in
as a callback to keep this module free of type-specific logic.
"""

from __future__ import annotations

from collections.abc import Callable

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.api.taxonomy_block import UpdateTaxonomyBlockRequest
from robosystems.models.extensions import (
  Association,
  Element,
  Rule,
  Structure,
  Taxonomy,
)
from robosystems.operations.taxonomy_block.rule_persistence import (
  persist_tenant_rules,
)


def apply_top_level_fields(
  taxonomy: Taxonomy,
  payload: UpdateTaxonomyBlockRequest,
) -> None:
  """Mutate the taxonomy row's simple fields in place."""
  if payload.name is not None:
    taxonomy.name = payload.name
  if payload.description is not None:
    taxonomy.description = payload.description
  if payload.version is not None:
    taxonomy.version = payload.version


def apply_elements_to_add(
  session: Session,
  taxonomy: Taxonomy,
  payload: UpdateTaxonomyBlockRequest,
  updated_by: str,
  *,
  element_factory: Callable[..., Element],
  library_parent_lookup: Callable[[set[str]], dict[str, str]] | None = None,
) -> dict[str, Element]:
  """Insert new elements using the handler-supplied factory.

  ``element_factory`` receives the request + derived qname + updated_by
  and returns an un-added Element row (so type-specific period/balance/
  classification rules stay in the handler).

  ``library_parent_lookup`` is called after pass 1 flush for any
  ``parent_ref`` that doesn't resolve locally. Returns a qname→id map.
  When None, unresolved parent_refs raise ValueError (custom_ontology
  + CoA have no library fallback).
  """
  if not payload.elements_to_add:
    return {}

  existing_elements_by_qname: dict[str, Element] = {
    str(e.qname): e
    for e in session.execute(select(Element).where(Element.taxonomy_id == taxonomy.id))
    .scalars()
    .all()
    if e.qname
  }

  new_elements_by_qname: dict[str, Element] = {}
  parent_refs_for_resolution: dict[str, str | None] = {}

  for req in payload.elements_to_add:
    element, qname = element_factory(req, taxonomy, updated_by)
    session.add(element)
    new_elements_by_qname[qname] = element
    parent_refs_for_resolution[qname] = req.parent_ref

  session.flush()

  library_qname_to_id: dict[str, str] = {}
  if library_parent_lookup is not None:
    needed_library_qnames = {
      ref
      for ref in parent_refs_for_resolution.values()
      if ref
      and ref not in new_elements_by_qname
      and ref not in existing_elements_by_qname
    }
    if needed_library_qnames:
      library_qname_to_id = library_parent_lookup(needed_library_qnames)

  for qname, parent_ref in parent_refs_for_resolution.items():
    if not parent_ref:
      continue
    local = new_elements_by_qname.get(parent_ref) or existing_elements_by_qname.get(
      parent_ref
    )
    if local is not None:
      new_elements_by_qname[qname].parent_id = local.id
      continue
    library_id = library_qname_to_id.get(parent_ref)
    if library_id is None:
      raise ValueError(
        f"element {qname!r} parent_ref {parent_ref!r} did not resolve in "
        f"the taxonomy or parent library."
      )
    new_elements_by_qname[qname].parent_id = library_id

  return new_elements_by_qname


def apply_structures_to_add(
  session: Session,
  taxonomy: Taxonomy,
  payload: UpdateTaxonomyBlockRequest,
  updated_by: str,
) -> dict[str, Structure]:
  """Insert new structures; return name→Structure lookup of just the new ones."""
  new_structures_by_name: dict[str, Structure] = {}
  for req in payload.structures_to_add:
    structure = Structure(
      name=req.name,
      description=req.description,
      structure_type=req.structure_type,
      taxonomy_id=taxonomy.id,
      is_active=True,
      metadata_=dict(req.metadata),
      created_by=updated_by,
    )
    session.add(structure)
    new_structures_by_name[req.name] = structure
  if new_structures_by_name:
    session.flush()
  return new_structures_by_name


def apply_associations_to_add(
  session: Session,
  taxonomy: Taxonomy,
  new_elements_by_qname: dict[str, Element],
  new_structures_by_name: dict[str, Structure],
  payload: UpdateTaxonomyBlockRequest,
  updated_by: str,
  *,
  library_ref_lookup: Callable[[set[str]], dict[str, str]] | None = None,
) -> None:
  """Insert new associations, resolving refs via current-state symbol tables."""
  if not payload.associations_to_add:
    return

  existing_elements_by_qname: dict[str, str] = {
    str(e.qname): str(e.id)
    for e in session.execute(select(Element).where(Element.taxonomy_id == taxonomy.id))
    .scalars()
    .all()
    if e.qname
  }
  existing_structures_by_name: dict[str, str] = {
    str(s.name): str(s.id)
    for s in session.execute(
      select(Structure).where(Structure.taxonomy_id == taxonomy.id)
    )
    .scalars()
    .all()
  }

  library_refs: set[str] = set()
  if library_ref_lookup is not None:
    for req in payload.associations_to_add:
      if (
        req.from_ref not in new_elements_by_qname
        and req.from_ref not in existing_elements_by_qname
      ):
        library_refs.add(req.from_ref)
      if (
        req.to_ref not in new_elements_by_qname
        and req.to_ref not in existing_elements_by_qname
      ):
        library_refs.add(req.to_ref)
  library_ref_to_id: dict[str, str] = (
    library_ref_lookup(library_refs)
    if library_refs and library_ref_lookup is not None
    else {}
  )

  def _resolve_element(ref: str) -> str | None:
    local_new = new_elements_by_qname.get(ref)
    if local_new is not None:
      return local_new.id
    return existing_elements_by_qname.get(ref) or library_ref_to_id.get(ref)

  def _resolve_structure(name: str) -> str | None:
    new = new_structures_by_name.get(name)
    if new is not None:
      return new.id
    return existing_structures_by_name.get(name)

  for req in payload.associations_to_add:
    structure_id = _resolve_structure(req.structure_ref)
    if structure_id is None:
      raise ValueError(
        f"association structure_ref {req.structure_ref!r} did not resolve."
      )
    from_id = _resolve_element(req.from_ref)
    to_id = _resolve_element(req.to_ref)
    if from_id is None:
      raise ValueError(f"association from_ref {req.from_ref!r} did not resolve.")
    if to_id is None:
      raise ValueError(f"association to_ref {req.to_ref!r} did not resolve.")
    assoc = Association(
      structure_id=structure_id,
      from_element_id=from_id,
      to_element_id=to_id,
      association_type=req.association_type,
      order_value=req.order_value,
      arcrole=req.arcrole,
      weight=req.weight,
      metadata_=dict(req.metadata),
      created_by=updated_by,
    )
    session.add(assoc)


def apply_rules_to_remove(
  session: Session,
  taxonomy: Taxonomy,
  payload: UpdateTaxonomyBlockRequest,
) -> None:
  """Delete rules by id — validator already rejected auto-origin."""
  if not payload.rules_to_remove:
    return
  session.execute(Rule.__table__.delete().where(Rule.id.in_(payload.rules_to_remove)))


def apply_rules_to_add(
  session: Session,
  taxonomy: Taxonomy,
  payload: UpdateTaxonomyBlockRequest,
  new_elements_by_qname: dict[str, Element],
  new_structures_by_name: dict[str, Structure],
  updated_by: str,
) -> None:
  """Persist new tenant rules. Resolves refs against current + new state."""
  if not payload.rules_to_add:
    return

  elements_by_qname: dict[str, Element] = dict(new_elements_by_qname)
  for e in (
    session.execute(select(Element).where(Element.taxonomy_id == taxonomy.id))
    .scalars()
    .all()
  ):
    if e.qname and e.qname not in elements_by_qname:
      elements_by_qname[e.qname] = e

  structures_by_name: dict[str, Structure] = dict(new_structures_by_name)
  for s in (
    session.execute(select(Structure).where(Structure.taxonomy_id == taxonomy.id))
    .scalars()
    .all()
  ):
    if s.name not in structures_by_name:
      structures_by_name[s.name] = s

  persist_tenant_rules(
    session,
    taxonomy,
    payload.rules_to_add,
    elements_by_qname,
    structures_by_name,
    updated_by,
  )


__all__ = [
  "apply_associations_to_add",
  "apply_elements_to_add",
  "apply_rules_to_add",
  "apply_rules_to_remove",
  "apply_structures_to_add",
  "apply_top_level_fields",
]
