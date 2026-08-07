"""Read operations for Taxonomy Blocks — envelope lookups + listing.

The two public reads are the source of truth for the GraphQL fields
(``taxonomyBlock`` / ``taxonomyBlocks``). Shape is defined once;
agents, SDK callers, and the UI all see the same envelope that the
create/update handlers emit.

Rows whose ``taxonomy_type`` isn't registered (notably ``'mapping'``)
surface as ``None`` — mapping taxonomies have no envelope representation;
they're addressed through the mapping association surface.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.api.taxonomy_block import TaxonomyBlockEnvelope
from robosystems.models.extensions import Taxonomy
from robosystems.operations.taxonomy_block import registry as registry_module


def get_taxonomy_block(
  session: Session, taxonomy_id: str
) -> TaxonomyBlockEnvelope | None:
  """Fetch one taxonomy block by id, dispatching via its ``taxonomy_type``.

  Returns ``None`` when the taxonomy doesn't exist or its type isn't
  registered — callers map that to a GraphQL null / REST 404.
  """
  taxonomy = session.get(Taxonomy, taxonomy_id)
  if taxonomy is None:
    return None
  try:
    entry = registry_module.get(taxonomy.taxonomy_type)
  except KeyError:
    return None
  return entry.dispatch_build_envelope(session, taxonomy_id)


def list_taxonomy_blocks(
  session: Session,
  *,
  taxonomy_type: str | None = None,
  parent_taxonomy_id: str | None = None,
  category: str | None = None,
  limit: int = 50,
  offset: int = 0,
  library_sentinel: bool = False,
) -> list[TaxonomyBlockEnvelope]:
  """List taxonomy blocks with optional filters + pagination.

  When ``library_sentinel=True`` (request routed to the library
  ``graph_id='library'`` endpoint), results are restricted to entries
  whose registry declares ``surfaces_in_library=True`` — currently just
  ``reporting_standard``. On a tenant graph_id no such filter applies.

  Filters compose with AND. ``taxonomy_type`` that isn't registered
  raises :class:`ValueError` — the caller supplied a bad filter.
  """
  if taxonomy_type is not None:
    try:
      entry = registry_module.get(taxonomy_type)
    except KeyError as exc:
      raise ValueError(str(exc)) from exc
    if library_sentinel and not entry.surfaces_in_library:
      return []
    if category is not None and entry.category != category:
      return []
    candidate_ids: list[str] = [entry.id]
  else:
    entries = registry_module.list_registered()
    if library_sentinel:
      entries = [e for e in entries if e.surfaces_in_library]
    if category is not None:
      entries = [e for e in entries if e.category == category]
    candidate_ids = [e.id for e in entries]

  if not candidate_ids:
    return []

  # Tenant-authored blocks sort before library-seeded ones so a default
  # call surfaces the tenant's own work first. Without this, library
  # reporting_standard rows (fac, rs-gaap) would swamp the default
  # limit=50 and hide tenant CoA / schedule / custom ontology blocks.
  query = (
    select(Taxonomy)
    .where(Taxonomy.taxonomy_type.in_(candidate_ids))
    .where(Taxonomy.is_active.is_(True))
    .order_by(
      (Taxonomy.created_by == "library-seeder").asc(),
      Taxonomy.taxonomy_type,
      Taxonomy.name,
    )
    .limit(limit)
    .offset(offset)
  )
  if parent_taxonomy_id is not None:
    query = query.where(Taxonomy.parent_taxonomy_id == parent_taxonomy_id)

  rows = session.execute(query).scalars().all()

  envelopes: list[TaxonomyBlockEnvelope] = []
  for taxonomy in rows:
    try:
      entry = registry_module.get(taxonomy.taxonomy_type)
    except KeyError:
      continue
    envelope = entry.dispatch_build_envelope(session, taxonomy.id)
    if envelope is not None:
      envelopes.append(envelope)
  return envelopes


__all__ = [
  "get_taxonomy_block",
  "list_taxonomy_blocks",
]
