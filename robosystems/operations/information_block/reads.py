"""Read operations for Information Blocks — envelope lookups + listing.

The two public reads are the source of truth for both the GraphQL field
(``informationBlock`` / ``informationBlocks``) and the hand-written MCP
tools (``get-information-block`` / ``list-information-blocks``). Shape
is defined once; agents, SDK callers, and the UI all see the same
envelope.

Unknown ``block_type`` filters raise :class:`ValueError`; new block
types register themselves in the registry and surface here without
changes to this module.
"""

from __future__ import annotations

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from robosystems.models.api.information_block import InformationBlockEnvelope
from robosystems.models.extensions import Association, Structure
from robosystems.models.extensions.roboledger import FactSet
from robosystems.operations.information_block import registry as registry_module
from robosystems.operations.information_block.envelope import DISCLOSURE_BLOCK_TYPE


def get_information_block(
  session: Session,
  structure_id: str,
  fact_set_id: str | None = None,
) -> InformationBlockEnvelope | None:
  """Fetch one block by id, dispatching via the structure's block_type.

  Returns ``None`` when the structure doesn't exist or its type isn't
  registered — callers map that to a GraphQL null / REST 404. When
  ``fact_set_id`` is provided the envelope is rehydrated from that
  specific FactSet instead of the latest one (used by Report Block
  rehydration to surface a frozen snapshot).
  """
  structure = session.get(Structure, structure_id)
  if structure is None:
    return None
  try:
    entry = registry_module.get(structure.block_type)
  except KeyError:
    # Existing rows whose block_type isn't a registered block type
    # surface as ``None`` rather than raising — the envelope just can't
    # be built.
    return None
  return entry.dispatch_build_envelope(session, structure_id, fact_set_id)


def get_information_block_for_fact_set(
  session: Session, fact_set_id: str
) -> InformationBlockEnvelope | None:
  """Rehydrate the Information Block envelope pinned to a FactSet.

  Looks up the FactSet's Structure, then dispatches the registered
  block-type handler with the explicit fact_set pin. Returns ``None``
  when the FactSet doesn't exist, has no Structure, or its
  ``block_type`` isn't a registered block type.

  Used by the Report Block read path: each ReportBlockItem pins a
  ``fact_set_id``; assembling the envelope for that item is exactly
  this lookup.
  """
  fact_set = session.get(FactSet, fact_set_id)
  if fact_set is None or fact_set.structure_id is None:
    return None
  return get_information_block(session, fact_set.structure_id, fact_set_id)


def list_information_blocks(
  session: Session,
  *,
  block_type: str | None = None,
  category: str | None = None,
  limit: int = 50,
  offset: int = 0,
  library_sentinel: bool = False,
) -> list[InformationBlockEnvelope]:
  """List blocks with optional block_type + category filters.

  When ``library_sentinel=True`` (request routed to the library
  ``graph_id='library'`` endpoint), results are restricted to block
  types whose registry entry declares ``surfaces_in_library=True`` —
  keeps canonical library structures from leaking as block envelopes
  until their block type is explicitly modeled.

  On a tenant graph_id (``library_sentinel=False``), no such filter
  applies.
  """
  # Resolve the candidate block_type id set based on the sentinel flag
  # and any explicit caller-supplied filter.
  if block_type is not None:
    try:
      entry = registry_module.get(block_type)
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
  # `list-information-blocks` call surfaces the tenant's own work first.
  # Without this, library-seeded statements (dozens per tenant graph)
  # swamp the default `limit=50` and a user browsing without filters
  # never sees their schedules.
  # A disclosure structure renders only when it owns a presentation arc; the
  # rs-gaap-disclosures package seeds ~17 arc-less `disclosures:*` identity rows
  # per tenant whose envelopes build to None. Excluding them at the SQL level
  # keeps them out of the LIMIT/OFFSET window so pages don't come back short.
  has_presentation_arc = (
    select(Association.id)
    .where(Association.structure_id == Structure.id)
    .where(Association.association_type == "presentation")
    .exists()
  )
  query = (
    select(Structure)
    .where(Structure.block_type.in_(candidate_ids))
    .where(Structure.is_active.is_(True))
    .where(or_(Structure.block_type != DISCLOSURE_BLOCK_TYPE, has_presentation_arc))
    .order_by(
      (Structure.created_by == "library-seeder").asc(),
      Structure.block_type,
      Structure.name,
    )
    .limit(limit)
    .offset(offset)
  )
  rows = session.execute(query).scalars().all()

  envelopes: list[InformationBlockEnvelope] = []
  for structure in rows:
    try:
      entry = registry_module.get(structure.block_type)
    except KeyError:
      continue
    envelope = entry.dispatch_build_envelope(session, structure.id)
    if envelope is not None:
      envelopes.append(envelope)
  return envelopes


__all__ = [
  "get_information_block",
  "get_information_block_for_fact_set",
  "list_information_blocks",
]
