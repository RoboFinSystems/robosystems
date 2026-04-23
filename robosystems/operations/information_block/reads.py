"""Read operations for Information Blocks — envelope lookups + listing.

The two public reads are the source of truth for both the GraphQL field
(``informationBlock`` / ``informationBlocks``) and the hand-written MCP
tools (``get-information-block`` / ``list-information-blocks``). Shape
is defined once; agents, SDK callers, and the UI all see the same
envelope.

Phase a only knows about ``block_type='schedule'``; unknown block_type
filters raise :class:`ValueError`. Future phases add more entries to
the registry — no changes needed here.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.api.information_block import InformationBlockEnvelope
from robosystems.models.extensions import Structure
from robosystems.operations.information_block import registry as registry_module


def get_information_block(
  session: Session, structure_id: str
) -> InformationBlockEnvelope | None:
  """Fetch one block by id, dispatching via the structure's block_type.

  Returns ``None`` when the structure doesn't exist or its type isn't
  registered — callers map that to a GraphQL null / REST 404.
  """
  structure = session.get(Structure, structure_id)
  if structure is None:
    return None
  try:
    entry = registry_module.get(structure.structure_type)
  except KeyError:
    # Existing rows with a structure_type that isn't yet modeled as a
    # block type (e.g. 'balance_sheet' before Phase b) surface as
    # None rather than raising — the envelope just can't be built.
    return None
  return entry.dispatch_build_envelope(session, structure_id)


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

  query = (
    select(Structure)
    .where(Structure.structure_type.in_(candidate_ids))
    .where(Structure.is_active.is_(True))
    .order_by(Structure.structure_type, Structure.name)
    .limit(limit)
    .offset(offset)
  )
  rows = session.execute(query).scalars().all()

  envelopes: list[InformationBlockEnvelope] = []
  for structure in rows:
    try:
      entry = registry_module.get(structure.structure_type)
    except KeyError:
      continue
    envelope = entry.dispatch_build_envelope(session, structure.id)
    if envelope is not None:
      envelopes.append(envelope)
  return envelopes


__all__ = [
  "get_information_block",
  "list_information_blocks",
]
