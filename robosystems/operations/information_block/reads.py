"""Information Block reads (envelope lookup + listing), shared by GraphQL and
the MCP tools."""

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
  scenario_id: str | None = None,
  series: bool = False,
  series_history: int | None = None,
  series_forecast: int | None = None,
) -> InformationBlockEnvelope | None:
  """Fetch one block by id, dispatching via the structure's block_type.

  ``None`` when the structure doesn't exist or its type isn't registered.
  A ``fact_set_id`` pin overrides ``scenario_id`` and ``series``.
  ``scenario_id`` (a forecast block's id; ``None`` = actuals) and the
  ``series*`` options are ignored by block types they don't apply to.
  """
  structure = session.get(Structure, structure_id)
  if structure is None:
    return None
  try:
    entry = registry_module.get(structure.block_type)
  except KeyError:
    return None
  return entry.dispatch_build_envelope(
    session,
    structure_id,
    fact_set_id,
    scenario_id=scenario_id,
    series=series,
    series_history=series_history,
    series_forecast=series_forecast,
  )


def get_information_block_for_fact_set(
  session: Session, fact_set_id: str
) -> InformationBlockEnvelope | None:
  """Rehydrate the Information Block envelope pinned to a FactSet, or ``None``."""
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
  scenario_id: str | None = None,
) -> list[InformationBlockEnvelope]:
  """List blocks with optional block_type + category filters.

  ``library_sentinel`` (the ``library`` graph) restricts results to block
  types with ``surfaces_in_library``. ``scenario_id`` affects each
  envelope's binding, not which structures are listed.
  """
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

  # Tenant-authored blocks sort first so library statements don't swamp the
  # default page. Arc-less disclosure rows build to None, so they're
  # excluded in SQL to keep pages full.
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
    envelope = entry.dispatch_build_envelope(
      session, structure.id, None, scenario_id=scenario_id
    )
    if envelope is not None:
      envelopes.append(envelope)
  return envelopes


__all__ = [
  "get_information_block",
  "get_information_block_for_fact_set",
  "list_information_blocks",
]
