"""Information Block reads (envelope lookup + listing), shared by GraphQL and
the MCP tools."""

from __future__ import annotations

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from robosystems.models.api.information_block import InformationBlockEnvelope
from robosystems.models.extensions import Association, Structure
from robosystems.models.extensions.roboledger import FactSet
from robosystems.operations.information_block import registry as registry_module
from robosystems.operations.information_block.envelope import DISCLOSURE_BLOCK_TYPE
from robosystems.operations.roboledger.entity_scope import find_entity_id


def _owner_entity_id_column():
  """The entity a block is its own, as a column over ``Structure``.

  Schedules and reconciliations carry it on the row; a forecast's is its
  lever set's (the ``custom`` set whose scenario is the block itself). NULL
  for a block shared by the group: the statements, the metric catalog, the
  library.
  """
  lever_entity = (
    select(FactSet.entity_id)
    .where(
      FactSet.structure_id == Structure.id,
      FactSet.factset_type == "custom",
      FactSet.scenario_id == Structure.id,
    )
    .order_by(FactSet.created_at.desc())
    .limit(1)
    .correlate(Structure)
    .scalar_subquery()
  )
  return func.coalesce(Structure.entity_id, lever_entity)


def owner_entity_id(session: Session, structure_id: str) -> str | None:
  """The entity whose own block this is, or None for a shared block."""
  return session.execute(
    select(_owner_entity_id_column()).where(Structure.id == structure_id)
  ).scalar()


def _requested_entity_id(
  session: Session,
  entity_id: str | None,
  scenario_id: str | None,
  library_sentinel: bool,
) -> str | None:
  """The entity a read asks for: the named one (validated), else the
  scenario's own, else the group parent. None on the library and on a graph
  with no entity yet, where no set is anyone's to filter."""
  if library_sentinel:
    return None
  if entity_id:
    return find_entity_id(session, entity_id)
  if scenario_id:
    scenario_owner = owner_entity_id(session, scenario_id)
    if scenario_owner is not None:
      return scenario_owner
  return find_entity_id(session)


def get_information_block(
  session: Session,
  structure_id: str,
  fact_set_id: str | None = None,
  scenario_id: str | None = None,
  series: bool = False,
  series_history: int | None = None,
  series_forecast: int | None = None,
  entity_id: str | None = None,
  library_sentinel: bool = False,
) -> InformationBlockEnvelope | None:
  """Fetch one block by id, dispatching via the structure's block_type.

  ``None`` when the structure doesn't exist or its type isn't registered.
  A ``fact_set_id`` pin overrides ``scenario_id`` and ``series``.
  ``scenario_id`` (a forecast block's id; ``None`` = actuals) and the
  ``series*`` options are ignored by block types they don't apply to.

  ``entity_id`` picks whose sets a block shared by the group shows; omitted,
  the scenario's entity, else the group parent. A block that is one
  entity's own always shows its owner's. A named entity outside the graph
  raises :class:`EntityNotInGraphError`.
  """
  structure = session.get(Structure, structure_id)
  if structure is None:
    return None
  try:
    entry = registry_module.get(structure.block_type)
  except KeyError:
    return None
  scope = _requested_entity_id(session, entity_id, scenario_id, library_sentinel)
  if not library_sentinel:
    scope = owner_entity_id(session, structure_id) or scope
  envelope = entry.dispatch_build_envelope(
    session,
    structure_id,
    fact_set_id,
    scenario_id=scenario_id,
    series=series,
    series_history=series_history,
    series_forecast=series_forecast,
    entity_id=scope,
  )
  return _stamp_entity(envelope, scope)


def _stamp_entity(
  envelope: InformationBlockEnvelope | None, entity_id: str | None
) -> InformationBlockEnvelope | None:
  """Name the entity whose books the envelope read; a pinned set names its
  own."""
  if envelope is None:
    return None
  if envelope.fact_set is not None and envelope.fact_set.entity_id:
    entity_id = envelope.fact_set.entity_id
  return envelope.model_copy(update={"entity_id": entity_id})


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
  entity_id: str | None = None,
) -> list[InformationBlockEnvelope]:
  """List blocks with optional block_type + category filters.

  ``library_sentinel`` (the ``library`` graph) restricts results to block
  types with ``surfaces_in_library``. ``scenario_id`` affects each
  envelope's binding, not which structures are listed.

  ``entity_id`` (omitted: the scenario's entity, else the group parent)
  lists the blocks shared by the group plus that entity's own, never another
  entity's schedules, reconciliations or forecasts; shared blocks show its
  sets.
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
  scope = _requested_entity_id(session, entity_id, scenario_id, library_sentinel)
  owner = _owner_entity_id_column()
  query = (
    select(Structure, owner)
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
  if scope is not None:
    query = query.where(or_(owner.is_(None), owner == scope))
  rows = session.execute(query).all()

  envelopes: list[InformationBlockEnvelope] = []
  for structure, owner_id in rows:
    try:
      entry = registry_module.get(structure.block_type)
    except KeyError:
      continue
    block_scope = owner_id or scope
    envelope = _stamp_entity(
      entry.dispatch_build_envelope(
        session, structure.id, None, scenario_id=scenario_id, entity_id=block_scope
      ),
      block_scope,
    )
    if envelope is not None:
      envelopes.append(envelope)
  return envelopes


__all__ = [
  "get_information_block",
  "get_information_block_for_fact_set",
  "list_information_blocks",
  "owner_entity_id",
]
