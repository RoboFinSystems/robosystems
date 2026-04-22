"""Handlers for ``block_type='schedule'`` — the first Information Block type.

Two public handlers bind the generic construction machinery to the
existing Schedule POC:

- :func:`create` delegates to ``cmd_create_schedule`` and returns the
  new structure's id.
- :func:`build_envelope` reloads the Structure + its bundled atoms and
  packs them into the typed :class:`InformationBlockEnvelope`.

Schedule is the reference implementation of the **declarative**
construction mode (user declares the mechanics + seed params; the
system generates atoms). Phase b's Statement block type will be the
reference **compositional** mode; Phase η's Metric block type will be
the reference **derivative** mode.
"""

from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.schedules import (
  CreateScheduleRequest,
  DeleteScheduleRequest,
  EntryTemplateRequest,
  ScheduleMetadataRequest,
  UpdateScheduleRequest,
)
from robosystems.models.api.information_block import (
  ArtifactResponse,
  InformationBlockEnvelope,
  InformationModelResponse,
  ScheduleMechanics,
)
from robosystems.models.extensions import Association, Element, Structure, Taxonomy
from robosystems.models.extensions.roboledger import Entry, Fact
from robosystems.operations.information_block.envelope import (
  association_to_connection,
  element_to_lite,
  fact_to_lite,
  load_rules_for_structure,
)
from robosystems.operations.roboledger.commands.schedules import (
  create_schedule as cmd_create_schedule,
)
from robosystems.operations.roboledger.commands.schedules import (
  delete_schedule as cmd_delete_schedule,
)
from robosystems.operations.roboledger.commands.schedules import (
  update_schedule as cmd_update_schedule,
)

# Shared display identity — values mirrored in the registry entry. Kept
# here as constants so the envelope builder can fill the wire shape
# without a registry re-import (avoids a circular dependency —
# registry.py imports these handlers).
SCHEDULE_BLOCK_TYPE = "schedule"
SCHEDULE_DISPLAY_NAME = "Schedule"
SCHEDULE_CATEGORY = "Close"


def create(
  session: Session,
  payload: CreateScheduleRequest,
  created_by: str,
) -> str:
  """Create a schedule via the existing command, return its structure_id.

  The generic ``cmd_create_information_block`` dispatcher validates the
  incoming opaque payload against this block type's registered
  ``create_request_model`` (:class:`CreateScheduleRequest`) before
  invoking this handler, so the argument is already shape-correct.
  """
  response = cmd_create_schedule(session, payload, created_by=created_by)
  return response.structure_id


def update(
  session: Session,
  payload: UpdateScheduleRequest,
  updated_by: str,
) -> str:
  """Update a schedule via the existing command, return its structure_id.

  The underlying ``cmd_update_schedule`` does not take an
  ``updated_by`` parameter today; the dispatch signature accepts it for
  consistency with :func:`create` and will wire through once audit
  tracking on schedule updates is added.
  """
  response = cmd_update_schedule(session, payload)
  return response.structure_id


def delete(
  session: Session,
  payload: DeleteScheduleRequest,
  deleted_by: str,
) -> str:
  """Delete a schedule via the existing command, return the deleted id.

  The underlying ``cmd_delete_schedule`` returns ``{"deleted": True}``;
  we surface the structure_id from the input payload for the unified
  response envelope.
  """
  cmd_delete_schedule(session, payload)
  return payload.structure_id


def _load_schedule_mechanics(
  structure: Structure, periods_with_entries: int
) -> ScheduleMechanics:
  """Build the typed Schedule mechanics from a Structure row.

  Prefers the Phase δ typed column (``artifact_mechanics``). Falls back
  to the legacy ``metadata_`` JSONB shape for Schedule rows that
  pre-date the Phase δ backfill — both paths produce the same
  :class:`ScheduleMechanics` envelope arm.
  """
  mechanics_blob = structure.artifact_mechanics
  if mechanics_blob:
    return ScheduleMechanics.model_validate(
      {**mechanics_blob, "periods_with_entries": periods_with_entries}
    )

  meta = structure.metadata_ or {}
  raw_schedule_meta = meta.get("schedule_metadata")
  raw_entry_template = meta.get("entry_template")
  if not raw_entry_template:
    raise ValueError(
      f"Schedule structure {structure.id!r} has no entry_template in metadata_ "
      "and no artifact_mechanics — the row may be corrupted or from an unsupported "
      "pre-δ path. Check the Schedule creation path and the Phase δ backfill."
    )
  return ScheduleMechanics(
    kind="closing_entry_generator",
    entry_template=EntryTemplateRequest.model_validate(raw_entry_template),
    schedule_metadata=(
      ScheduleMetadataRequest.model_validate(raw_schedule_meta)
      if raw_schedule_meta
      else None
    ),
    periods_with_entries=periods_with_entries,
  )


def build_envelope(
  session: Session, structure_id: str
) -> InformationBlockEnvelope | None:
  """Reload a schedule Structure and pack its Information Block envelope.

  Returns ``None`` when the structure doesn't exist or isn't a schedule,
  so the generic reader can cleanly distinguish misses from errors.

  Phase δ reads mechanics from the typed ``artifact_mechanics`` column,
  falling back to legacy ``metadata_`` JSONB for rows that pre-date the
  backfill.
  """
  structure = session.get(Structure, structure_id)
  if structure is None or structure.structure_type != SCHEDULE_BLOCK_TYPE:
    return None

  # Runtime state: how many closing entries (draft OR posted) trace back
  # to this schedule. Phase ζ makes this derivable from typed FactSets
  # and this counter gets removed.
  periods_with_entries = (
    session.execute(
      select(func.count(Entry.id)).where(
        Entry.source_structure_id == structure_id,
        Entry.status.in_(("draft", "posted")),
      )
    ).scalar()
    or 0
  )

  mechanics = _load_schedule_mechanics(structure, periods_with_entries)

  taxonomy_name = session.execute(
    select(Taxonomy.name).where(Taxonomy.id == structure.taxonomy_id)
  ).scalar()

  associations = (
    session.execute(select(Association).where(Association.structure_id == structure_id))
    .scalars()
    .all()
  )

  element_ids = {a.to_element_id for a in associations} | {
    a.from_element_id for a in associations
  }
  if element_ids:
    elements = (
      session.execute(select(Element).where(Element.id.in_(element_ids)))
      .scalars()
      .all()
    )
  else:
    elements = []

  # Schedules publish only in-scope facts — historical facts were
  # already reflected in opening balances and shouldn't surface as
  # envelope data (they'd confuse agents into re-drafting closed work).
  facts = (
    session.execute(
      select(Fact).where(
        Fact.structure_id == structure_id,
        Fact.fact_scope == "in_scope",
      )
    )
    .scalars()
    .all()
  )

  rules = load_rules_for_structure(
    session,
    structure_id,
    element_ids=list(element_ids),
    association_ids=[a.id for a in associations],
  )

  return InformationBlockEnvelope(
    id=structure.id,
    block_type=SCHEDULE_BLOCK_TYPE,
    name=structure.name,
    display_name=SCHEDULE_DISPLAY_NAME,
    category=SCHEDULE_CATEGORY,
    taxonomy_id=structure.taxonomy_id,
    taxonomy_name=taxonomy_name,
    information_model=InformationModelResponse(
      concept_arrangement=structure.concept_arrangement or "roll_forward",
      member_arrangement=structure.member_arrangement,
    ),
    artifact=ArtifactResponse(
      topic=structure.description,
      parenthetical_note=structure.parenthetical_note,
      template=None,
      mechanics=mechanics,
    ),
    elements=[element_to_lite(e) for e in elements],
    connections=[association_to_connection(a) for a in associations],
    facts=[fact_to_lite(f) for f in facts],
    rules=rules,
  )


__all__ = [
  "SCHEDULE_BLOCK_TYPE",
  "SCHEDULE_CATEGORY",
  "SCHEDULE_DISPLAY_NAME",
  "build_envelope",
  "create",
  "delete",
  "update",
]
