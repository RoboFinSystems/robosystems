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

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.schedules import CreateScheduleRequest
from robosystems.models.api.information_block import (
  ArtifactResponse,
  InformationBlockEnvelope,
  InformationModelResponse,
  ScheduleMechanics,
)
from robosystems.models.extensions import Association, Element, Structure
from robosystems.models.extensions.roboledger import Fact
from robosystems.operations.information_block.envelope import (
  association_to_connection,
  element_to_lite,
  fact_to_lite,
)
from robosystems.operations.roboledger.commands.schedules import (
  create_schedule as cmd_create_schedule,
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


def build_envelope(
  session: Session, structure_id: str
) -> InformationBlockEnvelope | None:
  """Reload a schedule Structure and pack its Information Block envelope.

  Returns ``None`` when the structure doesn't exist or isn't a schedule,
  so the generic reader can cleanly distinguish misses from errors.

  Phase a reads mechanics from the existing ``metadata_`` JSONB; Phase d
  migrates the fields onto typed columns and this code path switches to
  reading those columns without changing the envelope shape.
  """
  structure = session.get(Structure, structure_id)
  if structure is None or structure.structure_type != SCHEDULE_BLOCK_TYPE:
    return None

  meta = structure.metadata_ or {}
  mechanics = ScheduleMechanics(
    kind="closing_entry_generator",
    entry_template=meta.get("entry_template", {}) or {},
    schedule_metadata=meta.get("schedule_metadata", {}) or {},
  )

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

  return InformationBlockEnvelope(
    id=structure.id,
    block_type=SCHEDULE_BLOCK_TYPE,
    name=structure.name,
    display_name=SCHEDULE_DISPLAY_NAME,
    category=SCHEDULE_CATEGORY,
    information_model=InformationModelResponse(
      concept_arrangement="roll_forward",
      member_arrangement=None,
    ),
    artifact=ArtifactResponse(
      topic=structure.description,
      parenthetical_note=None,
      template=None,
      mechanics=mechanics,
    ),
    elements=[element_to_lite(e) for e in elements],
    connections=[association_to_connection(a) for a in associations],
    facts=[fact_to_lite(f) for f in facts],
  )


__all__ = [
  "SCHEDULE_BLOCK_TYPE",
  "SCHEDULE_CATEGORY",
  "SCHEDULE_DISPLAY_NAME",
  "build_envelope",
  "create",
]
