"""Handlers for ``block_type='schedule'``: bind the Information Block
dispatch to the Schedule commands in
:mod:`robosystems.operations.roboledger.commands.schedules`.
"""

from __future__ import annotations

from collections.abc import Sequence

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
from robosystems.models.extensions import Structure
from robosystems.models.extensions.roboledger import Entry, Fact
from robosystems.operations.information_block.envelope import (
  association_to_connection,
  elements_to_lites,
  fact_to_lite,
  load_base_envelope_atoms,
)

# `roboledger.commands.schedules` is imported inside the handlers below: a
# module-level import closes an import cycle through the registry.
SCHEDULE_BLOCK_TYPE = "schedule"
SCHEDULE_DISPLAY_NAME = "Schedule"
SCHEDULE_CATEGORY = "Close"


def create(
  session: Session,
  payload: CreateScheduleRequest,
  created_by: str,
) -> str:
  """Create a schedule via the existing command, return its structure_id."""
  from robosystems.operations.roboledger.commands.schedules import (
    create_schedule as cmd_create_schedule,
  )

  response = cmd_create_schedule(session, payload, created_by=created_by)
  return response.structure_id


def update(
  session: Session,
  payload: UpdateScheduleRequest,
  updated_by: str,
) -> str:
  """Update a schedule via the existing command, return its structure_id."""
  from robosystems.operations.roboledger.commands.schedules import (
    update_schedule as cmd_update_schedule,
  )

  response = cmd_update_schedule(session, payload, updated_by=updated_by)
  return response.structure_id


def delete(
  session: Session,
  payload: DeleteScheduleRequest,
  deleted_by: str,
) -> str:
  """Delete a schedule via the existing command, return the deleted id."""
  from robosystems.operations.roboledger.commands.schedules import (
    delete_schedule as cmd_delete_schedule,
  )

  cmd_delete_schedule(session, payload)
  return payload.structure_id


def _load_schedule_mechanics(
  structure: Structure, periods_with_entries: int
) -> ScheduleMechanics:
  """Build the typed Schedule mechanics from ``artifact_mechanics``, falling
  back to the ``metadata_`` shape older rows carry."""
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
      "and no artifact_mechanics — the row may be corrupted or written by an "
      "unsupported legacy path. Check the Schedule creation path and the "
      "artifact_mechanics backfill."
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


def _latest_instant_per_element(facts: Sequence[Fact]) -> list[Fact]:
  """Keep only the most recent instant fact per element (max ``period_end``)."""
  latest: dict[str, Fact] = {}
  for fact in facts:
    current = latest.get(fact.element_id)
    if current is None or fact.period_end > current.period_end:
      latest[fact.element_id] = fact
  return list(latest.values())


def build_envelope(
  session: Session,
  structure_id: str,
  fact_set_id: str | None = None,
  scenario_id: str | None = None,
  series: bool = False,
  series_history: int | None = None,
  series_forecast: int | None = None,
) -> InformationBlockEnvelope | None:
  """Reload a schedule Structure and pack its Information Block envelope.

  ``None`` when the structure doesn't exist or isn't a schedule.
  ``scenario_id`` is ignored (schedules have no scenarios). ``fact_set_id``
  pins facts to a filed snapshot; unpinned, every in-scope fact publishes.
  """
  atoms = load_base_envelope_atoms(
    session,
    structure_id,
    expected_block_type=SCHEDULE_BLOCK_TYPE,
    fact_set_id=fact_set_id,
  )
  if atoms is None:
    return None

  structure = atoms.structure

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

  # Only in-scope facts: historical ones are already in opening balances and
  # would invite re-drafting closed work.
  fact_filters = [
    Fact.structure_id == structure_id,
    Fact.fact_scope == "in_scope",
  ]
  if fact_set_id is not None:
    fact_filters.append(Fact.fact_set_id == fact_set_id)
  facts = list(session.execute(select(Fact).where(*fact_filters)).scalars().all())

  # The series' beginning balance is the last closed period's ending instant,
  # which the in_scope filter dropped: re-include the latest historical
  # instant per balance element (not historical movements). A pinned
  # snapshot is already self-contained.
  if facts and fact_set_id is None:
    balance_element_ids = {f.element_id for f in facts if f.period_type == "instant"}
    if balance_element_ids:
      historical_instants = (
        session.execute(
          select(Fact).where(
            Fact.structure_id == structure_id,
            Fact.fact_scope == "historical",
            Fact.period_type == "instant",
            Fact.element_id.in_(balance_element_ids),
          )
        )
        .scalars()
        .all()
      )
      facts.extend(_latest_instant_per_element(historical_instants))

  # Tenant-authored schedules never have a disclosure mapping.
  disclosure_id: str | None = None
  _elements_by_id = {e.id: e for e in atoms.elements}
  return InformationBlockEnvelope(
    id=structure.id,
    block_type=SCHEDULE_BLOCK_TYPE,
    name=structure.name,
    display_name=SCHEDULE_DISPLAY_NAME,
    category=SCHEDULE_CATEGORY,
    taxonomy_id=structure.taxonomy_id,
    taxonomy_name=atoms.taxonomy_name,
    disclosure_id=disclosure_id,
    information_model=InformationModelResponse(
      concept_arrangement=structure.concept_arrangement or "roll_forward",
      member_arrangement=structure.member_arrangement,
    ),
    artifact=ArtifactResponse(
      topic=structure.description,
      renderer_note=structure.renderer_note,
      template=None,
      mechanics=mechanics,
    ),
    elements=elements_to_lites(session, atoms.elements),
    connections=[
      association_to_connection(a, atoms.classifications_by_assoc.get(a.id, []))
      for a in atoms.associations
    ],
    facts=[fact_to_lite(f, _elements_by_id) for f in facts],
    rules=atoms.rules,
    fact_set=atoms.fact_set,
    verification_results=atoms.verification_results,
    verification_summary=atoms.verification_summary,
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
