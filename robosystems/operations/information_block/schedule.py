"""Handlers for ``block_type='schedule'`` — the declarative construction
mode reference.

Two public handlers bind the generic construction machinery to the Schedule
commands in :mod:`robosystems.operations.roboledger.commands.schedules`:

- :func:`create` delegates to ``cmd_create_schedule`` and returns the
  new structure's id.
- :func:`build_envelope` reloads the Structure + its bundled atoms and
  packs them into the typed :class:`InformationBlockEnvelope`.

Schedule is the reference implementation of the **declarative**
construction mode — the user declares the mechanics + seed params and
the system generates atoms. The statement family covers the
**compositional** mode and the metric block covers the **derivative**
mode.
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

  ``updated_by`` is forwarded so any side effects of the update — such as
  superseding pending obligations when the entry template changes —
  record the right actor on the freshly emitted event rows.
  """
  response = cmd_update_schedule(session, payload, updated_by=updated_by)
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

  Reads the typed ``artifact_mechanics`` column when populated, falling
  back to the ``metadata_`` JSONB shape that older Schedule rows still
  carry — both paths produce the same :class:`ScheduleMechanics` arm.
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
  """Keep only the most recent instant fact per element (max ``period_end``).

  Used to pick a roll-forward's carry-in opening balances: among the prior
  closed periods' ending running-balance instants, only the latest per
  element is the current window's beginning balance — earlier ones are
  superseded interior balances and would double up the series.
  """
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

  Returns ``None`` when the structure doesn't exist or isn't a schedule,
  so the generic reader can cleanly distinguish misses from errors.
  Mechanics are read from the typed ``artifact_mechanics`` column with
  fallback to ``metadata_`` JSONB.

  ``scenario_id`` is accepted for dispatch-signature parity and ignored
  — schedules are physical-ledger projections with no scenario slices.

  ``fact_set_id`` pins the envelope to a specific FactSet snapshot —
  the Report-Block rehydration path uses this to surface the frozen
  fact slice that was reviewed at file time. When provided, facts are
  filtered by ``fact_set_id`` so that viewing a filed Report shows the
  exact snapshot rather than today's facts. The default (no pin)
  publishes every in-scope fact for the Structure, which is what the
  live closing-book mode expects.
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

  # Runtime state: how many closing entries (draft OR posted) trace back
  # to this schedule. Kept on the mechanics arm until typed FactSets
  # make this derivable.
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

  # Schedules publish only in-scope facts — historical facts were
  # already reflected in opening balances and shouldn't surface as
  # envelope data (they'd confuse agents into re-drafting closed work).
  # When a FactSet pin is supplied (Report-Block rehydration), facts
  # are also scoped to that pinned snapshot so a filed Report renders
  # the exact slice reviewed at file time, not today's drafts.
  fact_filters = [
    Fact.structure_id == structure_id,
    Fact.fact_scope == "in_scope",
  ]
  if fact_set_id is not None:
    fact_filters.append(Fact.fact_set_id == fact_set_id)
  facts = list(session.execute(select(Fact).where(*fact_filters)).scalars().all())

  # Roll-forward carry-in opening balance. A roll_forward series must open
  # with a valid Beginning Balance, but the first in-scope period's opening
  # balance *is* the immediately prior (now-closed) period's ending running
  # balance — an `instant` fact tagged `historical` and thus dropped by the
  # in_scope filter above. Without it the series arrives as movements + an
  # ending balance with no beginning. Re-include just the latest historical
  # `instant` per in-scope balance element (the carry-in balance); historical
  # `duration` movements stay excluded, so agents still can't re-draft closed
  # work. Skipped on the FactSet-pinned path (the frozen snapshot is already
  # self-contained) and when there are no in-scope facts to open.
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

  # Schedules are tenant-authored; they never have a disclosure mapping.
  # Short-circuit the DB roundtrip — saves a query per envelope on a
  # call path that's invoked once per item in the list view.
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
