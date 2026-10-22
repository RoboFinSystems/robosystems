"""A schedule that round-trips: its definition read back from the envelope
and saved through rebuild-schedule, a preview that keeps nothing, the
direction check, and the warnings an edit of posted history gets."""

from __future__ import annotations

import os
import uuid
from datetime import date

import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.api.extensions.schedules import (
  CreateScheduleRequest,
  DeleteScheduleRequest,
  EntryTemplateRequest,
  PreviewScheduleRequest,
  RebuildScheduleRequest,
  ScheduleMetadataRequest,
)
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.element_trait import ElementTrait
from robosystems.models.extensions.entity import Entity
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.fact import Fact
from robosystems.models.extensions.structure import Structure
from robosystems.models.extensions.trait import Trait
from robosystems.operations.information_block.schedule import build_envelope
from robosystems.operations.roboledger.commands.schedules import (
  create_schedule,
  delete_schedule,
  preview_schedule,
  rebuild_schedule,
)

pytestmark = pytest.mark.unit


@pytest.fixture()
def ext_session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_roundtrip_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))

  session = sessionmaker(bind=engine)()
  session.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(bind=session.connection())
  session.commit()
  session.execute(text(f'SET search_path TO "{schema}"'))

  try:
    yield session
  finally:
    session.rollback()
    session.close()
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


def _element(
  session, name: str, balance_type: str | None = None, trait: str | None = None
) -> str:
  element = Element(
    name=name, code=name[:8], balance_type=balance_type, created_by="test"
  )
  session.add(element)
  session.flush()
  if trait:
    trait_row = session.execute(
      select(Trait).where(
        Trait.category == "elementsOfFinancialStatements", Trait.identifier == trait
      )
    ).scalar()
    if trait_row is None:
      trait_row = Trait(category="elementsOfFinancialStatements", identifier=trait)
      session.add(trait_row)
      session.flush()
    session.add(
      ElementTrait(element_id=element.id, trait_id=trait_row.id, is_primary=True)
    )
    session.flush()
  return str(element.id)


def _prepaid(session) -> tuple[str, str, CreateScheduleRequest]:
  session.add(Entity(name="Fictional Co", created_by="usr"))
  expense = _element(session, "Insurance Expense", "debit", "expense")
  prepaid = _element(session, "Prepaid Insurance", "debit", "asset")
  body = CreateScheduleRequest(
    name="Prepaid insurance",
    element_ids=[expense, prepaid],
    period_start=date(2026, 1, 1),
    period_end=date(2026, 6, 30),
    monthly_amount=10_000,
    entry_template=EntryTemplateRequest(
      debit_element_id=expense, credit_element_id=prepaid
    ),
  )
  return expense, prepaid, body


def _mechanics(session, structure_id: str):
  envelope = build_envelope(session, structure_id)
  assert envelope is not None
  return envelope.artifact.mechanics


def test_the_envelope_reads_back_the_whole_definition(ext_session):
  session = ext_session
  _e, _p, body = _prepaid(session)
  created = create_schedule(session, body, created_by="usr")

  mechanics = _mechanics(session, created.structure_id)
  assert mechanics.period_start == date(2026, 1, 1)
  assert mechanics.period_end == date(2026, 6, 30)
  assert mechanics.monthly_amount == 10_000
  assert mechanics.posted_periods == []
  assert mechanics.edit_warning is None


def test_an_edit_saves_back_through_rebuild(ext_session):
  """The form's round trip: load the definition, change the amount and the
  term, save it through rebuild-schedule, and read the change back."""
  session = ext_session
  _e, _p, body = _prepaid(session)
  created = create_schedule(session, body, created_by="usr")
  mechanics = _mechanics(session, created.structure_id)

  rebuild_schedule(
    session,
    RebuildScheduleRequest(
      structure_id=created.structure_id,
      period_start=mechanics.period_start,
      period_end=date(2026, 3, 31),
      monthly_amount=20_000,
      entry_template=mechanics.entry_template,
    ),
    created_by="usr",
  )

  mechanics = _mechanics(session, created.structure_id)
  assert mechanics.period_end == date(2026, 3, 31)
  assert mechanics.monthly_amount == 20_000
  amounts = [
    float(v)
    for (v,) in session.execute(
      select(Fact.value)
      .where(Fact.structure_id == created.structure_id, Fact.period_type == "duration")
      .order_by(Fact.period_end)
    )
  ]
  assert amounts == [200.0, 200.0, 200.0]


def test_a_rebuild_with_no_fields_is_unchanged(ext_session):
  """A schedule made before the definition was readable rebuilds as it was."""
  session = ext_session
  _e, _p, body = _prepaid(session)
  created = create_schedule(session, body, created_by="usr")

  def _facts():
    return sorted(
      (str(f.element_id), f.period_end, f.period_type, float(f.value))
      for f in session.query(Fact).filter(Fact.structure_id == created.structure_id)
    )

  before = _facts()
  rebuild_schedule(session, RebuildScheduleRequest(structure_id=created.structure_id))
  assert _facts() == before


def test_a_preview_returns_the_roll_forward_and_keeps_nothing(ext_session):
  session = ext_session
  _e, _p, body = _prepaid(session)
  session.commit()

  preview = preview_schedule(
    session, PreviewScheduleRequest(**body.model_dump()), created_by="usr"
  )

  assert [row.amount for row in preview.roll_forward] == [100.0] * 6
  assert preview.roll_forward[0].opening == 600.0
  assert preview.roll_forward[-1].closing == 0.0
  assert preview.envelope.block_type == "schedule"
  assert preview.warnings == []
  session.commit()
  assert session.execute(select(func.count(Structure.id))).scalar() == 0
  assert session.execute(select(func.count(Fact.id))).scalar() == 0


def test_a_preview_refuses_what_create_refuses(ext_session):
  session = ext_session
  _e, _p, body = _prepaid(session)
  bad = PreviewScheduleRequest(
    **{
      **body.model_dump(),
      "schedule_metadata": ScheduleMetadataRequest(original_amount=200_000),
    }
  )
  with pytest.raises(ValueError, match="does not reach original_amount"):
    preview_schedule(session, bad, created_by="usr")


def test_a_template_that_debits_the_depreciated_asset_is_refused(ext_session):
  session = ext_session
  session.add(Entity(name="Fictional Co", created_by="usr"))
  equipment = _element(session, "Equipment", "debit", "asset")
  accumulated = _element(session, "Accumulated Depreciation", "credit", "contraAsset")
  with pytest.raises(ValueError, match="asset would climb"):
    create_schedule(
      session,
      CreateScheduleRequest(
        name="Backwards",
        element_ids=[equipment, accumulated],
        period_start=date(2026, 1, 1),
        period_end=date(2026, 12, 31),
        monthly_amount=10_000,
        entry_template=EntryTemplateRequest(
          debit_element_id=equipment, credit_element_id=accumulated
        ),
        schedule_metadata=ScheduleMetadataRequest(
          original_amount=120_000, asset_element_id=equipment
        ),
      ),
      created_by="usr",
    )


def test_a_swapped_prepaid_is_refused(ext_session):
  """Debit the prepaid and credit the expense, and the prepaid climbs."""
  session = ext_session
  expense, prepaid, body = _prepaid(session)
  swapped = body.model_copy(
    update={
      "entry_template": EntryTemplateRequest(
        debit_element_id=prepaid, credit_element_id=expense
      )
    }
  )
  with pytest.raises(ValueError, match="swap debit and credit"):
    create_schedule(session, swapped, created_by="usr")


def test_a_template_debiting_the_accumulation_is_refused(ext_session):
  session = ext_session
  session.add(Entity(name="Fictional Co", created_by="usr"))
  expense = _element(session, "Depreciation Expense", "debit", "expense")
  accumulated = _element(session, "Accumulated Depreciation", "debit", "contraAsset")
  with pytest.raises(ValueError, match="accumulation would fall"):
    create_schedule(
      session,
      CreateScheduleRequest(
        name="Backwards",
        element_ids=[expense, accumulated],
        period_start=date(2026, 1, 1),
        period_end=date(2026, 12, 31),
        monthly_amount=10_000,
        entry_template=EntryTemplateRequest(
          debit_element_id=accumulated, credit_element_id=expense
        ),
      ),
      created_by="usr",
    )


def _post_january(session, structure_id: str) -> None:
  from robosystems.models.extensions.roboledger.line_item import LineItem

  structure = session.get(Structure, structure_id)
  template = structure.metadata_["entry_template"]
  entry = Entry(
    entity_id=structure.entity_id,
    type="closing",
    status="posted",
    posting_date=date(2026, 1, 31),
    memo="January",
    source_structure_id=structure_id,
    provenance="schedule_derived",
    created_by="usr",
  )
  session.add(entry)
  session.flush()
  session.add_all(
    [
      LineItem(
        entry_id=entry.id,
        element_id=template["debit_element_id"],
        debit_amount=10_000,
        credit_amount=0,
        line_order=1,
      ),
      LineItem(
        entry_id=entry.id,
        element_id=template["credit_element_id"],
        debit_amount=0,
        credit_amount=10_000,
        line_order=2,
      ),
    ]
  )
  session.commit()


def test_posted_history_is_named_before_an_edit(ext_session):
  session = ext_session
  _e, _p, body = _prepaid(session)
  created = create_schedule(session, body, created_by="usr")
  _post_january(session, created.structure_id)

  mechanics = _mechanics(session, created.structure_id)
  assert mechanics.posted_periods == ["2026-01"]
  assert mechanics.edit_warning is not None
  assert "2026-01" in mechanics.edit_warning

  with pytest.raises(ValueError, match=r"in 2026-01"):
    delete_schedule(session, DeleteScheduleRequest(structure_id=created.structure_id))
  session.rollback()
  with pytest.raises(ValueError, match=r"in 2026-01"):
    rebuild_schedule(session, RebuildScheduleRequest(structure_id=created.structure_id))
  session.rollback()

  preview = preview_schedule(
    session,
    PreviewScheduleRequest(
      **{**body.model_dump(), "period_start": date(2026, 2, 1)},
      replaces_structure_id=created.structure_id,
    ),
    created_by="usr",
  )
  assert preview.replaces is not None
  assert preview.replaces.path == "terminate_and_succeed"
  assert preview.replaces.posted_periods == ["2026-01"]
  # The schedule being replaced is not also reported as an overlap.
  assert preview.warnings == []


def test_a_recreated_schedule_warns_of_the_one_it_overlaps(ext_session):
  session = ext_session
  _e, _p, body = _prepaid(session)
  first = create_schedule(session, body, created_by="usr")
  _post_january(session, first.structure_id)

  second = create_schedule(
    session,
    body.model_copy(update={"name": "Prepaid insurance again"}),
    created_by="usr",
  )

  assert len(second.warnings) == 1
  assert first.structure_id in second.warnings[0]
  assert "2026-01" in second.warnings[0]
