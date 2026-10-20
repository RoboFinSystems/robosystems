"""Management dimensions reach the lines events, bank lines and schedules write.

Against real Postgres: an event's members fill the axes each of its lines
leaves open; a bank line's account side carries the members its
classification names, re-tagged in place once posted and mirrored back when
the entry is re-tagged; a schedule's drafts carry its template's members.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest
from sqlalchemy import select

from robosystems.models.api.event_block import (
  CreateEventBlockRequest,
  UpdateEventBlockRequest,
)
from robosystems.models.api.extensions.dimensions import (
  CreateDimensionRequest,
  UpdateDimensionRequest,
)
from robosystems.models.api.extensions.journal_entries import (
  CreateJournalEntryRequest,
  JournalEntryLineItemInput,
  UpdateJournalEntryRequest,
)
from robosystems.models.api.extensions.schedules import (
  CreateScheduleRequest,
  EntryTemplateRequest,
  ScheduleMetadataRequest,
)
from robosystems.models.extensions import Structure
from robosystems.models.extensions.roboledger import Entry, Event, LineItem
from robosystems.models.extensions.roboledger.dimension_junctions import (
  event_dimensions,
)
from robosystems.operations.event_block.commands import (
  create_event_block,
  update_event_block,
)
from robosystems.operations.roboledger.commands.dimensions import (
  DuplicateDimensionError,
  create_dimension,
  update_dimension,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  create_journal_entry,
  update_journal_entry,
)
from robosystems.operations.roboledger.commands.schedules import create_schedule
from robosystems.operations.roboledger.line_dimensions import line_dimension_ids
from robosystems.operations.roboledger.schedules import ScheduleService
from tests.ledger_entity import entity_account

pytestmark = pytest.mark.unit

GRAPH_ID = "kg0123456789abcdef0e"


def _member(session, dimension_type: str, name: str) -> str:
  return create_dimension(
    session,
    CreateDimensionRequest(dimension_type=dimension_type, name=name),
    "usr_1",
  ).id


@pytest.fixture()
def books(two_entities):
  t = two_entities
  entity = t.parent.id
  t.cash = entity_account(t.session, entity, "Checking")
  t.revenue = entity_account(t.session, entity, "Consulting Revenue")
  t.rent = entity_account(t.session, entity, "Rent Expense")
  t.supplies = entity_account(t.session, entity, "Supplies Expense")
  t.session.commit()
  t.sales = _member(t.session, "department", "Sales")
  t.ops = _member(t.session, "department", "Operations")
  t.retail = _member(t.session, "class", "Retail")
  return t


def _tags(t, entry_id: str) -> list[list[str]]:
  lines = (
    t.session.execute(
      select(LineItem)
      .where(LineItem.entry_id == entry_id)
      .order_by(LineItem.line_order)
    )
    .scalars()
    .all()
  )
  held = line_dimension_ids(t.session, (li.id for li in lines))
  return [sorted(held.get(str(li.id), [])) for li in lines]


def _event_entry(t, event_id: str) -> str:
  return str(
    t.session.execute(
      select(Entry.id).where(Entry.triggered_by_event_id == event_id)
    ).scalar_one()
  )


class TestEventInheritance:
  def _recorded(self, t, lines: list[dict], dimension_ids: list[str]):
    return create_event_block(
      t.session,
      CreateEventBlockRequest(
        event_type="journal_entry_recorded",
        event_category="adjustment",
        event_class="economic",
        source="manual",
        occurred_at=datetime(2026, 9, 15),
        amount=50_000,
        apply_handlers=True,
        dimension_ids=dimension_ids,
        metadata={
          "posting_date": "2026-09-15",
          "memo": "Fees",
          "line_items": lines,
        },
      ),
      "usr_1",
      graph_id=GRAPH_ID,
    )

  def test_each_line_takes_the_events_members_on_the_axes_it_leaves_open(self, books):
    t = books
    envelope = self._recorded(
      t,
      [
        {"element_id": t.cash, "debit_amount": 50_000},
        {
          "element_id": t.revenue,
          "credit_amount": 30_000,
          "dimension_ids": [t.ops],
        },
        {
          "element_id": t.revenue,
          "credit_amount": 20_000,
          "dimension_ids": [t.retail],
        },
      ],
      [t.sales],
    )

    assert _tags(t, _event_entry(t, envelope.id)) == [
      [t.sales],
      [t.ops],
      sorted([t.retail, t.sales]),
    ]

  def test_two_members_of_one_axis_on_an_event_are_refused(self, books):
    t = books
    with pytest.raises(ValueError, match="one per axis"):
      self._recorded(
        t,
        [
          {"element_id": t.cash, "debit_amount": 100},
          {"element_id": t.revenue, "credit_amount": 100},
        ],
        [t.sales, t.ops],
      )


def _deposit(t, cents: int = 100_000) -> Event:
  event = Event(
    entity_id=t.parent.id,
    event_type="bank_transaction",
    event_category="treasury",
    event_class="economic",
    resource_type="money",
    resource_element_id=t.cash,
    occurred_at=datetime(2026, 8, 12, tzinfo=UTC),
    effective_at=datetime(2026, 8, 12, tzinfo=UTC),
    status="captured",
    source="plaid",
    amount=-cents,
    description="Office costs",
    metadata_={},
    created_by="test",
  )
  t.session.add(event)
  t.session.commit()
  return event


def _update(t, event: Event, **fields):
  return update_event_block(
    t.session,
    UpdateEventBlockRequest(event_id=str(event.id), **fields),
    "usr_1",
    graph_id=GRAPH_ID,
  )


def _by_account(t, event: Event) -> dict[str, list[str]]:
  entry_id = _event_entry(t, str(event.id))
  lines = t.session.execute(
    select(LineItem.id, LineItem.element_id).where(LineItem.entry_id == entry_id)
  ).all()
  held = line_dimension_ids(t.session, (i for i, _e in lines))
  return {str(e): sorted(held.get(str(i), [])) for i, e in lines}


class TestBankLines:
  def test_the_account_side_carries_the_classifications_members(self, books):
    t = books
    event = _deposit(t)
    _update(
      t,
      event,
      transition_to="committed",
      metadata_patch={
        "classified_element_id": t.rent,
        "classified_dimension_ids": [t.ops],
      },
    )

    assert _by_account(t, event) == {t.cash: [], t.rent: [t.ops]}

  def test_a_split_part_carries_its_own_members_else_the_lines(self, books):
    t = books
    event = _deposit(t)
    _update(
      t,
      event,
      transition_to="committed",
      metadata_patch={
        "classified_dimension_ids": [t.ops],
        "classified_allocations": [
          {"element_id": t.rent, "amount": 60_000, "dimension_ids": [t.sales]},
          {"element_id": t.supplies, "amount": 40_000},
        ],
      },
    )

    assert _by_account(t, event) == {
      t.cash: [],
      t.rent: [t.sales],
      t.supplies: [t.ops],
    }

  def test_a_posted_line_is_re_tagged_in_place(self, books):
    t = books
    event = _deposit(t)
    _update(
      t,
      event,
      transition_to="committed",
      metadata_patch={
        "classified_element_id": t.rent,
        "classified_dimension_ids": [t.ops],
      },
    )

    _update(t, event, metadata_patch={"classified_dimension_ids": [t.sales]})

    assert _by_account(t, event)[t.rent] == [t.sales]
    t.session.refresh(event)
    assert len(event.metadata_["dimension_retags"]) == 1

  def test_a_re_tag_of_the_entry_is_written_back_to_the_event(self, books):
    t = books
    event = _deposit(t)
    _update(
      t,
      event,
      transition_to="committed",
      metadata_patch={"classified_element_id": t.rent},
    )
    entry_id = _event_entry(t, str(event.id))
    rent_line = t.session.execute(
      select(LineItem.id).where(
        LineItem.entry_id == entry_id, LineItem.element_id == t.rent
      )
    ).scalar_one()

    update_journal_entry(
      t.session,
      UpdateJournalEntryRequest(
        entry_id=entry_id,
        line_dimensions=[{"line_id": str(rent_line), "dimension_ids": [t.ops]}],
      ),
    )

    t.session.refresh(event)
    assert event.metadata_["classified_dimension_ids"] == [t.ops]

  def test_the_events_own_members_are_not_copied_onto_its_classification(self, books):
    t = books
    event = _deposit(t)
    t.session.execute(
      event_dimensions.insert(), [{"event_id": event.id, "dimension_id": t.retail}]
    )
    _update(
      t,
      event,
      transition_to="committed",
      metadata_patch={"classified_element_id": t.rent},
    )
    entry_id = _event_entry(t, str(event.id))
    rent_line = t.session.execute(
      select(LineItem.id).where(
        LineItem.entry_id == entry_id, LineItem.element_id == t.rent
      )
    ).scalar_one()

    update_journal_entry(
      t.session,
      UpdateJournalEntryRequest(
        entry_id=entry_id,
        line_dimensions=[
          {"line_id": str(rent_line), "dimension_ids": [t.ops, t.retail]}
        ],
      ),
    )

    t.session.refresh(event)
    assert event.metadata_["classified_dimension_ids"] == [t.ops]


class TestSchedules:
  def test_a_schedules_drafts_carry_its_templates_members(self, books):
    t = books
    created = create_schedule(
      t.session,
      CreateScheduleRequest(
        name="Office lease",
        element_ids=[t.rent, t.cash],
        period_start=date(2026, 1, 1),
        period_end=date(2026, 12, 31),
        monthly_amount=10_000,
        entry_template=EntryTemplateRequest(
          debit_element_id=t.rent,
          credit_element_id=t.cash,
          dimension_ids=[t.ops],
        ),
        schedule_metadata=ScheduleMetadataRequest(original_amount=120_000),
      ),
      created_by="usr_1",
    )
    t.session.commit()

    result = ScheduleService().create_closing_entry(
      t.session,
      structure_id=created.structure_id,
      posting_date=date(2026, 3, 31),
      period_start=date(2026, 3, 1),
      period_end=date(2026, 3, 31),
      created_by="usr_1",
    )

    assert _tags(t, result.entry_id) == [[t.ops], [t.ops]]

  def test_a_kept_draft_and_its_reversal_follow_the_template(self, books):
    """Members added to the template reach a draft that is not regenerated,
    and its auto-reversal; members taken off it come off both."""
    t = books
    created = create_schedule(
      t.session,
      CreateScheduleRequest(
        name="Accrued rent",
        element_ids=[t.rent, t.cash],
        period_start=date(2026, 1, 1),
        period_end=date(2026, 12, 31),
        monthly_amount=10_000,
        entry_template=EntryTemplateRequest(
          debit_element_id=t.rent, credit_element_id=t.cash, auto_reverse=True
        ),
        schedule_metadata=ScheduleMetadataRequest(original_amount=120_000),
      ),
      created_by="usr_1",
    )
    t.session.commit()

    def march():
      return ScheduleService().create_closing_entry(
        t.session,
        structure_id=created.structure_id,
        posting_date=date(2026, 3, 31),
        period_start=date(2026, 3, 1),
        period_end=date(2026, 3, 31),
        created_by="usr_1",
      )

    def retemplate(dimension_ids: list[str]):
      structure = t.session.get(Structure, created.structure_id)
      structure.metadata_ = {
        **structure.metadata_,
        "entry_template": {
          **structure.metadata_["entry_template"],
          "dimension_ids": dimension_ids,
        },
      }
      t.session.commit()

    first = march()
    reversal = first.reversal.entry_id
    retemplate([t.ops])
    kept = march()

    assert kept.outcome == "unchanged"
    assert _tags(t, kept.entry_id) == [[t.ops], [t.ops]]
    assert _tags(t, reversal) == [[t.ops], [t.ops]]

    retemplate([])
    march()

    assert _tags(t, kept.entry_id) == [[], []]
    assert _tags(t, reversal) == [[], []]


class TestReviewFollowUps:
  def test_a_retired_member_stays_only_on_the_line_that_carries_it(self, books):
    t = books
    posted = create_journal_entry(
      t.session,
      CreateJournalEntryRequest(
        posting_date=date(2026, 9, 15),
        memo="Fees",
        line_items=[
          JournalEntryLineItemInput(
            element_id=t.cash, debit_amount=100, dimension_ids=[t.sales]
          ),
          JournalEntryLineItemInput(element_id=t.revenue, credit_amount=100),
        ],
      ),
      "usr_1",
      entity_id=t.parent.id,
    )
    update_dimension(
      t.session, UpdateDimensionRequest(dimension_id=t.sales, is_active=False), "usr_1"
    )
    cash_line, revenue_line = (li.id for li in posted.line_items)

    with pytest.raises(ValueError, match="retired"):
      update_journal_entry(
        t.session,
        UpdateJournalEntryRequest(
          entry_id=posted.id,
          line_dimensions=[
            {"line_id": cash_line, "dimension_ids": [t.sales]},
            {"line_id": revenue_line, "dimension_ids": [t.sales]},
          ],
        ),
      )

  def test_a_retired_members_key_points_at_bringing_it_back(self, books):
    t = books
    update_dimension(
      t.session, UpdateDimensionRequest(dimension_id=t.ops, is_active=False), "usr_1"
    )
    with pytest.raises(DuplicateDimensionError, match="is_active=true"):
      _member(t.session, "department", "Operations")

  def test_a_name_with_no_letters_or_digits_needs_a_value(self, books):
    with pytest.raises(ValueError, match="give the member a value"):
      _member(books.session, "department", "—")
