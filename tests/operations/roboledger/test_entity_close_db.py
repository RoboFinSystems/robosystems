"""Two entities in one tenant each run their own close: one calendar and one
set of periods apiece, and nothing one entity's close, reopen or closed-month
guard does reaches its sibling's books."""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.extensions.journal_entries import (
  CreateJournalEntryRequest,
  JournalEntryLineItemInput,
)
from robosystems.models.api.extensions.schedules import (
  CreateScheduleRequest,
  EntryTemplateRequest,
)
from robosystems.models.api.fact_provenance import AssertedProvenance
from robosystems.models.extensions import Element
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.operations.roboledger.commands._guards import (
  ClosedPeriodError,
  assert_period_not_closed,
)
from robosystems.operations.roboledger.commands.fiscal_calendar import (
  close_period,
  reopen_period,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  create_journal_entry,
)
from robosystems.operations.roboledger.commands.schedules import create_schedule
from robosystems.operations.roboledger.fact_set import create_fact_set
from robosystems.operations.roboledger.fiscal_calendar import (
  FiscalCalendarError,
  FiscalCalendarService,
  PeriodCloseService,
)
from robosystems.operations.roboledger.fiscal_calendar.service import (
  CloseableGateResult,
)
from robosystems.operations.roboledger.reads.period_drafts import list_period_drafts
from robosystems.operations.roboledger.reports.statement_sets import (
  StatementStampResult,
  has_canonical_statement_sets,
)

pytestmark = pytest.mark.unit

GRAPH_ID = "kg0123456789abcdef08"
_COMMANDS = "robosystems.operations.roboledger.commands.fiscal_calendar"
JULY = (date(2026, 7, 1), date(2026, 7, 31))


@pytest.fixture(autouse=True)
def _no_platform():
  """The fence and the stale mark talk to other databases; the tenant schema
  under test is the whole subject here."""
  with (
    patch(f"{_COMMANDS}.exclusive_period_fence"),
    patch("robosystems.operations.extensions.staleness.mark_graph_stale"),
    patch(
      "robosystems.operations.roboledger.reads.fiscal_calendar.qb_sync_state",
      return_value=(False, None),
    ),
  ):
    yield


def _stamp(session, *, graph_id, period_start, period_end, actor_id, entity_id):
  """Stand in for the pivot: one canonical statement set for the entity."""
  fact_set = create_fact_set(
    session,
    period_start=period_start,
    period_end=period_end,
    factset_type="report",
    entity_id=entity_id,
    provenance=AssertedProvenance(
      source_system="test", asserted_by=actor_id, basis_note="fixture"
    ),
    created_by=actor_id,
  )
  session.flush()
  return StatementStampResult(stamped=True, fact_set_ids={"statement": fact_set.id})


@pytest.fixture()
def group(two_entities):
  """Both entities' calendars closed through June, with July and August open
  and a pair of accounts each."""
  t = two_entities
  service = FiscalCalendarService()
  accounts: dict[str, tuple[str, str]] = {}
  for entity in (t.parent, t.sub):
    service.initialize(
      t.session,
      GRAPH_ID,
      closed_through="2026-06",
      actor_id="usr_1",
      entity_id=entity.id,
    )
    service.ensure_fiscal_periods(
      t.session,
      GRAPH_ID,
      start_period="2026-06",
      end_period="2026-08",
      closed_through="2026-06",
      entity_id=entity.id,
    )
    pair = []
    for name in ("Cash", "Rent"):
      element = Element(name=f"{entity.name} {name}", code=name, created_by="usr_1")
      t.session.add(element)
      t.session.flush()
      pair.append(str(element.id))
    accounts[entity.id] = (pair[0], pair[1])
  t.session.commit()
  t.accounts = accounts
  t.service = service
  return t


def _draft(t, entity_id: str, posting_date: date, cents: int = 50_000) -> str:
  cash, rent = t.accounts[entity_id]
  created = create_journal_entry(
    t.session,
    CreateJournalEntryRequest(
      posting_date=posting_date,
      memo="Rent received",
      line_items=[
        JournalEntryLineItemInput(element_id=cash, debit_amount=cents),
        JournalEntryLineItemInput(element_id=rent, credit_amount=cents),
      ],
    ),
    "usr_1",
    entity_id=entity_id,
  )
  return created.id


def _close(t, entity_id: str | None, period: str):
  return close_period(
    t.session,
    MagicMock(),
    GRAPH_ID,
    period,
    actor_id="usr_1",
    allow_stale_sync=False,
    note=None,
    service=t.service,
    close_service=PeriodCloseService(t.service, statement_stamper=_stamp),
    entity_id=entity_id,
  )


def _period_status(t, entity_id: str, period: str) -> str:
  return (
    t.session.query(FiscalPeriod.status)
    .filter(FiscalPeriod.entity_id == entity_id, FiscalPeriod.name == period)
    .scalar()
  )


class TestCloseIsPerEntity:
  def test_closing_one_entity_leaves_its_sibling_open(self, group):
    t = group
    sub_draft = _draft(t, t.sub.id, date(2026, 7, 15))
    parent_draft = _draft(t, t.parent.id, date(2026, 7, 20))

    response = _close(t, t.sub.id, "2026-07")

    assert response.entries_posted == 1
    assert t.session.get(Entry, sub_draft).status == "posted"
    assert t.session.get(Entry, parent_draft).status == "draft"
    assert _period_status(t, t.sub.id, "2026-07") == "closed"
    assert _period_status(t, t.parent.id, "2026-07") == "open"
    assert (
      t.service.get(t.session, GRAPH_ID, entity_id=t.sub.id).closed_through_period
      == "2026-07"
    )
    assert t.service.get(t.session, GRAPH_ID).closed_through_period == "2026-06"

  def test_each_entity_gets_its_own_statements(self, group):
    t = group
    _draft(t, t.sub.id, date(2026, 7, 15))

    _close(t, t.sub.id, "2026-07")

    start, end = JULY
    assert has_canonical_statement_sets(
      t.session, period_start=start, period_end=end, entity_id=t.sub.id
    )
    assert not has_canonical_statement_sets(
      t.session, period_start=start, period_end=end, entity_id=t.parent.id
    )

  def test_a_close_with_no_entity_named_is_the_parents(self, group):
    t = group
    sub_draft = _draft(t, t.sub.id, date(2026, 7, 15))
    parent_draft = _draft(t, t.parent.id, date(2026, 7, 20))

    _close(t, None, "2026-07")

    assert t.session.get(Entry, parent_draft).status == "posted"
    assert t.session.get(Entry, sub_draft).status == "draft"
    assert _period_status(t, t.sub.id, "2026-07") == "open"

  def test_entities_close_on_their_own_sequence(self, group):
    t = group
    _close(t, t.sub.id, "2026-07")
    _close(t, t.sub.id, "2026-08")

    # The parent is still on July: August is out of sequence for it alone.
    gate = t.service.closeable_gate(t.session, GRAPH_ID, "2026-08")
    assert CloseableGateResult.SEQUENCE in gate.blockers
    assert (
      t.service.get(t.session, GRAPH_ID, entity_id=t.sub.id).closed_through_period
      == "2026-08"
    )

  def test_the_drafts_listed_for_review_are_one_entitys(self, group):
    t = group
    sub_draft = _draft(t, t.sub.id, date(2026, 7, 15))
    parent_draft = _draft(t, t.parent.id, date(2026, 7, 20))

    sub_drafts = list_period_drafts(t.session, "2026-07", entity_id=t.sub.id)
    parent_drafts = list_period_drafts(t.session, "2026-07")

    assert [d.entry_id for d in sub_drafts.drafts] == [sub_draft]
    assert [d.entry_id for d in parent_drafts.drafts] == [parent_draft]


class TestClosedMonthGuardIsPerEntity:
  def test_a_closed_month_refuses_only_its_own_entitys_writes(self, group):
    t = group
    _close(t, t.sub.id, "2026-07")

    with pytest.raises(ClosedPeriodError):
      _draft(t, t.sub.id, date(2026, 7, 28))
    # The parent's July is still open.
    assert _draft(t, t.parent.id, date(2026, 7, 28))

  def test_the_guard_reads_the_named_entitys_calendar(self, group):
    t = group
    _close(t, t.sub.id, "2026-07")

    assert_period_not_closed(t.session, date(2026, 7, 10), entity_id=t.parent.id)
    assert_period_not_closed(t.session, date(2026, 7, 10))
    with pytest.raises(ClosedPeriodError):
      assert_period_not_closed(t.session, date(2026, 7, 10), entity_id=t.sub.id)


class TestGateCountsOneEntity:
  def test_a_siblings_pending_obligation_does_not_hold_the_close(self, group):
    t = group
    t.session.add(
      Event(
        entity_id=t.sub.id,
        event_type="schedule_entry_due",
        event_category="recognition",
        event_class="economic",
        occurred_at=datetime(2026, 7, 31, 23, 59, 59, tzinfo=UTC),
        source="schedule",
        status="pending",
        created_by="usr_1",
      )
    )
    t.session.flush()

    parent_gate = t.service.closeable_gate(t.session, GRAPH_ID, "2026-07")
    sub_gate = t.service.closeable_gate(
      t.session, GRAPH_ID, "2026-07", entity_id=t.sub.id
    )

    assert parent_gate.is_closeable
    assert CloseableGateResult.PENDING_OBLIGATIONS in sub_gate.blockers

  def test_a_siblings_uncommitted_event_does_not_hold_the_close(self, group):
    t = group
    t.session.add(
      Event(
        entity_id=t.sub.id,
        event_type="bank_transaction",
        event_category="treasury",
        event_class="economic",
        occurred_at=datetime(2026, 7, 12, tzinfo=UTC),
        source="manual",
        status="captured",
        created_by="usr_1",
      )
    )
    t.session.flush()

    parent_gate = t.service.closeable_gate(t.session, GRAPH_ID, "2026-07")
    sub_gate = t.service.closeable_gate(
      t.session, GRAPH_ID, "2026-07", entity_id=t.sub.id
    )

    assert parent_gate.is_closeable
    assert CloseableGateResult.UNPOSTED_SOURCE_EVENTS in sub_gate.blockers


class TestReopenIsPerEntity:
  def test_reopening_one_entity_keeps_its_siblings_statements(self, group):
    t = group
    _close(t, t.parent.id, "2026-07")
    _close(t, t.sub.id, "2026-07")

    result = reopen_period(
      t.session,
      MagicMock(),
      GRAPH_ID,
      "2026-07",
      actor_id="usr_1",
      reason="late invoice",
      note=None,
      service=t.service,
      entity_id=t.sub.id,
    )

    start, end = JULY
    assert result.statement_sets_retracted == 1
    assert _period_status(t, t.sub.id, "2026-07") == "closing"
    assert _period_status(t, t.parent.id, "2026-07") == "closed"
    assert has_canonical_statement_sets(
      t.session, period_start=start, period_end=end, entity_id=t.parent.id
    )
    assert not has_canonical_statement_sets(
      t.session, period_start=start, period_end=end, entity_id=t.sub.id
    )
    assert t.service.get(t.session, GRAPH_ID).closed_through_period == "2026-07"
    assert (
      t.service.get(t.session, GRAPH_ID, entity_id=t.sub.id).closed_through_period
      == "2026-06"
    )


class TestSchedulesFollowTheirEntity:
  def _schedule(self, t, entity_id: str):
    cash, rent = t.accounts[entity_id]
    return create_schedule(
      t.session,
      CreateScheduleRequest(
        name="Insurance",
        element_ids=[cash, rent],
        period_start=date(2026, 7, 1),
        period_end=date(2026, 9, 30),
        monthly_amount=10_000,
        entry_template=EntryTemplateRequest(
          debit_element_id=cash, credit_element_id=rent
        ),
      ),
      created_by="usr_1",
      entity_id=entity_id,
    )

  def _obligations(self, t, entity_id: str) -> dict[str, int]:
    tally: dict[str, int] = {}
    for event in t.session.query(Event).filter(
      Event.entity_id == entity_id, Event.event_type == "schedule_entry_due"
    ):
      tally[event.status] = tally.get(event.status, 0) + 1
    return tally

  def test_a_new_schedule_starts_after_its_own_entitys_closed_months(self, group):
    t = group
    _close(t, t.sub.id, "2026-07")

    self._schedule(t, t.sub.id)
    self._schedule(t, t.parent.id)

    # July is closed for the subsidiary only, so only its July is history.
    assert self._obligations(t, t.sub.id) == {"voided": 1, "pending": 2}
    assert self._obligations(t, t.parent.id) == {"pending": 3}


class TestOneCadencePerGraph:
  def test_a_second_entity_cannot_start_its_year_in_another_month(self, two_entities):
    t = two_entities
    service = FiscalCalendarService()
    service.initialize(t.session, GRAPH_ID, fiscal_year_start_month=7, actor_id="usr_1")

    with pytest.raises(FiscalCalendarError, match="month 7"):
      service.initialize(
        t.session,
        GRAPH_ID,
        fiscal_year_start_month=1,
        actor_id="usr_1",
        entity_id=t.sub.id,
      )

    service.initialize(
      t.session,
      GRAPH_ID,
      fiscal_year_start_month=7,
      actor_id="usr_1",
      entity_id=t.sub.id,
    )
    assert (
      service.get(t.session, GRAPH_ID, entity_id=t.sub.id).fiscal_year_start_month == 7
    )
