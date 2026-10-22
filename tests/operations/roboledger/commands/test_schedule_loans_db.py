"""Loan schedules against real Postgres: the effective-interest table, the
legs each period drafts, the current-portion reclass, and the schedule check
on the note."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, date, datetime

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.api.extensions.schedules import (
  CreateScheduleRequest,
  EntryTemplateRequest,
  LoanTermsRequest,
  PreviewScheduleRequest,
  RebuildScheduleRequest,
  ScheduleLegRequest,
  ScheduleMetadataRequest,
  UpdateScheduleRequest,
)
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.entity import Entity
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.fact import Fact
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.operations.event_block.promotion import promote_pending_obligations
from robosystems.operations.information_block.schedule import build_envelope
from robosystems.operations.roboledger.commands.schedules import (
  create_schedule,
  preview_schedule,
  rebuild_schedule,
  update_schedule,
)
from robosystems.operations.roboledger.reconciliations.resolvers import (
  ReconciliationWindow,
  ScheduleRegisterResolver,
)

pytestmark = pytest.mark.unit

GRAPH_ID = "kg0123456789abcdef02"


@pytest.fixture()
def ext_session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_loans_{uuid.uuid4().hex[:12]}"
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


def _element(session, name: str, balance_type: str) -> str:
  element = Element(
    name=name, code=name[:8], balance_type=balance_type, created_by="test"
  )
  session.add(element)
  session.flush()
  return str(element.id)


def _accounts(session) -> dict[str, str]:
  session.add(Entity(name="Fictional Co", created_by="usr"))
  return {
    "interest": _element(session, "Interest Expense", "debit"),
    "accrued": _element(session, "Accrued Interest", "credit"),
    "note": _element(session, "Notes Payable", "credit"),
    "current": _element(session, "Notes Payable, Current", "credit"),
    "cash": _element(session, "Cash", "debit"),
  }


def _line_of_credit(a: dict[str, str]) -> CreateScheduleRequest:
  """$80,000 drawn on the 5th at 9.9%, actual/360, interest only: accrue the
  interest and pay it through the bank."""
  return CreateScheduleRequest(
    name="Line of credit interest",
    element_ids=[a["interest"], a["accrued"], a["note"]],
    period_start=date(2026, 8, 1),
    period_end=date(2027, 7, 31),
    entry_template=EntryTemplateRequest(
      debit_element_id=a["interest"],
      credit_element_id=a["accrued"],
      legs=[
        ScheduleLegRequest(
          element_id=a["interest"], side="debit", amount_source="interest"
        ),
        ScheduleLegRequest(
          element_id=a["accrued"], side="credit", amount_source="interest"
        ),
      ],
    ),
    schedule_metadata=ScheduleMetadataRequest(
      method="effective_interest",
      loan_terms=LoanTermsRequest(
        principal=8_000_000,
        annual_rate=0.099,
        start_date=date(2026, 8, 5),
        day_count="actual/360",
        interest_only=True,
        note_element_id=a["note"],
      ),
    ),
  )


def _drafts(session, structure_id: str) -> dict[date, list[tuple[str, int, int]]]:
  out: dict[date, list[tuple[str, int, int]]] = {}
  for entry in session.query(Entry).filter(
    Entry.source_structure_id == structure_id, Entry.status == "draft"
  ):
    out[entry.posting_date] = sorted(
      (str(li.element_id), int(li.debit_amount), int(li.credit_amount))
      for li in session.query(LineItem).filter(LineItem.entry_id == entry.id)
    )
  return out


def test_a_line_of_credit_drafts_the_banks_interest_to_the_cent(ext_session):
  session = ext_session
  a = _accounts(session)
  created = create_schedule(session, _line_of_credit(a), created_by="usr")
  promote_pending_obligations(
    session, GRAPH_ID, as_of=datetime(2026, 10, 15, tzinfo=UTC), dispatch_handlers=True
  )
  session.commit()

  drafts = _drafts(session, created.structure_id)
  assert drafts[date(2026, 8, 31)] == sorted(
    [(a["interest"], 59_400, 0), (a["accrued"], 0, 59_400)]
  )
  assert drafts[date(2026, 9, 30)] == sorted(
    [(a["interest"], 66_000, 0), (a["accrued"], 0, 66_000)]
  )
  note_balances = {
    float(v)
    for (v,) in session.execute(
      select(Fact.value).where(
        Fact.structure_id == created.structure_id, Fact.element_id == a["note"]
      )
    )
  }
  assert note_balances == {80_000.0}


def test_the_schedule_check_states_the_notes_balance(ext_session):
  session = ext_session
  a = _accounts(session)
  create_schedule(session, _line_of_credit(a), created_by="usr")

  side = ScheduleRegisterResolver().resolve(
    session,
    ReconciliationWindow(
      period="2026-09", period_end=date(2026, 9, 30), fiscal_year_start=date(2026, 1, 1)
    ),
  )
  assert side.balances[a["note"]] == -8_000_000


def test_a_loan_round_trips_through_rebuild(ext_session):
  session = ext_session
  a = _accounts(session)
  created = create_schedule(session, _line_of_credit(a), created_by="usr")
  mechanics = build_envelope(session, created.structure_id).artifact.mechanics
  assert mechanics.schedule_metadata.loan_terms.day_count == "actual/360"
  assert len(mechanics.entry_template.legs) == 2

  # The rate changes before anything has posted: save it back.
  terms = mechanics.schedule_metadata.loan_terms.model_copy(
    update={"annual_rate": 0.12}
  )
  rebuild_schedule(
    session,
    RebuildScheduleRequest(
      structure_id=created.structure_id,
      schedule_metadata=ScheduleMetadataRequest(
        method="effective_interest", loan_terms=terms
      ),
    ),
    created_by="usr",
  )
  first = session.execute(
    select(Fact.value)
    .where(
      Fact.structure_id == created.structure_id,
      Fact.element_id == a["interest"],
      Fact.period_type == "duration",
    )
    .order_by(Fact.period_end)
    .limit(1)
  ).scalar()
  assert float(first) == 720.0  # 80,000 x 12% x 27/360


def test_a_loans_terms_change_only_through_rebuild(ext_session):
  session = ext_session
  a = _accounts(session)
  created = create_schedule(session, _line_of_credit(a), created_by="usr")
  with pytest.raises(ValueError, match="rebuild-schedule"):
    update_schedule(
      session,
      UpdateScheduleRequest(
        structure_id=created.structure_id,
        schedule_metadata=ScheduleMetadataRequest(method="straight_line"),
      ),
    )


def _term_loan(
  a: dict[str, str], legs: list[ScheduleLegRequest]
) -> CreateScheduleRequest:
  return CreateScheduleRequest(
    name="Term loan",
    element_ids=list(a.values()),
    period_start=date(2026, 1, 1),
    period_end=date(2028, 12, 31),
    entry_template=EntryTemplateRequest(
      debit_element_id=a["interest"], credit_element_id=a["cash"], legs=legs
    ),
    schedule_metadata=ScheduleMetadataRequest(
      method="effective_interest",
      loan_terms=LoanTermsRequest(
        principal=3_600_000,
        annual_rate=0.06,
        start_date=date(2026, 1, 1),
        first_payment_date=date(2026, 1, 31),
        maturity_date=date(2028, 12, 31),
        note_element_id=a["note"],
        current_portion_element_id=a["current"],
      ),
    ),
  )


def test_a_payment_split_by_the_schedule_drafts_principal_interest_and_reclass(
  ext_session,
):
  session = ext_session
  a = _accounts(session)
  legs = [
    ScheduleLegRequest(element_id=a["note"], side="debit", amount_source="principal"),
    ScheduleLegRequest(
      element_id=a["interest"], side="debit", amount_source="interest"
    ),
    ScheduleLegRequest(element_id=a["cash"], side="credit", amount_source="payment"),
  ]
  preview = preview_schedule(
    session,
    PreviewScheduleRequest(**_term_loan(a, legs).model_dump()),
    created_by="usr",
  )
  january = preview.roll_forward[0]
  assert january.interest == 180.0  # 36,000 x 6% / 12
  assert january.payment == pytest.approx(january.interest + january.principal)
  assert preview.roll_forward[-1].closing == 0.0

  created = create_schedule(session, _term_loan(a, legs), created_by="usr")
  promote_pending_obligations(
    session, GRAPH_ID, as_of=datetime(2026, 2, 10, tzinfo=UTC), dispatch_handlers=True
  )
  session.commit()

  lines = _drafts(session, created.structure_id)[date(2026, 1, 31)]
  assert sum(d for _e, d, _c in lines) == sum(c for _e, _d, c in lines)
  by_account: dict[str, int] = {}
  for element_id, debit, credit in lines:
    by_account[element_id] = by_account.get(element_id, 0) + debit - credit
  principal = round(january.principal * 100)
  current = round(january.current_portion * 100)
  assert by_account[a["interest"]] == 18_000
  assert by_account[a["cash"]] == -round(january.payment * 100)
  # The note takes the principal repaid and gives up the next twelve months'.
  assert by_account[a["note"]] == principal + current
  assert by_account[a["current"]] == -current


def test_legs_that_do_not_balance_are_refused_with_the_period(ext_session):
  session = ext_session
  a = _accounts(session)
  legs = [
    ScheduleLegRequest(
      element_id=a["interest"], side="debit", amount_source="interest"
    ),
    ScheduleLegRequest(element_id=a["cash"], side="credit", amount_source="payment"),
  ]
  with pytest.raises(ValueError, match="2026-01: the legs do not balance"):
    create_schedule(session, _term_loan(a, legs), created_by="usr")
