"""A schedule whose debit account is changed mid-life, against real Postgres:
every reader of its facts still finds the months it has left."""

from __future__ import annotations

from datetime import date

import pytest

from robosystems.models.api.extensions.schedules import (
  EntryTemplateRequest,
  UpdateScheduleRequest,
)
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.operations.event_block.promotion import (
  find_stranded_obligations,
  promote_pending_obligations,
)
from robosystems.operations.roboledger.commands.schedules import update_schedule
from robosystems.operations.roboledger.reconciliations.engine import (
  _undrafted_schedule_balances,
)
from robosystems.operations.roboledger.reconciliations.resolvers import (
  _recognized_by_schedule,
)
from robosystems.operations.roboledger.schedules.service import ScheduleService

from .test_ledger_write_guards_db import (  # noqa: F401  (ext_session is a fixture)
  AS_OF,
  GRAPH_ID,
  JAN_END,
  JAN_START,
  _element,
  _schedule,
  ext_session,
)

pytestmark = pytest.mark.unit


@pytest.fixture()
def edited(ext_session):  # noqa: F811
  session = ext_session
  structure_id, _old_debit, credit = _schedule(session)
  new_debit = _element(session, "Amortization Expense")
  session.commit()
  update_schedule(
    session,
    UpdateScheduleRequest(
      structure_id=structure_id,
      entry_template=EntryTemplateRequest(
        debit_element_id=new_debit, credit_element_id=credit
      ),
    ),
    updated_by="usr",
  )
  session.commit()
  return session, structure_id, new_debit


def _drafting(session, structure_id, new_debit):
  promote_pending_obligations(session, GRAPH_ID, as_of=AS_OF, dispatch_handlers=True)
  session.commit()
  drafts = session.query(Entry).filter(Entry.source_structure_id == structure_id)
  lines = session.query(LineItem).filter(
    LineItem.entry_id.in_([entry.id for entry in drafts])
  )
  assert any(
    line.element_id == new_debit and line.debit_amount == 10_000 for line in lines
  )
  assert find_stranded_obligations(session, as_of=AS_OF) == []


def _close_status(session, structure_id, _new_debit):
  status = ScheduleService().get_period_close_status(session, JAN_START, JAN_END)
  (item,) = [s for s in status.schedules if s.structure_id == structure_id]
  assert item.amount == 100.0


def _register(session, structure_id, _new_debit):
  recognized = _recognized_by_schedule(session, date(2026, 1, 31))[structure_id]
  assert recognized.started
  assert recognized.planned == 30_000


def _close_gate(session, _structure_id, new_debit):
  # January has matured and is not drafted yet: the gate counts it on the
  # account the draft will post to.
  balances = _undrafted_schedule_balances(session, date(2026, 2, 15), None)
  assert balances.get(new_debit) == 10_000


@pytest.mark.parametrize(
  "reader",
  [_drafting, _close_status, _register, _close_gate],
  ids=["drafting", "close-status", "schedule-register", "close-gate"],
)
def test_a_debit_account_change_leaves_the_schedule_readable(edited, reader):
  session, structure_id, new_debit = edited
  reader(session, structure_id, new_debit)
