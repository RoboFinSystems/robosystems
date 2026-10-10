"""The reconciliation suite on a subsidiary's books, against real Postgres.

Each entity keeps its own chart, calendar and close, so its reconciliations
are its own: the blocks, the balances they record, and the close they hold.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

import pytest
from sqlalchemy import select

from robosystems.models.api.extensions.reconciliations import (
  PreviewReconciliationsRequest,
  RecordStatementBalanceRequest,
  RefreshReconciliationsRequest,
  SetReconciliationPolicyRequest,
  SignOffReconciliationRequest,
)
from robosystems.models.api.extensions.schedules import (
  CreateScheduleRequest,
  EntryTemplateRequest,
  ScheduleMetadataRequest,
)
from robosystems.models.extensions import (
  Element,
  Entity,
  EntityTaxonomy,
  Structure,
  Taxonomy,
)
from robosystems.models.extensions.roboledger import Entry, Event, LineItem
from robosystems.operations.roboledger.commands.reconciliations import (
  StatementAccountError,
  preview_reconciliations,
  record_statement_balance,
  refresh_reconciliations,
  set_reconciliation_policy,
  sign_off_reconciliation,
)
from robosystems.operations.roboledger.commands.schedules import create_schedule
from robosystems.operations.roboledger.entity_scope import EntityNotInGraphError
from robosystems.operations.roboledger.reads.reconciliations import (
  list_reconciliations,
  unreconciled_for_close,
)
from robosystems.operations.roboledger.reconciliations import (
  NoSourceLedgerError,
  NothingToReconcileError,
  SourceLedgerResolver,
)
from tests.ledger_entity import PARENT_ENTITY_ID

from .conftest import GRAPH_ID, classified_account, entry

pytestmark = pytest.mark.unit

SUB = "ent_test_sub"
_COMMANDS = "robosystems.operations.roboledger.commands.reconciliations"


def _sub_entry(session, posting_date: date, debit: str, credit: str, cents: int):
  booked = Entry(
    entity_id=SUB,
    posting_date=posting_date,
    status="posted",
    type="standard",
    provenance="manual_entry",
    created_by="test",
  )
  session.add(booked)
  session.flush()
  session.add(
    LineItem(entry_id=booked.id, element_id=debit, debit_amount=cents, line_order=1)
  )
  session.add(
    LineItem(entry_id=booked.id, element_id=credit, credit_amount=cents, line_order=2)
  )
  session.flush()


@pytest.fixture()
def group(ext_session):
  """The parent and a subsidiary with a chart of its own. At 2026-08-31 the
  parent's checking holds 4,800.00 and the subsidiary's 2,000.00."""
  session = ext_session
  session.add(
    Entity(
      id=SUB,
      name="Sub LLC",
      is_parent=False,
      parent_entity_id=PARENT_ENTITY_ID,
      source="native",
      created_by="test",
    )
  )
  chart = Taxonomy(
    name="Sub LLC chart", taxonomy_type="chart_of_accounts", created_by="test"
  )
  session.add(chart)
  session.flush()
  session.add(
    EntityTaxonomy(
      entity_id=SUB,
      taxonomy_id=chart.id,
      basis="chart_of_accounts",
      is_primary=True,
    )
  )
  session.flush()

  def sub_account(name: str, trait: str, balance_type: str = "debit") -> str:
    element_id = classified_account(session, name, trait, balance_type=balance_type)
    session.get(Element, element_id).taxonomy_id = chart.id
    session.flush()
    return element_id

  parent = {
    "cash": classified_account(session, "Checking", "asset"),
    "capital": classified_account(
      session, "Owner Capital", "equity", balance_type="credit"
    ),
  }
  sub = {
    "cash": sub_account("Sub Checking", "asset"),
    "capital": sub_account("Sub Capital", "equity", "credit"),
    "prepaid": sub_account("Sub Prepaid", "asset"),
    "software": sub_account("Sub Software", "expense"),
  }
  entry(session, date(2026, 1, 10), parent["cash"], parent["capital"], 480_000)
  _sub_entry(session, date(2026, 2, 1), sub["cash"], sub["capital"], 200_000)
  session.commit()
  return session, parent, sub


def _record(session, element_id, balance, *, entity_id=None):
  result = record_statement_balance(
    session,
    RecordStatementBalanceRequest(
      element_id=element_id,
      entity_id=entity_id,
      as_of=date(2026, 8, 31),
      balance=balance,
    ),
    graph_id=GRAPH_ID,
    created_by="usr",
  )
  session.commit()
  return result


def _refresh(session, *, entity_id=None, fetch=None):
  fetch = fetch or patch.object(
    SourceLedgerResolver, "_fetch", side_effect=NoSourceLedgerError("no source")
  )
  with fetch:
    result = refresh_reconciliations(
      session,
      RefreshReconciliationsRequest(period="2026-08", entity_id=entity_id),
      graph_id=GRAPH_ID,
      created_by="usr",
    )
  session.commit()
  return result


def _listed(session, entity_id=None) -> list[str]:
  return [
    rec.name
    for rec in list_reconciliations(
      session, "2026-08", entity_id=entity_id
    ).reconciliations
  ]


def test_a_subsidiarys_statement_is_on_its_own_books(group):
  session, _parent, sub = group

  rec = _record(session, sub["cash"], 2_000.00, entity_id=SUB)

  assert (rec.status, rec.ledger_balance, rec.independent_balance) == (
    "reconciled",
    2_000.00,
    2_000.00,
  )
  assert session.get(Structure, rec.structure_id).entity_id == SUB
  (observation,) = session.execute(
    select(Event).where(Event.event_type == "balance_observed")
  ).scalars()
  assert observation.entity_id == SUB
  assert _listed(session, SUB) == ["Sub Checking (statement)"]
  assert _listed(session) == []


def test_an_account_in_another_entitys_chart_is_refused(group):
  session, parent, sub = group

  with pytest.raises(StatementAccountError, match="this entity's chart"):
    _record(session, parent["cash"], 4_800.00, entity_id=SUB)
  session.rollback()
  # Without an entity the parent is meant, and the account is not its.
  with pytest.raises(StatementAccountError, match="this entity's chart"):
    _record(session, sub["cash"], 2_000.00)


def test_an_entity_outside_the_graph_is_refused(group):
  session, _parent, sub = group

  with pytest.raises(EntityNotInGraphError):
    _record(session, sub["cash"], 2_000.00, entity_id="ent_nowhere")
  session.rollback()
  with pytest.raises(EntityNotInGraphError):
    _refresh(session, entity_id="ent_nowhere")


def test_a_subsidiary_refresh_never_reads_the_source_ledger(group):
  """QuickBooks keeps the parent's books only, so a subsidiary's refresh
  compares its schedules and creates its blocks without asking for it."""
  session, _parent, sub = group
  _sub_entry(session, date(2026, 1, 5), sub["prepaid"], sub["cash"], 120_000)
  create_schedule(
    session,
    CreateScheduleRequest(
      name="Sub annual license",
      element_ids=[sub["software"], sub["prepaid"]],
      period_start=date(2026, 9, 1),
      period_end=date(2027, 8, 31),
      monthly_amount=10_000,
      entry_template=EntryTemplateRequest(
        debit_element_id=sub["software"], credit_element_id=sub["prepaid"]
      ),
      schedule_metadata=ScheduleMetadataRequest(
        original_amount=120_000, booked_on=date(2026, 1, 5)
      ),
    ),
    created_by="usr",
    entity_id=SUB,
  )
  session.commit()

  result = _refresh(
    session,
    entity_id=SUB,
    fetch=patch.object(
      SourceLedgerResolver, "resolve", side_effect=AssertionError("read QuickBooks")
    ),
  )

  (rec,) = result.reconciliations
  assert (rec.name, rec.status, rec.required_for_close) == (
    "Sub Prepaid (schedules)",
    "reconciled",
    True,
  )
  assert session.get(Structure, rec.structure_id).entity_id == SUB
  assert _listed(session) == []


def test_a_subsidiary_with_nothing_to_reconcile_says_so(group):
  """The parent having blocks is not the subsidiary having any."""
  session, parent, _sub = group
  _record(session, parent["cash"], 4_800.00)

  with pytest.raises(NothingToReconcileError):
    _refresh(session, entity_id=SUB)


def test_the_source_ledger_check_is_refused_for_a_subsidiary(group):
  session, _parent, _sub = group

  with (
    patch.object(SourceLedgerResolver, "_fetch") as fetch,
    pytest.raises(NoSourceLedgerError, match="group parent's books are synced"),
  ):
    preview_reconciliations(
      session,
      PreviewReconciliationsRequest(
        period="2026-08", method="source_ledger", entity_id=SUB
      ),
      graph_id=GRAPH_ID,
    )
  fetch.assert_not_called()


def test_a_subsidiary_block_holds_only_its_own_close(group):
  session, parent, sub = group
  rec = _record(session, sub["cash"], 1_900.00, entity_id=SUB)
  set_reconciliation_policy(
    session,
    SetReconciliationPolicyRequest(
      structure_id=rec.structure_id, required_for_close=True
    ),
    graph_id=GRAPH_ID,
    created_by="usr",
  )
  _record(session, parent["cash"], 4_800.00)
  session.commit()

  assert [
    r.name for r in unreconciled_for_close(session, "2026-08", entity_id=SUB)
  ] == ["Sub Checking (statement)"]
  assert unreconciled_for_close(session, "2026-08") == []


def test_a_subsidiary_block_is_signed_off_on_its_own_books(group):
  session, _parent, sub = group
  rec = _record(session, sub["cash"], 2_000.00, entity_id=SUB)

  with patch(f"{_COMMANDS}._explicit_write_members", return_value={"usr", "rev"}):
    signed = sign_off_reconciliation(
      session,
      SignOffReconciliationRequest(structure_id=rec.structure_id, period="2026-08"),
      graph_id=GRAPH_ID,
      created_by="rev",
    )
  session.commit()

  assert (signed.status, signed.reviewed_by) == ("reviewed", "rev")
  (sign_off,) = session.execute(
    select(Event).where(Event.event_type == "reconciliation_signed_off")
  ).scalars()
  assert sign_off.entity_id == SUB
