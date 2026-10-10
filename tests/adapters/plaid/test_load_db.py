"""The transfer-leg paths that only a real database can prove.

Settling a leg against a counterpart already on the graph runs the kernel's
own capture and transition inside the load's session — the row locks, the
period fence, the flush ordering — and merging deletes a live event under
the pair that replaces it. The in-memory stubs in ``test_load.py`` cannot
model either; these run them against ``robosystems_test``.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from robosystems.adapters.bank_feed.chart import BankAccount, ChartIndex
from robosystems.adapters.plaid.client import TransactionsSync
from robosystems.adapters.plaid.pipeline.load import load_sync
from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.extensions.entity import Entity
from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.event_block.commands import (
  create_event_block_in_session,
  fire_handler_on_commit,
)
from tests.adapters.plaid.fixtures import txn
from tests.operations.roboledger.commands.test_reconciling_items_db import (
  CASH,
  GRAPH_ID,
  NEW_EXPENSE,
  _seed_elements,
)

pytestmark = pytest.mark.unit


CONNECTION = "conn_plaid_1"
ITEM = "item_plaid_1"
CHECKING, SAVINGS = "acc_chk", "acc_sav"


def _accounts() -> list[BankAccount]:
  return [
    BankAccount(CHECKING, "Checking ••1234", "checking", "asset", "debit", "Bank"),
    BankAccount(SAVINGS, "Savings ••5678", "savings", "asset", "debit", "Bank"),
  ]


def _outflow(db, elements, *, status: str) -> Event:
  """Leg A: $500 out of checking, classified by the operator to the savings
  account — captured, classified, or committed (posted)."""
  event, _envelope = create_event_block_in_session(
    db,
    CreateEventBlockRequest(
      event_type="external_transfer",
      event_category="treasury",
      event_class="economic",
      event_action="transfer",
      resource_type="money",
      source="plaid",
      external_id="plaid_txn_t_out",
      occurred_at=datetime(2026, 7, 9),
      amount=-50000,
      resource_element_id=elements[CASH],
      apply_handlers=False,
      metadata={
        "connection_id": CONNECTION,
        "item_id": ITEM,
        "transaction_id": "t_out",
        "account_id": CHECKING,
        "account_name": "Checking ••1234",
        "transfer_candidate": True,
        "classified_element_id": elements[NEW_EXPENSE],
      },
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )
  if status != "captured":
    event.status = status
    if status == "committed":
      fire_handler_on_commit(db, event, "user_test")
  db.flush()
  return event


def _inflow_sync() -> TransactionsSync:
  """Leg B: the $500 arriving on savings two days later."""
  return TransactionsSync(
    added=[
      txn(
        "t_in",
        SAVINGS,
        -500.00,
        "2026-07-11",
        name="ONLINE TRANSFER FROM CHK 1234",
        primary="TRANSFER_IN",
        detailed="TRANSFER_IN_ACCOUNT_TRANSFER",
      )
    ],
    next_cursor="c1",
    update_status="HISTORICAL_UPDATE_COMPLETE",
  )


def _load(db, elements):
  return load_sync(
    db,
    graph_id=GRAPH_ID,
    connection_id=CONNECTION,
    item_id=ITEM,
    created_by="user_test",
    accounts=_accounts(),
    sync=_inflow_sync(),
    account_elements={CHECKING: elements[CASH], SAVINGS: elements[NEW_EXPENSE]},
    chart=ChartIndex(),
  )


def test_a_leg_classified_to_the_other_bank_account_merges_into_a_pair(session):
  elements = _seed_elements(session)
  outflow = _outflow(session, elements, status="classified")
  outflow_id = str(outflow.id)

  report = _load(session, elements)
  session.flush()

  assert report.transfers_matched == 1 and report.events_created == 1
  assert session.get(Event, outflow_id) is None  # the single leg is gone
  pair = session.query(Event).filter(Event.external_id == "plaid_xfer_t_in_t_out").one()
  assert pair.event_type == "internal_transfer" and pair.status == "captured"
  assert pair.amount == 50000 and pair.resource_element_id == elements[NEW_EXPENSE]
  assert pair.metadata_["legs"] == ["t_out", "t_in"]
  assert pair.metadata_["from_element_id"] == elements[CASH]
  assert (pair.metadata_["from_date"], pair.metadata_["to_date"]) == (
    "2026-07-09",
    "2026-07-11",
  )


def test_a_leg_posted_to_the_other_bank_account_voids_the_arriving_one(session):
  elements = _seed_elements(session)
  outflow = _outflow(session, elements, status="committed")

  report = _load(session, elements)
  session.flush()

  assert report.legs_voided == 1 and report.transfers_matched == 0
  # The posted leg is untouched; the arriving leg exists, voided against it.
  assert session.get(Event, str(outflow.id)).status == "committed"
  inflow = session.query(Event).filter(Event.external_id == "plaid_txn_t_in").one()
  assert inflow.status == "voided"
  assert inflow.metadata_["counterpart_event_id"] == str(outflow.id)
  assert inflow.metadata_["counterpart_status"] == "committed"
  assert str(outflow.id) in inflow.metadata_["voided_reason"]
  assert session.query(Event).filter(Event.source == "plaid").count() == 2


def test_a_captured_line_is_stored_on_its_accounts_entity(session):
  """The transform stamps each line with its account's entity; the stored row
  must carry it, not fall to the group parent (a subsidiary's bank feed
  beside a QuickBooks-kept parent)."""
  elements = _seed_elements(session)
  session.add(
    Entity(
      id="ent_sub",
      name="Subsidiary LLC",
      is_parent=False,
      parent_entity_id="ent_test_parent",
      created_by="test",
    )
  )
  session.flush()
  sync = TransactionsSync(
    added=[
      txn(
        "t_fee",
        SAVINGS,
        9.50,
        "2026-07-12",
        name="MONTHLY SERVICE FEE",
        primary="BANK_FEES",
        detailed="BANK_FEES_OTHER_BANK_FEES",
      )
    ],
    next_cursor="c1",
    update_status="HISTORICAL_UPDATE_COMPLETE",
  )
  report = load_sync(
    session,
    graph_id=GRAPH_ID,
    connection_id=CONNECTION,
    item_id=ITEM,
    created_by="user_test",
    accounts=_accounts(),
    sync=sync,
    account_elements={CHECKING: elements[CASH], SAVINGS: elements[NEW_EXPENSE]},
    chart=ChartIndex(),
    account_entities={SAVINGS: "ent_sub"},
  )
  assert report.events_created == 1, report.errors
  stored = session.query(Event).filter(Event.external_id == "plaid_txn_t_fee").one()
  assert stored.entity_id == "ent_sub"


def test_a_replay_never_rekeys_a_balance_reading_as_a_line(session):
  """A first sync records the day's balance before it loads the lines. A
  deposit equal to that balance (a new account's first) must be captured, not
  matched to the reading by account, day and amount."""
  elements = _seed_elements(session)
  reading = Event(
    entity_id="ent_test_parent",
    event_type="balance_observed",
    event_category="reconciliation",
    event_class="support",
    resource_type="money",
    resource_element_id=elements[CASH],
    occurred_at=datetime(2026, 7, 12, 18, 0),
    effective_at=datetime(2026, 7, 12),
    status="committed",
    source="plaid",
    external_id="plaid_balance_bank_current_acc_chk_20260712T180000Z",
    amount=50000,
    metadata_={
      "kind": "bank_current",
      "as_of": "2026-07-12",
      "account_id": CHECKING,
      "connection_id": CONNECTION,
    },
    created_by="user_test",
  )
  session.add(reading)
  session.flush()
  sync = TransactionsSync(
    added=[
      txn(
        "t_first",
        CHECKING,
        -500.00,
        "2026-07-12",
        name="OPENING DEPOSIT",
        primary="TRANSFER_IN",
        detailed="TRANSFER_IN_DEPOSIT",
      )
    ],
    next_cursor="c1",
    update_status="HISTORICAL_UPDATE_COMPLETE",
  )
  report = load_sync(
    session,
    graph_id=GRAPH_ID,
    connection_id=CONNECTION,
    item_id=ITEM,
    created_by="user_test",
    accounts=_accounts(),
    sync=sync,
    account_elements={CHECKING: elements[CASH], SAVINGS: elements[NEW_EXPENSE]},
    chart=ChartIndex(),
    rekey_replaced=True,
  )
  assert report.events_rekeyed == 0
  assert report.events_created == 1, report.errors
  session.refresh(reading)
  assert reading.external_id == "plaid_balance_bank_current_acc_chk_20260712T180000Z"
  assert "rekeyed_from" not in reading.metadata_


def _merchant_sync(transaction_id: str, day: str) -> TransactionsSync:
  return TransactionsSync(
    added=[
      txn(
        transaction_id,
        CHECKING,
        1200.00,
        day,
        name="GUSTO PAYROLL",
        primary="GENERAL_SERVICES",
        detailed="GENERAL_SERVICES_OTHER_GENERAL_SERVICES",
        merchant="Gusto",
        entity_id="ent_gusto",
      )
    ],
    next_cursor=f"c_{transaction_id}",
    update_status="HISTORICAL_UPDATE_COMPLETE",
  )


def test_a_committed_line_teaches_the_next_line_from_its_counterparty(session):
  from robosystems.models.api.event_block import UpdateEventBlockRequest
  from robosystems.operations.event_block.commands import update_event_block

  elements = _seed_elements(session)

  def pull(transaction_id: str, day: str) -> Event:
    report = load_sync(
      session,
      graph_id=GRAPH_ID,
      connection_id=CONNECTION,
      item_id=ITEM,
      created_by="user_test",
      accounts=_accounts(),
      sync=_merchant_sync(transaction_id, day),
      account_elements={CHECKING: elements[CASH], SAVINGS: elements[NEW_EXPENSE]},
      chart=ChartIndex(),
    )
    assert report.events_created == 1, report.errors
    session.flush()
    return (
      session.query(Event)
      .filter(Event.external_id == f"plaid_txn_{transaction_id}")
      .one()
    )

  first = pull("t_g1", "2026-07-02")
  assert first.agent_id is not None
  update_event_block(
    session,
    UpdateEventBlockRequest(
      event_id=str(first.id),
      transition_to="committed",
      metadata_patch={"classified_element_id": elements[NEW_EXPENSE]},
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )

  second = pull("t_g2", "2026-07-16")
  assert second.agent_id == first.agent_id
  assert second.metadata_["suggested_element_id"] == elements[NEW_EXPENSE]
  assert second.metadata_["suggestion_source"] == "agent_default"
  assert session.get(Event, str(first.id)).metadata_["suggestion_outcome"] in (
    "none",
    "overridden",
  )
