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
  _skip_platform_db_checks,  # noqa: F401  (autouse: stubs the platform-DB checks)
)
from tests.operations.roboledger.commands.test_reconciling_items_db import (
  session as _session_fixture,  # noqa: F401  (registers the `session` fixture)
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
  pair = session.query(Event).filter(Event.external_id == "plaid_xfer_t_in").one()
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
