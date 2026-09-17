"""Load: removals, refreshes, cross-sync transfer legs, and the orchestration."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from robosystems.adapters.bank_feed.chart import ChartIndex
from robosystems.adapters.plaid.client import TransactionsSync
from robosystems.adapters.plaid.pipeline.load import (
  PlaidLoadReport,
  _flag_removed,
  _merge_into_pair,
  apply_removed,
  load_sync,
  merge_legs,
  reconcile_existing,
  survivor_payload,
)
from robosystems.adapters.plaid.pipeline.transform import bank_accounts
from tests.adapters.plaid.fixtures import (
  CARD_ID,
  CHECKING_ID,
  INSTITUTION,
  ITEM_ID,
  SAVINGS_ID,
  accounts,
  transactions,
)

MODULE = "robosystems.adapters.plaid.pipeline.load"
ELEMENTS = {CHECKING_ID: "e_chk", SAVINGS_ID: "e_sav", CARD_ID: "e_card"}


class _Session:
  def __init__(self):
    self.nested = 0
    self.flushes = 0

  @contextmanager
  def begin_nested(self):
    self.nested += 1
    yield

  def flush(self):
    self.flushes += 1


def _event(**fields):
  base = {
    "id": "evt_1",
    "status": "captured",
    "amount": -1240,
    "occurred_at": datetime(2026, 3, 14),
    "description": "Harbor Coffee Co",
    "agent_id": None,
    "currency": "USD",
    "resource_element_id": "e_card",
    "external_id": "plaid_txn_t_coffee",
    "payload_drift": False,
    "metadata_": {"connection_id": "conn_1", "transaction_id": "t_coffee"},
  }
  base.update(fields)
  return SimpleNamespace(**base)


def _payload(amount=-1240, day="2026-03-14", **metadata):
  return {
    "external_id": "plaid_txn_t_coffee",
    "amount": amount,
    "occurred_at": f"{day}T00:00:00Z",
    "description": "Harbor Coffee Co",
    "resource_element_id": "e_card",
    "metadata": {
      "connection_id": "conn_1",
      "transaction_id": "t_coffee",
      "account_id": CARD_ID,
      "account_name": "Harborline Bank Business Card ••9012",
      "posted_date": day,
      **metadata,
    },
  }


@pytest.mark.unit
class TestReconcileExisting:
  def test_captured_event_takes_the_new_hint_amount_and_date(self):
    event, report = _event(), PlaidLoadReport()
    reconcile_existing(
      event,
      _payload(
        amount=-1300,
        day="2026-03-15",
        suggested_account_name="Business meals",
        suggested_element_id="e_te",
      ),
      report,
    )
    assert report.events_updated == 1
    assert event.amount == -1300
    assert event.occurred_at == datetime(2026, 3, 15)
    assert event.metadata_["suggested_element_id"] == "e_te"

  def test_unchanged_captured_event_is_existing(self):
    event, report = _event(), PlaidLoadReport()
    reconcile_existing(event, _payload(), report)
    assert (report.events_existing, report.events_updated) == (1, 0)

  def test_posted_event_records_the_change_and_keeps_the_books(self):
    event, report = _event(status="committed"), PlaidLoadReport()
    reconcile_existing(event, _payload(amount=-1300), report)
    assert event.amount == -1240
    change = event.metadata_["source_change"]
    assert change["amount"] == -1300 and "detected_at" in change
    assert report.source_changes == 1
    assert not event.payload_drift

  def test_posted_event_with_only_hint_changes_is_existing(self):
    event, report = _event(status="committed"), PlaidLoadReport()
    reconcile_existing(event, _payload(suggested_account_name="Rent"), report)
    assert report.events_existing == 1
    assert "suggested_account_name" not in event.metadata_


@pytest.mark.unit
class TestRemovals:
  def test_flagging_a_posted_line_stashes_a_payload_with_no_entry(self):
    event = _event(status="committed")
    _flag_removed(event, ["t_coffee"])
    assert event.payload_drift is True
    accepted = event.metadata_["drift_payload"]
    assert accepted["source_removed"] is True
    assert accepted["source_removed_transaction_ids"] == ["t_coffee"]
    assert "drift_payload" not in accepted
    assert "drift_detected_at" in event.metadata_

  def test_unposted_deleted_posted_flagged_and_a_pairs_survivor_restored(self):
    captured = _event(id="evt_cap", external_id="plaid_txn_a")
    posted = _event(id="evt_post", external_id="plaid_txn_b", status="committed")
    pair = _event(
      id="evt_pair",
      external_id="plaid_xfer_c",
      amount=50000,
      occurred_at=datetime(2026, 3, 19),
      metadata_={
        "connection_id": "conn_1",
        "item_id": ITEM_ID,
        "legs": ["c_out", "c_in"],
        "from_account_id": CHECKING_ID,
        "to_account_id": SAVINGS_ID,
        "from_element_id": "e_chk",
        "to_element_id": "e_sav",
        "from_account_name": "Checking",
        "bank_description": "ONLINE TRANSFER",
      },
    )
    report = PlaidLoadReport()
    with (
      patch(
        f"{MODULE}.existing_events",
        return_value={"plaid_txn_a": captured, "plaid_txn_b": posted},
      ),
      patch(f"{MODULE}.pair_events_by_leg", return_value={"c_in": pair}),
      patch(f"{MODULE}._delete_events") as delete,
      patch(f"{MODULE}.capture_event") as capture,
    ):
      consumed = apply_removed(
        _Session(),
        report,
        ["a", "b", "c_in"],
        graph_id="kg_1",
        connection_id="conn_1",
        created_by="usr_1",
      )
    assert consumed == {"evt_cap", "evt_pair"}
    assert delete.call_args.args[1] == ["evt_cap", "evt_pair"]
    assert report.events_removed == 2 and report.reconciling_items == 1
    assert posted.payload_drift is True
    survivor = capture.call_args.args[1]
    assert survivor["external_id"] == "plaid_txn_c_out"
    assert survivor["event_type"] == "external_transfer"
    assert survivor["amount"] == -50000
    assert survivor["resource_element_id"] == "e_chk"
    assert survivor["metadata"]["transfer_candidate"] is True

  def test_a_pair_that_loses_both_legs_leaves_nothing(self):
    pair = _event(amount=50000, metadata_={"legs": ["x", "y"]})
    assert survivor_payload(pair, ["x", "y"]) is None

  def test_nothing_removed_is_a_no_op(self):
    report = PlaidLoadReport()
    with patch(f"{MODULE}.existing_events") as existing:
      assert (
        apply_removed(
          _Session(), report, [], graph_id="g", connection_id="c", created_by="u"
        )
        == set()
      )
    existing.assert_not_called()


@pytest.mark.unit
class TestTransferLegsAcrossSyncs:
  def _waiting(self):
    return _event(
      id="evt_wait",
      amount=-50000,
      occurred_at=datetime(2026, 3, 17),
      resource_element_id="e_chk",
      metadata_={
        "connection_id": "conn_1",
        "transaction_id": "t_out",
        "account_id": CHECKING_ID,
        "account_name": "Checking",
      },
    )

  def _incoming(self):
    return {
      "external_id": "plaid_txn_t_in",
      "amount": 50000,
      "occurred_at": "2026-03-19T00:00:00Z",
      "resource_element_id": "e_sav",
      "metadata": {
        "transaction_id": "t_in",
        "account_id": SAVINGS_ID,
        "account_name": "Savings",
        "transfer_candidate": True,
      },
    }

  def test_merge_orders_the_legs_by_direction(self):
    merged = merge_legs(
      self._waiting(), self._incoming(), connection_id="conn_1", item_id=ITEM_ID
    )
    assert merged["event_type"] == "internal_transfer"
    assert merged["external_id"] == "plaid_xfer_t_in"
    assert merged["metadata"]["legs"] == ["t_out", "t_in"]
    assert merged["metadata"]["from_element_id"] == "e_chk"
    assert merged["metadata"]["to_element_id"] == "e_sav"
    assert merged["occurred_at"] == "2026-03-19T00:00:00Z"

  def test_merge_replaces_the_waiting_leg(self):
    report = PlaidLoadReport()
    with (
      patch(f"{MODULE}._delete_events") as delete,
      patch(f"{MODULE}.capture_event", return_value=True) as capture,
    ):
      assert _merge_into_pair(
        _Session(),
        self._waiting(),
        self._incoming(),
        graph_id="kg_1",
        connection_id="conn_1",
        item_id=ITEM_ID,
        created_by="usr_1",
        report=report,
      )
    assert delete.call_args.args[1] == ["evt_wait"]
    assert capture.call_args.args[1]["event_type"] == "internal_transfer"
    assert report.transfers_matched == 1

  def test_a_merge_that_cannot_capture_rolls_back(self):
    report = PlaidLoadReport(events_failed=2, errors=["earlier"])

    def failed_capture(session, payload, *, graph_id, created_by, report):
      report.events_failed += 1
      report.errors.append("pair failed")
      return False

    with (
      patch(f"{MODULE}._delete_events"),
      patch(f"{MODULE}.capture_event", side_effect=failed_capture),
    ):
      assert not _merge_into_pair(
        _Session(),
        self._waiting(),
        self._incoming(),
        graph_id="kg_1",
        connection_id="conn_1",
        item_id=ITEM_ID,
        created_by="usr_1",
        report=report,
      )
    assert report.events_failed == 2 and report.errors == ["earlier"]
    assert report.transfers_matched == 0


@pytest.mark.unit
class TestLoadSync:
  def _load(self, *, existing=None, waiting=None, removed=(), modified=()):
    captured: list[dict] = []
    session = _Session()
    sync = TransactionsSync(
      added=transactions(), modified=list(modified), removed=list(removed)
    )
    with (
      patch(f"{MODULE}.apply_removed", return_value=set()) as removals,
      patch(f"{MODULE}.existing_events", return_value=existing or {}),
      patch(f"{MODULE}.pair_events_by_leg", return_value={}),
      patch(f"{MODULE}.ensure_agents", return_value=({}, 3)),
      patch(f"{MODULE}.find_waiting_leg", return_value=waiting),
      patch(f"{MODULE}._merge_into_pair", return_value=True) as merge,
      patch(
        f"{MODULE}.capture_event",
        side_effect=lambda s, payload, **kw: captured.append(payload) or True,
      ),
    ):
      report = load_sync(
        session,
        graph_id="kg_1",
        connection_id="conn_1",
        item_id=ITEM_ID,
        created_by="usr_1",
        accounts=bank_accounts(accounts(), institution=INSTITUTION),
        sync=sync,
        account_elements=ELEMENTS,
        chart=ChartIndex(),
      )
    return report, captured, removals, merge, session

  def test_fresh_item_captures_every_event(self):
    report, captured, removals, merge, session = self._load()
    assert report.agents_created == 3
    assert len(captured) == 8
    assert report.earliest_occurred_at == "2025-12-30T00:00:00Z"
    removals.assert_called_once()
    merge.assert_not_called()
    assert session.flushes == 1

  def test_existing_events_are_reconciled_not_captured(self):
    existing = {"plaid_txn_t_coffee": _event()}
    report, captured, *_ = self._load(existing=existing)
    assert "plaid_txn_t_coffee" not in {p["external_id"] for p in captured}
    # The stored line predates the merchant and category keys: a refresh.
    assert report.events_updated == 1
    assert existing["plaid_txn_t_coffee"].metadata_["merchant_name"] == (
      "Harbor Coffee Co"
    )

  def test_a_waiting_leg_is_merged_instead_of_captured(self):
    report, captured, _removals, merge, _session = self._load(waiting=_event())
    # The one unpaired transfer candidate (the owner draw) went to the merge.
    assert merge.call_count == 1
    assert "plaid_txn_t_owner_draw" not in {p["external_id"] for p in captured}

  def test_a_modified_row_supersedes_its_added_row(self):
    changed = transactions()[0]
    changed["amount"] = 20.00
    _report, captured, *_ = self._load(modified=[changed])
    coffee = next(p for p in captured if p["external_id"] == "plaid_txn_t_coffee")
    assert coffee["amount"] == -2000

  def test_removed_ids_reach_the_removal_step(self):
    _report, _captured, removals, *_ = self._load(
      removed=[{"transaction_id": "t_gone", "account_id": CARD_ID}, {}]
    )
    assert removals.call_args.args[2] == ["t_gone"]


@pytest.mark.unit
def test_report_counts_include_the_plaid_outcomes():
  counts = PlaidLoadReport(events_removed=2, transfers_matched=1).as_counts()
  assert counts["events_removed"] == 2 and counts["transfers_matched"] == 1
  assert {"reconciling_items", "source_changes", "events_captured"} <= set(counts)
