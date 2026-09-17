"""Load: removals, refreshes, cross-sync transfer legs, and the orchestration."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import ANY, patch

import pytest

from robosystems.adapters.bank_feed.chart import ChartIndex
from robosystems.adapters.plaid.client import TransactionsSync
from robosystems.adapters.plaid.pipeline.load import (
  PlaidLoadReport,
  _flag_removed,
  _merge_into_pair,
  apply_removed,
  find_waiting_leg,
  load_sync,
  merge_legs,
  reconcile_existing,
  reconcile_pairs,
  rekey_replaced_events,
  settle_with_counterpart,
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
    "event_type": "bank_transaction",
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

  def test_classified_event_takes_the_amount_but_keeps_its_hints(self):
    event = _event(
      status="classified",
      metadata_={
        "connection_id": "conn_1",
        "suggested_element_id": "e_te",
        "accept_suggestion": True,
      },
    )
    report = PlaidLoadReport()
    reconcile_existing(
      event, _payload(amount=-1300, suggested_element_id="e_other"), report
    )
    assert event.amount == -1300
    assert event.metadata_["suggested_element_id"] == "e_te"
    assert report.events_updated == 1 and not event.payload_drift

  def test_posted_change_is_a_reconciling_item_with_the_entry_it_should_have(self):
    event = _event(
      status="committed",
      metadata_={
        "connection_id": "conn_1",
        "transaction_id": "t_coffee",
        "classified_element_id": "e_te",
        "reconciliation_history": [{"disposition": "accept"}],
      },
    )
    report = PlaidLoadReport()
    reconcile_existing(event, _payload(amount=-1300, day="2026-03-15"), report)
    assert event.amount == -1240  # the books are untouched
    assert event.payload_drift is True and report.reconciling_items == 1
    accepted = event.metadata_["drift_payload"]
    assert accepted["source_amount"] == -1300
    assert accepted["source_posted_date"] == "2026-03-15"
    assert accepted["posting_date"] == "2026-03-15"
    assert "reconciliation_history" not in accepted
    # Money out: DR the classified account, CR the bank leg, at the new amount.
    assert accepted["line_items"] == [
      {"element_id": "e_te", "debit_amount": 1300, "credit_amount": 0},
      {"element_id": "e_card", "debit_amount": 0, "credit_amount": 1300},
    ]

  def test_a_change_already_raised_or_resolved_is_not_raised_again(self):
    pending = _event(status="committed", payload_drift=True)
    pending.metadata_ = {
      "connection_id": "conn_1",
      "drift_payload": {"source_amount": -1300, "source_posted_date": "2026-03-14"},
    }
    resolved = _event(
      status="committed",
      metadata_={"source_amount": -1300, "source_posted_date": "2026-03-14"},
    )
    for event in (pending, resolved):
      report = PlaidLoadReport()
      reconcile_existing(event, _payload(amount=-1300), report)
      assert report.events_existing == 1 and report.reconciling_items == 0

  def test_a_rule_posted_line_is_flagged_with_no_entry_to_restate(self):
    event = _event(status="committed")
    reconcile_existing(event, _payload(amount=-1300), PlaidLoadReport())
    accepted = event.metadata_["drift_payload"]
    assert event.payload_drift is True
    assert accepted["source_amount"] == -1300 and "line_items" not in accepted

  def test_posted_event_with_only_hint_changes_is_existing(self):
    event, report = _event(status="committed"), PlaidLoadReport()
    reconcile_existing(event, _payload(suggested_account_name="Rent"), report)
    assert report.events_existing == 1
    assert "suggested_account_name" not in event.metadata_
    assert not event.payload_drift

  def test_a_voided_event_is_left_alone(self):
    event, report = _event(status="voided"), PlaidLoadReport()
    reconcile_existing(event, _payload(amount=-9999), report)
    assert event.amount == -1240 and report.events_existing == 1


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
        "from_date": "2026-03-17",
        "to_date": "2026-03-19",
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
    # The outflow keeps its own date, not the pair's later one.
    assert survivor["occurred_at"] == "2026-03-17T00:00:00Z"

  def test_a_posted_pair_losing_a_leg_releases_the_survivor(self):
    pair = _pair(status="committed")
    report = PlaidLoadReport()
    with (
      patch(f"{MODULE}.existing_events", return_value={}),
      patch(f"{MODULE}.pair_events_by_leg", return_value={"t_xfer_in": pair}),
      patch(f"{MODULE}._delete_events") as delete,
      patch(f"{MODULE}.capture_event") as capture,
    ):
      consumed = apply_removed(
        _Session(),
        report,
        ["t_xfer_in"],
        graph_id="kg_1",
        connection_id="conn_1",
        created_by="usr_1",
      )
    assert consumed == set() and report.reconciling_items == 1
    delete.assert_not_called()
    # The pair is flagged for reversal and no longer claims the surviving leg.
    assert pair.payload_drift is True
    accepted = pair.metadata_["drift_payload"]
    assert accepted["source_removed"] is True and accepted["legs"] == ["t_xfer_in"]
    assert accepted["released_legs"] == ["t_xfer_out"]
    assert pair.metadata_["legs"] == ["t_xfer_in"]
    assert pair.metadata_["released_legs"] == ["t_xfer_out"]
    # The leg still real at the bank lives on as its own line, on its own date.
    survivor = capture.call_args.args[1]
    assert survivor["external_id"] == "plaid_txn_t_xfer_out"
    assert survivor["amount"] == -50000
    assert survivor["occurred_at"] == "2026-03-17T00:00:00Z"

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

  def test_a_rollback_at_the_error_cap_keeps_every_earlier_error(self):
    earlier = [f"e{i}" for i in range(10)]
    report = PlaidLoadReport(errors=list(earlier))

    def capped_capture(session, payload, *, graph_id, created_by, report):
      report.events_failed += 1
      return False

    with (
      patch(f"{MODULE}._delete_events"),
      patch(f"{MODULE}.capture_event", side_effect=capped_capture),
    ):
      _merge_into_pair(
        _Session(),
        self._waiting(),
        self._incoming(),
        graph_id="kg_1",
        connection_id="conn_1",
        item_id=ITEM_ID,
        created_by="usr_1",
        report=report,
      )
    assert report.errors == earlier

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
  def _load(
    self,
    *,
    existing=None,
    waiting=None,
    removed=(),
    modified=(),
    replay=False,
    old_events=(),
  ):
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
      patch(f"{MODULE}._replay_candidates", return_value=list(old_events)),
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
        rekey_replaced=replay,
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

  def test_a_replay_rekeys_the_old_items_events_instead_of_capturing(self):
    # The coffee line as an earlier Item captured and posted it, then purged:
    # no transaction id or description left, the classification intact.
    old = _event(
      status="committed",
      external_id="plaid_txn_old_coffee",
      metadata_={"connection_id": "conn_old", "classified_element_id": "e_te"},
    )
    report, captured, *_ = self._load(replay=True, old_events=[old])
    assert "plaid_txn_t_coffee" not in {p["external_id"] for p in captured}
    assert report.events_rekeyed == 1 and report.events_existing == 1
    assert old.external_id == "plaid_txn_t_coffee"
    assert old.metadata_["transaction_id"] == "t_coffee"
    assert old.metadata_["item_id"] == ITEM_ID
    assert old.metadata_["connection_id"] == "conn_1"
    assert old.metadata_["classified_element_id"] == "e_te"
    assert old.metadata_["rekeyed_from"] == [
      {
        "external_id": "plaid_txn_old_coffee",
        "at": old.metadata_["rekeyed_from"][0]["at"],
        "connection_id": "conn_old",
      }
    ]

  def test_an_incremental_sync_never_rekeys(self):
    old = _event(status="committed", external_id="plaid_txn_old_coffee")
    report, captured, *_ = self._load(replay=False, old_events=[old])
    assert "plaid_txn_t_coffee" in {p["external_id"] for p in captured}
    assert report.events_rekeyed == 0 and old.external_id == "plaid_txn_old_coffee"


@pytest.mark.unit
def test_report_counts_include_the_plaid_outcomes():
  counts = PlaidLoadReport(events_removed=2, transfers_matched=1).as_counts()
  assert counts["events_removed"] == 2 and counts["transfers_matched"] == 1
  assert {
    "reconciling_items",
    "events_captured",
    "events_rekeyed",
    "pairs_dissolved",
    "legs_voided",
  } <= set(counts)


def _pair(status="captured", **meta):
  """The fixture's checking → savings pair as the inbox holds it."""
  return _event(
    id="evt_pair",
    status=status,
    event_type="internal_transfer",
    amount=50000,
    occurred_at=datetime(2026, 3, 19),
    resource_element_id="e_sav",
    external_id="plaid_xfer_t_xfer_in",
    description="Transfer Checking to Savings",
    metadata_={
      "connection_id": "conn_1",
      "item_id": ITEM_ID,
      "legs": ["t_xfer_out", "t_xfer_in"],
      "from_account_id": CHECKING_ID,
      "to_account_id": SAVINGS_ID,
      "from_element_id": "e_chk",
      "to_element_id": "e_sav",
      "from_account_name": "Checking",
      "to_account_name": "Savings",
      "from_date": "2026-03-17",
      "to_date": "2026-03-19",
      "bank_description": "ONLINE TRANSFER FROM CHK 1234",
      **meta,
    },
  )


def _legs(**changes):
  """The pair's two fixture transactions, with ``amount`` / ``date`` overrides
  keyed by transaction id."""
  legs = {t["transaction_id"]: t for t in transactions()}
  out = [legs["t_xfer_out"], legs["t_xfer_in"]]
  for leg in out:
    for key, value in changes.get(leg["transaction_id"], {}).items():
      leg[key] = value
  return out


@pytest.mark.unit
class TestReconcilePairs:
  """A modified leg of a pair reconciles the pair, or dissolves it."""

  def _run(self, pair, txns, *, consumed=None):
    report = PlaidLoadReport()
    consumed = set() if consumed is None else consumed
    with patch(f"{MODULE}._delete_events") as delete:
      released = reconcile_pairs(
        _Session(),
        {t["transaction_id"]: pair for t in txns},
        {t["transaction_id"]: t for t in txns},
        by_account={
          a.account_id: a for a in bank_accounts(accounts(), institution=INSTITUTION)
        },
        account_elements=ELEMENTS,
        chart=ChartIndex(),
        agent_ids={},
        connection_id="conn_1",
        item_id=ITEM_ID,
        report=report,
        consumed=consumed,
      )
    return released, report, delete

  def test_an_unposted_pair_takes_a_change_its_legs_still_share(self):
    pair = _pair()
    both = {
      "t_xfer_out": {"amount": 600.0},
      "t_xfer_in": {"amount": -600.0, "date": "2026-03-20"},
    }
    released, report, delete = self._run(pair, _legs(**both))
    assert released == [] and report.events_updated == 1
    delete.assert_not_called()
    assert pair.amount == 60000
    assert pair.occurred_at == datetime(2026, 3, 20)
    assert (pair.metadata_["from_date"], pair.metadata_["to_date"]) == (
      "2026-03-17",
      "2026-03-20",
    )

  def test_an_unchanged_leg_is_existing(self):
    released, report, delete = self._run(_pair(), _legs())
    assert released == [] and report.events_existing == 1
    delete.assert_not_called()

  def test_an_unposted_pair_whose_legs_part_dissolves(self):
    pair = _pair()
    consumed: set[str] = set()
    released, report, delete = self._run(
      pair, _legs(t_xfer_in={"date": "2026-03-25"}), consumed=consumed
    )
    delete.assert_called_once_with(ANY, ["evt_pair"])
    assert consumed == {"evt_pair"} and report.pairs_dissolved == 1
    by_id = {p["external_id"]: p for p in released}
    assert set(by_id) == {"plaid_txn_t_xfer_out", "plaid_txn_t_xfer_in"}
    # The changed leg from the bank's new record; the other from the pair.
    assert by_id["plaid_txn_t_xfer_in"]["occurred_at"] == "2026-03-25T00:00:00Z"
    assert by_id["plaid_txn_t_xfer_in"]["amount"] == 50000
    assert by_id["plaid_txn_t_xfer_out"]["occurred_at"] == "2026-03-17T00:00:00Z"
    assert by_id["plaid_txn_t_xfer_out"]["amount"] == -50000
    for payload in released:
      assert payload["event_type"] == "external_transfer"
      assert payload["metadata"]["transfer_candidate"] is True

  def test_a_posted_pair_whose_legs_moved_together_is_a_reconciling_item(self):
    pair = _pair(status="committed")
    both = {"t_xfer_out": {"amount": 600.0}, "t_xfer_in": {"amount": -600.0}}
    released, report, delete = self._run(pair, _legs(**both))
    assert released == [] and report.reconciling_items == 1
    delete.assert_not_called()
    assert pair.payload_drift is True and pair.amount == 50000  # books untouched
    accepted = pair.metadata_["drift_payload"]
    assert accepted["source_amount"] == 60000
    assert accepted["source_posted_date"] == "2026-03-19"
    assert [leg["amount"] for leg in accepted["source_legs"]] == [-60000, 60000]
    assert accepted["posting_date"] == "2026-03-19"
    assert accepted["line_items"] == [
      {"element_id": "e_sav", "debit_amount": 60000, "credit_amount": 0},
      {"element_id": "e_chk", "debit_amount": 0, "credit_amount": 60000},
    ]

  def test_a_posted_pair_whose_legs_part_is_reversed_and_releases_both(self):
    pair = _pair(status="committed")
    released, report, delete = self._run(pair, _legs(t_xfer_out={"amount": 450.0}))
    delete.assert_not_called()
    assert report.reconciling_items == 1 and report.pairs_dissolved == 1
    accepted = pair.metadata_["drift_payload"]
    assert accepted["pair_dissolved"] is True and "line_items" not in accepted
    assert accepted["legs"] == [] and pair.metadata_["legs"] == []
    assert set(accepted["released_legs"]) == {"t_xfer_out", "t_xfer_in"}
    assert {p["external_id"] for p in released} == {
      "plaid_txn_t_xfer_out",
      "plaid_txn_t_xfer_in",
    }
    out = next(p for p in released if p["external_id"] == "plaid_txn_t_xfer_out")
    assert out["amount"] == -45000

  def test_a_change_already_raised_is_not_raised_again(self):
    pair = _pair(status="committed")
    pair.payload_drift = True
    pair.metadata_["drift_payload"] = {
      "source_legs": [
        {"transaction_id": "t_xfer_out", "amount": -60000, "date": "2026-03-17"},
        {"transaction_id": "t_xfer_in", "amount": 60000, "date": "2026-03-19"},
      ]
    }
    both = {"t_xfer_out": {"amount": 600.0}, "t_xfer_in": {"amount": -600.0}}
    released, report, _delete = self._run(pair, _legs(**both))
    assert released == [] and report.events_existing == 1
    assert report.reconciling_items == 0


class _QuerySession(_Session):
  def __init__(self, rows):
    super().__init__()
    self.rows = rows

  def execute(self, _statement):
    rows = self.rows
    return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: rows))


@pytest.mark.unit
class TestCounterparts:
  """The other side of a leg is already on the graph."""

  def _other(self, status, contra="e_sav", **meta):
    return _event(
      id="evt_other",
      status=status,
      event_type="external_transfer",
      amount=-50000,
      occurred_at=datetime(2026, 3, 17),
      resource_element_id="e_chk",
      external_id="plaid_txn_t_out",
      metadata_={
        "connection_id": "conn_1",
        "transaction_id": "t_out",
        "account_id": CHECKING_ID,
        "account_name": "Checking",
        "transfer_candidate": True,
        **({"classified_element_id": contra} if contra else {}),
        **meta,
      },
    )

  def _incoming(self):
    return {
      "external_id": "plaid_txn_t_in",
      "event_type": "external_transfer",
      "amount": 50000,
      "occurred_at": "2026-03-19T00:00:00Z",
      "resource_element_id": "e_sav",
      "metadata": {
        "connection_id": "conn_1",
        "transaction_id": "t_in",
        "account_id": SAVINGS_ID,
        "account_name": "Savings",
        "transfer_candidate": True,
      },
    }

  def _settle(self, other, payload=None, **patches):
    payload = payload or self._incoming()
    report = PlaidLoadReport()
    consumed: set[str] = set()
    with (
      patch(f"{MODULE}._merge_into_pair", return_value=True) as merge,
      patch(f"{MODULE}.capture_event", return_value=True) as capture,
      patch(
        f"{MODULE}.existing_events",
        return_value={
          "plaid_txn_t_in": _event(id="evt_new", external_id="plaid_txn_t_in")
        },
      ),
      patch("robosystems.operations.event_block.commands.update_event_block") as void,
    ):
      handled = settle_with_counterpart(
        _Session(),
        other,
        payload,
        graph_id="kg_1",
        connection_id="conn_1",
        item_id=ITEM_ID,
        created_by="usr_1",
        report=report,
        consumed=consumed,
      )
    return SimpleNamespace(
      handled=handled,
      payload=payload,
      report=report,
      consumed=consumed,
      merge=merge,
      capture=capture,
      void=void,
    )

  def test_a_captured_leg_merges(self):
    run = self._settle(self._other("captured", contra=None))
    assert run.handled and run.consumed == {"evt_other"}
    run.merge.assert_called_once()
    run.capture.assert_not_called()

  def test_a_leg_classified_to_this_legs_bank_account_merges(self):
    run = self._settle(self._other("classified", contra="e_sav"))
    assert run.handled and run.merge.call_count == 1

  def test_a_leg_classified_elsewhere_leaves_this_one_to_the_operator(self):
    run = self._settle(self._other("classified", contra="e_draw"))
    assert not run.handled
    run.merge.assert_not_called()
    run.capture.assert_not_called()  # the main loop captures it
    metadata = run.payload["metadata"]
    assert metadata["counterpart_event_id"] == "evt_other"
    assert metadata["counterpart_status"] == "classified"
    assert metadata["classification_source"] == "counterpart"

  def test_a_leg_posted_to_this_legs_bank_account_voids_this_one(self):
    run = self._settle(self._other("committed", contra="e_sav"))
    assert run.handled and run.report.legs_voided == 1
    run.merge.assert_not_called()
    run.capture.assert_called_once()
    body = run.void.call_args.args[1]
    assert body.event_id == "evt_new" and body.transition_to == "voided"
    assert "evt_other" in body.metadata_patch["voided_reason"]
    assert run.void.call_args.kwargs["graph_id"] == "kg_1"

  def test_a_leg_posted_elsewhere_or_through_a_rule_is_captured_with_its_counterpart(
    self,
  ):
    for other in (
      self._other("fulfilled", contra="e_draw"),
      self._other("committed", contra=None),
    ):
      run = self._settle(other)
      assert not run.handled
      run.void.assert_not_called()
      assert run.payload["metadata"]["counterpart_event_id"] == "evt_other"

  def test_a_still_captured_leg_outranks_a_closer_posted_one(self):
    posted = _event(
      id="evt_posted", status="committed", occurred_at=datetime(2026, 3, 19)
    )
    captured = _event(
      id="evt_captured", status="captured", occurred_at=datetime(2026, 3, 16)
    )
    classified = _event(
      id="evt_classified", status="classified", occurred_at=datetime(2026, 3, 18)
    )
    found = find_waiting_leg(
      _QuerySession([posted, classified, captured]),
      self._incoming(),
      connection_id="conn_1",
      consumed=set(),
    )
    assert found is captured


@pytest.mark.unit
class TestFlagPayloads:
  """Every flag rebuilds the bank's state; nothing from the last one rides along."""

  def _resolved(self, **extra):
    """A posted line whose earlier change was resolved: the accepted payload
    is the live metadata now, entry and all."""
    return _event(
      status="committed",
      metadata_={
        "connection_id": "conn_1",
        "transaction_id": "t_coffee",
        "classified_element_id": "e_te",
        "source_amount": -1300,
        "source_posted_date": "2026-03-15",
        "posting_date": "2026-03-15",
        "memo": "Harbor Coffee Co",
        "line_items": [
          {"element_id": "e_te", "debit_amount": 1300, "credit_amount": 0},
          {"element_id": "e_card", "debit_amount": 0, "credit_amount": 1300},
        ],
        "reconciliation_history": [{"disposition": "catch_up"}],
        **extra,
      },
    )

  def test_a_removal_after_a_resolved_change_stashes_no_entry(self):
    event = self._resolved()
    _flag_removed(event, ["t_coffee"])
    accepted = event.metadata_["drift_payload"]
    assert accepted["source_removed"] is True
    assert accepted["classified_element_id"] == "e_te"
    for key in (
      "line_items",
      "posting_date",
      "memo",
      "source_amount",
      "source_posted_date",
      "reconciliation_history",
    ):
      assert key not in accepted, key

  def test_a_second_change_replans_from_scratch(self):
    event = self._resolved()
    report = PlaidLoadReport()
    reconcile_existing(event, _payload(amount=-1500, day="2026-03-16"), report)
    accepted = event.metadata_["drift_payload"]
    assert report.reconciling_items == 1
    assert (accepted["source_amount"], accepted["source_posted_date"]) == (
      -1500,
      "2026-03-16",
    )
    assert accepted["posting_date"] == "2026-03-16"
    assert accepted["line_items"] == [
      {"element_id": "e_te", "debit_amount": 1500, "credit_amount": 0},
      {"element_id": "e_card", "debit_amount": 0, "credit_amount": 1500},
    ]

  def test_a_change_that_cannot_be_replanned_drops_the_stale_entry(self):
    event = self._resolved(classified_element_id=None)  # posted through a rule
    reconcile_existing(event, _payload(amount=-1500), PlaidLoadReport())
    accepted = event.metadata_["drift_payload"]
    assert accepted["source_amount"] == -1500
    assert "line_items" not in accepted and "posting_date" not in accepted


@pytest.mark.unit
class TestRekey:
  """After a re-Link the history comes back under new ids; a replay finds it."""

  def _old(self, txn="old_1", *, status="committed", item_id="item_old", **meta):
    return _event(
      status=status,
      external_id=f"plaid_txn_{txn}",
      metadata_={
        "connection_id": "conn_1",
        "transaction_id": txn,
        "account_id": "acct_old",
        "item_id": item_id,
        "bank_description": "HARBOR COFFEE",
        **meta,
      },
    )

  def _new(self, txn="new_1", **meta):
    payload = _payload(**{"bank_description": "HARBOR COFFEE", **meta})
    payload["external_id"] = f"plaid_txn_{txn}"
    payload["metadata"]["transaction_id"] = txn
    payload["metadata"]["item_id"] = "item_new"
    return payload

  def _rekey(self, payloads, candidates, *, known=None, item_id="item_new"):
    report = PlaidLoadReport()
    with patch(f"{MODULE}._replay_candidates", return_value=list(candidates)):
      out = rekey_replaced_events(
        _Session(),
        payloads,
        known=known or {},
        connection_id="conn_1",
        item_id=item_id,
        report=report,
      )
    return out, report

  def test_an_old_items_event_takes_the_new_identity(self):
    old, new = self._old(), self._new()
    out, report = self._rekey([new], [old])
    assert out == {"plaid_txn_new_1": old} and report.events_rekeyed == 1
    assert old.external_id == "plaid_txn_new_1"
    assert old.metadata_["transaction_id"] == "new_1"
    assert old.metadata_["item_id"] == "item_new"
    assert old.metadata_["account_id"] == CARD_ID
    trail = old.metadata_["rekeyed_from"]
    assert trail[0]["external_id"] == "plaid_txn_old_1"
    assert trail[0]["transaction_id"] == "old_1" and trail[0]["item_id"] == "item_old"
    assert (
      trail[0]["account_id"] == "acct_old" and trail[0]["connection_id"] == "conn_1"
    )

  def test_another_connections_live_line_is_never_touched(self):
    """Same chart account, date and amount, no description on the old side —
    but it still carries another connection's Item, so it is another feed's."""
    theirs = self._old(item_id="item_theirs", connection_id="conn_2")
    theirs.metadata_.pop("bank_description")
    out, report = self._rekey([self._new()], [theirs])
    assert out == {} and report.events_rekeyed == 0
    assert theirs.external_id == "plaid_txn_old_1"

  def test_the_same_items_events_are_never_rekeyed(self):
    out, _ = self._rekey([self._new()], [self._old(item_id="item_new")])
    assert out == {}

  def test_an_event_a_payload_already_identifies_is_never_a_target(self):
    old = self._old(item_id="")
    out, _ = self._rekey([self._new()], [old], known={"plaid_txn_other": old})
    assert out == {}

  def test_descriptions_decide_between_same_day_same_amount_lines(self):
    coffee = self._old("old_c")
    lunch = self._old("old_l", bank_description="HARBOR LUNCH")
    new_lunch = self._new("new_l", bank_description="HARBOR LUNCH")
    new_coffee = self._new("new_c")
    out, _ = self._rekey([new_lunch, new_coffee], [coffee, lunch])
    assert out["plaid_txn_new_l"] is lunch and out["plaid_txn_new_c"] is coffee

  def test_differing_descriptions_never_cross(self):
    out, _ = self._rekey([self._new()], [self._old(bank_description="SOMEWHERE ELSE")])
    assert out == {}

  def test_a_purged_line_matches_without_a_description(self):
    old = self._old(item_id="")
    old.metadata_ = {"connection_id": "conn_old", "classified_element_id": "e_te"}
    out, _ = self._rekey([self._new()], [old])
    assert out == {"plaid_txn_new_1": old}
    assert old.metadata_["connection_id"] == "conn_1"
    assert old.metadata_["classified_element_id"] == "e_te"

  def test_a_posted_line_matches_on_the_date_the_bank_last_reported(self):
    old = self._old(source_amount=-1240, source_posted_date="2026-03-16")
    new = self._new()
    new["occurred_at"] = "2026-03-16T00:00:00Z"
    out, _ = self._rekey([new], [old])
    assert out == {"plaid_txn_new_1": old}

  def test_a_captured_line_matches_on_its_own_columns(self):
    old = self._old(status="captured")
    out, _ = self._rekey([self._new()], [old])
    assert out == {"plaid_txn_new_1": old}

  def test_a_pair_rekeys_its_legs(self):
    pair = _event(
      status="committed",
      event_type="internal_transfer",
      amount=50000,
      resource_element_id="e_sav",
      external_id="plaid_xfer_old_a",
      metadata_={
        "connection_id": "conn_1",
        "item_id": "item_old",
        "legs": ["old_a", "old_b"],
        "from_element_id": "e_chk",
        "to_element_id": "e_sav",
        "from_account_id": "a_old",
        "to_account_id": "b_old",
        "bank_description": "TRANSFER",
      },
    )
    payload = {
      "external_id": "plaid_xfer_new_a",
      "event_type": "internal_transfer",
      "amount": 50000,
      "occurred_at": "2026-03-14T00:00:00Z",
      "resource_element_id": "e_sav",
      "metadata": {
        "connection_id": "conn_1",
        "item_id": "item_new",
        "legs": ["new_a", "new_b"],
        "from_element_id": "e_chk",
        "to_element_id": "e_sav",
        "from_account_id": CHECKING_ID,
        "to_account_id": SAVINGS_ID,
        "bank_description": "TRANSFER",
      },
    }
    out, _ = self._rekey([payload], [pair])
    assert out == {"plaid_xfer_new_a": pair}
    assert pair.external_id == "plaid_xfer_new_a"
    assert pair.metadata_["legs"] == ["new_a", "new_b"]
    assert pair.metadata_["from_account_id"] == CHECKING_ID
    assert pair.metadata_["rekeyed_from"][0]["legs"] == ["old_a", "old_b"]

  def test_nothing_to_match_costs_no_query(self):
    with patch(f"{MODULE}._replay_candidates") as query:
      out = rekey_replaced_events(
        _Session(),
        [self._new()],
        known={"plaid_txn_new_1": self._old()},
        connection_id="conn_1",
        item_id="item_new",
        report=PlaidLoadReport(),
      )
    assert out == {}
    query.assert_not_called()
