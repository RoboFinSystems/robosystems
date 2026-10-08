"""The CDC plan: what the extract asks QuickBooks, and what it leaves for the
load — deletions, and the back-dated days to pull again."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from robosystems.adapters.quickbooks.pipeline.cdc import (
  CDC_ENTITY_LABELS,
  DISTINCT_DATE_CAP,
  PLAN_FILE,
  TXN_ENTITIES,
  WATERMARK_SKEW,
  CdcPlan,
  plan_cdc,
  read_plan,
  write_plan,
)

WATERMARK = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
NOW = datetime(2026, 9, 21, 12, 0, tzinfo=UTC)
WINDOW_START = "2026-08-09"


def _client(changed=None, too_old=False):
  client = MagicMock()
  client.cdc.return_value = (changed or {}, too_old)
  return client


@pytest.mark.unit
class TestPlanCdc:
  def test_no_watermark_is_todays_path(self):
    client = _client()
    plan = plan_cdc(client, None, WINDOW_START, now=NOW)
    assert plan == CdcPlan(
      checked=False, reason="no_watermark", next_watermark="2026-09-21T11:55:00+00:00"
    )
    client.cdc.assert_not_called()

  def test_asks_for_the_transaction_types_the_report_covers(self):
    client = _client()
    plan_cdc(client, WATERMARK, WINDOW_START, now=NOW)
    client.cdc.assert_called_once_with(WATERMARK, list(TXN_ENTITIES))
    assert "Customer" not in TXN_ENTITIES and "Account" not in TXN_ENTITIES

  def test_a_rejected_watermark_falls_through_and_says_so(self):
    log = MagicMock()
    plan = plan_cdc(_client(too_old=True), WATERMARK, WINDOW_START, log=log, now=NOW)
    assert plan.checked is False and plan.reason == "watermark_too_old"
    assert plan.watermark == WATERMARK.isoformat()
    log.warning.assert_called_once()

  def test_a_watermark_past_thirty_days_is_never_sent(self):
    client = _client()
    plan = plan_cdc(client, WATERMARK, WINDOW_START, now=WATERMARK + timedelta(days=30))
    client.cdc.assert_not_called()
    assert plan.checked is False and plan.reason == "watermark_too_old"

  def test_a_naive_watermark_is_utc(self):
    client = _client()
    plan_cdc(client, WATERMARK.replace(tzinfo=None), WINDOW_START, now=NOW)
    client.cdc.assert_called_once_with(WATERMARK, list(TXN_ENTITIES))

  def test_the_next_watermark_is_taken_before_cdc_is_asked(self):
    # A change made while the sync runs lands after this stamp, so the next
    # sync's CDC sees it.
    for client in (_client(), _client(too_old=True)):
      plan = plan_cdc(client, WATERMARK, WINDOW_START, now=NOW)
      assert plan.next_watermark == (NOW - WATERMARK_SKEW).isoformat()

  def test_deletions_and_back_dated_edits_are_planned(self):
    changed = {
      "Invoice": [
        {"Id": "42", "status": "Deleted", "MetaData": {"LastUpdatedTime": "t1"}},
        {"Id": "7", "TxnDate": "2026-05-15"},
        {"Id": "8", "TxnDate": "2026-09-01"},
      ],
      "Purchase": [{"Id": "9", "TxnDate": "2026-05-15"}],
      "JournalEntry": [{"Id": "3", "status": "Deleted"}],
    }
    plan = plan_cdc(_client(changed), WATERMARK, WINDOW_START, now=NOW)
    assert plan.checked is True and plan.changed == 5
    assert plan.deletions == [
      {"entity": "Invoice", "id": "42", "last_updated": "t1"},
      {"entity": "JournalEntry", "id": "3", "last_updated": None},
    ]
    # One pull per distinct back-dated day; in-window edits need none.
    assert plan.extra_windows == [["2026-05-15", "2026-05-15"]]

  def test_many_back_dated_days_become_one_span_from_the_earliest(self):
    rows = [
      {"Id": str(i), "TxnDate": f"2026-03-{i:02d}"}
      for i in range(1, DISTINCT_DATE_CAP + 2)
    ]
    plan = plan_cdc(_client({"Bill": rows}), WATERMARK, WINDOW_START, now=NOW)
    assert plan.extra_windows == [["2026-03-01", "2026-08-08"]]

  def test_the_plan_round_trips_through_the_extract_dir(self, tmp_path):
    plan = CdcPlan(
      checked=True,
      watermark=WATERMARK.isoformat(),
      changed=2,
      deletions=[{"entity": "Bill", "id": "5", "last_updated": None}],
      extra_windows=[["2026-05-15", "2026-05-15"]],
    )
    path = write_plan(tmp_path / "extract", plan)
    assert path.name == PLAN_FILE and json.loads(path.read_text())["checked"]
    assert read_plan(tmp_path / "extract") == plan
    assert read_plan(tmp_path / "elsewhere") is None


@pytest.mark.unit
class TestLabelMap:
  def test_every_requested_entity_has_report_labels(self):
    assert set(CDC_ENTITY_LABELS) == set(TXN_ENTITIES)

  def test_the_labels_observed_on_the_sandbox_are_listed(self):
    # One id per label resolved against every entity type, 2026-10-08.
    assert set(CDC_ENTITY_LABELS["Purchase"]) == {
      "Cash Expense",
      "Expense",
      "Check",
      "Credit Card Expense",
      "Credit Card Credit",
    }
    assert "Refund" in CDC_ENTITY_LABELS["RefundReceipt"]
    assert "Bill Payment (Credit Card)" in CDC_ENTITY_LABELS["BillPayment"]
